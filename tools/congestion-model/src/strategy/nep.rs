use std::collections::BTreeMap;

use crate::model::ChunkExecutionContext;
use crate::strategy::QueueFactory;
use crate::{GGas, QueueId, Receipt, ShardId, TransactionId, GAS_LIMIT, PGAS, TGAS};

pub struct NepStrategy {
    pub shard_id: Option<ShardId>,
    pub all_shards: Vec<ShardId>,
    pub other_shards: Vec<ShardId>,

    // The queues for receipts going to other shards.
    pub outgoing_queues: BTreeMap<ShardId, QueueId>,

    // How much gas are we allowed to send to other shards.
    pub outgoing_gas_limit: BTreeMap<ShardId, GGas>,

    // numbers to fine-tune
    pub min_tx_gas: GGas,
    pub max_tx_gas: GGas,
    pub min_send_limit: GGas,
    pub max_send_limit: GGas,
    pub global_outgoing_congestion_limit: f64,
    pub max_incoming_gas: GGas,
    pub max_incoming_congestion_memory: u64,
    pub max_outgoing_congestion_memory: u64,
    pub max_outgoing_gas: GGas,
}

impl Default for NepStrategy {
    fn default() -> Self {
        Self {
            // parameters which can be set by the model runner
            min_tx_gas: 5 * TGAS,
            max_tx_gas: 500 * TGAS,
            min_send_limit: 0,
            max_send_limit: 30 * PGAS,
            global_outgoing_congestion_limit: 0.9,
            max_incoming_gas: 100 * PGAS,
            max_outgoing_gas: 100 * PGAS,
            max_incoming_congestion_memory: 250_000_000,
            max_outgoing_congestion_memory: 250_000_000,

            // init fills these
            shard_id: Default::default(),
            all_shards: Default::default(),
            other_shards: Default::default(),
            outgoing_queues: Default::default(),
            outgoing_gas_limit: Default::default(),
        }
    }
}

#[derive(Default, Clone)]
struct CongestedShardsInfo {
    incoming_congestion: f64,
    outgoing_congestion: f64,
}

impl crate::CongestionStrategy for NepStrategy {
    fn init(
        &mut self,
        id: crate::ShardId,
        all_shards: &[crate::ShardId],
        queue_factory: &mut dyn QueueFactory,
    ) {
        self.shard_id = Some(id);
        self.all_shards = all_shards.to_vec();
        self.other_shards = all_shards.iter().map(|s| *s).filter(|s| *s != id).collect();

        for shard_id in &self.other_shards {
            let name = format!("outgoing_receipts_{}", shard_id);
            let queue = queue_factory.register_queue(id, &name);
            self.outgoing_queues.insert(*shard_id, queue);
        }
    }

    fn compute_chunk(&mut self, ctx: &mut ChunkExecutionContext) {
        self.init_send_limit(ctx);

        self.process_outgoing_receipts(ctx);

        self.process_new_transactions(ctx);

        self.process_incoming_receipts(ctx);

        self.update_block_info(ctx);
    }
}

impl NepStrategy {
    // Step 1: Compute bandwidth limits to other shards based on the congestion information
    fn init_send_limit(&mut self, ctx: &mut ChunkExecutionContext<'_>) {
        self.outgoing_gas_limit.clear();

        for shard_id in self.other_shards.clone() {
            let CongestedShardsInfo { incoming_congestion, .. } = self.get_info(ctx, &shard_id);
            let send_limit = mix(self.max_send_limit, self.min_send_limit, incoming_congestion);

            self.outgoing_gas_limit.insert(shard_id, send_limit);
        }
    }

    // Step 2: Drain receipts in the outgoing buffer from the previous round
    //
    // Goes through buffered outgoing receipts and sends as many as possible up
    // to the send limit for each shard. Updates the send limit for every sent receipt.
    fn process_outgoing_receipts(&mut self, ctx: &mut ChunkExecutionContext<'_>) {
        for (other_shard_id, queue_id) in &self.outgoing_queues {
            let send_limit = self.outgoing_gas_limit.get_mut(other_shard_id).unwrap();

            loop {
                let Some(receipt) = ctx.queue(*queue_id).front() else {
                    break;
                };

                if receipt.attached_gas > *send_limit {
                    break;
                }

                let receipt = ctx.queue(*queue_id).pop_front().unwrap();
                *send_limit -= receipt.attached_gas;

                ctx.forward_receipt(receipt);
            }
        }
    }

    // Step 3: Convert all transactions to receipts included in the chunk
    //
    // * limit the gas for new transaction based on the self incoming congestion
    // * filter transactions to a shard based on the receiver's memory congestion
    //
    // The outgoing receipts are processed as in `process_outgoing_receipts`.
    fn process_new_transactions(&mut self, ctx: &mut ChunkExecutionContext<'_>) {
        let incoming_congestion = self.get_incoming_congestion(ctx);
        let tx_limit = mix(self.max_tx_gas, self.min_tx_gas, incoming_congestion);

        while ctx.gas_burnt() < tx_limit {
            let Some(tx) = ctx.incoming_transactions().pop_front() else {
                // no more transactions to process
                break;
            };

            if self.get_global_stop(ctx) {
                break;
            }

            if self.get_filter_stop(ctx, tx) {
                break;
            }

            let outgoing = ctx.accept_transaction(tx);
            self.forward_or_buffer(ctx, outgoing);
        }
    }

    // Checks if any of the shards are congested to the point where all shards
    // should stop accepting any transactions.
    //
    // TODO consider smooth slow down
    fn get_global_stop(&mut self, ctx: &mut ChunkExecutionContext<'_>) -> bool {
        for shard_id in self.all_shards.clone() {
            let CongestedShardsInfo { outgoing_congestion, .. } = self.get_info(ctx, &shard_id);
            if outgoing_congestion > self.global_outgoing_congestion_limit {
                return true;
            }
        }

        return false;
    }

    // Checks if the transaction receiver is in a congested shard. If so the
    // transaction should be rejected.
    //
    // TODO consider smooth slow down
    fn get_filter_stop(&mut self, ctx: &mut ChunkExecutionContext<'_>, tx: TransactionId) -> bool {
        let filter_outgoing_congestion_limit = 0.5;

        let receiver = ctx.tx_receiver(tx);
        // Note: I also tried using the incoming congestion for the filter stop instead.
        // Positive
        // The fairness test utilization is about 2x better. (depends on
        // strategy parameters and model config)
        // Negative
        // In the available workloads, it leads to larger queues when big
        // receipts are involved and slightly worse 90th percentile delays in
        // all-to-one workloads.
        // Also, the linear imbalance workloads lose about 50% utilization.
        let CongestedShardsInfo { outgoing_congestion, .. } = self.get_info(ctx, &receiver);
        outgoing_congestion > filter_outgoing_congestion_limit
    }

    // Step 4: Execute receipts in the order of local, delayed, incoming
    // Step 5: Remaining local or incoming receipts are added to the end of the
    // delayed receipts queue
    //
    // In the model there is no distinction between local, delayed and incoming.
    // All of those are stored in the incoming queue so we just process that.
    //
    // Always process as many receipts as allowed by the GAS_LIMIT.
    //
    // The outgoing receipts are processed as in `process_outgoing_receipts`.
    fn process_incoming_receipts(&mut self, ctx: &mut ChunkExecutionContext<'_>) {
        while ctx.gas_burnt() < GAS_LIMIT {
            let Some(receipt) = ctx.incoming_receipts().pop_front() else {
                break;
            };

            let outgoing = ctx.execute_receipt(receipt);
            for receipt in outgoing {
                self.forward_or_buffer(ctx, receipt);
            }
        }
    }

    // Step 6: Compute own congestion information for the next block
    fn update_block_info(&mut self, ctx: &mut ChunkExecutionContext<'_>) {
        let incoming_congestion = self.get_incoming_congestion(ctx);
        let outgoing_congestion = self.get_outgoing_congestion(ctx);

        tracing::debug!(
            target: "model",
            shard_id=?self.shard_id(),
            incoming_congestion=format!("{incoming_congestion:.2}"),
            outgoing_congestion=format!("{outgoing_congestion:.2}"),
            "chunk info"
        );

        let info = CongestedShardsInfo { incoming_congestion, outgoing_congestion };
        ctx.current_block_info().insert(info);
    }

    fn get_incoming_congestion(&self, ctx: &mut ChunkExecutionContext) -> f64 {
        f64::max(self.incoming_memory_congestion(ctx), self.incoming_gas_congestion(ctx))
    }

    fn incoming_memory_congestion(&self, ctx: &mut ChunkExecutionContext) -> f64 {
        let memory_consumption = ctx.incoming_receipts().size();
        let memory_congestion =
            memory_consumption as f64 / self.max_incoming_congestion_memory as f64;
        f64::clamp(memory_congestion, 0.0, 1.0)
    }

    fn incoming_gas_congestion(&self, ctx: &mut ChunkExecutionContext) -> f64 {
        let gas_backlog = ctx.incoming_receipts().attached_gas() as f64;
        f64::clamp(gas_backlog / self.max_incoming_gas as f64, 0.0, 1.0)
    }

    fn get_outgoing_congestion(&self, ctx: &mut ChunkExecutionContext) -> f64 {
        f64::max(self.outgoing_memory_congestion(ctx), self.outgoing_gas_congestion(ctx))
    }

    fn outgoing_memory_congestion(&self, ctx: &mut ChunkExecutionContext) -> f64 {
        let mut memory_consumption = 0;
        for (_, queue_id) in &self.outgoing_queues {
            memory_consumption += ctx.queue(*queue_id).size();
        }

        let memory_congestion =
            memory_consumption as f64 / self.max_outgoing_congestion_memory as f64;
        f64::clamp(memory_congestion, 0.0, 1.0)
    }

    fn outgoing_gas_congestion(&self, ctx: &mut ChunkExecutionContext) -> f64 {
        let mut gas_backlog = 0;
        for (_, queue_id) in &self.outgoing_queues {
            gas_backlog += ctx.queue(*queue_id).attached_gas();
        }

        let gas_congestion = gas_backlog as f64 / self.max_outgoing_gas as f64;
        f64::clamp(gas_congestion, 0.0, 1.0)
    }

    // Forward or buffer a receipt.
    // Local receipts are always forwarded.
    fn forward_or_buffer(&mut self, ctx: &mut ChunkExecutionContext<'_>, receipt: Receipt) {
        let shard_id = receipt.receiver;

        // If we are the receiver just forward the receipt.
        if shard_id == self.shard_id() {
            ctx.forward_receipt(receipt);
            return;
        }

        let send_limit = self.outgoing_gas_limit.get_mut(&shard_id).unwrap();
        if receipt.attached_gas > *send_limit {
            ctx.queue(self.outgoing_queues[&shard_id]).push_back(receipt);
            return;
        }

        *send_limit -= receipt.attached_gas;
        ctx.forward_receipt(receipt);
    }

    fn get_info(
        &mut self,
        ctx: &mut ChunkExecutionContext<'_>,
        shard_id: &ShardId,
    ) -> CongestedShardsInfo {
        let Some(info) = ctx.prev_block_info().get(&shard_id) else {
            // If there is no info assume there is no congestion.
            return CongestedShardsInfo::default();
        };
        info.get::<CongestedShardsInfo>().unwrap().clone()
    }

    fn shard_id(&self) -> ShardId {
        self.shard_id.unwrap()
    }

    /// Define 100% congestion limit in gas.
    pub fn with_gas_limits(mut self, incoming: GGas, outgoing: GGas) -> Self {
        self.max_incoming_gas = incoming;
        self.max_outgoing_gas = outgoing;
        self
    }

    /// Define 100% congestion limit in bytes.
    pub fn with_memory_limits(
        mut self,
        incoming: bytesize::ByteSize,
        outgoing: bytesize::ByteSize,
    ) -> Self {
        self.max_incoming_congestion_memory = incoming.as_u64();
        self.max_outgoing_congestion_memory = outgoing.as_u64();
        self
    }

    /// Gas spent on new transactions.
    pub fn with_tx_gas_limit_range(mut self, min: GGas, max: GGas) -> Self {
        self.min_tx_gas = min;
        self.max_tx_gas = max;
        self
    }

    /// Gas allowance to sent to other shards.
    pub fn with_send_gas_limit_range(mut self, min: GGas, max: GGas) -> Self {
        self.min_send_limit = min;
        self.max_send_limit = max;
        self
    }

    /// At how much % congestion the global stop should kick in.
    pub fn with_global_stop_limit(mut self, congestion_level: f64) -> Self {
        self.global_outgoing_congestion_limit = congestion_level;
        self
    }
}

fn mix(x: u64, y: u64, a: f64) -> u64 {
    assert!(0.0 <= a);
    assert!(a <= 1.0);
    let x = x as f64;
    let y = y as f64;

    let result = x * (1.0 - a) + y * a;

    result as u64
}

use std::collections::BTreeMap;

use crate::model::ChunkExecutionContext;
use crate::strategy::QueueFactory;
use crate::{GAS_LIMIT, GGas, PGAS, QueueId, Receipt, ShardId, TGAS, TransactionId};

pub struct SmoothTrafficLight {
    pub shard_id: Option<ShardId>,
    pub all_shards: Vec<ShardId>,
    pub other_shards: Vec<ShardId>,

    // The queues for receipts going to other shards.
    pub outgoing_buffer: BTreeMap<ShardId, QueueId>,

    // How much gas are we allowed to send to other shards.
    pub outgoing_gas_allowance: BTreeMap<ShardId, GGas>,

    // new tx acceptance
    pub min_tx_gas: GGas,
    pub max_tx_gas: GGas,
    pub reject_tx_congestion_limit: f64,

    // receipt forwarding limits
    pub min_send_limit_amber: GGas,
    pub max_send_limit: GGas,
    pub red_send_limit: GGas,
    pub smooth_slow_down: bool,

    // queue limits to calculate congestion level
    pub red_incoming_gas: GGas,
    pub red_outgoing_gas: GGas,
    pub memory_limit: u64,
}

impl Default for SmoothTrafficLight {
    fn default() -> Self {
        Self {
            // new tx acceptance
            min_tx_gas: 20 * TGAS,
            max_tx_gas: 500 * TGAS,
            reject_tx_congestion_limit: 0.25,

            // receipt forwarding limits
            min_send_limit_amber: 1 * PGAS,
            max_send_limit: 300 * PGAS,
            red_send_limit: 1 * PGAS,
            smooth_slow_down: true,

            // queue limits to calculate congestion level
            red_incoming_gas: 20 * PGAS,
            red_outgoing_gas: 2 * PGAS,
            memory_limit: bytesize::mb(1000u64),

            // init fills these
            shard_id: Default::default(),
            all_shards: Default::default(),
            other_shards: Default::default(),
            outgoing_buffer: Default::default(),
            outgoing_gas_allowance: Default::default(),
        }
    }
}

#[derive(Default, Clone)]
struct CongestedShardsInfo {
    congestion_level: f64,
    allowed_shard: Option<ShardId>,
}

impl crate::CongestionStrategy for SmoothTrafficLight {
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
            self.outgoing_buffer.insert(*shard_id, queue);
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

impl SmoothTrafficLight {
    // Step 1: Compute bandwidth limits to other shards based on the congestion information
    fn init_send_limit(&mut self, ctx: &ChunkExecutionContext<'_>) {
        self.outgoing_gas_allowance.clear();

        for shard_id in self.other_shards.clone() {
            let CongestedShardsInfo { congestion_level, allowed_shard } =
                self.get_info(ctx, &shard_id);
            let send_limit = if congestion_level < 1.0 {
                // amber
                mix(self.max_send_limit, self.min_send_limit_amber, congestion_level)
            } else {
                // red
                if Some(self.shard_id()) == allowed_shard { self.red_send_limit } else { 0 }
            };

            self.outgoing_gas_allowance.insert(shard_id, send_limit);
        }
    }

    // Step 2: Drain receipts in the outgoing buffer from the previous round
    //
    // Goes through buffered outgoing receipts and sends as many as possible up
    // to the send limit for each shard. Updates the send limit for every sent receipt.
    fn process_outgoing_receipts(&mut self, ctx: &mut ChunkExecutionContext<'_>) {
        for (other_shard_id, queue_id) in &self.outgoing_buffer {
            let send_allowance = self.outgoing_gas_allowance.get_mut(other_shard_id).unwrap();

            loop {
                let Some(receipt) = ctx.queue(*queue_id).front() else {
                    break;
                };

                if receipt.attached_gas > *send_allowance {
                    break;
                }

                let receipt = ctx.queue(*queue_id).pop_front().unwrap();
                *send_allowance -= receipt.attached_gas;

                ctx.forward_receipt(receipt);
            }
        }
    }

    // Step 3: Convert all transactions to receipts included in the chunk
    //
    // * limit the gas for new transaction based on the self incoming congestion
    // * filter transactions to a shard based on the receiver's congestion level
    //
    // The outgoing receipts are processed as in `process_outgoing_receipts`.
    fn process_new_transactions(&mut self, ctx: &mut ChunkExecutionContext<'_>) {
        let incoming_congestion = self.incoming_gas_congestion(ctx);
        let tx_allowance = mix(self.max_tx_gas, self.min_tx_gas, incoming_congestion);

        while ctx.gas_burnt() < tx_allowance {
            let Some(tx) = ctx.incoming_transactions().pop_front() else {
                // no more transactions to process
                break;
            };

            if self.get_filter_stop(ctx, tx) {
                // reject receipt
                continue;
            }

            let outgoing = ctx.accept_transaction(tx);
            self.forward_or_buffer(ctx, outgoing);
        }
    }

    // Checks if the transaction receiver is in a congested shard. If so the
    // transaction should be rejected.
    fn get_filter_stop(&self, ctx: &ChunkExecutionContext<'_>, tx: TransactionId) -> bool {
        let receiver = ctx.tx_receiver(tx);

        let CongestedShardsInfo { congestion_level, .. } = self.get_info(ctx, &receiver);
        congestion_level > self.reject_tx_congestion_limit
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
    fn update_block_info(&self, ctx: &mut ChunkExecutionContext<'_>) {
        let incoming_gas_congestion = self.incoming_gas_congestion(ctx);
        let outgoing_gas_congestion = self.outgoing_gas_congestion(ctx);
        let memory_congestion = self.memory_congestion(ctx);

        let max_congestion =
            incoming_gas_congestion.max(outgoing_gas_congestion).max(memory_congestion);
        let red = max_congestion >= 1.0;
        let info = if red {
            CongestedShardsInfo {
                congestion_level: 1.0,
                allowed_shard: Some(self.round_robin_shard(ctx.block_height() as usize)),
            }
        } else if !self.smooth_slow_down {
            // Initial traffic light:
            // Usually, signal other shards to slow down based on our incoming gas congestion only.
            // All other limits (outgoing gas, memory) are ignored until they hit a red light.
            // The goal is to never hit red, unless we have to prevent the unbounded queues vs deadlocks tradeoff.
            // If we hit red, it slows down incoming traffic massively, which should bring us out of red soon.
            CongestedShardsInfo { congestion_level: incoming_gas_congestion, allowed_shard: None }
        } else {
            // Simplified and smoothed.
            // Requires a larger max memory limit but will smoothly reduce based
            // on size, which can even lead to smaller peak buffer size.
            CongestedShardsInfo { congestion_level: max_congestion, allowed_shard: None }
        };

        ctx.current_block_info().insert(info);
    }

    fn round_robin_shard(&self, seed: usize) -> ShardId {
        let num_other_shards = self.all_shards.len() - 1;
        let mut index = (seed + *self.shard_id.unwrap()) % num_other_shards;
        if self.all_shards[index] == self.shard_id() {
            index = self.all_shards.len() - 1;
        }
        self.all_shards[index]
    }

    fn memory_congestion(&self, ctx: &mut ChunkExecutionContext) -> f64 {
        let mut memory_consumption = ctx.incoming_receipts().size();
        for (_, queue_id) in &self.outgoing_buffer {
            memory_consumption += ctx.queue(*queue_id).size();
        }

        f64::clamp(memory_consumption as f64 / self.memory_limit as f64, 0.0, 1.0)
    }

    fn incoming_gas_congestion(&self, ctx: &mut ChunkExecutionContext) -> f64 {
        let gas_backlog = ctx.incoming_receipts().attached_gas() as f64;
        f64::clamp(gas_backlog / self.red_incoming_gas as f64, 0.0, 1.0)
    }

    fn outgoing_gas_congestion(&self, ctx: &mut ChunkExecutionContext) -> f64 {
        let mut gas_backlog = 0;
        for (_, queue_id) in &self.outgoing_buffer {
            gas_backlog += ctx.queue(*queue_id).attached_gas();
        }

        let gas_congestion = gas_backlog as f64 / self.red_outgoing_gas as f64;
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

        let send_limit = self.outgoing_gas_allowance.get_mut(&shard_id).unwrap();
        if receipt.attached_gas > *send_limit {
            ctx.queue(self.outgoing_buffer[&shard_id]).push_back(receipt);
            return;
        }

        *send_limit -= receipt.attached_gas;
        ctx.forward_receipt(receipt);
    }

    fn get_info(&self, ctx: &ChunkExecutionContext<'_>, shard_id: &ShardId) -> CongestedShardsInfo {
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
        self.red_incoming_gas = incoming;
        self.red_outgoing_gas = outgoing;
        self
    }

    /// Define 100% congestion limit in bytes.
    pub fn with_memory_limit(mut self, limit: bytesize::ByteSize) -> Self {
        self.memory_limit = limit.as_u64();
        self
    }

    /// Define at what congestion level new transactions to a shard must be rejected.
    pub fn with_tx_reject_threshold(mut self, threshold: f64) -> Self {
        self.reject_tx_congestion_limit = threshold;
        self
    }

    /// Set to false to use a less smooth strategy for slowing down, only
    /// looking at memory and outgoing congestion once it is at the threshold.
    /// This can give higher utilization but will lead to larger buffers.
    pub fn with_smooth_slow_down(mut self, yes: bool) -> Self {
        self.smooth_slow_down = yes;
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
        self.min_send_limit_amber = min;
        self.max_send_limit = max;
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

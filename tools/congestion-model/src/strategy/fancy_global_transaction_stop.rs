use std::collections::BTreeMap;

use crate::model::ChunkExecutionContext;
use crate::strategy::QueueFactory;
use crate::{GAS_LIMIT, GGas, QueueId, Receipt, ShardId};

/// Fancy Global Transaction Stop implements the following ideas:
/// * At any shard we have a dedicated outgoing queue for every other shard.
/// * There are two types of congestion
///   * The "incoming receipts" congestion
///   * The "outgoing receipts" congestion
/// * The incoming receipts congestion is dealt with by:
///   * The shard, by processing incoming receipts before accepting new transactions.
///   * The other shards, by slowing down the outflow of receipts.
/// * The outgoing receipts congestion is dealt with by:
///   * All shards, by stopping accepting new transactions
///
/// Deadlock prevention
/// This strategy should never deadlock.
/// For the sake of the argument let's assume there is a deadlock.
/// * If incoming receipts is not empty those receipts will be processed and
///   progress will be made.
/// Assume incoming receipts is empty
/// * If any outgoing receipts queue is non empty then some receipts will be
///   forwarded and progress will be made.
/// Assume outgoing receipts queue is empty
/// * When outgoing receipts queues are empty new transactions will be included
///   and progress will be made.
///
/// This can be improved with some small heuristics here and there. I'm not
/// implementing it right now because I want to make sure that the core ideas
/// work without help.
/// * round robin full outflow allowance
/// * filter out transactions aimed at congested shards
pub struct FancyGlobalTransactionStop {
    pub shard_id: Option<ShardId>,
    pub all_shards: Vec<ShardId>,

    // The soft limit on the incoming receipts queue attached gas. We begin to
    // consider a shard congested when the incoming queue attached gas exceeds
    // half of this value. As the incoming queue attached gas approaches the
    // limit other shards will slow down the flow of receipts down to zero.
    pub incoming_attached_gas_limit: GGas,
    // The maximum on how much attached gas we are allowed to send to other
    // shard in a single round. This is a per shard value. In practice the limit
    // can be reduced based on the incoming receipts congestion in the receiving
    // shard.
    pub outgoing_attached_gas_limit: GGas,

    // The queues for receipts going to other shards.
    pub outgoing_receipts: BTreeMap<ShardId, QueueId>,

    // How much attached gas was sent to other shards this round.
    pub sent_outgoing_receipts_gas: BTreeMap<ShardId, GGas>,
}

struct CongestedShardsInfo {
    // The attached gas of the incoming receipts queue.
    // An indicator of the incoming receipts congestion.
    incoming_receipts_attached_gas: GGas,

    // The max of the attached gas of the outgoing receipts queues.
    // An indicator of the outgoing receipts congestion.
    // TODO perhaps sum would be better
    max_outgoing_receipts_attached_gas: GGas,
}

impl crate::CongestionStrategy for FancyGlobalTransactionStop {
    fn init(
        &mut self,
        id: crate::ShardId,
        all_shards: &[crate::ShardId],
        queue_factory: &mut dyn QueueFactory,
    ) {
        for shard_id in all_shards {
            if shard_id == &id {
                continue;
            }
            let name = format!("outgoing_receipts_{}", shard_id);
            let queue = queue_factory.register_queue(id, &name);
            self.outgoing_receipts.insert(*shard_id, queue);
        }

        self.shard_id = Some(id);
        self.all_shards = all_shards.to_vec();
    }

    fn compute_chunk(&mut self, ctx: &mut ChunkExecutionContext) {
        self.process_outgoing_receipts(ctx);

        self.process_incoming_receipts(ctx);

        self.process_new_transactions(ctx);

        self.update_block_info(ctx);
    }
}

impl FancyGlobalTransactionStop {
    // Send the outgoing receipts to other shards. If the other shard is under
    // congestion we limit how much attached gas is allowed to be sent by
    // setting a send_limit limit.
    fn process_outgoing_receipts(&mut self, ctx: &mut ChunkExecutionContext<'_>) {
        self.sent_outgoing_receipts_gas.clear();

        for (other_shard_id, queue_id) in &self.outgoing_receipts {
            let send_limit = self.get_outgoing_gas_limit(ctx, *other_shard_id);
            let sent = self.sent_outgoing_receipts_gas.entry(*other_shard_id).or_default();
            while *sent < send_limit {
                let Some(receipt) = ctx.queue(*queue_id).pop_front() else {
                    break;
                };

                *sent += receipt.attached_gas;
                ctx.forward_receipt(receipt);
            }
        }
    }

    // Process the incoming receipts. Always process as many receipts as allowed
    // by the GAS_LIMIT. The outgoing receipts are handled the same way as in
    // `process_outgoing_receipts`.
    fn process_incoming_receipts(&mut self, ctx: &mut ChunkExecutionContext<'_>) {
        while ctx.gas_burnt() < GAS_LIMIT {
            let Some(receipt) = ctx.incoming_receipts().pop_front() else {
                break;
            };

            let outgoing = ctx.execute_receipt(receipt);
            for receipt in outgoing {
                self.forward_or_queue(ctx, receipt);
            }
        }
    }

    // Process new transactions. The processing is limited to the gas_limit
    // and the outgoing receipts are handled the same way as in
    // `process_outgoing_receipts`.
    fn process_new_transactions(&mut self, ctx: &mut ChunkExecutionContext<'_>) {
        let gas_limit = self.get_new_tx_gas_limit(ctx);

        while ctx.gas_burnt() < gas_limit {
            let Some(tx) = ctx.incoming_transactions().pop_front() else {
                break;
            };

            let outgoing = ctx.accept_transaction(tx);
            self.forward_or_queue(ctx, outgoing);
        }
    }

    // Forward or queue a receipt.
    // Local receipts are always forwarded. FIXME local congestion
    fn forward_or_queue(&mut self, ctx: &mut ChunkExecutionContext<'_>, receipt: Receipt) {
        let shard_id = receipt.receiver;

        // If we are the receiver just forward the receipt.
        if shard_id == self.shard_id.unwrap() {
            ctx.forward_receipt(receipt);
            return;
        }

        // We are only allowed to send up to send_limit attached gas to the shard_id.
        let send_limit = self.get_outgoing_gas_limit(ctx, shard_id);
        let sent = self.sent_outgoing_receipts_gas.entry(shard_id).or_default();
        if *sent >= send_limit {
            ctx.queue(self.outgoing_receipts[&shard_id]).push_back(receipt);
            return;
        }

        // We can safely forward the receipt to the receiver.
        *sent += receipt.attached_gas;
        ctx.forward_receipt(receipt);
    }

    // Returns the new transactions gas limit. This is the limit on the chunk
    // gas beyond which new transactions won't be included. By default it is
    // GAS_LIMIT but it can be reduced if the outgoing receipt queues are
    // filling up. Keep in mind that by default incoming receipts are always
    // included first so if the incoming receipts queue is full then no new
    // transactions will be included anyway.
    fn get_new_tx_gas_limit(&self, ctx: &mut ChunkExecutionContext) -> GGas {
        // Get the max of the outgoing receipts queues across all shards.
        // This is the same as global transaction stop.
        let max_gas = self.get_global_max_outgoing_receipts_attached_gas(ctx);

        // How much remaining space is there in the fullest queue.
        let remaining = self.outgoing_attached_gas_limit.saturating_sub(max_gas);

        // If the fullest queue is less than half full we want to process a full chunk.
        let half_max = self.outgoing_attached_gas_limit / 2;
        if remaining > half_max {
            return GAS_LIMIT;
        }

        // If the buffer is more than half full scale the GAS_LIMIT.
        // It's linear with respect to `remaining` with the borders at
        // * remaining == 0        -> gas limit == 0
        // * remaining == half_max -> gas_limit == GAS_LIMIT
        // TODO overflows?
        GAS_LIMIT * remaining / half_max
    }

    // Returns the outgoing gas limit. This is how much attached gas we allow
    // ourselves to send to the other_shard_id. By default it is
    // max_incoming_attached_gas but it can be reduced if the other shard
    // incoming receipts queue is filling up.
    fn get_outgoing_gas_limit(
        &self,
        ctx: &mut ChunkExecutionContext,
        other_shard_id: ShardId,
    ) -> GGas {
        let Some(info) = ctx.prev_block_info().get(&other_shard_id) else {
            return self.outgoing_attached_gas_limit;
        };
        let info = info.get::<CongestedShardsInfo>().unwrap();

        let remaining =
            self.incoming_attached_gas_limit.saturating_sub(info.incoming_receipts_attached_gas);

        // If the queue is less than half full we want to process a full chunk.
        let half_max = self.incoming_attached_gas_limit / 2;
        if remaining > half_max {
            return self.outgoing_attached_gas_limit;
        }

        // If the buffer is more than half full scale the max_outgoing_attached_gas.
        // It's linear with respect to `remaining` with the borders at
        // * remaining == 0        -> gas limit == 0
        // * remaining == half_max -> gas_limit == max_outgoing_attached_gas
        // TODO overflows?
        self.outgoing_attached_gas_limit * remaining / half_max
    }

    fn get_max_outgoing_receipts_attached_gas(&self, ctx: &mut ChunkExecutionContext) -> GGas {
        let mut result: GGas = 0;
        for (_, queue_id) in &self.outgoing_receipts {
            let queue_attached_gas = ctx.queue(*queue_id).attached_gas();
            result = std::cmp::max(result, queue_attached_gas);
        }

        result
    }

    fn get_global_max_outgoing_receipts_attached_gas(
        &self,
        ctx: &mut ChunkExecutionContext,
    ) -> GGas {
        let mut result = 0;
        for shard_id in &self.all_shards {
            let Some(info) = ctx.prev_block_info().get(shard_id) else {
                continue;
            };
            let info = info.get::<CongestedShardsInfo>().unwrap();
            result = result.max(info.max_outgoing_receipts_attached_gas);
        }
        result
    }

    fn update_block_info(&self, ctx: &mut ChunkExecutionContext<'_>) {
        let info = CongestedShardsInfo {
            incoming_receipts_attached_gas: ctx.incoming_receipts().attached_gas(),
            max_outgoing_receipts_attached_gas: self.get_max_outgoing_receipts_attached_gas(ctx),
        };
        ctx.current_block_info().insert(info);
    }
}

impl Default for FancyGlobalTransactionStop {
    fn default() -> Self {
        Self {
            // * assume transactions burn 10% of attached gas
            // * aim for buffering receipts to fill 3 chunks
            incoming_attached_gas_limit: 3 * 10 * GAS_LIMIT,
            // * assume transactions burn 10% of attached gas
            // * aim for buffering receipts to fill 2 chunks
            //   * in practice this may get multiplied by num_shards
            outgoing_attached_gas_limit: 2 * 10 * GAS_LIMIT,

            outgoing_receipts: BTreeMap::new(),
            sent_outgoing_receipts_gas: BTreeMap::new(),

            // overwritten at init
            shard_id: None,
            all_shards: vec![],
        }
    }
}

use crate::model::ChunkExecutionContext;
use crate::strategy::QueueFactory;
use crate::{QueueId, Receipt, ShardId, GAS_LIMIT, TX_GAS_LIMIT};

/// Have a fixed max queue size per shard and apply backpressure by stop
/// forwarding receipts when a receiving shard has reached its limit.
pub struct SimpleBackpressure {
    pub max_receipts: usize,
    pub delayed_outgoing_receipts: Option<QueueId>,
    pub id: Option<ShardId>,
}

struct CongestedShardsInfo {
    congested: bool,
}

impl crate::CongestionStrategy for SimpleBackpressure {
    fn init(
        &mut self,
        id: crate::ShardId,
        _other_shards: &[crate::ShardId],
        queue_factory: &mut dyn QueueFactory,
    ) {
        self.delayed_outgoing_receipts =
            Some(queue_factory.register_queue(id, "delayed_outgoing_receipts"));
        self.id = Some(id);
    }

    fn compute_chunk(&mut self, ctx: &mut ChunkExecutionContext) {
        // first attempt forwarding previously buffered outgoing receipts
        let buffered: Vec<_> =
            ctx.queue(self.delayed_outgoing_receipts.unwrap()).drain(..).collect();
        for receipt in buffered {
            self.forward_or_buffer(receipt, ctx);
        }

        // stop accepting transactions our local queue capacities are exhausted
        while !self.congested(ctx) && ctx.gas_burnt() < TX_GAS_LIMIT {
            if let Some(tx) = ctx.incoming_transactions().pop_front() {
                let outgoing = ctx.accept_transaction(tx);
                self.forward_or_buffer(outgoing, ctx);
            } else {
                // no more transaction incoming
                break;
            }
        }

        // keep executing existing receipts even when shards are congested
        while ctx.gas_burnt() < GAS_LIMIT {
            if let Some(receipt) = ctx.incoming_receipts().pop_front() {
                let outgoing = ctx.execute_receipt(receipt);
                for receipt in outgoing {
                    self.forward_or_buffer(receipt, ctx);
                }
            } else {
                // no more receipts to execute
                break;
            }
        }

        // share info with other shards
        let info = CongestedShardsInfo { congested: self.congested(ctx) };
        ctx.current_block_info().insert(info);
    }
}

impl SimpleBackpressure {
    fn forward_or_buffer(&self, receipt: Receipt, ctx: &mut ChunkExecutionContext) {
        if let Some(info) = ctx.prev_block_info().get(&receipt.receiver) {
            if info.get::<CongestedShardsInfo>().unwrap().congested
                && receipt.receiver != self.id.unwrap()
            {
                ctx.queue(self.delayed_outgoing_receipts.unwrap()).push_back(receipt);
                return;
            }
        }
        ctx.forward_receipt(receipt);
    }

    fn total_queued_receipts(&self, ctx: &mut ChunkExecutionContext) -> usize {
        ctx.incoming_receipts().len() + ctx.queue(self.delayed_outgoing_receipts.unwrap()).len()
    }

    fn congested(&mut self, ctx: &mut ChunkExecutionContext) -> bool {
        let congested = self.total_queued_receipts(ctx) > self.max_receipts;
        congested
    }
}

impl Default for SimpleBackpressure {
    fn default() -> Self {
        Self {
            max_receipts: 1000,

            // overwritten at init
            delayed_outgoing_receipts: None,
            id: None,
        }
    }
}

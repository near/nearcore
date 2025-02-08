use crate::model::ChunkExecutionContext;
use crate::strategy::QueueFactory;
use crate::{GAS_LIMIT, TX_GAS_LIMIT};

/// Stop all shards from accepting new transactions when a limit of delayed
/// receipts is reached in any shard.
pub struct GlobalTxStopShard {
    pub max_delayed_receipts: usize,
}

struct DelayedQueueInfo {
    num_delayed: usize,
}

impl crate::CongestionStrategy for GlobalTxStopShard {
    fn init(
        &mut self,
        _id: crate::ShardId,
        _other_shards: &[crate::ShardId],
        _queue_factory: &mut dyn QueueFactory,
    ) {
    }

    fn compute_chunk(&mut self, ctx: &mut ChunkExecutionContext) {
        let mut any_shard_congested = false;
        for shard_info in ctx.prev_block_info().values() {
            if shard_info.get::<DelayedQueueInfo>().unwrap().num_delayed > self.max_delayed_receipts
            {
                any_shard_congested = true;
            }
        }

        // stop accepting transactions when any shard is congested
        if !any_shard_congested {
            while ctx.gas_burnt() < TX_GAS_LIMIT {
                if let Some(tx) = ctx.incoming_transactions().pop_front() {
                    let outgoing = ctx.accept_transaction(tx);
                    ctx.forward_receipt(outgoing);
                } else {
                    // no more transaction incoming
                    break;
                }
            }
        }

        // keep executing existing receipts even when a shard is congested
        while ctx.gas_burnt() < GAS_LIMIT {
            if let Some(receipt) = ctx.incoming_receipts().pop_front() {
                let outgoing = ctx.execute_receipt(receipt);
                for receipt in outgoing {
                    ctx.forward_receipt(receipt);
                }
            } else {
                // no more receipts to execute
                break;
            }
        }

        // share info with other shards
        let info = DelayedQueueInfo { num_delayed: ctx.incoming_receipts().len() };
        ctx.current_block_info().insert(info);
    }
}

impl Default for GlobalTxStopShard {
    fn default() -> Self {
        Self { max_delayed_receipts: 1000 }
    }
}

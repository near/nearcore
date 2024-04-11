use crate::model::ChunkExecutionContext;
use crate::strategy::QueueFactory;
use crate::{GAS_LIMIT, TX_GAS_LIMIT};

pub struct NoQueueShard {}

impl crate::CongestionStrategy for NoQueueShard {
    fn init(
        &mut self,
        _id: crate::ShardId,
        _other_shards: &[crate::ShardId],
        _queue_factory: &mut dyn QueueFactory,
    ) {
    }

    fn compute_chunk(&mut self, ctx: &mut ChunkExecutionContext) {
        while ctx.gas_burnt() < TX_GAS_LIMIT {
            if let Some(tx) = ctx.incoming_transactions().pop_front() {
                let outgoing = ctx.accept_transaction(tx);
                ctx.forward_receipt(outgoing);
            } else {
                // no more transaction incoming
                break;
            }
        }
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
    }
}

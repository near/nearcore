use crate::GAS_LIMIT;
use crate::model::ChunkExecutionContext;
use crate::strategy::QueueFactory;

#[derive(Default)]
/// No queues, no backpressure. But always prioritize existing receipts over new
/// transactions. This can easily starve a shard but it maximizes how many
/// transactions get done in the global system.
pub struct NewTxLast {}

impl crate::CongestionStrategy for NewTxLast {
    fn init(
        &mut self,
        _id: crate::ShardId,
        _other_shards: &[crate::ShardId],
        _queue_factory: &mut dyn QueueFactory,
    ) {
    }

    fn compute_chunk(&mut self, ctx: &mut ChunkExecutionContext) {
        // Start with receipts and reserve no chunk space to new transactions.
        // In contrast to nearcore today, which gives new transactions priority with up to halve the chunks space.
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
        while ctx.gas_burnt() < GAS_LIMIT {
            if let Some(tx) = ctx.incoming_transactions().pop_front() {
                let outgoing = ctx.accept_transaction(tx);
                ctx.forward_receipt(outgoing);
            } else {
                // no more transaction incoming
                break;
            }
        }
    }
}

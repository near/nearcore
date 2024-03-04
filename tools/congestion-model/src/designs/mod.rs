use crate::model::ChunkExecutionContext;
use crate::{QueueId, ShardId};

pub use global_tx_stop::GlobalTxStopShard;
pub use no_queues::NoQueueShard;
pub use simple_backpressure::SimpleBackpressure;

mod global_tx_stop;
mod no_queues;
mod simple_backpressure;

/// Implement the shard behavior to define a new congestion control design.
pub trait Shard {
    /// Initial state and register all necessary queues.
    fn init(&mut self, id: ShardId, other_shards: &[ShardId], queue_factory: &mut dyn QueueFactory);

    /// Decide which receipts to execute, which to delay, and which to forward.
    fn compute_chunk(&mut self, ctx: &mut ChunkExecutionContext);
}

/// Use this to create queues.
pub trait QueueFactory {
    fn register_queue(&mut self, to: ShardId) -> QueueId;
}

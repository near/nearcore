use crate::model::ChunkExecutionContext;
use crate::{QueueId, ShardId};

pub use fancy_global_transaction_stop::FancyGlobalTransactionStop;
pub use global_tx_stop::GlobalTxStopShard;
pub use nep::NepStrategy;
pub use new_tx_last::NewTxLast;
pub use no_queues::NoQueueShard;
pub use simple_backpressure::SimpleBackpressure;
pub use traffic_light::TrafficLight;

mod fancy_global_transaction_stop;
mod global_tx_stop;
mod nep;
mod new_tx_last;
mod no_queues;
mod simple_backpressure;
mod traffic_light;

/// Implement the shard behavior to define a new congestion control strategy.
///
/// The model execution will take one `CongestionStrategy` trait object per
/// shard. They normally are all of the same concrete type and hence execute
/// the same code on each shard.
pub trait CongestionStrategy {
    /// Initial state and register all necessary queues for one shard.
    fn init(&mut self, id: ShardId, other_shards: &[ShardId], queue_factory: &mut dyn QueueFactory);

    /// Decide which receipts to execute, which to delay, and which to forward.
    fn compute_chunk(&mut self, ctx: &mut ChunkExecutionContext);
}

/// Use this to create queues.
pub trait QueueFactory {
    fn register_queue(&mut self, to: ShardId, name: &str) -> QueueId;
}

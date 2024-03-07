use crate::model::ChunkExecutionContext;
use crate::{QueueId, ShardId};

pub use global_tx_stop::GlobalTxStopShard;
pub use no_queues::NoQueueShard;
pub use simple_backpressure::SimpleBackpressure;

mod global_tx_stop;
mod no_queues;
mod simple_backpressure;

// The stats writer can be used to dump stats into a CSV file.
// In the strategy implementation you should write the header in the init method
// and the individual values in the compute_chunk method. Please use the
// write_field. The model will take care of writing the record terminator.
pub type StatsWriter = Option<Box<csv::Writer<std::fs::File>>>;

/// Implement the shard behavior to define a new congestion control strategy.
///
/// The model execution will take one `CongestionStrategy` trait object per
/// shard. They normally are all of the same concrete type and hence execute
/// the same code on each shard.
pub trait CongestionStrategy {
    /// Initial state and register all necessary queues for one shard.
    fn init(
        &mut self,
        id: ShardId,
        other_shards: &[ShardId],
        queue_factory: &mut dyn QueueFactory,
        stats_writer: &mut StatsWriter,
    );

    /// Decide which receipts to execute, which to delay, and which to forward.
    fn compute_chunk(&mut self, ctx: &mut ChunkExecutionContext, stats_writer: &mut StatsWriter);
}

/// Use this to create queues.
pub trait QueueFactory {
    fn register_queue(&mut self, to: ShardId) -> QueueId;
}

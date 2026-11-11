pub mod error;

pub use error::Error;

/// Which shards to apply, out of the ones the node tracks in this or the next epoch. A catch-up shard
/// is one it tracks in the next epoch but in neither this epoch nor the one before
/// (`ShardTracker::should_catch_up_shard`), so its state comes from catchup state sync.
#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub enum ApplyChunksMode {
    /// All of them: block processing, once the state of the catch-up shards is there.
    IncludeCatchupShards,
    /// All but the catch-up shards: block processing, before their state is there.
    ExcludeCatchupShards,
    /// Only the catch-up shards: `catchup_blocks`, applying the blocks held back for them.
    OnlyCatchupShards,
}

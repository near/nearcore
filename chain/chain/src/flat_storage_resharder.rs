//! Logic for resharding flat storage in parallel to chain processing.
//!
//! See [FlatStorageReshard] for more details about how the resharding takes place.

use std::sync::Arc;

use near_epoch_manager::EpochManagerAdapter;
use near_primitives::types::BlockHeight;

use crate::types::RuntimeAdapter;

/// `FlatStorageReshard` takes care of updating flat storage when a resharding event
/// happens.
///
/// On an high level, the operations supported are:
/// - #### Shard splitting
///     Parent shard must be split into two children. The entire operation freezes the flat storage
///     for the involved shards.
///     Children shards are created empty and the key-values of the parent will be copied into one of them,
///     in the background.
///
///     After the copy is finished the children shard will have the correct state at some past block height.
///     It'll be necessary to perform catchup before the flat storage can be put again in Ready state.
///     The parent shard storage is not needed anymore and can be removed.
pub struct FlatStorageReshard {
    /// Height on top of which this struct was created.
    start_height: BlockHeight,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    runtime: Arc<dyn RuntimeAdapter>,
    // TODO(Trisfald)
    // add shard_uid parent, children
    // add metrics
    // add object to hold intermediate state
}

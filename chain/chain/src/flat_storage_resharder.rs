//! Logic for resharding flat storage in parallel to chain processing.
//!
//! See [FlatStorageResharder] for more details about how the resharding takes place.

use std::sync::{Arc, Mutex};

use near_epoch_manager::EpochManagerAdapter;
use near_store::{flat::FlatStorageReshardingStatus, ShardUId, StorageError};
use tracing::{error, info};

use crate::types::RuntimeAdapter;

/// `FlatStorageResharder` takes care of updating flat storage when a resharding event
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
pub struct FlatStorageResharder {
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    runtime: Arc<dyn RuntimeAdapter>,
    resharding_event: Mutex<Option<FlatStorageReshardingEvent>>,
}

impl FlatStorageResharder {
    /// Creates a new `FlatStorageResharder`.
    pub fn new(
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime: Arc<dyn RuntimeAdapter>,
    ) -> Self {
        let resharding_event = Mutex::new(None);
        Self { epoch_manager, runtime, resharding_event }
    }

    /// Resumes a resharding operation that was in progress.
    pub fn resume(
        &self,
        shard_uid: &ShardUId,
        status: &FlatStorageReshardingStatus,
    ) -> Result<(), StorageError> {
        match status {
            FlatStorageReshardingStatus::CreatingChild => {
                // Nothing to do here because the parent will take care of resuming work.
            }
            FlatStorageReshardingStatus::SplittingParent(_) => {
                let parent_shard_uid = shard_uid;
                info!(target: "resharding", ?parent_shard_uid, "resuming flat storage resharding");
                self.check_no_resharding_in_progress()?;
                // On resume flat storage status is already set.
                // TODO(trisfald): create flat storage resharding event
            }
            FlatStorageReshardingStatus::CatchingUp(_) => {
                // TODO(trisfald)
                todo!()
            }
        }
        Ok(())
    }

    /// Starts the operation of splitting a parent shard flat storage into two children.
    pub fn split_shard(&self, parent_shard_uid: &ShardUId) -> Result<(), StorageError> {
        info!(target: "resharding", ?parent_shard_uid, "initiating flat storage split");
        self.check_no_resharding_in_progress()?;
        // Change parent shard flat storage status.

        // todo(Trisfald): create flat storage resharding event
        Ok(())
    }

    fn check_no_resharding_in_progress(&self) -> Result<(), StorageError> {
        // Do not allow multiple resharding operations in parallel.
        if self.resharding_event.lock().unwrap().is_some() {
            error!(target: "resharding", "trying to start a new flat storage resharding operation while one is already in progress!");
            Err(StorageError::FlatStorageReshardingAlreadyInProgress)
        } else {
            Ok(())
        }
    }
}

/// Struct to describe, perform and track progress of a flat storage resharding.
pub struct FlatStorageReshardingEvent {
    // TODO(Trisfald)
    // add shard_uid parent, children
    // add metrics
    // add object to hold intermediate state
}

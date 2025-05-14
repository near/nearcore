use crate::adapter::flat_store::{FlatStoreAdapter, FlatStoreUpdateAdapter};
use crate::flat::{
    BlockInfo, FlatStorageReadyStatus, FlatStorageReshardingStatus, FlatStorageStatus,
};
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::{BlockHeight, RawStateChangesWithTrieKey};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

use super::chunk_view::FlatStorageChunkView;
use super::{
    FlatStateChanges, FlatStateDelta, FlatStateDeltaMetadata, FlatStorage, FlatStorageError,
};

/// `FlatStorageManager` provides a way to construct new flat state to pass to new tries.
/// It is owned by NightshadeRuntime, and thus can be owned by multiple threads, so the implementation
/// must be thread safe.
#[derive(Clone)]
pub struct FlatStorageManager(Arc<FlatStorageManagerInner>);

/// Stores the reference block hash and min height of all the prev hashes of that block's chunks.
struct SnapshotBlock {
    block_hash: CryptoHash,
    min_chunk_prev_height: BlockHeight,
}

pub struct FlatStorageManagerInner {
    store: FlatStoreAdapter,
    /// Here we store the flat_storage per shard. The reason why we don't use the same
    /// FlatStorage for all shards is that there are two modes of block processing,
    /// normal block processing and block catchups. Since these are performed on different range
    /// of blocks, we need flat storage to be able to support different range of blocks
    /// on different shards. So we simply store a different state for each shard.
    /// This may cause some overhead because the data like shards that the node is processing for
    /// this epoch can share the same `head` and `tail`, similar for shards for the next epoch,
    /// but such overhead is negligible comparing the delta sizes, so we think it's ok.
    flat_storages: Mutex<HashMap<ShardUId, FlatStorage>>,
    /// Set to Some() when there's a state snapshot in progress. Used to signal to the resharding flat
    /// storage catchup code that it shouldn't advance past this block height
    want_snapshot: Mutex<Option<SnapshotBlock>>,
}

impl FlatStorageManager {
    pub fn new(store: FlatStoreAdapter) -> Self {
        Self(Arc::new(FlatStorageManagerInner {
            store,
            flat_storages: Default::default(),
            want_snapshot: Default::default(),
        }))
    }

    /// When a node starts from an empty database, this function must be called to ensure
    /// information such as flat head is set up correctly in the database.
    /// Note that this function is different from `create_flat_storage_for_shard`,
    /// it must be called before `create_flat_storage_for_shard` if the node starts from
    /// an empty database.
    pub fn set_flat_storage_for_genesis(
        &self,
        store_update: &mut FlatStoreUpdateAdapter,
        shard_uid: ShardUId,
        genesis_block: &CryptoHash,
        genesis_height: BlockHeight,
    ) {
        let flat_storages = self.0.flat_storages.lock();
        assert!(!flat_storages.contains_key(&shard_uid));
        store_update.set_flat_storage_status(
            shard_uid,
            FlatStorageStatus::Ready(FlatStorageReadyStatus {
                flat_head: BlockInfo::genesis(*genesis_block, genesis_height),
            }),
        );
    }

    /// Creates flat storage instance for shard `shard_uid`. This function
    /// allows creating flat storage if it already exists even though it's not
    /// desired. It needs to allow that to cover a resharding restart case.
    /// TODO (#7327): this behavior may change when we implement support for state sync
    /// and resharding.
    pub fn create_flat_storage_for_shard(&self, shard_uid: ShardUId) -> Result<(), StorageError> {
        tracing::debug!(target: "store", ?shard_uid, "Creating flat storage for shard");
        let want_snapshot = self.0.want_snapshot.lock();
        let disable_updates = want_snapshot.is_some();

        let mut flat_storages = self.0.flat_storages.lock();
        let flat_storage = FlatStorage::new(self.0.store.clone(), shard_uid)?;
        if disable_updates {
            flat_storage.set_flat_head_update_mode(false);
        }
        let original_value = flat_storages.insert(shard_uid, flat_storage);
        if original_value.is_some() {
            // Generally speaking this shouldn't happen. Starting from resharding V3 it shouldn't
            // happen even if the node is restarted.
            tracing::warn!(target: "store", ?shard_uid, "Creating flat storage for shard that already has flat storage.");
        }
        Ok(())
    }

    /// Sets the status to `Ready` if it's currently `Resharding(CatchingUp)`
    fn mark_flat_storage_ready(&self, shard_uid: ShardUId) -> Result<(), StorageError> {
        // Don't use Self::get_flat_storage_status() because there's no need to panic if this fails, since this is used
        // during state snapshotting where an error isn't critical to node operation.
        let status = self.0.store.get_flat_storage_status(shard_uid)?;
        let flat_head = match status {
            FlatStorageStatus::Ready(_) => return Ok(()),
            FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CatchingUp(flat_head)) => {
                flat_head
            }
            _ => {
                return Err(StorageError::StorageInconsistentState(format!(
                    "Unexpected flat storage status: {:?}",
                    &status
                )));
            }
        };
        let mut store_update = self.0.store.store_update();
        store_update.set_flat_storage_status(
            shard_uid,
            FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head }),
        );
        // TODO: Consider adding a StorageError::IO variant?
        store_update.commit().map_err(|_| StorageError::StorageInternalError)?;
        Ok(())
    }

    // If the flat storage status is Resharding(CatchingUp), sets it to Ready(), and then calls create_flat_storage_for_shard()
    // This is used in creating state snapshots when this might be a flat storage that is in the middle of catchup, and that
    // should now be considered `Ready` in the state snapshot, even if not in the main DB.
    pub fn mark_ready_and_create_flat_storage(
        &self,
        shard_uid: ShardUId,
    ) -> Result<(), StorageError> {
        self.mark_flat_storage_ready(shard_uid)?;
        self.create_flat_storage_for_shard(shard_uid)
    }

    /// Update flat storage for given processed or caught up block, which includes:
    /// - merge deltas from current flat storage head to new one given in
    /// `new_flat_head`;
    /// - update flat storage head to the new one;
    /// - remove info about unreachable blocks from memory.
    pub fn update_flat_storage_for_shard(
        &self,
        shard_uid: ShardUId,
        new_flat_head: CryptoHash,
    ) -> Result<(), StorageError> {
        if let Some(flat_storage) = self.get_flat_storage_for_shard(shard_uid) {
            // Try to update flat head.
            flat_storage.update_flat_head(&new_flat_head).unwrap_or_else(|err| {
                match &err {
                    FlatStorageError::BlockNotSupported(_) => {
                        // It's possible that new head is not a child of current flat head, e.g. when we have a
                        // fork:
                        //
                        //      (flat head)        /-------> 6
                        // 1 ->      2     -> 3 -> 4
                        //                         \---> 5
                        //
                        // where during postprocessing (5) we call `update_flat_head(3)` and then for (6) we can
                        // call `update_flat_head(2)` because (2) will be last visible final block from it.
                        // In such case, just log an error.
                        debug!(
                            target: "store",
                            ?new_flat_head,
                            ?err,
                            ?shard_uid,
                            "Cannot update flat head");
                    }
                    _ => {
                        // All other errors are unexpected, so we panic.
                        panic!("Cannot update flat head of shard {shard_uid:?} to {new_flat_head:?}: {err:?}");
                    }
                }
            });
        } else {
            tracing::debug!(target: "store", ?shard_uid, ?new_flat_head, "No flat storage!!!");
        }
        Ok(())
    }

    pub fn save_flat_state_changes(
        &self,
        block_hash: CryptoHash,
        prev_hash: CryptoHash,
        height: BlockHeight,
        shard_uid: ShardUId,
        state_changes: &[RawStateChangesWithTrieKey],
    ) -> Result<FlatStoreUpdateAdapter<'static>, StorageError> {
        let prev_block_with_changes = if state_changes.is_empty() {
            // The current block has no flat state changes.
            // Find the last block with flat state changes by looking it up in
            // the prev block.
            self.0
                .store
                .get_prev_block_with_changes(shard_uid, block_hash, prev_hash)
                .map_err(|e| StorageError::from(e))?
        } else {
            // The current block has flat state changes.
            None
        };

        let delta = FlatStateDelta {
            changes: FlatStateChanges::from_state_changes(state_changes),
            metadata: FlatStateDeltaMetadata {
                block: BlockInfo { hash: block_hash, height, prev_hash },
                prev_block_with_changes,
            },
        };

        let store_update = if let Some(flat_storage) = self.get_flat_storage_for_shard(shard_uid) {
            // If flat storage exists, we add a block to it.
            flat_storage.add_delta(delta).map_err(|e| StorageError::from(e))?
        } else {
            // Otherwise, save delta to disk so it will be used for flat storage creation later.
            debug!(target: "store", %shard_uid, "Add delta for flat storage creation");
            let mut store_update = self.0.store.store_update();
            store_update.set_delta(shard_uid, &delta);
            store_update
        };

        Ok(store_update)
    }

    pub fn get_flat_storage_status(&self, shard_uid: ShardUId) -> FlatStorageStatus {
        self.0.store.get_flat_storage_status(shard_uid).expect("failed to read flat storage status")
    }

    /// Creates `FlatStorageChunkView` to access state for `shard_uid` and block `block_hash`.
    /// Note that:
    /// * the state includes changes by the block `block_hash`;
    /// * request to get value locks shared `FlatStorage` struct which may cause write slowdowns.
    pub fn chunk_view(
        &self,
        shard_uid: ShardUId,
        block_hash: CryptoHash,
    ) -> Option<FlatStorageChunkView> {
        let flat_storage = {
            let flat_storages = self.0.flat_storages.lock();
            // It is possible that flat storage state does not exist yet because it is being created in
            // background.
            match flat_storages.get(&shard_uid) {
                Some(flat_storage) => flat_storage.clone(),
                None => {
                    debug!(target: "store", "FlatStorage is not ready");
                    return None;
                }
            }
        };
        Some(FlatStorageChunkView::new(self.0.store.clone(), block_hash, flat_storage))
    }

    // TODO (#7327): consider returning Result<FlatStorage, Error> when we expect flat storage to exist
    pub fn get_flat_storage_for_shard(&self, shard_uid: ShardUId) -> Option<FlatStorage> {
        let flat_storages = self.0.flat_storages.lock();
        flat_storages.get(&shard_uid).cloned()
    }

    /// Removes FlatStorage object from FlatStorageManager.
    /// If FlatStorageManager did have that object, then removes all information about Flat State and returns Ok(true).
    /// Otherwise does nothing and returns Ok(false).
    pub fn remove_flat_storage_for_shard(
        &self,
        shard_uid: ShardUId,
        store_update: &mut FlatStoreUpdateAdapter,
    ) -> Result<bool, StorageError> {
        let mut flat_storages = self.0.flat_storages.lock();
        if let Some(flat_store) = flat_storages.remove(&shard_uid) {
            flat_store.clear_state(store_update)?;
            tracing::info!(target: "store", ?shard_uid, "remove_flat_storage_for_shard successful");
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Returns None if there's no resharding flat storage split in progress
    /// If there is, returns Some(None) if there's at least one child shard that hasn't been split and had its
    /// status set to `CatchingUp`. If they've all been split already and are in the catchup phase,
    /// returns the lowest height among all shards that resharding catchup has advanced to.
    pub fn resharding_catchup_height_reached(
        &self,
        shard_uids: impl Iterator<Item = ShardUId>,
    ) -> Result<Option<Option<BlockHeight>>, StorageError> {
        let mut ret = None;
        for shard_uid in shard_uids {
            match self.0.store.get_flat_storage_status(shard_uid)? {
                FlatStorageStatus::Resharding(FlatStorageReshardingStatus::CatchingUp(
                    flat_head,
                )) => {
                    if let Some(Some(min_height)) = ret {
                        ret = Some(Some(std::cmp::min(min_height, flat_head.height)));
                    } else {
                        ret = Some(Some(flat_head.height));
                    }
                }
                FlatStorageStatus::Resharding(_) => return Ok(Some(None)),
                _ => {}
            };
        }
        Ok(ret)
    }

    /// Should be called when we want to take a state snapshot. Disallows flat head updates, and signals to any resharding
    /// flat storage code that it should not advance beyond this hash
    // TODO(#12919): This sets the currently active snapshot request to `block_hash` in favor of any previous requests. We
    // should modify this to be sure that the correct block hash that will end up on the canonical chain is taken. Right now
    // we rely on the canonical one being requested after any other forks, which may not be the case.
    pub fn want_snapshot(&self, block_hash: CryptoHash, min_chunk_prev_height: BlockHeight) {
        {
            let mut want_snapshot = self.0.want_snapshot.lock();
            *want_snapshot = Some(SnapshotBlock { block_hash, min_chunk_prev_height });
        }
        let flat_storages = self.0.flat_storages.lock();
        for flat_storage in flat_storages.values() {
            flat_storage.set_flat_head_update_mode(false);
        }
        tracing::debug!(target: "store", "Locked flat head updates");
    }

    /// Should be called when we're done taking a state snapshot. If `block_hash` was the most recently requested snapshot, this
    /// allows flat head updates, and signals to any resharding flat storage code that it can advance now.
    pub fn snapshot_taken(&self, block_hash: &CryptoHash) {
        {
            let mut want_snapshot = self.0.want_snapshot.lock();
            if let Some(want) = &*want_snapshot {
                if &want.block_hash != block_hash {
                    return;
                }
            } else {
                tracing::warn!(target: "store", %block_hash, "State snapshot being marked as taken without a corresponding pending request set");
            }
            *want_snapshot = None;
        }
        let flat_storages = self.0.flat_storages.lock();
        for flat_storage in flat_storages.values() {
            flat_storage.set_flat_head_update_mode(true);
        }
        tracing::debug!(target: "store", "Unlocked flat head updates");
    }

    // Returns Some() if a state snapshot should be taken, and therefore any resharding flat storage code should not advance
    // past the given hash
    pub fn snapshot_height_wanted(&self) -> Option<BlockHeight> {
        let want_snapshot = self.0.want_snapshot.lock();
        want_snapshot.as_ref().map(|s| s.min_chunk_prev_height)
    }

    // Returns Some() with the corresponding block hash if a state snapshot has been requested
    pub fn snapshot_hash_wanted(&self) -> Option<CryptoHash> {
        let want_snapshot = self.0.want_snapshot.lock();
        want_snapshot.as_ref().map(|s| s.block_hash)
    }
}

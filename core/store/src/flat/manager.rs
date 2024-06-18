use crate::flat::{
    store_helper, BlockInfo, FlatStorageReadyStatus, FlatStorageStatus, POISONED_LOCK_ERR,
};
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::{BlockHeight, RawStateChangesWithTrieKey};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::debug;

use crate::{Store, StoreUpdate};

use super::chunk_view::FlatStorageChunkView;
use super::{
    FlatStateChanges, FlatStateDelta, FlatStateDeltaMetadata, FlatStorage, FlatStorageError,
};

/// `FlatStorageManager` provides a way to construct new flat state to pass to new tries.
/// It is owned by NightshadeRuntime, and thus can be owned by multiple threads, so the implementation
/// must be thread safe.
#[derive(Clone)]
pub struct FlatStorageManager(Arc<FlatStorageManagerInner>);

pub struct FlatStorageManagerInner {
    store: Store,
    /// Here we store the flat_storage per shard. The reason why we don't use the same
    /// FlatStorage for all shards is that there are two modes of block processing,
    /// normal block processing and block catchups. Since these are performed on different range
    /// of blocks, we need flat storage to be able to support different range of blocks
    /// on different shards. So we simply store a different state for each shard.
    /// This may cause some overhead because the data like shards that the node is processing for
    /// this epoch can share the same `head` and `tail`, similar for shards for the next epoch,
    /// but such overhead is negligible comparing the delta sizes, so we think it's ok.
    flat_storages: Mutex<HashMap<ShardUId, FlatStorage>>,
}

impl FlatStorageManager {
    pub fn new(store: Store) -> Self {
        Self(Arc::new(FlatStorageManagerInner { store, flat_storages: Default::default() }))
    }

    /// When a node starts from an empty database, this function must be called to ensure
    /// information such as flat head is set up correctly in the database.
    /// Note that this function is different from `create_flat_storage_for_shard`,
    /// it must be called before `create_flat_storage_for_shard` if the node starts from
    /// an empty database.
    pub fn set_flat_storage_for_genesis(
        &self,
        store_update: &mut StoreUpdate,
        shard_uid: ShardUId,
        genesis_block: &CryptoHash,
        genesis_height: BlockHeight,
    ) {
        let flat_storages = self.0.flat_storages.lock().expect(POISONED_LOCK_ERR);
        assert!(!flat_storages.contains_key(&shard_uid));
        store_helper::set_flat_storage_status(
            store_update,
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
        let mut flat_storages = self.0.flat_storages.lock().expect(POISONED_LOCK_ERR);
        let flat_storage = FlatStorage::new(self.0.store.clone(), shard_uid)?;
        let original_value = flat_storages.insert(shard_uid, flat_storage);
        if original_value.is_some() {
            // Generally speaking this shouldn't happen. It may only happen when
            // the node is restarted in the middle of resharding.
            //
            // TODO(resharding) It would be better to detect when building state
            // is finished for a shard and skip doing it again after restart. We
            // can then assert that the flat storage is only created once.
            tracing::warn!(target: "store", ?shard_uid, "Creating flat storage for shard that already has flat storage.");
        }
        Ok(())
    }

    /// Update flat storage for given processed or caught up block, which includes:
    /// - merge deltas from current flat storage head to new one;
    /// - update flat storage head to the hash of final block visible from given one;
    /// - remove info about unreachable blocks from memory.
    pub fn update_flat_storage_for_shard(
        &self,
        shard_uid: ShardUId,
        new_flat_head: CryptoHash,
    ) -> Result<(), StorageError> {
        if let Some(flat_storage) = self.get_flat_storage_for_shard(shard_uid) {
            // Try to update flat head.
            flat_storage.update_flat_head_impl(&new_flat_head, false).unwrap_or_else(|err| {
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
            tracing::debug!(target: "store", ?shard_uid, "No flat storage!!!");
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
    ) -> Result<StoreUpdate, StorageError> {
        let prev_block_with_changes = if state_changes.is_empty() {
            // The current block has no flat state changes.
            // Find the last block with flat state changes by looking it up in
            // the prev block.
            store_helper::get_prev_block_with_changes(
                &self.0.store,
                shard_uid,
                block_hash,
                prev_hash,
            )
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
            let mut store_update: StoreUpdate = self.0.store.store_update();
            store_helper::set_delta(&mut store_update, shard_uid, &delta);
            store_update
        };

        Ok(store_update)
    }

    pub fn get_flat_storage_status(&self, shard_uid: ShardUId) -> FlatStorageStatus {
        store_helper::get_flat_storage_status(&self.0.store, shard_uid)
            .expect("failed to read flat storage status")
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
            let flat_storages = self.0.flat_storages.lock().expect(POISONED_LOCK_ERR);
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

    pub fn get_shard_uids(&self) -> Vec<ShardUId> {
        let flat_storages = self.0.flat_storages.lock().expect(POISONED_LOCK_ERR);
        flat_storages.keys().cloned().collect()
    }

    // TODO (#7327): consider returning Result<FlatStorage, Error> when we expect flat storage to exist
    pub fn get_flat_storage_for_shard(&self, shard_uid: ShardUId) -> Option<FlatStorage> {
        let flat_storages = self.0.flat_storages.lock().expect(POISONED_LOCK_ERR);
        flat_storages.get(&shard_uid).cloned()
    }

    /// Removes FlatStorage object from FlatStorageManager.
    /// If FlatStorageManager did have that object, then removes all information about Flat State and returns Ok(true).
    /// Otherwise does nothing and returns Ok(false).
    pub fn remove_flat_storage_for_shard(
        &self,
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) -> Result<bool, StorageError> {
        let mut flat_storages = self.0.flat_storages.lock().expect(POISONED_LOCK_ERR);
        if let Some(flat_store) = flat_storages.remove(&shard_uid) {
            flat_store.clear_state(store_update)?;
            tracing::info!(target: "store", ?shard_uid, "remove_flat_storage_for_shard successful");
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Updates `move_head_enabled` for all shards and returns whether it succeeded.
    /// If at least one of the shards fails to update move_head_enabled, then that operation is rolled back for all shards.
    ///
    /// Rollbacks should work, because we assume that this function is the only
    /// entry point to locking/unlocking flat head updates in a system with
    /// multiple FlatStorages running in parallel.
    pub fn set_flat_state_updates_mode(&self, enabled: bool) -> bool {
        let flat_storages = self.0.flat_storages.lock().expect(POISONED_LOCK_ERR);
        let mut all_updated = true;
        let mut updated_flat_storages = vec![];
        let mut updated_shard_uids = vec![];
        for (shard_uid, flat_storage) in flat_storages.iter() {
            if flat_storage.set_flat_head_update_mode(enabled) {
                updated_flat_storages.push(flat_storage);
                updated_shard_uids.push(shard_uid);
            } else {
                all_updated = false;
                tracing::error!(target: "store", rolling_back_shards = ?updated_shard_uids, enabled, ?shard_uid, "Locking/Unlocking of flat head updates failed for shard. Reverting.");
                break;
            }
        }
        if all_updated {
            tracing::debug!(target: "store", enabled, "Locking/Unlocking of flat head updates succeeded");
            true
        } else {
            // Do rollback.
            // It does allow for a data race if somebody updates move_head_enabled on individual shards.
            // The assumption is that all shards get locked/unlocked at the same time.
            for flat_storage in updated_flat_storages {
                flat_storage.set_flat_head_update_mode(!enabled);
            }
            tracing::error!(target: "store", enabled, "Locking/Unlocking of flat head updates failed");
            false
        }
    }
}

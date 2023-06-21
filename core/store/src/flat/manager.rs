use crate::flat::{
    store_helper, BlockInfo, FlatStorageReadyStatus, FlatStorageStatus, POISONED_LOCK_ERR,
};
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::BlockHeight;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::debug;

use crate::{Store, StoreUpdate};

use super::chunk_view::FlatStorageChunkView;
use super::FlatStorage;

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

    pub fn test(store: Store, shard_uids: &[ShardUId], flat_head: CryptoHash) -> Self {
        let mut flat_storages = HashMap::default();
        for shard_uid in shard_uids {
            let mut store_update = store.store_update();
            store_helper::set_flat_storage_status(
                &mut store_update,
                *shard_uid,
                FlatStorageStatus::Ready(FlatStorageReadyStatus {
                    flat_head: BlockInfo::genesis(flat_head, 0),
                }),
            );
            store_update.commit().expect("failed to set flat storage status");
            flat_storages.insert(*shard_uid, FlatStorage::new(store.clone(), *shard_uid).unwrap());
        }
        Self(Arc::new(FlatStorageManagerInner { store, flat_storages: Mutex::new(flat_storages) }))
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

    /// Creates flat storage instance for shard `shard_id`. The function also checks that
    /// the shard's flat storage state hasn't been set before, otherwise it panics.
    /// TODO (#7327): this behavior may change when we implement support for state sync
    /// and resharding.
    pub fn create_flat_storage_for_shard(&self, shard_uid: ShardUId) -> Result<(), StorageError> {
        let mut flat_storages = self.0.flat_storages.lock().expect(POISONED_LOCK_ERR);
        let original_value =
            flat_storages.insert(shard_uid, FlatStorage::new(self.0.store.clone(), shard_uid)?);
        // TODO (#7327): maybe we should propagate the error instead of assert here
        // assert is fine now because this function is only called at construction time, but we
        // will need to be more careful when we want to implement flat storage for resharding
        assert!(original_value.is_none());
        Ok(())
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

    // TODO (#7327): consider returning Result<FlatStorage, Error> when we expect flat storage to exist
    pub fn get_flat_storage_for_shard(&self, shard_uid: ShardUId) -> Option<FlatStorage> {
        let flat_storages = self.0.flat_storages.lock().expect(POISONED_LOCK_ERR);
        flat_storages.get(&shard_uid).cloned()
    }

    pub fn remove_flat_storage_for_shard(&self, shard_uid: ShardUId) -> Result<(), StorageError> {
        let mut flat_storages = self.0.flat_storages.lock().expect(POISONED_LOCK_ERR);

        if let Some(flat_store) = flat_storages.remove(&shard_uid) {
            flat_store.clear_state()?;
        }

        Ok(())
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

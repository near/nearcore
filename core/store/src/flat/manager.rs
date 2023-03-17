use crate::flat::{
    store_helper, BlockInfo, FlatStorageReadyStatus, FlatStorageStatus, POISONED_LOCK_ERR,
};
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
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

    /// When a node starts from an empty database, this function must be called to ensure
    /// information such as flat head is set up correctly in the database.
    /// Note that this function is different from `add_flat_storage_for_shard`,
    /// it must be called before `add_flat_storage_for_shard` if the node starts from
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

    /// Add a flat storage state for shard `shard_id`. The function also checks that
    /// the shard's flat storage state hasn't been set before, otherwise it panics.
    /// TODO (#7327): this behavior may change when we implement support for state sync
    /// and resharding.
    pub fn add_flat_storage_for_shard(&self, shard_uid: ShardUId, flat_storage: FlatStorage) {
        let mut flat_storages = self.0.flat_storages.lock().expect(POISONED_LOCK_ERR);
        let original_value = flat_storages.insert(shard_uid, flat_storage);
        // TODO (#7327): maybe we should propagate the error instead of assert here
        // assert is fine now because this function is only called at construction time, but we
        // will need to be more careful when we want to implement flat storage for resharding
        assert!(original_value.is_none());
    }

    /// Creates `FlatStorageChunkView` to access state for `shard_id` and block `block_hash`. Note that
    /// the state includes changes by the block `block_hash`.
    /// `block_hash`: only create FlatStorageChunkView if it is not None. This is a hack we have temporarily
    ///               to not introduce too many changes in the trie interface.
    /// `is_view`: whether this flat state is used for view client. We use a separate set of caches
    ///            for flat state for client vs view client. For now, we don't support flat state
    ///            for view client, so we simply return None if `is_view` is True.
    /// TODO (#7327): take block_hash as CryptoHash instead of Option<CryptoHash>
    /// TODO (#7327): implement support for view_client
    pub fn chunk_view(
        &self,
        shard_uid: ShardUId,
        block_hash: Option<CryptoHash>,
        is_view: bool,
    ) -> Option<FlatStorageChunkView> {
        let block_hash = match block_hash {
            Some(block_hash) => block_hash,
            None => {
                return None;
            }
        };

        if is_view {
            // TODO (#7327): Technically, like TrieCache, we should have a separate set of caches for Client and
            // ViewClient. Right now, we can get by by not enabling flat state for view trie
            None
        } else {
            let flat_storage = {
                let flat_storages = self.0.flat_storages.lock().expect(POISONED_LOCK_ERR);
                // It is possible that flat storage state does not exist yet because it is being created in
                // background.
                match flat_storages.get(&shard_uid) {
                    Some(flat_storage) => flat_storage.clone(),
                    None => {
                        debug!(target: "chain", "FlatStorage is not ready");
                        return None;
                    }
                }
            };
            Some(FlatStorageChunkView::new(self.0.store.clone(), block_hash, flat_storage))
        }
    }

    // TODO (#7327): change the function signature to Result<FlatStorage, Error> when
    // we stabilize feature protocol_feature_flat_state. We use option now to return None when
    // the feature is not enabled. Ideally, it should return an error because it is problematic
    // if the flat storage state does not exist
    pub fn get_flat_storage_for_shard(&self, shard_uid: ShardUId) -> Option<FlatStorage> {
        let flat_storages = self.0.flat_storages.lock().expect(POISONED_LOCK_ERR);
        flat_storages.get(&shard_uid).cloned()
    }

    pub fn remove_flat_storage_for_shard(
        &self,
        shard_uid: ShardUId,
        shard_layout: ShardLayout,
    ) -> Result<(), StorageError> {
        let mut flat_storages = self.0.flat_storages.lock().expect(POISONED_LOCK_ERR);

        match flat_storages.remove(&shard_uid) {
            None => {}
            Some(flat_storage) => {
                flat_storage.clear_state(shard_layout)?;
            }
        }

        Ok(())
    }
}

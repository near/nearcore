use crate::archive::cloud_archival_utils::CloudArchivalReaderError;
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::state_part::StatePartId;
use near_primitives::state_sync::ShardStateSyncResponseHeader;
use near_primitives::types::{EpochHeight, EpochId, RawStateChangesWithTrieKey};
use near_store::adapter::StoreAdapter;
use near_store::adapter::trie_store::{TrieStoreAdapter, TrieStoreUpdateAdapter};
use near_store::archive::cloud_storage::CloudStorage;
use near_store::flat::FlatStorageManager;
use near_store::trie::AccessOptions;
use near_store::{
    ShardTries, ShardUId, StateSnapshotConfig, Store, Trie, TrieChanges, TrieConfig, TrieDBStorage,
    TrieStorage,
};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

/// The tries a reader applies recorded state changes into.
pub fn build_shard_tries(store: &Store) -> ShardTries {
    ShardTries::new(
        store.trie_store(),
        TrieConfig::default(),
        FlatStorageManager::new(store.flat_store()),
        StateSnapshotConfig::Disabled,
    )
}

/// Downloads one shard's state snapshot and writes its parts into the trie, leaving the
/// root the header names reachable.
pub(crate) async fn install_state_snapshot(
    cloud_storage: &CloudStorage,
    tries: &ShardTries,
    shard_uid: ShardUId,
    epoch_height: EpochHeight,
    epoch_id: EpochId,
    header: &ShardStateSyncResponseHeader,
) -> Result<(), CloudArchivalReaderError> {
    let shard_id = shard_uid.shard_id();
    let state_root = header.chunk_prev_state_root();
    let num_parts = header.num_state_parts();
    for part_index in 0..num_parts {
        let part_id = StatePartId::new(part_index, num_parts);
        let state_part =
            cloud_storage.retrieve_state_part(epoch_height, epoch_id, shard_id, part_id).await?;
        let partial_state = state_part.to_partial_state().map_err(|error| {
            CloudArchivalReaderError::StatePartDeserialization { shard_uid, part_id, error }
        })?;
        let apply_result = Trie::apply_state_part(&state_root, part_id, partial_state);
        let mut store_update = tries.store_update();
        tries.apply_all(&apply_result.trie_changes, shard_uid, &mut store_update);
        store_update.commit();
    }
    tracing::info!(target: "cloud_archival", %shard_uid, num_parts, "installed a state snapshot");
    Ok(())
}

/// Trie storage for one batch: the nodes that batch has applied, over the nodes the store
/// already holds. A height reads what the height below it wrote, and a store read does
/// not see an uncommitted store update.
struct BatchTrieStorage {
    /// Behind a lock because `retrieve_raw_bytes` takes `&self` and the trie holds this
    /// storage as `Arc<dyn TrieStorage>`, which is `Sync`.
    uncommitted: Mutex<HashMap<CryptoHash, Arc<[u8]>>>,
    committed: TrieDBStorage,
}

impl BatchTrieStorage {
    fn new(store: TrieStoreAdapter, shard_uid: ShardUId) -> Self {
        // TODO(cloud_archival): serve committed nodes through the shard cache, which needs
        // an accessor for the one `ShardTries` holds.
        let committed = TrieDBStorage::new(store, shard_uid);
        // TODO(cloud_archival): consider serving the batch's own nodes out of the pending
        // transaction, so an inserted node is held once.
        Self { uncommitted: Mutex::new(HashMap::new()), committed }
    }

    /// Serves the nodes `trie_changes` inserted to every read that follows.
    fn insert(&self, trie_changes: &TrieChanges) {
        let mut uncommitted = self.uncommitted.lock();
        for insertion in trie_changes.insertions() {
            uncommitted.insert(*insertion.hash(), insertion.payload().into());
        }
    }
}

impl TrieStorage for BatchTrieStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
        if let Some(bytes) = self.uncommitted.lock().get(hash) {
            return Ok(bytes.clone());
        }
        self.committed.retrieve_raw_bytes(hash)
    }
}

/// One shard batch's trie writes: each `apply` moves the shard's state one height, and
/// `commit` writes every height together.
///
/// An update dropped without `commit` writes nothing, so a batch that fails part way
/// leaves the trie where it started.
pub(crate) struct BatchTrieUpdate {
    tries: ShardTries,
    shard_uid: ShardUId,
    store_update: TrieStoreUpdateAdapter<'static>,
    storage: Arc<BatchTrieStorage>,
    /// The root the heights applied so far have left.
    state_root: CryptoHash,
}

impl BatchTrieUpdate {
    /// Opens a batch that applies onto `state_root`.
    pub(crate) fn new(tries: &ShardTries, shard_uid: ShardUId, state_root: CryptoHash) -> Self {
        let storage = Arc::new(BatchTrieStorage::new(tries.store(), shard_uid));
        Self {
            tries: tries.clone(),
            shard_uid,
            store_update: tries.store_update(),
            storage,
            state_root,
        }
    }

    /// Applies one height's recorded state changes and returns the root they leave.
    pub(crate) fn apply(
        &mut self,
        state_changes: &[RawStateChangesWithTrieKey],
    ) -> Result<CryptoHash, CloudArchivalReaderError> {
        let entries = state_changes.iter().map(|change| {
            // A key can change more than once within a block, and the last value is the
            // one the block leaves behind.
            let data = change.changes.last().expect("a recorded key has a change").data.clone();
            (change.trie_key.to_vec(), data)
        });
        let trie = Trie::new(self.storage.clone(), self.state_root, None);
        let trie_changes = trie.update(entries, AccessOptions::NO_SIDE_EFFECTS)?;
        // Insertions alone, so every root the walk passes stays reachable for a query at
        // that height.
        self.tries.apply_insertions(&trie_changes, self.shard_uid, &mut self.store_update);
        self.storage.insert(&trie_changes);
        self.state_root = trie_changes.new_root;
        Ok(self.state_root)
    }

    /// Commits the batch.
    pub(crate) fn commit(self) {
        self.store_update.commit();
    }
}

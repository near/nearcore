use crate::flat::{FlatStorageManager, FlatStorageStatus};
use crate::trie::config::TrieConfig;
use crate::trie::prefetching_trie_storage::PrefetchingThreadsHandle;
use crate::trie::trie_storage::{TrieCache, TrieCachingStorage};
use crate::trie::{TrieRefcountChange, POISONED_LOCK_ERR};
use crate::{metrics, DBCol, PrefetchApi};
use crate::{Store, StoreUpdate, Trie, TrieChanges, TrieUpdate};

use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{self, ShardUId, ShardVersion};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{
    NumShards, RawStateChange, RawStateChangesWithTrieKey, StateChangeCause, StateRoot,
};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

use super::state_snapshot::{StateSnapshot, StateSnapshotConfig};

struct ShardTriesInner {
    store: Store,
    trie_config: TrieConfig,
    /// Cache reserved for client actor to use
    caches: RwLock<HashMap<ShardUId, TrieCache>>,
    /// Cache for readers.
    view_caches: RwLock<HashMap<ShardUId, TrieCache>>,
    flat_storage_manager: FlatStorageManager,
    /// Prefetcher state, such as IO threads, per shard.
    prefetchers: RwLock<HashMap<ShardUId, (PrefetchApi, PrefetchingThreadsHandle)>>,
    /// Provides access to the snapshot of the DB at the beginning of an epoch.
    // Needs a synchronization primitive because it can be concurrently accessed:
    // * writes by StateSnapshotActor
    // * reads by ViewClientActor
    state_snapshot: Arc<RwLock<Option<StateSnapshot>>>,
    /// Configures how to make state snapshots.
    state_snapshot_config: StateSnapshotConfig,
}

#[derive(Clone)]
pub struct ShardTries(Arc<ShardTriesInner>);

impl ShardTries {
    pub fn new_with_state_snapshot(
        store: Store,
        trie_config: TrieConfig,
        shard_uids: &[ShardUId],
        flat_storage_manager: FlatStorageManager,
        state_snapshot_config: StateSnapshotConfig,
    ) -> Self {
        let caches = Self::create_initial_caches(&trie_config, &shard_uids, false);
        let view_caches = Self::create_initial_caches(&trie_config, &shard_uids, true);
        metrics::HAS_STATE_SNAPSHOT.set(0);
        ShardTries(Arc::new(ShardTriesInner {
            store,
            trie_config,
            caches: RwLock::new(caches),
            view_caches: RwLock::new(view_caches),
            flat_storage_manager,
            prefetchers: Default::default(),
            state_snapshot: Arc::new(RwLock::new(None)),
            state_snapshot_config,
        }))
    }

    pub fn new(
        store: Store,
        trie_config: TrieConfig,
        shard_uids: &[ShardUId],
        flat_storage_manager: FlatStorageManager,
    ) -> Self {
        Self::new_with_state_snapshot(
            store,
            trie_config,
            shard_uids,
            flat_storage_manager,
            StateSnapshotConfig::Disabled,
        )
    }

    /// Create `ShardTries` with a fixed number of shards with shard version 0.
    ///
    /// If your test cares about the shard version, use `test_shard_version` instead.
    pub fn test(store: Store, num_shards: NumShards) -> Self {
        let shard_version = 0;
        Self::test_shard_version(store, shard_version, num_shards)
    }

    pub fn test_shard_version(store: Store, version: ShardVersion, num_shards: NumShards) -> Self {
        assert_ne!(0, num_shards);
        let shard_uids: Vec<ShardUId> =
            (0..num_shards as u32).map(|shard_id| ShardUId { shard_id, version }).collect();
        let trie_config = TrieConfig::default();

        ShardTries::new(store.clone(), trie_config, &shard_uids, FlatStorageManager::new(store))
    }

    /// Create caches for all shards according to the trie config.
    fn create_initial_caches(
        config: &TrieConfig,
        shard_uids: &[ShardUId],
        is_view: bool,
    ) -> HashMap<ShardUId, TrieCache> {
        shard_uids
            .iter()
            .map(|&shard_uid| (shard_uid, TrieCache::new(config, shard_uid, is_view)))
            .collect()
    }

    pub fn new_trie_update(&self, shard_uid: ShardUId, state_root: StateRoot) -> TrieUpdate {
        TrieUpdate::new(self.get_trie_for_shard(shard_uid, state_root))
    }

    pub fn new_trie_update_view(&self, shard_uid: ShardUId, state_root: StateRoot) -> TrieUpdate {
        TrieUpdate::new(self.get_view_trie_for_shard(shard_uid, state_root))
    }

    fn get_trie_for_shard_internal(
        &self,
        shard_uid: ShardUId,
        state_root: StateRoot,
        is_view: bool,
        block_hash: Option<CryptoHash>,
    ) -> Trie {
        let caches_to_use = if is_view { &self.0.view_caches } else { &self.0.caches };
        let cache = {
            let mut caches = caches_to_use.write().expect(POISONED_LOCK_ERR);
            caches
                .entry(shard_uid)
                .or_insert_with(|| TrieCache::new(&self.0.trie_config, shard_uid, is_view))
                .clone()
        };
        // Do not enable prefetching on view caches.
        // 1) Performance of view calls is not crucial.
        // 2) A lot of the prefetcher code assumes there is only one "main-thread" per shard active.
        //    If you want to enable it for view calls, at least make sure they don't share
        //    the `PrefetchApi` instances with the normal calls.
        let prefetch_enabled = !is_view
            && (self.0.trie_config.enable_receipt_prefetching
                || (!self.0.trie_config.sweat_prefetch_receivers.is_empty()
                    && !self.0.trie_config.sweat_prefetch_senders.is_empty()));
        let prefetch_api = prefetch_enabled.then(|| {
            self.0
                .prefetchers
                .write()
                .expect(POISONED_LOCK_ERR)
                .entry(shard_uid)
                .or_insert_with(|| {
                    PrefetchApi::new(
                        self.0.store.clone(),
                        cache.clone(),
                        shard_uid,
                        &self.0.trie_config,
                    )
                })
                .0
                .clone()
        });

        let storage = Rc::new(TrieCachingStorage::new(
            self.0.store.clone(),
            cache,
            shard_uid,
            is_view,
            prefetch_api,
        ));
        let flat_storage_chunk_view = block_hash
            .and_then(|block_hash| self.0.flat_storage_manager.chunk_view(shard_uid, block_hash));

        Trie::new(storage, state_root, flat_storage_chunk_view)
    }

    pub fn get_trie_for_shard(&self, shard_uid: ShardUId, state_root: StateRoot) -> Trie {
        self.get_trie_for_shard_internal(shard_uid, state_root, false, None)
    }

    pub fn get_trie_with_block_hash_for_shard_from_snapshot(
        &self,
        shard_uid: ShardUId,
        state_root: StateRoot,
        block_hash: &CryptoHash,
    ) -> Result<Trie, StorageError> {
        let (store, flat_storage_manager) = self.get_state_snapshot(block_hash)?;
        let cache = {
            let mut caches = self.0.view_caches.write().expect(POISONED_LOCK_ERR);
            caches
                .entry(shard_uid)
                .or_insert_with(|| TrieCache::new(&self.0.trie_config, shard_uid, true))
                .clone()
        };
        let storage = Rc::new(TrieCachingStorage::new(store, cache, shard_uid, true, None));
        let flat_storage_chunk_view = flat_storage_manager.chunk_view(shard_uid, *block_hash);

        Ok(Trie::new(storage, state_root, flat_storage_chunk_view))
    }

    pub fn get_trie_with_block_hash_for_shard(
        &self,
        shard_uid: ShardUId,
        state_root: StateRoot,
        block_hash: &CryptoHash,
        is_view: bool,
    ) -> Trie {
        self.get_trie_for_shard_internal(shard_uid, state_root, is_view, Some(*block_hash))
    }

    pub fn get_view_trie_for_shard(&self, shard_uid: ShardUId, state_root: StateRoot) -> Trie {
        self.get_trie_for_shard_internal(shard_uid, state_root, true, None)
    }

    pub fn store_update(&self) -> StoreUpdate {
        StoreUpdate::new(self.get_db().clone())
    }

    pub fn get_store(&self) -> Store {
        self.0.store.clone()
    }

    pub(crate) fn get_db(&self) -> &Arc<dyn crate::Database> {
        &self.0.store.storage
    }

    pub fn get_flat_storage_manager(&self) -> FlatStorageManager {
        self.0.flat_storage_manager.clone()
    }

    pub(crate) fn state_snapshot_config(&self) -> &StateSnapshotConfig {
        &self.0.state_snapshot_config
    }

    pub(crate) fn state_snapshot(&self) -> &Arc<RwLock<Option<StateSnapshot>>> {
        &self.0.state_snapshot
    }

    pub fn update_cache(&self, ops: Vec<(&CryptoHash, Option<&[u8]>)>, shard_uid: ShardUId) {
        let mut caches = self.0.caches.write().expect(POISONED_LOCK_ERR);
        let cache = caches
            .entry(shard_uid)
            .or_insert_with(|| TrieCache::new(&self.0.trie_config, shard_uid, false))
            .clone();
        cache.update_cache(ops);
    }

    fn apply_deletions_inner(
        &self,
        deletions: &[TrieRefcountChange],
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) {
        let mut ops = Vec::new();
        for TrieRefcountChange { trie_node_or_value_hash, rc, .. } in deletions.iter() {
            let key = TrieCachingStorage::get_key_from_shard_uid_and_hash(
                shard_uid,
                trie_node_or_value_hash,
            );
            store_update.decrement_refcount_by(DBCol::State, key.as_ref(), *rc);
            ops.push((trie_node_or_value_hash, None));
        }

        self.update_cache(ops, shard_uid);
    }

    fn apply_insertions_inner(
        &self,
        insertions: &[TrieRefcountChange],
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) {
        let mut ops = Vec::new();
        for TrieRefcountChange { trie_node_or_value_hash, trie_node_or_value, rc } in
            insertions.iter()
        {
            let key = TrieCachingStorage::get_key_from_shard_uid_and_hash(
                shard_uid,
                trie_node_or_value_hash,
            );
            store_update.increment_refcount_by(DBCol::State, key.as_ref(), trie_node_or_value, *rc);
            ops.push((trie_node_or_value_hash, Some(trie_node_or_value.as_slice())));
        }
        self.update_cache(ops, shard_uid);
    }

    fn apply_all_inner(
        &self,
        trie_changes: &TrieChanges,
        shard_uid: ShardUId,
        apply_deletions: bool,
        store_update: &mut StoreUpdate,
    ) -> StateRoot {
        self.apply_insertions_inner(&trie_changes.insertions, shard_uid, store_update);
        if apply_deletions {
            self.apply_deletions_inner(&trie_changes.deletions, shard_uid, store_update);
        }
        trie_changes.new_root
    }

    pub fn apply_insertions(
        &self,
        trie_changes: &TrieChanges,
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) {
        // `itoa` is much faster for printing shard_id to a string than trivial alternatives.
        let mut buffer = itoa::Buffer::new();
        let shard_id = buffer.format(shard_uid.shard_id);

        metrics::APPLIED_TRIE_INSERTIONS
            .with_label_values(&[&shard_id])
            .inc_by(trie_changes.insertions.len() as u64);
        self.apply_insertions_inner(&trie_changes.insertions, shard_uid, store_update)
    }

    pub fn apply_deletions(
        &self,
        trie_changes: &TrieChanges,
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) {
        // `itoa` is much faster for printing shard_id to a string than trivial alternatives.
        let mut buffer = itoa::Buffer::new();
        let shard_id = buffer.format(shard_uid.shard_id);

        metrics::APPLIED_TRIE_DELETIONS
            .with_label_values(&[&shard_id])
            .inc_by(trie_changes.deletions.len() as u64);
        self.apply_deletions_inner(&trie_changes.deletions, shard_uid, store_update)
    }

    pub fn revert_insertions(
        &self,
        trie_changes: &TrieChanges,
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) {
        // `itoa` is much faster for printing shard_id to a string than trivial alternatives.
        let mut buffer = itoa::Buffer::new();
        let shard_id = buffer.format(shard_uid.shard_id);

        metrics::REVERTED_TRIE_INSERTIONS
            .with_label_values(&[&shard_id])
            .inc_by(trie_changes.insertions.len() as u64);
        self.apply_deletions_inner(&trie_changes.insertions, shard_uid, store_update)
    }

    pub fn apply_all(
        &self,
        trie_changes: &TrieChanges,
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) -> StateRoot {
        self.apply_all_inner(trie_changes, shard_uid, true, store_update)
    }

    /// Returns the status of the given shard of flat storage in the state snapshot.
    /// `sync_prev_prev_hash` needs to match the block hash that identifies that snapshot.
    pub fn get_snapshot_flat_storage_status(
        &self,
        sync_prev_prev_hash: CryptoHash,
        shard_uid: ShardUId,
    ) -> Result<FlatStorageStatus, StorageError> {
        let (_store, manager) = self.get_state_snapshot(&sync_prev_prev_hash)?;
        Ok(manager.get_flat_storage_status(shard_uid))
    }
}

pub struct WrappedTrieChanges {
    tries: ShardTries,
    shard_uid: ShardUId,
    trie_changes: TrieChanges,
    state_changes: Vec<RawStateChangesWithTrieKey>,
    block_hash: CryptoHash,
}

// Partial implementation. Skips `tries` due to its complexity and
// `trie_changes` and `state_changes` due to their large volume.
impl std::fmt::Debug for WrappedTrieChanges {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WrappedTrieChanges")
            .field("tries", &"<not shown>")
            .field("shard_uid", &self.shard_uid)
            .field("trie_changes", &"<not shown>")
            .field("state_changes", &"<not shown>")
            .field("block_hash", &self.block_hash)
            .finish()
    }
}

impl WrappedTrieChanges {
    pub fn new(
        tries: ShardTries,
        shard_uid: ShardUId,
        trie_changes: TrieChanges,
        state_changes: Vec<RawStateChangesWithTrieKey>,
        block_hash: CryptoHash,
    ) -> Self {
        WrappedTrieChanges { tries, shard_uid, trie_changes, state_changes, block_hash }
    }

    pub fn state_changes(&self) -> &[RawStateChangesWithTrieKey] {
        &self.state_changes
    }

    /// Save insertions of trie nodes into Store.
    pub fn insertions_into(&self, store_update: &mut StoreUpdate) {
        self.tries.apply_insertions(&self.trie_changes, self.shard_uid, store_update)
    }

    /// Save deletions of trie nodes into Store.
    pub fn deletions_into(&self, store_update: &mut StoreUpdate) {
        self.tries.apply_deletions(&self.trie_changes, self.shard_uid, store_update)
    }

    /// Save state changes into Store.
    ///
    /// NOTE: the changes are drained from `self`.
    pub fn state_changes_into(&mut self, store_update: &mut StoreUpdate) {
        for mut change_with_trie_key in self.state_changes.drain(..) {
            assert!(
                !change_with_trie_key.changes.iter().any(|RawStateChange { cause, .. }| matches!(
                    cause,
                    StateChangeCause::NotWritableToDisk
                )),
                "NotWritableToDisk changes must never be finalized."
            );

            // Resharding changes must not be finalized, however they may be introduced here when we are
            // evaluating changes for split state in process_split_state function
            change_with_trie_key
                .changes
                .retain(|change| change.cause != StateChangeCause::Resharding);
            if change_with_trie_key.changes.is_empty() {
                continue;
            }

            let storage_key = if cfg!(feature = "serialize_all_state_changes") {
                // Serialize all kinds of state changes without any filtering.
                // Without this it's not possible to replay state changes to get an identical state root.

                // This branch will become the default in the near future.

                match change_with_trie_key.trie_key.get_account_id() {
                    // If a TrieKey itself doesn't identify the Shard, then we need to add shard id to the row key.
                    None => KeyForStateChanges::delayed_receipt_key_from_trie_key(
                        &self.block_hash,
                        &change_with_trie_key.trie_key,
                        &self.shard_uid,
                    ),
                    // TrieKey has enough information to identify the shard it comes from.
                    _ => KeyForStateChanges::from_trie_key(
                        &self.block_hash,
                        &change_with_trie_key.trie_key,
                    ),
                }
            } else {
                // This branch is the current neard behavior.
                // Only a subset of state changes get serialized.

                // Filtering trie keys for user facing RPC reporting.
                // NOTE: If the trie key is not one of the account specific, it may cause key conflict
                // when the node tracks multiple shards. See #2563.
                match &change_with_trie_key.trie_key {
                    TrieKey::Account { .. }
                    | TrieKey::ContractCode { .. }
                    | TrieKey::AccessKey { .. }
                    | TrieKey::ContractData { .. } => {}
                    _ => continue,
                };
                KeyForStateChanges::from_trie_key(&self.block_hash, &change_with_trie_key.trie_key)
            };

            store_update.set(
                DBCol::StateChanges,
                storage_key.as_ref(),
                &borsh::to_vec(&change_with_trie_key).expect("Borsh serialize cannot fail"),
            );
        }
    }

    pub fn trie_changes_into(&mut self, store_update: &mut StoreUpdate) -> std::io::Result<()> {
        store_update.set_ser(
            DBCol::TrieChanges,
            &shard_layout::get_block_shard_uid(&self.block_hash, &self.shard_uid),
            &self.trie_changes,
        )
    }
}

#[derive(thiserror::Error, Debug)]
pub enum KeyForStateChangesError {
    #[error("Row key of StateChange of kind DelayedReceipt or DelayedReceiptIndices doesn't contain ShardUId: row_key: {0:?} ; trie_key: {1:?}")]
    DelayedReceiptRowKeyError(Vec<u8>, TrieKey),
}

#[derive(derive_more::AsRef, derive_more::Into)]
pub struct KeyForStateChanges(Vec<u8>);

impl KeyForStateChanges {
    const PREFIX_LEN: usize = CryptoHash::LENGTH;

    fn new(block_hash: &CryptoHash, reserve_capacity: usize) -> Self {
        let mut key_prefix = Vec::with_capacity(block_hash.as_bytes().len() + reserve_capacity);
        key_prefix.extend(block_hash.as_bytes());
        Self(key_prefix)
    }

    pub fn for_block(block_hash: &CryptoHash) -> Self {
        Self::new(block_hash, 0)
    }

    pub fn from_raw_key(block_hash: &CryptoHash, raw_key: &[u8]) -> Self {
        let mut key = Self::new(block_hash, raw_key.len());
        key.0.extend(raw_key);
        key
    }

    pub fn from_trie_key(block_hash: &CryptoHash, trie_key: &TrieKey) -> Self {
        let mut key = Self::new(block_hash, trie_key.len());
        trie_key.append_into(&mut key.0);
        key
    }

    /// Without changing the existing TrieKey format, encodes ShardUId into the row key.
    /// See `delayed_receipt_key_from_trie_key` for decoding this row key.
    pub fn delayed_receipt_key_from_trie_key(
        block_hash: &CryptoHash,
        trie_key: &TrieKey,
        shard_uid: &ShardUId,
    ) -> Self {
        let mut key = Self::new(block_hash, trie_key.len() + std::mem::size_of::<ShardUId>());
        trie_key.append_into(&mut key.0);
        key.0.extend(shard_uid.to_bytes());
        key
    }

    /// Extracts ShardUId from row key which contains ShardUId encoded.
    /// See `delayed_receipt_key_from_trie_key` for encoding ShardUId into the row key.
    pub fn delayed_receipt_key_decode_shard_uid(
        row_key: &[u8],
        block_hash: &CryptoHash,
        trie_key: &TrieKey,
    ) -> Result<ShardUId, KeyForStateChangesError> {
        let prefix = KeyForStateChanges::from_trie_key(block_hash, trie_key);
        let prefix = prefix.as_ref();

        let suffix = &row_key[prefix.len()..];
        let shard_uid = ShardUId::try_from(suffix).map_err(|_err| {
            KeyForStateChangesError::DelayedReceiptRowKeyError(row_key.to_vec(), trie_key.clone())
        })?;
        Ok(shard_uid)
    }

    /// Iterates over deserialized row values where row key matches `self`.
    pub fn find_iter<'a>(
        &'a self,
        store: &'a Store,
    ) -> impl Iterator<Item = Result<RawStateChangesWithTrieKey, std::io::Error>> + 'a {
        // Split off the irrelevant part of the key, so only the original trie_key is left.
        self.find_rows_iter(store).map(|row| row.map(|kv| kv.1))
    }

    /// Iterates over deserialized row values where the row key matches `self` exactly.
    pub fn find_exact_iter<'a>(
        &'a self,
        store: &'a Store,
    ) -> impl Iterator<Item = std::io::Result<RawStateChangesWithTrieKey>> + 'a {
        let trie_key_len = self.0.len() - Self::PREFIX_LEN;
        self.find_iter(store).filter_map(move |result| match result {
            Ok(changes) if changes.trie_key.len() == trie_key_len => {
                debug_assert_eq!(changes.trie_key.to_vec(), &self.0[Self::PREFIX_LEN..]);
                Some(Ok(changes))
            }
            Ok(_) => None,
            Err(err) => Some(Err(err)),
        })
    }

    /// Iterates over pairs of `(row key, deserialized row value)` where row key matches `self`.
    pub fn find_rows_iter<'a>(
        &'a self,
        store: &'a Store,
    ) -> impl Iterator<Item = Result<(Box<[u8]>, RawStateChangesWithTrieKey), std::io::Error>> + 'a
    {
        debug_assert!(
            self.0.len() >= Self::PREFIX_LEN,
            "Key length: {}, prefix length: {}, key: {:?}",
            self.0.len(),
            Self::PREFIX_LEN,
            self.0
        );
        store.iter_prefix_ser::<RawStateChangesWithTrieKey>(DBCol::StateChanges, &self.0).map(
            move |change| {
                // Split off the irrelevant part of the key, so only the original trie_key is left.
                let (key, state_changes) = change?;
                debug_assert!(key.starts_with(&self.0), "Key: {:?}, row key: {:?}", self.0, key);
                Ok((key, state_changes))
            },
        )
    }
}

#[cfg(test)]
mod test {
    use crate::{
        config::TrieCacheConfig, test_utils::create_test_store,
        trie::DEFAULT_SHARD_CACHE_TOTAL_SIZE_LIMIT, TrieConfig,
    };

    use super::*;
    use std::{assert_eq, str::FromStr};

    #[test]
    fn test_delayed_receipt_row_key() {
        let trie_key1 = TrieKey::DelayedReceipt { index: 1 };
        let trie_key2 = TrieKey::DelayedReceiptIndices {};
        let shard_uid = ShardUId { version: 10, shard_id: 5 };

        // Random value.
        let block_hash =
            CryptoHash::from_str("32222222222233333333334444444444445555555777").unwrap();

        for trie_key in [trie_key1.clone(), trie_key2] {
            let row_key = KeyForStateChanges::delayed_receipt_key_from_trie_key(
                &block_hash,
                &trie_key,
                &shard_uid,
            );

            let got_shard_uid = KeyForStateChanges::delayed_receipt_key_decode_shard_uid(
                row_key.as_ref(),
                &block_hash,
                &trie_key,
            )
            .unwrap();
            assert_eq!(shard_uid, got_shard_uid);
        }

        // Forget to add a ShardUId to the key, fail to extra a ShardUId from the key.
        let row_key_without_shard_uid = KeyForStateChanges::from_trie_key(&block_hash, &trie_key1);
        assert!(KeyForStateChanges::delayed_receipt_key_decode_shard_uid(
            row_key_without_shard_uid.as_ref(),
            &block_hash,
            &trie_key1
        )
        .is_err());

        // Add an extra byte to the key, fail to extra a ShardUId from the key.
        let mut row_key_extra_bytes = KeyForStateChanges::delayed_receipt_key_from_trie_key(
            &block_hash,
            &trie_key1,
            &shard_uid,
        );
        row_key_extra_bytes.0.extend([8u8]);
        assert!(KeyForStateChanges::delayed_receipt_key_decode_shard_uid(
            row_key_extra_bytes.as_ref(),
            &block_hash,
            &trie_key1
        )
        .is_err());

        // This is the internal detail of how delayed_receipt_key_from_trie_key() works.
        let mut row_key_with_single_shard_uid =
            KeyForStateChanges::from_trie_key(&block_hash, &trie_key1);
        row_key_with_single_shard_uid.0.extend(shard_uid.to_bytes());
        assert_eq!(
            KeyForStateChanges::delayed_receipt_key_decode_shard_uid(
                row_key_with_single_shard_uid.as_ref(),
                &block_hash,
                &trie_key1
            )
            .unwrap(),
            shard_uid
        );
    }

    //TODO(jbajic) Simplify logic for creating configuration
    #[test]
    fn test_insert_delete_trie_cache() {
        let store = create_test_store();
        let trie_cache_config = TrieCacheConfig {
            default_max_bytes: DEFAULT_SHARD_CACHE_TOTAL_SIZE_LIMIT,
            per_shard_max_bytes: Default::default(),
            shard_cache_deletions_queue_capacity: 0,
        };
        let trie_config = TrieConfig {
            shard_cache_config: trie_cache_config.clone(),
            view_shard_cache_config: trie_cache_config,
            enable_receipt_prefetching: false,
            sweat_prefetch_receivers: Vec::new(),
            sweat_prefetch_senders: Vec::new(),
        };
        let shard_uids = Vec::from([ShardUId { shard_id: 0, version: 0 }]);
        let shard_uid = *shard_uids.first().unwrap();

        let trie = ShardTries::new(
            store.clone(),
            trie_config,
            &shard_uids,
            FlatStorageManager::new(store),
        );

        let trie_caches = &trie.0.caches;
        // Assert only one cache for one shard exists
        assert_eq!(trie_caches.read().unwrap().len(), 1);
        // Assert the shard uid is correct
        assert!(trie_caches.read().unwrap().get(&shard_uid).is_some());

        // Read from cache
        let key = CryptoHash::hash_borsh("alice");
        let val: Vec<u8> = Vec::from([0, 1, 2, 3, 4]);

        assert!(trie_caches.read().unwrap().get(&shard_uid).unwrap().get(&key).is_none());

        let insert_ops = Vec::from([(&key, Some(val.as_slice()))]);
        trie.update_cache(insert_ops, shard_uid);
        assert_eq!(
            trie_caches.read().unwrap().get(&shard_uid).unwrap().get(&key).unwrap().to_vec(),
            val
        );

        let deletions_ops = Vec::from([(&key, None)]);
        trie.update_cache(deletions_ops, shard_uid);
        assert!(trie_caches.read().unwrap().get(&shard_uid).unwrap().get(&key).is_none());
    }

    #[test]
    fn test_shard_cache_max_value() {
        let store = create_test_store();
        let trie_cache_config = TrieCacheConfig {
            default_max_bytes: DEFAULT_SHARD_CACHE_TOTAL_SIZE_LIMIT,
            per_shard_max_bytes: Default::default(),
            shard_cache_deletions_queue_capacity: 0,
        };
        let trie_config = TrieConfig {
            shard_cache_config: trie_cache_config.clone(),
            view_shard_cache_config: trie_cache_config,
            enable_receipt_prefetching: false,
            sweat_prefetch_receivers: Vec::new(),
            sweat_prefetch_senders: Vec::new(),
        };
        let shard_uids = Vec::from([ShardUId { shard_id: 0, version: 0 }]);
        let shard_uid = *shard_uids.first().unwrap();

        let trie = ShardTries::new(
            store.clone(),
            trie_config,
            &shard_uids,
            FlatStorageManager::new(store),
        );

        let trie_caches = &trie.0.caches;

        // Insert into cache value at the configured maximum size
        let key = CryptoHash::hash_borsh("alice");
        let val: Vec<u8> = vec![0; TrieConfig::max_cached_value_size() - 1];
        let insert_ops = Vec::from([(&key, Some(val.as_slice()))]);
        trie.update_cache(insert_ops, shard_uid);
        assert_eq!(
            trie_caches.read().unwrap().get(&shard_uid).unwrap().get(&key).unwrap().to_vec(),
            val
        );

        // Try to insert into cache value bigger then the maximum allowed size
        let key = CryptoHash::from_str("32222222222233333333334444444444445555555777").unwrap();
        let val: Vec<u8> = vec![0; TrieConfig::max_cached_value_size()];
        let insert_ops = Vec::from([(&key, Some(val.as_slice()))]);
        trie.update_cache(insert_ops, shard_uid);
        assert!(trie_caches.read().unwrap().get(&shard_uid).unwrap().get(&key).is_none());
    }
}

use crate::flat::FlatStorageManager;
use crate::trie::config::TrieConfig;
use crate::trie::prefetching_trie_storage::PrefetchingThreadsHandle;
use crate::trie::trie_storage::{TrieCache, TrieCachingStorage};
use crate::trie::{TrieRefcountChange, POISONED_LOCK_ERR};
use crate::{metrics, DBCol, DBOp, DBTransaction, PrefetchApi};
use crate::{Store, StoreUpdate, Trie, TrieChanges, TrieUpdate};
use borsh::BorshSerialize;
use near_primitives::borsh::maybestd::collections::HashMap;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{self, ShardUId, ShardVersion};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{
    NumShards, RawStateChange, RawStateChangesWithTrieKey, StateChangeCause, StateRoot,
};
use std::sync::{Arc, RwLock};

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
}

#[derive(Clone)]
pub struct ShardTries(Arc<ShardTriesInner>);

impl ShardTries {
    pub fn new(
        store: Store,
        trie_config: TrieConfig,
        shard_uids: &[ShardUId],
        flat_storage_manager: FlatStorageManager,
    ) -> Self {
        let caches = Self::create_initial_caches(&trie_config, &shard_uids, false);
        let view_caches = Self::create_initial_caches(&trie_config, &shard_uids, true);
        ShardTries(Arc::new(ShardTriesInner {
            store,
            trie_config,
            caches: RwLock::new(caches),
            view_caches: RwLock::new(view_caches),
            flat_storage_manager,
            prefetchers: Default::default(),
        }))
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

        ShardTries::new(
            store.clone(),
            trie_config,
            &shard_uids,
            FlatStorageManager::test(store, &shard_uids, CryptoHash::default()),
        )
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

    pub(crate) fn is_same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub fn new_trie_update(&self, shard_uid: ShardUId, state_root: StateRoot) -> TrieUpdate {
        TrieUpdate::new(self.get_trie_for_shard(shard_uid, state_root))
    }

    pub fn new_trie_update_view(&self, shard_uid: ShardUId, state_root: StateRoot) -> TrieUpdate {
        TrieUpdate::new(self.get_view_trie_for_shard(shard_uid, state_root))
    }

    #[allow(unused_variables)]
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

        let storage = Box::new(TrieCachingStorage::new(
            self.0.store.clone(),
            cache,
            shard_uid,
            is_view,
            prefetch_api,
        ));
        let flat_storage_chunk_view =
            self.0.flat_storage_manager.chunk_view(shard_uid, block_hash, is_view);

        Trie::new(storage, state_root, flat_storage_chunk_view)
    }

    pub fn get_trie_for_shard(&self, shard_uid: ShardUId, state_root: StateRoot) -> Trie {
        self.get_trie_for_shard_internal(shard_uid, state_root, false, None)
    }

    pub fn get_trie_with_block_hash_for_shard(
        &self,
        shard_uid: ShardUId,
        state_root: StateRoot,
        block_hash: &CryptoHash,
    ) -> Trie {
        self.get_trie_for_shard_internal(shard_uid, state_root, false, Some(*block_hash))
    }

    pub fn get_view_trie_for_shard(&self, shard_uid: ShardUId, state_root: StateRoot) -> Trie {
        self.get_trie_for_shard_internal(shard_uid, state_root, true, None)
    }

    pub fn store_update(&self) -> StoreUpdate {
        StoreUpdate::new_with_tries(self.clone())
    }

    pub fn get_store(&self) -> Store {
        self.0.store.clone()
    }

    pub(crate) fn get_db(&self) -> &Arc<dyn crate::Database> {
        &self.0.store.storage
    }

    pub(crate) fn update_cache(&self, transaction: &DBTransaction) -> std::io::Result<()> {
        let mut caches = self.0.caches.write().expect(POISONED_LOCK_ERR);
        let mut shards = HashMap::new();
        for op in &transaction.ops {
            match op {
                DBOp::UpdateRefcount { col, key, value } => {
                    if *col == DBCol::State {
                        let (shard_uid, hash) =
                            TrieCachingStorage::get_shard_uid_and_hash_from_key(key)?;
                        shards
                            .entry(shard_uid)
                            .or_insert(vec![])
                            .push((hash, Some(value.as_slice())));
                    }
                }
                DBOp::DeleteAll { col } => {
                    if *col == DBCol::State {
                        // Delete is possible in reset_data_pre_state_sync
                        for (_, cache) in caches.iter() {
                            cache.clear();
                        }
                    }
                }
                DBOp::Set { col, .. }
                | DBOp::Insert { col, .. }
                | DBOp::Delete { col, .. }
                | DBOp::DeleteRange { col, .. } => {
                    assert_ne!(*col, DBCol::State);
                }
            }
        }
        for (shard_uid, ops) in shards {
            let cache = caches
                .entry(shard_uid)
                .or_insert_with(|| TrieCache::new(&self.0.trie_config, shard_uid, false))
                .clone();
            cache.update_cache(ops);
        }
        Ok(())
    }

    fn apply_deletions_inner(
        &self,
        deletions: &[TrieRefcountChange],
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) {
        store_update.set_shard_tries(self);
        for TrieRefcountChange { trie_node_or_value_hash, rc, .. } in deletions.iter() {
            let key = TrieCachingStorage::get_key_from_shard_uid_and_hash(
                shard_uid,
                trie_node_or_value_hash,
            );
            store_update.decrement_refcount_by(DBCol::State, key.as_ref(), *rc);
        }
    }

    fn apply_insertions_inner(
        &self,
        insertions: &[TrieRefcountChange],
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) {
        store_update.set_shard_tries(self);
        for TrieRefcountChange { trie_node_or_value_hash, trie_node_or_value, rc } in
            insertions.iter()
        {
            let key = TrieCachingStorage::get_key_from_shard_uid_and_hash(
                shard_uid,
                trie_node_or_value_hash,
            );
            store_update.increment_refcount_by(DBCol::State, key.as_ref(), trie_node_or_value, *rc);
        }
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
}

pub struct WrappedTrieChanges {
    tries: ShardTries,
    shard_uid: ShardUId,
    trie_changes: TrieChanges,
    state_changes: Vec<RawStateChangesWithTrieKey>,
    block_hash: CryptoHash,
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
        for change_with_trie_key in self.state_changes.drain(..) {
            assert!(
                !change_with_trie_key.changes.iter().any(|RawStateChange { cause, .. }| matches!(
                    cause,
                    StateChangeCause::NotWritableToDisk
                )),
                "NotWritableToDisk changes must never be finalized."
            );

            assert!(
                !change_with_trie_key.changes.iter().any(|RawStateChange { cause, .. }| matches!(
                    cause,
                    StateChangeCause::Resharding
                )),
                "Resharding changes must never be finalized."
            );

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
                &change_with_trie_key.try_to_vec().expect("Borsh serialize cannot fail"),
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
    fn estimate_prefix_len() -> usize {
        std::mem::size_of::<CryptoHash>()
    }

    fn new(block_hash: &CryptoHash, reserve_capacity: usize) -> Self {
        let mut key_prefix = Vec::with_capacity(Self::estimate_prefix_len() + reserve_capacity);
        key_prefix.extend(block_hash.as_ref());
        debug_assert_eq!(key_prefix.len(), Self::estimate_prefix_len());
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
    ) -> impl Iterator<Item = Result<RawStateChangesWithTrieKey, std::io::Error>> + 'a {
        let prefix_len = Self::estimate_prefix_len();
        let trie_key_len = self.0.len() - prefix_len;
        self.find_iter(store).filter_map(move |change| {
            let state_changes = match change {
                Ok(change) => change,
                error => {
                    return Some(error);
                }
            };
            if state_changes.trie_key.len() != trie_key_len {
                None
            } else {
                debug_assert_eq!(&state_changes.trie_key.to_vec()[..], &self.0[prefix_len..]);
                Some(Ok(state_changes))
            }
        })
    }

    /// Iterates over pairs of `(row key, deserialized row value)` where row key matches `self`.
    pub fn find_rows_iter<'a>(
        &'a self,
        store: &'a Store,
    ) -> impl Iterator<Item = Result<(Box<[u8]>, RawStateChangesWithTrieKey), std::io::Error>> + 'a
    {
        let prefix_len = Self::estimate_prefix_len();
        debug_assert!(
            self.0.len() >= prefix_len,
            "Key length: {}, prefix length: {}, key: {:?}",
            self.0.len(),
            prefix_len,
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
    use super::*;
    use std::str::FromStr;

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
}

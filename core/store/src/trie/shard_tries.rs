use std::io;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

use borsh::BorshSerialize;
use near_primitives::borsh::maybestd::collections::HashMap;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout;
use near_primitives::shard_layout::{ShardUId, ShardVersion};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{
    NumShards, RawStateChange, RawStateChangesWithTrieKey, StateChangeCause, StateRoot,
};

use crate::flat_state::FlatState;
use crate::trie::trie_storage::{TrieCache, TrieCachingStorage};
use crate::trie::{TrieRefcountChange, POISONED_LOCK_ERR};
use crate::{metrics, DBCol, DBOp, DBTransaction};
use crate::{Store, StoreUpdate, Trie, TrieChanges, TrieUpdate};

/// Responsible for creation of trie caches, stores necessary configuration for it.
#[derive(Default)]
pub struct TrieCacheFactory {
    capacities: HashMap<ShardUId, usize>,
    shard_version: ShardVersion,
    num_shards: NumShards,
}

impl TrieCacheFactory {
    pub fn new(
        capacities: HashMap<ShardUId, usize>,
        shard_version: ShardVersion,
        num_shards: NumShards,
    ) -> Self {
        Self { capacities, shard_version, num_shards }
    }

    /// Create new cache for the given shard uid.
    pub fn create_cache(&self, shard_uid: &ShardUId, is_view: bool) -> TrieCache {
        let capacity = if is_view { None } else { self.capacities.get(shard_uid) };
        match capacity {
            Some(capacity) => TrieCache::with_capacities(*capacity, shard_uid.shard_id, is_view),
            None => TrieCache::new(shard_uid.shard_id, is_view),
        }
    }

    /// Create caches on the initialization of storage structures.
    pub fn create_initial_caches(&self, is_view: bool) -> HashMap<ShardUId, TrieCache> {
        assert_ne!(self.num_shards, 0);
        let shards: Vec<_> = (0..self.num_shards)
            .map(|shard_id| ShardUId { version: self.shard_version, shard_id: shard_id as u32 })
            .collect();
        shards
            .iter()
            .map(|&shard_uid| (shard_uid, self.create_cache(&shard_uid, is_view)))
            .collect()
    }
}

struct ShardTriesInner {
    store: Store,
    trie_cache_factory: TrieCacheFactory,
    /// Cache reserved for client actor to use
    caches: RwLock<HashMap<ShardUId, TrieCache>>,
    /// Cache for readers.
    view_caches: RwLock<HashMap<ShardUId, TrieCache>>,
}

#[derive(Clone)]
pub struct ShardTries(Arc<ShardTriesInner>);

impl ShardTries {
    pub fn new(store: Store, trie_cache_factory: TrieCacheFactory) -> Self {
        let caches = trie_cache_factory.create_initial_caches(false);
        let view_caches = trie_cache_factory.create_initial_caches(true);
        ShardTries(Arc::new(ShardTriesInner {
            store,
            trie_cache_factory,
            caches: RwLock::new(caches),
            view_caches: RwLock::new(view_caches),
        }))
    }

    pub fn test(store: Store, num_shards: NumShards) -> Self {
        Self::new(store, TrieCacheFactory::new(Default::default(), 0, num_shards))
    }

    pub fn is_same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub fn new_trie_update(&self, shard_uid: ShardUId, state_root: StateRoot) -> TrieUpdate {
        TrieUpdate::new(Rc::new(self.get_trie_for_shard(shard_uid, state_root)))
    }

    pub fn new_trie_update_view(&self, shard_uid: ShardUId, state_root: StateRoot) -> TrieUpdate {
        TrieUpdate::new(Rc::new(self.get_view_trie_for_shard(shard_uid, state_root)))
    }

    #[allow(unused_variables)]
    fn get_trie_for_shard_internal(
        &self,
        shard_uid: ShardUId,
        state_root: StateRoot,
        is_view: bool,
        flat_state: Option<FlatState>,
    ) -> Trie {
        let caches_to_use = if is_view { &self.0.view_caches } else { &self.0.caches };
        let cache = {
            let mut caches = caches_to_use.write().expect(POISONED_LOCK_ERR);
            caches
                .entry(shard_uid)
                .or_insert_with(|| self.0.trie_cache_factory.create_cache(&shard_uid, is_view))
                .clone()
        };
        let storage =
            Box::new(TrieCachingStorage::new(self.0.store.clone(), cache, shard_uid, is_view));
        Trie::new(storage, state_root, flat_state)
    }

    pub fn get_trie_for_shard(&self, shard_uid: ShardUId, state_root: StateRoot) -> Trie {
        self.get_trie_for_shard_internal(shard_uid, state_root, false, None)
    }

    pub fn get_trie_with_flat_state_for_shard(
        &self,
        shard_uid: ShardUId,
        state_root: StateRoot,
        prev_block_hash: &CryptoHash,
    ) -> Trie {
        let flat_state = crate::flat_state::maybe_new(true, &self.0.store, prev_block_hash);
        self.get_trie_for_shard_internal(shard_uid, state_root, false, flat_state)
    }

    pub fn get_view_trie_for_shard(&self, shard_uid: ShardUId, state_root: StateRoot) -> Trie {
        self.get_trie_for_shard_internal(shard_uid, state_root, true, None)
    }

    pub fn get_store(&self) -> Store {
        self.0.store.clone()
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
                        shards.entry(shard_uid).or_insert(vec![]).push((hash, Some(value)));
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
                DBOp::Set { col, .. } | DBOp::Insert { col, .. } | DBOp::Delete { col, .. } => {
                    assert_ne!(*col, DBCol::State);
                }
            }
        }
        for (shard_uid, ops) in shards {
            let cache = caches
                .entry(shard_uid)
                .or_insert_with(|| self.0.trie_cache_factory.create_cache(&shard_uid, false))
                .clone();
            cache.update_cache(ops);
        }
        Ok(())
    }

    fn apply_deletions_inner(
        &self,
        deletions: &Vec<TrieRefcountChange>,
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
        insertions: &Vec<TrieRefcountChange>,
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
    ) -> (StoreUpdate, StateRoot) {
        let mut store_update = StoreUpdate::new_with_tries(self.clone());
        self.apply_insertions_inner(&trie_changes.insertions, shard_uid, &mut store_update);
        if apply_deletions {
            self.apply_deletions_inner(&trie_changes.deletions, shard_uid, &mut store_update);
        }
        (store_update, trie_changes.new_root)
    }

    pub fn apply_insertions(
        &self,
        trie_changes: &TrieChanges,
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) {
        metrics::APPLIED_TRIE_INSERTIONS
            .with_label_values(&[&format!("{}", shard_uid.shard_id)])
            .inc_by(trie_changes.insertions.len() as u64);
        self.apply_insertions_inner(&trie_changes.insertions, shard_uid, store_update)
    }

    pub fn apply_deletions(
        &self,
        trie_changes: &TrieChanges,
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) {
        metrics::APPLIED_TRIE_DELETIONS
            .with_label_values(&[&format!("{}", shard_uid.shard_id)])
            .inc_by(trie_changes.deletions.len() as u64);
        self.apply_deletions_inner(&trie_changes.deletions, shard_uid, store_update)
    }

    pub fn revert_insertions(
        &self,
        trie_changes: &TrieChanges,
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) {
        metrics::REVERTED_TRIE_INSERTIONS
            .with_label_values(&[&format!("{}", shard_uid.shard_id)])
            .inc_by(trie_changes.insertions.len() as u64);
        self.apply_deletions_inner(&trie_changes.insertions, shard_uid, store_update)
    }

    pub fn apply_all(
        &self,
        trie_changes: &TrieChanges,
        shard_uid: ShardUId,
    ) -> (StoreUpdate, StateRoot) {
        self.apply_all_inner(trie_changes, shard_uid, true)
    }

    #[cfg(feature = "protocol_feature_flat_state")]
    pub fn apply_genesis_changes_to_flat_state(
        &self,
        changes: &[RawStateChangesWithTrieKey],
        store_update: &mut StoreUpdate,
    ) {
        for change in changes.iter() {
            let key = change.trie_key.to_vec();
            if near_primitives::state_record::is_delayed_receipt_key(&key) {
                continue;
            }

            // `RawStateChangesWithTrieKey` stores all sequential changes for a key within a chunk, so it is sufficient
            // to take only the last change.
            let last_change = &change
                .changes
                .last()
                .expect("Committed entry should have at least one change")
                .data;
            match last_change {
                Some(value) => {
                    let value_ref_ser = near_primitives::state::ValueRef::create_serialized(value);
                    store_update.set(DBCol::FlatState, &key, &value_ref_ser);
                }
                None => {
                    store_update.delete(DBCol::FlatState, &key);
                }
            };
        }
    }

    // TODO(#7327): consider uniting with `apply_all`
    #[cfg(feature = "protocol_feature_flat_state")]
    pub fn apply_changes_to_flat_state(
        &self,
        block_hash: &CryptoHash,
        changes: &[RawStateChangesWithTrieKey],
        store_update: &mut StoreUpdate,
    ) {
        for change in changes.iter() {
            let key = change.trie_key.to_vec();
            if near_primitives::state_record::is_delayed_receipt_key(&key) {
                continue;
            }

            // `RawStateChangesWithTrieKey` stores all sequential changes for a key within a chunk, so it is sufficient
            // to take only the last change.
            let last_change = &change
                .changes
                .last()
                .expect("Committed entry should have at least one change")
                .data;
            let flat_state_delta_key = KeyForStateChanges::from_raw_key(block_hash, &key);
            match last_change {
                Some(value) => {
                    let value_ref_ser = near_primitives::state::ValueRef::create_serialized(value);
                    store_update.set(
                        DBCol::FlatStateDeltas,
                        flat_state_delta_key.as_ref(),
                        &value_ref_ser,
                    );
                }
                None => {
                    store_update.set(DBCol::FlatStateDeltas, flat_state_delta_key.as_ref(), &[]);
                }
            };
        }
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
        #[cfg(feature = "protocol_feature_flat_state")]
        self.tries.apply_changes_to_flat_state(&self.block_hash, &self.state_changes, store_update);

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
            let storage_key =
                KeyForStateChanges::from_trie_key(&self.block_hash, &change_with_trie_key.trie_key);
            store_update.set(
                DBCol::StateChanges,
                storage_key.as_ref(),
                &change_with_trie_key.try_to_vec().expect("Borsh serialize cannot fail"),
            );
        }
    }

    pub fn trie_changes_into(&mut self, store_update: &mut StoreUpdate) -> io::Result<()> {
        store_update.set_ser(
            DBCol::TrieChanges,
            &shard_layout::get_block_shard_uid(&self.block_hash, &self.shard_uid),
            &self.trie_changes,
        )
    }
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

    pub fn find_iter<'a>(
        &'a self,
        store: &'a Store,
    ) -> impl Iterator<Item = Result<RawStateChangesWithTrieKey, std::io::Error>> + 'a {
        let prefix_len = Self::estimate_prefix_len();
        debug_assert!(self.0.len() >= prefix_len);
        store.iter_prefix_ser::<RawStateChangesWithTrieKey>(DBCol::StateChanges, &self.0).map(
            move |change| {
                // Split off the irrelevant part of the key, so only the original trie_key is left.
                let (key, state_changes) = change?;
                debug_assert!(key.starts_with(&self.0));
                Ok(state_changes)
            },
        )
    }

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
}

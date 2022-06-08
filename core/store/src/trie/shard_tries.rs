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

use crate::trie::trie_storage::{TrieCache, TrieCachingStorage};
use crate::trie::{TrieRefcountChange, POISONED_LOCK_ERR};
use crate::{DBCol, DBOp, DBTransaction};
use crate::{Store, StoreUpdate, Trie, TrieChanges, TrieUpdate};

struct ShardTriesInner {
    store: Store,
    /// Cache reserved for client actor to use
    caches: RwLock<HashMap<ShardUId, TrieCache>>,
    /// Cache for readers.
    view_caches: RwLock<HashMap<ShardUId, TrieCache>>,
}

#[derive(Clone)]
pub struct ShardTries(Arc<ShardTriesInner>);

impl ShardTries {
    fn get_new_cache(shards: &[ShardUId]) -> HashMap<ShardUId, TrieCache> {
        shards.iter().map(|&shard_id| (shard_id, TrieCache::new())).collect()
    }

    pub fn new(store: Store, shard_version: ShardVersion, num_shards: NumShards) -> Self {
        assert_ne!(num_shards, 0);
        let shards: Vec<_> = (0..num_shards)
            .map(|shard_id| ShardUId { version: shard_version, shard_id: shard_id as u32 })
            .collect();
        ShardTries(Arc::new(ShardTriesInner {
            store,
            caches: RwLock::new(Self::get_new_cache(&shards)),
            view_caches: RwLock::new(Self::get_new_cache(&shards)),
        }))
    }

    pub fn is_same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub fn new_trie_update(&self, shard_uid: ShardUId, state_root: CryptoHash) -> TrieUpdate {
        TrieUpdate::new(Rc::new(self.get_trie_for_shard(shard_uid)), state_root)
    }

    pub fn new_trie_update_view(&self, shard_uid: ShardUId, state_root: CryptoHash) -> TrieUpdate {
        TrieUpdate::new(Rc::new(self.get_view_trie_for_shard(shard_uid)), state_root)
    }

    fn get_trie_for_shard_internal(&self, shard_uid: ShardUId, is_view: bool) -> Trie {
        let caches_to_use = if is_view { &self.0.view_caches } else { &self.0.caches };
        let cache = {
            let mut caches = caches_to_use.write().expect(POISONED_LOCK_ERR);
            caches.entry(shard_uid).or_insert_with(TrieCache::new).clone()
        };
        let store = Box::new(TrieCachingStorage::new(self.0.store.clone(), cache, shard_uid));
        Trie::new(store, shard_uid)
    }

    pub fn get_trie_for_shard(&self, shard_uid: ShardUId) -> Trie {
        self.get_trie_for_shard_internal(shard_uid, false)
    }

    pub fn get_view_trie_for_shard(&self, shard_uid: ShardUId) -> Trie {
        self.get_trie_for_shard_internal(shard_uid, true)
    }

    pub fn get_store(&self) -> Store {
        self.0.store.clone()
    }

    pub(crate) fn update_cache(&self, transaction: &DBTransaction) -> std::io::Result<()> {
        let mut caches = self.0.caches.write().expect(POISONED_LOCK_ERR);
        let mut shards = HashMap::new();
        for op in &transaction.ops {
            match op {
                DBOp::UpdateRefcount { col, ref key, ref value } if *col == DBCol::State => {
                    let (shard_uid, hash) =
                        TrieCachingStorage::get_shard_uid_and_hash_from_key(key)?;
                    shards.entry(shard_uid).or_insert(vec![]).push((hash, Some(value)));
                }
                DBOp::Set { col, .. } if *col == DBCol::State => unreachable!(),
                DBOp::Delete { col, .. } if *col == DBCol::State => unreachable!(),
                DBOp::DeleteAll { col } if *col == DBCol::State => {
                    // Delete is possible in reset_data_pre_state_sync
                    for (_, cache) in caches.iter() {
                        cache.clear();
                    }
                }
                _ => {}
            }
        }
        for (shard_uid, ops) in shards {
            let cache = caches.entry(shard_uid).or_insert_with(TrieCache::new).clone();
            cache.update_cache(ops);
        }
        Ok(())
    }

    fn apply_deletions_inner(
        deletions: &Vec<TrieRefcountChange>,
        tries: ShardTries,
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) {
        store_update.tries = Some(tries);
        for TrieRefcountChange { trie_node_or_value_hash, trie_node_or_value, rc } in
            deletions.iter()
        {
            let key = TrieCachingStorage::get_key_from_shard_uid_and_hash(
                shard_uid,
                trie_node_or_value_hash,
            );
            store_update.update_refcount(
                DBCol::State,
                key.as_ref(),
                trie_node_or_value,
                -(*rc as i64),
            );
        }
    }

    fn apply_insertions_inner(
        insertions: &Vec<TrieRefcountChange>,
        tries: ShardTries,
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) {
        store_update.tries = Some(tries);
        for TrieRefcountChange { trie_node_or_value_hash, trie_node_or_value, rc } in
            insertions.iter()
        {
            let key = TrieCachingStorage::get_key_from_shard_uid_and_hash(
                shard_uid,
                trie_node_or_value_hash,
            );
            store_update.update_refcount(
                DBCol::State,
                key.as_ref(),
                trie_node_or_value,
                *rc as i64,
            );
        }
    }

    fn apply_all_inner(
        trie_changes: &TrieChanges,
        tries: ShardTries,
        shard_uid: ShardUId,
        apply_deletions: bool,
    ) -> (StoreUpdate, StateRoot) {
        let mut store_update = StoreUpdate::new_with_tries(tries.clone());
        ShardTries::apply_insertions_inner(
            &trie_changes.insertions,
            tries.clone(),
            shard_uid,
            &mut store_update,
        );
        if apply_deletions {
            ShardTries::apply_deletions_inner(
                &trie_changes.deletions,
                tries,
                shard_uid,
                &mut store_update,
            );
        }
        (store_update, trie_changes.new_root)
    }

    pub fn apply_insertions(
        &self,
        trie_changes: &TrieChanges,
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) {
        ShardTries::apply_insertions_inner(
            &trie_changes.insertions,
            self.clone(),
            shard_uid,
            store_update,
        )
    }

    pub fn apply_deletions(
        &self,
        trie_changes: &TrieChanges,
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) {
        ShardTries::apply_deletions_inner(
            &trie_changes.deletions,
            self.clone(),
            shard_uid,
            store_update,
        )
    }

    pub fn revert_insertions(
        &self,
        trie_changes: &TrieChanges,
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) {
        ShardTries::apply_deletions_inner(
            &trie_changes.insertions,
            self.clone(),
            shard_uid,
            store_update,
        )
    }

    pub fn apply_all(
        &self,
        trie_changes: &TrieChanges,
        shard_uid: ShardUId,
    ) -> (StoreUpdate, StateRoot) {
        ShardTries::apply_all_inner(trie_changes, self.clone(), shard_uid, true)
    }

    // apply_all with less memory overhead
    pub fn apply_genesis(
        &self,
        trie_changes: TrieChanges,
        shard_uid: ShardUId,
    ) -> (StoreUpdate, StateRoot) {
        assert_eq!(trie_changes.old_root, CryptoHash::default());
        assert!(trie_changes.deletions.is_empty());
        // Not new_with_tries on purpose
        let mut store_update = StoreUpdate::new(self.get_store().storage.clone());
        for TrieRefcountChange { trie_node_or_value_hash, trie_node_or_value, rc } in
            trie_changes.insertions.into_iter()
        {
            let key = TrieCachingStorage::get_key_from_shard_uid_and_hash(
                shard_uid,
                &trie_node_or_value_hash,
            );
            store_update.update_refcount(
                DBCol::State,
                key.as_ref(),
                &trie_node_or_value,
                rc as i64,
            );
        }
        (store_update, trie_changes.new_root)
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

    pub fn insertions_into(&self, store_update: &mut StoreUpdate) {
        self.tries.apply_insertions(&self.trie_changes, self.shard_uid, store_update)
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

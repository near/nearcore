use crate::db::{DBCol, DBOp, DBTransaction};
use crate::trie::trie_storage::{TrieCache, TrieCachingStorage};
use crate::{StorageError, Store, StoreUpdate, Trie, TrieChanges, TrieUpdate};
use borsh::BorshSerialize;
use near_primitives::hash::CryptoHash;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{
    NumShards, RawStateChange, RawStateChangesWithTrieKey, ShardId, StateChangeCause, StateRoot,
};
use near_primitives::utils::get_block_shard_id;
use std::rc::Rc;
use std::sync::Arc;

#[derive(Clone)]
pub struct ShardTries {
    pub(crate) store: Arc<Store>,
    pub(crate) caches: Arc<Vec<TrieCache>>,
}

impl ShardTries {
    pub fn new(store: Arc<Store>, num_shards: NumShards) -> Self {
        assert_ne!(num_shards, 0);
        let caches = Arc::new((0..num_shards).map(|_| TrieCache::new()).collect::<Vec<_>>());
        ShardTries { store, caches }
    }

    pub fn new_trie_update(&self, shard_id: ShardId, state_root: CryptoHash) -> TrieUpdate {
        TrieUpdate::new(Rc::new(self.get_trie_for_shard(shard_id)), state_root)
    }

    pub fn get_trie_for_shard(&self, shard_id: ShardId) -> Trie {
        let store = Box::new(TrieCachingStorage::new(
            self.store.clone(),
            self.caches[shard_id as usize].clone(),
            shard_id,
        ));
        Trie::new(store, shard_id)
    }

    pub fn get_store(&self) -> Arc<Store> {
        self.store.clone()
    }

    pub fn update_cache(&self, transaction: &DBTransaction) -> std::io::Result<()> {
        let mut shards = vec![Vec::new(); self.caches.len()];
        for op in &transaction.ops {
            match op {
                DBOp::UpdateRefcount { col, ref key, ref value } if *col == DBCol::ColState => {
                    let (shard_id, hash) = TrieCachingStorage::get_shard_id_and_hash_from_key(key)?;
                    shards[shard_id as usize].push((hash, Some(value.clone())));
                }
                DBOp::Insert { col, .. } if *col == DBCol::ColState => unreachable!(),
                DBOp::Delete { col, key } if *col == DBCol::ColState => {
                    // Delete is possible in reset_data_pre_state_sync
                    let (shard_id, hash) = TrieCachingStorage::get_shard_id_and_hash_from_key(key)?;
                    shards[shard_id as usize].push((hash, None));
                }
                _ => {}
            }
        }
        for (shard_id, ops) in shards.into_iter().enumerate() {
            self.caches[shard_id].update_cache(ops);
        }
        Ok(())
    }

    fn apply_deletions_inner(
        deletions: &Vec<(CryptoHash, Vec<u8>, u32)>,
        tries: ShardTries,
        shard_id: ShardId,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
        store_update.tries = Some(tries.clone());
        for (hash, value, rc) in deletions.iter() {
            let key = TrieCachingStorage::get_key_from_shard_id_and_hash(shard_id, hash);
            store_update.update_refcount(DBCol::ColState, key.as_ref(), &value, -(*rc as i64));
        }
        Ok(())
    }

    fn apply_insertions_inner(
        insertions: &Vec<(CryptoHash, Vec<u8>, u32)>,
        tries: ShardTries,
        shard_id: ShardId,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
        store_update.tries = Some(tries);
        for (hash, value, rc) in insertions.iter() {
            let key = TrieCachingStorage::get_key_from_shard_id_and_hash(shard_id, hash);
            store_update.update_refcount(DBCol::ColState, key.as_ref(), &value, *rc as i64);
        }
        Ok(())
    }

    fn apply_all_inner(
        trie_changes: &TrieChanges,
        tries: ShardTries,
        shard_id: ShardId,
        apply_deletions: bool,
    ) -> Result<(StoreUpdate, StateRoot), StorageError> {
        let mut store_update = StoreUpdate::new_with_tries(tries.clone());
        ShardTries::apply_insertions_inner(
            &trie_changes.insertions,
            tries.clone(),
            shard_id,
            &mut store_update,
        )?;
        if apply_deletions {
            ShardTries::apply_deletions_inner(
                &trie_changes.deletions,
                tries,
                shard_id,
                &mut store_update,
            )?;
        }
        Ok((store_update, trie_changes.new_root))
    }

    pub fn apply_insertions(
        &self,
        trie_changes: &TrieChanges,
        shard_id: ShardId,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
        ShardTries::apply_insertions_inner(
            &trie_changes.insertions,
            self.clone(),
            shard_id,
            store_update,
        )
    }

    pub fn apply_deletions(
        &self,
        trie_changes: &TrieChanges,
        shard_id: ShardId,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
        ShardTries::apply_deletions_inner(
            &trie_changes.deletions,
            self.clone(),
            shard_id,
            store_update,
        )
    }

    pub fn revert_insertions(
        &self,
        trie_changes: &TrieChanges,
        shard_id: ShardId,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
        ShardTries::apply_deletions_inner(
            &trie_changes.insertions,
            self.clone(),
            shard_id,
            store_update,
        )
    }

    pub fn apply_all(
        &self,
        trie_changes: &TrieChanges,
        shard_id: ShardId,
    ) -> Result<(StoreUpdate, StateRoot), StorageError> {
        ShardTries::apply_all_inner(trie_changes, self.clone(), shard_id, true)
    }
}

pub struct WrappedTrieChanges {
    tries: ShardTries,
    shard_id: ShardId,
    trie_changes: TrieChanges,
    state_changes: Vec<RawStateChangesWithTrieKey>,
    block_hash: CryptoHash,
}

impl WrappedTrieChanges {
    pub fn new(
        tries: ShardTries,
        shard_id: ShardId,
        trie_changes: TrieChanges,
        state_changes: Vec<RawStateChangesWithTrieKey>,
        block_hash: CryptoHash,
    ) -> Self {
        WrappedTrieChanges { tries, shard_id, trie_changes, state_changes, block_hash }
    }

    pub fn insertions_into(&self, store_update: &mut StoreUpdate) -> Result<(), StorageError> {
        self.tries.apply_insertions(&self.trie_changes, self.shard_id, store_update)
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
            let storage_key = KeyForStateChanges::new_from_trie_key(
                &self.block_hash,
                &change_with_trie_key.trie_key,
            );
            store_update.set(
                DBCol::ColStateChanges,
                storage_key.as_ref(),
                &change_with_trie_key.try_to_vec().expect("Borsh serialize cannot fail"),
            );
        }
    }

    pub fn wrapped_into(
        &mut self,
        mut store_update: &mut StoreUpdate,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.insertions_into(&mut store_update)?;
        self.state_changes_into(&mut store_update);
        store_update.set_ser(
            DBCol::ColTrieChanges,
            &get_block_shard_id(&self.block_hash, self.shard_id),
            &self.trie_changes,
        )?;
        Ok(())
    }
}

#[derive(derive_more::AsRef, derive_more::Into)]
pub struct KeyForStateChanges(Vec<u8>);

impl KeyForStateChanges {
    fn estimate_prefix_len() -> usize {
        std::mem::size_of::<CryptoHash>()
    }

    fn get_prefix_with_capacity(block_hash: &CryptoHash, reserve_capacity: usize) -> Self {
        let mut key_prefix = Vec::with_capacity(Self::estimate_prefix_len() + reserve_capacity);
        key_prefix.extend(block_hash.as_ref());
        debug_assert_eq!(key_prefix.len(), Self::estimate_prefix_len());
        Self(key_prefix)
    }

    pub fn get_prefix(block_hash: &CryptoHash) -> Self {
        Self::get_prefix_with_capacity(block_hash, 0)
    }

    pub fn new(block_hash: &CryptoHash, raw_key: &[u8]) -> Self {
        let mut key = Self::get_prefix_with_capacity(block_hash, raw_key.len());
        key.0.extend(raw_key);
        key
    }

    pub fn new_from_trie_key(block_hash: &CryptoHash, trie_key: &TrieKey) -> Self {
        let mut key = Self::get_prefix_with_capacity(block_hash, trie_key.len());
        key.0.extend(trie_key.to_vec());
        key
    }

    pub fn find_iter<'a: 'b, 'b>(
        &'a self,
        store: &'b Store,
    ) -> impl Iterator<Item = Result<RawStateChangesWithTrieKey, std::io::Error>> + 'b {
        let prefix_len = Self::estimate_prefix_len();
        debug_assert!(self.0.len() >= prefix_len);
        store.iter_prefix_ser::<RawStateChangesWithTrieKey>(DBCol::ColStateChanges, &self.0).map(
            move |change| {
                // Split off the irrelevant part of the key, so only the original trie_key is left.
                let (key, state_changes) = change?;
                debug_assert!(key.starts_with(&self.0));
                Ok(state_changes)
            },
        )
    }

    pub fn find_exact_iter<'a: 'b, 'b>(
        &'a self,
        store: &'b Store,
    ) -> impl Iterator<Item = Result<RawStateChangesWithTrieKey, std::io::Error>> + 'b {
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

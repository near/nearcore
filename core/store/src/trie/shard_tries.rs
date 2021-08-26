use std::rc::Rc;
use std::sync::Arc;

use borsh::BorshSerialize;
use near_primitives::borsh::maybestd::collections::HashMap;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout;
use near_primitives::shard_layout::{ShardUId, ShardVersion};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{
    NumShards, RawStateChange, RawStateChangesWithTrieKey, ShardId, StateChangeCause, StateRoot,
};

use crate::db::{DBCol, DBOp, DBTransaction};
use crate::trie::trie_storage::{TrieCache, TrieCachingStorage};
use crate::trie::TrieRefcountChange;
use crate::{StorageError, Store, StoreUpdate, Trie, TrieChanges, TrieUpdate};
use std::str::from_utf8;

#[derive(Clone)]
pub struct ShardTries {
    pub(crate) store: Arc<Store>,
    /// Cache reserved for client actor to use
    pub(crate) caches: Arc<HashMap<ShardUId, TrieCache>>,
    /// Cache for readers.
    pub(crate) view_caches: Arc<HashMap<ShardUId, TrieCache>>,
}

impl ShardTries {
    fn get_new_cache(shards: &Vec<ShardUId>) -> Arc<HashMap<ShardUId, TrieCache>> {
        let mut cache = HashMap::new();
        shards.iter().for_each(|x| {
            cache.insert(*x, TrieCache::new());
            ()
        });
        Arc::new(cache)
    }

    pub fn new(store: Arc<Store>, shard_version: ShardVersion, num_shards: NumShards) -> Self {
        assert_ne!(num_shards, 0);
        let shards = (0..num_shards)
            .map(|shard_id| ShardUId { version: shard_version, shard_id: shard_id as u32 })
            .collect();
        ShardTries {
            store,
            caches: Self::get_new_cache(&shards),
            view_caches: Self::get_new_cache(&shards),
        }
    }

    pub fn add_new_shards(&mut self, shard_version: ShardVersion, num_shards: NumShards) {
        let shards = (0..num_shards)
            .map(|shard_id| ShardUId { version: shard_version, shard_id: shard_id as u32 })
            .collect();
        Arc::get_mut(&mut self.caches)
            .unwrap()
            .extend(Self::get_new_cache(&shards).as_ref().clone());
        Arc::get_mut(&mut self.view_caches)
            .unwrap()
            .extend(Self::get_new_cache(&shards).as_ref().clone());
    }

    pub fn new_trie_update(&self, shard_uid: ShardUId, state_root: CryptoHash) -> TrieUpdate {
        TrieUpdate::new(Rc::new(self.get_trie_for_shard(shard_uid)), state_root)
    }

    pub fn new_trie_update_view(&self, shard_uid: ShardUId, state_root: CryptoHash) -> TrieUpdate {
        TrieUpdate::new(Rc::new(self.get_view_trie_for_shard(shard_uid)), state_root)
    }

    fn get_trie_for_shard_internal(&self, shard_uid: ShardUId, is_view: bool) -> Trie {
        let cache = if is_view {
            self.view_caches[&shard_uid].clone()
        } else {
            self.caches[&shard_uid].clone()
        };
        let store = Box::new(TrieCachingStorage::new(self.store.clone(), cache, shard_uid));
        Trie::new(store, shard_uid)
    }

    pub fn get_trie_for_shard(&self, shard_uid: ShardUId) -> Trie {
        self.get_trie_for_shard_internal(shard_uid, false)
    }

    pub fn get_view_trie_for_shard(&self, shard_uid: ShardUId) -> Trie {
        self.get_trie_for_shard_internal(shard_uid, true)
    }

    pub fn get_store(&self) -> Arc<Store> {
        self.store.clone()
    }

    pub fn update_cache(&self, transaction: &DBTransaction) -> std::io::Result<()> {
        let mut shards = self
            .caches
            .iter()
            .map(|(shard_uid, ..)| (*shard_uid, Vec::new()))
            .collect::<HashMap<_, _>>();
        for op in &transaction.ops {
            match op {
                DBOp::UpdateRefcount { col, ref key, ref value } if *col == DBCol::ColState => {
                    let (shard_uid, hash) =
                        TrieCachingStorage::get_shard_uid_and_hash_from_key(key)?;
                    shards.entry(shard_uid).or_insert(vec![]).push((hash, Some(value.clone())));
                }
                DBOp::Insert { col, .. } if *col == DBCol::ColState => unreachable!(),
                DBOp::Delete { col, .. } if *col == DBCol::ColState => unreachable!(),
                DBOp::DeleteAll { col } if *col == DBCol::ColState => {
                    // Delete is possible in reset_data_pre_state_sync
                    for (_, cache) in self.caches.iter() {
                        cache.clear();
                    }
                }
                _ => {}
            }
        }
        for (shard_uid, ops) in shards {
            self.caches[&shard_uid].update_cache(ops);
        }
        Ok(())
    }

    fn apply_deletions_inner(
        deletions: &Vec<TrieRefcountChange>,
        tries: ShardTries,
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
        store_update.tries = Some(tries.clone());
        for TrieRefcountChange { trie_node_or_value_hash, trie_node_or_value, rc } in
            deletions.iter()
        {
            let key = TrieCachingStorage::get_key_from_shard_uid_and_hash(
                shard_uid,
                trie_node_or_value_hash,
            );
            store_update.update_refcount(
                DBCol::ColState,
                key.as_ref(),
                &trie_node_or_value,
                -(*rc as i64),
            );
        }
        Ok(())
    }

    fn apply_insertions_inner(
        insertions: &Vec<TrieRefcountChange>,
        tries: ShardTries,
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
        store_update.tries = Some(tries);
        for TrieRefcountChange { trie_node_or_value_hash, trie_node_or_value, rc } in
            insertions.iter()
        {
            let key = TrieCachingStorage::get_key_from_shard_uid_and_hash(
                shard_uid,
                trie_node_or_value_hash,
            );
            store_update.update_refcount(
                DBCol::ColState,
                key.as_ref(),
                &trie_node_or_value,
                *rc as i64,
            );
        }
        Ok(())
    }

    fn apply_all_inner(
        trie_changes: &TrieChanges,
        tries: ShardTries,
        shard_uid: ShardUId,
        apply_deletions: bool,
    ) -> Result<(StoreUpdate, StateRoot), StorageError> {
        let mut store_update = StoreUpdate::new_with_tries(tries.clone());
        ShardTries::apply_insertions_inner(
            &trie_changes.insertions,
            tries.clone(),
            shard_uid,
            &mut store_update,
        )?;
        if apply_deletions {
            ShardTries::apply_deletions_inner(
                &trie_changes.deletions,
                tries,
                shard_uid,
                &mut store_update,
            )?;
        }
        Ok((store_update, trie_changes.new_root))
    }

    pub fn apply_insertions(
        &self,
        trie_changes: &TrieChanges,
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
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
    ) -> Result<(), StorageError> {
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
    ) -> Result<(), StorageError> {
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
    ) -> Result<(StoreUpdate, StateRoot), StorageError> {
        ShardTries::apply_all_inner(trie_changes, self.clone(), shard_uid, true)
    }

    /// Apply `changes` to build states for new shards
    /// `state_roots` contains state roots for the new shards
    /// Ignore changes on DelayedReceipts or DelayedReceiptsIndices
    /// Update `store_update` and return new state_roots
    /// used for building states for new shards in resharding
    pub fn apply_changes_to_new_states<'a>(
        &self,
        state_roots: HashMap<ShardId, StateRoot>,
        changes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        key_to_shard_id: Box<dyn Fn(&Vec<u8>) -> Option<ShardUId> + 'a>,
    ) -> Result<(StoreUpdate, HashMap<ShardId, StateRoot>), StorageError> {
        let mut changes_by_shard = HashMap::new();
        for (raw_key, value) in changes.into_iter() {
            if let Some(new_shard_uid) = key_to_shard_id(&raw_key) {
                assert!(
                    state_roots.contains_key(&new_shard_uid.shard_id()),
                    "Cannot find state roots for shard {:?} for key {}. Only has state roots for shards {:?}",
                    new_shard_uid,
                    from_utf8(&raw_key).unwrap(),
                    state_roots.keys(),
                );
                changes_by_shard
                    .entry(new_shard_uid)
                    .or_insert_with(Vec::new)
                    .push((raw_key, value));
            }
        }
        let mut new_state_roots = state_roots.clone();
        let mut store_update = StoreUpdate::new_with_tries(self.clone());
        for (shard_uid, changes) in changes_by_shard.into_iter() {
            let shard_id = shard_uid.shard_id();
            let trie = self.get_trie_for_shard(shard_uid.clone());
            let trie_changes = trie.update(&state_roots[&shard_id], changes.into_iter())?;
            let (update, state_root) = self.apply_all(&trie_changes, shard_uid.clone())?;
            new_state_roots.insert(shard_id, state_root);
            store_update.merge(update);
        }
        Ok((store_update, new_state_roots))
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
                DBCol::ColState,
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

    pub fn insertions_into(&self, store_update: &mut StoreUpdate) -> Result<(), StorageError> {
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
            &shard_layout::get_block_shard_uid(&self.block_hash, self.shard_uid),
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

#[cfg(test)]
mod tests {
    use crate::test_utils::{create_tries, gen_changes, test_populate_trie};
    use crate::{ShardUId, Trie};
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::types::NumShards;
    use std::collections::HashMap;

    #[test]
    fn test_apply_changes_to_new_states() {
        let mut rng = rand::thread_rng();

        for _ in 0..20 {
            let mut tries = create_tries();
            // add 4 new shards for version 1
            let num_shards = 4;
            tries.add_new_shards(1, num_shards);
            let mut state_root = Trie::empty_root();
            let mut state_roots: HashMap<_, _> =
                (0..num_shards).map(|x| (x, CryptoHash::default())).collect();
            for _ in 0..10 {
                let trie = tries.get_trie_for_shard(ShardUId::default());
                let changes = gen_changes(&mut rng, 100);
                state_root =
                    test_populate_trie(&tries, &state_root, ShardUId::default(), changes.clone());

                let (store_update, new_state_roots) = tries
                    .apply_changes_to_new_states(
                        state_roots,
                        changes,
                        Box::new(|raw_key| {
                            Some(ShardUId {
                                version: 1,
                                shard_id: (hash(raw_key).0[0] as NumShards % num_shards) as u32,
                            })
                        }),
                    )
                    .unwrap();
                store_update.commit().unwrap();
                state_roots = new_state_roots;

                // check that the 4 tries combined to the orig trie
                let trie_items: HashMap<_, _> =
                    trie.iter(&state_root).unwrap().map(Result::unwrap).collect();
                let mut combined_trie_items: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
                state_roots.iter().for_each(|(shard_id, state_root)| {
                    let trie = tries.get_view_trie_for_shard(ShardUId {
                        version: 1,
                        shard_id: *shard_id as u32,
                    });
                    combined_trie_items.extend(trie.iter(state_root).unwrap().map(Result::unwrap));
                });
                assert_eq!(trie_items, combined_trie_items);
            }
        }
    }
}

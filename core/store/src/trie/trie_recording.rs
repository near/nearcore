use crate::PartialStorage;
use near_primitives::challenge::PartialState;
use near_primitives::hash::CryptoHash;
use near_primitives::types::AccountId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// A simple struct to capture a state proof as it's being accumulated.
pub struct TrieRecorder {
    recorded: HashMap<CryptoHash, Arc<[u8]>>,
    size: usize,
    /// Counts removals performed while recording.
    /// recorded_storage_size_upper_bound takes it into account when calculating the total size.
    removal_counter: usize,
    /// Counts the total size of the contract codes read while recording.
    code_len_counter: usize,
    /// Account IDs for which the code should be recorded.
    pub codes_to_record: HashSet<AccountId>,
}

impl TrieRecorder {
    pub fn new() -> Self {
        Self {
            recorded: HashMap::new(),
            size: 0,
            removal_counter: 0,
            code_len_counter: 0,
            codes_to_record: Default::default(),
        }
    }

    pub fn record(&mut self, hash: &CryptoHash, node: Arc<[u8]>) {
        let size = node.len();
        if self.recorded.insert(*hash, node).is_none() {
            self.size += size;
        }
    }

    pub fn record_removal(&mut self) {
        self.removal_counter = self.removal_counter.saturating_add(1)
    }

    pub fn record_code_len(&mut self, code_len: usize) {
        self.code_len_counter = self.code_len_counter.saturating_add(code_len)
    }

    pub fn recorded_storage(&mut self) -> PartialStorage {
        let mut nodes: Vec<_> = self.recorded.drain().map(|(_key, value)| value).collect();
        nodes.sort();
        PartialStorage { nodes: PartialState::TrieValues(nodes) }
    }

    pub fn recorded_storage_size(&self) -> usize {
        debug_assert!(self.size == self.recorded.values().map(|v| v.len()).sum::<usize>());
        self.size
    }

    /// Size of the recorded state proof plus some additional size added to cover removals
    /// and contract codes.
    /// An upper-bound estimation of the true recorded size after finalization.
    /// See https://github.com/near/nearcore/issues/10890 and https://github.com/near/nearcore/pull/11000 for details.
    pub fn recorded_storage_size_upper_bound(&self) -> usize {
        // Charge 2000 bytes for every removal
        let removals_size = self.removal_counter.saturating_mul(2000);
        self.recorded_storage_size()
            .saturating_add(removals_size)
            .saturating_add(self.code_len_counter)
    }
}

#[cfg(test)]
mod trie_recording_tests {
    use crate::db::refcount::decode_value_with_rc;
    use crate::test_utils::{
        gen_larger_changes, simplify_changes, test_populate_flat_storage, test_populate_trie,
        TestTriesBuilder,
    };
    use crate::trie::mem::metrics::MEM_TRIE_NUM_LOOKUPS;
    use crate::trie::TrieNodesCount;
    use crate::{DBCol, KeyLookupMode, PartialStorage, ShardTries, Store, Trie};
    use borsh::BorshDeserialize;
    use near_primitives::challenge::PartialState;
    use near_primitives::congestion_info::CongestionInfo;
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::shard_layout::{get_block_shard_uid, ShardUId};
    use near_primitives::state::ValueRef;
    use near_primitives::types::chunk_extra::ChunkExtra;
    use near_primitives::types::StateRoot;
    use near_primitives::version::{ProtocolFeature, PROTOCOL_VERSION};
    use rand::prelude::SliceRandom;
    use rand::{random, thread_rng, Rng};
    use std::collections::{HashMap, HashSet};
    use std::num::NonZeroU32;

    const NUM_ITERATIONS_PER_TEST: usize = 300;

    /// Prepared on-disk trie and flat storage for testing.
    struct PreparedTrie {
        store: Store,
        shard_uid: ShardUId,
        /// All the data we've put into the trie.
        data_in_trie: HashMap<Vec<u8>, Vec<u8>>,
        /// The keys that we should be using to call get() on the trie with.
        keys_to_get: Vec<Vec<u8>>,
        /// The keys that we should be using to call get_optimized_ref() on the
        /// trie with.
        keys_to_get_ref: Vec<Vec<u8>>,
        /// The keys to be updated after trie reads.
        updates: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        state_root: StateRoot,
    }

    /// Prepare a trie for testing; this will prepare both a trie and a flat
    /// storage with some dummy block info. If `use_missing_keys` is true,
    /// the keys to test with will also include some keys that are not in the
    /// trie.
    fn prepare_trie(
        use_missing_keys: bool,
        p_existing_key: f64,
        p_missing_key: f64,
    ) -> PreparedTrie {
        let tries_for_building = TestTriesBuilder::new().with_flat_storage(true).build();
        let shard_uid = ShardUId::single_shard();
        let trie_changes = gen_larger_changes(&mut thread_rng(), 50);
        let trie_changes = simplify_changes(&trie_changes);
        if trie_changes.is_empty() {
            // try again
            return prepare_trie(use_missing_keys, p_existing_key, p_missing_key);
        }
        let state_root = test_populate_trie(
            &tries_for_building,
            &Trie::EMPTY_ROOT,
            shard_uid,
            trie_changes.clone(),
        );
        test_populate_flat_storage(
            &tries_for_building,
            shard_uid,
            &CryptoHash::default(),
            &CryptoHash::default(),
            &trie_changes,
        );

        let congestion_info = ProtocolFeature::CongestionControl
            .enabled(PROTOCOL_VERSION)
            .then(CongestionInfo::default);

        // ChunkExtra is needed for in-memory trie loading code to query state roots.
        let chunk_extra = ChunkExtra::new(
            PROTOCOL_VERSION,
            &state_root,
            CryptoHash::default(),
            Vec::new(),
            0,
            0,
            0,
            congestion_info,
        );
        let mut update_for_chunk_extra = tries_for_building.store_update();
        update_for_chunk_extra
            .set_ser(
                DBCol::ChunkExtra,
                &get_block_shard_uid(&CryptoHash::default(), &shard_uid),
                &chunk_extra,
            )
            .unwrap();
        update_for_chunk_extra.commit().unwrap();

        let data_in_trie = trie_changes
            .iter()
            .map(|(key, value)| (key.clone(), value.clone().unwrap()))
            .collect::<HashMap<_, _>>();
        let existing_keys: HashSet<_> = trie_changes
            .into_iter()
            .map(|(key, _)| key)
            .filter(|_| thread_rng().gen_bool(p_existing_key))
            .collect();
        let missing_keys = if use_missing_keys {
            existing_keys
                .iter()
                .cloned()
                .map(|mut key| {
                    *key.last_mut().unwrap() = 100;
                    key
                })
                .filter(|key| !existing_keys.contains(key) && thread_rng().gen_bool(p_missing_key))
                .collect::<HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
        } else {
            vec![]
        };
        let mut keys: Vec<_> =
            existing_keys.iter().cloned().chain(missing_keys.into_iter()).collect();
        keys.shuffle(&mut thread_rng());
        let updates = keys
            .iter()
            .map(|key| {
                let value = if thread_rng().gen_bool(0.5) {
                    Some(vec![thread_rng().gen_range(0..10) as u8])
                } else {
                    None
                };
                (key.clone(), value)
            })
            .filter(|_| random())
            .collect::<Vec<_>>();
        let (keys_to_get, keys_to_get_ref) =
            keys.into_iter().filter(|_| random()).partition::<Vec<_>, _>(|_| random());
        PreparedTrie {
            store: tries_for_building.get_store(),
            shard_uid,
            data_in_trie,
            keys_to_get,
            keys_to_get_ref,
            updates,
            state_root,
        }
    }

    /// Delete state that we should not be relying on if in-memory tries are
    /// loaded, to help make sure that in-memory tries are used.
    ///
    /// The only thing we don't delete are the values, which may not be
    /// inlined.
    fn destructively_delete_in_memory_state_from_disk(
        store: &Store,
        data_in_trie: &HashMap<Vec<u8>, Vec<u8>>,
    ) {
        let key_hashes_to_keep = data_in_trie.iter().map(|(_, v)| hash(&v)).collect::<HashSet<_>>();
        let mut update = store.store_update();
        for result in store.iter_raw_bytes(DBCol::State) {
            let (key, value) = result.unwrap();
            let (_, refcount) = decode_value_with_rc(&value);
            let key_hash: CryptoHash = CryptoHash::try_from_slice(&key[8..]).unwrap();
            if !key_hashes_to_keep.contains(&key_hash) {
                update.decrement_refcount_by(
                    DBCol::State,
                    &key,
                    NonZeroU32::new(refcount as u32).unwrap(),
                );
            }
        }
        update.delete_all(DBCol::FlatState);
        update.commit().unwrap();
    }

    fn get_trie_for_shard(
        tries: &ShardTries,
        shard_uid: ShardUId,
        state_root: StateRoot,
        use_flat_storage: bool,
    ) -> Trie {
        if use_flat_storage {
            tries.get_trie_with_block_hash_for_shard(
                shard_uid,
                state_root,
                &CryptoHash::default(),
                false,
            )
        } else {
            tries.get_trie_for_shard(shard_uid, state_root)
        }
    }

    /// Assert equality of partial storages with human-readable output.
    fn assert_partial_storage(storage: &PartialStorage, other_storage: &PartialStorage) {
        let PartialState::TrieValues(nodes) = &storage.nodes;
        let PartialState::TrieValues(other_nodes) = &other_storage.nodes;
        let nodes: HashSet<Vec<u8>> = HashSet::from_iter(nodes.into_iter().map(|key| key.to_vec()));
        let other_nodes: HashSet<Vec<u8>> =
            HashSet::from_iter(other_nodes.into_iter().map(|key| key.to_vec()));
        let d: Vec<&Vec<u8>> = other_nodes.difference(&nodes).collect();
        assert_eq!(d, Vec::<&Vec<u8>>::default(), "Missing nodes in first storage");
        let d: Vec<&Vec<u8>> = nodes.difference(&other_nodes).collect();
        assert_eq!(d, Vec::<&Vec<u8>>::default(), "Missing nodes in second storage");
    }

    /// Verifies that when operating on a trie, the results are completely consistent
    /// regardless of whether we're operating on the real storage (with or without chunk
    /// cache), while recording reads, or when operating on recorded partial storage.
    fn test_trie_recording_consistency(
        enable_accounting_cache: bool,
        use_missing_keys: bool,
        use_flat_storage: bool,
    ) {
        for _ in 0..NUM_ITERATIONS_PER_TEST {
            let p_existing_key = thread_rng().gen_range(0.3..1.0);
            let p_missing_key = thread_rng().gen_range(0.7..1.0);
            let PreparedTrie {
                store,
                shard_uid,
                data_in_trie,
                keys_to_get,
                keys_to_get_ref,
                updates,
                state_root,
            } = prepare_trie(use_missing_keys, p_existing_key, p_missing_key);
            let tries = TestTriesBuilder::new()
                .with_store(store.clone())
                .with_flat_storage(use_flat_storage)
                .build();
            let lookup_mode =
                if use_flat_storage { KeyLookupMode::FlatStorage } else { KeyLookupMode::Trie };
            let mem_trie_lookup_counts_before = MEM_TRIE_NUM_LOOKUPS.get();

            // Check that while using flat storage counters are all zero.
            // Only use get_optimized_ref(), because get() will actually
            // dereference values which can cause trie reads.
            if use_flat_storage {
                let trie = get_trie_for_shard(&tries, shard_uid, state_root, use_flat_storage);
                for key in data_in_trie.keys() {
                    trie.get_optimized_ref(key, lookup_mode).unwrap();
                }
                assert_eq!(
                    trie.get_trie_nodes_count(),
                    TrieNodesCount { db_reads: 0, mem_reads: 0 }
                );
            }

            // Let's capture the baseline node counts - this is what will happen
            // in production.
            let trie = get_trie_for_shard(&tries, shard_uid, state_root, use_flat_storage);
            trie.accounting_cache.borrow_mut().set_enabled(enable_accounting_cache);
            for key in &keys_to_get {
                assert_eq!(trie.get(key).unwrap(), data_in_trie.get(key).cloned());
            }
            for key in &keys_to_get_ref {
                assert_eq!(
                    trie.get_optimized_ref(key, lookup_mode)
                        .unwrap()
                        .map(|value| value.into_value_ref()),
                    data_in_trie.get(key).map(|value| ValueRef::new(&value))
                );
            }
            let baseline_trie_nodes_count = trie.get_trie_nodes_count();
            println!("Baseline trie nodes count: {:?}", baseline_trie_nodes_count);
            trie.update(updates.iter().cloned()).unwrap();

            // Now let's do this again while recording, and make sure that the counters
            // we get are exactly the same.
            let trie = get_trie_for_shard(&tries, shard_uid, state_root, use_flat_storage)
                .recording_reads();
            trie.accounting_cache.borrow_mut().set_enabled(enable_accounting_cache);
            for key in &keys_to_get {
                assert_eq!(trie.get(key).unwrap(), data_in_trie.get(key).cloned());
            }
            for key in &keys_to_get_ref {
                assert_eq!(
                    trie.get_optimized_ref(key, lookup_mode)
                        .unwrap()
                        .map(|value| value.into_value_ref()),
                    data_in_trie.get(key).map(|value| ValueRef::new(&value))
                );
            }
            assert_eq!(trie.get_trie_nodes_count(), baseline_trie_nodes_count);
            trie.update(updates.iter().cloned()).unwrap();
            let baseline_partial_storage = trie.recorded_storage().unwrap();

            // Now let's do this again with memtries enabled. Check that counters
            // are the same.
            assert_eq!(MEM_TRIE_NUM_LOOKUPS.get(), mem_trie_lookup_counts_before);
            tries.load_mem_trie(&shard_uid, None).unwrap();
            // Delete the on-disk state so that we really know we're using
            // in-memory tries.
            destructively_delete_in_memory_state_from_disk(&store, &data_in_trie);
            let trie = get_trie_for_shard(&tries, shard_uid, state_root, use_flat_storage)
                .recording_reads();
            trie.accounting_cache.borrow_mut().set_enabled(enable_accounting_cache);
            for key in &keys_to_get {
                assert_eq!(trie.get(key).unwrap(), data_in_trie.get(key).cloned());
            }
            for key in &keys_to_get_ref {
                assert_eq!(
                    trie.get_optimized_ref(key, lookup_mode)
                        .unwrap()
                        .map(|value| value.into_value_ref()),
                    data_in_trie.get(key).map(|value| ValueRef::new(&value))
                );
            }
            assert_eq!(trie.get_trie_nodes_count(), baseline_trie_nodes_count);
            trie.update(updates.iter().cloned()).unwrap();

            // Now, let's check that when doing the same lookups with the captured partial storage,
            // we still get the same counters.
            let partial_storage = trie.recorded_storage().unwrap();
            assert_partial_storage(&baseline_partial_storage, &partial_storage);
            println!(
                "Partial storage has {} nodes from {} entries",
                partial_storage.nodes.len(),
                data_in_trie.len()
            );
            let trie =
                Trie::from_recorded_storage(partial_storage.clone(), state_root, use_flat_storage);
            trie.accounting_cache.borrow_mut().set_enabled(enable_accounting_cache);
            for key in &keys_to_get {
                assert_eq!(trie.get(key).unwrap(), data_in_trie.get(key).cloned());
            }
            for key in &keys_to_get_ref {
                assert_eq!(
                    trie.get_optimized_ref(key, lookup_mode)
                        .unwrap()
                        .map(|value| value.into_value_ref()),
                    data_in_trie.get(key).map(|value| ValueRef::new(&value))
                );
            }
            assert_eq!(trie.get_trie_nodes_count(), baseline_trie_nodes_count);
            trie.update(updates.iter().cloned()).unwrap();

            // Build a Trie using recorded storage and enable recording_reads on this Trie
            let trie = Trie::from_recorded_storage(partial_storage, state_root, use_flat_storage)
                .recording_reads();
            trie.accounting_cache.borrow_mut().set_enabled(enable_accounting_cache);
            for key in &keys_to_get {
                assert_eq!(trie.get(key).unwrap(), data_in_trie.get(key).cloned());
            }
            for key in &keys_to_get_ref {
                assert_eq!(
                    trie.get_optimized_ref(key, lookup_mode)
                        .unwrap()
                        .map(|value| value.into_value_ref()),
                    data_in_trie.get(key).map(|value| ValueRef::new(&value))
                );
            }
            assert_eq!(trie.get_trie_nodes_count(), baseline_trie_nodes_count);
            trie.update(updates.iter().cloned()).unwrap();
            assert_partial_storage(&baseline_partial_storage, &trie.recorded_storage().unwrap());

            if !keys_to_get.is_empty() || !keys_to_get_ref.is_empty() {
                // sanity check that we did indeed use in-memory tries.
                assert!(MEM_TRIE_NUM_LOOKUPS.get() > mem_trie_lookup_counts_before);
            }
        }
    }

    #[test]
    fn test_trie_recording_consistency_no_accounting_cache() {
        test_trie_recording_consistency(false, false, false);
    }

    #[test]
    fn test_trie_recording_consistency_with_accounting_cache() {
        test_trie_recording_consistency(true, false, false);
    }

    #[test]
    fn test_trie_recording_consistency_no_accounting_cache_with_missing_keys() {
        test_trie_recording_consistency(false, true, false);
    }

    #[test]
    fn test_trie_recording_consistency_with_accounting_cache_and_missing_keys() {
        test_trie_recording_consistency(true, true, false);
    }

    #[test]
    fn test_trie_recording_consistency_with_flat_storage_no_accounting_cache() {
        test_trie_recording_consistency(false, false, true);
    }

    #[test]
    fn test_trie_recording_consistency_with_flat_storage_with_accounting_cache() {
        test_trie_recording_consistency(true, false, true);
    }

    #[test]
    fn test_trie_recording_consistency_with_flat_storage_no_accounting_cache_with_missing_keys() {
        test_trie_recording_consistency(false, true, true);
    }

    #[test]
    fn test_trie_recording_consistency_with_flat_storage_with_accounting_cache_and_missing_keys() {
        test_trie_recording_consistency(true, true, true);
    }
}

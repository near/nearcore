use crate::PartialStorage;
use near_primitives::challenge::PartialState;
use near_primitives::hash::CryptoHash;
use std::collections::HashMap;
use std::sync::Arc;

/// A simple struct to capture a state proof as it's being accumulated.
pub struct TrieRecorder {
    recorded: HashMap<CryptoHash, Arc<[u8]>>,
}

impl TrieRecorder {
    pub fn new() -> Self {
        Self { recorded: HashMap::new() }
    }

    pub fn record(&mut self, hash: &CryptoHash, node: Arc<[u8]>) {
        self.recorded.insert(*hash, node);
    }

    pub fn recorded_storage(&mut self) -> PartialStorage {
        let mut nodes: Vec<_> = self.recorded.drain().map(|(_key, value)| value).collect();
        nodes.sort();
        PartialStorage { nodes: PartialState::TrieValues(nodes) }
    }
}

#[cfg(test)]
mod trie_recording_tests {
    use crate::test_utils::{
        create_tries_complex_with_flat_storage, gen_larger_changes, simplify_changes,
        test_populate_flat_storage, test_populate_trie,
    };
    use crate::Trie;
    use near_primitives::hash::CryptoHash;
    use near_primitives::shard_layout::ShardUId;
    use near_vm_runner::logic::TrieNodesCount;
    use std::collections::HashMap;

    const NUM_ITERATIONS_PER_TEST: usize = 100;

    /// Verifies that when operating on a trie, the results are completely consistent
    /// regardless of whether we're operating on the real storage (with or without chunk
    /// cache), while recording reads, or when operating on recorded partial storage.
    fn test_trie_recording_consistency(enable_accounting_cache: bool, use_missing_keys: bool) {
        let mut rng = rand::thread_rng();
        for _ in 0..NUM_ITERATIONS_PER_TEST {
            let tries = create_tries_complex_with_flat_storage(1, 2);

            let shard_uid = ShardUId { version: 1, shard_id: 0 };
            let trie_changes = gen_larger_changes(&mut rng, 50);
            let trie_changes = simplify_changes(&trie_changes);
            if trie_changes.is_empty() {
                continue;
            }
            let state_root =
                test_populate_trie(&tries, &Trie::EMPTY_ROOT, shard_uid, trie_changes.clone());
            let data_in_trie = trie_changes
                .iter()
                .map(|(key, value)| (key.clone(), value.clone().unwrap()))
                .collect::<HashMap<_, _>>();
            let keys_to_test_with = trie_changes
                .iter()
                .map(|(key, _)| {
                    let mut key = key.clone();
                    if use_missing_keys {
                        key.push(100);
                    }
                    key
                })
                .collect::<Vec<_>>();

            // Let's capture the baseline node counts - this is what will happen
            // in production.
            let trie = tries.get_trie_for_shard(shard_uid, state_root);
            trie.accounting_cache.borrow_mut().set_enabled(enable_accounting_cache);
            for key in &keys_to_test_with {
                assert_eq!(trie.get(key).unwrap(), data_in_trie.get(key).cloned());
            }
            let baseline_trie_nodes_count = trie.get_trie_nodes_count();
            println!("Baseline trie nodes count: {:?}", baseline_trie_nodes_count);

            // Now let's do this again while recording, and make sure that the counters
            // we get are exactly the same.
            let trie = tries.get_trie_for_shard(shard_uid, state_root).recording_reads();
            trie.accounting_cache.borrow_mut().set_enabled(enable_accounting_cache);
            for key in &keys_to_test_with {
                assert_eq!(trie.get(key).unwrap(), data_in_trie.get(key).cloned());
            }
            assert_eq!(trie.get_trie_nodes_count(), baseline_trie_nodes_count);

            // Now, let's check that when doing the same lookups with the captured partial storage,
            // we still get the same counters.
            let partial_storage = trie.recorded_storage().unwrap();
            println!(
                "Partial storage has {} nodes from {} entries",
                partial_storage.nodes.len(),
                trie_changes.len()
            );
            let trie = Trie::from_recorded_storage(partial_storage, state_root, false);
            trie.accounting_cache.borrow_mut().set_enabled(enable_accounting_cache);
            for key in &keys_to_test_with {
                assert_eq!(trie.get(key).unwrap(), data_in_trie.get(key).cloned());
            }
            assert_eq!(trie.get_trie_nodes_count(), baseline_trie_nodes_count);
        }
    }

    #[test]
    fn test_trie_recording_consistency_no_accounting_cache() {
        test_trie_recording_consistency(false, false);
    }

    #[test]
    fn test_trie_recording_consistency_with_accounting_cache() {
        test_trie_recording_consistency(true, false);
    }

    #[test]
    fn test_trie_recording_consistency_no_accounting_cache_with_missing_keys() {
        test_trie_recording_consistency(false, true);
    }

    #[test]
    fn test_trie_recording_consistency_with_accounting_cache_and_missing_keys() {
        test_trie_recording_consistency(true, true);
    }

    /// Verifies that when operating on a trie, the results are completely consistent
    /// regardless of whether we're operating on the real storage (with or without chunk
    /// cache), while recording reads, or when operating on recorded partial storage.
    /// This test additionally verifies this when flat storage is used.
    fn test_trie_recording_consistency_with_flat_storage(
        enable_accounting_cache: bool,
        use_missing_keys: bool,
    ) {
        let mut rng = rand::thread_rng();
        for _ in 0..NUM_ITERATIONS_PER_TEST {
            let tries = create_tries_complex_with_flat_storage(1, 2);

            let shard_uid = ShardUId { version: 1, shard_id: 0 };
            let trie_changes = gen_larger_changes(&mut rng, 50);
            let trie_changes = simplify_changes(&trie_changes);
            if trie_changes.is_empty() {
                continue;
            }
            let state_root =
                test_populate_trie(&tries, &Trie::EMPTY_ROOT, shard_uid, trie_changes.clone());
            test_populate_flat_storage(
                &tries,
                shard_uid,
                &CryptoHash::default(),
                &CryptoHash::default(),
                &trie_changes,
            );

            let data_in_trie = trie_changes
                .iter()
                .map(|(key, value)| (key.clone(), value.clone().unwrap()))
                .collect::<HashMap<_, _>>();
            let keys_to_test_with = trie_changes
                .iter()
                .map(|(key, _)| {
                    let mut key = key.clone();
                    if use_missing_keys {
                        key.push(100);
                    }
                    key
                })
                .collect::<Vec<_>>();

            // First, check that the trie is using flat storage, so that counters are all zero.
            // Only use get_ref(), because get() will actually dereference values which can
            // cause trie reads.
            let trie = tries.get_trie_with_block_hash_for_shard(
                shard_uid,
                state_root,
                &CryptoHash::default(),
                false,
            );
            for key in &keys_to_test_with {
                trie.get_ref(&key, crate::KeyLookupMode::FlatStorage).unwrap();
            }
            assert_eq!(trie.get_trie_nodes_count(), TrieNodesCount { db_reads: 0, mem_reads: 0 });

            // Now, let's capture the baseline node counts - this is what will happen
            // in production.
            let trie = tries.get_trie_with_block_hash_for_shard(
                shard_uid,
                state_root,
                &CryptoHash::default(),
                false,
            );
            trie.accounting_cache.borrow_mut().set_enabled(enable_accounting_cache);
            for key in &keys_to_test_with {
                assert_eq!(trie.get(key).unwrap(), data_in_trie.get(key).cloned());
            }
            let baseline_trie_nodes_count = trie.get_trie_nodes_count();
            println!("Baseline trie nodes count: {:?}", baseline_trie_nodes_count);

            // Let's do this again, but this time recording reads. We'll make sure
            // the counters are exactly the same even when we're recording.
            let trie = tries
                .get_trie_with_block_hash_for_shard(
                    shard_uid,
                    state_root,
                    &CryptoHash::default(),
                    false,
                )
                .recording_reads();
            trie.accounting_cache.borrow_mut().set_enabled(enable_accounting_cache);
            for key in &keys_to_test_with {
                assert_eq!(trie.get(key).unwrap(), data_in_trie.get(key).cloned());
            }
            assert_eq!(trie.get_trie_nodes_count(), baseline_trie_nodes_count);

            // Now, let's check that when doing the same lookups with the captured partial storage,
            // we still get the same counters.
            let partial_storage = trie.recorded_storage().unwrap();
            println!(
                "Partial storage has {} nodes from {} entries",
                partial_storage.nodes.len(),
                trie_changes.len()
            );
            let trie = Trie::from_recorded_storage(partial_storage, state_root, true);
            trie.accounting_cache.borrow_mut().set_enabled(enable_accounting_cache);
            for key in &keys_to_test_with {
                assert_eq!(trie.get(key).unwrap(), data_in_trie.get(key).cloned());
            }
            assert_eq!(trie.get_trie_nodes_count(), baseline_trie_nodes_count);
        }
    }

    #[test]
    fn test_trie_recording_consistency_with_flat_storage_no_accounting_cache() {
        test_trie_recording_consistency_with_flat_storage(false, false);
    }

    #[test]
    fn test_trie_recording_consistency_with_flat_storage_with_accounting_cache() {
        test_trie_recording_consistency_with_flat_storage(true, false);
    }

    #[test]
    fn test_trie_recording_consistency_with_flat_storage_no_accounting_cache_with_missing_keys() {
        test_trie_recording_consistency_with_flat_storage(false, true);
    }

    #[test]
    fn test_trie_recording_consistency_with_flat_storage_with_accounting_cache_and_missing_keys() {
        test_trie_recording_consistency_with_flat_storage(true, true);
    }
}

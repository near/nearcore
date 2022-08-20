use crate::test_utils::{create_tries_complex, gen_changes, simplify_changes, test_populate_trie};
use crate::trie::trie_storage::{TrieMemoryPartialStorage, TrieStorage};
use crate::{PartialStorage, Trie, TrieUpdate};
use near_primitives::errors::StorageError;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::TrieNodesCount;
use rand::seq::SliceRandom;
use rand::Rng;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::Arc;

/// TrieMemoryPartialStorage, but contains only the first n requested nodes.
pub struct IncompletePartialStorage {
    pub(crate) recorded_storage: HashMap<CryptoHash, Vec<u8>>,
    pub(crate) visited_nodes: RefCell<HashSet<CryptoHash>>,
    pub node_count_to_fail_after: usize,
}

impl IncompletePartialStorage {
    pub fn new(partial_storage: PartialStorage, nodes_count_to_fail_at: usize) -> Self {
        let recorded_storage =
            partial_storage.nodes.0.into_iter().map(|value| (hash(&value), value)).collect();
        Self {
            recorded_storage,
            visited_nodes: Default::default(),
            node_count_to_fail_after: nodes_count_to_fail_at,
        }
    }
}

impl TrieStorage for IncompletePartialStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
        let result = self
            .recorded_storage
            .get(hash)
            .map_or_else(|| Err(StorageError::TrieNodeMissing), |val| Ok(val.as_slice().into()));

        if result.is_ok() {
            self.visited_nodes.borrow_mut().insert(*hash);
        }

        if self.visited_nodes.borrow().len() > self.node_count_to_fail_after {
            Err(StorageError::TrieNodeMissing)
        } else {
            result
        }
    }

    fn as_partial_storage(&self) -> Option<&TrieMemoryPartialStorage> {
        // Make sure it's not called - it pretends to be PartialStorage but is not
        unimplemented!()
    }

    fn get_trie_nodes_count(&self) -> TrieNodesCount {
        unimplemented!();
    }
}

fn setup_storage<F, Out>(trie: Rc<Trie>, test: &mut F) -> (PartialStorage, Out)
where
    F: FnMut(Rc<Trie>) -> Result<Out, StorageError>,
    Out: PartialEq + Debug,
{
    let recording_trie = Rc::new(trie.recording_reads());
    let output = test(Rc::clone(&recording_trie)).expect("should not fail");
    (recording_trie.recorded_storage().unwrap(), output)
}

fn test_incomplete_storage<F, Out>(trie: Rc<Trie>, mut test: F)
where
    F: FnMut(Rc<Trie>) -> Result<Out, StorageError>,
    Out: PartialEq + Debug,
{
    let (storage, expected) = setup_storage(Rc::clone(&trie), &mut test);
    let size = storage.nodes.0.len();
    print!("Test touches {} nodes, expected result {:?}...", size, expected);
    for i in 0..(size + 1) {
        let storage = IncompletePartialStorage::new(storage.clone(), i);
        let new_trie =
            Trie { storage: Box::new(storage), root: trie.get_root().clone(), flat_state: None };
        let expected_result =
            if i < size { Err(&StorageError::TrieNodeMissing) } else { Ok(&expected) };
        assert_eq!(test(Rc::new(new_trie)).as_ref(), expected_result);
    }
    println!("Success");
}

#[test]
fn test_reads_with_incomplete_storage() {
    let mut rng = rand::thread_rng();
    for _ in 0..50 {
        let tries = create_tries_complex(1, 2);
        let shard_uid = ShardUId { version: 1, shard_id: 0 };
        let trie_changes = gen_changes(&mut rng, 20);
        let trie_changes = simplify_changes(&trie_changes);
        if trie_changes.is_empty() {
            continue;
        }
        let state_root =
            test_populate_trie(&tries, &Trie::EMPTY_ROOT, shard_uid, trie_changes.clone());
        let trie = Rc::new(tries.get_trie_for_shard(shard_uid, state_root));

        {
            let (key, _) = trie_changes.choose(&mut rng).unwrap();
            println!("Testing lookup {:?}", key);
            let lookup_test = |trie: Rc<Trie>| -> Result<_, StorageError> { trie.get(key) };
            test_incomplete_storage(Rc::clone(&trie), lookup_test);
        }
        {
            println!("Testing TrieIterator over whole trie");
            let trie_records = |trie: Rc<Trie>| -> Result<_, StorageError> {
                let iterator = trie.iter()?;
                iterator.collect::<Result<Vec<_>, _>>()
            };
            test_incomplete_storage(Rc::clone(&trie), trie_records);
        }
        {
            let (key, _) = trie_changes.choose(&mut rng).unwrap();
            let key_prefix = &key[0..rng.gen_range(0, key.len() + 1)];
            println!("Testing TrieUpdateIterator over prefix {:?}", key_prefix);
            let trie_update_keys = |trie: Rc<Trie>| -> Result<_, StorageError> {
                let trie_update = TrieUpdate::new(trie);
                let keys = trie_update.iter(key_prefix)?.collect::<Result<Vec<_>, _>>()?;
                Ok(keys)
            };
            test_incomplete_storage(Rc::clone(&trie), trie_update_keys);
        }
    }
}

#[cfg(test)]
mod nodes_counter_tests {
    use super::*;
    use crate::test_utils::create_tries;
    use crate::trie::nibble_slice::NibbleSlice;

    fn create_trie_key(nibbles: &[u8]) -> Vec<u8> {
        NibbleSlice::encode_nibbles(&nibbles, false).into_vec()
    }

    fn create_trie(items: &[(Vec<u8>, Option<Vec<u8>>)]) -> Rc<Trie> {
        let tries = create_tries();
        let shard_uid = ShardUId { version: 1, shard_id: 0 };
        let trie_changes = simplify_changes(&items);
        let state_root = test_populate_trie(&tries, &Trie::EMPTY_ROOT, shard_uid, trie_changes);
        let trie = tries.get_trie_for_shard(shard_uid, state_root);
        Rc::new(trie)
    }

    // Get values corresponding to keys one by one, returning vector of numbers of touched nodes for each `get`.
    fn get_touched_nodes_numbers(trie: Rc<Trie>, items: &[(Vec<u8>, Option<Vec<u8>>)]) -> Vec<u64> {
        items
            .iter()
            .map(|(key, value)| {
                let initial_count = trie.get_trie_nodes_count().db_reads;
                let got_value = trie.get(key).unwrap();
                assert_eq!(*value, got_value);
                trie.get_trie_nodes_count().db_reads - initial_count
            })
            .collect()
    }

    // Test nodes counter and trie cache size on the sample of trie items.
    #[test]
    fn test_count() {
        // For keys with nibbles [000, 011, 100], we expect 6 touched nodes to get value for the first key 000:
        // Extension -> Branch -> Branch -> Leaf plus retrieving the value by its hash. In total
        // there will be 9 distinct nodes, because 011 and 100 both add one Leaf and value.
        let trie_items = vec![
            (create_trie_key(&vec![0, 0, 0]), Some(vec![0])),
            (create_trie_key(&vec![0, 1, 1]), Some(vec![1])),
            (create_trie_key(&vec![1, 0, 0]), Some(vec![2])),
        ];
        let trie = create_trie(&trie_items);
        assert_eq!(get_touched_nodes_numbers(trie.clone(), &trie_items), vec![5, 5, 4]);

        let storage = trie.storage.as_caching_storage().unwrap();
        assert_eq!(storage.shard_cache.len(), 9);
    }

    // Check that same values are stored in the same trie node.
    #[test]
    fn test_repeated_values_count() {
        // For these keys there will be 5 nodes with distinct hashes, because each path looks like
        // Extension([0, 0]) -> Branch -> Leaf([48/49]) -> value.
        // TODO: explain the exact values in path items here
        let trie_items = vec![
            (create_trie_key(&vec![0, 0]), Some(vec![1])),
            (create_trie_key(&vec![1, 1]), Some(vec![1])),
        ];
        let trie = create_trie(&trie_items);
        assert_eq!(get_touched_nodes_numbers(trie.clone(), &trie_items), vec![4, 4]);

        let storage = trie.storage.as_caching_storage().unwrap();
        assert_eq!(storage.shard_cache.len(), 5);
    }
}

#[cfg(test)]
mod caching_storage_tests {
    use super::*;
    use crate::test_utils::{create_test_store, create_tries};
    use crate::trie::trie_storage::{TrieCache, TrieCachingStorage, TRIE_LIMIT_CACHED_VALUE_SIZE};
    use crate::trie::TrieRefcountChange;
    use crate::{Store, TrieChanges};
    use assert_matches::assert_matches;
    use near_primitives::hash::hash;
    use near_primitives::types::TrieCacheMode;

    fn create_store_with_values(values: &[Vec<u8>], shard_uid: ShardUId) -> Store {
        let tries = create_tries();
        let mut trie_changes = TrieChanges::empty(Trie::EMPTY_ROOT);
        trie_changes.insertions = values
            .iter()
            .map(|value| TrieRefcountChange {
                trie_node_or_value_hash: hash(value),
                trie_node_or_value: value.clone(),
                rc: std::num::NonZeroU32::new(1).unwrap(),
            })
            .collect();
        let (store_update, _) = tries.apply_all(&trie_changes, shard_uid);
        store_update.commit().unwrap();
        tries.get_store()
    }

    /// Put the item into the cache. Check that getting it from cache returns the correct value.
    #[test]
    fn test_retrieve() {
        let value = vec![1u8];
        let values = vec![value.clone()];
        let shard_uid = ShardUId::single_shard();
        let store = create_store_with_values(&values, shard_uid);
        let trie_cache = TrieCache::new(0, false);
        let trie_caching_storage =
            TrieCachingStorage::new(store, trie_cache.clone(), shard_uid, false);
        let key = hash(&value);
        assert_eq!(trie_cache.get(&key), None);

        for _ in 0..2 {
            let count_before = trie_caching_storage.get_trie_nodes_count();
            let result = trie_caching_storage.retrieve_raw_bytes(&key);
            let count_delta = trie_caching_storage.get_trie_nodes_count() - count_before;
            assert_eq!(result.unwrap().as_ref(), value);
            assert_eq!(count_delta.db_reads, 1);
            assert_eq!(count_delta.mem_reads, 0);
            assert_eq!(trie_cache.get(&key).unwrap().as_ref(), value);
        }
    }

    /// Check that if item is not present in a store, retrieval returns an error.
    #[test]
    fn test_retrieve_error() {
        let shard_uid = ShardUId::single_shard();
        let store = create_test_store();
        let trie_caching_storage =
            TrieCachingStorage::new(store, TrieCache::new(0, false), shard_uid, false);
        let value = vec![1u8];
        let key = hash(&value);

        let result = trie_caching_storage.retrieve_raw_bytes(&key);
        assert_matches!(result, Err(StorageError::StorageInconsistentState(_)));
    }

    /// Check that large values does not fall into shard cache, but fall into chunk cache.
    #[test]
    fn test_large_value() {
        let value = vec![1u8].repeat(TRIE_LIMIT_CACHED_VALUE_SIZE + 1);
        let values = vec![value.clone()];
        let shard_uid = ShardUId::single_shard();
        let store = create_store_with_values(&values, shard_uid);
        let trie_cache = TrieCache::new(0, false);
        let trie_caching_storage =
            TrieCachingStorage::new(store, trie_cache.clone(), shard_uid, false);
        let key = hash(&value);

        trie_caching_storage.set_mode(TrieCacheMode::CachingChunk);
        let _ = trie_caching_storage.retrieve_raw_bytes(&key);

        let count_before = trie_caching_storage.get_trie_nodes_count();
        let result = trie_caching_storage.retrieve_raw_bytes(&key);
        let count_delta = trie_caching_storage.get_trie_nodes_count() - count_before;
        assert_eq!(trie_cache.get(&key), None);
        assert_eq!(result.unwrap().as_ref(), value);
        assert_eq!(count_delta.db_reads, 0);
        assert_eq!(count_delta.mem_reads, 1);
    }

    /// Check that positions of item and costs of its retrieval are returned correctly.
    #[test]
    fn test_counter_with_caching() {
        let values = vec![vec![1u8]];
        let shard_uid = ShardUId::single_shard();
        let store = create_store_with_values(&values, shard_uid);
        let trie_cache = TrieCache::new(0, false);
        let trie_caching_storage =
            TrieCachingStorage::new(store, trie_cache.clone(), shard_uid, false);
        let value = &values[0];
        let key = hash(&value);

        // In the beginning, we are in the CachingShard mode and item is not present in cache.
        assert_eq!(trie_cache.get(&key), None);

        // Because we are in the CachingShard mode, item should be placed into shard cache.
        let result = trie_caching_storage.retrieve_raw_bytes(&key);
        assert_eq!(result.unwrap().as_ref(), value);

        // Move to CachingChunk mode. Retrieval should increment the counter, because it is the first time we accessed
        // item while caching chunk.
        trie_caching_storage.set_mode(TrieCacheMode::CachingChunk);
        let count_before = trie_caching_storage.get_trie_nodes_count();
        let result = trie_caching_storage.retrieve_raw_bytes(&key);
        let count_delta = trie_caching_storage.get_trie_nodes_count() - count_before;
        assert_eq!(result.unwrap().as_ref(), value);
        assert_eq!(count_delta.db_reads, 1);
        assert_eq!(count_delta.mem_reads, 0);

        // After previous retrieval, item must be copied to chunk cache. Retrieval shouldn't increment the counter.
        let count_before = trie_caching_storage.get_trie_nodes_count();
        let result = trie_caching_storage.retrieve_raw_bytes(&key);
        let count_delta = trie_caching_storage.get_trie_nodes_count() - count_before;
        assert_eq!(result.unwrap().as_ref(), value);
        assert_eq!(count_delta.db_reads, 0);
        assert_eq!(count_delta.mem_reads, 1);

        // Even if we switch to caching shard, retrieval shouldn't increment the counter. Chunk cache only grows and is
        // dropped only when trie caching storage is dropped.
        trie_caching_storage.set_mode(TrieCacheMode::CachingShard);
        let count_before = trie_caching_storage.get_trie_nodes_count();
        let result = trie_caching_storage.retrieve_raw_bytes(&key);
        let count_delta = trie_caching_storage.get_trie_nodes_count() - count_before;
        assert_eq!(result.unwrap().as_ref(), value);
        assert_eq!(count_delta.db_reads, 0);
        assert_eq!(count_delta.mem_reads, 1);
    }

    /// Check that if an item present in chunk cache gets evicted from the shard cache, it stays in the chunk cache.
    #[test]
    fn test_chunk_cache_presence() {
        let shard_cache_size = 5;
        let values: Vec<Vec<u8>> = (0..shard_cache_size as u8 + 1).map(|i| vec![i]).collect();
        let shard_uid = ShardUId::single_shard();
        let store = create_store_with_values(&values, shard_uid);
        let trie_cache = TrieCache::with_capacities(shard_cache_size, 0, false);
        let trie_caching_storage =
            TrieCachingStorage::new(store, trie_cache.clone(), shard_uid, false);

        let value = &values[0];
        let key = hash(&value);

        trie_caching_storage.set_mode(TrieCacheMode::CachingChunk);
        let result = trie_caching_storage.retrieve_raw_bytes(&key);
        assert_eq!(result.unwrap().as_ref(), value);

        trie_caching_storage.set_mode(TrieCacheMode::CachingShard);
        for value in values[1..].iter() {
            let result = trie_caching_storage.retrieve_raw_bytes(&hash(value));
            assert_eq!(result.unwrap().as_ref(), value);
        }

        // Check that the first element gets evicted, but the counter is not incremented.
        assert_eq!(trie_cache.get(&key), None);
        let count_before = trie_caching_storage.get_trie_nodes_count();
        let result = trie_caching_storage.retrieve_raw_bytes(&key);
        let count_delta = trie_caching_storage.get_trie_nodes_count() - count_before;
        assert_eq!(result.unwrap().as_ref(), value);
        assert_eq!(count_delta.db_reads, 0);
        assert_eq!(count_delta.mem_reads, 1);
    }
}

use crate::test_utils::{gen_changes, simplify_changes, test_populate_trie, TestTriesBuilder};
use crate::trie::trie_storage::{TrieMemoryPartialStorage, TrieStorage};
use crate::trie::TrieNodesCount;
use crate::{PartialStorage, Trie, TrieUpdate};
use assert_matches::assert_matches;
use near_primitives::challenge::PartialState;
use near_primitives::errors::{MissingTrieValueContext, StorageError};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::shard_layout::ShardUId;
use rand::seq::SliceRandom;
use rand::Rng;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::Arc;

/// TrieMemoryPartialStorage, but contains only the first n requested nodes.
pub struct IncompletePartialStorage {
    pub(crate) recorded_storage: HashMap<CryptoHash, Arc<[u8]>>,
    pub(crate) visited_nodes: RefCell<HashSet<CryptoHash>>,
    pub node_count_to_fail_after: usize,
}

impl IncompletePartialStorage {
    pub fn new(partial_storage: PartialStorage, nodes_count_to_fail_at: usize) -> Self {
        let PartialState::TrieValues(nodes) = partial_storage.nodes;
        let recorded_storage = nodes.into_iter().map(|value| (hash(&value), value)).collect();
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
            .cloned()
            .expect("Recorded storage is missing the given hash");

        self.visited_nodes.borrow_mut().insert(*hash);

        if self.visited_nodes.borrow().len() > self.node_count_to_fail_after {
            Err(StorageError::MissingTrieValue(
                MissingTrieValueContext::TrieMemoryPartialStorage,
                *hash,
            ))
        } else {
            Ok(result)
        }
    }

    fn as_partial_storage(&self) -> Option<&TrieMemoryPartialStorage> {
        // Make sure it's not called - it pretends to be PartialStorage but is not
        unimplemented!()
    }
}

fn setup_storage<F, Out>(trie: Trie, test: &mut F) -> (PartialStorage, Trie, Out)
where
    F: FnMut(Trie) -> Result<(Trie, Out), StorageError>,
    Out: PartialEq + Debug,
{
    let recording_trie = trie.recording_reads();
    let (recording_trie, output) = test(recording_trie).expect("should not fail");
    (recording_trie.recorded_storage().unwrap(), recording_trie, output)
}

fn test_incomplete_storage<F, Out>(trie: Trie, mut test: F)
where
    F: FnMut(Trie) -> Result<(Trie, Out), StorageError>,
    Out: PartialEq + Debug,
{
    let (storage, trie, expected) = setup_storage(trie, &mut test);
    let size = storage.nodes.len();
    println!("Test touches {} nodes, expected result {:?}...", size, expected);
    for i in 0..(size + 1) {
        let storage = IncompletePartialStorage::new(storage.clone(), i);
        let new_trie = Trie::new(Rc::new(storage), *trie.get_root(), None);
        let result = test(new_trie).map(|v| v.1);
        if i < size {
            assert_matches!(
                result,
                Err(StorageError::MissingTrieValue(
                    MissingTrieValueContext::TrieMemoryPartialStorage,
                    _
                ))
            );
        } else {
            assert_eq!(result.as_ref(), Ok(&expected));
        }
    }
    println!("Success");
}

#[test]
fn test_reads_with_incomplete_storage() {
    let mut rng = rand::thread_rng();
    for _ in 0..50 {
        let tries = TestTriesBuilder::new().with_shard_layout(1, 2).build();
        let shard_uid = ShardUId { version: 1, shard_id: 0 };
        let trie_changes = gen_changes(&mut rng, 20);
        let trie_changes = simplify_changes(&trie_changes);
        if trie_changes.is_empty() {
            continue;
        }
        let state_root =
            test_populate_trie(&tries, &Trie::EMPTY_ROOT, shard_uid, trie_changes.clone());
        let get_trie = || tries.get_trie_for_shard(shard_uid, state_root);

        {
            let (key, _) = trie_changes.choose(&mut rng).unwrap();
            println!("Testing lookup {:?}", key);
            let lookup_test =
                |trie: Trie| -> Result<_, StorageError> { trie.get(key).map(move |v| (trie, v)) };
            test_incomplete_storage(get_trie(), lookup_test);
        }
        {
            println!("Testing TrieIterator over whole trie");
            let trie_records = |trie: Trie| -> Result<_, StorageError> {
                let iterator = trie.iter()?;
                iterator.collect::<Result<Vec<_>, _>>().map(move |v| (trie, v))
            };
            test_incomplete_storage(get_trie(), trie_records);
        }
        {
            let (key, _) = trie_changes.choose(&mut rng).unwrap();
            let key_prefix = &key[0..rng.gen_range(0..key.len() + 1)];
            println!("Testing TrieUpdateIterator over prefix {:?}", key_prefix);
            let trie_update_keys = |trie: Trie| -> Result<_, StorageError> {
                let trie_update = TrieUpdate::new(trie);
                let keys = trie_update.iter(key_prefix)?.collect::<Result<Vec<_>, _>>()?;
                Ok((trie_update.trie, keys))
            };
            test_incomplete_storage(get_trie(), trie_update_keys);
        }
    }
}

#[cfg(test)]
mod nodes_counter_tests {
    use super::*;
    use crate::trie::nibble_slice::NibbleSlice;

    fn create_trie_key(nibbles: &[u8]) -> Vec<u8> {
        NibbleSlice::encode_nibbles(&nibbles, false).into_vec()
    }

    fn create_trie(items: &[(Vec<u8>, Option<Vec<u8>>)]) -> Rc<Trie> {
        let tries = TestTriesBuilder::new().build();
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
        // Extension -> Branch -> Branch -> Leaf plus retrieving the value by its hash.
        let trie_items = vec![
            (create_trie_key(&[0, 0, 0]), Some(vec![0])),
            (create_trie_key(&[0, 1, 1]), Some(vec![1])),
            (create_trie_key(&[1, 0, 0]), Some(vec![2])),
        ];
        let trie = create_trie(&trie_items);
        assert_eq!(get_touched_nodes_numbers(trie, &trie_items), vec![5, 5, 4]);
    }

    // Check that same values are stored in the same trie node.
    #[test]
    fn test_repeated_values_count() {
        // For these keys there will be 5 nodes with distinct hashes, because each path looks like
        // Extension([0, 0]) -> Branch -> Leaf([48/49]) -> value.
        // TODO: explain the exact values in path items here
        let trie_items = vec![
            (create_trie_key(&[0, 0]), Some(vec![1])),
            (create_trie_key(&[1, 1]), Some(vec![1])),
        ];
        let trie = create_trie(&trie_items);
        assert_eq!(get_touched_nodes_numbers(trie, &trie_items), vec![4, 4]);
    }
}

#[cfg(test)]
mod trie_storage_tests {
    use super::*;
    use crate::test_utils::create_test_store;
    use crate::trie::accounting_cache::TrieAccountingCache;
    use crate::trie::trie_storage::{TrieCache, TrieCachingStorage, TrieDBStorage};
    use crate::trie::TrieRefcountAddition;
    use crate::{DBCol, Store, TrieChanges, TrieConfig};
    use assert_matches::assert_matches;
    use near_o11y::testonly::init_test_logger;
    use near_primitives::congestion_info::CongestionInfo;
    use near_primitives::hash::hash;
    use near_primitives::shard_layout::get_block_shard_uid;
    use near_primitives::types::chunk_extra::ChunkExtra;
    use near_primitives::version::{ProtocolFeature, PROTOCOL_VERSION};

    fn create_store_with_values(values: &[Vec<u8>], shard_uid: ShardUId) -> Store {
        let tries = TestTriesBuilder::new().build();
        let mut trie_changes = TrieChanges::empty(Trie::EMPTY_ROOT);
        trie_changes.insertions = values
            .iter()
            .map(|value| TrieRefcountAddition {
                trie_node_or_value_hash: hash(value),
                trie_node_or_value: value.clone(),
                rc: std::num::NonZeroU32::new(1).unwrap(),
            })
            .collect();
        let mut store_update = tries.store_update();
        tries.apply_all(&trie_changes, shard_uid, &mut store_update);
        store_update.commit().unwrap();
        tries.get_store()
    }

    /// Put item into storage. Check that it is retrieved correctly.
    #[test]
    fn test_retrieve_db() {
        let value = vec![1u8];
        let values = vec![value.clone()];
        let shard_uid = ShardUId::single_shard();
        let store = create_store_with_values(&values, shard_uid);
        let trie_db_storage = TrieDBStorage::new(store, shard_uid);
        let key = hash(&value);
        assert_eq!(trie_db_storage.retrieve_raw_bytes(&key).unwrap().as_ref(), value);
        let wrong_key = hash(&[2]);
        assert_matches!(trie_db_storage.retrieve_raw_bytes(&wrong_key), Err(_));
    }

    /// Put item into storage. Check that getting it from cache returns the correct value.
    #[test]
    fn test_retrieve_caching() {
        let value = vec![1u8];
        let values = vec![value.clone()];
        let shard_uid = ShardUId::single_shard();
        let store = create_store_with_values(&values, shard_uid);
        let trie_cache = TrieCache::new(&TrieConfig::default(), shard_uid, false);
        let trie_caching_storage =
            TrieCachingStorage::new(store, trie_cache.clone(), shard_uid, false, None);
        let mut accounting_cache = TrieAccountingCache::new(None);
        let key = hash(&value);
        assert_eq!(trie_cache.get(&key), None);

        for _ in 0..2 {
            let count_before = accounting_cache.get_trie_nodes_count();
            let result =
                accounting_cache.retrieve_raw_bytes_with_accounting(&key, &trie_caching_storage);
            let count_delta =
                accounting_cache.get_trie_nodes_count().checked_sub(&count_before).unwrap();
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
        let trie_caching_storage = TrieCachingStorage::new(
            store,
            TrieCache::new(&TrieConfig::default(), shard_uid, false),
            shard_uid,
            false,
            None,
        );
        let value = vec![1u8];
        let key = hash(&value);

        let result = trie_caching_storage.retrieve_raw_bytes(&key);
        assert_matches!(result, Err(StorageError::MissingTrieValue(_, _)));
    }

    /// Check that large values does not fall into shard cache, but fall into accounting cache.
    #[test]
    fn test_large_value() {
        let value = [1u8].repeat(TrieConfig::max_cached_value_size() + 1);
        let values = vec![value.clone()];
        let shard_uid = ShardUId::single_shard();
        let store = create_store_with_values(&values, shard_uid);
        let trie_cache = TrieCache::new(&TrieConfig::default(), shard_uid, false);
        let trie_caching_storage =
            TrieCachingStorage::new(store, trie_cache.clone(), shard_uid, false, None);
        let mut accounting_cache = TrieAccountingCache::new(None);
        let key = hash(&value);

        accounting_cache.set_enabled(true);
        let _ = accounting_cache.retrieve_raw_bytes_with_accounting(&key, &trie_caching_storage);

        let count_before: TrieNodesCount = accounting_cache.get_trie_nodes_count();
        let result =
            accounting_cache.retrieve_raw_bytes_with_accounting(&key, &trie_caching_storage);
        let count_delta =
            accounting_cache.get_trie_nodes_count().checked_sub(&count_before).unwrap();
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
        let trie_cache = TrieCache::new(&TrieConfig::default(), shard_uid, false);
        let trie_caching_storage =
            TrieCachingStorage::new(store, trie_cache.clone(), shard_uid, false, None);
        let mut accounting_cache = TrieAccountingCache::new(None);
        let value = &values[0];
        let key = hash(&value);

        // In the beginning, we are in the CachingShard mode and item is not present in cache.
        assert_eq!(trie_cache.get(&key), None);

        // Because we are in the CachingShard mode, item should be placed into shard cache.
        let result = trie_caching_storage.retrieve_raw_bytes(&key);
        assert_eq!(result.unwrap().as_ref(), value);

        // Move to CachingChunk mode. Retrieval should increment the counter, because it is the first time we accessed
        // item while caching chunk.
        accounting_cache.set_enabled(true);
        let count_before = accounting_cache.get_trie_nodes_count();
        let result =
            accounting_cache.retrieve_raw_bytes_with_accounting(&key, &trie_caching_storage);
        let count_delta =
            accounting_cache.get_trie_nodes_count().checked_sub(&count_before).unwrap();
        assert_eq!(result.unwrap().as_ref(), value);
        assert_eq!(count_delta.db_reads, 1);
        assert_eq!(count_delta.mem_reads, 0);

        // After previous retrieval, item must be copied to accounting cache. Retrieval shouldn't increment the counter.
        let count_before = accounting_cache.get_trie_nodes_count();
        let result =
            accounting_cache.retrieve_raw_bytes_with_accounting(&key, &trie_caching_storage);
        let count_delta =
            accounting_cache.get_trie_nodes_count().checked_sub(&count_before).unwrap();
        assert_eq!(result.unwrap().as_ref(), value);
        assert_eq!(count_delta.db_reads, 0);
        assert_eq!(count_delta.mem_reads, 1);

        // Even if we switch to caching shard, retrieval shouldn't increment the counter. Accounting cache only grows and is
        // dropped only when trie caching storage is dropped.
        accounting_cache.set_enabled(true);
        let count_before = accounting_cache.get_trie_nodes_count();
        let result =
            accounting_cache.retrieve_raw_bytes_with_accounting(&key, &trie_caching_storage);
        let count_delta =
            accounting_cache.get_trie_nodes_count().checked_sub(&count_before).unwrap();
        assert_eq!(result.unwrap().as_ref(), value);
        assert_eq!(count_delta.db_reads, 0);
        assert_eq!(count_delta.mem_reads, 1);
    }

    /// Check that if an item present in accounting cache gets evicted from the shard cache,
    /// it stays in the accounting cache.
    #[test]
    fn test_accounting_cache_presence() {
        let shard_cache_size = 5;
        let values: Vec<Vec<u8>> = (0..shard_cache_size as u8 + 1).map(|i| vec![i]).collect();
        let shard_uid = ShardUId::single_shard();
        let store = create_store_with_values(&values, shard_uid);
        let mut trie_config = TrieConfig::default();
        trie_config
            .shard_cache_config
            .per_shard_max_bytes
            .insert(shard_uid, bytesize::ByteSize(shard_cache_size));
        let trie_cache = TrieCache::new(&trie_config, shard_uid, false);
        let trie_caching_storage =
            TrieCachingStorage::new(store, trie_cache.clone(), shard_uid, false, None);
        let mut accounting_cache = TrieAccountingCache::new(None);

        let value = &values[0];
        let key = hash(&value);

        accounting_cache.set_enabled(true);
        let result =
            accounting_cache.retrieve_raw_bytes_with_accounting(&key, &trie_caching_storage);
        assert_eq!(result.unwrap().as_ref(), value);

        accounting_cache.set_enabled(true);
        for value in values[1..].iter() {
            let result = accounting_cache
                .retrieve_raw_bytes_with_accounting(&hash(value), &trie_caching_storage);
            assert_eq!(result.unwrap().as_ref(), value);
        }

        // Check that the first element gets evicted, but the counter is not incremented.
        assert_eq!(trie_cache.get(&key), None);
        let count_before = accounting_cache.get_trie_nodes_count();
        let result =
            accounting_cache.retrieve_raw_bytes_with_accounting(&key, &trie_caching_storage);
        let count_delta =
            accounting_cache.get_trie_nodes_count().checked_sub(&count_before).unwrap();
        assert_eq!(result.unwrap().as_ref(), value);
        assert_eq!(count_delta.db_reads, 0);
        assert_eq!(count_delta.mem_reads, 1);
    }

    fn test_memtrie_and_disk_updates_consistency(updates: Vec<(Vec<u8>, Option<Vec<u8>>)>) {
        init_test_logger();
        let base_changes = vec![
            (vec![7], Some(vec![1])),
            (vec![7, 0], Some(vec![2])),
            (vec![7, 1], Some(vec![3])),
        ];
        let tries = TestTriesBuilder::new().build();
        let shard_uid = ShardUId::single_shard();

        let state_root =
            test_populate_trie(&tries, &Trie::EMPTY_ROOT, shard_uid, base_changes.clone());
        let trie = tries.get_trie_for_shard(shard_uid, state_root).recording_reads();
        let changes = trie.update(updates.clone()).unwrap();
        tracing::info!("Changes: {:?}", changes);

        let recorded_normal = trie.recorded_storage();

        let store = create_test_store();
        let congestion_info = ProtocolFeature::CongestionControl
            .enabled(PROTOCOL_VERSION)
            .then(CongestionInfo::default);
        // ChunkExtra is needed for in-memory trie loading code to query state roots.
        let chunk_extra = ChunkExtra::new(
            PROTOCOL_VERSION,
            &Trie::EMPTY_ROOT,
            CryptoHash::default(),
            Vec::new(),
            0,
            0,
            0,
            congestion_info,
        );
        let mut update_for_chunk_extra = store.store_update();
        update_for_chunk_extra
            .set_ser(
                DBCol::ChunkExtra,
                &get_block_shard_uid(&CryptoHash::default(), &shard_uid),
                &chunk_extra,
            )
            .unwrap();
        update_for_chunk_extra.commit().unwrap();

        let tries = TestTriesBuilder::new()
            .with_store(store)
            .with_flat_storage(true)
            .with_in_memory_tries()
            .build();
        let shard_uid = ShardUId::single_shard();

        let state_root = test_populate_trie(&tries, &Trie::EMPTY_ROOT, shard_uid, base_changes);
        let trie = tries.get_trie_for_shard(shard_uid, state_root).recording_reads();
        let changes = trie.update(updates).unwrap();

        tracing::info!("Changes: {:?}", changes);

        let recorded_memtrie = trie.recorded_storage();

        assert_eq!(recorded_normal, recorded_memtrie);
    }

    // Checks that when branch restructuring is triggered on updating trie,
    // impacted child is recorded on memtrie.
    //
    // Needed when branch has two children, one of which is removed, branch
    // could be converted to extension, so reading of the only remaining child
    // is also required.
    #[test]
    fn test_memtrie_recorded_branch_restructuring() {
        test_memtrie_and_disk_updates_consistency(vec![
            (vec![7], Some(vec![1])),
            (vec![7, 0], Some(vec![2])),
            (vec![7, 1], Some(vec![3])),
        ]);
    }

    // Checks that when non-existent key is removed, only nodes along the path
    // to it is recorded.
    // Needed because old disk trie logic was always reading neighbouring children
    // along the path to recompute memory usages, which is not needed if trie
    // structure doesn't change.
    #[test]
    fn test_memtrie_recorded_delete_non_existent_key() {
        test_memtrie_and_disk_updates_consistency(vec![(vec![8], None)]);
    }
}

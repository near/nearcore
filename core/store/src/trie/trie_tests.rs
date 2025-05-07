use crate::test_utils::{TestTriesBuilder, gen_changes, simplify_changes, test_populate_trie};
use crate::trie::AccessOptions;
use crate::trie::trie_storage::{TrieMemoryPartialStorage, TrieStorage};
use crate::{PartialStorage, Trie, TrieUpdate};
use assert_matches::assert_matches;
use near_primitives::errors::{MissingTrieValueContext, StorageError};
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::state::PartialState;
use parking_lot::RwLock;
use rand::Rng;
use rand::seq::SliceRandom;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;

/// TrieMemoryPartialStorage, but contains only the first n requested nodes.
pub struct IncompletePartialStorage {
    pub(crate) recorded_storage: HashMap<CryptoHash, Arc<[u8]>>,
    pub(crate) visited_nodes: RwLock<HashSet<CryptoHash>>,
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

        let mut lock = self.visited_nodes.write();
        lock.insert(*hash);

        if lock.len() > self.node_count_to_fail_after {
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
    let recording_trie = trie.recording_reads_new_recorder();
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
        let new_trie = Trie::new(Arc::new(storage), *trie.get_root(), None);
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
        let shard_layout = ShardLayout::multi_shard(2, 1);
        let shard_uid = shard_layout.shard_uids().next().unwrap();

        let tries = TestTriesBuilder::new().with_shard_layout(shard_layout).build();
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
            let lookup_test = |trie: Trie| -> Result<_, StorageError> {
                trie.get(key, AccessOptions::DEFAULT).map(move |v| (trie, v))
            };
            test_incomplete_storage(get_trie(), lookup_test);
        }
        {
            println!("Testing TrieIterator over whole trie");
            let trie_records = |trie: Trie| -> Result<_, StorageError> {
                let iterator = trie.disk_iter()?;
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
mod trie_storage_tests {
    use super::*;
    use crate::adapter::StoreAdapter;
    use crate::adapter::trie_store::TrieStoreAdapter;
    use crate::test_utils::create_test_store;
    use crate::trie::iterator::TrieIterator;
    use crate::trie::trie_storage::{TrieCache, TrieCachingStorage, TrieDBStorage};
    use crate::trie::{AccessOptions, TrieRefcountAddition};
    use crate::{TrieChanges, TrieConfig};
    use assert_matches::assert_matches;
    use near_o11y::testonly::init_test_logger;
    use near_primitives::hash::hash;

    fn create_store_with_values(values: &[Vec<u8>], shard_uid: ShardUId) -> TrieStoreAdapter {
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
        tries.store()
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
        let key = hash(&value);
        assert_eq!(trie_cache.get(&key), None);

        for _ in 0..2 {
            let result = trie_caching_storage.retrieve_raw_bytes(&key);
            assert_eq!(result.unwrap().as_ref(), value);
            assert_eq!(trie_cache.get(&key).unwrap().as_ref(), value);
        }
    }

    /// Check that if item is not present in a store, retrieval returns an error.
    #[test]
    fn test_retrieve_error() {
        let shard_uid = ShardUId::single_shard();
        let store = create_test_store();
        let trie_caching_storage = TrieCachingStorage::new(
            store.trie_store(),
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
        let trie = tries.get_trie_for_shard(shard_uid, state_root).recording_reads_new_recorder();
        let changes = trie.update(updates.clone(), AccessOptions::DEFAULT).unwrap();
        tracing::info!("Changes: {:?}", changes);

        let recorded_normal = trie.recorded_storage();

        let tries =
            TestTriesBuilder::new().with_flat_storage(true).with_in_memory_tries(true).build();
        let shard_uid = ShardUId::single_shard();

        let state_root = test_populate_trie(&tries, &Trie::EMPTY_ROOT, shard_uid, base_changes);
        let trie = tries.get_trie_for_shard(shard_uid, state_root).recording_reads_new_recorder();
        let changes = trie.update(updates, AccessOptions::DEFAULT).unwrap();

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
            (vec![7], Some(vec![10])),
            (vec![7, 0], None),
            (vec![7, 6], Some(vec![8])),
        ]);
    }

    // Checks that when non-existent key is removed, only nodes along the path
    // to it is recorded.
    // Needed because old disk trie logic was always reading neighboring children
    // along the path to recompute memory usages, which is not needed if trie
    // structure doesn't change.
    #[test]
    fn test_memtrie_recorded_delete_non_existent_key() {
        test_memtrie_and_disk_updates_consistency(vec![(vec![8], None)]);
    }

    #[test]
    fn test_memtrie_iteration_recording() {
        init_test_logger();

        let base_changes = vec![
            (vec![6], Some(vec![0])),
            (vec![7], Some(vec![1])),
            (vec![7, 0], Some(vec![2])),
            (vec![7, 1], Some(vec![3])),
            (vec![8], Some(vec![4])),
        ];

        let tries =
            TestTriesBuilder::new().with_flat_storage(true).with_in_memory_tries(true).build();
        let shard_uid = ShardUId::single_shard();

        let state_root = test_populate_trie(&tries, &Trie::EMPTY_ROOT, shard_uid, base_changes);

        let iter_prefix = vec![7];
        let expected_iter_results =
            vec![(vec![7], vec![1]), (vec![7, 0], vec![2]), (vec![7, 1], vec![3])];

        let disk_iter_recorded = {
            let trie =
                tries.get_trie_for_shard(shard_uid, state_root).recording_reads_new_recorder();
            let mut disk_iter = trie.disk_iter().unwrap();
            disk_iter.seek_prefix(&iter_prefix).unwrap();
            let disk_iter_results = disk_iter.collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(disk_iter_results, expected_iter_results);
            trie.recorded_storage().unwrap()
        };

        let memtrie_iter_recorded = {
            let trie =
                tries.get_trie_for_shard(shard_uid, state_root).recording_reads_new_recorder();
            let lock = trie.lock_for_iter();
            let mut memtrie_iter = lock.iter().unwrap();
            match memtrie_iter {
                TrieIterator::Disk(_) => {
                    panic!("Expected Memtrie iterator, got Disk iterator");
                }
                TrieIterator::Memtrie(_) => {}
            }
            memtrie_iter.seek_prefix(&iter_prefix).unwrap();
            let memtrie_iter_results = memtrie_iter.collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(memtrie_iter_results, expected_iter_results);
            trie.recorded_storage().unwrap()
        };

        assert_eq!(disk_iter_recorded, memtrie_iter_recorded);

        let partial_recorded = {
            let trie = Trie::from_recorded_storage(memtrie_iter_recorded, state_root, true)
                .recording_reads_new_recorder();
            let mut disk_iter = trie.disk_iter().unwrap();
            disk_iter.seek_prefix(&iter_prefix).unwrap();
            let disk_iter_results = disk_iter.collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(disk_iter_results, expected_iter_results);
            trie.recorded_storage().unwrap()
        };

        assert_eq!(disk_iter_recorded, partial_recorded);
    }
}

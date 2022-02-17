use crate::test_utils::{create_tries_complex, gen_changes, simplify_changes, test_populate_trie};
use crate::trie::trie_storage::{TrieMemoryPartialStorage, TrieStorage};
use crate::{PartialStorage, Trie, TrieUpdate};
use near_primitives::errors::StorageError;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::shard_layout::ShardUId;
use rand::seq::SliceRandom;
use rand::Rng;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::rc::Rc;

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
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Vec<u8>, StorageError> {
        let result = self
            .recorded_storage
            .get(hash)
            .map_or_else(|| Err(StorageError::TrieNodeMissing), |val| Ok(val.clone()));

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
        let trie = Trie { storage: Box::new(storage) };
        let expected_result =
            if i < size { Err(&StorageError::TrieNodeMissing) } else { Ok(&expected) };
        assert_eq!(test(Rc::new(trie)).as_ref(), expected_result);
    }
    println!("Success");
}

#[test]
fn test_reads_with_incomplete_storage() {
    let mut rng = rand::thread_rng();
    for _ in 0..50 {
        let tries = create_tries_complex(1, 2);
        let shard_uid = ShardUId { version: 1, shard_id: 0 };
        let trie = tries.get_trie_for_shard(shard_uid);
        let trie = Rc::new(trie);
        let mut state_root = Trie::empty_root();
        let trie_changes = gen_changes(&mut rng, 20);
        let trie_changes = simplify_changes(&trie_changes);
        if trie_changes.is_empty() {
            continue;
        }
        state_root = test_populate_trie(&tries, &state_root, shard_uid, trie_changes.clone());

        {
            let (key, _) = trie_changes.choose(&mut rng).unwrap();
            println!("Testing lookup {:?}", key);
            let lookup_test =
                |trie: Rc<Trie>| -> Result<_, StorageError> { trie.get(&state_root, key) };
            test_incomplete_storage(Rc::clone(&trie), lookup_test);
        }
        {
            println!("Testing TrieIterator over whole trie");
            let trie_records = |trie: Rc<Trie>| -> Result<_, StorageError> {
                let iterator = trie.iter(&state_root)?;
                iterator.collect::<Result<Vec<_>, _>>()
            };
            test_incomplete_storage(Rc::clone(&trie), trie_records);
        }
        {
            let (key, _) = trie_changes.choose(&mut rng).unwrap();
            let key_prefix = &key[0..rng.gen_range(0, key.len() + 1)];
            println!("Testing TrieUpdateIterator over prefix {:?}", key_prefix);
            let trie_update_keys = |trie: Rc<Trie>| -> Result<_, StorageError> {
                let trie_update = TrieUpdate::new(trie, state_root);
                let keys = trie_update.iter(key_prefix)?.collect::<Result<Vec<_>, _>>()?;
                Ok(keys)
            };
            test_incomplete_storage(Rc::clone(&trie), trie_update_keys);
        }
    }
}

#[cfg(test)]
mod trie_counter_tests {
    use super::*;
    use crate::test_utils::create_tries;
    use crate::trie::POISONED_LOCK_ERR;

    fn test_trie_items() -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        vec![
            (b"aaa".to_vec(), Some(vec![0])),
            (b"abb".to_vec(), Some(vec![1])),
            (b"baa".to_vec(), Some(vec![2])),
        ]
    }

    fn create_trie(items: &[(Vec<u8>, Option<Vec<u8>>)]) -> (Rc<Trie>, CryptoHash) {
        let tries = create_tries();
        let shard_uid = ShardUId { version: 1, shard_id: 0 };
        let trie = tries.get_trie_for_shard(shard_uid);
        let trie = Rc::new(trie);
        let state_root = Trie::empty_root();
        let trie_changes = simplify_changes(&items);
        let state_root = test_populate_trie(&tries, &state_root, shard_uid, trie_changes.clone());
        (trie, state_root)
    }

    // Helper for tests ensuring the correct behaviour of trie counter.
    // For example, on testing set of keys `[b"aaa", b"abb", b"baa"]`, we expect 6 touched nodes to get value for the
    // first key: Branch -> Extension -> Branch -> Extension -> Leaf plus retrieving the value by its hash.
    fn get_touched_nodes_numbers(
        trie: Rc<Trie>,
        state_root: CryptoHash,
        items: &[(Vec<u8>, Option<Vec<u8>>)],
    ) -> Vec<u64> {
        items
            .iter()
            .map(|(key, value)| {
                let initial_counter = trie.get_touched_nodes_count();
                let got_value = trie.get(&state_root, key).unwrap();
                assert_eq!(*value, got_value);
                trie.get_touched_nodes_count() - initial_counter
            })
            .collect()
    }

    #[test]
    fn count_touched_nodes() {
        let trie_items = test_trie_items();
        let (trie, state_root) = create_trie(&trie_items);
        assert_eq!(get_touched_nodes_numbers(trie.clone(), state_root, &trie_items), vec![6, 6, 4]);

        #[cfg(not(feature = "no_cache"))]
        {
            let storage = trie.storage.as_caching_storage().unwrap();
            let guard = storage.cache.0.lock().expect(POISONED_LOCK_ERR);
            assert_eq!(guard.len(), 14);
        }
    }
}

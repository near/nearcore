use std::cell::RefCell;
use std::sync::Arc;

use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;

use super::mem::iter::STMemTrieIterator;
use super::ops::interface::GenericTrieInternalStorage;
use super::ops::iter::{TrieItem, TrieIteratorImpl};
use super::trie_storage_update::{TrieStorageNode, TrieStorageNodePtr};
use super::{AccessOptions, Trie, ValueHandle};

pub struct DiskTrieIteratorInner<'a> {
    trie: &'a Trie,
    /// If not `None`, a list of all nodes that the iterator has visited.
    /// This is used only for TrieViewer.
    /// TODO: Remove this once we shift to using recorded storage in trie iterator.
    visited_nodes: Option<RefCell<Vec<Arc<[u8]>>>>,
}

impl<'a> DiskTrieIteratorInner<'a> {
    pub fn new(trie: &'a Trie) -> Self {
        Self { trie, visited_nodes: None }
    }

    pub fn remember_visited_nodes(&mut self, record_nodes: bool) {
        self.visited_nodes = record_nodes.then(|| RefCell::new(Vec::new()))
    }

    pub fn into_visited_nodes(self) -> Vec<Arc<[u8]>> {
        self.visited_nodes.map(|n| n.into_inner()).unwrap_or_default()
    }
}

impl<'a> GenericTrieInternalStorage<TrieStorageNodePtr, ValueHandle> for DiskTrieIteratorInner<'a> {
    fn get_root(&self) -> Option<TrieStorageNodePtr> {
        if self.trie.root == CryptoHash::default() {
            return None;
        }
        Some(self.trie.root)
    }

    fn get_and_record_node(
        &self,
        ptr: TrieStorageNodePtr,
    ) -> Result<TrieStorageNode, StorageError> {
        let node = self.trie.retrieve_raw_node(&ptr, true, AccessOptions::DEFAULT)?.map(
            |(bytes, node)| {
                if let Some(ref visited_nodes) = self.visited_nodes {
                    visited_nodes.borrow_mut().push(bytes);
                }
                TrieStorageNode::from_raw_trie_node(node.node)
            },
        );
        Ok(node.unwrap_or_default())
    }

    fn get_and_record_value(&self, value_ref: ValueHandle) -> Result<Vec<u8>, StorageError> {
        match value_ref {
            ValueHandle::HashAndSize(value) => {
                self.trie.retrieve_value(&value.hash, AccessOptions::DEFAULT)
            }
            ValueHandle::InMemory(value) => panic!("Unexpected in-memory value: {:?}", value),
        }
    }
}

pub(crate) type DiskTrieIterator<'a> =
    TrieIteratorImpl<TrieStorageNodePtr, ValueHandle, DiskTrieIteratorInner<'a>>;

pub enum TrieIterator<'a> {
    Disk(DiskTrieIterator<'a>),
    Memtrie(STMemTrieIterator<'a>),
}

impl<'a> Iterator for TrieIterator<'a> {
    type Item = Result<TrieItem, StorageError>;

    fn next(&mut self) -> Option<Result<TrieItem, StorageError>> {
        match self {
            TrieIterator::Disk(iter) => iter.next(),
            TrieIterator::Memtrie(iter) => iter.next(),
        }
    }
}

impl<'a> TrieIterator<'a> {
    pub fn seek_prefix<K: AsRef<[u8]>>(&mut self, key: K) -> Result<(), StorageError> {
        match self {
            TrieIterator::Disk(iter) => iter.seek_prefix(key),
            TrieIterator::Memtrie(iter) => iter.seek_prefix(key),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Trie;
    use crate::test_utils::{TestTriesBuilder, gen_changes, simplify_changes, test_populate_trie};
    use crate::trie::iterator::TrieIterator;
    use crate::trie::nibble_slice::NibbleSlice;
    use itertools::Itertools;
    use near_primitives::shard_layout::{ShardLayout, ShardUId};
    use rand::Rng;
    use rand::seq::SliceRandom;
    use std::collections::BTreeMap;

    fn value() -> Option<Vec<u8>> {
        Some(vec![0])
    }

    /// Checks that for visiting interval of trie nodes first state key is
    /// included and the last one is excluded.
    #[test]
    fn test_visit_interval() {
        let trie_changes = vec![(b"aa".to_vec(), Some(vec![1])), (b"abb".to_vec(), Some(vec![2]))];
        let tries = TestTriesBuilder::new().build();
        let state_root =
            test_populate_trie(&tries, &Trie::EMPTY_ROOT, ShardUId::single_shard(), trie_changes);
        let trie = tries.get_trie_for_shard(ShardUId::single_shard(), state_root);
        let path_begin: Vec<_> = NibbleSlice::new(b"aa").iter().collect();
        let path_end: Vec<_> = NibbleSlice::new(b"abb").iter().collect();
        let mut trie_iter = trie.disk_iter().unwrap();
        let items = trie_iter.visit_nodes_interval(&path_begin, &path_end).unwrap();
        let trie_items: Vec<_> = items.into_iter().map(|item| item.key).flatten().collect();
        assert_eq!(trie_items, vec![b"aa"]);
    }

    fn test_iterator(use_memtries: bool) {
        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            let (trie_changes, map, trie) = gen_random_trie(&mut rng, use_memtries);

            {
                let lock = trie.lock_for_iter();
                let iter = lock.iter().unwrap();
                if use_memtries {
                    assert!(matches!(iter, TrieIterator::Memtrie(_)));
                } else {
                    assert!(matches!(iter, TrieIterator::Disk(_)));
                }
                let result1: Vec<_> = iter.map(Result::unwrap).collect();
                let result2: Vec<_> = map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                assert_eq!(result1, result2);
            }
            test_seek_prefix(&trie, &map, &[], use_memtries);

            for (seek_key, _) in &trie_changes {
                test_seek_prefix(&trie, &map, seek_key, use_memtries);
            }
            for _ in 0..20 {
                let alphabet = &b"abcdefgh"[0..rng.gen_range(2..8)];
                let key_length = rng.gen_range(1..8);
                let seek_key: Vec<u8> =
                    (0..key_length).map(|_| *alphabet.choose(&mut rng).unwrap()).collect();
                test_seek_prefix(&trie, &map, &seek_key, use_memtries);
            }
        }
    }

    #[test]
    fn test_disk_iterator() {
        test_iterator(false);
    }

    #[test]
    fn test_memtrie_iterator() {
        test_iterator(true);
    }

    #[test]
    fn test_iterator_with_prune_condition_base() {
        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            let (trie_changes, map, trie) = gen_random_trie(&mut rng, false);

            // Check that pruning just one key (and it's subtree) works as expected.
            for (prune_key, _) in &trie_changes {
                let prune_key = prune_key.clone();
                let prune_key_nibbles = NibbleSlice::new(prune_key.as_slice()).iter().collect_vec();
                let prune_condition =
                    move |key_nibbles: &Vec<u8>| key_nibbles.starts_with(&prune_key_nibbles);

                let result1 = trie
                    .disk_iter_with_prune_condition(Some(Box::new(prune_condition.clone())))
                    .unwrap()
                    .map(Result::unwrap)
                    .collect_vec();

                let result2 = map
                    .iter()
                    .filter(|(key, _)| {
                        !prune_condition(&NibbleSlice::new(key).iter().collect_vec())
                    })
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect_vec();

                assert_eq!(result1, result2);
            }
        }
    }

    // Check that pruning a node doesn't descend into it's subtree.
    // A buggy pruning implementation could still iterate over all the
    // nodes but simply not return them. This test makes sure this is
    // not the case.
    #[test]
    fn test_iterator_with_prune_condition_subtree() {
        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            let (trie_changes, map, trie) = gen_random_trie(&mut rng, false);

            // Test pruning by all keys that are present in the trie.
            for (prune_key, _) in &trie_changes {
                // This prune condition is not valid in a sense that it only
                // prunes a single node but not it's subtree. This is
                // intentional to test that iterator won't descend into the
                // subtree.
                let prune_key_nibbles = NibbleSlice::new(prune_key.as_slice()).iter().collect_vec();
                let prune_condition =
                    move |key_nibbles: &Vec<u8>| key_nibbles == &prune_key_nibbles;
                // This is how the prune condition should work.
                let prune_key_nibbles = NibbleSlice::new(prune_key.as_slice()).iter().collect_vec();
                let proper_prune_condition =
                    move |key_nibbles: &Vec<u8>| key_nibbles.starts_with(&prune_key_nibbles);

                let result1 = trie
                    .disk_iter_with_prune_condition(Some(Box::new(prune_condition.clone())))
                    .unwrap()
                    .map(Result::unwrap)
                    .collect_vec();
                let result2 = map
                    .iter()
                    .filter(|(key, _)| {
                        !proper_prune_condition(&NibbleSlice::new(key).iter().collect_vec())
                    })
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect_vec();

                assert_eq!(result1, result2);
            }
        }
    }

    // Utility function for testing trie iteration with the prune condition set.
    // * `keys` is a list of keys to be inserted into the trie
    // * `pruned_keys` is the expected list of keys that should be the result of iteration
    fn test_prune_max_depth_impl(
        keys: &Vec<Vec<u8>>,
        pruned_keys: &Vec<Vec<u8>>,
        max_depth: usize,
    ) {
        let shard_uid = ShardUId::single_shard();
        let tries = TestTriesBuilder::new().build();
        let trie_changes = keys.iter().map(|key| (key.clone(), value())).collect();
        let state_root = test_populate_trie(&tries, &Trie::EMPTY_ROOT, shard_uid, trie_changes);
        let trie = tries.get_trie_for_shard(shard_uid, state_root);
        let iter = trie.disk_iter_with_max_depth(max_depth).unwrap();
        let keys: Vec<_> = iter.map(|item| item.unwrap().0).collect();

        assert_eq!(&keys, pruned_keys);
    }

    #[test]
    fn test_prune_max_depth() {
        // simple trie with an extension
        //     extension(11111)
        //      branch(5, 6)
        //    leaf(5)  leaf(6)
        let extension_keys = vec![vec![0x11, 0x11, 0x15], vec![0x11, 0x11, 0x16]];
        // max_depth is expressed in nibbles
        // both leaf nodes are at depth 6 (11 11 15) and (11 11 16)

        // pruning by max depth 5 should return an empty result
        test_prune_max_depth_impl(&extension_keys, &vec![], 5);
        // pruning by max depth 6 should return both leaves
        test_prune_max_depth_impl(&extension_keys, &extension_keys, 6);

        // long chain of branches
        let chain_keys = vec![
            vec![0x11],
            vec![0x11, 0x11],
            vec![0x11, 0x11, 0x11],
            vec![0x11, 0x11, 0x11, 0x11],
            vec![0x11, 0x11, 0x11, 0x11, 0x11],
        ];
        test_prune_max_depth_impl(&chain_keys, &vec![], 1);
        test_prune_max_depth_impl(&chain_keys, &vec![vec![0x11]], 2);
        test_prune_max_depth_impl(&chain_keys, &vec![vec![0x11]], 3);
        test_prune_max_depth_impl(&chain_keys, &vec![vec![0x11], vec![0x11, 0x11]], 4);
        test_prune_max_depth_impl(&chain_keys, &vec![vec![0x11], vec![0x11, 0x11]], 5);
    }

    fn gen_random_trie(
        rng: &mut rand::rngs::ThreadRng,
        use_memtries: bool,
    ) -> (Vec<(Vec<u8>, Option<Vec<u8>>)>, BTreeMap<Vec<u8>, Vec<u8>>, Trie) {
        let shard_layout = ShardLayout::multi_shard(2, 1);
        let shard_uid = shard_layout.shard_uids().next().unwrap();
        let tries = TestTriesBuilder::new()
            .with_shard_layout(shard_layout)
            .with_flat_storage(use_memtries)
            .with_in_memory_tries(use_memtries)
            .build();
        let trie_changes = gen_changes(rng, 10);
        let trie_changes = simplify_changes(&trie_changes);

        let mut map = BTreeMap::new();
        for (key, value) in &trie_changes {
            if let Some(value) = value {
                map.insert(key.clone(), value.clone());
            }
        }
        let state_root =
            test_populate_trie(&tries, &Trie::EMPTY_ROOT, shard_uid, trie_changes.clone());
        let trie = tries.get_trie_for_shard(shard_uid, state_root);
        (trie_changes, map, trie)
    }

    fn test_seek_prefix(
        trie: &Trie,
        map: &BTreeMap<Vec<u8>, Vec<u8>>,
        seek_key: &[u8],
        is_memtrie: bool,
    ) {
        let lock = trie.lock_for_iter();
        let mut iterator = lock.iter().unwrap();
        if is_memtrie {
            assert!(matches!(iterator, TrieIterator::Memtrie(_)));
        } else {
            assert!(matches!(iterator, TrieIterator::Disk(_)));
        }
        iterator.seek_prefix(&seek_key).unwrap();
        let mut got = Vec::with_capacity(5);
        for item in iterator {
            let (key, value) = item.unwrap();
            assert!(key.starts_with(seek_key), "‘{key:x?}’ does not start with ‘{seek_key:x?}’");
            if got.len() < 5 {
                got.push((key, value));
            }
        }
        let want: Vec<_> = map
            .range(seek_key.to_vec()..)
            .map(|(k, v)| (k.clone(), v.clone()))
            .take(5)
            .filter(|(x, _)| x.starts_with(seek_key))
            .collect();
        assert_eq!(got, want);
    }
}

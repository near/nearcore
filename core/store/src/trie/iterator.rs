use near_primitives::hash::CryptoHash;

use crate::trie::nibble_slice::NibbleSlice;
use crate::trie::{TrieNode, TrieNodeWithSize, ValueHandle};
use crate::{StorageError, Trie};

/// Crumb is a piece of trie iteration state. It describes a node on the trail and processing status of that node.
#[derive(Debug)]
struct Crumb {
    node: TrieNodeWithSize,
    status: CrumbStatus,
    prefix_boundary: bool,
}

/// The status of processing of a node during trie iteration.
/// Each node is processed in the following order:
/// Entering -> At -> AtChild(0) -> ... -> AtChild(15) -> Exiting
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub(crate) enum CrumbStatus {
    Entering,
    At,
    AtChild(u8),
    Exiting,
}

impl Crumb {
    fn increment(&mut self) {
        if self.prefix_boundary {
            self.status = CrumbStatus::Exiting;
            return;
        }
        self.status = match (&self.status, &self.node.node) {
            (_, &TrieNode::Empty) => CrumbStatus::Exiting,
            (&CrumbStatus::Entering, _) => CrumbStatus::At,
            (&CrumbStatus::At, &TrieNode::Branch(_, _)) => CrumbStatus::AtChild(0),
            (&CrumbStatus::AtChild(x), &TrieNode::Branch(_, _)) if x < 15 => {
                CrumbStatus::AtChild(x + 1)
            }
            _ => CrumbStatus::Exiting,
        }
    }
}

/// Trie iteration is done using a stack based approach.
/// There are two stacks that we track while iterating: the trail and the key_nibbles.
/// The trail is a vector of trie nodes on the path from root node to the node that is
/// currently being processed together with processing status - the Crumb.
/// The key_nibbles is a vector of nibbles from the state root node to the node that is
/// currently being processed.
/// The trail and the key_nibbles may have different lengths e.g. an extension trie node
/// will add only a single item to the trail but may add multiple nibbles to the key_nibbles.
pub struct TrieIterator<'a> {
    trie: &'a Trie,
    trail: Vec<Crumb>,
    pub(crate) key_nibbles: Vec<u8>,

    /// If not `None`, a list of all nodes that the iterator has visited.
    visited_nodes: Option<Vec<std::sync::Arc<[u8]>>>,

    /// Prune condition is an optional closure that given the key nibbles
    /// decides if the given trie node should be pruned.
    ///
    /// If the prune conditions returns true for a given node, this node and the
    /// whole sub-tree rooted at this node will be pruned and skipped in iteration.
    ///
    /// Please note that since the iterator supports seeking the prune condition
    /// should have the property that if a prefix of a key should be pruned then
    /// the key also should be pruned. Otherwise it would be possible to bypass
    /// the pruning by seeking inside of the pruned sub-tree.
    prune_condition: Option<Box<dyn Fn(&Vec<u8>) -> bool>>,
}

/// The TrieTiem is a tuple of (key, value) of the node.
pub type TrieItem = (Vec<u8>, Vec<u8>);

/// Item extracted from Trie during depth first traversal, corresponding to some Trie node.
#[derive(Debug)]
pub struct TrieTraversalItem {
    /// Hash of the node.
    pub hash: CryptoHash,
    /// Key of the node if it stores a value.
    pub key: Option<Vec<u8>>,
}

impl<'a> TrieIterator<'a> {
    #![allow(clippy::new_ret_no_self)]
    /// Create a new iterator.
    pub(super) fn new(
        trie: &'a Trie,
        prune_condition: Option<Box<dyn Fn(&Vec<u8>) -> bool>>,
    ) -> Result<Self, StorageError> {
        let mut r = TrieIterator {
            trie,
            trail: Vec::with_capacity(8),
            key_nibbles: Vec::with_capacity(64),
            visited_nodes: None,
            prune_condition,
        };
        r.descend_into_node(&trie.root)?;
        Ok(r)
    }

    /// Position the iterator on the first element with key >= `key`.
    pub fn seek_prefix<K: AsRef<[u8]>>(&mut self, key: K) -> Result<(), StorageError> {
        self.seek_nibble_slice(NibbleSlice::new(key.as_ref()), true).map(drop)
    }

    /// Configures whether the iterator should remember all the nodes its
    /// visiting.
    ///
    /// Use [`Self::into_visited_nodes`] to retrieve the list.
    pub fn remember_visited_nodes(&mut self, remember: bool) {
        self.visited_nodes = remember.then(|| Vec::new());
    }

    /// Consumes iterator and returns list of nodes it’s visited.
    ///
    /// By default the iterator *doesn’t* remember nodes it visits.  To enable
    /// that feature use [`Self::remember_visited_nodes`] method.  If the
    /// feature is disabled, this method returns an empty list.  Otherwise
    /// it returns list of nodes visited since the feature was enabled.
    pub fn into_visited_nodes(self) -> Vec<std::sync::Arc<[u8]>> {
        self.visited_nodes.unwrap_or(Vec::new())
    }

    /// Returns the hash of the last node
    pub(crate) fn seek_nibble_slice(
        &mut self,
        mut key: NibbleSlice<'_>,
        is_prefix_seek: bool,
    ) -> Result<CryptoHash, StorageError> {
        self.trail.clear();
        self.key_nibbles.clear();
        // Checks if a key in an extension or leaf matches our search query.
        //
        // When doing prefix seek, this checks whether `key` is a prefix of
        // `ext_key`.  When doing regular range seek, this checks whether `key`
        // is no greater than `ext_key`.  If those conditions aren’t met, the
        // node with `ext_key` should not match our query.
        let check_ext_key = |key: &NibbleSlice, ext_key: &NibbleSlice| {
            if is_prefix_seek {
                ext_key.starts_with(key)
            } else {
                ext_key >= key
            }
        };

        let mut hash = self.trie.root;
        let mut prev_prefix_boundary = &mut false;
        loop {
            *prev_prefix_boundary = is_prefix_seek;
            self.descend_into_node(&hash)?;
            let Crumb { status, node, prefix_boundary } = self.trail.last_mut().unwrap();
            prev_prefix_boundary = prefix_boundary;
            match &node.node {
                TrieNode::Empty => break,
                TrieNode::Leaf(leaf_key, _) => {
                    let existing_key = NibbleSlice::from_encoded(leaf_key).0;
                    if !check_ext_key(&key, &existing_key) {
                        self.key_nibbles.extend(existing_key.iter());
                        *status = CrumbStatus::Exiting;
                    }
                    break;
                }
                TrieNode::Branch(children, _) => {
                    if key.is_empty() {
                        break;
                    }
                    let idx = key.at(0);
                    self.key_nibbles.push(idx);
                    *status = CrumbStatus::AtChild(idx);
                    if let Some(ref child) = children[idx] {
                        hash = *child.unwrap_hash();
                        key = key.mid(1);
                    } else {
                        *prefix_boundary = is_prefix_seek;
                        break;
                    }
                }
                TrieNode::Extension(ext_key, child) => {
                    let existing_key = NibbleSlice::from_encoded(ext_key).0;
                    if key.starts_with(&existing_key) {
                        key = key.mid(existing_key.len());
                        hash = *child.unwrap_hash();
                        *status = CrumbStatus::At;
                        self.key_nibbles.extend(existing_key.iter());
                    } else {
                        if !check_ext_key(&key, &existing_key) {
                            *status = CrumbStatus::Exiting;
                            self.key_nibbles.extend(existing_key.iter());
                        }
                        break;
                    }
                }
            }
        }
        Ok(hash)
    }

    /// Fetches block by its hash and adds it to the trail.
    ///
    /// The node is stored as the last [`Crumb`] in the trail.  If iterator is
    /// configured to remember all the nodes its visiting (which can be enabled
    /// with [`Self::remember_visited_nodes`]), the node will be added to the
    /// list.
    fn descend_into_node(&mut self, hash: &CryptoHash) -> Result<(), StorageError> {
        let (bytes, node) = self.trie.retrieve_node(hash)?;
        if let Some(ref mut visited) = self.visited_nodes {
            visited.push(bytes.ok_or(StorageError::MissingTrieValue)?);
        }
        self.trail.push(Crumb { status: CrumbStatus::Entering, node, prefix_boundary: false });
        Ok(())
    }

    fn key(&self) -> Vec<u8> {
        let mut result = <Vec<u8>>::with_capacity(self.key_nibbles.len() / 2);
        for i in (1..self.key_nibbles.len()).step_by(2) {
            result.push(self.key_nibbles[i - 1] * 16 + self.key_nibbles[i]);
        }
        result
    }

    fn has_value(&self) -> bool {
        match self.trail.last() {
            Some(b) => match &b.status {
                CrumbStatus::At => b.node.node.has_value(),
                _ => false,
            },
            None => false, // Trail finished
        }
    }

    fn iter_step(&mut self) -> Option<IterStep> {
        let last = self.trail.last_mut()?;
        last.increment();
        Some(match (last.status, &last.node.node) {
            (CrumbStatus::Exiting, n) => {
                match n {
                    TrieNode::Leaf(ref key, _) | TrieNode::Extension(ref key, _) => {
                        let existing_key = NibbleSlice::from_encoded(key).0;
                        let l = self.key_nibbles.len();
                        self.key_nibbles.truncate(l - existing_key.len());
                    }
                    TrieNode::Branch(_, _) => {
                        self.key_nibbles.pop();
                    }
                    _ => {}
                }
                IterStep::PopTrail
            }
            (CrumbStatus::At, TrieNode::Branch(_, Some(value))) => {
                let hash = match value {
                    ValueHandle::HashAndSize(value) => value.hash,
                    ValueHandle::InMemory(_node) => unreachable!(),
                };
                IterStep::Value(hash)
            }
            (CrumbStatus::At, TrieNode::Branch(_, None)) => IterStep::Continue,
            (CrumbStatus::At, TrieNode::Leaf(key, value)) => {
                let hash = match value {
                    ValueHandle::HashAndSize(value) => value.hash,
                    ValueHandle::InMemory(_node) => unreachable!(),
                };
                let key = NibbleSlice::from_encoded(key).0;
                self.key_nibbles.extend(key.iter());
                IterStep::Value(hash)
            }
            (CrumbStatus::At, TrieNode::Extension(key, child)) => {
                let hash = *child.unwrap_hash();
                let key = NibbleSlice::from_encoded(key).0;
                self.key_nibbles.extend(key.iter());
                IterStep::Descend(hash)
            }
            (CrumbStatus::AtChild(i), TrieNode::Branch(children, _)) => {
                if i == 0 {
                    self.key_nibbles.push(0);
                }
                if let Some(ref child) = children[i] {
                    if i != 0 {
                        *self.key_nibbles.last_mut().expect("Pushed child value before") = i;
                    }
                    IterStep::Descend(*child.unwrap_hash())
                } else {
                    IterStep::Continue
                }
            }
            _ => panic!("Should never see Entering or AtChild without a Branch here."),
        })
    }

    fn common_prefix(str1: &[u8], str2: &[u8]) -> usize {
        let mut prefix = 0;
        while prefix < str1.len() && prefix < str2.len() && str1[prefix] == str2[prefix] {
            prefix += 1;
        }
        prefix
    }

    /// Note that path_begin and path_end are not bytes, they are nibbles
    /// Visits all nodes belonging to the interval [path_begin, path_end) in depth-first search
    /// order and return key-value pairs for each visited node with value stored
    /// Used to generate split states for re-sharding
    pub(crate) fn get_trie_items(
        &mut self,
        path_begin: &[u8],
        path_end: &[u8],
    ) -> Result<Vec<TrieItem>, StorageError> {
        let path_begin_encoded = NibbleSlice::encode_nibbles(path_begin, false);
        self.seek_nibble_slice(NibbleSlice::from_encoded(&path_begin_encoded).0, false)?;

        let mut trie_items = vec![];
        for item in self {
            let trie_item = item?;
            let key_encoded: Vec<_> = NibbleSlice::new(&trie_item.0).iter().collect();
            if &key_encoded[..] >= path_end {
                return Ok(trie_items);
            }
            trie_items.push(trie_item);
        }
        Ok(trie_items)
    }

    /// Visits all nodes belonging to the interval [path_begin, path_end) in depth-first search
    /// order and return TrieTraversalItem for each visited node.
    /// Used to generate and apply state parts for state sync.
    pub fn visit_nodes_interval(
        &mut self,
        path_begin: &[u8],
        path_end: &[u8],
    ) -> Result<Vec<TrieTraversalItem>, StorageError> {
        let _span = tracing::debug_span!(
            target: "runtime",
            "visit_nodes_interval")
        .entered();
        let path_begin_encoded = NibbleSlice::encode_nibbles(path_begin, true);
        let last_hash =
            self.seek_nibble_slice(NibbleSlice::from_encoded(&path_begin_encoded).0, false)?;
        let mut prefix = Self::common_prefix(path_end, &self.key_nibbles);
        if self.key_nibbles[prefix..] >= path_end[prefix..] {
            return Ok(vec![]);
        }
        let mut nodes_list = Vec::new();

        // Actually (self.key_nibbles[..] == path_begin) always because path_begin always ends in a node
        if &self.key_nibbles[..] >= path_begin {
            nodes_list.push(TrieTraversalItem {
                hash: last_hash,
                key: self.has_value().then(|| self.key()),
            });
        }

        loop {
            let iter_step = match self.iter_step() {
                Some(iter_step) => iter_step,
                None => break,
            };
            match iter_step {
                IterStep::PopTrail => {
                    self.trail.pop();
                    prefix = std::cmp::min(self.key_nibbles.len(), prefix);
                }
                IterStep::Descend(hash) => {
                    prefix += Self::common_prefix(&path_end[prefix..], &self.key_nibbles[prefix..]);
                    if self.key_nibbles[prefix..] >= path_end[prefix..] {
                        break;
                    }
                    self.descend_into_node(&hash)?;
                    nodes_list.push(TrieTraversalItem { hash, key: None });
                }
                IterStep::Continue => {}
                IterStep::Value(hash) => {
                    if self.key_nibbles[prefix..] >= path_end[prefix..] {
                        break;
                    }
                    self.trie.storage.retrieve_raw_bytes(&hash)?;
                    nodes_list.push(TrieTraversalItem {
                        hash,
                        key: self.has_value().then(|| self.key()),
                    });
                }
            }
        }
        Ok(nodes_list)
    }
}

#[derive(Debug)]
enum IterStep {
    Continue,
    PopTrail,
    Descend(CryptoHash),
    Value(CryptoHash),
}

impl<'a> Iterator for TrieIterator<'a> {
    type Item = Result<TrieItem, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let iter_step = self.iter_step()?;

            let can_process = match &self.prune_condition {
                Some(prune_condition) => !prune_condition(&self.key_nibbles),
                None => true,
            };

            match (iter_step, can_process) {
                (IterStep::Continue, _) => {}
                (IterStep::PopTrail, _) => {
                    self.trail.pop();
                }
                // Skip processing the node if can process is false.
                (_, false) => {}
                (IterStep::Descend(hash), true) => match self.descend_into_node(&hash) {
                    Ok(_) => (),
                    Err(err) => return Some(Err(err)),
                },
                (IterStep::Value(hash), true) => {
                    return Some(
                        self.trie
                            .storage
                            .retrieve_raw_bytes(&hash)
                            .map(|value| (self.key(), value.to_vec())),
                    )
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use itertools::Itertools;
    use rand::seq::SliceRandom;
    use rand::Rng;

    use crate::test_utils::{
        create_tries, create_tries_complex, gen_changes, simplify_changes, test_populate_trie,
    };
    use crate::trie::iterator::IterStep;
    use crate::trie::nibble_slice::NibbleSlice;
    use crate::Trie;
    use near_primitives::shard_layout::ShardUId;

    fn value() -> Option<Vec<u8>> {
        Some(vec![0])
    }

    /// Checks that for visiting interval of trie nodes first state key is
    /// included and the last one is excluded.
    #[test]
    fn test_visit_interval() {
        let trie_changes = vec![(b"aa".to_vec(), Some(vec![1])), (b"abb".to_vec(), Some(vec![2]))];
        let tries = create_tries();
        let state_root =
            test_populate_trie(&tries, &Trie::EMPTY_ROOT, ShardUId::single_shard(), trie_changes);
        let trie = tries.get_trie_for_shard(ShardUId::single_shard(), state_root);
        let path_begin: Vec<_> = NibbleSlice::new(b"aa").iter().collect();
        let path_end: Vec<_> = NibbleSlice::new(b"abb").iter().collect();
        let mut trie_iter = trie.iter().unwrap();
        let items = trie_iter.visit_nodes_interval(&path_begin, &path_end).unwrap();
        let trie_items: Vec<_> = items.into_iter().map(|item| item.key).flatten().collect();
        assert_eq!(trie_items, vec![b"aa"]);
    }

    #[test]
    fn test_iterator() {
        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            let (trie_changes, map, trie) = gen_random_trie(&mut rng);

            {
                let result1: Vec<_> = trie.iter().unwrap().map(Result::unwrap).collect();
                let result2: Vec<_> = map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                assert_eq!(result1, result2);
            }
            test_seek_prefix(&trie, &map, &[]);

            let empty_vec = vec![];
            let max_key = map.keys().max().unwrap_or(&empty_vec);
            let min_key = map.keys().min().unwrap_or(&empty_vec);
            test_get_trie_items(&trie, &map, &[], &[]);
            test_get_trie_items(&trie, &map, min_key, max_key);
            for (seek_key, _) in trie_changes.iter() {
                test_seek_prefix(&trie, &map, seek_key);
                test_get_trie_items(&trie, &map, min_key, seek_key);
                test_get_trie_items(&trie, &map, seek_key, max_key);
            }
            for _ in 0..20 {
                let alphabet = &b"abcdefgh"[0..rng.gen_range(2..8)];
                let key_length = rng.gen_range(1..8);
                let seek_key: Vec<u8> =
                    (0..key_length).map(|_| *alphabet.choose(&mut rng).unwrap()).collect();
                test_seek_prefix(&trie, &map, &seek_key);

                let seek_key2: Vec<u8> =
                    (0..key_length).map(|_| *alphabet.choose(&mut rng).unwrap()).collect();
                let path_begin = seek_key.clone().min(seek_key2.clone());
                let path_end = seek_key.clone().max(seek_key2.clone());
                test_get_trie_items(&trie, &map, &path_begin, &path_end);
            }
        }
    }

    #[test]
    fn test_iterator_with_prune_condition_base() {
        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            let (trie_changes, map, trie) = gen_random_trie(&mut rng);

            // Check that pruning just one key (and it's subtree) works as expected.
            for (prune_key, _) in &trie_changes {
                let prune_key = prune_key.clone();
                let prune_key_nibbles = NibbleSlice::new(prune_key.as_slice()).iter().collect_vec();
                let prune_condition =
                    move |key_nibbles: &Vec<u8>| key_nibbles.starts_with(&prune_key_nibbles);

                let result1 = trie
                    .iter_with_prune_condition(Some(Box::new(prune_condition.clone())))
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
            let (trie_changes, map, trie) = gen_random_trie(&mut rng);

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
                    .iter_with_prune_condition(Some(Box::new(prune_condition.clone())))
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
        let tries = create_tries();
        let trie_changes = keys.iter().map(|key| (key.clone(), value())).collect();
        let state_root = test_populate_trie(&tries, &Trie::EMPTY_ROOT, shard_uid, trie_changes);
        let trie = tries.get_trie_for_shard(shard_uid, state_root);
        let iter = trie.iter_with_max_depth(max_depth).unwrap();
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
    ) -> (Vec<(Vec<u8>, Option<Vec<u8>>)>, BTreeMap<Vec<u8>, Vec<u8>>, Trie) {
        let tries = create_tries_complex(1, 2);
        let shard_uid = ShardUId { version: 1, shard_id: 0 };
        let trie_changes = gen_changes(rng, 10);
        let trie_changes = simplify_changes(&trie_changes);

        let mut map = BTreeMap::new();
        for (key, value) in trie_changes.iter() {
            if let Some(value) = value {
                map.insert(key.clone(), value.clone());
            }
        }
        let state_root =
            test_populate_trie(&tries, &Trie::EMPTY_ROOT, shard_uid, trie_changes.clone());
        let trie = tries.get_trie_for_shard(shard_uid, state_root);
        (trie_changes, map, trie)
    }

    fn test_get_trie_items(
        trie: &Trie,
        map: &BTreeMap<Vec<u8>, Vec<u8>>,
        path_begin: &[u8],
        path_end: &[u8],
    ) {
        let path_begin_nibbles: Vec<_> = NibbleSlice::new(path_begin).iter().collect();
        let path_end_nibbles: Vec<_> = NibbleSlice::new(path_end).iter().collect();
        let result1 =
            trie.iter().unwrap().get_trie_items(&path_begin_nibbles, &path_end_nibbles).unwrap();
        let result2: Vec<_> = map
            .range(path_begin.to_vec()..path_end.to_vec())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(result1, result2);

        // test when path_end ends in [16]
        let result1 = trie.iter().unwrap().get_trie_items(&path_begin_nibbles, &[16u8]).unwrap();
        let result2: Vec<_> =
            map.range(path_begin.to_vec()..).map(|(k, v)| (k.clone(), v.clone())).collect();
        assert_eq!(result1, result2);
    }

    fn test_seek_prefix(trie: &Trie, map: &BTreeMap<Vec<u8>, Vec<u8>>, seek_key: &[u8]) {
        let mut iterator = trie.iter().unwrap();
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

    #[test]
    fn test_has_value() {
        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            let tries = create_tries();
            let trie_changes = gen_changes(&mut rng, 10);
            let trie_changes = simplify_changes(&trie_changes);
            let state_root = test_populate_trie(
                &tries,
                &Trie::EMPTY_ROOT,
                ShardUId::single_shard(),
                trie_changes.clone(),
            );
            let trie = tries.get_trie_for_shard(ShardUId::single_shard(), state_root);
            let mut iterator = trie.iter().unwrap();
            loop {
                let iter_step = match iterator.iter_step() {
                    Some(iter_step) => iter_step,
                    None => break,
                };
                match iter_step {
                    IterStep::Value(_) => assert!(iterator.has_value()),
                    _ => assert!(!iterator.has_value()),
                }
                match iter_step {
                    IterStep::PopTrail => {
                        iterator.trail.pop();
                    }
                    IterStep::Descend(hash) => iterator.descend_into_node(&hash).unwrap(),
                    _ => {}
                }
            }
        }
    }
}

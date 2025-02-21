//! Iterator implementation that is shared between DiskTrieIterator and MemTrieIterator.
use std::sync::Arc;

use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;

use crate::NibbleSlice;
use crate::trie::ValueHandle;
use crate::trie::iterator::DiskTrieIteratorInner;
use crate::trie::trie_storage_update::TrieStorageNodePtr;

use super::interface::{GenericTrieInternalStorage, GenericTrieNode};

/// The TrieItem is a tuple of (key, value) of the node.
pub type TrieItem = (Vec<u8>, Vec<u8>);

/// Crumb is a piece of trie iteration state. It describes a node on the trail and processing status of that node.
#[derive(Debug)]
struct Crumb<N, V> {
    node: GenericTrieNode<N, V>,
    status: CrumbStatus,
    prefix_boundary: bool,
}

/// The status of processing of a node during trie iteration.
/// Each node is processed in the following order:
/// Entering -> At -> AtChild(0) -> ... -> AtChild(15) -> Exiting
#[derive(Debug, Clone, Copy)]
pub(crate) enum CrumbStatus {
    Entering,
    At,
    AtChild(u8),
    Exiting,
}

impl<N, V> Crumb<N, V> {
    fn increment(&mut self) {
        if self.prefix_boundary {
            self.status = CrumbStatus::Exiting;
            return;
        }
        self.status = match (&self.status, &self.node) {
            (_, GenericTrieNode::Empty) => CrumbStatus::Exiting,
            (&CrumbStatus::Entering, _) => CrumbStatus::At,
            (&CrumbStatus::At, GenericTrieNode::Branch { .. }) => CrumbStatus::AtChild(0),
            (&CrumbStatus::AtChild(x), GenericTrieNode::Branch { .. }) if x < 15 => {
                CrumbStatus::AtChild(x + 1)
            }
            _ => CrumbStatus::Exiting,
        }
    }
}

/// Trie iteration is done using a stack based approach.
///
/// There are two stacks that we track while iterating: the trail and the key_nibbles.
/// The trail is a vector of trie nodes on the path from root node to the node that is
/// currently being processed together with processing status - the Crumb.
///
/// The key_nibbles is a vector of nibbles from the state root node to the node that is
/// currently being processed.
///
/// The trail and the key_nibbles may have different lengths e.g. an extension trie node
/// will add only a single item to the trail but may add multiple nibbles to the key_nibbles.
pub struct TrieIteratorImpl<TrieNodePtr, ValueHandle, I>
where
    TrieNodePtr: Copy,
    ValueHandle: Clone,
    I: GenericTrieInternalStorage<TrieNodePtr, ValueHandle>,
{
    trail: Vec<Crumb<TrieNodePtr, ValueHandle>>,
    key_nibbles: Vec<u8>,

    /// We use this trie_interface as a distinction point between disk and memory trie.
    /// It provides the necessary methods to fetch nodes and values.
    trie_interface: I,

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

impl<N, V, I> TrieIteratorImpl<N, V, I>
where
    N: Copy,
    V: Clone,
    I: GenericTrieInternalStorage<N, V>,
{
    /// Create a new iterator.
    pub fn new(
        trie_interface: I,
        prune_condition: Option<Box<dyn Fn(&Vec<u8>) -> bool>>,
    ) -> Result<Self, StorageError> {
        let root = trie_interface.get_root();
        let mut iter = Self {
            trail: Vec::with_capacity(8),
            key_nibbles: Vec::with_capacity(64),
            trie_interface,
            prune_condition,
        };
        iter.descend_into_node(root)?;
        Ok(iter)
    }

    /// Position the iterator on the first element with key >= `key`.
    pub fn seek_prefix<K: AsRef<[u8]>>(&mut self, key: K) -> Result<(), StorageError> {
        self.seek_nibble_slice(NibbleSlice::new(key.as_ref()), true)?;
        Ok(())
    }

    /// Returns the hash of the last node.
    fn seek_nibble_slice(
        &mut self,
        mut key: NibbleSlice<'_>,
        is_prefix_seek: bool,
    ) -> Result<Option<N>, StorageError> {
        self.trail.clear();
        self.key_nibbles.clear();

        // Checks if a key in an extension or leaf matches our search query.
        //
        // When doing prefix seek, this checks whether `key` is a prefix of
        // `ext_key`.  When doing regular range seek, this checks whether `key`
        // is no greater than `ext_key`.  If those conditions aren’t met, the
        // node with `ext_key` should not match our query.
        let check_ext_key = |key: &NibbleSlice, ext_key: &NibbleSlice| {
            if is_prefix_seek { ext_key.starts_with(key) } else { ext_key >= key }
        };

        let mut ptr = self.trie_interface.get_root();
        let mut prev_prefix_boundary = &mut false;
        loop {
            *prev_prefix_boundary = is_prefix_seek;
            self.descend_into_node(ptr)?;
            let Crumb { status, node, prefix_boundary } = self.trail.last_mut().unwrap();
            prev_prefix_boundary = prefix_boundary;
            match &node {
                GenericTrieNode::Empty => break,
                GenericTrieNode::Leaf { extension, .. } => {
                    let existing_key = NibbleSlice::from_encoded(extension).0;
                    if !check_ext_key(&key, &existing_key) {
                        self.key_nibbles.extend(existing_key.iter());
                        *status = CrumbStatus::Exiting;
                    }
                    break;
                }
                GenericTrieNode::Branch { children, .. } => {
                    if key.is_empty() {
                        break;
                    }
                    let idx = key.at(0);
                    self.key_nibbles.push(idx);
                    *status = CrumbStatus::AtChild(idx);
                    if let Some(child) = children[idx as usize] {
                        ptr = Some(child);
                        key = key.mid(1);
                    } else {
                        *prefix_boundary = is_prefix_seek;
                        break;
                    }
                }
                GenericTrieNode::Extension { extension, child, .. } => {
                    let existing_key = NibbleSlice::from_encoded(extension).0;
                    if key.starts_with(&existing_key) {
                        key = key.mid(existing_key.len());
                        ptr = Some(*child);
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
        Ok(ptr)
    }

    /// Fetches node by its ptr and adds it to the trail.
    ///
    /// The node is stored as the last [`Crumb`] in the trail.
    fn descend_into_node(&mut self, ptr: Option<N>) -> Result<(), StorageError> {
        let node = match ptr {
            Some(ptr) => self.trie_interface.get_and_record_node(ptr)?,
            None => GenericTrieNode::Empty,
        };
        self.trail.push(Crumb { status: CrumbStatus::Entering, node, prefix_boundary: false });
        Ok(())
    }

    /// Make key from nibbles.
    fn key(&self) -> Vec<u8> {
        let mut result = <Vec<u8>>::with_capacity(self.key_nibbles.len() / 2);
        for i in (1..self.key_nibbles.len()).step_by(2) {
            result.push(self.key_nibbles[i - 1] * 16 + self.key_nibbles[i]);
        }
        result
    }

    /// Calculates the next step of the iteration.
    fn iter_step(&mut self) -> Option<IterStep<N, V>> {
        let last = self.trail.last_mut()?;
        last.increment();

        let step = match (last.status, &last.node) {
            (CrumbStatus::Exiting, n) => {
                match n {
                    GenericTrieNode::Leaf { extension, .. }
                    | GenericTrieNode::Extension { extension, .. } => {
                        let existing_key = NibbleSlice::from_encoded(&extension).0;
                        let l = self.key_nibbles.len();
                        self.key_nibbles.truncate(l - existing_key.len());
                    }
                    GenericTrieNode::Branch { .. } => {
                        self.key_nibbles.pop();
                    }
                    GenericTrieNode::Empty => {}
                }
                IterStep::PopTrail
            }
            (CrumbStatus::At, GenericTrieNode::Branch { value, .. }) => match value {
                Some(value) => IterStep::Value(value.clone()),
                None => IterStep::Continue,
            },
            (CrumbStatus::At, GenericTrieNode::Leaf { extension, value }) => {
                let key = NibbleSlice::from_encoded(&extension).0;
                self.key_nibbles.extend(key.iter());
                IterStep::Value(value.clone())
            }
            (CrumbStatus::At, GenericTrieNode::Extension { extension, child }) => {
                let key = NibbleSlice::from_encoded(&extension).0;
                self.key_nibbles.extend(key.iter());
                IterStep::Descend(*child)
            }
            (CrumbStatus::AtChild(i), GenericTrieNode::Branch { children, .. }) => {
                if i == 0 {
                    self.key_nibbles.push(0);
                }

                if let Some(ref child) = children[i as usize] {
                    if i != 0 {
                        *self.key_nibbles.last_mut().expect("Pushed child value before") = i;
                    }
                    IterStep::Descend(*child)
                } else {
                    IterStep::Continue
                }
            }
            _ => panic!("Should never see Entering or AtChild without a Branch here."),
        };
        Some(step)
    }
}

#[derive(Debug)]
enum IterStep<N, V> {
    Continue,
    PopTrail,
    Descend(N),
    Value(V),
}

impl<N, V, I> Iterator for TrieIteratorImpl<N, V, I>
where
    N: Copy,
    V: Clone,
    I: GenericTrieInternalStorage<N, V>,
{
    type Item = Result<TrieItem, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let iter_step = self.iter_step()?;

            let should_prune = match self.prune_condition {
                Some(ref prune_condition) => prune_condition(&self.key_nibbles),
                None => false,
            };

            match (iter_step, should_prune) {
                (IterStep::Continue, _) => {}
                (IterStep::PopTrail, _) => {
                    self.trail.pop();
                }
                // Skip processing node if it should be pruned.
                (_, true) => {}
                (IterStep::Descend(ptr), false) => match self.descend_into_node(Some(ptr)) {
                    Ok(_) => {}
                    Err(e) => return Some(Err(e)),
                },
                (IterStep::Value(value_ref), false) => {
                    let value = self.trie_interface.get_and_record_value(value_ref);
                    return Some(value.map(|value| (self.key(), value)));
                }
            }
        }
    }
}

/// *************************************************************************************************
/// Section below is used for state sync. Ideally we should have a better way of reading the trie
/// nodes and working with the iterator, example using trie recorder and prune conditions but while
/// we figure that out, we can keep the implementation below
/// *************************************************************************************************

// Item extracted from Trie during depth first traversal, corresponding to some Trie node.
#[derive(Debug)]
pub struct TrieTraversalItem {
    /// Hash of the node.
    pub hash: CryptoHash,
    /// Key of the node if it stores a value.
    pub key: Option<Vec<u8>>,
}

// Extension for State Parts processing
impl<I> TrieIteratorImpl<CryptoHash, ValueHandle, I>
where
    I: GenericTrieInternalStorage<TrieStorageNodePtr, ValueHandle>,
{
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
        let last_hash = self
            .seek_nibble_slice(NibbleSlice::from_encoded(&path_begin_encoded).0, false)?
            .unwrap_or_default();
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
                    self.descend_into_node(Some(hash))?;
                    nodes_list.push(TrieTraversalItem { hash, key: None });
                }
                IterStep::Continue => {}
                IterStep::Value(value) => {
                    if self.key_nibbles[prefix..] >= path_end[prefix..] {
                        break;
                    }
                    self.trie_interface.get_and_record_value(value)?;
                    let hash = match value {
                        ValueHandle::HashAndSize(hash) => hash.hash,
                        ValueHandle::InMemory(_) => panic!("Unexpected in-memory value"),
                    };
                    nodes_list.push(TrieTraversalItem {
                        hash,
                        key: self.has_value().then(|| self.key()),
                    });
                }
            }
        }
        Ok(nodes_list)
    }

    fn common_prefix(str1: &[u8], str2: &[u8]) -> usize {
        let mut prefix = 0;
        while prefix < str1.len() && prefix < str2.len() && str1[prefix] == str2[prefix] {
            prefix += 1;
        }
        prefix
    }

    fn has_value(&self) -> bool {
        match self.trail.last() {
            Some(b) => match &b.status {
                CrumbStatus::At => match &b.node {
                    GenericTrieNode::Branch { value, .. } => value.is_some(),
                    GenericTrieNode::Leaf { .. } => true,
                    _ => false,
                },
                _ => false,
            },
            None => false, // Trail finished
        }
    }
}

/// TODO: Remove this once we shift to using recorded storage in trie iterator.
impl<'a> TrieIteratorImpl<CryptoHash, ValueHandle, DiskTrieIteratorInner<'a>> {
    pub fn remember_visited_nodes(&mut self, record_nodes: bool) {
        self.trie_interface.remember_visited_nodes(record_nodes)
    }

    pub fn into_visited_nodes(self) -> Vec<Arc<[u8]>> {
        self.trie_interface.into_visited_nodes()
    }
}

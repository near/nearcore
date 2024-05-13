use super::node::{MemTrieNodePtr, MemTrieNodeView};
use crate::{trie::OptimizedValueRef, NibbleSlice};
use near_primitives::errors::StorageError;

#[derive(Debug)]
struct Crumb<'a> {
    node: Option<MemTrieNodeView<'a>>,
    status: CrumbStatus,
    prefix_boundary: bool,
}

#[derive(Debug, Clone, Copy)]
enum CrumbStatus {
    Entering,
    At,
    AtChild(u8),
    Exiting,
}

impl<'a> Crumb<'a> {
    fn increment(&mut self) {
        if self.prefix_boundary {
            self.status = CrumbStatus::Exiting;
            return;
        }
        self.status = match (&self.status, &self.node) {
            (_, None) => CrumbStatus::Exiting,
            (&CrumbStatus::Entering, _) => CrumbStatus::At,
            (&CrumbStatus::At, Some(MemTrieNodeView::Branch { .. })) => CrumbStatus::AtChild(0),
            (&CrumbStatus::At, Some(MemTrieNodeView::BranchWithValue { .. })) => {
                CrumbStatus::AtChild(0)
            }
            (&CrumbStatus::AtChild(x), Some(MemTrieNodeView::Branch { .. })) if x < 15 => {
                CrumbStatus::AtChild(x + 1)
            }
            (&CrumbStatus::AtChild(x), Some(MemTrieNodeView::BranchWithValue { .. })) if x < 15 => {
                CrumbStatus::AtChild(x + 1)
            }
            _ => CrumbStatus::Exiting,
        }
    }
}

pub struct MemTrieIterator<'a> {
    root: Option<MemTrieNodePtr<'a>>,
    trail: Vec<Crumb<'a>>,
    key_nibbles: Vec<u8>,

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

    value_getter: Box<dyn Fn(OptimizedValueRef) -> Result<Vec<u8>, StorageError> + 'a>,
}

/// The TrieTiem is a tuple of (key, value) of the node.
pub type TrieItem = (Vec<u8>, Vec<u8>);

impl<'a> MemTrieIterator<'a> {
    pub fn new(
        root: Option<MemTrieNodePtr<'a>>,
        prune_condition: Option<Box<dyn Fn(&Vec<u8>) -> bool>>,
        value_getter: Box<dyn Fn(OptimizedValueRef) -> Result<Vec<u8>, StorageError> + 'a>,
    ) -> Self {
        let mut r = MemTrieIterator {
            root,
            trail: Vec::new(),
            key_nibbles: Vec::new(),
            prune_condition,
            value_getter,
        };
        r.descend_into_node(root);
        r
    }

    pub fn seek_prefix<K: AsRef<[u8]>>(&mut self, key: K) {
        self.seek_nibble_slice(NibbleSlice::new(key.as_ref()), true);
    }

    /// Returns the hash of the last node
    pub(crate) fn seek_nibble_slice(
        &mut self,
        mut key: NibbleSlice<'_>,
        is_prefix_seek: bool,
    ) -> Option<MemTrieNodePtr<'a>> {
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

        let mut ptr = self.root;
        let mut prev_prefix_boundary = &mut false;
        loop {
            *prev_prefix_boundary = is_prefix_seek;
            self.descend_into_node(ptr);
            let Crumb { status, node, prefix_boundary } = self.trail.last_mut().unwrap();
            prev_prefix_boundary = prefix_boundary;
            match &node {
                None => break,
                Some(MemTrieNodeView::Leaf { extension, .. }) => {
                    let existing_key = NibbleSlice::from_encoded(extension.raw_slice()).0;
                    if !check_ext_key(&key, &existing_key) {
                        self.key_nibbles.extend(existing_key.iter());
                        *status = CrumbStatus::Exiting;
                    }
                    break;
                }
                Some(MemTrieNodeView::Branch { children, .. })
                | Some(MemTrieNodeView::BranchWithValue { children, .. }) => {
                    if key.is_empty() {
                        break;
                    }
                    let idx = key.at(0);
                    self.key_nibbles.push(idx);
                    *status = CrumbStatus::AtChild(idx);
                    if let Some(child) = children.get(idx as usize) {
                        ptr = Some(child);
                        key = key.mid(1);
                    } else {
                        *prefix_boundary = is_prefix_seek;
                        break;
                    }
                }
                Some(MemTrieNodeView::Extension { extension, child, .. }) => {
                    let existing_key = NibbleSlice::from_encoded(extension.raw_slice()).0;
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
        ptr
    }

    /// Fetches node by its ptr and adds it to the trail.
    ///
    /// The node is stored as the last [`Crumb`] in the trail.  If iterator is
    /// configured to remember all the nodes its visiting (which can be enabled
    /// with [`Self::remember_visited_nodes`]), the node will be added to the
    /// list.
    fn descend_into_node(&mut self, ptr: Option<MemTrieNodePtr<'a>>) {
        let node = ptr.map(|ptr| ptr.view());
        self.trail.push(Crumb { status: CrumbStatus::Entering, node, prefix_boundary: false });
    }

    fn key(&self) -> Vec<u8> {
        let mut result = <Vec<u8>>::with_capacity(self.key_nibbles.len() / 2);
        for i in (1..self.key_nibbles.len()).step_by(2) {
            result.push(self.key_nibbles[i - 1] * 16 + self.key_nibbles[i]);
        }
        result
    }

    fn iter_step(&mut self) -> Option<IterStep<'a>> {
        let last = self.trail.last_mut()?;
        last.increment();
        Some(match (last.status, &last.node) {
            (CrumbStatus::Exiting, n) => {
                match n {
                    Some(MemTrieNodeView::Leaf { extension, .. })
                    | Some(MemTrieNodeView::Extension { extension, .. }) => {
                        let existing_key = NibbleSlice::from_encoded(extension.raw_slice()).0;
                        let l = self.key_nibbles.len();
                        self.key_nibbles.truncate(l - existing_key.len());
                    }
                    Some(MemTrieNodeView::Branch { .. })
                    | Some(MemTrieNodeView::BranchWithValue { .. }) => {
                        self.key_nibbles.pop();
                    }
                    _ => {}
                }
                IterStep::PopTrail
            }
            (CrumbStatus::At, Some(MemTrieNodeView::BranchWithValue { value, .. })) => {
                IterStep::Value(value.to_optimized_value_ref())
            }
            (CrumbStatus::At, Some(MemTrieNodeView::Branch { .. })) => IterStep::Continue,
            (CrumbStatus::At, Some(MemTrieNodeView::Leaf { extension, value })) => {
                let key = NibbleSlice::from_encoded(extension.raw_slice()).0;
                self.key_nibbles.extend(key.iter());
                IterStep::Value(value.to_optimized_value_ref())
            }
            (CrumbStatus::At, Some(MemTrieNodeView::Extension { extension, child, .. })) => {
                let key = NibbleSlice::from_encoded(extension.raw_slice()).0;
                self.key_nibbles.extend(key.iter());
                IterStep::Descend(*child)
            }
            (CrumbStatus::AtChild(i), Some(MemTrieNodeView::Branch { children, .. }))
            | (CrumbStatus::AtChild(i), Some(MemTrieNodeView::BranchWithValue { children, .. })) => {
                if i == 0 {
                    self.key_nibbles.push(0);
                }
                if let Some(ref child) = children.get(i as usize) {
                    if i != 0 {
                        *self.key_nibbles.last_mut().expect("Pushed child value before") = i;
                    }
                    IterStep::Descend(*child)
                } else {
                    IterStep::Continue
                }
            }
            _ => panic!("Should never see Entering or AtChild without a Branch here."),
        })
    }
}

#[derive(Debug)]
enum IterStep<'a> {
    Continue,
    PopTrail,
    Descend(MemTrieNodePtr<'a>),
    Value(OptimizedValueRef),
}

impl<'a> Iterator for MemTrieIterator<'a> {
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
                (IterStep::Descend(ptr), true) => {
                    self.descend_into_node(Some(ptr));
                }
                (IterStep::Value(value_ref), true) => {
                    return Some((self.value_getter)(value_ref).map(|value| (self.key(), value)))
                }
            }
        }
    }
}

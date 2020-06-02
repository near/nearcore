use near_primitives::hash::CryptoHash;

use crate::trie::nibble_slice::NibbleSlice;
use crate::trie::{NodeHandle, TrieNode, TrieNodeWithSize, ValueHandle};
use crate::{StorageError, Trie};

#[derive(Debug)]
struct Crumb {
    node: TrieNodeWithSize,
    status: CrumbStatus,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum CrumbStatus {
    Entering,
    At,
    AtChild(usize),
    Exiting,
}

impl Crumb {
    fn increment(&mut self) {
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

pub struct TrieIterator<'a> {
    trie: &'a Trie,
    trail: Vec<Crumb>,
    pub(crate) key_nibbles: Vec<u8>,
    root: CryptoHash,
}

pub type TrieItem<'a> = Result<(Vec<u8>, Vec<u8>), StorageError>;

impl<'a> TrieIterator<'a> {
    #![allow(clippy::new_ret_no_self)]
    /// Create a new iterator.
    pub fn new(trie: &'a Trie, root: &CryptoHash) -> Result<Self, StorageError> {
        let mut r = TrieIterator {
            trie,
            trail: Vec::with_capacity(8),
            key_nibbles: Vec::with_capacity(64),
            root: *root,
        };
        let node = trie.retrieve_node(root)?;
        r.descend_into_node(&node);
        Ok(r)
    }

    /// Position the iterator on the first element with key => `key`.
    pub fn seek<K: AsRef<[u8]>>(&mut self, key: K) -> Result<(), StorageError> {
        self.seek_nibble_slice(NibbleSlice::new(key.as_ref()))
    }

    pub(crate) fn seek_nibble_slice(
        &mut self,
        mut key: NibbleSlice<'_>,
    ) -> Result<(), StorageError> {
        self.trail.clear();
        self.key_nibbles.clear();
        let mut hash = NodeHandle::Hash(self.root);
        loop {
            let node = match hash {
                NodeHandle::Hash(hash) => self.trie.retrieve_node(&hash)?,
                NodeHandle::InMemory(_node) => unreachable!(),
            };
            let copy_node = node.clone();
            match node.node {
                TrieNode::Empty => return Ok(()),
                TrieNode::Leaf(leaf_key, _) => {
                    let existing_key = NibbleSlice::from_encoded(&leaf_key).0;
                    self.trail.push(Crumb {
                        status: if existing_key >= key {
                            CrumbStatus::Entering
                        } else {
                            CrumbStatus::Exiting
                        },
                        node: copy_node,
                    });
                    self.key_nibbles.extend(existing_key.iter());
                    return Ok(());
                }
                TrieNode::Branch(mut children, _) => {
                    if key.is_empty() {
                        self.trail.push(Crumb { status: CrumbStatus::Entering, node: copy_node });
                        return Ok(());
                    } else {
                        let idx = key.at(0) as usize;
                        self.trail.push(Crumb {
                            status: CrumbStatus::AtChild(idx as usize),
                            node: copy_node,
                        });
                        self.key_nibbles.push(key.at(0));
                        if let Some(child) = children[idx].take() {
                            hash = child;
                            key = key.mid(1);
                        } else {
                            return Ok(());
                        }
                    }
                }
                TrieNode::Extension(ext_key, child) => {
                    let existing_key = NibbleSlice::from_encoded(&ext_key).0;
                    if key.starts_with(&existing_key) {
                        self.trail.push(Crumb { status: CrumbStatus::At, node: copy_node });
                        self.key_nibbles.extend(existing_key.iter());
                        hash = child;
                        key = key.mid(existing_key.len());
                    } else {
                        self.descend_into_node(&copy_node);
                        return Ok(());
                    }
                }
            }
        }
    }

    fn descend_into_node(&mut self, node: &TrieNodeWithSize) {
        self.trail.push(Crumb { status: CrumbStatus::Entering, node: node.clone() });
        match &self.trail.last().expect("Just pushed item").node.node {
            TrieNode::Leaf(ref key, _) | TrieNode::Extension(ref key, _) => {
                let key = NibbleSlice::from_encoded(key).0;
                self.key_nibbles.extend(key.iter());
            }
            _ => {}
        }
    }

    fn key(&self) -> Vec<u8> {
        let mut result = <Vec<u8>>::with_capacity(self.key_nibbles.len() / 2);
        for i in (1..self.key_nibbles.len()).step_by(2) {
            result.push(self.key_nibbles[i - 1] * 16 + self.key_nibbles[i]);
        }
        result
    }
}

impl<'a> Iterator for TrieIterator<'a> {
    type Item = TrieItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        enum IterStep {
            Continue,
            PopTrail,
            Descend(Result<Box<TrieNodeWithSize>, StorageError>),
        }
        loop {
            let iter_step = {
                self.trail.last_mut()?.increment();
                let b = self.trail.last().expect("Trail finished.");
                match (b.status.clone(), &b.node.node) {
                    (CrumbStatus::Exiting, n) => {
                        match n {
                            TrieNode::Leaf(ref key, _) | TrieNode::Extension(ref key, _) => {
                                let existing_key = NibbleSlice::from_encoded(&key).0;
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
                        let value = match value {
                            ValueHandle::HashAndSize(_, hash) => self.trie.retrieve_raw_bytes(hash),
                            ValueHandle::InMemory(_node) => unreachable!(),
                        };
                        return Some(value.map(|value| (self.key(), value)));
                    }
                    (CrumbStatus::At, TrieNode::Branch(_, None)) => IterStep::Continue,
                    (CrumbStatus::At, TrieNode::Leaf(_, value)) => {
                        let value = match value {
                            ValueHandle::HashAndSize(_, hash) => self.trie.retrieve_raw_bytes(hash),
                            ValueHandle::InMemory(_node) => unreachable!(),
                        };
                        return Some(value.map(|value| (self.key(), value)));
                    }
                    (CrumbStatus::At, TrieNode::Extension(_, child)) => {
                        let next_node = match child {
                            NodeHandle::Hash(hash) => self.trie.retrieve_node(hash).map(Box::new),
                            NodeHandle::InMemory(_node) => unreachable!(),
                        };
                        IterStep::Descend(next_node)
                    }
                    (CrumbStatus::AtChild(i), TrieNode::Branch(children, _))
                        if children[i].is_some() =>
                    {
                        match i {
                            0 => self.key_nibbles.push(0),
                            i => {
                                *self.key_nibbles.last_mut().expect("Pushed child value before") =
                                    i as u8
                            }
                        }
                        let next_node = match &children[i] {
                            Some(NodeHandle::Hash(hash)) => {
                                self.trie.retrieve_node(&hash).map(Box::new)
                            }
                            Some(NodeHandle::InMemory(_node)) => unreachable!(),
                            _ => panic!("Wrapped with is_some()"),
                        };
                        IterStep::Descend(next_node)
                    }
                    (CrumbStatus::AtChild(i), TrieNode::Branch(_, _)) => {
                        if i == 0 {
                            self.key_nibbles.push(0);
                        }
                        IterStep::Continue
                    }
                    _ => panic!("Should never see Entering or AtChild without a Branch here."),
                }
            };
            match iter_step {
                IterStep::PopTrail => {
                    self.trail.pop();
                }
                IterStep::Descend(Ok(node)) => self.descend_into_node(&node),
                IterStep::Descend(Err(e)) => return Some(Err(e)),
                IterStep::Continue => {}
            }
        }
    }
}

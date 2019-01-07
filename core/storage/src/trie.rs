use std::sync::Arc;
use std::collections::HashMap;
use primitives::hash::{CryptoHash, hash_struct};
use primitives::traits::{Encode, Decode};
pub use kvdb::{DBValue, KeyValueDB};

use nibble_slice::NibbleSlice;

#[derive(Serialize, Deserialize, Clone, Hash, Debug)]
enum NodeHandle {
    InMemory(Box<TrieNode>),
    Hash(CryptoHash),
}

#[derive(Serialize, Deserialize, Clone, Hash, Debug)]
enum TrieNode {
    /// Null trie node. Could be an empty root or an empty branch entry.
    Empty,
    /// Key and value of the leaf node.
    Leaf(Vec<u8>, Vec<u8>),
    /// Branch of 16 possible children and value if key ends here.
    Branch([Option<NodeHandle>; 16], Option<Vec<u8>>),
    /// Key and child of extension.
    Extension(Vec<u8>, NodeHandle),
}

impl TrieNode {
    fn new(rc_node: RcTrieNode) -> TrieNode {
        match rc_node.data {
            RawTrieNode::Leaf(key, value) => TrieNode::Leaf(key, value),
            RawTrieNode::Branch(children, value) => {
                let mut new_children: [Option<NodeHandle>; 16] = Default::default();
                for i in 0..children.len() {
                    new_children[i] = children[i].map(NodeHandle::Hash);
                }
                TrieNode::Branch(new_children, value)
            },
            RawTrieNode::Extension(key, child) => TrieNode::Extension(key, NodeHandle::Hash(child))
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum RawTrieNode {
    Leaf(Vec<u8>, Vec<u8>),
    Branch([Option<CryptoHash>; 16], Option<Vec<u8>>),
    Extension(Vec<u8>, CryptoHash),
}

#[derive(Serialize, Deserialize, Debug)]
struct RcTrieNode {
    data: RawTrieNode,
    rc: u32,
}

pub struct Trie {
    storage: Arc<KeyValueDB>,
    column: Option<u32>,
    null_node: CryptoHash,
}

pub type DBChanges = HashMap<Vec<u8>, Option<Vec<u8>>>;

impl Trie {
    pub fn new(storage: Arc<KeyValueDB>, column: Option<u32>) -> Self {
        Trie {
            storage,
            column,
            null_node: Trie::empty_root(),
        }
    }

    pub fn empty_root() -> CryptoHash {
        CryptoHash::default()
    }

    fn retrieve_node(&self, hash: &CryptoHash) -> Result<TrieNode, String> {
        if *hash == self.null_node {
            return Ok(TrieNode::Empty);
        }
        if let Ok(Some(bytes)) =  self.storage.get(self.column, hash.as_ref()) {
            match RcTrieNode::decode(&bytes) {
                Some(value) => Ok(TrieNode::new(value)),
                None => Err(format!("Failed to decode node {}", hash))
            }
        } else {
            Err(format!("Node {} not found in storage", hash))
        }
    }

    fn lookup(&self, root: &CryptoHash, mut key: NibbleSlice) -> Result<Option<Vec<u8>>, String> {
        let mut hash = *root;

        for _ in 0.. {
            if hash == self.null_node {
                return Ok(None);
            }
            let node = match self.storage.get(self.column, hash.as_ref()) {
                Ok(Some(bytes)) => RcTrieNode::decode(&bytes)
                    .map(|trie_node| trie_node.data),
                _ => return Err(format!("Node {} not found in storage", hash))
            };

            match node {
                Some(RawTrieNode::Leaf(existing_key, value)) => {
                    return Ok(if NibbleSlice::from_encoded(&existing_key).0 == key { Some(value) } else { None });
                },
                Some(RawTrieNode::Extension(existing_key, child)) => {
                    let existing_key = NibbleSlice::from_encoded(&existing_key).0;
                    if key.starts_with(&existing_key) {
                        hash = child;
                        key = key.mid(existing_key.len());
                    } else {
                        return Ok(None);
                    }
                },
                Some(RawTrieNode::Branch(mut children, value)) => if key.is_empty() {
                    return Ok(value);
                } else {
                    match children[key.at(0) as usize].take() {
                        Some(x) => {
                            hash = x;
                            key = key.mid(1);
                        },
                        None => return Ok(None),
                    }
                },
                _ => return Err(format!("Node {} not found in storage.", hash))
            };
        }
        Ok(None)
    }

    pub fn get(&self, root: &CryptoHash, key: &[u8]) -> Option<Vec<u8>> {
        let key = NibbleSlice::new(key);
        match self.lookup(root, key) {
            Ok(value) => value,
            Err(err) => {
                println!("Failed to lookup key={:?} for root={:?}: {}", key, root, err);
                None
            }
        }
    }

    fn insert(&self, node: TrieNode, partial: NibbleSlice, value: Vec<u8>) -> Result<TrieNode, String> {
        match node {
            TrieNode::Empty => {
                let leaf_node = TrieNode::Leaf(partial.encoded(true).into_vec(), value);
                Ok(leaf_node)
            },
            TrieNode::Branch(mut children, existing_value) => {
                // If the key ends here, store the value in branch's value.
                if partial.is_empty() {
                    Ok(TrieNode::Branch(children, Some(value)))
                } else {
                    let idx = partial.at(0) as usize;
                    let partial = partial.mid(1);
                    let child = children[idx].take();
                    let new_hash = match child {
                        Some(NodeHandle::Hash(hash)) => self.insert(self.retrieve_node(&hash)?, partial, value)?,
                        Some(NodeHandle::InMemory(node)) => self.insert(*node, partial, value)?,
                        _ => TrieNode::Leaf(partial.encoded(true).into_vec(), value),
                    };
                    children[idx] = Some(NodeHandle::InMemory(Box::new(new_hash)));
                    Ok(TrieNode::Branch(children, existing_value))
                }
            },
            TrieNode::Leaf(key, existing_value) => {
                let existing_key = NibbleSlice::from_encoded(&key).0;
                let common_prefix = partial.common_prefix(&existing_key);
                if common_prefix == existing_key.len() && common_prefix == partial.len() {
                    // Equivalent leaf.
                    Ok(TrieNode::Leaf(key.clone(), value))
                } else if common_prefix == 0 {
                    let mut children = Default::default();
                    let branch_node = if existing_key.is_empty() {
                        TrieNode::Branch(children, Some(existing_value))
                    } else {
                        let idx = existing_key.at(0) as usize;
                        let new_leaf = TrieNode::Leaf(existing_key.mid(1).encoded(true).into_vec(), existing_value);
                        children[idx] = Some(NodeHandle::InMemory(Box::new(new_leaf)));
                        TrieNode::Branch(children, None)
                    };
                    self.insert(branch_node, partial, value)
                } else if common_prefix == existing_key.len() {
                    let branch_node = TrieNode::Branch(Default::default(), Some(existing_value));
                    self.insert(branch_node, partial.mid(common_prefix), value)
                } else {
                    // Partially shared prefix: convert to leaf and call recursively to add a branch.
                    let low = TrieNode::Leaf(existing_key.mid(common_prefix).encoded(true).into_vec(), existing_value);
                    let child = self.insert(low, partial.mid(common_prefix), value)?;
                    Ok(TrieNode::Extension(
                        partial.encoded_leftmost(common_prefix, false).into_vec(),
                        NodeHandle::InMemory(Box::new(child))))
                }
            },
            TrieNode::Extension(key, child) => {
                let existing_key = NibbleSlice::from_encoded(&key).0;
                let common_prefix = partial.common_prefix(&existing_key);
                if common_prefix == 0 {
                    let idx = existing_key.at(0) as usize;
                    let mut children: [Option<NodeHandle>; 16] = Default::default();
                    children[idx] = if existing_key.len() == 1 {
                        Some(child)
                    } else {
                        let ext_node = TrieNode::Extension(existing_key.mid(1).encoded(false).into_vec(), child);
                        Some(NodeHandle::InMemory(Box::new(ext_node)))
                    };
                    let branch_node = TrieNode::Branch(children, None);
                    self.insert(branch_node, partial, value)
                } else if common_prefix == existing_key.len() {
                    let child_node = match child {
                        NodeHandle::Hash(hash) => self.retrieve_node(&hash)?,
                        NodeHandle::InMemory(node) => *node,
                    };
                    let new_child = NodeHandle::InMemory(Box::new(self.insert(child_node, partial.mid(common_prefix), value)?));
                    Ok(TrieNode::Extension(key.clone(), new_child))
                } else {
                    // Partially shared prefix: covert to shorter extension and recursively add a branch.
                    let low = TrieNode::Extension(existing_key.mid(common_prefix).encoded(false).into_vec(), child);
                    let new_child = NodeHandle::InMemory(Box::new(self.insert(low, partial.mid(common_prefix), value)?));
                    Ok(TrieNode::Extension(existing_key.encoded_leftmost(common_prefix, false).into_vec(), new_child))
                }
            }
        }
    }

    fn delete(&self, node: TrieNode, hash: Option<CryptoHash>, partial: NibbleSlice, death_row: &mut HashMap<CryptoHash, u32>) -> Result<Option<TrieNode>, String> {
        match node {
            TrieNode::Empty => Err("Removing empty node".to_string()),
            TrieNode::Leaf(key, _) => if NibbleSlice::from_encoded( & key).0 == partial {
                if let Some(hash) = hash { *death_row.entry(hash).or_insert(0) += 1 }
                Ok(None)
            } else {
                Err("Removing missing leaf node".to_string())
            },
            TrieNode::Branch(mut children, value) => {
                if let Some(hash) = hash { *death_row.entry(hash).or_insert(0) += 1; }
                if partial.is_empty() {
                    if children.iter().filter(|&x| x.is_some()).count() == 0 {
                        Ok(None)
                    } else {
                        Ok(Some(TrieNode::Branch(children, None)))
                    }
                } else {
                    let idx = partial.at(0) as usize;
                    if let Some(node_or_hash) = children[idx].take() {
                        let new_node = match node_or_hash {
                            NodeHandle::Hash(hash) => {
                                self.delete(self.retrieve_node(&hash)?, Some(hash), partial.mid(1), death_row)?
                            },
                            NodeHandle::InMemory(node) => {
                                self.delete(*node, None, partial.mid(1), death_row)?
                            }
                        };
                        children[idx] = match new_node {
                            Some(node) => Some(NodeHandle::InMemory(Box::new(node))),
                            None => None
                        };
                        if children.iter().filter(|x| x.is_some()).count() == 0 {
                            match value {
                                Some(value) => Ok(Some(TrieNode::Leaf(NibbleSlice::new(&[]).encoded(true).into_vec(), value))),
                                None => Ok(None)
                            }
                        } else {
                            Ok(Some(TrieNode::Branch(children, value)))
                        }
                    } else {
                        Err("Removing missing node".to_string())
                    }
                }
            },
            TrieNode::Extension(key, child) => {
                if let Some(hash) = hash { *death_row.entry(hash).or_insert(0) += 1 }
                let (common_prefix, existing_len) = {
                    let existing_key = NibbleSlice::from_encoded(&key).0;
                    (existing_key.common_prefix(&partial), existing_key.len())
                };
                if common_prefix == existing_len {
                    let result = match child {
                        NodeHandle::Hash(hash) => {
                            self.delete(self.retrieve_node(&hash)?, Some(hash), partial.mid(existing_len), death_row)?
                        },
                        NodeHandle::InMemory(node) => {
                            self.delete(*node, None, partial.mid(existing_len), death_row)?
                        }
                    };
                    // TODO: fix tree if the child is not a branch.
                    match result {
                        Some(node) => Ok(Some(TrieNode::Extension(key, NodeHandle::InMemory(Box::new(node))))),
                        None => Ok(None)
                    }
                } else {
                    Err("Removing missing node".to_string())
                }
            }
        }
    }

    fn flatten_nodes(&self, node: TrieNode, nodes: &mut HashMap<CryptoHash, RcTrieNode>) -> CryptoHash {
        let rc_node = match node {
            TrieNode::Empty => return self.null_node,
            TrieNode::Branch(mut children, value) => {
                let mut new_children: [Option<CryptoHash>; 16] = Default::default();
                for i in 0..children.len() {
                    new_children[i] = match children[i].take() {
                        Some(NodeHandle::InMemory(child_node)) => Some(self.flatten_nodes(*child_node, nodes)),
                        Some(NodeHandle::Hash(hash)) => Some(hash),
                        _ => None,
                    }
                };
                RcTrieNode { data: RawTrieNode::Branch(new_children, value), rc: 0 }
            },
            TrieNode::Extension(key, child) => {
                let child = match child {
                    NodeHandle::InMemory(child) => self.flatten_nodes(*child, nodes),
                    NodeHandle::Hash(hash) => hash,
                };
                RcTrieNode { data: RawTrieNode::Extension(key, child), rc: 0 }
            }
            TrieNode::Leaf(key, value) => {
                RcTrieNode { data: RawTrieNode::Leaf(key, value), rc: 0 }
            },
        };
        let hash = hash_struct(&rc_node.data);
        let entry = nodes.entry(hash).or_insert(rc_node);
        entry.rc += 1;
        hash
    }

    pub fn update<I>(&self, root: &CryptoHash, changes: I) -> (DBChanges, CryptoHash)
            where I: Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>{
        let mut death_row: HashMap<CryptoHash, u32> = HashMap::default();
        let mut last_root = Some(*root);
        let mut root_node = self.retrieve_node(root).expect("Root not found");
        for (key, value) in changes {
            let key = NibbleSlice::new(&key);
            match value {
                Some(arr) => {
                    root_node = self.insert(root_node, key, arr).expect("Failed to insert");
                    last_root = None;
                },
                None => {
                    root_node = match self.delete(root_node, last_root, key, &mut death_row).expect("Failed to delete") {
                        Some(value) => value,
                        None => TrieNode::Empty
                    };
                    last_root = None;
                }
            }
        }
        let mut db_changes = HashMap::default();

        let mut nodes = HashMap::default();
        // TODO: The reference counting doesn't account for the number of existing nodes in
        // storage that were not touched by this update.
        let new_root = self.flatten_nodes(root_node, &mut nodes);
        for (key, value) in nodes.iter() {
            db_changes.insert(key.as_ref().to_vec(), value.encode());
        }
        for (hash, _) in death_row {
            db_changes.insert(hash.as_ref().to_vec(), None);
        }
        (db_changes, new_root)
    }

    pub fn iter<'a>(&'a self, root: &CryptoHash) -> Result<Box<TrieIterator<'a>>, String> {
        TrieIterator::new(self, root).map(|iter| Box::new(iter) as Box<_>)
    }
}

pub type TrieItem<'a> = Result<(Vec<u8>, DBValue), String>;

#[derive(Clone, Eq, PartialEq, Debug)]
enum CrumbStatus {
    Entering,
    At,
    AtChild(usize),
    Exiting,
}

#[derive(Debug)]
struct Crumb {
    node: TrieNode,
    status: CrumbStatus,
}

impl Crumb {
    fn increment(&mut self) {
        self.status = match (&self.status, &self.node) {
            (_, &TrieNode::Empty) => CrumbStatus::Exiting,
            (&CrumbStatus::Entering, _) => CrumbStatus::At,
            (&CrumbStatus::At, &TrieNode::Branch(_, _)) => CrumbStatus::AtChild(0),
            (&CrumbStatus::AtChild(x), &TrieNode::Branch(_, _)) if x < 15 => CrumbStatus::AtChild(x + 1),
            _ => CrumbStatus::Exiting,
        }
    }
}

pub struct TrieIterator<'a> {
    trie: &'a Trie,
    trail: Vec<Crumb>,
    key_nibbles: Vec<u8>,
    root: CryptoHash,
}

impl<'a> TrieIterator<'a> {
    /// Create a new iterator.
    pub fn new(trie: &'a Trie, root: &CryptoHash) -> Result<Self, String> {
        let mut r = TrieIterator {
            trie,
            trail: Vec::with_capacity(8),
            key_nibbles: Vec::with_capacity(64),
            root: *root,
        };
        if let Ok(node) = trie.retrieve_node(root) {
            r.descend_into_node(&node);
            return Ok(r);
        }
        Err(format!("Root hash {} not found", root))
    }

    /// Position the iterator on the first element with key => `key`.
    pub fn seek(&mut self, key: &[u8]) -> Result<(), String> {
        self.trail.clear();
        self.key_nibbles.clear();
        let mut hash = NodeHandle::Hash(self.root);
        let mut key = NibbleSlice::new(key);
        loop {
            let node = match hash {
                NodeHandle::Hash(hash) => self.trie.retrieve_node(&hash)?,
                NodeHandle::InMemory(node) => *node,
            };
            let copy_node = node.clone();
            match node {
               TrieNode::Empty => return Ok(()),
               TrieNode::Leaf(leaf_key, _) => {
                   let existing_key = NibbleSlice::from_encoded(&leaf_key).0;
                    self.trail.push(Crumb {
                        status: if existing_key >= key { CrumbStatus::Entering } else { CrumbStatus::Exiting },
                        node: copy_node,
                    });
                   self.key_nibbles.extend(existing_key.iter());
                   return Ok(())
               },
               TrieNode::Branch(mut children, _) => {
                   if key.is_empty() {
                       self.trail.push(Crumb {
                           status: CrumbStatus::Entering,
                           node: copy_node
                       });
                       return Ok(())
                   } else {
                       let idx = key.at(0) as usize;
                       self.trail.push(Crumb {
                          status: CrumbStatus::AtChild(idx as usize),
                           node: copy_node
                       });
                       self.key_nibbles.push(key.at(0));
                       if let Some(child) = children[idx].take() {
                           hash = child;
                           key = key.mid(1);
                       } else {
                           return Ok(());
                       }
                   }
               },
               TrieNode::Extension(ext_key, child) => {
                   let existing_key = NibbleSlice::from_encoded(&ext_key).0;
                   if key.starts_with(&existing_key) {
                       self.trail.push(Crumb {
                          status: CrumbStatus::At,
                           node: copy_node
                       });
                       self.key_nibbles.extend(existing_key.iter());
                       hash = child;
                       key = key.mid(existing_key.len());
                   } else {
                       // ???
                       return Ok(());
                   }
               }
           }
        }
    }

    fn descend_into_node(&mut self, node: &TrieNode) {
        self.trail.push(Crumb {
           status: CrumbStatus::Entering, node: node.clone()
        });
        match &self.trail.last().expect("Just pushed item").node {
            TrieNode::Leaf(ref key, _) | TrieNode::Extension(ref key, _) => {
                let key = NibbleSlice::from_encoded(key).0;
                self.key_nibbles.extend((0..key.len()).map(|i| key.at(i)));
            },
            _ => {}
        }
    }

    fn key(&self) -> Vec<u8> {
        let mut result = <Vec<u8>>::with_capacity(self.key_nibbles.len() / 2);
        let mut i = 1;
        while i < self.key_nibbles.len() {
            result.push(self.key_nibbles[i - 1] * 16 + self.key_nibbles[i]);
            i += 2;
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
            Descend(Result<Box<TrieNode>, String>),
        }
        loop {
            let iter_step = {
                self.trail.last_mut()?.increment();
                let b = self.trail.last().expect("Trail finished.");
                match (b.status.clone(), &b.node) {
                    (CrumbStatus::Exiting, n) => {
                        match n {
                            &TrieNode::Extension(ref key, _) => {
                                let l = self.key_nibbles.len();
                                self.key_nibbles.truncate(l - key.len());
                            },
                            &TrieNode::Branch(_, _) => { self.key_nibbles.pop(); },
                            _ => {}
                        }
                        IterStep::PopTrail
                    },
                    (CrumbStatus::At, TrieNode::Branch(_, value)) => if value.is_some() {
                        let value = value.clone().expect("is_some() called");
                        return Some(Ok((self.key(), DBValue::from_slice(&value))))
                    } else {
                        IterStep::Continue
                    },
                    (CrumbStatus::At, TrieNode::Leaf(_, value)) => {
                        return Some(Ok((self.key(), DBValue::from_slice(value))))
                    },
                    (CrumbStatus::At, TrieNode::Extension(_, child)) => {
                        let next_node = match child {
                            NodeHandle::Hash(hash) => self.trie.retrieve_node(hash).map(Box::new),
                            NodeHandle::InMemory(node) => Ok(node.clone()),
                        };
                        IterStep::Descend(next_node)
                    },
                    (CrumbStatus::AtChild(i), TrieNode::Branch(children, _)) if children[i].is_some() => {
                        match i {
                            0 => self.key_nibbles.push(0),
                            i => *self.key_nibbles.last_mut().expect("Pushed child value before") = i as u8,
                        }
                        let next_node = match &children[i] {
                            Some(NodeHandle::Hash(hash)) => self.trie.retrieve_node(&hash).map(Box::new),
                            Some(NodeHandle::InMemory(node)) => Ok(node.clone()),
                            _ => panic!() // Wrapper with is_some()
                        };
                        IterStep::Descend(next_node)
                    },
                    (CrumbStatus::AtChild(i), TrieNode::Branch(_, _)) => {
                        if i == 0 {
                            self.key_nibbles.push(0);
                        }
                        IterStep::Continue
                    },
                    _ => panic!() // Should never see Entering or AtChild without a Branch here.
                }
            };
            match iter_step {
                IterStep::PopTrail => {
                    self.trail.pop();
                },
                IterStep::Descend(Ok(node)) => {
                    self.descend_into_node(&node)
                },
                IterStep::Descend(Err(e)) => return Some(Err(e)),
                IterStep::Continue => {},
            }
        }
    }
}

pub fn apply_changes(storage: &Arc<KeyValueDB>, col: Option<u32>, changes: DBChanges) -> std::io::Result<()> {
    let mut db_transaction = storage.transaction();
    for (key, value) in changes {
        match value {
            Some(arr) => db_transaction.put(col, key.as_ref(), &arr),
            None => db_transaction.delete(col, key.as_ref())
        }
    }
    storage.write(db_transaction)
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_utils::create_memory_db;

    type TrieChanges = Vec<(Vec<u8>, Option<Vec<u8>>)>;

    fn test_populate_trie(storage: &Arc<KeyValueDB>, trie: &Trie, root: &CryptoHash, changes: TrieChanges) -> CryptoHash {
        let mut other_changes = changes.clone();
        let (db_changes, root) = trie.update(root, other_changes.drain(..));
        apply_changes(storage, Some(0), db_changes).is_ok();
        for (key, value) in changes {
            assert_eq!(trie.get(&root, &key), value);
        }
        root
    }

    fn test_clear_trie(storage: &Arc<KeyValueDB>, trie: &Trie, root: &CryptoHash, changes: TrieChanges) -> CryptoHash {
        let delete_changes: TrieChanges = changes.iter().map(|(key, _)| (key.clone(), None)).collect();
        let mut other_delete_changes = delete_changes.clone();
        let (db_changes, root) = trie.update(root, other_delete_changes.drain(..));
        apply_changes(storage, Some(0), db_changes).is_ok();
        for (key, _) in delete_changes {
            assert_eq!(trie.get(&root, &key), None);
        }
        root
    }

    #[test]
    fn test_basic_trie() {
        let storage: Arc<KeyValueDB> = Arc::new(create_memory_db());
        let trie = Trie::new(storage.clone(), Some(0));
        let empty_root = Trie::empty_root();
        // assert_eq!(trie.get(&empty_root, &[122]), None);
        let changes = vec![
            (b"doge".to_vec(), Some(b"coin".to_vec())),
            (b"docu".to_vec(), Some(b"value".to_vec())),
            (b"do".to_vec(), Some(b"verb".to_vec())),
            (b"horse".to_vec(), Some(b"stallion".to_vec())),
            (b"dog".to_vec(), Some(b"puppy".to_vec())),
            (b"h".to_vec(), Some(b"value".to_vec())),
        ];
        let root = test_populate_trie(&storage, &trie, &empty_root, changes.clone());
        let new_root = test_clear_trie(&storage, &trie, &root, changes);
        assert_eq!(new_root, empty_root);
        assert_eq!(storage.iter(Some(0)).fold(0, |acc, _| acc + 1), 0);
    }

    #[test]
    fn test_trie_iter() {
        let storage: Arc<KeyValueDB> = Arc::new(create_memory_db());
        let trie = Trie::new(storage.clone(), Some(0));
        let pairs = vec![
            (b"a".to_vec(), Some(b"111".to_vec())),
            (b"b".to_vec(), Some(b"222".to_vec())),
            (b"x".to_vec(), Some(b"333".to_vec())),
            (b"y".to_vec(), Some(b"444".to_vec()))];
        let root = test_populate_trie(&storage, &trie, &Trie::empty_root(), pairs.clone());
        let mut iter_pairs = vec![];
        for pair in trie.iter(&root).unwrap() {
            let (key, value) = pair.unwrap();
            iter_pairs.push((key, Some(value.to_vec())));
        }
        assert_eq!(pairs, iter_pairs);

        let mut other_iter = trie.iter(&root).unwrap();
        other_iter.seek(b"r").unwrap();
        assert_eq!(other_iter.next().unwrap().unwrap().0, b"x".to_vec());
    }

    #[test]
    fn test_trie_same_node() {
        let storage: Arc<KeyValueDB> = Arc::new(create_memory_db());
        let trie = Trie::new(storage.clone(), Some(0));
        let changes = vec![
            (b"dogaa".to_vec(), Some(b"puppy".to_vec())),
            (b"dogbb".to_vec(), Some(b"puppy".to_vec())),
            (b"cataa".to_vec(), Some(b"puppy".to_vec())),
            (b"catbb".to_vec(), Some(b"puppy".to_vec())),
            (b"dogax".to_vec(), Some(b"puppy".to_vec())),
        ];
        test_populate_trie(&storage, &trie, &Trie::empty_root(), changes);
    }
}

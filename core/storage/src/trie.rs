use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use primitives::hash::{CryptoHash, hash_struct};
use primitives::traits::{Encode, Decode};
pub use kvdb::{DBValue, KeyValueDB};

use nibble_slice::NibbleSlice;


#[derive(Serialize, Deserialize, Clone, Hash, Debug)]
struct TrieLeaf {
    key: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Hash, Debug)]
struct TrieBranch {
    children: [Option<CryptoHash>; 16],
    /// Optional value, if this is also a last node for some key.
    value: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize, Clone, Hash, Debug)]
struct TrieExtension {
    key: Vec<u8>,
    child: CryptoHash,
}

#[derive(Serialize, Deserialize, Clone, Hash, Debug)]
enum TrieNodeData {
    /// Null trie node. Could be an empty root or an empty branch entry.
    Empty,
    Leaf(TrieLeaf),
    Branch(TrieBranch),
    Extension(TrieExtension),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TrieNode {
    data: TrieNodeData,
    rc: i32,
}

struct TrieNodeStorage {
    storage: Arc<KeyValueDB>,
    column: Option<u32>,
    null_node: CryptoHash,
    nodes: HashMap<CryptoHash, TrieNode>,
    hashes: u32,
}

impl TrieNodeStorage {
    pub fn new(storage: Arc<KeyValueDB>, column: Option<u32>, null_node: CryptoHash) -> Self {
        TrieNodeStorage {
            storage,
            column,
            null_node,
            nodes: HashMap::new(),
            hashes: 0,
        }
    }

    fn add(&mut self, node: TrieNodeData) -> CryptoHash {
        let hash = hash_struct(&node);
        self.hashes += 1;
        match self.get(&hash) {
            Some(_) => {
                self.nodes.entry(hash).and_modify(|e| e.rc += 1);
            },
            None => {
                self.nodes.insert(hash, TrieNode { data: node, rc: 1 });
            }
        };
        hash
    }

    fn delete(&mut self, node: &TrieNode) {
        let hash = hash_struct(&node.data);
        self.hashes += 1;
        if let Some(node) = self.nodes.get_mut(&hash) {
            assert!(node.rc > 0, format!("{:?}", node));
            node.rc -= 1;
        }
    }

    fn replace(&mut self, prev_node: &TrieNode, node: TrieNodeData) -> CryptoHash {
        self.delete(prev_node);
        self.add(node)
    }

    fn emplace(&mut self, prev_node: &TrieNode, node: TrieNodeData) -> TrieNode {
        self.delete(prev_node);
        let hash = self.add(node);
        self.get(&hash).unwrap()
    }

    fn get(&mut self, hash: &CryptoHash) -> Option<TrieNode> {
        if *hash == self.null_node {
            return Some(TrieNode { data: TrieNodeData::Empty, rc: 0 });
        }
        if self.nodes.contains_key(hash) {
            self.nodes.get(hash).cloned()
        } else {
            if let Ok(Some(bytes)) =  self.storage.get(self.column, hash.as_ref()) {
                let node = TrieNode::decode(&bytes);
                match node {
                    Some(n) => {
                        self.nodes.insert(*hash, n.clone());
                        Some(n)
                    },
                    _ => None
                }

            } else {
                None
            }
        }
    }
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

    fn lookup(&self, node_storage: &mut TrieNodeStorage, root: &CryptoHash, mut key: NibbleSlice) -> Result<Option<Vec<u8>>, String> {
        let mut hash = *root;

        for depth in 0.. {
            let node = match node_storage.get(&hash) {
                Some(value) => value,
                None => return Err(format!("Failed to find node for {} key in storage at depth {}", hash, depth))
            };

            match node.data {
                TrieNodeData::Empty => return Ok(None),
                TrieNodeData::Leaf(ref leaf) => {
                    return Ok(if NibbleSlice::from_encoded(&leaf.key).0 == key { Some(leaf.value.clone()) } else { None });
                },
                TrieNodeData::Extension(ref ext) => {
                    let existing_key = NibbleSlice::from_encoded(&ext.key).0;
                    if key.starts_with(&existing_key) {
                        hash = ext.child;
                        key = key.mid(existing_key.len());
                    } else {
                        return Ok(None);
                    }
                },
                TrieNodeData::Branch(ref branch) => if key.is_empty() {
                    return Ok(branch.value.clone());
                } else {
                    match branch.children[key.at(0) as usize] {
                        Some(x) => {
                            hash = x;
                            key = key.mid(1);
                        },
                        None => return Ok(None),
                    }
                }
            };
        }
        Ok(None)
    }

    pub fn get(&self, root: &CryptoHash, key: &[u8]) -> Option<Vec<u8>> {
        let mut node_storage = TrieNodeStorage::new(self.storage.clone(), self.column, self.null_node);
        let key = NibbleSlice::new(key);
        match self.lookup(&mut node_storage, root, key) {
            Ok(value) => value,
            Err(err) => {
                println!("Failed to lookup: {}", err);
                None
            }
        }
    }

    fn insert(&self, node_storage: &mut TrieNodeStorage, node: &TrieNode, partial: NibbleSlice, value: Vec<u8>) -> Result<CryptoHash, String> {
        match node.data {
            TrieNodeData::Empty => {
                let leaf_node = TrieNodeData::Leaf(TrieLeaf { key: partial.encoded(true).into_vec(), value });
                Ok(node_storage.add(leaf_node))
            },
            TrieNodeData::Branch(ref branch) => {
                // If the key ends here, store the value in branch's value.
                if partial.is_empty() {
                    let mut branch_node = TrieBranch { children: [None; 16], value: Some(value.to_vec()) };
                    branch_node.children.clone_from_slice(&branch.children);
                    Ok(node_storage.replace(node, TrieNodeData::Branch(branch_node)))
                } else {
                    let idx = partial.at(0) as usize;
                    let partial = partial.mid(1);
                    let hash = branch.children[idx];
                    let new_hash = match hash {
                        Some(hash) => {
                            match node_storage.get(&hash) {
                                Some(child_node) => self.insert(node_storage, &child_node, partial, value),
                                None => Err(format!("Failed to lookup expected {} node", hash))
                            }?
                        },
                        _ => {
                            let leaf_node = TrieNodeData::Leaf(TrieLeaf { key: partial.encoded(true).into_vec(), value });
                            node_storage.add(leaf_node)
                        }
                    };
                    let mut branch_node = TrieBranch { children: [None; 16], value: branch.value.clone() };
                    branch_node.children.clone_from_slice(&branch.children);
                    branch_node.children[idx] = Some(new_hash);
                    Ok(node_storage.replace(node, TrieNodeData::Branch(branch_node)))
                }
            },
            TrieNodeData::Leaf(ref leaf) => {
                let existing_key = NibbleSlice::from_encoded(&leaf.key).0;
                let common_prefix = partial.common_prefix(&existing_key);
                if common_prefix == existing_key.len() && common_prefix == partial.len() {
                    // Equivalent leaf.
                    let new_leaf = TrieNodeData::Leaf(TrieLeaf { key: leaf.key.clone(), value: value.to_vec()});
                    Ok(node_storage.add(new_leaf))
                } else if common_prefix == 0 {
                    let mut children = [None; 16];
                    let branch_node = if existing_key.is_empty() {
                        TrieBranch { children, value: Some(leaf.value.clone()) }
                    } else {
                        let idx = existing_key.at(0) as usize;
                        let hash = node_storage.add(TrieNodeData::Leaf(TrieLeaf {key: existing_key.mid(1).encoded(true).into_vec(), value: leaf.value.clone()}));
                        children[idx] = Some(hash);
                        TrieBranch { children, value: None }
                    };
                    let branch_node = node_storage.emplace(node, TrieNodeData::Branch(branch_node));
                    self.insert(node_storage, &branch_node, partial, value)
                } else if common_prefix == existing_key.len() {
                    let branch_node = node_storage.emplace(node, TrieNodeData::Branch(TrieBranch { children: [None; 16], value: Some(leaf.value.clone()) }));
                    self.insert(node_storage, &branch_node, partial.mid(common_prefix), value)
                } else {
                    // Partially shared prefix: convert to leaf and call recursively to add a branch.
                    let low = TrieNodeData::Leaf(TrieLeaf { key: existing_key.mid(common_prefix).encoded(true).into_vec(), value: leaf.value.clone()});
                    let low = node_storage.emplace(node, low);
                    let child = self.insert(node_storage, &low, partial.mid(common_prefix), value)?;
                    Ok(node_storage.add(TrieNodeData::Extension(
                        TrieExtension { key: partial.encoded_leftmost(common_prefix, false).into_vec(), child })))
                }
            },
            TrieNodeData::Extension(ref ext) => {
                let existing_key = NibbleSlice::from_encoded(&ext.key).0;
                let common_prefix = partial.common_prefix(&existing_key);
                if common_prefix == 0 {
                    let idx = existing_key.at(0) as usize;
                    let mut children = [None; 16];
                    children[idx] = if existing_key.len() == 1 {
                        Some(ext.child)
                    } else {
                        Some(node_storage.add(TrieNodeData::Extension(
                            TrieExtension {key: existing_key.mid(1).encoded(false).into_vec(), child: ext.child})))
                    };
                    let branch_node = node_storage.emplace(node, TrieNodeData::Branch(TrieBranch { children, value: None }));
                    self.insert(node_storage, &branch_node, partial, value)
                } else if common_prefix == existing_key.len() {
                    match node_storage.get(&ext.child) {
                        Some(child) => {
                            let child = self.insert(node_storage, &child, partial.mid(common_prefix), value)?;
                            Ok(node_storage.replace(node, TrieNodeData::Extension(TrieExtension { key: ext.key.clone(), child })))
                        },
                        None => Err("Missing child for extension".to_string())
                    }
                } else {
                    // Partially shared prefix: covert to shorter extension and recursively add a branch.
                    let low = TrieNodeData::Extension(TrieExtension { key: existing_key.mid(common_prefix).encoded(false).into_vec(), child: ext.child});
                    let low = node_storage.emplace(node, low);
                    let child = self.insert(node_storage, &low, partial.mid(common_prefix), value)?;
                    Ok(node_storage.add(TrieNodeData::Extension(TrieExtension { key: existing_key.encoded_leftmost(common_prefix, false).into_vec(), child })))
                }
            }
        }
    }

    fn delete(&self, node_storage: &mut TrieNodeStorage, node: &TrieNode, partial: NibbleSlice) -> Result<Option<CryptoHash>, String> {
        match node.data {
            TrieNodeData::Empty => {
                Err("Removing empty node".to_string())
            },
            TrieNodeData::Leaf(ref leaf) => {
                if NibbleSlice::from_encoded(&leaf.key).0 == partial {
                    node_storage.delete(node);
                    Ok(None)
                } else {
                    Err("Deleting missing leaf node".to_string())
                }
            },
            TrieNodeData::Branch(ref branch) => {
                if partial.is_empty() {
                    let mut branch_node = TrieBranch { children: [None; 16], value: None };
                    branch_node.children.clone_from_slice(&branch.children);
                    if branch_node.children.iter().filter(|x| x.is_some()).count() == 0 {
                        node_storage.delete(&node);
                        Ok(None)
                    } else {
                        Ok(Some(node_storage.replace(&node, TrieNodeData::Branch(branch_node))))
                    }
                } else {
                    let idx = partial.at(0) as usize;
                    if let Some(hash) = branch.children[idx] {
                        match node_storage.get(&hash) {
                            Some(child) => {
                                let new_value = self.delete(node_storage, &child, partial.mid(1))?;
                                let mut branch_node = TrieBranch { children: [None; 16], value: branch.value.clone() };
                                branch_node.children.clone_from_slice(&branch.children);
                                branch_node.children[idx] = new_value;
                                node_storage.delete(&node);
                                if branch_node.children.iter().filter(|x| x.is_some()).count() == 0 && branch_node.value.is_none() {
                                    Ok(None)
                                } else {
                                    Ok(Some(node_storage.add(TrieNodeData::Branch(branch_node))))
                                }
                            },
                            None => Err(format!("Failed to lookup expected {} node", hash))
                        }
                    } else {
                        Err("Deleting missing leaf node".to_string())
                    }
                }
            },
            TrieNodeData::Extension(ref ext) => {
                let (common_prefix, existing_len) = {
                    let existing_key = NibbleSlice::from_encoded(&ext.key).0;
                    (existing_key.common_prefix(&partial), existing_key.len())
                };
                if common_prefix == existing_len {
                    node_storage.delete(node);
                    match node_storage.get(&ext.child) {
                        Some(sub_node) => {
                            match self.delete(node_storage, &sub_node, partial.mid(common_prefix)) {
                                Ok(Some(new_value)) => {
                                    let ext_node = TrieNodeData::Extension(TrieExtension { key: ext.key.clone(), child: new_value });
                                    Ok(Some(node_storage.add(ext_node)))
                                },
                                Ok(None) => Ok(None),
                                Err(e) => Err(e),
                            }
                        },
                        None => Err(format!("Failed to lookup expected {} node", ext.child))
                    }
                } else {
                    Err("No node".to_string())
                }
            }
        }
    }

    #[allow(dead_code)]
    fn print_node(&self, node_storage: &mut TrieNodeStorage, node: &TrieNode) {
        println!("{:?}: {:?}", hash_struct(&node.data), node);
        match node.data {
            TrieNodeData::Branch(ref branch) => {
                for child in branch.children.iter() {
                    match child {
                        Some(hash) => {
                            let n = node_storage.get(&hash).expect("Printing node failed");
                            self.print_node(node_storage, &n)
                        },
                        None => (),
                    };
                }
            },
            TrieNodeData::Extension(ref ext) => {
                let n = node_storage.get(&ext.child).expect("Printing node failed");
                self.print_node(node_storage, &n);
            },
            _ => (),
        }
    }

    #[allow(dead_code)]
    fn present(&self, node_storage: &mut TrieNodeStorage, root: CryptoHash) {
        let root_node = node_storage.get(&root).expect("Printing node failed");
        self.print_node(node_storage, &root_node);
    }

    pub fn update<I>(&self, root: &CryptoHash, changes: I) -> (DBChanges, CryptoHash)
            where I: Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>{
        let mut node_storage = TrieNodeStorage::new(self.storage.clone(), self.column, self.null_node);
        let mut last_root = *root;
        for (key, value) in changes {
            let mut root_node = node_storage.get(&last_root).expect("Failed to find root");
            let key = NibbleSlice::new(&key);
            match value {
                Some(arr) => {
//                    println!("\nInserting {:?}", key);
                    last_root = self.insert(&mut node_storage, &root_node, key, arr).expect("Failed to insert");
//                    self.present(&mut node_storage, last_root);
                },
                None => {
//                    println!("\nDeleting {:?}", key);
                    last_root = match self.delete(&mut node_storage, &root_node, key).expect("Failed to delete") {
                        Some(value) => value,
                        None => self.null_node
                    };
//                    self.present(&mut node_storage, last_root);
                }
            }
        }
        let mut db_changes = HashMap::default();
        for (hash, node) in node_storage.nodes {
            if node.rc > 0 {
                db_changes.insert(hash.as_ref().to_vec(), node.encode());
            } else {
                db_changes.insert(hash.as_ref().to_vec(), None);
            }
        }
        // println!("Hashes: {}", node_storage.hashes);
        (db_changes, last_root)
    }

    pub fn iter<'a>(&'a self, root: &CryptoHash) -> Result<Box<TrieIterator<'a>>, String> {
        TrieIterator::new(self, root).map(|iter| Box::new(iter) as Box<_>)
    }
}

pub type TrieItem<'a> = Result<(Vec<u8>, DBValue), String>;

enum CrumbStatus {
    Entering,
    At,
    AtChild(usize),
    Exiting,
}

struct Crumb {
    node: TrieNode,
    status: CrumbStatus,
}

pub struct TrieIterator<'a> {
    db: &'a Trie,
    trail: Vec<Crumb>,
    key_nibbles: Vec<u8>,
    root: CryptoHash,
    node_storage: TrieNodeStorage,
}

impl<'a> TrieIterator<'a> {
    /// Create a new iterator.
    pub fn new(db: &'a Trie, root: &CryptoHash) -> Result<TrieIterator<'a>, String> {
        let mut r = TrieIterator {
            db,
            trail: Vec::with_capacity(8),
            key_nibbles: Vec::with_capacity(64),
            root: *root,
            node_storage: TrieNodeStorage::new(db.storage.clone(), db.column, db.null_node)
        };
        if let Some(node) = r.node_storage.get(root) {
            r.trail.push(Crumb {node, status: CrumbStatus::At});
            return Ok(r);
        }
        Err(format!("Root hash {} not found", root))
    }

    /// Position the iterator on the first element with key => `key`.
    pub fn seek(&mut self, key: &[u8]) -> Result<(), String> {
        self.trail.clear();
        self.key_nibbles.clear();
        let mut hash = self.root;
        let mut key = NibbleSlice::new(key);
        loop {
           if let Some(node) = self.node_storage.get(&hash) {
               match node.data {
                   TrieNodeData::Empty => return Ok(()),
                   TrieNodeData::Leaf(ref leaf) => {
                       let existing_key = NibbleSlice::from_encoded(&leaf.key).0;
                        self.trail.push(Crumb {
                            status: if existing_key >= key { CrumbStatus::Entering } else { CrumbStatus::Exiting },
                            node: node.clone(),
                        });
                       self.key_nibbles.extend(existing_key.iter());
                       return Ok(())
                   },
                   TrieNodeData::Branch(ref branch) => {
                       if key.is_empty() {
                           self.trail.push(Crumb {
                               status: CrumbStatus::Entering,
                               node: node.clone()
                           });
                           return Ok(())
                       } else {
                           let idx = key.at(0) as usize;
                           self.trail.push(Crumb {
                              status: CrumbStatus::AtChild(idx as usize),
                               node: node.clone()
                           });
                           self.key_nibbles.push(key.at(0));
                           if let Some(ref child) = branch.children[idx] {
                               hash = *child;
                               key = key.mid(1);
                           } else {
                               return Ok(());
                           }
                       }
                   },
                   TrieNodeData::Extension(ref ext) => {
                       let existing_key = NibbleSlice::from_encoded(&ext.key).0;
                       if key.starts_with(&existing_key) {
                           self.trail.push(Crumb {
                              status: CrumbStatus::At,
                               node: node.clone()
                           });
                           self.key_nibbles.extend(existing_key.iter());
                           hash = ext.child;
                           key = key.mid(existing_key.len());
                       } else {
                           // ???
                           return Ok(());
                       }
                   }
               }
           } else {
               return Err(format!("Node {} not found", hash));
           }
        }
    }

    fn descend_into_node(&mut self, node: &TrieNode) {
        self.trail.push(Crumb {
           status: CrumbStatus::Entering, node: node.clone()
        });
//        match &self.trail.last().expect("Just pushed item").node.data {
//            TrieNodeData::
//        }
    }
}

impl<'a> Iterator for TrieIterator<'a> {
    type Item = TrieItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(Ok((vec![], DBValue::from_slice(b"test"))))
//        enum IterStep<'b> {
//            Continue,
//            PopTrail,
//            Descend(Result<Cow<'b, DBValue>, String>),
//        }
//        loop {
//            let iter_step = {
//
//            };
//
//            match iter_step {
//                IterStep::PopTrail => {
//                    self.trail.pop();
//                },
//                IterStep::Descend(Ok(d)) => {
//                    let node =
//                },
//                IterStep::Descend(Err(r)) => {
//                    return Some(Err(e))
//                },
//                IterStep::Continue => {},
//            }
//        }
    }
}

pub fn apply_changes(storage: &Arc<KeyValueDB>, col: Option<u32>, changes: DBChanges) -> std::io::Result<()> {
    let mut db_transaction = storage.transaction();
    for (key, value) in changes {
        // println!("{:?} {:?}", CryptoHash::new(&key), value.is_some());
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
        assert_eq!(trie.get(&empty_root, &[122]), None);
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
//        let mut iter_pairs = vec![];
//        for pair in trie.iter(&root).unwrap() {
//            let (key, value) = pair.unwrap();
//            iter_pairs.push((key, Some(value.to_vec())));
//        }
//        assert_eq!(pairs, iter_pairs);

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

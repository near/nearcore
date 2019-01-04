use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use primitives::hash::{CryptoHash, hash_struct};
use primitives::traits::{Encode, Decode};
pub use kvdb::{DBValue, KeyValueDB};

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
    rc: u32,
}

struct TrieNodeStorage {
    storage: Arc<KeyValueDB>,
    column: Option<u32>,
    null_node: CryptoHash,
    nodes: HashMap<CryptoHash, TrieNode>,
    remove_nodes: HashSet<CryptoHash>,
}

impl TrieNodeStorage {
    pub fn new(storage: Arc<KeyValueDB>, column: Option<u32>, null_node: CryptoHash) -> Self {
        TrieNodeStorage {
            storage,
            column,
            null_node,
            nodes: HashMap::new(),
            remove_nodes: HashSet::new(),
        }
    }

    fn add(&mut self, node: TrieNodeData) -> CryptoHash {
        let hash = hash_struct(&node);
        match self.get(&hash) {
            Some(_) => {
                self.nodes.entry(hash).and_modify(|e| e.rc += 1);
            },
            None => {
                self.nodes.insert(hash, TrieNode{ data: node, rc: 1 });
            }
        };
        hash
    }

    fn delete(&mut self, node: &TrieNode) {
        let hash = hash_struct(node);
        // Is there better way to implement this?
        if self.get(&hash).is_some() {
            self.nodes.entry(hash).and_modify(|e| {
                e.rc -= 1;
            });
            if self.nodes.get(&hash).expect("Just modified").rc == 0 {
                self.remove_nodes.insert(hash);
            }
        }
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

/// Converts array of bytes to array of half-bytes (e.g. values 0-15).
fn vec_to_nibbles(key: &[u8]) -> Vec<u8> {
    let mut result = vec![];
    for value in key {
        result.push(value / 16);
        result.push(value % 16);
    }
    result
}

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

    fn lookup(&self, node_storage: &mut TrieNodeStorage, node: &TrieNode, nibbles: &[u8], position: usize) -> Result<Option<Vec<u8>>, String> {
        match node.data {
            TrieNodeData::Empty => Ok(None),
            TrieNodeData::Branch(ref branch) => {
                // If the key ends at the given branch, return it's value.
                if nibbles.len() == position {
                    Ok(branch.value.clone())
                } else {
                    let key = branch.children[nibbles[position] as usize];
                    match key {
                        Some(hash) => {
                            match &node_storage.get(&hash) {
                                Some(node) => self.lookup(node_storage, node, nibbles, position + 1),
                                None => Err(format!("Failed to find node for {} key in storage", hash)),
                            }
                        },
                        // key is not in children of give leaf.
                        _ => Ok(None)
                    }
                }
            },
            TrieNodeData::Leaf(ref leaf) => {
                let key = leaf.key.as_slice();
                if *key == nibbles[position..] {
                    Ok(Some(leaf.value.clone()))
                } else {
                    Ok(None)
                }
            },
            TrieNodeData::Extension(ref ext) => {
                if nibbles[position..].starts_with(&ext.key) {
                    match node_storage.get(&ext.child) {
                        Some(node) => self.lookup(node_storage, &node, nibbles, position + ext.key.len()),
                        None => Err(format!("Failed to find node for {} key in storage", ext.child))
                    }
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// TODO: do we want to propagate errors?
    pub fn get(&self, root: &CryptoHash, key: &[u8]) -> Option<Vec<u8>> {
        let nibble = vec_to_nibbles(key);
        let mut node_storage = TrieNodeStorage::new(self.storage.clone(), self.column, self.null_node);
        // let root = self.node_by_hash(&node_storage, root).expect("Root hash is not found");
        let root = node_storage.get(root).expect("Root hash is not found");
        match self.lookup(&mut node_storage, &root, &nibble, 0) {
            Ok(value) => value,
            Err(_) => {
                println!("Failed to lookup");
                None
            }
        }
    }

    fn insert(&self, node_storage: &mut TrieNodeStorage, node: &TrieNode, nibbles: &[u8], position: usize, value: &[u8]) -> Result<CryptoHash, String> {
        match node.data {
            TrieNodeData::Empty => {
                let leaf_node = TrieNodeData::Leaf(TrieLeaf { key: nibbles[position..].to_vec(), value: value.to_vec() });
                Ok(node_storage.add(leaf_node))
            },
            TrieNodeData::Branch(ref branch) => {
                // If the key ends here, store the value in branch's value.
                if position == nibbles.len() {
                    let mut branch_node = TrieBranch { children: [None; 16], value: Some(value.to_vec()) };
                    branch_node.children.clone_from_slice(&branch.children);
                    node_storage.delete(node);
                    Ok(node_storage.add(TrieNodeData::Branch(branch_node)))
                } else {
                    let hash = branch.children[nibbles[position] as usize];
                    let new_hash = match hash {
                        Some(hash) => {
                            match node_storage.get(&hash) {
                                Some(child_node) => self.insert(node_storage, &child_node, nibbles, position + 1, value),
                                None => Err(format!("Failed to lookup expected {} node", hash))
                            }?
                        },
                        _ => {
                            let leaf_node = TrieNodeData::Leaf(TrieLeaf { key: nibbles[position + 1..].to_vec(), value: value.to_vec() });
                            node_storage.add(leaf_node)
                        }
                    };
                    let mut branch_node = TrieBranch { children: [None; 16], value: branch.value.clone() };
                    branch_node.children.clone_from_slice(&branch.children);
                    branch_node.children[nibbles[position] as usize] = Some(new_hash);
                    node_storage.delete(node);
                    Ok(node_storage.add(TrieNodeData::Branch(branch_node)))
                }
            },
            TrieNodeData::Leaf(ref leaf) => {
                // If this is the same key.
                if *(leaf.key) == nibbles[position..] {
                    let new_leaf = TrieNodeData::Leaf(TrieLeaf { key: leaf.key.clone(), value: value.to_vec()});
                    return Ok(node_storage.add(new_leaf));
                }
                // Check if given key has common prefix with this leaf.
                let mut prefix = 0;
                while prefix < leaf.key.len() && prefix + position < nibbles.len() && leaf.key[prefix] == nibbles[position + prefix] {
                    prefix += 1;
                }
                let mut branch_value = None;
                if prefix + position == nibbles.len() {
                    branch_value = Some(value.to_vec());
                } else if prefix == leaf.key.len() {
                    branch_value = Some(leaf.value.clone());
                }
                let mut branch_node = TrieBranch { children: [None; 16], value: branch_value };
                if prefix < leaf.key.len() {
                    let prev_leaf = TrieNodeData::Leaf(TrieLeaf { key: leaf.key[prefix + 1..].to_vec(), value: leaf.value.clone() });
                    branch_node.children[leaf.key[prefix] as usize] = Some(node_storage.add(prev_leaf));
                }
                if prefix + position < nibbles.len() {
                    let new_leaf = TrieNodeData::Leaf(TrieLeaf { key: nibbles[position + prefix + 1..].to_vec(), value: value.to_vec()});
                    branch_node.children[nibbles[position + prefix] as usize] = Some(node_storage.add(new_leaf));
                }
                node_storage.delete(node);
                let branch_hash = node_storage.add(TrieNodeData::Branch(branch_node));
                if prefix > 0 {
                    let ext_node = TrieNodeData::Extension(TrieExtension { key: nibbles[position..position + prefix].to_vec(), child: branch_hash });
                    Ok(node_storage.add(ext_node))
                } else {
                    Ok(branch_hash)
                }
            },
            TrieNodeData::Extension(ref ext) => {
                let mut prefix = 0;
                while prefix < ext.key.len() && prefix + position < nibbles.len() && ext.key[prefix] == nibbles[position + prefix] {
                    prefix += 1;
                }
                if prefix == ext.key.len() {
                    match node_storage.get(&ext.child) {
                        Some(child_node) => {
                            let new_hash = self.insert(node_storage, &child_node, nibbles, position + ext.key.len(), value)?;
                            node_storage.delete(node);
                            Ok(node_storage.add(TrieNodeData::Extension(TrieExtension { key: ext.key.clone(), child: new_hash })))
                        },
                        None => {
                            Err(format!("Failed to look up expected {} node", ext.child))
                        }
                    }
                } else {
                    let mut branch_value = None;
                    if nibbles.len() == position + prefix {
                        branch_value = Some(value.to_vec());
                    }
                    let mut branch_node = TrieBranch { children: [None; 16], value: branch_value };
                    if ext.key.len() > prefix + 1 {
                        let prev_ext = TrieNodeData::Extension(TrieExtension { key: ext.key[prefix + 1..].to_vec(), child: ext.child });
                        branch_node.children[ext.key[prefix] as usize] = Some(node_storage.add(prev_ext));
                    } else {
                        branch_node.children[ext.key[prefix] as usize] = Some(ext.child);
                    }
                    if nibbles.len() > position + prefix {
                        let new_leaf = TrieNodeData::Leaf(TrieLeaf { key: nibbles[position + prefix + 1..].to_vec(), value: value.to_vec()});
                        branch_node.children[nibbles[position + prefix] as usize] = Some(node_storage.add(new_leaf));
                    }
                    node_storage.delete(node);
                    let branch_hash = node_storage.add(TrieNodeData::Branch(branch_node));
                    if prefix > 0 {
                        let ext_node = TrieNodeData::Extension(TrieExtension { key: nibbles[position..position + prefix].to_vec(), child: branch_hash});
                        Ok(node_storage.add(ext_node))
                    } else {
                        Ok(branch_hash)
                    }
                }
            }
        }
    }

    fn delete(&self, node_storage: &mut TrieNodeStorage, node: &TrieNode, nibbles: &[u8], position: usize) -> Result<Option<CryptoHash>, String> {
        match node.data {
            TrieNodeData::Empty => {
                Err("Removing empty node".to_string())
            },
            TrieNodeData::Leaf(ref leaf) => {
                if *leaf.key == nibbles[position..] {
                    node_storage.delete(node);
                    Ok(None)
                } else {
                    Err("Deleting missing leaf node".to_string())
                }
            },
            TrieNodeData::Branch(ref branch) => {
                if position == nibbles.len() {
                    node_storage.delete(node);
                    let mut branch_node = TrieBranch { children: [None; 16], value: None };
                    branch_node.children.clone_from_slice(&branch.children);
                    if branch_node.children.iter().filter(|x| x.is_some()).count() == 0 {
                        Ok(None)
                    } else {
                        Ok(Some(node_storage.add(TrieNodeData::Branch(branch_node))))
                    }
                } else {
                    let hash = branch.children[nibbles[position] as usize];
                    match hash {
                        Some(hash) => {
                            match node_storage.get(&hash) {
                                Some(sub_node) => {
                                    match self.delete(node_storage, &sub_node, nibbles, position + 1) {
                                        Ok(new_value) => {
                                            let mut branch_node = TrieBranch { children: [None; 16], value: branch.value.clone() };
                                            branch_node.children.clone_from_slice(&branch.children);
                                            branch_node.children[nibbles[position] as usize] = new_value;
                                            if branch_node.children.iter().filter(|x| x.is_some()).count() == 0 && branch_node.value.is_none() {
                                                Ok(None)
                                            } else {
                                                Ok(Some(node_storage.add(TrieNodeData::Branch(branch_node))))
                                            }
                                        },
                                        Err(e) => Err(e)
                                    }
                                },
                                None => Err(format!("Failed to lookup expected {} node", hash))
                            }
                        },
                        _ => Err("Deleting missing leaf node".to_string())
                    }
                }
            },
            TrieNodeData::Extension(ref ext) => {
                let mut prefix = 0;
                while prefix < ext.key.len() && prefix + position < nibbles.len() && ext.key[prefix] == nibbles[prefix + position] {
                    prefix += 1;
                }
                if prefix == ext.key.len() {
                    match node_storage.get(&ext.child) {
                        Some(sub_node) => {
                            match self.delete(node_storage, &sub_node, nibbles, position + ext.key.len()) {
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
        println!("{:?}: {:?}", hash_struct(&node), node);
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
            let nibbles = vec_to_nibbles(&key);
            match value {
                Some(arr) => {
//                    println!("\nInserting {:?}, nibbles = {:?}", key, nibbles);
                    last_root = self.insert(&mut node_storage, &root_node, &nibbles, 0, &arr).expect("Failed to insert");
//                    self.present(&mut node_storage, last_root);
                },
                None => {
                    // println!("\nDeleting {:?}, nibbles = {:?}", key, nibbles);
                    last_root = match self.delete(&mut node_storage, &root_node, &nibbles, 0).expect("Failed to delete") {
                        Some(value) => value,
                        None => self.null_node
                    };
//                    self.present(&node_storage, last_root);
                }
            }
        }
        let mut db_changes = HashMap::default();
        for (hash, node) in node_storage.nodes {
            if !node_storage.remove_nodes.contains(&hash) && node.rc > 0 {
                db_changes.insert(hash.as_ref().to_vec(), node.encode());
            }
        }
        for hash in node_storage.remove_nodes {
            db_changes.insert(hash.as_ref().to_vec(), None);
        }
        (db_changes, last_root)
    }
}

pub fn apply_changes(storage: &Arc<KeyValueDB>, col: Option<u32>, changes: DBChanges) -> std::io::Result<()> {
    let mut db_transaction = storage.transaction();
    for (key, value) in changes {
//        println!("{:?} {:?}", key, value);
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

use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use primitives::hash::{CryptoHash, hash_struct};
use primitives::traits::{Encode, Decode};
pub use kvdb::{DBValue, KeyValueDB};

#[derive(Serialize, Deserialize, Clone, Hash, Debug)]
struct TrieLeaf {
    key: Vec<u8>,
    value: Vec<u8>
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
enum TrieNode {
    /// Null trie node. Could be an empty root or an empty branch entry.
    Empty,
    Leaf(TrieLeaf),
    Branch(TrieBranch),
    Extension(TrieExtension),
}

struct TrieNodeStorage {
    nodes: HashMap<CryptoHash, TrieNode>,
    remove_nodes: HashSet<CryptoHash>,
}

impl TrieNodeStorage {
    pub fn new() -> Self {
        TrieNodeStorage {
            nodes: HashMap::new(),
            remove_nodes: HashSet::new(),
        }
    }

    fn add(&mut self, node: TrieNode) -> CryptoHash {
        let hash = hash_struct(&node);
        self.nodes.insert(hash, node);
        hash
    }

    fn delete(&mut self, node: &TrieNode) {
        let hash = hash_struct(node);
        if self.nodes.contains_key(&hash) {
            self.nodes.remove(&hash);
        } else {
            self.remove_nodes.insert(hash);
        }
    }
}

pub struct Trie {
    storage: Arc<KeyValueDB>,
    column: Option<u32>,
    null_node: CryptoHash,
}

// pub type TrieChanges = HashMap<Vec<u8>, Option<Vec<u8>>>;
pub type TrieChanges = Vec<(Vec<u8>, Option<Vec<u8>>)>;
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
        hash_struct(&TrieNode::Empty)
    }

    fn node_by_hash(&self, node_storage: &TrieNodeStorage, hash: &CryptoHash) -> Result<TrieNode, String> {
        if *hash == self.null_node {
            return Ok(TrieNode::Empty);
        }
        match node_storage.nodes.get(hash) {
            Some(node) => Ok(node.clone()),
            None => {
                if let Ok(Some(bytes)) =  self.storage.get(self.column, hash.as_ref()) {
                    let node = TrieNode::decode(&bytes);
                    match node {
                        Some(n) => Ok(n),
                        _ => Err(format!("Storage value for key {:?} could not decode into TrieNode", hash))
                    }

                } else {
                    Err(format!("Key {:?} is not found in storage.", hash))
                }
            }
        }
    }

    fn lookup(&self, node_storage: &TrieNodeStorage, node: &TrieNode, nibbles: &[u8], position: usize) -> Result<Option<Vec<u8>>, String> {
        match node {
            TrieNode::Empty => Ok(None),
            TrieNode::Branch(branch) => {
                // If the key ends at the given branch, return it's value.
                if nibbles.len() == position {
                    Ok(branch.value.clone())
                } else {
                    let key = branch.children[nibbles[position] as usize];
                    match key {
                        Some(hash) => self.lookup(node_storage,&self.node_by_hash(node_storage, &hash)?, nibbles, position + 1),
                        // key is not in children of give leaf.
                        _ => Ok(None)
                    }
                }
            },
            TrieNode::Leaf(leaf) => {
                let key = leaf.key.as_slice();
                if *key == nibbles[position..] {
                    Ok(Some(leaf.value.clone()))
                } else {
                    Ok(None)
                }
            },
            TrieNode::Extension(ext) => {
                if nibbles[position..].starts_with(&ext.key) {
                    self.lookup(node_storage, &self.node_by_hash(node_storage, &ext.child)?, nibbles, position + ext.key.len())
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// TODO: do we want to propagate errors?
    pub fn get(&self, root: &CryptoHash, key: &[u8]) -> Option<Vec<u8>> {
        let nibble = vec_to_nibbles(key);
        let node_storage = TrieNodeStorage::new();
        let root = self.node_by_hash(&node_storage, root).expect("Root hash is not found");
        match self.lookup(&node_storage, &root, &nibble, 0) {
            Ok(value) => value,
            Err(_) => {
                println!("Failed to lookup");
                None
            }
        }
    }

    fn insert(&self, node_storage: &mut TrieNodeStorage, node: &TrieNode, nibbles: &[u8], position: usize, value: &[u8]) -> Result<CryptoHash, String> {
        match node {
            TrieNode::Empty => {
                let leaf_node = TrieNode::Leaf(TrieLeaf { key: nibbles[position..].to_vec(), value: value.to_vec() });
                Ok(node_storage.add(leaf_node))
            },
            TrieNode::Branch(branch) => {
                let hash = branch.children[nibbles[position] as usize];
                // If the key ends here, store the value in branch's value.
                if position + 1 == nibbles.len() {
                    let mut branch_node = TrieBranch { children: [None; 16], value: Some(value.to_vec()) };
                    branch_node.children.clone_from_slice(&branch.children);
                    node_storage.delete(node);
                    Ok(node_storage.add(TrieNode::Branch(branch_node)))
                } else {
                    let new_hash = match hash {
                        Some(hash) => {
                            let child_node = self.node_by_hash(&node_storage, &hash)?;
                            self.insert(node_storage, &child_node, nibbles, position + 1, value)?
                        },
                        _ => {
                            let leaf_node = TrieNode::Leaf(TrieLeaf { key: nibbles[position + 1..].to_vec(), value: value.to_vec() });
                            node_storage.add(leaf_node)
                        }
                    };
                    let mut branch_node = TrieBranch { children: [None; 16], value: branch.value.clone() };
                    branch_node.children.clone_from_slice(&branch.children);
                    branch_node.children[nibbles[position] as usize] = Some(new_hash);
                    node_storage.delete(node);
                    Ok(node_storage.add(TrieNode::Branch(branch_node)))
                }
            },
            TrieNode::Leaf(leaf) => {
                // If this is the same key.
                if *(leaf.key) == nibbles[position..] {
                    let new_leaf = TrieNode::Leaf(TrieLeaf { key: leaf.key.clone(), value: value.to_vec()});
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
                    let prev_leaf = TrieNode::Leaf(TrieLeaf { key: leaf.key[prefix + 1..].to_vec(), value: leaf.value.clone() });
                    branch_node.children[leaf.key[prefix] as usize] = Some(node_storage.add(prev_leaf));
                }
                if prefix + position < nibbles.len() {
                    let new_leaf = TrieNode::Leaf(TrieLeaf { key: nibbles[position + prefix + 1..].to_vec(), value: value.to_vec()});
                    branch_node.children[nibbles[position + prefix] as usize] = Some(node_storage.add(new_leaf));
                }
                node_storage.delete(node);
                let branch_hash = node_storage.add(TrieNode::Branch(branch_node));
                if prefix > 0 {
                    let ext_node = TrieNode::Extension(TrieExtension { key: nibbles[position..position + prefix].to_vec(), child: branch_hash });
                    Ok(node_storage.add(ext_node))
                } else {
                    Ok(branch_hash)
                }
            },
            TrieNode::Extension(ext) => {
                let mut prefix = 0;
                while prefix < ext.key.len() && prefix + position < nibbles.len() && ext.key[prefix] == nibbles[position + prefix] {
                    prefix += 1;
                }
                if prefix == ext.key.len() {
                    let child_node = self.node_by_hash(&node_storage, &ext.child)?;
                    let new_hash = self.insert(node_storage, &child_node, nibbles, position + ext.key.len(), value)?;
                    node_storage.delete(node);
                    Ok(node_storage.add(TrieNode::Extension(TrieExtension { key: ext.key.clone(), child: new_hash })))
                } else {
                    let mut branch_value = None;
                    if nibbles.len() == position + prefix {
                        branch_value = Some(value.to_vec());
                    }
                    let mut branch_node = TrieBranch { children: [None; 16], value: branch_value };
                    if ext.key.len() > prefix {
                        let prev_ext = TrieNode::Extension(TrieExtension { key: ext.key[prefix + 1..].to_vec(), child: ext.child });
                        branch_node.children[ext.key[prefix] as usize] = Some(node_storage.add(prev_ext));
                    } else {
                        branch_node.children[ext.key[prefix] as usize] = Some(ext.child);
                    }
                    if nibbles.len() > position + prefix {
                        let new_leaf = TrieNode::Leaf(TrieLeaf { key: nibbles[position + prefix + 1..].to_vec(), value: value.to_vec()});
                        branch_node.children[nibbles[position + prefix] as usize] = Some(node_storage.add(new_leaf));
                    }
                    node_storage.delete(node);
                    let branch_hash = node_storage.add(TrieNode::Branch(branch_node));
                    if prefix > 0 {
                        let ext_node = TrieNode::Extension(TrieExtension { key: nibbles[position..position + prefix].to_vec(), child: branch_hash});
                        Ok(node_storage.add(ext_node))
                    } else {
                        Ok(branch_hash)
                    }
                }
            }
        }
    }

    fn print_node(&self, node_storage: &TrieNodeStorage, node: &TrieNode) {
        println!("{:?}", node);
        match node {
            TrieNode::Branch(branch) => {
                for child in branch.children.iter() {
                    match child {
                        Some(hash) => self.print_node(node_storage, &self.node_by_hash(node_storage, hash).expect("Printing node failed")),
                        None => (),
                    };
                }
            },
            TrieNode::Extension(ext) => {
                self.print_node(node_storage, &self.node_by_hash(node_storage, &ext.child).expect("Printing node failed"));
            },
            _ => (),
        }
    }

    fn present(&self, node_storage: &TrieNodeStorage, root: CryptoHash) {
        let root_node = self.node_by_hash(node_storage, &root).expect("Printing node failed");
        self.print_node(node_storage, &root_node);
    }

    pub fn update(&self, root: &CryptoHash, changes: TrieChanges) -> (CryptoHash, DBChanges) {
        let mut node_storage = TrieNodeStorage::new();
        let mut last_root = root.clone();
        for (key, value) in changes {
            let mut root_node = self.node_by_hash(&node_storage, &last_root).expect("Failed to find root");
            let nibbles = vec_to_nibbles(&key);
            println!("\nInserting {:?}, nibbles = {:?}", key, nibbles);
            match value {
                Some(arr) => {
                    last_root = self.insert(&mut node_storage, &root_node, &nibbles, 0, &arr).expect("Failed to insert");
                    self.present(&node_storage, last_root);
                },
                None => {
                    // last_root = self.delete(&mut node_)
                }
            }
        }
        let mut db_changes = HashMap::default();
        for (hash, node) in node_storage.nodes {
            if !node_storage.remove_nodes.contains(&hash) {
                db_changes.insert(hash.as_ref().to_vec(), node.encode());
            }
        }
        for hash in node_storage.remove_nodes {
            db_changes.insert(hash.as_ref().to_vec(), None);
        }
        (last_root, db_changes)
    }
}

fn apply_changes(storage: &Arc<KeyValueDB>, col: Option<u32>, changes: DBChanges) -> std::io::Result<()> {
    let mut db_transaction = storage.transaction();
    for (key, value) in changes {
        println!("{:?} {:?}", key, value);
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

    fn test_populate_trie(storage: &Arc<KeyValueDB>, trie: &Trie, root: &CryptoHash, changes: TrieChanges) -> CryptoHash {
        let (root, db_changes) = trie.update(root, changes.clone());
        apply_changes(storage, Some(0), db_changes).is_ok();
        for (key, value) in changes {
            assert_eq!(trie.get(&root, &key), value);
        }
        root
    }

    fn test_clear_trie(storage: &Arc<KeyValueDB>, trie: &Trie, root: &CryptoHash, changes: TrieChanges) -> CryptoHash {
        let delete_changes: TrieChanges = changes.iter().map(|(key, _)| (key.clone(), None)).collect();
        let (root, db_changes) = trie.update(root, delete_changes.clone());
        apply_changes(storage, Some(0), db_changes).is_ok();
        for (key, _) in delete_changes {
//            println!("Get {:?} {:?}", key, vec_to_nibbles(&key));
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
        let changes: TrieChanges = vec![
            (b"docu".to_vec(), Some(b"value".to_vec())),
            (b"do".to_vec(), Some(b"verb".to_vec())),
            (b"dog".to_vec(), Some(b"puppy".to_vec())),
            (b"doge".to_vec(), Some(b"coin".to_vec())),
            (b"horse".to_vec(), Some(b"stallion".to_vec())),
        ]; //.iter().cloned().collect();
        let root = test_populate_trie(&storage, &trie, &empty_root, changes.clone());
//        let new_root = test_clear_trie(&storage, &trie, &root, changes);
//        assert_eq!(new_root, empty_root)
    }
}
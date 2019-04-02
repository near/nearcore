use self::nibble_slice::NibbleSlice;
use crate::storages::shard::ShardChainStorage;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
pub use kvdb::DBValue;
use primitives::hash::{hash, CryptoHash};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::{Cursor, Read, Write};
use std::sync::Arc;
use std::sync::RwLock;

mod nibble_slice;
pub mod update;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

#[derive(Clone, Hash, Debug)]
enum NodeHandle {
    InMemory(Box<TrieNode>),
    Hash(CryptoHash),
}

#[derive(Clone, Hash, Debug)]
#[allow(clippy::large_enum_variant)]
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
    fn new(rc_node: RawTrieNode) -> TrieNode {
        match rc_node {
            RawTrieNode::Leaf(key, value) => TrieNode::Leaf(key, value),
            RawTrieNode::Branch(children, value) => {
                let mut new_children: [Option<NodeHandle>; 16] = Default::default();
                for i in 0..children.len() {
                    new_children[i] = children[i].map(NodeHandle::Hash);
                }
                TrieNode::Branch(new_children, value)
            }
            RawTrieNode::Extension(key, child) => TrieNode::Extension(key, NodeHandle::Hash(child)),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
#[allow(clippy::large_enum_variant)]
enum RawTrieNode {
    Leaf(Vec<u8>, Vec<u8>),
    Branch([Option<CryptoHash>; 16], Option<Vec<u8>>),
    Extension(Vec<u8>, CryptoHash),
}

const LEAF_NODE: u8 = 0;
const BRANCH_NODE_NO_VALUE: u8 = 1;
const BRANCH_NODE_WITH_VALUE: u8 = 2;
const EXTENSION_NODE: u8 = 3;

#[derive(Debug, Eq, PartialEq)]
struct RcTrieNode {
    data: RawTrieNode,
    rc: u32,
}

fn decode_children(cursor: &mut Cursor<&[u8]>) -> Result<[Option<CryptoHash>; 16], std::io::Error> {
    let mut children: [Option<CryptoHash>; 16] = Default::default();
    let bitmap = cursor.read_u16::<LittleEndian>()?;
    let mut pos = 1;
    for child in &mut children {
        if bitmap & pos != 0 {
            let mut arr = vec![0; 32];
            cursor.read_exact(&mut arr)?;
            *child = Some(CryptoHash::try_from(arr).unwrap());
        }
        pos <<= 1;
    }
    Ok(children)
}

impl RawTrieNode {
    fn encode(&self) -> Result<Vec<u8>, std::io::Error> {
        let mut cursor = Cursor::new(Vec::new());
        match &self {
            RawTrieNode::Leaf(key, value) => {
                cursor.write_u8(LEAF_NODE)?;
                cursor.write_u32::<LittleEndian>(key.len() as u32)?;
                cursor.write_all(&key)?;
                cursor.write_u32::<LittleEndian>(value.len() as u32)?;
                cursor.write_all(&value)?;
            }
            RawTrieNode::Branch(children, value) => {
                if let Some(bytes) = value {
                    cursor.write_u8(BRANCH_NODE_WITH_VALUE)?;
                    cursor.write_u32::<LittleEndian>(bytes.len() as u32)?;
                    cursor.write_all(&bytes)?;
                } else {
                    cursor.write_u8(BRANCH_NODE_NO_VALUE)?;
                }
                let mut bitmap: u16 = 0;
                let mut pos: u16 = 1;
                for child in children.iter() {
                    if child.is_some() {
                        bitmap |= pos
                    }
                    pos <<= 1;
                }
                cursor.write_u16::<LittleEndian>(bitmap)?;
                for child in children.iter() {
                    if let Some(hash) = child {
                        cursor.write_all(hash.as_ref())?;
                    }
                }
            }
            RawTrieNode::Extension(key, child) => {
                cursor.write_u8(EXTENSION_NODE)?;
                cursor.write_u32::<LittleEndian>(key.len() as u32)?;
                cursor.write_all(&key)?;
                cursor.write_all(child.as_ref())?;
            }
        }
        Ok(cursor.into_inner())
    }

    fn decode(bytes: &[u8]) -> Result<Self, std::io::Error> {
        let mut cursor = Cursor::new(bytes);
        match cursor.read_u8()? {
            LEAF_NODE => {
                let key_length = cursor.read_u32::<LittleEndian>()?;
                let mut key = vec![0; key_length as usize];
                cursor.read_exact(&mut key)?;
                let value_length = cursor.read_u32::<LittleEndian>()?;
                let mut value = vec![0; value_length as usize];
                cursor.read_exact(&mut value)?;
                Ok(RawTrieNode::Leaf(key, value))
            }
            BRANCH_NODE_NO_VALUE => {
                let children = decode_children(&mut cursor)?;
                Ok(RawTrieNode::Branch(children, None))
            }
            BRANCH_NODE_WITH_VALUE => {
                let value_length = cursor.read_u32::<LittleEndian>()?;
                let mut value = vec![0; value_length as usize];
                cursor.read_exact(&mut value)?;
                let children = decode_children(&mut cursor)?;
                Ok(RawTrieNode::Branch(children, Some(value)))
            }
            EXTENSION_NODE => {
                let key_length = cursor.read_u32::<LittleEndian>()?;
                let mut key = vec![0; key_length as usize];
                cursor.read_exact(&mut key)?;
                let mut child = vec![0; 32];
                cursor.read_exact(&mut child)?;
                Ok(RawTrieNode::Extension(key, CryptoHash::try_from(child).unwrap()))
            }
            _ => Err(std::io::Error::new(std::io::ErrorKind::Other, "Wrong type")),
        }
    }
}

impl RcTrieNode {
    fn encode(data: &Vec<u8>, rc: u32) -> Result<Vec<u8>, std::io::Error> {
        let mut cursor = Cursor::new(Vec::new());
        cursor.write_all(data)?;
        cursor.write_u32::<LittleEndian>(rc)?;
        Ok(cursor.into_inner())
    }

    fn decode_raw(bytes: &Vec<u8>) -> Result<(Vec<u8>, u32), std::io::Error> {
        let mut cursor = Cursor::new(&bytes[bytes.len() - 4..]);
        let rc = cursor.read_u32::<LittleEndian>()?;
        Ok((bytes[..bytes.len() - 4].to_vec(), rc))
    }

    fn decode(bytes: &Vec<u8>) -> Result<(RawTrieNode, u32), std::io::Error> {
        let node = RawTrieNode::decode(&bytes[..bytes.len() - 4])?;
        let mut cursor = Cursor::new(&bytes[bytes.len() - 4..]);
        let rc = cursor.read_u32::<LittleEndian>()?;
        Ok((node, rc))
    }
}

pub struct Trie {
    storage: Arc<RwLock<ShardChainStorage>>,
    null_node: CryptoHash,
}

pub type DBChanges = HashMap<Vec<u8>, Option<Vec<u8>>>;

impl Trie {
    pub fn new(storage: Arc<RwLock<ShardChainStorage>>) -> Self {
        Trie { storage, null_node: Trie::empty_root() }
    }

    pub fn empty_root() -> CryptoHash {
        CryptoHash::default()
    }

    fn retrieve_raw_node(&self, hash: &CryptoHash) -> Option<(Vec<u8>, u32)> {
        if let Ok(Some(bytes)) = self.storage.read().expect(POISONED_LOCK_ERR).get_state(hash) {
            match RcTrieNode::decode_raw(&bytes) {
                Ok((bytes, rc)) => Some((bytes, rc)),
                Err(_) => None,
            }
        } else {
            None
        }
    }

    fn retrieve_node(&self, hash: &CryptoHash) -> Result<TrieNode, String> {
        if *hash == self.null_node {
            return Ok(TrieNode::Empty);
        }
        if let Ok(Some(bytes)) = self.storage.read().expect(POISONED_LOCK_ERR).get_state(hash) {
            match RcTrieNode::decode(&bytes) {
                Ok((value, _)) => Ok(TrieNode::new(value)),
                Err(_) => Err(format!("Failed to decode node {}", hash)),
            }
        } else {
            Err(format!("Node {} not found in storage", hash))
        }
    }

    fn lookup(&self, root: &CryptoHash, mut key: NibbleSlice) -> Result<Option<Vec<u8>>, String> {
        let mut hash = *root;

        loop {
            if hash == self.null_node {
                return Ok(None);
            }
            let node = match self.storage.read().expect(POISONED_LOCK_ERR).get_state(&hash) {
                Ok(Some(bytes)) => RcTrieNode::decode(&bytes)
                    .map(|trie_node| trie_node.0)
                    .map_err(|_| "Failed to decode node".to_string())?,
                _ => return Err(format!("Node {} not found in storage", hash)),
            };

            match node {
                RawTrieNode::Leaf(existing_key, value) => {
                    return Ok(if NibbleSlice::from_encoded(&existing_key).0 == key {
                        Some(value)
                    } else {
                        None
                    });
                }
                RawTrieNode::Extension(existing_key, child) => {
                    let existing_key = NibbleSlice::from_encoded(&existing_key).0;
                    if key.starts_with(&existing_key) {
                        hash = child;
                        key = key.mid(existing_key.len());
                    } else {
                        return Ok(None);
                    }
                }
                RawTrieNode::Branch(mut children, value) => {
                    if key.is_empty() {
                        return Ok(value);
                    } else {
                        match children[key.at(0) as usize].take() {
                            Some(x) => {
                                hash = x;
                                key = key.mid(1);
                            }
                            None => return Ok(None),
                        }
                    }
                }
            };
        }
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

    fn insert(
        &self,
        node: TrieNode,
        partial: NibbleSlice,
        value: Vec<u8>,
    ) -> Result<TrieNode, String> {
        match node {
            TrieNode::Empty => {
                let leaf_node = TrieNode::Leaf(partial.encoded(true).into_vec(), value);
                Ok(leaf_node)
            }
            TrieNode::Branch(mut children, existing_value) => {
                // If the key ends here, store the value in branch's value.
                if partial.is_empty() {
                    Ok(TrieNode::Branch(children, Some(value)))
                } else {
                    let idx = partial.at(0) as usize;
                    let partial = partial.mid(1);
                    let child = children[idx].take();
                    let new_hash = match child {
                        Some(NodeHandle::Hash(hash)) => {
                            self.insert(self.retrieve_node(&hash)?, partial, value)?
                        }
                        Some(NodeHandle::InMemory(node)) => self.insert(*node, partial, value)?,
                        _ => TrieNode::Leaf(partial.encoded(true).into_vec(), value),
                    };
                    children[idx] = Some(NodeHandle::InMemory(Box::new(new_hash)));
                    Ok(TrieNode::Branch(children, existing_value))
                }
            }
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
                        let new_leaf = TrieNode::Leaf(
                            existing_key.mid(1).encoded(true).into_vec(),
                            existing_value,
                        );
                        children[idx] = Some(NodeHandle::InMemory(Box::new(new_leaf)));
                        TrieNode::Branch(children, None)
                    };
                    self.insert(branch_node, partial, value)
                } else if common_prefix == existing_key.len() {
                    let branch_node = TrieNode::Branch(Default::default(), Some(existing_value));
                    let child = self.insert(branch_node, partial.mid(common_prefix), value)?;
                    Ok(TrieNode::Extension(
                        existing_key.encoded(false).into_vec(),
                        NodeHandle::InMemory(Box::new(child)),
                    ))
                } else {
                    // Partially shared prefix: convert to leaf and call recursively to add a branch.
                    let low = TrieNode::Leaf(
                        existing_key.mid(common_prefix).encoded(true).into_vec(),
                        existing_value,
                    );
                    let child = self.insert(low, partial.mid(common_prefix), value)?;
                    Ok(TrieNode::Extension(
                        partial.encoded_leftmost(common_prefix, false).into_vec(),
                        NodeHandle::InMemory(Box::new(child)),
                    ))
                }
            }
            TrieNode::Extension(key, child) => {
                let existing_key = NibbleSlice::from_encoded(&key).0;
                let common_prefix = partial.common_prefix(&existing_key);
                if common_prefix == 0 {
                    let idx = existing_key.at(0) as usize;
                    let mut children: [Option<NodeHandle>; 16] = Default::default();
                    children[idx] = if existing_key.len() == 1 {
                        Some(child)
                    } else {
                        let ext_node = TrieNode::Extension(
                            existing_key.mid(1).encoded(false).into_vec(),
                            child,
                        );
                        Some(NodeHandle::InMemory(Box::new(ext_node)))
                    };
                    let branch_node = TrieNode::Branch(children, None);
                    self.insert(branch_node, partial, value)
                } else if common_prefix == existing_key.len() {
                    let child_node = match child {
                        NodeHandle::Hash(hash) => self.retrieve_node(&hash)?,
                        NodeHandle::InMemory(node) => *node,
                    };
                    let new_child = NodeHandle::InMemory(Box::new(self.insert(
                        child_node,
                        partial.mid(common_prefix),
                        value,
                    )?));
                    Ok(TrieNode::Extension(key.clone(), new_child))
                } else {
                    // Partially shared prefix: covert to shorter extension and recursively add a branch.
                    let low = TrieNode::Extension(
                        existing_key.mid(common_prefix).encoded(false).into_vec(),
                        child,
                    );
                    let new_child = NodeHandle::InMemory(Box::new(self.insert(
                        low,
                        partial.mid(common_prefix),
                        value,
                    )?));
                    Ok(TrieNode::Extension(
                        existing_key.encoded_leftmost(common_prefix, false).into_vec(),
                        new_child,
                    ))
                }
            }
        }
    }

    /// Deletes a node from the trie which has key = `partial` given root node.
    /// Returns (new root node or `None` if this was the node to delete, was it updated).
    /// While deleting keeps track of all the removed / updated nodes in `death_row`.
    fn delete(
        &self,
        node: TrieNode,
        hash: Option<CryptoHash>,
        partial: NibbleSlice,
        death_row: &mut HashMap<CryptoHash, u32>,
    ) -> Result<(Option<TrieNode>, bool), String> {
        match node {
            TrieNode::Empty => Ok((Some(node), false)),
            TrieNode::Leaf(key, value) => {
                if NibbleSlice::from_encoded(&key).0 == partial {
                    if let Some(hash) = hash {
                        *death_row.entry(hash).or_insert(0) += 1
                    }
                    Ok((None, true))
                } else {
                    Ok((Some(TrieNode::Leaf(key, value)), false))
                }
            }
            TrieNode::Branch(mut children, value) => {
                if partial.is_empty() {
                    if let Some(hash) = hash {
                        *death_row.entry(hash).or_insert(0) += 1;
                    }
                    if children.iter().filter(|&x| x.is_some()).count() == 0 {
                        Ok((None, true))
                    } else {
                        Ok((Some(TrieNode::Branch(children, None)), value.is_none()))
                    }
                } else {
                    let idx = partial.at(0) as usize;
                    if let Some(node_or_hash) = children[idx].take() {
                        let (new_node, changed) = match node_or_hash {
                            NodeHandle::Hash(hash) => self.delete(
                                self.retrieve_node(&hash)?,
                                Some(hash),
                                partial.mid(1),
                                death_row,
                            )?,
                            NodeHandle::InMemory(node) => {
                                self.delete(*node, None, partial.mid(1), death_row)?
                            }
                        };
                        children[idx] = match new_node {
                            Some(node) => Some(NodeHandle::InMemory(Box::new(node))),
                            None => None,
                        };
                        if let Some(hash) = hash {
                            if changed {
                                *death_row.entry(hash).or_insert(0) += 1;
                            }
                        }
                        if children.iter().filter(|x| x.is_some()).count() == 0 {
                            match value {
                                Some(value) => Ok((
                                    Some(TrieNode::Leaf(
                                        NibbleSlice::new(&[]).encoded(true).into_vec(),
                                        value,
                                    )),
                                    true,
                                )),
                                None => Ok((None, true)),
                            }
                        } else {
                            Ok((Some(TrieNode::Branch(children, value)), changed))
                        }
                    } else {
                        Ok((Some(TrieNode::Branch(children, value)), false))
                    }
                }
            }
            TrieNode::Extension(key, child) => {
                let (common_prefix, existing_len) = {
                    let existing_key = NibbleSlice::from_encoded(&key).0;
                    (existing_key.common_prefix(&partial), existing_key.len())
                };
                if common_prefix == existing_len {
                    let (result, changed) = match child {
                        NodeHandle::Hash(hash) => self.delete(
                            self.retrieve_node(&hash)?,
                            Some(hash),
                            partial.mid(existing_len),
                            death_row,
                        )?,
                        NodeHandle::InMemory(node) => {
                            self.delete(*node, None, partial.mid(existing_len), death_row)?
                        }
                    };
                    if let Some(hash) = hash {
                        if changed {
                            *death_row.entry(hash).or_insert(0) += 1
                        }
                    }
                    match result {
                        Some(node) => Ok((
                            Some(TrieNode::Extension(key, NodeHandle::InMemory(Box::new(node)))),
                            changed,
                        )),
                        None => Ok((None, true)),
                    }
                } else {
                    Ok((Some(TrieNode::Extension(key, child)), false))
                }
            }
        }
    }

    fn flatten_nodes(
        &self,
        node: TrieNode,
        nodes: &mut HashMap<CryptoHash, (Vec<u8>, u32)>,
    ) -> CryptoHash {
        let rc_node = match node {
            TrieNode::Empty => return self.null_node,
            TrieNode::Branch(mut children, value) => {
                let mut new_children: [Option<CryptoHash>; 16] = Default::default();
                for i in 0..children.len() {
                    new_children[i] = match children[i].take() {
                        Some(NodeHandle::InMemory(child_node)) => {
                            Some(self.flatten_nodes(*child_node, nodes))
                        }
                        Some(NodeHandle::Hash(hash)) => Some(hash),
                        _ => None,
                    }
                }
                RawTrieNode::Branch(new_children, value)
            }
            TrieNode::Extension(key, child) => {
                let child = match child {
                    NodeHandle::InMemory(child) => self.flatten_nodes(*child, nodes),
                    NodeHandle::Hash(hash) => hash,
                };
                RawTrieNode::Extension(key, child)
            }
            TrieNode::Leaf(key, value) => RawTrieNode::Leaf(key, value),
        };
        let data = rc_node.encode().expect("Failed to serialize");
        let key = hash(&data);
        if let Some(value) = nodes.get_mut(&key) {
            value.1 += 1;
        } else {
            let node_rc = if let Some((_, rc)) = self.retrieve_raw_node(&key) { rc } else { 0 };
            nodes.insert(key, (data, node_rc + 1));
        }
        key
    }

    pub fn update<I>(&self, root: &CryptoHash, changes: I) -> (DBChanges, CryptoHash)
    where
        I: Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
    {
        let mut death_row: HashMap<CryptoHash, u32> = HashMap::default();
        let mut last_root = Some(*root);
        let mut root_node = self.retrieve_node(root).expect("Root not found");
        for (key, value) in changes {
            let key = NibbleSlice::new(&key);
            match value {
                Some(arr) => {
                    root_node = self.insert(root_node, key, arr).expect("Failed to insert");
                    last_root = None;
                }
                None => {
                    root_node = match self
                        .delete(root_node, last_root, key, &mut death_row)
                        .expect("Failed to remove element")
                    {
                        (Some(value), _) => value,
                        (None, _) => TrieNode::Empty,
                    };
                    last_root = None;
                }
            }
        }
        let mut db_changes = HashMap::default();

        let mut nodes = HashMap::default();
        let new_root = self.flatten_nodes(root_node, &mut nodes);
        for (key, (value, mut rc)) in nodes.drain() {
            if let Some(death_rc) = death_row.get(&key) {
                rc -= death_rc;
                death_row.remove(&key);
            }
            if rc > 0 {
                let bytes = RcTrieNode::encode(&value, rc).expect("Failed to serialize");
                db_changes.insert(key.as_ref().to_vec(), Some(bytes));
            } else {
                db_changes.insert(key.as_ref().to_vec(), None);
            }
        }
        for (hash, death_rc) in death_row {
            if let Some((bytes, rc)) = self.retrieve_raw_node(&hash) {
                if rc - death_rc > 0 {
                    let bytes =
                        RcTrieNode::encode(&bytes, rc - death_rc).expect("Failed to serialize");
                    db_changes.insert(hash.as_ref().to_vec(), Some(bytes));
                    continue;
                }
            }
            db_changes.insert(hash.as_ref().to_vec(), None);
        }
        (db_changes, new_root)
    }

    pub fn iter<'a>(&'a self, root: &CryptoHash) -> Result<TrieIterator<'a>, String> {
        TrieIterator::new(self, root)
    }

    #[inline]
    pub fn apply_changes(&self, changes: DBChanges) -> std::io::Result<()> {
        self.storage.read().expect(POISONED_LOCK_ERR).apply_state_updates(&changes)
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
    key_nibbles: Vec<u8>,
    root: CryptoHash,
}

impl<'a> TrieIterator<'a> {
    #![allow(clippy::new_ret_no_self)]
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

    fn descend_into_node(&mut self, node: &TrieNode) {
        self.trail.push(Crumb { status: CrumbStatus::Entering, node: node.clone() });
        match &self.trail.last().expect("Just pushed item").node {
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
            Descend(Result<Box<TrieNode>, String>),
        }
        loop {
            let iter_step = {
                self.trail.last_mut()?.increment();
                let b = self.trail.last().expect("Trail finished.");
                match (b.status.clone(), &b.node) {
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
                    (CrumbStatus::At, TrieNode::Branch(_, value)) => {
                        if value.is_some() {
                            let value = value.clone().expect("is_some() called");
                            return Some(Ok((self.key(), DBValue::from_slice(&value))));
                        } else {
                            IterStep::Continue
                        }
                    }
                    (CrumbStatus::At, TrieNode::Leaf(_, value)) => {
                        return Some(Ok((self.key(), DBValue::from_slice(value))));
                    }
                    (CrumbStatus::At, TrieNode::Extension(_, child)) => {
                        let next_node = match child {
                            NodeHandle::Hash(hash) => self.trie.retrieve_node(hash).map(Box::new),
                            NodeHandle::InMemory(node) => Ok(node.clone()),
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
                            Some(NodeHandle::InMemory(node)) => Ok(node.clone()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::create_trie;

    type TrieChanges = Vec<(Vec<u8>, Option<Vec<u8>>)>;

    fn test_populate_trie(trie: &Trie, root: &CryptoHash, changes: TrieChanges) -> CryptoHash {
        let mut other_changes = changes.clone();
        let (db_changes, root) = trie.update(root, other_changes.drain(..));
        trie.apply_changes(db_changes).is_ok();
        for (key, value) in changes {
            assert_eq!(trie.get(&root, &key), value);
        }
        root
    }

    fn test_clear_trie(trie: &Trie, root: &CryptoHash, changes: TrieChanges) -> CryptoHash {
        let delete_changes: TrieChanges =
            changes.iter().map(|(key, _)| (key.clone(), None)).collect();
        let mut other_delete_changes = delete_changes.clone();
        let (db_changes, root) = trie.update(root, other_delete_changes.drain(..));
        trie.apply_changes(db_changes).is_ok();
        for (key, _) in delete_changes {
            assert_eq!(trie.get(&root, &key), None);
        }
        root
    }

    #[test]
    fn test_encode_decode() {
        let node = RawTrieNode::Leaf(vec![1, 2, 3], vec![123, 245, 255]);
        let buf = node.encode().expect("Failed to serialize");
        let new_node = RawTrieNode::decode(&buf).expect("Failed to deserialize");
        assert_eq!(node, new_node);

        let mut children: [Option<CryptoHash>; 16] = Default::default();
        children[3] = Some(CryptoHash::default());
        let node = RawTrieNode::Branch(children, Some(vec![123, 245, 255]));
        let buf = node.encode().expect("Failed to serialize");
        let new_node = RawTrieNode::decode(&buf).expect("Failed to deserialize");
        assert_eq!(node, new_node);

        let node = RawTrieNode::Extension(vec![123, 245, 255], CryptoHash::default());
        let buf = node.encode().expect("Failed to serialize");
        let new_node = RawTrieNode::decode(&buf).expect("Failed to deserialize");
        assert_eq!(node, new_node);
    }

    #[test]
    fn test_basic_trie() {
        let trie = create_trie();
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
        let root = test_populate_trie(&trie, &empty_root, changes.clone());
        let new_root = test_clear_trie(&trie, &root, changes);
        assert_eq!(new_root, empty_root);
        assert_eq!(trie.iter(&new_root).unwrap().fold(0, |acc, _| acc + 1), 0);
    }

    #[test]
    fn test_trie_iter() {
        let trie = create_trie();
        let pairs = vec![
            (b"a".to_vec(), Some(b"111".to_vec())),
            (b"b".to_vec(), Some(b"222".to_vec())),
            (b"x".to_vec(), Some(b"333".to_vec())),
            (b"y".to_vec(), Some(b"444".to_vec())),
        ];
        let root = test_populate_trie(&trie, &Trie::empty_root(), pairs.clone());
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
    fn test_trie_leaf_into_branch() {
        let trie = create_trie();
        let changes = vec![
            (b"dog".to_vec(), Some(b"puppy".to_vec())),
            (b"dog2".to_vec(), Some(b"puppy".to_vec())),
            (b"xxx".to_vec(), Some(b"puppy".to_vec())),
        ];
        test_populate_trie(&trie, &Trie::empty_root(), changes);
    }

    #[test]
    fn test_trie_same_node() {
        let trie = create_trie();
        let changes = vec![
            (b"dogaa".to_vec(), Some(b"puppy".to_vec())),
            (b"dogbb".to_vec(), Some(b"puppy".to_vec())),
            (b"cataa".to_vec(), Some(b"puppy".to_vec())),
            (b"catbb".to_vec(), Some(b"puppy".to_vec())),
            (b"dogax".to_vec(), Some(b"puppy".to_vec())),
        ];
        test_populate_trie(&trie, &Trie::empty_root(), changes);
    }

    #[test]
    fn test_trie_iter_seek_stop_at_extension() {
        let trie = create_trie();
        let changes = vec![
            (vec![0, 116, 101, 115, 116], Some(vec![0])),
            (vec![2, 116, 101, 115, 116], Some(vec![0])),
            (
                vec![
                    0, 116, 101, 115, 116, 44, 98, 97, 108, 97, 110, 99, 101, 115, 58, 98, 111, 98,
                    46, 110, 101, 97, 114,
                ],
                Some(vec![0]),
            ),
            (
                vec![
                    0, 116, 101, 115, 116, 44, 98, 97, 108, 97, 110, 99, 101, 115, 58, 110, 117,
                    108, 108,
                ],
                Some(vec![0]),
            ),
        ];
        let root = test_populate_trie(&trie, &Trie::empty_root(), changes);
        let mut iter = trie.iter(&root).unwrap();
        iter.seek(&vec![0, 116, 101, 115, 116, 44]).unwrap();
        let mut pairs = vec![];
        for pair in iter {
            pairs.push(pair.unwrap().0);
        }
        assert_eq!(
            pairs[..2],
            [
                vec![
                    0, 116, 101, 115, 116, 44, 98, 97, 108, 97, 110, 99, 101, 115, 58, 98, 111, 98,
                    46, 110, 101, 97, 114
                ],
                vec![
                    0, 116, 101, 115, 116, 44, 98, 97, 108, 97, 110, 99, 101, 115, 58, 110, 117,
                    108, 108
                ],
            ]
        );
    }

    #[test]
    fn test_trie_remove_non_existant_key() {
        let trie = create_trie();
        let mut initial = vec![
            (vec![99, 44, 100, 58, 58, 49], Some(vec![1])),
            (vec![99, 44, 100, 58, 58, 50], Some(vec![1])),
            (vec![99, 44, 100, 58, 58, 50, 51], Some(vec![1])),
        ];
        let (db_changes, root) = trie.update(&Trie::empty_root(), initial.drain(..));
        trie.apply_changes(db_changes).is_ok();

        let mut changes = vec![
            (vec![99, 44, 100, 58, 58, 45, 49], None),
            (vec![99, 44, 100, 58, 58, 50, 52], None),
        ];
        let (db_changes, root) = trie.update(&root, changes.drain(..));
        trie.apply_changes(db_changes).is_ok();
        for r in trie.iter(&root).unwrap() {
            r.unwrap();
        }
    }

    #[test]
    fn test_equal_leafs() {
        let trie = create_trie();
        let mut initial = vec![
            (vec![1, 2, 3], Some(vec![1])),
            (vec![2, 2, 3], Some(vec![1])),
            (vec![3, 2, 3], Some(vec![1])),
        ];
        let (db_changes, root) = trie.update(&Trie::empty_root(), initial.drain(..));
        trie.apply_changes(db_changes).is_ok();
        for r in trie.iter(&root).unwrap() {
            println!("{:?}", r.unwrap());
        }

        let mut changes = vec![(vec![1, 2, 3], None)];
        let (db_changes, root) = trie.update(&root, changes.drain(..));
        trie.apply_changes(db_changes).is_ok();
        for r in trie.iter(&root).unwrap() {
            r.unwrap();
        }
    }
}

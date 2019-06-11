use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;
use std::io::{Cursor, ErrorKind, Read, Write};
use std::sync::{Arc, Mutex};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use cached::{Cached, SizedCache};
pub use kvdb::DBValue;
use kvdb::{DBOp, DBTransaction};
use log::error;

use near_primitives::hash::{hash, CryptoHash};

use crate::{Store, StoreUpdate, COL_STATE};

use self::nibble_slice::NibbleSlice;

mod nibble_slice;
pub mod update;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

#[derive(Clone, Hash, Debug, Copy)]
struct StorageHandle(usize);

#[derive(Clone, Hash, Debug)]
enum NodeHandle {
    InMemory(StorageHandle),
    Hash(CryptoHash),
}

#[derive(Clone, Hash, Debug)]
enum TrieNode {
    /// Null trie node. Could be an empty root or an empty branch entry.
    Empty,
    /// Key and value of the leaf node.
    Leaf(Vec<u8>, Vec<u8>),
    /// Branch of 16 possible children and value if key ends here.
    Branch(Box<[Option<NodeHandle>; 16]>, Option<Vec<u8>>),
    /// Key and child of extension.
    Extension(Vec<u8>, NodeHandle),
}

impl TrieNode {
    fn new(rc_node: RawTrieNode) -> TrieNode {
        match rc_node {
            RawTrieNode::Leaf(key, value) => TrieNode::Leaf(key, value),
            RawTrieNode::Branch(children, value) => {
                let mut new_children: Box<[Option<NodeHandle>; 16]> = Default::default();
                for i in 0..children.len() {
                    new_children[i] = children[i].map(NodeHandle::Hash);
                }
                TrieNode::Branch(new_children, value)
            }
            RawTrieNode::Extension(key, child) => TrieNode::Extension(key, NodeHandle::Hash(child)),
        }
    }

    fn print(
        &self,
        f: &mut dyn fmt::Write,
        memory: &NodesStorage,
        spaces: &mut String,
    ) -> fmt::Result {
        match self {
            TrieNode::Empty => {
                write!(f, "{}Empty", spaces)?;
            }
            TrieNode::Leaf(key, _value) => {
                let slice = NibbleSlice::from_encoded(key);
                write!(f, "{}Leaf({:?}, val)", spaces, slice.0)?;
            }
            TrieNode::Branch(children, value) => {
                writeln!(
                    f,
                    "{}Branch({}){{",
                    spaces,
                    if value.is_some() { "Some" } else { "None" }
                )?;
                spaces.push_str(" ");
                for (idx, child) in
                    children.iter().enumerate().filter(|(_idx, child)| child.is_some())
                {
                    let child = child.as_ref().unwrap();
                    write!(f, "{}{:01x}->", spaces, idx)?;
                    match child {
                        NodeHandle::Hash(hash) => {
                            write!(f, "{}", hash)?;
                        }
                        NodeHandle::InMemory(handle) => {
                            let child = memory.node_ref(*handle);
                            child.print(f, memory, spaces)?;
                        }
                    }
                    writeln!(f)?;
                }
                spaces.remove(spaces.len() - 1);
                write!(f, "{}}}", spaces)?;
            }
            TrieNode::Extension(key, child) => {
                let slice = NibbleSlice::from_encoded(key);
                writeln!(f, "{}Extension({:?})", spaces, slice)?;
                spaces.push_str(" ");
                match child {
                    NodeHandle::Hash(hash) => {
                        write!(f, "{}{}", spaces, hash)?;
                    }
                    NodeHandle::InMemory(handle) => {
                        let child = memory.node_ref(*handle);
                        child.print(f, memory, spaces)?;
                    }
                }
                writeln!(f)?;
                spaces.remove(spaces.len() - 1);
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn deep_to_string(&self, memory: &NodesStorage) -> String {
        let mut buf = String::new();
        self.print(&mut buf, memory, &mut "".to_string()).expect("printing failed");
        buf
    }
}

#[derive(Debug, Eq, PartialEq)]
#[allow(clippy::large_enum_variant)]
enum RawTrieNode {
    Leaf(Vec<u8>, Vec<u8>),
    Branch([Option<CryptoHash>; 16], Option<Vec<u8>>),
    Extension(Vec<u8>, CryptoHash),
}

struct NodesStorage {
    nodes: Vec<Option<TrieNode>>,
    refcount_changes: HashMap<CryptoHash, (Vec<u8>, i32)>,
}

const INVALID_STORAGE_HANDLE: &str = "invalid storage handle";

/// Local mutable storage that owns node objects.
impl NodesStorage {
    fn new() -> NodesStorage {
        NodesStorage { nodes: Vec::new(), refcount_changes: HashMap::new() }
    }

    fn destroy(&mut self, handle: StorageHandle) -> TrieNode {
        self.nodes
            .get_mut(handle.0)
            .expect(INVALID_STORAGE_HANDLE)
            .take()
            .expect(INVALID_STORAGE_HANDLE)
    }

    fn node_ref(&self, handle: StorageHandle) -> &TrieNode {
        self.nodes
            .get(handle.0)
            .expect(INVALID_STORAGE_HANDLE)
            .as_ref()
            .expect(INVALID_STORAGE_HANDLE)
    }

    fn store(&mut self, node: TrieNode) -> StorageHandle {
        self.nodes.push(Some(node));
        StorageHandle(self.nodes.len() - 1)
    }

    fn store_at(&mut self, handle: StorageHandle, node: TrieNode) {
        debug_assert!(self.nodes.get(handle.0).expect(INVALID_STORAGE_HANDLE).is_none());
        self.nodes[handle.0] = Some(node);
    }
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
            let mut arr = [0; 32];
            cursor.read_exact(&mut arr)?;
            *child = Some(CryptoHash::try_from(&arr[..]).unwrap());
        }
        pos <<= 1;
    }
    Ok(children)
}

impl RawTrieNode {
    fn encode_into(&self, out: &mut Vec<u8>) -> Result<(), std::io::Error> {
        let mut cursor = Cursor::new(out);
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
        Ok(())
    }

    #[allow(dead_code)]
    fn encode(&self) -> Result<Vec<u8>, std::io::Error> {
        let mut out = Vec::new();
        self.encode_into(&mut out)?;
        Ok(out)
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
    fn encode(data: &[u8], rc: u32) -> Result<Vec<u8>, std::io::Error> {
        let mut cursor = Cursor::new(Vec::with_capacity(data.len() + 4));
        cursor.write_all(data)?;
        cursor.write_u32::<LittleEndian>(rc)?;
        Ok(cursor.into_inner())
    }

    fn decode_raw(bytes: &[u8]) -> Result<(&[u8], u32), std::io::Error> {
        let mut cursor = Cursor::new(&bytes[bytes.len() - 4..]);
        let rc = cursor.read_u32::<LittleEndian>()?;
        Ok((&bytes[..bytes.len() - 4], rc))
    }

    fn decode(bytes: &[u8]) -> Result<(RawTrieNode, u32), std::io::Error> {
        let node = RawTrieNode::decode(&bytes[..bytes.len() - 4])?;
        let mut cursor = Cursor::new(&bytes[bytes.len() - 4..]);
        let rc = cursor.read_u32::<LittleEndian>()?;
        Ok((node, rc))
    }
}

pub struct TrieCachingStorage {
    store: Arc<Store>,
    cache: Arc<Mutex<SizedCache<CryptoHash, Option<Vec<u8>>>>>,
}

impl TrieCachingStorage {
    fn new(store: Arc<Store>) -> TrieCachingStorage {
        // TODO defend from huge values in cache
        TrieCachingStorage { store, cache: Arc::new(Mutex::new(SizedCache::with_size(10000))) }
    }

    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Option<(Vec<u8>)> {
        let mut guard = self.cache.lock().expect(POISONED_LOCK_ERR);
        if let Some(val) = (*guard).cache_get(hash) {
            val.clone()
        } else {
            let result = if let Ok(Some(bytes)) = self.store.get(COL_STATE, hash.as_ref()) {
                Some(bytes)
            } else {
                None
            };
            (*guard).cache_set(*hash, result.clone());
            result
        }
    }

    fn retrieve_rc(&self, hash: &CryptoHash) -> Option<u32> {
        let mut guard = self.cache.lock().expect(POISONED_LOCK_ERR);
        if let Some(val) = (*guard).cache_get(hash) {
            val.as_ref().map(|vec| RcTrieNode::decode_raw(&vec).expect("failed to decode").1)
        } else {
            let val = if let Ok(Some(bytes)) = self.store.get(COL_STATE, hash.as_ref()) {
                Some(bytes)
            } else {
                None
            };
            let rc =
                val.as_ref().map(|vec| RcTrieNode::decode_raw(&vec).expect("failed to decode").1);
            (*guard).cache_set(*hash, val);
            rc
        }
    }

    fn retrieve_node(&self, hash: &CryptoHash) -> Result<TrieNode, String> {
        if let Some(bytes) = self.retrieve_raw_bytes(hash) {
            match RcTrieNode::decode(&bytes) {
                Ok((value, _)) => Ok(TrieNode::new(value)),
                Err(_) => Err(format!("Failed to decode node {}", hash)),
            }
        } else {
            Err(format!("Node {} not found in storage", hash))
        }
    }
}

pub struct Trie {
    storage: TrieCachingStorage,
}

///
/// TrieChanges stores delta for refcount.
/// Multiple versions of the state work the following way:
///         __changes1___state1
/// state0 /
///        \__changes2___state2
///
/// To store state0, state1 and state2, apply insertions from changes1 and changes2
///
/// Then, to discard state2, apply insertions from changes2 as deletions
///
/// Then, to discard state0, apply deletions from changes1.
/// deleting state0 while both state1 and state2 exist is not possible.
/// Applying deletions from changes1 while state2 exists makes accessing state2 invalid.
///
///
/// create a fork -> apply insertions
/// resolve a fork -> apply opposite of insertions
/// discard old parent which has no forks from it -> apply deletions
///
/// Having old_root and values in deletions allows to apply TrieChanges in reverse
///
/// StoreUpdate are the changes from current state refcount to refcount + delta.
pub struct TrieChanges {
    #[allow(dead_code)]
    old_root: CryptoHash,
    pub new_root: CryptoHash,
    insertions: Vec<(CryptoHash, Vec<u8>, u32)>, // key, value, rc
    deletions: Vec<(CryptoHash, Vec<u8>, u32)>,  // key, value, rc
}

impl TrieChanges {
    pub fn empty(old_root: CryptoHash) -> Self {
        TrieChanges { old_root, new_root: old_root, insertions: vec![], deletions: vec![] }
    }
    pub fn insertions_into(
        &self,
        trie: Arc<Trie>,
        store_update: &mut StoreUpdate,
    ) -> Result<(), Box<dyn std::error::Error>> {
        store_update.trie = Some(trie.clone());
        for (key, value, rc) in self.insertions.iter() {
            let storage_rc = trie.storage.retrieve_rc(&key).unwrap_or_default();
            let bytes = RcTrieNode::encode(&value, storage_rc + rc)?;
            store_update.set(COL_STATE, key.as_ref(), &bytes);
        }
        Ok(())
    }

    pub fn deletions_into(
        &self,
        trie: Arc<Trie>,
        store_update: &mut StoreUpdate,
    ) -> Result<(), Box<dyn std::error::Error>> {
        store_update.trie = Some(trie.clone());
        for (key, value, rc) in self.deletions.iter() {
            let storage_rc = trie.storage.retrieve_rc(&key).unwrap_or_default();
            assert!(*rc <= storage_rc);
            if *rc < storage_rc {
                let bytes = RcTrieNode::encode(&value, storage_rc - rc)?;
                store_update.set(COL_STATE, key.as_ref(), &bytes);
            } else {
                store_update.delete(COL_STATE, key.as_ref());
            }
        }
        Ok(())
    }

    pub fn into(
        self,
        trie: Arc<Trie>,
    ) -> Result<(StoreUpdate, CryptoHash), Box<dyn std::error::Error>> {
        let mut store_update =
            StoreUpdate::new_with_trie(trie.storage.store.storage.clone(), trie.clone());
        self.insertions_into(trie.clone(), &mut store_update)?;
        self.deletions_into(trie.clone(), &mut store_update)?;
        Ok((store_update, self.new_root))
    }
}

pub struct WrappedTrieChanges {
    trie: Arc<Trie>,
    trie_changes: TrieChanges,
}

impl WrappedTrieChanges {
    pub fn new(trie: Arc<Trie>, trie_changes: TrieChanges) -> Self {
        WrappedTrieChanges { trie, trie_changes }
    }

    pub fn insertions_into(
        &self,
        store_update: &mut StoreUpdate,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.trie_changes.insertions_into(self.trie.clone(), store_update)
    }

    pub fn deletions_into(
        &self,
        store_update: &mut StoreUpdate,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.trie_changes.deletions_into(self.trie.clone(), store_update)
    }
}

enum FlattenNodesCrumb {
    Entering,
    AtChild(Box<[Option<CryptoHash>; 16]>, usize),
    Exiting,
}

impl Trie {
    pub fn new(store: Arc<Store>) -> Self {
        Trie { storage: TrieCachingStorage::new(store) }
    }

    pub fn empty_root() -> CryptoHash {
        CryptoHash::default()
    }

    fn move_node_to_mutable(
        &self,
        memory: &mut NodesStorage,
        hash: &CryptoHash,
    ) -> Result<StorageHandle, Box<dyn std::error::Error>> {
        if *hash == Trie::empty_root() {
            Ok(memory.store(TrieNode::Empty))
        } else {
            if let Some(bytes) = self.storage.retrieve_raw_bytes(hash) {
                match RcTrieNode::decode(&bytes) {
                    Ok((value, _)) => {
                        let result = memory.store(TrieNode::new(value));
                        memory
                            .refcount_changes
                            .entry(*hash)
                            .or_insert_with(|| {
                                (
                                    RcTrieNode::decode_raw(&bytes)
                                        .expect("calling after decode()")
                                        .0
                                        .to_vec(),
                                    0,
                                )
                            })
                            .1 -= 1;
                        Ok(result)
                    }
                    Err(_) => Err(format!("Failed to decode node {}", hash).into()),
                }
            } else {
                Err(format!("Node {} not found in storage", hash).into())
            }
        }
    }

    fn retrieve_node(&self, hash: &CryptoHash) -> Result<TrieNode, String> {
        if *hash == Trie::empty_root() {
            return Ok(TrieNode::Empty);
        }
        self.storage.retrieve_node(hash)
    }

    fn lookup(&self, root: &CryptoHash, mut key: NibbleSlice) -> Result<Option<Vec<u8>>, String> {
        let mut hash = *root;

        loop {
            if hash == Trie::empty_root() {
                return Ok(None);
            }
            let node = match self.storage.retrieve_raw_bytes(&hash) {
                Some(bytes) => RcTrieNode::decode(&bytes)
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
                error!(target: "store", "Failed to lookup key={:?} for root={:?}: {}", key, root, err);
                None
            }
        }
    }

    /// Allowed to mutate nodes in NodesStorage.
    /// Insert while holding StorageHandles to NodesStorage is unsafe
    fn insert(
        &self,
        memory: &mut NodesStorage,
        node: StorageHandle,
        partial: NibbleSlice,
        value: Vec<u8>,
    ) -> Result<StorageHandle, Box<dyn std::error::Error>> {
        let root_handle = node;
        let mut handle = node;
        let mut value = Some(value);
        let mut partial = partial;

        loop {
            match memory.destroy(handle) {
                TrieNode::Empty => {
                    let leaf_node =
                        TrieNode::Leaf(partial.encoded(true).into_vec(), value.take().unwrap());
                    memory.store_at(handle, leaf_node);
                    break;
                }
                TrieNode::Branch(mut children, existing_value) => {
                    // If the key ends here, store the value in branch's value.
                    if partial.is_empty() {
                        memory.store_at(
                            handle,
                            TrieNode::Branch(children, Some(value.take().unwrap())),
                        );
                        break;
                    } else {
                        let idx = partial.at(0) as usize;
                        let child = children[idx].take();
                        let child = match child {
                            Some(NodeHandle::Hash(hash)) => {
                                self.move_node_to_mutable(memory, &hash)?
                            }
                            Some(NodeHandle::InMemory(handle)) => handle,
                            None => memory.store(TrieNode::Empty),
                        };
                        children[idx] = Some(NodeHandle::InMemory(child));
                        memory.store_at(handle, TrieNode::Branch(children, existing_value));
                        handle = child;
                        partial = partial.mid(1);
                        continue;
                    }
                }
                TrieNode::Leaf(key, existing_value) => {
                    let existing_key = NibbleSlice::from_encoded(&key).0;
                    let common_prefix = partial.common_prefix(&existing_key);
                    if common_prefix == existing_key.len() && common_prefix == partial.len() {
                        // Equivalent leaf.
                        memory.store_at(handle, TrieNode::Leaf(key, value.take().unwrap()));
                        break;
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
                            children[idx] = Some(NodeHandle::InMemory(memory.store(new_leaf)));
                            TrieNode::Branch(children, None)
                        };
                        memory.store_at(handle, branch_node);
                        continue;
                    } else if common_prefix == existing_key.len() {
                        let child = memory
                            .store(TrieNode::Branch(Default::default(), Some(existing_value)));
                        memory.store_at(
                            handle,
                            TrieNode::Extension(
                                existing_key.encoded(false).into_vec(),
                                NodeHandle::InMemory(child),
                            ),
                        );
                        handle = child;
                        partial = partial.mid(common_prefix);
                        continue;
                    } else {
                        // Partially shared prefix: convert to leaf and call recursively to add a branch.
                        let child = memory.store(TrieNode::Leaf(
                            existing_key.mid(common_prefix).encoded(true).into_vec(),
                            existing_value,
                        ));
                        memory.store_at(
                            handle,
                            TrieNode::Extension(
                                partial.encoded_leftmost(common_prefix, false).into_vec(),
                                NodeHandle::InMemory(child),
                            ),
                        );
                        handle = child;
                        partial = partial.mid(common_prefix);
                        continue;
                    }
                }
                TrieNode::Extension(key, child) => {
                    let existing_key = NibbleSlice::from_encoded(&key).0;
                    let common_prefix = partial.common_prefix(&existing_key);
                    if common_prefix == 0 {
                        let idx = existing_key.at(0) as usize;
                        let mut children: Box<[Option<NodeHandle>; 16]> = Default::default();
                        children[idx] = if existing_key.len() == 1 {
                            Some(child)
                        } else {
                            let ext_node = TrieNode::Extension(
                                existing_key.mid(1).encoded(false).into_vec(),
                                child,
                            );
                            Some(NodeHandle::InMemory(memory.store(ext_node)))
                        };
                        let branch_node = TrieNode::Branch(children, None);
                        memory.store_at(handle, branch_node);
                        continue;
                    } else if common_prefix == existing_key.len() {
                        let child = match child {
                            NodeHandle::Hash(hash) => self.move_node_to_mutable(memory, &hash)?,
                            NodeHandle::InMemory(handle) => handle,
                        };
                        memory.store_at(
                            handle,
                            TrieNode::Extension(key, NodeHandle::InMemory(child)),
                        );
                        handle = child;
                        partial = partial.mid(common_prefix);
                        continue;
                    } else {
                        // Partially shared prefix: covert to shorter extension and recursively add a branch.
                        let child = memory.store(TrieNode::Extension(
                            existing_key.mid(common_prefix).encoded(false).into_vec(),
                            child,
                        ));
                        memory.store_at(
                            handle,
                            TrieNode::Extension(
                                existing_key.encoded_leftmost(common_prefix, false).into_vec(),
                                NodeHandle::InMemory(child),
                            ),
                        );
                        handle = child;
                        partial = partial.mid(common_prefix);
                        continue;
                    }
                }
            }
        }
        Ok(root_handle)
    }

    /// Deletes a node from the trie which has key = `partial` given root node.
    /// Returns (new root node or `None` if this was the node to delete, was it updated).
    /// While deleting keeps track of all the removed / updated nodes in `death_row`.
    fn delete(
        &self,
        memory: &mut NodesStorage,
        node: StorageHandle,
        partial: NibbleSlice,
    ) -> Result<(StorageHandle, bool), Box<dyn std::error::Error>> {
        let mut handle = node;
        let mut partial = partial;
        let root_node = handle;
        let mut path: Vec<StorageHandle> = Vec::new();
        let deleted: bool;
        loop {
            path.push(handle);
            match memory.destroy(handle) {
                TrieNode::Empty => {
                    memory.store_at(handle, TrieNode::Empty);
                    deleted = false;
                    break;
                }
                TrieNode::Leaf(key, value) => {
                    if NibbleSlice::from_encoded(&key).0 == partial {
                        memory.store_at(handle, TrieNode::Empty);
                        deleted = true;
                        break;
                    } else {
                        memory.store_at(handle, TrieNode::Leaf(key, value));
                        deleted = false;
                        break;
                    }
                }
                TrieNode::Branch(mut children, value) => {
                    if partial.is_empty() {
                        if children.iter().filter(|&x| x.is_some()).count() == 0 {
                            memory.store_at(handle, TrieNode::Empty);
                            deleted = value.is_some();
                            break;
                        } else {
                            memory.store_at(handle, TrieNode::Branch(children, None));
                            deleted = value.is_some();
                            break;
                        }
                    } else {
                        let idx = partial.at(0) as usize;
                        if let Some(node_or_hash) = children[idx].take() {
                            let node = match node_or_hash {
                                NodeHandle::Hash(hash) => {
                                    self.move_node_to_mutable(memory, &hash)?
                                }
                                NodeHandle::InMemory(node) => node,
                            };
                            children[idx] = Some(NodeHandle::InMemory(node));
                            memory.store_at(handle, TrieNode::Branch(children, value));
                            handle = node;
                            partial = partial.mid(1);
                            continue;
                        } else {
                            memory.store_at(handle, TrieNode::Branch(children, value));
                            deleted = false;
                            break;
                        }
                    }
                }
                TrieNode::Extension(key, child) => {
                    let (common_prefix, existing_len) = {
                        let existing_key = NibbleSlice::from_encoded(&key).0;
                        (existing_key.common_prefix(&partial), existing_key.len())
                    };
                    if common_prefix == existing_len {
                        let node = match child {
                            NodeHandle::Hash(hash) => self.move_node_to_mutable(memory, &hash)?,
                            NodeHandle::InMemory(node) => node,
                        };
                        memory
                            .store_at(handle, TrieNode::Extension(key, NodeHandle::InMemory(node)));
                        partial = partial.mid(existing_len);
                        handle = node;
                        continue;
                    } else {
                        memory.store_at(handle, TrieNode::Extension(key, child));
                        deleted = false;
                        break;
                    }
                }
            }
        }
        self.fix_nodes(memory, path)?;
        Ok((root_node, deleted))
    }

    fn fix_nodes(
        &self,
        memory: &mut NodesStorage,
        path: Vec<StorageHandle>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for handle in path.into_iter().rev() {
            match memory.destroy(handle) {
                TrieNode::Empty => {
                    memory.store_at(handle, TrieNode::Empty);
                }
                TrieNode::Leaf(key, value) => {
                    memory.store_at(handle, TrieNode::Leaf(key, value));
                }
                TrieNode::Branch(mut children, value) => {
                    children.iter_mut().for_each(|child| {
                        if let Some(NodeHandle::InMemory(h)) = child {
                            if let TrieNode::Empty = memory.node_ref(*h) {
                                *child = None
                            }
                        }
                    });
                    let num_children = children.iter().filter(|&x| x.is_some()).count();
                    if num_children == 0 {
                        if let Some(value) = value {
                            let empty = NibbleSlice::new(&[]).encoded(true).into_vec();
                            memory.store_at(handle, TrieNode::Leaf(empty, value));
                        } else {
                            memory.store_at(handle, TrieNode::Empty);
                        }
                    } else if num_children == 1 && value.is_none() {
                        // Branch with one child becomes extension
                        // Extension followed by leaf becomes leaf
                        // Extension followed by extension becomes extension
                        let idx =
                            children.iter().enumerate().find(|(_i, x)| x.is_some()).unwrap().0;
                        let key = NibbleSlice::new(&[(idx << 4) as u8])
                            .encoded_leftmost(1, false)
                            .into_vec();
                        self.fix_extension_node(
                            memory,
                            handle,
                            key,
                            children[idx].take().unwrap(),
                        )?;
                    } else {
                        memory.store_at(handle, TrieNode::Branch(children, value));
                    }
                }
                TrieNode::Extension(key, child) => {
                    self.fix_extension_node(memory, handle, key, child)?;
                }
            }
        }
        Ok(())
    }

    fn fix_extension_node(
        &self,
        memory: &mut NodesStorage,
        handle: StorageHandle,
        key: Vec<u8>,
        child: NodeHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let child = match child {
            NodeHandle::Hash(hash) => self.move_node_to_mutable(memory, &hash)?,
            NodeHandle::InMemory(h) => h,
        };
        match memory.destroy(child) {
            TrieNode::Empty => {
                memory.store_at(handle, TrieNode::Empty);
            }
            TrieNode::Leaf(child_key, value) => {
                let key = NibbleSlice::from_encoded(&key)
                    .0
                    .merge_encoded(&NibbleSlice::from_encoded(&child_key).0, true)
                    .into_vec();
                memory.store_at(handle, TrieNode::Leaf(key, value));
            }
            TrieNode::Branch(children, value) => {
                memory.store_at(child, TrieNode::Branch(children, value));
                memory.store_at(handle, TrieNode::Extension(key, NodeHandle::InMemory(child)));
            }
            TrieNode::Extension(child_key, child_child) => {
                let key = NibbleSlice::from_encoded(&key)
                    .0
                    .merge_encoded(&NibbleSlice::from_encoded(&child_key).0, false)
                    .into_vec();
                memory.store_at(handle, TrieNode::Extension(key, child_child));
            }
        }
        Ok(())
    }

    fn flatten_nodes(
        old_root: &CryptoHash,
        memory: NodesStorage,
        node: StorageHandle,
    ) -> Result<TrieChanges, Box<dyn std::error::Error>> {
        let mut stack: Vec<(StorageHandle, FlattenNodesCrumb)> = Vec::new();
        stack.push((node, FlattenNodesCrumb::Entering));
        let mut last_hash = CryptoHash::default();
        let mut buffer: Vec<u8> = Vec::new();
        let mut memory = memory;
        while let Some((node, position)) = stack.pop() {
            let raw_node = match memory.node_ref(node) {
                TrieNode::Empty => {
                    last_hash = Trie::empty_root();
                    continue;
                }
                TrieNode::Branch(children, value) => match position {
                    FlattenNodesCrumb::Entering => {
                        let new_children: [Option<CryptoHash>; 16] = Default::default();
                        stack.push((node, FlattenNodesCrumb::AtChild(Box::new(new_children), 0)));
                        continue;
                    }
                    FlattenNodesCrumb::AtChild(mut new_children, mut i) => {
                        if i > 0 && children[i - 1].is_some() {
                            new_children[i - 1] = Some(last_hash);
                        }
                        while i < 16 {
                            match children[i].as_ref() {
                                Some(NodeHandle::InMemory(_)) => {
                                    break;
                                }
                                Some(NodeHandle::Hash(hash)) => {
                                    new_children[i] = Some(*hash);
                                }
                                None => {}
                            }
                            i += 1;
                        }
                        if i < 16 {
                            match children[i].as_ref() {
                                Some(NodeHandle::InMemory(child_node)) => {
                                    stack.push((
                                        node,
                                        FlattenNodesCrumb::AtChild(new_children, i + 1),
                                    ));
                                    stack.push((*child_node, FlattenNodesCrumb::Entering));
                                    continue;
                                }
                                _ => unreachable!(),
                            }
                        }
                        RawTrieNode::Branch(*new_children, value.clone())
                    }
                    FlattenNodesCrumb::Exiting => unreachable!(),
                },
                TrieNode::Extension(key, child) => match position {
                    FlattenNodesCrumb::Entering => match child {
                        NodeHandle::InMemory(child) => {
                            stack.push((node, FlattenNodesCrumb::Exiting));
                            stack.push((*child, FlattenNodesCrumb::Entering));
                            continue;
                        }
                        NodeHandle::Hash(hash) => RawTrieNode::Extension(key.clone(), *hash),
                    },
                    FlattenNodesCrumb::Exiting => RawTrieNode::Extension(key.clone(), last_hash),
                    _ => unreachable!(),
                },
                TrieNode::Leaf(key, value) => RawTrieNode::Leaf(key.clone(), value.clone()),
            };
            raw_node.encode_into(&mut buffer)?;
            let key = hash(&buffer);

            let (_value, rc) =
                memory.refcount_changes.entry(key).or_insert_with(|| (buffer.clone(), 0));
            *rc += 1;
            buffer.clear();
            last_hash = key;
        }
        let (insertions, deletions) =
            Trie::convert_to_insertions_and_deletions(memory.refcount_changes);
        Ok(TrieChanges { old_root: *old_root, new_root: last_hash, insertions, deletions })
    }

    fn convert_to_insertions_and_deletions(
        changes: HashMap<CryptoHash, (Vec<u8>, i32)>,
    ) -> ((Vec<(CryptoHash, Vec<u8>, u32)>, Vec<(CryptoHash, Vec<u8>, u32)>)) {
        let mut deletions = Vec::new();
        let mut insertions = Vec::new();
        for (key, (value, rc)) in changes.into_iter() {
            if rc > 0 {
                insertions.push((key, value, rc as u32));
            } else if rc < 0 {
                deletions.push((key, value, (-rc) as u32));
            }
        }
        // Sort so that trie changes have unique representation
        insertions.sort();
        deletions.sort();
        (insertions, deletions)
    }

    pub fn update<I>(
        &self,
        root: &CryptoHash,
        changes: I,
    ) -> Result<TrieChanges, Box<dyn std::error::Error>>
    where
        I: Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
    {
        let mut memory = NodesStorage::new();
        let mut root_node = self.move_node_to_mutable(&mut memory, root)?;
        for (key, value) in changes {
            let key = NibbleSlice::new(&key);
            match value {
                Some(arr) => {
                    root_node = self.insert(&mut memory, root_node, key, arr)?;
                }
                None => {
                    root_node = match self.delete(&mut memory, root_node, key)? {
                        (value, _) => value,
                    };
                }
            }
        }
        Trie::flatten_nodes(root, memory, root_node)
    }

    pub fn iter<'a>(
        &'a self,
        root: &CryptoHash,
    ) -> Result<TrieIterator<'a>, Box<dyn std::error::Error>> {
        TrieIterator::new(self, root)
    }

    #[inline]
    pub fn update_cache(&self, transaction: &DBTransaction) -> std::io::Result<()> {
        let mut guard = self.storage.cache.lock().expect(POISONED_LOCK_ERR);
        for op in &transaction.ops {
            match op {
                DBOp::Insert { col, ref key, ref value } if *col == COL_STATE => (*guard)
                    .cache_set(
                        CryptoHash::try_from(&key[..]).map_err(|_| {
                            std::io::Error::new(ErrorKind::Other, "Key is always a hash")
                        })?,
                        Some(value.to_vec()),
                    ),
                DBOp::Delete { col, ref key } if *col == COL_STATE => (*guard).cache_set(
                    CryptoHash::try_from(&key[..]).map_err(|_| {
                        std::io::Error::new(ErrorKind::Other, "Key is always a hash")
                    })?,
                    None,
                ),
                _ => {}
            }
        }
        Ok(())
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
    pub fn new(trie: &'a Trie, root: &CryptoHash) -> Result<Self, Box<dyn std::error::Error>> {
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
        Err(format!("Root hash {} not found", root).into())
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
                NodeHandle::InMemory(_node) => unreachable!(),
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
                        if let Some(value) = value {
                            return Some(Ok((self.key(), DBValue::from_slice(value))));
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

#[cfg(test)]
mod tests {
    use rand::seq::SliceRandom;
    use rand::{rngs::ThreadRng, Rng};

    use crate::test_utils::{create_test_store, create_trie};

    use super::*;

    type TrieChanges = Vec<(Vec<u8>, Option<Vec<u8>>)>;

    fn test_populate_trie(trie: Arc<Trie>, root: &CryptoHash, changes: TrieChanges) -> CryptoHash {
        let mut other_changes = changes.clone();
        let (store_update, root) =
            trie.update(root, other_changes.drain(..)).unwrap().into(trie.clone()).unwrap();
        store_update.commit().unwrap();
        for (key, value) in changes {
            assert_eq!(trie.get(&root, &key), value);
        }
        root
    }

    fn test_clear_trie(trie: Arc<Trie>, root: &CryptoHash, changes: TrieChanges) -> CryptoHash {
        let delete_changes: TrieChanges =
            changes.iter().map(|(key, _)| (key.clone(), None)).collect();
        let mut other_delete_changes = delete_changes.clone();
        let (store_update, root) =
            trie.update(root, other_delete_changes.drain(..)).unwrap().into(trie.clone()).unwrap();
        store_update.commit().unwrap();
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
        let root = test_populate_trie(trie.clone(), &empty_root, changes.clone());
        let new_root = test_clear_trie(trie.clone(), &root, changes);
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
        let root = test_populate_trie(trie.clone(), &Trie::empty_root(), pairs.clone());
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
        test_populate_trie(trie, &Trie::empty_root(), changes);
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
        test_populate_trie(trie, &Trie::empty_root(), changes);
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
        let root = test_populate_trie(trie.clone(), &Trie::empty_root(), changes);
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
    fn test_trie_remove_non_existent_key() {
        let trie = create_trie();
        let mut initial = vec![
            (vec![99, 44, 100, 58, 58, 49], Some(vec![1])),
            (vec![99, 44, 100, 58, 58, 50], Some(vec![1])),
            (vec![99, 44, 100, 58, 58, 50, 51], Some(vec![1])),
        ];
        let (store_update, root) = trie
            .update(&Trie::empty_root(), initial.drain(..))
            .unwrap()
            .into(trie.clone())
            .unwrap();
        store_update.commit().unwrap();

        let mut changes = vec![
            (vec![99, 44, 100, 58, 58, 45, 49], None),
            (vec![99, 44, 100, 58, 58, 50, 52], None),
        ];
        let (store_update, root) =
            trie.update(&root, changes.drain(..)).unwrap().into(trie.clone()).unwrap();
        store_update.commit().unwrap();
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
        let (store_update, root) = trie
            .update(&Trie::empty_root(), initial.drain(..))
            .unwrap()
            .into(trie.clone())
            .unwrap();
        store_update.commit().unwrap();
        for r in trie.iter(&root).unwrap() {
            r.unwrap();
        }

        let mut changes = vec![(vec![1, 2, 3], None)];
        let (store_update, root) =
            trie.update(&root, changes.drain(..)).unwrap().into(trie.clone()).unwrap();
        store_update.commit().unwrap();
        for r in trie.iter(&root).unwrap() {
            r.unwrap();
        }
    }

    fn gen_changes(rng: &mut ThreadRng) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let alphabet = &b"abcdefgh"[0..rng.gen_range(2, 8)];
        let max_length = rng.gen_range(2, 8);

        let mut state: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut result = Vec::new();
        let delete_probability = rng.gen_range(0.1, 0.5);
        let size = rng.gen_range(1, 20);
        for _ in 0..size {
            let key_length = rng.gen_range(1, max_length);
            let key: Vec<u8> =
                (0..key_length).map(|_| alphabet.choose(rng).unwrap().clone()).collect();

            let delete = rng.gen_range(0.0, 1.0) < delete_probability;
            if delete {
                let mut keys: Vec<_> = state.keys().cloned().collect();
                keys.push(key);
                let key = keys.choose(rng).unwrap().clone();
                state.remove(&key);
                result.push((key.clone(), None));
            } else {
                let value_length = rng.gen_range(1, max_length);
                let value: Vec<u8> =
                    (0..value_length).map(|_| alphabet.choose(rng).unwrap().clone()).collect();
                result.push((key.clone(), Some(value.clone())));
                state.insert(key, value);
            }
        }
        result
    }

    fn simplify_changes(
        changes: &Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let mut state: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        for (key, value) in changes.iter() {
            if let Some(value) = value {
                state.insert(key.clone(), value.clone());
            } else {
                state.remove(key);
            }
        }
        let mut result: Vec<_> = state.into_iter().map(|(k, v)| (k, Some(v))).collect();
        result.sort();
        result
    }

    #[test]
    fn test_trie_unique() {
        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            let trie = create_trie();
            let trie_changes = gen_changes(&mut rng);
            let simplified_changes = simplify_changes(&trie_changes);

            let (_store_update1, root1) = trie
                .update(&Trie::empty_root(), trie_changes.iter().cloned())
                .unwrap()
                .into(trie.clone())
                .unwrap();
            let (_store_update2, root2) = trie
                .update(&Trie::empty_root(), simplified_changes.iter().cloned())
                .unwrap()
                .into(trie.clone())
                .unwrap();
            if root1 != root2 {
                eprintln!("{:?}", trie_changes);
                eprintln!("{:?}", simplified_changes);
                eprintln!("root1: {}", root1);
                eprintln!("root2: {}", root2);
                panic!("MISMATCH!");
            }
            // TODO: compare state updates?
        }
    }

    #[test]
    fn test_trie_restart() {
        let store = create_test_store();
        let trie1 = Arc::new(Trie::new(store.clone()));
        let empty_root = Trie::empty_root();
        let changes = vec![
            (b"doge".to_vec(), Some(b"coin".to_vec())),
            (b"docu".to_vec(), Some(b"value".to_vec())),
            (b"do".to_vec(), Some(b"verb".to_vec())),
            (b"horse".to_vec(), Some(b"stallion".to_vec())),
            (b"dog".to_vec(), Some(b"puppy".to_vec())),
            (b"h".to_vec(), Some(b"value".to_vec())),
        ];
        let root = test_populate_trie(trie1, &empty_root, changes.clone());

        let trie2 = Arc::new(Trie::new(store));
        assert_eq!(trie2.get(&root, b"doge"), Some(b"coin".to_vec()));
    }
}

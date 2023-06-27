use crate::flat::{FlatStateChanges, FlatStorageChunkView};
pub use crate::trie::config::TrieConfig;
pub(crate) use crate::trie::config::{
    DEFAULT_SHARD_CACHE_DELETIONS_QUEUE_CAPACITY, DEFAULT_SHARD_CACHE_TOTAL_SIZE_LIMIT,
};
use crate::trie::insert_delete::NodesStorage;
use crate::trie::iterator::TrieIterator;
pub use crate::trie::nibble_slice::NibbleSlice;
pub use crate::trie::prefetching_trie_storage::{PrefetchApi, PrefetchError};
pub use crate::trie::shard_tries::{
    KeyForStateChanges, ShardTries, StateSnapshot, StateSnapshotConfig, WrappedTrieChanges,
};
pub use crate::trie::trie_storage::{TrieCache, TrieCachingStorage, TrieDBStorage, TrieStorage};
use crate::trie::trie_storage::{TrieMemoryPartialStorage, TrieRecordingStorage};
use crate::StorageError;
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::challenge::PartialState;
use near_primitives::contract::ContractCode;
use near_primitives::hash::{hash, CryptoHash};
pub use near_primitives::shard_layout::ShardUId;
use near_primitives::state::ValueRef;
use near_primitives::state_record::StateRecord;
use near_primitives::trie_key::TrieKey;
pub use near_primitives::types::TrieNodesCount;
use near_primitives::types::{StateRoot, StateRootNode};
pub use raw_node::{Children, RawTrieNode, RawTrieNodeWithSize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Write;
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::str;

mod config;
mod from_flat;
mod insert_delete;
pub mod iterator;
mod nibble_slice;
mod prefetching_trie_storage;
mod raw_node;
mod shard_tries;
pub mod split_state;
mod state_parts;
mod trie_storage;
#[cfg(test)]
mod trie_tests;
pub mod update;

pub use from_flat::construct_trie_from_flat;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

/// For fraud proofs
#[derive(Debug, Clone)]
pub struct PartialStorage {
    pub nodes: PartialState,
}

#[derive(Clone, Hash, Debug, Copy)]
pub(crate) struct StorageHandle(usize);

#[derive(Clone, Hash, Debug, Copy)]
pub(crate) struct StorageValueHandle(usize);

pub struct TrieCosts {
    pub byte_of_key: u64,
    pub byte_of_value: u64,
    pub node_cost: u64,
}

/// Whether a key lookup will be performed through flat storage or through iterating the trie
pub enum KeyLookupMode {
    FlatStorage,
    Trie,
}

const TRIE_COSTS: TrieCosts = TrieCosts { byte_of_key: 2, byte_of_value: 1, node_cost: 50 };

#[derive(Clone, Hash)]
enum NodeHandle {
    InMemory(StorageHandle),
    Hash(CryptoHash),
}

impl NodeHandle {
    fn unwrap_hash(&self) -> &CryptoHash {
        match self {
            Self::Hash(hash) => hash,
            Self::InMemory(_) => unreachable!(),
        }
    }
}

impl std::fmt::Debug for NodeHandle {
    fn fmt(&self, fmtr: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hash(hash) => write!(fmtr, "{hash}"),
            Self::InMemory(handle) => write!(fmtr, "@{}", handle.0),
        }
    }
}

#[derive(Clone, Hash)]
enum ValueHandle {
    InMemory(StorageValueHandle),
    HashAndSize(ValueRef),
}

impl std::fmt::Debug for ValueHandle {
    fn fmt(&self, fmtr: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HashAndSize(value) => write!(fmtr, "{value:?}"),
            Self::InMemory(StorageValueHandle(num)) => write!(fmtr, "@{num}"),
        }
    }
}

#[derive(Clone, Hash)]
enum TrieNode {
    /// Null trie node. Could be an empty root or an empty branch entry.
    Empty,
    /// Key and value of the leaf node.
    Leaf(Vec<u8>, ValueHandle),
    /// Branch of 16 possible children and value if key ends here.
    Branch(Box<Children<NodeHandle>>, Option<ValueHandle>),
    /// Key and child of extension.
    Extension(Vec<u8>, NodeHandle),
}

#[derive(Clone, Debug)]
pub struct TrieNodeWithSize {
    node: TrieNode,
    memory_usage: u64,
}

impl TrieNodeWithSize {
    fn from_raw(rc_node: RawTrieNodeWithSize) -> TrieNodeWithSize {
        TrieNodeWithSize::new(TrieNode::new(rc_node.node), rc_node.memory_usage)
    }

    fn new(node: TrieNode, memory_usage: u64) -> TrieNodeWithSize {
        TrieNodeWithSize { node, memory_usage }
    }

    fn memory_usage(&self) -> u64 {
        self.memory_usage
    }

    fn empty() -> TrieNodeWithSize {
        TrieNodeWithSize { node: TrieNode::Empty, memory_usage: 0 }
    }
}

impl TrieNode {
    fn new(rc_node: RawTrieNode) -> TrieNode {
        fn new_branch(children: Children, value: Option<ValueRef>) -> TrieNode {
            let children = children.0.map(|el| el.map(NodeHandle::Hash));
            let children = Box::new(Children(children));
            let value = value.map(ValueHandle::HashAndSize);
            TrieNode::Branch(children, value)
        }

        match rc_node {
            RawTrieNode::Leaf(key, value) => TrieNode::Leaf(key, ValueHandle::HashAndSize(value)),
            RawTrieNode::BranchNoValue(children) => new_branch(children, None),
            RawTrieNode::BranchWithValue(value, children) => new_branch(children, Some(value)),
            RawTrieNode::Extension(key, child) => TrieNode::Extension(key, NodeHandle::Hash(child)),
        }
    }

    #[cfg(test)]
    fn print(
        &self,
        f: &mut dyn std::fmt::Write,
        memory: &NodesStorage,
        spaces: &mut String,
    ) -> std::fmt::Result {
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
                spaces.push(' ');
                for (idx, child) in children.iter() {
                    write!(f, "{}{:01x}->", spaces, idx)?;
                    match child {
                        NodeHandle::Hash(hash) => {
                            write!(f, "{}", hash)?;
                        }
                        NodeHandle::InMemory(handle) => {
                            let child = &memory.node_ref(*handle).node;
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
                spaces.push(' ');
                match child {
                    NodeHandle::Hash(hash) => {
                        write!(f, "{}{}", spaces, hash)?;
                    }
                    NodeHandle::InMemory(handle) => {
                        let child = &memory.node_ref(*handle).node;
                        child.print(f, memory, spaces)?;
                    }
                }
                writeln!(f)?;
                spaces.remove(spaces.len() - 1);
            }
        }
        Ok(())
    }

    pub fn has_value(&self) -> bool {
        match self {
            Self::Branch(_, Some(_)) | Self::Leaf(_, _) => true,
            _ => false,
        }
    }

    #[cfg(test)]
    fn deep_to_string(&self, memory: &NodesStorage) -> String {
        let mut buf = String::new();
        self.print(&mut buf, memory, &mut "".to_string()).expect("printing failed");
        buf
    }

    fn memory_usage_for_value_length(value_length: u64) -> u64 {
        value_length * TRIE_COSTS.byte_of_value + TRIE_COSTS.node_cost
    }

    fn memory_usage_value(value: &ValueHandle, memory: Option<&NodesStorage>) -> u64 {
        let value_length = match value {
            ValueHandle::InMemory(handle) => memory
                .expect("InMemory nodes exist, but storage is not provided")
                .value_ref(*handle)
                .len() as u64,
            ValueHandle::HashAndSize(value) => u64::from(value.length),
        };
        Self::memory_usage_for_value_length(value_length)
    }

    fn memory_usage_direct_no_memory(&self) -> u64 {
        self.memory_usage_direct_internal(None)
    }

    fn memory_usage_direct(&self, memory: &NodesStorage) -> u64 {
        self.memory_usage_direct_internal(Some(memory))
    }

    fn memory_usage_direct_internal(&self, memory: Option<&NodesStorage>) -> u64 {
        match self {
            TrieNode::Empty => {
                // DEVNOTE: empty nodes don't exist in storage.
                // In the in-memory implementation Some(TrieNode::Empty) and None are interchangeable as
                // children of branch nodes which means cost has to be 0
                0
            }
            TrieNode::Leaf(key, value) => {
                TRIE_COSTS.node_cost
                    + (key.len() as u64) * TRIE_COSTS.byte_of_key
                    + Self::memory_usage_value(value, memory)
            }
            TrieNode::Branch(_children, value) => {
                TRIE_COSTS.node_cost
                    + value.as_ref().map_or(0, |value| Self::memory_usage_value(value, memory))
            }
            TrieNode::Extension(key, _child) => {
                TRIE_COSTS.node_cost + (key.len() as u64) * TRIE_COSTS.byte_of_key
            }
        }
    }
}

impl std::fmt::Debug for TrieNode {
    /// Formats single trie node.
    ///
    /// Width can be used to specify indentation.
    fn fmt(&self, fmtr: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let empty = "";
        let indent = fmtr.width().unwrap_or(0);
        match self {
            TrieNode::Empty => write!(fmtr, "{empty:indent$}Empty"),
            TrieNode::Leaf(key, value) => write!(
                fmtr,
                "{empty:indent$}Leaf({:?}, {value:?})",
                NibbleSlice::from_encoded(key).0
            ),
            TrieNode::Branch(children, value) => {
                match value {
                    Some(value) => write!(fmtr, "{empty:indent$}Branch({value:?}):"),
                    None => write!(fmtr, "{empty:indent$}Branch:"),
                }?;
                for (idx, child) in children.iter() {
                    write!(fmtr, "\n{empty:indent$} {idx:x}: {child:?}")?;
                }
                Ok(())
            }
            TrieNode::Extension(key, child) => {
                let key = NibbleSlice::from_encoded(key).0;
                write!(fmtr, "{empty:indent$}Extension({key:?}, {child:?})")
            }
        }
    }
}

pub struct Trie {
    pub storage: Rc<dyn TrieStorage>,
    root: StateRoot,
    pub flat_storage_chunk_view: Option<FlatStorageChunkView>,
}

/// Trait for reading data from a trie.
pub trait TrieAccess {
    /// Retrieves value with given key from the trie.
    ///
    /// This doesn’t allow to read data from different chunks (be it from
    /// different shards or different blocks).  That is, the shard and state
    /// root are already known by the object rather than being passed as
    /// argument.
    fn get(&self, key: &TrieKey) -> Result<Option<Vec<u8>>, StorageError>;
}

/// Stores reference count change for some key-value pair in DB.
#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct TrieRefcountChange {
    /// Hash of trie_node_or_value and part of the DB key.
    /// Used for uniting with shard id to get actual DB key.
    trie_node_or_value_hash: CryptoHash,
    /// DB value. Can be either serialized RawTrieNodeWithSize or value corresponding to
    /// some TrieKey.
    trie_node_or_value: Vec<u8>,
    /// Reference count difference which will be added to the total refcount if it corresponds to
    /// insertion and subtracted from it in the case of deletion.
    rc: std::num::NonZeroU32,
}

impl TrieRefcountChange {
    pub fn hash(&self) -> &CryptoHash {
        &self.trie_node_or_value_hash
    }

    pub fn payload(&self) -> &[u8] {
        self.trie_node_or_value.as_slice()
    }
}

impl Hash for TrieRefcountChange {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(&self.trie_node_or_value_hash.0);
        state.write_u32(self.rc.into());
    }
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
#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, Debug)]
pub struct TrieChanges {
    pub old_root: StateRoot,
    pub new_root: StateRoot,
    insertions: Vec<TrieRefcountChange>,
    deletions: Vec<TrieRefcountChange>,
}

impl TrieChanges {
    pub fn empty(old_root: StateRoot) -> Self {
        TrieChanges { old_root, new_root: old_root, insertions: vec![], deletions: vec![] }
    }

    pub fn insertions(&self) -> &[TrieRefcountChange] {
        self.insertions.as_slice()
    }
}

/// Result of applying state part to Trie.
pub struct ApplyStatePartResult {
    /// Trie changes after applying state part.
    pub trie_changes: TrieChanges,
    /// Flat state changes after applying state part, stored as delta.
    pub flat_state_delta: FlatStateChanges,
    /// Contract codes belonging to the state part.
    pub contract_codes: Vec<ContractCode>,
}

enum NodeOrValue {
    Node(RawTrieNodeWithSize),
    Value(std::sync::Arc<[u8]>),
}

impl Trie {
    pub const EMPTY_ROOT: StateRoot = StateRoot::new();

    pub fn new(
        storage: Rc<dyn TrieStorage>,
        root: StateRoot,
        flat_storage_chunk_view: Option<FlatStorageChunkView>,
    ) -> Self {
        Trie { storage, root, flat_storage_chunk_view }
    }

    pub fn recording_reads(&self) -> Self {
        let storage = TrieRecordingStorage {
            storage: Rc::clone(&self.storage),
            recorded: RefCell::new(Default::default()),
        };
        Trie { storage: Rc::new(storage), root: self.root, flat_storage_chunk_view: None }
    }

    pub fn recorded_storage(&self) -> Option<PartialStorage> {
        let storage = self.storage.as_recording_storage()?;
        let mut nodes: Vec<_> =
            storage.recorded.borrow_mut().drain().map(|(_key, value)| value).collect();
        nodes.sort();
        Some(PartialStorage { nodes: PartialState::TrieValues(nodes) })
    }

    pub fn from_recorded_storage(partial_storage: PartialStorage, root: StateRoot) -> Self {
        let PartialState::TrieValues(nodes) = partial_storage.nodes;
        let recorded_storage = nodes.into_iter().map(|value| (hash(&value), value)).collect();
        let storage = Rc::new(TrieMemoryPartialStorage::new(recorded_storage));
        Self::new(storage, root, None)
    }

    pub fn get_root(&self) -> &StateRoot {
        &self.root
    }

    #[cfg(test)]
    fn memory_usage_verify(&self, memory: &NodesStorage, handle: NodeHandle) -> u64 {
        if self.storage.as_recording_storage().is_some() {
            return 0;
        }
        let TrieNodeWithSize { node, memory_usage } = match handle {
            NodeHandle::InMemory(h) => memory.node_ref(h).clone(),
            NodeHandle::Hash(h) => self.retrieve_node(&h).expect("storage failure").1,
        };

        let mut memory_usage_naive = node.memory_usage_direct(memory);
        match &node {
            TrieNode::Empty => {}
            TrieNode::Leaf(_key, _value) => {}
            TrieNode::Branch(children, _value) => {
                memory_usage_naive += children
                    .iter()
                    .map(|(_, handle)| self.memory_usage_verify(memory, handle.clone()))
                    .sum::<u64>();
            }
            TrieNode::Extension(_key, child) => {
                memory_usage_naive += self.memory_usage_verify(memory, child.clone());
            }
        };
        if memory_usage_naive != memory_usage {
            eprintln!("Incorrectly calculated memory usage");
            eprintln!("Correct is {}", memory_usage_naive);
            eprintln!("Computed is {}", memory_usage);
            match handle {
                NodeHandle::InMemory(h) => {
                    eprintln!("TRIE!!!!");
                    eprintln!("{}", memory.node_ref(h).node.deep_to_string(memory));
                }
                NodeHandle::Hash(_h) => {
                    eprintln!("Bad node in storage!");
                }
            };
            assert_eq!(memory_usage_naive, memory_usage);
        }
        memory_usage
    }

    fn delete_value(
        &self,
        memory: &mut NodesStorage,
        value: &ValueHandle,
    ) -> Result<(), StorageError> {
        match value {
            ValueHandle::HashAndSize(value) => {
                let bytes = self.storage.retrieve_raw_bytes(&value.hash)?;
                memory
                    .refcount_changes
                    .entry(value.hash)
                    .or_insert_with(|| (bytes.to_vec(), 0))
                    .1 -= 1;
            }
            ValueHandle::InMemory(_) => {
                // do nothing
            }
        }
        Ok(())
    }

    // Prints the trie nodes starting from hash, up to max_depth depth.
    // The node hash can be any node in the trie.
    pub fn print_recursive(&self, f: &mut dyn std::io::Write, hash: &CryptoHash, max_depth: u32) {
        match self.retrieve_raw_node_or_value(hash) {
            Ok(NodeOrValue::Node(_)) => {
                let mut prefix: Vec<u8> = Vec::new();
                self.print_recursive_internal(f, hash, max_depth, &mut "".to_string(), &mut prefix)
                    .expect("write failed");
            }
            Ok(NodeOrValue::Value(value_bytes)) => {
                writeln!(
                    f,
                    "Given node is a value. Len: {}, Data: {:?} ",
                    value_bytes.len(),
                    &value_bytes[..std::cmp::min(10, value_bytes.len())]
                )
                .expect("write failed");
            }
            Err(err) => {
                writeln!(f, "Error when reading: {}", err).expect("write failed");
            }
        };
    }

    // Prints the trie leaves starting from the state root node, up to max_depth depth.
    // This method can only iterate starting from the root node and it only prints the
    // leaf nodes but it shows output in more human friendly way.
    pub fn print_recursive_leaves(&self, f: &mut dyn std::io::Write, max_depth: u32) {
        let iter = match self.iter_with_max_depth(max_depth as usize) {
            Ok(iter) => iter,
            Err(err) => {
                writeln!(f, "Error when getting the trie iterator: {}", err).expect("write failed");
                return;
            }
        };
        for node in iter {
            let (key, value) = match node {
                Ok((key, value)) => (key, value),
                Err(err) => {
                    writeln!(f, "Failed to iterate node with error: {err}").expect("write failed");
                    continue;
                }
            };

            // Try to parse the key in UTF8 which works only for the simplest keys (e.g. account),
            // or get whitespace padding instead.
            let key_string = match str::from_utf8(&key) {
                Ok(value) => String::from(value),
                Err(_) => " ".repeat(key.len()),
            };
            let state_record = StateRecord::from_raw_key_value(key.clone(), value);

            writeln!(f, "{} {state_record:?}", key_string).expect("write failed");
        }
    }

    // Converts the list of Nibbles to a readable string.
    fn nibbles_to_string(prefix: &[u8]) -> String {
        let (chunks, remainder) = stdx::as_chunks::<2, _>(prefix);
        let mut result = chunks
            .into_iter()
            .map(|chunk| (chunk[0] * 16) + chunk[1])
            .flat_map(|ch| std::ascii::escape_default(ch).map(char::from))
            .collect::<String>();
        if let Some(final_nibble) = remainder.first() {
            write!(&mut result, "\\x{:x}_", final_nibble).unwrap();
        }
        result
    }

    fn print_recursive_internal(
        &self,
        f: &mut dyn std::io::Write,
        hash: &CryptoHash,
        max_depth: u32,
        spaces: &mut String,
        prefix: &mut Vec<u8>,
    ) -> std::io::Result<()> {
        if max_depth == 0 {
            return Ok(());
        }

        let (bytes, raw_node, mem_usage) = match self.retrieve_raw_node(hash) {
            Ok(Some((bytes, raw_node))) => (bytes, raw_node.node, raw_node.memory_usage),
            Ok(None) => return writeln!(f, "{spaces}EmptyNode"),
            Err(err) => return writeln!(f, "{spaces}error {err}"),
        };

        let children = match raw_node {
            RawTrieNode::Leaf(key, value) => {
                let (slice, _) = NibbleSlice::from_encoded(key.as_slice());
                prefix.extend(slice.iter());

                let (chunks, remainder) = stdx::as_chunks::<2, _>(prefix);
                assert!(remainder.is_empty());
                let leaf_key =
                    chunks.into_iter().map(|chunk| (chunk[0] * 16) + chunk[1]).collect::<Vec<u8>>();
                let state_record = StateRecord::from_raw_key_value(leaf_key, bytes.to_vec());

                writeln!(
                    f,
                    "{spaces}Leaf {slice:?} {value:?} prefix:{} hash:{hash} mem_usage:{mem_usage} state_record:{:?}",
                    Self::nibbles_to_string(prefix),
                    state_record.map(|sr|format!("{}", sr)),
                )?;
                prefix.truncate(prefix.len() - slice.len());
                return Ok(());
            }
            RawTrieNode::BranchNoValue(children) => {
                writeln!(
                    f,
                    "{spaces}Branch value:(none) prefix:{} hash:{hash} mem_usage:{mem_usage}",
                    Self::nibbles_to_string(prefix),
                )?;
                children
            }
            RawTrieNode::BranchWithValue(value, children) => {
                writeln!(
                    f,
                    "{spaces}Branch value:{value:?} prefix:{} hash:{hash} mem_usage:{mem_usage}",
                    Self::nibbles_to_string(prefix),
                )?;
                children
            }
            RawTrieNode::Extension(key, child) => {
                let (slice, _) = NibbleSlice::from_encoded(key.as_slice());
                writeln!(
                    f,
                    "{}Extension {:?} child_hash:{} prefix:{} hash:{hash} mem_usage:{mem_usage}",
                    spaces,
                    slice,
                    child,
                    Self::nibbles_to_string(prefix),
                )?;
                spaces.push_str("  ");
                prefix.extend(slice.iter());
                self.print_recursive_internal(f, &child, max_depth - 1, spaces, prefix)?;
                prefix.truncate(prefix.len() - slice.len());
                spaces.truncate(spaces.len() - 2);
                return Ok(());
            }
        };

        for (idx, child) in children.iter() {
            writeln!(f, "{spaces} {idx:01x}->")?;
            spaces.push_str("  ");
            prefix.push(idx);
            self.print_recursive_internal(f, child, max_depth - 1, spaces, prefix)?;
            prefix.pop();
            spaces.truncate(spaces.len() - 2);
        }

        Ok(())
    }

    fn retrieve_raw_node(
        &self,
        hash: &CryptoHash,
    ) -> Result<Option<(std::sync::Arc<[u8]>, RawTrieNodeWithSize)>, StorageError> {
        if hash == &Self::EMPTY_ROOT {
            return Ok(None);
        }
        let bytes = self.storage.retrieve_raw_bytes(hash)?;
        let node = RawTrieNodeWithSize::try_from_slice(&bytes).map_err(|err| {
            StorageError::StorageInconsistentState(format!("Failed to decode node {hash}: {err}"))
        })?;
        Ok(Some((bytes, node)))
    }

    // Similar to retrieve_raw_node but handles the case where there is a Value (and not a Node) in the database.
    // This method is not safe to be used in any real scenario as it can incorrectly interpret a value as a trie node.
    // It's only provided as a convenience for debugging tools.
    fn retrieve_raw_node_or_value(&self, hash: &CryptoHash) -> Result<NodeOrValue, StorageError> {
        let bytes = self.storage.retrieve_raw_bytes(hash)?;
        match RawTrieNodeWithSize::try_from_slice(&bytes) {
            Ok(node) => Ok(NodeOrValue::Node(node)),
            Err(_) => Ok(NodeOrValue::Value(bytes)),
        }
    }

    fn move_node_to_mutable(
        &self,
        memory: &mut NodesStorage,
        hash: &CryptoHash,
    ) -> Result<StorageHandle, StorageError> {
        match self.retrieve_raw_node(hash)? {
            None => Ok(memory.store(TrieNodeWithSize::empty())),
            Some((bytes, node)) => {
                let result = memory.store(TrieNodeWithSize::from_raw(node));
                memory.refcount_changes.entry(*hash).or_insert_with(|| (bytes.to_vec(), 0)).1 -= 1;
                Ok(result)
            }
        }
    }

    /// Retrieves decoded node alongside with its raw bytes representation.
    ///
    /// Note that because Empty nodes (those which are referenced by
    /// [`Self::EMPTY_ROOT`] hash) aren’t stored in the database, they don’t
    /// have a bytes representation.  For those nodes the first return value
    /// will be `None`.
    fn retrieve_node(
        &self,
        hash: &CryptoHash,
    ) -> Result<(Option<std::sync::Arc<[u8]>>, TrieNodeWithSize), StorageError> {
        match self.retrieve_raw_node(hash)? {
            None => Ok((None, TrieNodeWithSize::empty())),
            Some((bytes, node)) => Ok((Some(bytes), TrieNodeWithSize::from_raw(node))),
        }
    }

    pub fn retrieve_root_node(&self) -> Result<StateRootNode, StorageError> {
        match self.retrieve_raw_node(&self.root)? {
            None => Ok(StateRootNode::empty()),
            Some((bytes, node)) => {
                Ok(StateRootNode { data: bytes, memory_usage: node.memory_usage })
            }
        }
    }

    fn lookup(&self, mut key: NibbleSlice<'_>) -> Result<Option<ValueRef>, StorageError> {
        let mut hash = self.root;
        loop {
            let node = match self.retrieve_raw_node(&hash)? {
                None => return Ok(None),
                Some((_bytes, node)) => node.node,
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
                RawTrieNode::BranchNoValue(mut children) => {
                    if key.is_empty() {
                        return Ok(None);
                    } else if let Some(h) = children[key.at(0)].take() {
                        hash = h;
                        key = key.mid(1);
                    } else {
                        return Ok(None);
                    }
                }
                RawTrieNode::BranchWithValue(value, mut children) => {
                    if key.is_empty() {
                        return Ok(Some(value));
                    } else if let Some(h) = children[key.at(0)].take() {
                        hash = h;
                        key = key.mid(1);
                    } else {
                        return Ok(None);
                    }
                }
            };
        }
    }

    /// Return the value reference to the `key`
    /// `mode`: whether we will try to perform the lookup through flat storage or trie.
    ///         Note that even if `mode == KeyLookupMode::FlatStorage`, we still may not use
    ///         flat storage if the trie is not created with a flat storage object in it.
    ///         Such double check may seem redundant but it is necessary for now.
    ///         Not all tries are created with flat storage, for example, we don't
    ///         enable flat storage for state-viewer. And we do not use flat
    ///         storage for key lookup performed in `storage_write`, so we need
    ///         the `use_flat_storage` to differentiate whether the lookup is performed for
    ///         storage_write or not.
    pub fn get_ref(
        &self,
        key: &[u8],
        mode: KeyLookupMode,
    ) -> Result<Option<ValueRef>, StorageError> {
        let use_flat_storage =
            matches!(mode, KeyLookupMode::FlatStorage) && self.flat_storage_chunk_view.is_some();

        if use_flat_storage {
            let flat_state_value =
                self.flat_storage_chunk_view.as_ref().unwrap().get_value(&key)?;
            Ok(flat_state_value.map(|value| value.to_value_ref()))
        } else {
            let key_nibbles = NibbleSlice::new(key);
            self.lookup(key_nibbles)
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        match self.get_ref(key, KeyLookupMode::FlatStorage)? {
            Some(ValueRef { hash, .. }) => {
                self.storage.retrieve_raw_bytes(&hash).map(|bytes| Some(bytes.to_vec()))
            }
            None => Ok(None),
        }
    }

    pub(crate) fn convert_to_insertions_and_deletions(
        changes: HashMap<CryptoHash, (Vec<u8>, i32)>,
    ) -> (Vec<TrieRefcountChange>, Vec<TrieRefcountChange>) {
        let mut deletions = Vec::new();
        let mut insertions = Vec::new();
        for (trie_node_or_value_hash, (trie_node_or_value, rc)) in changes.into_iter() {
            if rc > 0 {
                insertions.push(TrieRefcountChange {
                    trie_node_or_value_hash,
                    trie_node_or_value,
                    rc: std::num::NonZeroU32::new(rc as u32).unwrap(),
                });
            } else if rc < 0 {
                deletions.push(TrieRefcountChange {
                    trie_node_or_value_hash,
                    trie_node_or_value,
                    rc: std::num::NonZeroU32::new((-rc) as u32).unwrap(),
                });
            }
        }
        // Sort so that trie changes have unique representation
        insertions.sort();
        deletions.sort();
        (insertions, deletions)
    }

    pub fn update<I>(&self, changes: I) -> Result<TrieChanges, StorageError>
    where
        I: IntoIterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
    {
        let mut memory = NodesStorage::new();
        let mut root_node = self.move_node_to_mutable(&mut memory, &self.root)?;
        for (key, value) in changes {
            let key = NibbleSlice::new(&key);
            root_node = match value {
                Some(arr) => self.insert(&mut memory, root_node, key, arr),
                None => self.delete(&mut memory, root_node, key),
            }?;
        }

        #[cfg(test)]
        {
            self.memory_usage_verify(&memory, NodeHandle::InMemory(root_node));
        }
        Trie::flatten_nodes(&self.root, memory, root_node)
    }

    pub fn iter<'a>(&'a self) -> Result<TrieIterator<'a>, StorageError> {
        TrieIterator::new(self, None)
    }

    pub fn iter_with_max_depth<'a>(
        &'a self,
        max_depth: usize,
    ) -> Result<TrieIterator<'a>, StorageError> {
        TrieIterator::new(
            self,
            Some(Box::new(move |key_nibbles: &Vec<u8>| key_nibbles.len() > max_depth)),
        )
    }

    pub fn iter_with_prune_condition<'a>(
        &'a self,
        prune_condition: Option<Box<dyn Fn(&Vec<u8>) -> bool>>,
    ) -> Result<TrieIterator<'a>, StorageError> {
        TrieIterator::new(self, prune_condition)
    }

    pub fn get_trie_nodes_count(&self) -> TrieNodesCount {
        self.storage.get_trie_nodes_count()
    }
}

impl TrieAccess for Trie {
    fn get(&self, key: &TrieKey) -> Result<Option<Vec<u8>>, StorageError> {
        Trie::get(self, &key.to_vec())
    }
}

/// Methods used in the runtime-parameter-estimator for measuring trie internal
/// operations.
pub mod estimator {
    use borsh::{BorshDeserialize, BorshSerialize};
    use near_primitives::hash::CryptoHash;

    /// Create an encoded extension node with the given value as the key.
    /// This serves no purpose other than for the estimator.
    pub fn encode_extension_node(key: Vec<u8>) -> Vec<u8> {
        let hash = CryptoHash::hash_bytes(&key);
        let node = super::RawTrieNode::Extension(key, hash);
        let node = super::RawTrieNodeWithSize { node, memory_usage: 1 };
        node.try_to_vec().unwrap()
    }
    /// Decode am extension node and return its inner key.
    /// This serves no purpose other than for the estimator.
    pub fn decode_extension_node(bytes: &[u8]) -> Vec<u8> {
        let node = super::RawTrieNodeWithSize::try_from_slice(bytes).unwrap();
        match node.node {
            super::RawTrieNode::Extension(v, _) => v,
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use crate::test_utils::{
        create_test_store, create_tries, create_tries_complex, gen_changes, simplify_changes,
        test_populate_trie,
    };
    use crate::DBCol;

    use super::*;

    type TrieChanges = Vec<(Vec<u8>, Option<Vec<u8>>)>;
    const SHARD_VERSION: u32 = 1;

    fn test_clear_trie(
        tries: &ShardTries,
        root: &CryptoHash,
        shard_uid: ShardUId,
        changes: TrieChanges,
    ) -> CryptoHash {
        let delete_changes: TrieChanges =
            changes.iter().map(|(key, _)| (key.clone(), None)).collect();
        let trie_changes =
            tries.get_trie_for_shard(shard_uid, *root).update(delete_changes).unwrap();
        let mut store_update = tries.store_update();
        let root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
        let trie = tries.get_trie_for_shard(shard_uid, root);
        store_update.commit().unwrap();
        for (key, _) in changes {
            assert_eq!(trie.get(&key), Ok(None));
        }
        root
    }

    #[test]
    fn test_basic_trie() {
        // test trie version > 0
        let tries = create_tries_complex(SHARD_VERSION, 2);
        let shard_uid = ShardUId { version: SHARD_VERSION, shard_id: 0 };
        let trie = tries.get_trie_for_shard(shard_uid, Trie::EMPTY_ROOT);
        assert_eq!(trie.get(&[122]), Ok(None));
        let changes = vec![
            (b"doge".to_vec(), Some(b"coin".to_vec())),
            (b"docu".to_vec(), Some(b"value".to_vec())),
            (b"do".to_vec(), Some(b"verb".to_vec())),
            (b"horse".to_vec(), Some(b"stallion".to_vec())),
            (b"dog".to_vec(), Some(b"puppy".to_vec())),
            (b"h".to_vec(), Some(b"value".to_vec())),
        ];
        let root = test_populate_trie(&tries, &Trie::EMPTY_ROOT, shard_uid, changes.clone());
        let new_root = test_clear_trie(&tries, &root, shard_uid, changes);
        assert_eq!(new_root, Trie::EMPTY_ROOT);
        assert_eq!(trie.iter().unwrap().fold(0, |acc, _| acc + 1), 0);
    }

    #[test]
    fn test_trie_iter() {
        let tries = create_tries_complex(SHARD_VERSION, 2);
        let shard_uid = ShardUId { version: SHARD_VERSION, shard_id: 0 };
        let pairs = vec![
            (b"a".to_vec(), Some(b"111".to_vec())),
            (b"b".to_vec(), Some(b"222".to_vec())),
            (b"x".to_vec(), Some(b"333".to_vec())),
            (b"y".to_vec(), Some(b"444".to_vec())),
        ];
        let root = test_populate_trie(&tries, &Trie::EMPTY_ROOT, shard_uid, pairs.clone());
        let trie = tries.get_trie_for_shard(shard_uid, root);
        let mut iter_pairs = vec![];
        for pair in trie.iter().unwrap() {
            let (key, value) = pair.unwrap();
            iter_pairs.push((key, Some(value.to_vec())));
        }
        assert_eq!(pairs, iter_pairs);

        let assert_has_next = |want, other_iter: &mut TrieIterator| {
            assert_eq!(Some(want), other_iter.next().map(|item| item.unwrap().0).as_deref());
        };

        let mut other_iter = trie.iter().unwrap();
        other_iter.seek_prefix(b"r").unwrap();
        assert_eq!(other_iter.next(), None);
        other_iter.seek_prefix(b"x").unwrap();
        assert_has_next(b"x", &mut other_iter);
        assert_eq!(other_iter.next(), None);
        other_iter.seek_prefix(b"y").unwrap();
        assert_has_next(b"y", &mut other_iter);
        assert_eq!(other_iter.next(), None);
    }

    #[test]
    fn test_trie_leaf_into_branch() {
        let tries = create_tries_complex(SHARD_VERSION, 2);
        let shard_uid = ShardUId { version: SHARD_VERSION, shard_id: 0 };
        let changes = vec![
            (b"dog".to_vec(), Some(b"puppy".to_vec())),
            (b"dog2".to_vec(), Some(b"puppy".to_vec())),
            (b"xxx".to_vec(), Some(b"puppy".to_vec())),
        ];
        test_populate_trie(&tries, &Trie::EMPTY_ROOT, shard_uid, changes);
    }

    #[test]
    fn test_trie_same_node() {
        let tries = create_tries();
        let changes = vec![
            (b"dogaa".to_vec(), Some(b"puppy".to_vec())),
            (b"dogbb".to_vec(), Some(b"puppy".to_vec())),
            (b"cataa".to_vec(), Some(b"puppy".to_vec())),
            (b"catbb".to_vec(), Some(b"puppy".to_vec())),
            (b"dogax".to_vec(), Some(b"puppy".to_vec())),
        ];
        test_populate_trie(&tries, &Trie::EMPTY_ROOT, ShardUId::single_shard(), changes);
    }

    #[test]
    fn test_trie_iter_seek_stop_at_extension() {
        let tries = create_tries();
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
        let root = test_populate_trie(&tries, &Trie::EMPTY_ROOT, ShardUId::single_shard(), changes);
        let trie = tries.get_trie_for_shard(ShardUId::single_shard(), root);
        let mut iter = trie.iter().unwrap();
        iter.seek_prefix(&[0, 116, 101, 115, 116, 44]).unwrap();
        let mut pairs = vec![];
        for pair in iter {
            pairs.push(pair.unwrap().0);
        }
        assert_eq!(
            pairs,
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
        let tries = create_tries();
        let initial = vec![
            (vec![99, 44, 100, 58, 58, 49], Some(vec![1])),
            (vec![99, 44, 100, 58, 58, 50], Some(vec![1])),
            (vec![99, 44, 100, 58, 58, 50, 51], Some(vec![1])),
        ];
        let root = test_populate_trie(&tries, &Trie::EMPTY_ROOT, ShardUId::single_shard(), initial);

        let changes = vec![
            (vec![99, 44, 100, 58, 58, 45, 49], None),
            (vec![99, 44, 100, 58, 58, 50, 52], None),
        ];
        let root = test_populate_trie(&tries, &root, ShardUId::single_shard(), changes);
        let trie = tries.get_trie_for_shard(ShardUId::single_shard(), root);
        for r in trie.iter().unwrap() {
            r.unwrap();
        }
    }

    #[test]
    fn test_equal_leafs() {
        let initial = vec![
            (vec![1, 2, 3], Some(vec![1])),
            (vec![2, 2, 3], Some(vec![1])),
            (vec![3, 2, 3], Some(vec![1])),
        ];
        let tries = create_tries();
        let root = test_populate_trie(&tries, &Trie::EMPTY_ROOT, ShardUId::single_shard(), initial);
        tries.get_trie_for_shard(ShardUId::single_shard(), root).iter().unwrap().for_each(
            |result| {
                result.unwrap();
            },
        );

        let changes = vec![(vec![1, 2, 3], None)];
        let root = test_populate_trie(&tries, &root, ShardUId::single_shard(), changes);
        tries.get_trie_for_shard(ShardUId::single_shard(), root).iter().unwrap().for_each(
            |result| {
                result.unwrap();
            },
        );
    }

    #[test]
    fn test_trie_unique() {
        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            let tries = create_tries();
            let trie = tries.get_trie_for_shard(ShardUId::single_shard(), Trie::EMPTY_ROOT);
            let trie_changes = gen_changes(&mut rng, 20);
            let simplified_changes = simplify_changes(&trie_changes);

            let trie_changes1 = trie.update(trie_changes.iter().cloned()).unwrap();
            let trie_changes2 = trie.update(simplified_changes.iter().cloned()).unwrap();
            if trie_changes1.new_root != trie_changes2.new_root {
                eprintln!("{:?}", trie_changes);
                eprintln!("{:?}", simplified_changes);
                eprintln!("root1: {:?}", trie_changes1.new_root);
                eprintln!("root2: {:?}", trie_changes2.new_root);
                panic!("MISMATCH!");
            }
            // TODO: compare state updates?
        }
    }

    #[test]
    fn test_iterator_seek_prefix() {
        let mut rng = rand::thread_rng();
        for _test_run in 0..10 {
            let tries = create_tries();
            let trie_changes = gen_changes(&mut rng, 500);
            let state_root = test_populate_trie(
                &tries,
                &Trie::EMPTY_ROOT,
                ShardUId::single_shard(),
                trie_changes.clone(),
            );
            let trie = tries.get_trie_for_shard(ShardUId::single_shard(), state_root);

            // Those known keys.
            for (key, value) in trie_changes.into_iter().collect::<HashMap<_, _>>() {
                if let Some(value) = value {
                    let want = Some(Ok((key.clone(), value)));
                    let mut iterator = trie.iter().unwrap();
                    iterator.seek_prefix(&key).unwrap();
                    assert_eq!(want, iterator.next(), "key: {key:x?}");
                }
            }

            // Test some more random keys.
            let queries = gen_changes(&mut rng, 500).into_iter().map(|(key, _)| key);
            for query in queries {
                let mut iterator = trie.iter().unwrap();
                iterator.seek_prefix(&query).unwrap();
                if let Some(Ok((key, _))) = iterator.next() {
                    assert!(key.starts_with(&query), "‘{key:x?}’ does not start with ‘{query:x?}’");
                }
            }
        }
    }

    #[test]
    fn test_refcounts() {
        let mut rng = rand::thread_rng();
        for _test_run in 0..10 {
            let num_iterations = rng.gen_range(1..20);
            let tries = create_tries();
            let store = tries.get_store();
            let mut state_root = Trie::EMPTY_ROOT;
            for _ in 0..num_iterations {
                let trie_changes = gen_changes(&mut rng, 20);
                state_root =
                    test_populate_trie(&tries, &state_root, ShardUId::single_shard(), trie_changes);
                let memory_usage = tries
                    .get_trie_for_shard(ShardUId::single_shard(), state_root)
                    .retrieve_root_node()
                    .unwrap()
                    .memory_usage;
                println!("New memory_usage: {memory_usage}");
            }

            let trie = tries.get_trie_for_shard(ShardUId::single_shard(), state_root);
            let trie_changes = trie
                .iter()
                .unwrap()
                .map(|item| {
                    let (key, _) = item.unwrap();
                    (key, None)
                })
                .collect::<Vec<_>>();
            state_root =
                test_populate_trie(&tries, &state_root, ShardUId::single_shard(), trie_changes);
            assert_eq!(state_root, Trie::EMPTY_ROOT, "Trie must be empty");
            assert!(store.iter(DBCol::State).peekable().peek().is_none(), "Storage must be empty");
        }
    }

    #[test]
    fn test_trie_restart() {
        let store = create_test_store();
        let tries = ShardTries::test(store.clone(), 1);
        let empty_root = Trie::EMPTY_ROOT;
        let changes = vec![
            (b"doge".to_vec(), Some(b"coin".to_vec())),
            (b"docu".to_vec(), Some(b"value".to_vec())),
            (b"do".to_vec(), Some(b"verb".to_vec())),
            (b"horse".to_vec(), Some(b"stallion".to_vec())),
            (b"dog".to_vec(), Some(b"puppy".to_vec())),
            (b"h".to_vec(), Some(b"value".to_vec())),
        ];
        let root = test_populate_trie(&tries, &empty_root, ShardUId::single_shard(), changes);

        let tries2 = ShardTries::test(store, 1);
        let trie2 = tries2.get_trie_for_shard(ShardUId::single_shard(), root);
        assert_eq!(trie2.get(b"doge"), Ok(Some(b"coin".to_vec())));
    }

    // TODO: somehow also test that we don't record unnecessary nodes
    #[test]
    fn test_trie_recording_reads() {
        let store = create_test_store();
        let tries = ShardTries::test(store, 1);
        let empty_root = Trie::EMPTY_ROOT;
        let changes = vec![
            (b"doge".to_vec(), Some(b"coin".to_vec())),
            (b"docu".to_vec(), Some(b"value".to_vec())),
            (b"do".to_vec(), Some(b"verb".to_vec())),
            (b"horse".to_vec(), Some(b"stallion".to_vec())),
            (b"dog".to_vec(), Some(b"puppy".to_vec())),
            (b"h".to_vec(), Some(b"value".to_vec())),
        ];
        let root = test_populate_trie(&tries, &empty_root, ShardUId::single_shard(), changes);

        let trie2 = tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads();
        trie2.get(b"dog").unwrap();
        trie2.get(b"horse").unwrap();
        let partial_storage = trie2.recorded_storage();

        let trie3 = Trie::from_recorded_storage(partial_storage.unwrap(), root);

        assert_eq!(trie3.get(b"dog"), Ok(Some(b"puppy".to_vec())));
        assert_eq!(trie3.get(b"horse"), Ok(Some(b"stallion".to_vec())));
        assert_eq!(trie3.get(b"doge"), Err(StorageError::MissingTrieValue));
    }

    #[test]
    fn test_trie_recording_reads_update() {
        let store = create_test_store();
        let tries = ShardTries::test(store, 1);
        let empty_root = Trie::EMPTY_ROOT;
        let changes = vec![
            (b"doge".to_vec(), Some(b"coin".to_vec())),
            (b"docu".to_vec(), Some(b"value".to_vec())),
        ];
        let root = test_populate_trie(&tries, &empty_root, ShardUId::single_shard(), changes);
        // Trie: extension -> branch -> 2 leaves
        {
            let trie2 = tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads();
            trie2.get(b"doge").unwrap();
            // record extension, branch and one leaf with value, but not the other
            assert_eq!(trie2.recorded_storage().unwrap().nodes.len(), 4);
        }

        {
            let trie2 = tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads();
            let updates = vec![(b"doge".to_vec(), None)];
            trie2.update(updates).unwrap();
            // record extension, branch and both leaves (one with value)
            assert_eq!(trie2.recorded_storage().unwrap().nodes.len(), 5);
        }

        {
            let trie2 = tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads();
            let updates = vec![(b"dodo".to_vec(), Some(b"asdf".to_vec()))];
            trie2.update(updates).unwrap();
            // record extension and branch, but not leaves
            assert_eq!(trie2.recorded_storage().unwrap().nodes.len(), 2);
        }
    }

    #[test]
    fn test_dump_load_trie() {
        let store = create_test_store();
        let tries = ShardTries::test(store.clone(), 1);
        let empty_root = Trie::EMPTY_ROOT;
        let changes = vec![
            (b"doge".to_vec(), Some(b"coin".to_vec())),
            (b"docu".to_vec(), Some(b"value".to_vec())),
        ];
        let root = test_populate_trie(&tries, &empty_root, ShardUId::single_shard(), changes);
        let dir = tempfile::Builder::new().prefix("test_dump_load_trie").tempdir().unwrap();
        store.save_state_to_file(&dir.path().join("test.bin")).unwrap();
        let store2 = create_test_store();
        store2.load_state_from_file(&dir.path().join("test.bin")).unwrap();
        let tries2 = ShardTries::test(store2, 1);
        let trie2 = tries2.get_trie_for_shard(ShardUId::single_shard(), root);
        assert_eq!(trie2.get(b"doge").unwrap().unwrap(), b"coin");
    }
}

use near_primitives::errors::StorageError;
use near_primitives::state::FlatStateValue;

use crate::trie::{AccessOptions, TRIE_COSTS, ValueHandle};

/// For updated nodes, the ID is simply the index into the array of updated nodes we keep.
pub type UpdatedNodeId = usize;

/// Value to insert to trie or update existing value in the trie.
#[derive(Debug, Clone)]
pub enum GenericTrieValue {
    /// Value to update both memtrie and trie storage. Full value is required
    /// for that.
    MemtrieAndDisk(Vec<u8>),
    /// Value to update only memtrie. In such case it is enough to have a
    /// `FlatStateValue`.
    MemtrieOnly(FlatStateValue),
}

/// Trait for trie values to get their length.
pub trait HasValueLength {
    fn len(&self) -> u64;
}

impl HasValueLength for FlatStateValue {
    fn len(&self) -> u64 {
        self.value_len() as u64
    }
}

impl HasValueLength for ValueHandle {
    fn len(&self) -> u64 {
        match self {
            ValueHandle::HashAndSize(value) => value.length as u64,
            ValueHandle::InMemory(value) => value.1 as u64,
        }
    }
}

/// This enum represents pointer to a node in trie or pointer to an updated node in trie.
///
/// An old node is the pointer to a node currently in trie.
/// An updated node is the index in the TrieUpdate struct where we temporarily store updates
/// These eventually get written back to the trie during finalization and commit of trie update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenericNodeOrIndex<GenericTrieNodePtr> {
    Old(GenericTrieNodePtr),
    Updated(UpdatedNodeId),
}

/// A generic representation of a trie node
/// TrieNodePtr can potentially be of any type including GenericTrieNodePtr for normal nodes or
/// GenericNodeOrIndex<GenericTrieNodePtr> for updated nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenericTrieNode<TrieNodePtr, GenericValueHandle> {
    /// Used for either an empty root node (indicating an empty trie), or as a temporary
    /// node to ease implementation.
    Empty,
    Leaf {
        extension: Box<[u8]>,
        value: GenericValueHandle,
    },
    Extension {
        extension: Box<[u8]>,
        child: TrieNodePtr,
    },
    /// Corresponds to either a Branch or BranchWithValue node.
    Branch {
        children: Box<[Option<TrieNodePtr>; 16]>,
        value: Option<GenericValueHandle>,
    },
}

impl<N, V> Default for GenericTrieNode<N, V> {
    fn default() -> Self {
        Self::Empty
    }
}

impl<N, V> GenericTrieNode<N, V>
where
    V: HasValueLength,
{
    fn memory_usage_value(value_length: u64) -> u64 {
        value_length * TRIE_COSTS.byte_of_value + TRIE_COSTS.node_cost
    }

    /// Returns the memory usage of the **single** node, in Near's trie cost
    /// terms, not in terms of the physical memory usage.
    pub fn memory_usage_direct(&self) -> u64 {
        match self {
            Self::Empty => {
                // DEVNOTE: empty nodes don't exist in storage.
                // In the in-memory implementation Some(TrieNode::Empty) and None are interchangeable as
                // children of branch nodes which means cost has to be 0
                0
            }
            Self::Leaf { extension, value } => {
                TRIE_COSTS.node_cost
                    + (extension.len() as u64) * TRIE_COSTS.byte_of_key
                    + Self::memory_usage_value(value.len())
            }
            Self::Branch { value, .. } => {
                TRIE_COSTS.node_cost
                    + value.as_ref().map_or(0, |value| Self::memory_usage_value(value.len()))
            }
            Self::Extension { extension, .. } => {
                TRIE_COSTS.node_cost + (extension.len() as u64) * TRIE_COSTS.byte_of_key
            }
        }
    }
}

/// An updated node - a node that will eventually become a trie node.
/// It references children that are of type GenericNodeOrIndex<GenericTrieNodePtr>
/// i.e. either old nodes or updated nodes.
pub type GenericUpdatedTrieNode<GenericTrieNodePtr, GenericValueHandle> =
    GenericTrieNode<GenericNodeOrIndex<GenericTrieNodePtr>, GenericValueHandle>;

/// Simple conversion from GenericTrieNode to GenericUpdatedTrieNode
/// We change all occurrences of GenericTrieNodePtr to GenericNodeOrIndex::Old(ptr)
impl<N, V> From<GenericTrieNode<N, V>> for GenericUpdatedTrieNode<N, V> {
    fn from(node: GenericTrieNode<N, V>) -> Self {
        match node {
            GenericTrieNode::Empty => Self::Empty,
            GenericTrieNode::Leaf { extension, value } => Self::Leaf { extension, value },
            GenericTrieNode::Extension { extension, child } => {
                Self::Extension { extension, child: GenericNodeOrIndex::Old(child) }
            }
            GenericTrieNode::Branch { children, value } => {
                let children = Box::new(children.map(|child| child.map(GenericNodeOrIndex::Old)));
                Self::Branch { children, value }
            }
        }
    }
}

/// A trie node with its memory usage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenericTrieNodeWithSize<GenericTrieNodePtr, GenericValueHandle> {
    pub node: GenericTrieNode<GenericTrieNodePtr, GenericValueHandle>,
    pub memory_usage: u64,
}

impl<N, V> Default for GenericTrieNodeWithSize<N, V> {
    fn default() -> Self {
        Self { node: Default::default(), memory_usage: 0 }
    }
}

/// An updated node with its memory usage.
/// Needed to recompute subtree function (memory usage) on the fly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenericUpdatedTrieNodeWithSize<GenericTrieNodePtr, GenericValueHandle> {
    pub node: GenericUpdatedTrieNode<GenericTrieNodePtr, GenericValueHandle>,
    pub memory_usage: u64,
}

impl<N, V> GenericUpdatedTrieNodeWithSize<N, V> {
    pub fn empty() -> Self {
        Self { node: GenericUpdatedTrieNode::Empty, memory_usage: 0 }
    }
}

impl<N, V> From<GenericTrieNodeWithSize<N, V>> for GenericUpdatedTrieNodeWithSize<N, V> {
    fn from(node: GenericTrieNodeWithSize<N, V>) -> Self {
        Self { node: node.node.into(), memory_usage: node.memory_usage }
    }
}

/// Trait for trie updates to handle updated nodes.
///
/// So far, this is used to handle key-value insertions, deletions and range
/// retain operation. To be performant, such logic requires keeping track of
/// intermediate updated nodes, together with subtree function (memory usage).
///
/// `GenericTrieUpdate` abstracts the storage of updated nodes for the original
/// node type `GenericTrieNodePtr`.
///
/// In this storage, nodes are indexed by `UpdatedNodeId`.
/// Node is stored as `GenericUpdatedTrieNodeWithSize`, which stores children
/// as `GenericNodeOrIndex`. Each child may be either an old node or an updated
/// node.
///
/// The flow of interaction with this storage is:
/// - In the beginning, call `ensure_updated` for the
/// `GenericNodeOrIndex::Old(root_node)` which returns `UpdatedNodeId`,
/// it should be zero.
/// - For every update (single insert, single delete, recursive range
/// operation...), call corresponding method with `UpdatedNodeId` for
/// the root.
/// - Then, we hold the invariant that on every descent we have
/// `UpdatedNodeId`.
/// - So, first, we call `take_node` to get `GenericUpdatedTrieNodeWithSize`
/// back;
/// - We possibly descend into its children and modify the node;
/// - Then, we call `place_node` to put the node back and return to the
/// node parent.
/// - Finally, we end up with storage of new nodes, which are used to produce
/// new state root. The exact logic depends on trait implementation.
///
/// TODO(#12361): instead of `GenericValueHandle`, consider always using
/// `FlatStateValue`.
///
/// Note that it has nothing to do with `TrieUpdate` used for runtime to store
/// temporary state changes (TODO(#12361) - consider renaming it).
pub(crate) trait GenericTrieUpdate<'a, GenericTrieNodePtr, GenericValueHandle> {
    /// If the ID was old, converts underlying node to an updated one.
    fn ensure_updated(
        &mut self,
        node: GenericNodeOrIndex<GenericTrieNodePtr>,
        operation_options: AccessOptions,
    ) -> Result<UpdatedNodeId, StorageError>;

    /// Takes a node from the set of updated nodes, setting it to None.
    /// It is expected that place_node is then called to return the node to
    /// the same slot.
    fn take_node(
        &mut self,
        node_id: UpdatedNodeId,
    ) -> GenericUpdatedTrieNodeWithSize<GenericTrieNodePtr, GenericValueHandle>;

    /// Puts a node to the set of updated nodes at specific index.
    /// Needed when reference to node from parent needs to be preserved.
    fn place_node_at(
        &mut self,
        node_id: UpdatedNodeId,
        node: GenericUpdatedTrieNodeWithSize<GenericTrieNodePtr, GenericValueHandle>,
    );

    /// Puts a new node into the set of updated nodes.
    fn place_node(
        &mut self,
        node: GenericUpdatedTrieNodeWithSize<GenericTrieNodePtr, GenericValueHandle>,
    ) -> UpdatedNodeId;

    /// Gets a node reference in the set of updated nodes.
    fn get_node_ref(
        &self,
        node_id: UpdatedNodeId,
    ) -> &GenericUpdatedTrieNodeWithSize<GenericTrieNodePtr, GenericValueHandle>;

    /// Stores a state value in the trie.
    fn store_value(&mut self, value: GenericTrieValue) -> GenericValueHandle;

    /// Deletes a state value from the trie.
    fn delete_value(&mut self, value: GenericValueHandle) -> Result<(), StorageError>;
}

/// This is the interface used by TrieIterator to get nodes and values from the storage.
/// It is used to abstract the storage of trie nodes and values.
pub trait GenericTrieInternalStorage<GenericTrieNodePtr, GenericValueHandle> {
    // Get the root node of the trie.
    // Optionally return None if the trie is empty.
    fn get_root(&self) -> Option<GenericTrieNodePtr>;

    // Get a node from the storage.
    fn get_and_record_node(
        &self,
        ptr: GenericTrieNodePtr,
    ) -> Result<GenericTrieNode<GenericTrieNodePtr, GenericValueHandle>, StorageError>;

    // Get a value from the storage.
    fn get_and_record_value(&self, value_ref: GenericValueHandle) -> Result<Vec<u8>, StorageError>;
}

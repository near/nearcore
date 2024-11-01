use near_primitives::errors::StorageError;
use near_primitives::state::FlatStateValue;

use crate::trie::{ValueHandle, TRIE_COSTS};

/// For updated nodes, the ID is simply the index into the array of updated nodes we keep.
pub type GenericUpdatedNodeId = usize;

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

/// An old node means a node in the current in-memory trie. An updated node means a
/// node we're going to store in the in-memory trie but have not constructed there yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenericNodeOrIndex<GenericTrieNodePtr> {
    Old(GenericTrieNodePtr),
    Updated(GenericUpdatedNodeId),
}

/// An updated node - a node that will eventually become a trie node.
/// It references children that are either old or updated nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenericUpdatedTrieNode<GenericTrieNodePtr, GenericValueHandle> {
    /// Used for either an empty root node (indicating an empty trie), or as a temporary
    /// node to ease implementation.
    Empty,
    Leaf {
        extension: Box<[u8]>,
        value: GenericValueHandle,
    },
    Extension {
        extension: Box<[u8]>,
        child: GenericNodeOrIndex<GenericTrieNodePtr>,
    },
    /// Corresponds to either a Branch or BranchWithValue node.
    Branch {
        children: Box<[Option<GenericNodeOrIndex<GenericTrieNodePtr>>; 16]>,
        value: Option<GenericValueHandle>,
    },
}

impl<GenericTrieNodePtr, GenericValueHandle>
    GenericUpdatedTrieNode<GenericTrieNodePtr, GenericValueHandle>
where
    GenericValueHandle: HasValueLength,
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

/// Trait for trie updates to handle updated nodes.
///
/// So far, this is used to handle key-value insertions, deletions and range
/// retain operation. To be performant, such logic requires keeping track of
/// intermediate updated nodes, together with subtree function (memory usage).
///
/// `GenericTrieUpdate` abstracts the storage of updated nodes for the original
/// node type `GenericTrieNodePtr`.
///
/// In this storage, nodes are indexed by `GenericUpdatedNodeId`.
/// Node is stored as `GenericUpdatedTrieNodeWithSize`, which stores children
/// as `GenericNodeOrIndex`. Each child may be either an old node or an updated
/// node.
///
/// The flow of interaction with this storage is:
/// - In the beginning, call `ensure_updated` for the
/// `GenericNodeOrIndex::Old(root_node)` which returns `GenericUpdatedNodeId`,
/// it should be zero.
/// - For every update (single insert, single delete, recursive range
/// operation...), call corresponding method with `GenericUpdatedNodeId` for
/// the root.
/// - Then, we hold the invariant that on every descent we have
/// `GenericUpdatedNodeId`.
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
    ) -> Result<GenericUpdatedNodeId, StorageError>;

    /// Takes a node from the set of updated nodes, setting it to None.
    /// It is expected that place_node is then called to return the node to
    /// the same slot.
    fn take_node(
        &mut self,
        node_id: GenericUpdatedNodeId,
    ) -> GenericUpdatedTrieNodeWithSize<GenericTrieNodePtr, GenericValueHandle>;

    /// Puts a node to the set of updated nodes at specific index.
    /// Needed when reference to node from parent needs to be preserved.
    fn place_node_at(
        &mut self,
        node_id: GenericUpdatedNodeId,
        node: GenericUpdatedTrieNodeWithSize<GenericTrieNodePtr, GenericValueHandle>,
    );

    /// Puts a new node into the set of updated nodes.
    fn place_node(
        &mut self,
        node: GenericUpdatedTrieNodeWithSize<GenericTrieNodePtr, GenericValueHandle>,
    ) -> GenericUpdatedNodeId;

    /// Gets a node reference in the set of updated nodes.
    fn get_node_ref(
        &self,
        node_id: GenericUpdatedNodeId,
    ) -> &GenericUpdatedTrieNodeWithSize<GenericTrieNodePtr, GenericValueHandle>;

    /// Stores a state value in the trie.
    fn store_value(&mut self, value: GenericTrieValue) -> GenericValueHandle;

    /// Deletes a state value from the trie.
    fn delete_value(&mut self, value: GenericValueHandle) -> Result<(), StorageError>;
}

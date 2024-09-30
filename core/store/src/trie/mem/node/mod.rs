use super::arena::{ArenaMemory, ArenaMut, ArenaPos, ArenaPtr};
use super::flexible_data::children::ChildrenView;
use super::flexible_data::value::ValueView;
use crate::trie::{Children, TRIE_COSTS};
use crate::{RawTrieNode, RawTrieNodeWithSize};
use derive_where::derive_where;
use near_primitives::hash::CryptoHash;
use near_primitives::state::FlatStateValue;
use std::fmt::{Debug, Formatter};

mod encoding;
#[cfg(test)]
mod tests;
mod view;

/// The memory position of an encoded in-memory trie node.
/// With an `ArenaMemory`, this can be turned into a `MemTrieNodePtr`
/// to access to the node's contents.
///
/// In-memory trie nodes are completely stored in an `Arena`. Allocation and
/// deallocation of these nodes are done on the arena. Nodes allow multiple
/// references in the case of multiple state roots (trie roots), and are
/// internally refcounted. See `MemTries` for more details on the lifecycle
/// of trie nodes.
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct MemTrieNodeId {
    pub(crate) pos: ArenaPos,
}

impl MemTrieNodeId {
    pub fn new(arena: &mut impl ArenaMut, input: InputMemTrieNode) -> Self {
        Self::new_impl(arena, input, None)
    }

    pub fn new_with_hash(
        arena: &mut impl ArenaMut,
        input: InputMemTrieNode,
        hash: CryptoHash,
    ) -> Self {
        Self::new_impl(arena, input, Some(hash))
    }

    pub fn as_ptr<'a, M: ArenaMemory>(&self, arena: &'a M) -> MemTrieNodePtr<'a, M> {
        MemTrieNodePtr { ptr: arena.ptr(self.pos) }
    }
}

/// This is for internal use only, so that we can put `MemTrieNodeId` in an
/// SmallVec.
impl Default for MemTrieNodeId {
    fn default() -> Self {
        Self { pos: ArenaPos::invalid() }
    }
}

/// Pointer to an in-memory trie node that allows read-only access to the node
/// and all its descendants.
#[derive_where(Clone, Copy, PartialEq, Eq)]
pub struct MemTrieNodePtr<'a, M: ArenaMemory> {
    ptr: ArenaPtr<'a, M>,
}

impl<'a, M: ArenaMemory> Debug for MemTrieNodePtr<'a, M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.id().fmt(f)
    }
}

impl<'a, M: ArenaMemory> MemTrieNodePtr<'a, M> {
    pub fn from(ptr: ArenaPtr<'a, M>) -> Self {
        Self { ptr }
    }

    pub fn view(&self) -> MemTrieNodeView<'a, M> {
        self.view_kind(self.get_kind())
    }

    pub fn id(&self) -> MemTrieNodeId {
        MemTrieNodeId { pos: self.ptr.raw_pos() }
    }
}

/// Used to construct a new in-memory trie node.
#[derive(PartialEq, Eq, Debug, Clone)]
pub enum InputMemTrieNode<'a> {
    Leaf { value: &'a FlatStateValue, extension: &'a [u8] },
    Extension { extension: &'a [u8], child: MemTrieNodeId },
    Branch { children: [Option<MemTrieNodeId>; 16] },
    BranchWithValue { children: [Option<MemTrieNodeId>; 16], value: &'a FlatStateValue },
}

/// A view of the encoded data of `MemTrieNode`, obtainable via
/// `MemTrieNode::view()`.
#[derive_where(Debug, Clone)]
pub enum MemTrieNodeView<'a, M: ArenaMemory> {
    Leaf {
        extension: &'a [u8],
        value: ValueView<'a>,
    },
    Extension {
        hash: CryptoHash,
        memory_usage: u64,
        extension: &'a [u8],
        child: MemTrieNodePtr<'a, M>,
    },
    Branch {
        hash: CryptoHash,
        memory_usage: u64,
        children: ChildrenView<'a, M>,
    },
    BranchWithValue {
        hash: CryptoHash,
        memory_usage: u64,
        children: ChildrenView<'a, M>,
        value: ValueView<'a>,
    },
}

impl<'a> InputMemTrieNode<'a> {
    /// Converts the input node into a `RawTrieNodeWithSize`; this is used to initialize
    /// memory usage and to calculate hash when constructing the memtrie node.
    ///
    /// This must not be called if the node is a leaf.
    pub fn to_raw_trie_node_with_size_non_leaf<Memory: ArenaMemory>(
        &self,
        arena: &Memory,
    ) -> RawTrieNodeWithSize {
        match self {
            Self::Leaf { .. } => {
                unreachable!("Leaf nodes do not need hash computation")
            }
            Self::Extension { extension, child, .. } => {
                let view = child.as_ptr(arena).view();
                let memory_usage = TRIE_COSTS.node_cost
                    + extension.len() as u64 * TRIE_COSTS.byte_of_key
                    + view.memory_usage();
                let node = RawTrieNode::Extension(extension.to_vec(), view.node_hash());
                RawTrieNodeWithSize { node, memory_usage }
            }
            Self::Branch { children, .. } => {
                let mut memory_usage = TRIE_COSTS.node_cost;
                let mut hashes = [None; 16];
                for (i, child) in children.iter().enumerate() {
                    if let Some(child) = child {
                        let view = child.as_ptr(arena).view();
                        hashes[i] = Some(view.node_hash());
                        memory_usage += view.memory_usage();
                    }
                }
                let node = RawTrieNode::BranchNoValue(Children(hashes));
                RawTrieNodeWithSize { node, memory_usage }
            }
            Self::BranchWithValue { children, value, .. } => {
                let value_len = match value {
                    FlatStateValue::Ref(value_ref) => value_ref.len(),
                    FlatStateValue::Inlined(value) => value.len(),
                };
                let mut memory_usage = TRIE_COSTS.node_cost
                    + value_len as u64 * TRIE_COSTS.byte_of_value
                    + TRIE_COSTS.node_cost;
                let mut hashes = [None; 16];
                for (i, child) in children.iter().enumerate() {
                    if let Some(child) = child {
                        let view = child.as_ptr(arena).view();
                        hashes[i] = Some(view.node_hash());
                        memory_usage += view.memory_usage();
                    }
                }
                let node = RawTrieNode::BranchWithValue(value.to_value_ref(), Children(hashes));
                RawTrieNodeWithSize { node, memory_usage }
            }
        }
    }
}

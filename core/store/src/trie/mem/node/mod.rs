mod encoding;
mod mutation;
#[cfg(test)]
mod tests;
mod view;

use super::arena::{Arena, ArenaMemory, ArenaPos, ArenaPtr, ArenaPtrMut, ArenaSlice};
use super::flexible_data::children::ChildrenView;
use super::flexible_data::value::ValueView;
use near_primitives::hash::CryptoHash;
use near_primitives::state::FlatStateValue;
use std::fmt::{Debug, Formatter};

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
    pub fn new(arena: &mut Arena, input: InputMemTrieNode) -> Self {
        Self::new_impl(arena, input, None)
    }

    pub fn new_with_hash(arena: &mut Arena, input: InputMemTrieNode, hash: CryptoHash) -> Self {
        Self::new_impl(arena, input, Some(hash))
    }

    pub fn as_ptr<'a>(&self, arena: &'a ArenaMemory) -> MemTrieNodePtr<'a> {
        MemTrieNodePtr { ptr: arena.ptr(self.pos) }
    }

    pub(crate) fn as_ptr_mut<'a>(&self, arena: &'a mut ArenaMemory) -> MemTrieNodePtrMut<'a> {
        MemTrieNodePtrMut { ptr: arena.ptr_mut(self.pos) }
    }
}

/// This is for internal use only, so that we can put `MemTrieNodeId` in an
/// ElasticArray.
impl Default for MemTrieNodeId {
    fn default() -> Self {
        Self { pos: ArenaPos::invalid() }
    }
}

/// Pointer to an in-memory trie node that allows read-only access to the node
/// and all its descendants.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct MemTrieNodePtr<'a> {
    ptr: ArenaPtr<'a>,
}

/// Pointer to an in-memory trie node that allows mutable access to the node
/// and all its descendants. This is only for computing hashes, and internal
/// reference counting.
pub struct MemTrieNodePtrMut<'a> {
    ptr: ArenaPtrMut<'a>,
}

impl<'a> Debug for MemTrieNodePtr<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.id().fmt(f)
    }
}

impl<'a> MemTrieNodePtr<'a> {
    pub fn from(ptr: ArenaPtr<'a>) -> Self {
        Self { ptr }
    }

    pub fn view(&self) -> MemTrieNodeView<'a> {
        self.view_impl()
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
#[derive(Debug, Clone)]
pub enum MemTrieNodeView<'a> {
    Leaf {
        extension: ArenaSlice<'a>,
        value: ValueView<'a>,
    },
    Extension {
        hash: CryptoHash,
        memory_usage: u64,
        extension: ArenaSlice<'a>,
        child: MemTrieNodePtr<'a>,
    },
    Branch {
        hash: CryptoHash,
        memory_usage: u64,
        children: ChildrenView<'a>,
    },
    BranchWithValue {
        hash: CryptoHash,
        memory_usage: u64,
        children: ChildrenView<'a>,
        value: ValueView<'a>,
    },
}

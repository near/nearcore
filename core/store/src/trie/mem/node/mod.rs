#![allow(dead_code)] // still being implemented

mod encoding;
mod loading;
#[cfg(test)]
mod tests;
mod view;

use near_primitives::hash::CryptoHash;
use near_primitives::state::FlatStateValue;

use super::flexible_data::children::ChildrenView;
use super::flexible_data::value::ValueView;

/// An efficiently encoded in-memory trie node.
///
/// This struct is internally refcounted, similar to an Rc<T>. When all clones
/// of the same `MemTrieNode` are dropped, the associated memory allocation is
/// freed.
///
/// To construct a `MemTrieNode`, call `MemTrieNode::new`. To read its contents,
/// call `MemTrieNode::view()`, which returns a `MemTrieNodeView`.
#[derive(Debug, Hash, PartialEq, Eq)]
#[repr(C, packed(1))]
pub struct MemTrieNode {
    data: *const u8,
}

impl MemTrieNode {
    pub fn new(input: InputMemTrieNode) -> Self {
        unsafe { Self::new_impl(input) }
    }

    pub fn view(&self) -> MemTrieNodeView<'_> {
        unsafe { self.view_impl() }
    }

    pub fn hash(&self) -> CryptoHash {
        self.view().node_hash()
    }

    pub fn memory_usage(&self) -> u64 {
        self.view().memory_usage()
    }
}

impl Clone for MemTrieNode {
    fn clone(&self) -> Self {
        unsafe { self.clone_impl() }
    }
}

impl Drop for MemTrieNode {
    fn drop(&mut self) {
        unsafe { self.drop_impl() }
    }
}

/// Used to construct a new `MemTrieNode`.
#[derive(PartialEq, Eq, Debug, Clone)]
pub enum InputMemTrieNode {
    Leaf { value: FlatStateValue, extension: Box<[u8]> },
    Extension { extension: Box<[u8]>, child: MemTrieNode },
    Branch { children: Vec<Option<MemTrieNode>> },
    BranchWithValue { children: Vec<Option<MemTrieNode>>, value: FlatStateValue },
}

/// A view of the encoded data of `MemTrieNode`, obtainable via
/// `MemTrieNode::view()`.
#[derive(Debug, Clone)]
pub enum MemTrieNodeView<'a> {
    Leaf {
        extension: &'a [u8],
        value: ValueView<'a>,
    },
    Extension {
        hash: &'a CryptoHash,
        memory_usage: u64,
        extension: &'a [u8],
        child: &'a MemTrieNode,
    },
    Branch {
        hash: &'a CryptoHash,
        memory_usage: u64,
        children: ChildrenView<'a>,
    },
    BranchWithValue {
        hash: &'a CryptoHash,
        memory_usage: u64,
        children: ChildrenView<'a>,
        value: ValueView<'a>,
    },
}

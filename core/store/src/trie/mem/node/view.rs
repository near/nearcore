use super::{MemTrieNodePtr, MemTrieNodeView};
use crate::trie::mem::arena::ArenaMemory;
use crate::trie::TRIE_COSTS;
use crate::{RawTrieNode, RawTrieNodeWithSize};
use near_primitives::hash::{hash, CryptoHash};

impl<'a, M: ArenaMemory> MemTrieNodeView<'a, M> {
    /// Returns the node's hash. Requires that the hash is already computed.
    pub fn node_hash(&self) -> CryptoHash {
        match self {
            Self::Leaf { .. } => {
                let node = self.clone().to_raw_trie_node_with_size();
                hash(&borsh::to_vec(&node).unwrap())
            }
            Self::Extension { hash, .. }
            | Self::Branch { hash, .. }
            | Self::BranchWithValue { hash, .. } => {
                debug_assert_ne!(hash, &CryptoHash::default(), "Hash not computed");
                *hash
            }
        }
    }

    /// Returns the memory usage of the node, in Near's trie cost terms, not
    /// in terms of the physical memory usage.
    pub fn memory_usage(&self) -> u64 {
        match self {
            Self::Leaf { value, extension } => {
                TRIE_COSTS.node_cost
                    + extension.len() as u64 * TRIE_COSTS.byte_of_key
                    + value.len() as u64 * TRIE_COSTS.byte_of_value
                    + TRIE_COSTS.node_cost // yes, node_cost twice.
            }
            Self::Extension { memory_usage, .. }
            | Self::Branch { memory_usage, .. }
            | Self::BranchWithValue { memory_usage, .. } => *memory_usage,
        }
    }

    /// Converts this view into a `RawTrieNodeWithSize`, which is used to
    /// compute hashes and for serialization to disk.
    pub fn to_raw_trie_node_with_size(&self) -> RawTrieNodeWithSize {
        match self {
            Self::Leaf { value, extension } => {
                let node = RawTrieNode::Leaf(
                    extension.to_vec(),
                    value.clone().to_flat_value().to_value_ref(),
                );
                RawTrieNodeWithSize { node, memory_usage: self.memory_usage() }
            }
            Self::Extension { extension, child, .. } => {
                let view = child.view();
                let node = RawTrieNode::Extension(extension.to_vec(), view.node_hash());
                RawTrieNodeWithSize { node, memory_usage: self.memory_usage() }
            }
            Self::Branch { children, .. } => {
                let node = RawTrieNode::BranchNoValue(children.to_children());
                RawTrieNodeWithSize { node, memory_usage: self.memory_usage() }
            }
            Self::BranchWithValue { children, value, .. } => {
                let node = RawTrieNode::BranchWithValue(
                    value.to_flat_value().to_value_ref(),
                    children.to_children(),
                );
                RawTrieNodeWithSize { node, memory_usage: self.memory_usage() }
            }
        }
    }

    pub(crate) fn iter_children<'b>(
        &'b self,
    ) -> Box<dyn Iterator<Item = MemTrieNodePtr<'a, M>> + 'b> {
        match self {
            Self::Leaf { .. } => Box::new(std::iter::empty()),
            Self::Extension { child, .. } => Box::new(std::iter::once(*child)),
            Self::Branch { children, .. } | Self::BranchWithValue { children, .. } => {
                Box::new(children.iter())
            }
        }
    }
}

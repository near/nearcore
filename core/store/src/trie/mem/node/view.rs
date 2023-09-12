use super::{MemTrieNode, MemTrieNodeView};
use crate::trie::TRIE_COSTS;
use crate::{RawTrieNode, RawTrieNodeWithSize};
use borsh::BorshSerialize;
use near_primitives::hash::{hash, CryptoHash};

impl<'a> MemTrieNodeView<'a> {
    pub fn node_hash(self) -> CryptoHash {
        match self {
            Self::Leaf { .. } => {
                let node = self.to_raw_trie_node_with_size();
                hash(&node.try_to_vec().unwrap())
            }
            Self::Extension { hash, .. }
            | Self::Branch { hash, .. }
            | Self::BranchWithValue { hash, .. } => *hash,
        }
    }

    pub fn to_raw_trie_node_with_size(self) -> RawTrieNodeWithSize {
        match self {
            Self::Leaf { value, extension } => {
                let node = RawTrieNode::Leaf(
                    extension.to_vec(),
                    value.clone().to_flat_value().to_value_ref(),
                );
                let memory_usage = Self::Leaf { value, extension }.memory_usage();
                RawTrieNodeWithSize { node, memory_usage }
            }
            Self::Extension { extension, child, .. } => {
                let node = RawTrieNode::Extension(extension.to_vec(), child.hash());
                let memory_usage = TRIE_COSTS.node_cost
                    + child.memory_usage()
                    + extension.len() as u64 * TRIE_COSTS.byte_of_key;
                RawTrieNodeWithSize { node, memory_usage }
            }
            Self::Branch { children, .. } => {
                let node = RawTrieNode::BranchNoValue(children.to_children());
                let mut memory_usage = TRIE_COSTS.node_cost;
                for child in children.iter() {
                    memory_usage += child.memory_usage();
                }
                RawTrieNodeWithSize { node, memory_usage }
            }
            Self::BranchWithValue { children, value, .. } => {
                let mut memory_usage = TRIE_COSTS.node_cost
                    + value.len() as u64 * TRIE_COSTS.byte_of_value
                    + TRIE_COSTS.node_cost;
                let node = RawTrieNode::BranchWithValue(
                    value.to_flat_value().to_value_ref(),
                    children.to_children(),
                );
                for child in children.iter() {
                    memory_usage += child.memory_usage();
                }
                RawTrieNodeWithSize { node, memory_usage }
            }
        }
    }

    pub fn memory_usage(&self) -> u64 {
        match self {
            Self::Leaf { value, extension } => {
                TRIE_COSTS.node_cost
                    + extension.len() as u64 * TRIE_COSTS.byte_of_key
                    + value.len() as u64 * TRIE_COSTS.byte_of_value
                    + TRIE_COSTS.node_cost // yes, twice.
            }
            Self::Extension { memory_usage, .. }
            | Self::Branch { memory_usage, .. }
            | Self::BranchWithValue { memory_usage, .. } => {
                // Memory usage is computed after loading is complete.
                // For that, we use the to_raw_trie_node_with_size code path.
                // So make sure that's the case by checking here.
                assert!(*memory_usage != 0, "memory_usage is not computed yet");
                *memory_usage
            }
        }
    }

    pub(crate) fn iter_children(&'a self) -> Box<dyn Iterator<Item = &'a MemTrieNode> + 'a> {
        match self {
            MemTrieNodeView::Leaf { .. } => Box::new(std::iter::empty()),
            MemTrieNodeView::Extension { child, .. } => Box::new(std::iter::once(*child)),
            MemTrieNodeView::Branch { children, .. }
            | MemTrieNodeView::BranchWithValue { children, .. } => Box::new(children.iter()),
        }
    }
}

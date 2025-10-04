use super::arena::Arena;
use super::memtrie_update::MemTrieNodeWithSize;
use super::memtries::MemTries;
use super::node::MemTrieNodeId;
use crate::Trie;
use crate::trie::ops::interface::GenericTrieInternalStorage;
use crate::trie::ops::iter::TrieIteratorImpl;
use crate::trie::{AccessOptions, OptimizedValueRef};
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::state::FlatStateValue;

/// Tiny wrapper around `MemTries` and `Trie` to provide `GenericTrieInternalStorage` implementation.
pub struct MemTrieIteratorInner<'a> {
    memtrie: &'a MemTries,
    trie: &'a Trie,
}

impl<'a> MemTrieIteratorInner<'a> {
    pub fn new(memtrie: &'a MemTries, trie: &'a Trie) -> Self {
        Self { memtrie, trie }
    }
}

impl<'a> GenericTrieInternalStorage<MemTrieNodeId, FlatStateValue> for MemTrieIteratorInner<'a> {
    fn get_root(&self) -> Option<MemTrieNodeId> {
        let root_hash = self.trie.root;
        if root_hash == CryptoHash::default() {
            return None;
        }
        let root_node = self.memtrie.get_root(&root_hash).unwrap();
        let root_ptr = root_node.id();
        Some(root_ptr)
    }

    fn get_node_with_size(
        &self,
        node: MemTrieNodeId,
        opts: AccessOptions,
    ) -> Result<MemTrieNodeWithSize, StorageError> {
        let view = node.as_ptr(self.memtrie.arena.memory()).view();
        if opts.enable_state_witness_recording {
            if let Some(recorder) = &self.trie.recorder {
                recorder.record_memtrie_node(&view);
            }
        }
        let node = MemTrieNodeWithSize::from_existing_node_view(view);
        Ok(node)
    }

    fn get_value(
        &self,
        value_ref: FlatStateValue,
        opts: AccessOptions,
    ) -> Result<Vec<u8>, StorageError> {
        let optimized_value_ref = OptimizedValueRef::from_flat_value(value_ref);
        let value = self.trie.deref_optimized(opts, &optimized_value_ref)?;
        Ok(value)
    }
}

pub type STMemTrieIterator<'a> =
    TrieIteratorImpl<MemTrieNodeId, FlatStateValue, MemTrieIteratorInner<'a>>;

use super::encoding::{CommonHeader, NodeKind, NonLeafHeader};
use super::MemTrieNode;
use borsh::BorshSerialize;
use near_primitives::hash::hash;

impl MemTrieNode {
    unsafe fn compute_hash_and_memory_usage(&self) {
        let raw_trie_node_with_size = self.view().to_raw_trie_node_with_size();
        let mut decoder = self.decoder();
        match decoder.decode::<CommonHeader>().kind {
            NodeKind::Leaf => {}
            _ => {
                let nonleaf = decoder.decode_as_mut::<NonLeafHeader>();
                nonleaf.memory_usage = raw_trie_node_with_size.memory_usage;
                nonleaf.hash = hash(&raw_trie_node_with_size.try_to_vec().unwrap());
            }
        }
    }

    unsafe fn is_hash_and_memory_usage_computed(&self) -> bool {
        let mut decoder = self.decoder();
        match decoder.decode::<CommonHeader>().kind {
            NodeKind::Leaf => true,
            _ => decoder.decode::<NonLeafHeader>().memory_usage != 0,
        }
    }

    /// This is used after initially constructing the in-memory trie strcuture,
    /// to compute the hash and memory usage recursively. The computation is
    /// expensive, so we defer it in order to make it parallelizable.
    pub(crate) fn compute_hash_and_memory_usage_recursively(&self) {
        unsafe {
            if self.is_hash_and_memory_usage_computed() {
                return;
            }
            for child in self.view().iter_children() {
                child.compute_hash_and_memory_usage_recursively();
            }
            self.compute_hash_and_memory_usage();
        }
    }
}

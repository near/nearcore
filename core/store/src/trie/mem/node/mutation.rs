use crate::trie::mem::arena::ArenaMemory;
use crate::trie::mem::flexible_data::encoding::RawDecoderMut;

use super::encoding::{CommonHeader, NodeKind, NonLeafHeader};
use super::{MemTrieNodePtr, MemTrieNodePtrMut};

use near_primitives::hash::{hash, CryptoHash};

impl<'a> MemTrieNodePtrMut<'a> {
    fn as_const<'b>(&'b self) -> MemTrieNodePtr<'b> {
        MemTrieNodePtr { ptr: self.ptr.ptr() }
    }

    pub(crate) fn decoder_mut<'b>(&'b mut self) -> RawDecoderMut<'b> {
        RawDecoderMut::new(self.ptr.ptr_mut())
    }

    /// Obtains a list of mutable references to the children of this node,
    /// destroying this mutable reference.
    ///
    /// Despite being implemented with unsafe code, this is a safe operation
    /// because the children subtrees are disjoint (even if there are multiple
    /// roots). It is very similar to `split_at_mut` on mutable slices.
    fn split_children_mut(mut self) -> Vec<MemTrieNodePtrMut<'a>> {
        let arena_mut = self.ptr.arena_mut() as *mut ArenaMemory;
        let mut result = Vec::new();
        let view = self.as_const().view();
        for child in view.iter_children() {
            let child_id = child.id();
            let arena_mut_ref = unsafe { &mut *arena_mut };
            result.push(child_id.as_ptr_mut(arena_mut_ref));
        }
        result
    }

    /// Like `split_children_mut`, but does not destroy the reference itself.
    /// This is possible because of the returned references can only be used
    /// while this reference is being mutably held, but it does result in a
    /// different lifetime.
    fn children_mut<'b>(&'b mut self) -> Vec<MemTrieNodePtrMut<'b>> {
        let arena_mut = self.ptr.arena_mut() as *mut ArenaMemory;
        let mut result = Vec::new();
        let view = self.as_const().view();
        for child in view.iter_children() {
            let child_id = child.id();
            let arena_mut_ref = unsafe { &mut *arena_mut };
            result.push(child_id.as_ptr_mut(arena_mut_ref));
        }
        result
    }

    /// Computes the hash for this node, assuming children nodes already have
    /// computed hashes.
    fn compute_hash(&mut self) {
        let raw_trie_node_with_size = self.as_const().view().to_raw_trie_node_with_size();
        let mut decoder = self.decoder_mut();
        match decoder.decode::<CommonHeader>().kind {
            NodeKind::Leaf => {}
            _ => {
                let mut nonleaf = decoder.peek::<NonLeafHeader>();
                nonleaf.hash = hash(&borsh::to_vec(&raw_trie_node_with_size).unwrap());
                decoder.overwrite(nonleaf);
            }
        }
    }

    /// Whether the hash is computed for this node.
    fn is_hash_computed(&self) -> bool {
        let mut decoder = self.as_const().decoder();
        match decoder.decode::<CommonHeader>().kind {
            NodeKind::Leaf => true,
            _ => decoder.peek::<NonLeafHeader>().hash != CryptoHash::default(),
        }
    }

    /// Computes the hashes of this subtree recursively, stopping at any nodes
    /// whose hashes are already computed.
    pub(crate) fn compute_hash_recursively(&mut self) {
        if self.is_hash_computed() {
            return;
        }
        for mut child in self.children_mut() {
            child.compute_hash_recursively();
        }
        self.compute_hash();
    }

    /// Recursively expand the current subtree until we arrive at subtrees
    /// that are small enough (by memory usage); we store these subtrees in
    /// the provided vector. The returned subtrees cover all leaves but are
    /// disjoint.
    pub(crate) fn take_small_subtrees(
        self,
        threshold_memory_usage: u64,
        trees: &mut Vec<MemTrieNodePtrMut<'a>>,
    ) {
        if self.as_const().view().memory_usage() < threshold_memory_usage {
            trees.push(self);
        } else {
            for child in self.split_children_mut() {
                child.take_small_subtrees(threshold_memory_usage, trees);
            }
        }
    }

    pub(crate) fn get_small_children(
        self,
        threshold_memory_usage: u64,
    ) -> Vec<MemTrieNodePtrMut<'a>> {
        let mut children = Vec::<MemTrieNodePtrMut>::new();
        if self.as_const().view().memory_usage() > threshold_memory_usage {
            for child in self.split_children_mut() {
                if child.as_const().view().memory_usage() < threshold_memory_usage {
                    children.push(child);
                }
            }
        }
        children
    }
}

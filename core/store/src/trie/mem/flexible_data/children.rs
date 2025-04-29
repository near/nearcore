use super::FlexibleDataHeader;
use super::encoding::BorshFixedSize;
use crate::trie::Children;
use crate::trie::mem::arena::{ArenaMemory, ArenaMemoryMut, ArenaSlice, ArenaSliceMut};
use crate::trie::mem::node::{MemTrieNodeId, MemTrieNodePtr};
use borsh::{BorshDeserialize, BorshSerialize};
use derive_where::derive_where;
use std::mem::size_of;

/// Flexibly-sized data header for a variable-sized list of children trie nodes.
/// The header contains a 16-bit mask of which children are present, and the
/// flexible part is one pointer for each present child.
#[derive(Clone, Copy, BorshSerialize, BorshDeserialize)]
pub struct EncodedChildrenHeader {
    mask: u16,
}

impl BorshFixedSize for EncodedChildrenHeader {
    const SERIALIZED_SIZE: usize = size_of::<u16>();
}

impl FlexibleDataHeader for EncodedChildrenHeader {
    type InputData = [Option<MemTrieNodeId>; 16];
    type View<'a, M: ArenaMemory> = ChildrenView<'a, M>;

    fn from_input(children: &[Option<MemTrieNodeId>; 16]) -> EncodedChildrenHeader {
        let mut mask = 0u16;
        for i in 0..16 {
            if children[i].is_some() {
                mask |= 1 << i;
            }
        }
        EncodedChildrenHeader { mask }
    }

    fn flexible_data_length(&self) -> usize {
        self.mask.count_ones() as usize * size_of::<usize>()
    }

    fn encode_flexible_data<M: ArenaMemoryMut>(
        &self,
        children: &[Option<MemTrieNodeId>; 16],
        target: &mut ArenaSliceMut<M>,
    ) {
        let mut j = 0;
        for (i, child) in children.iter().enumerate() {
            if self.mask & (1 << i) != 0 {
                target.write_pos_at(j, child.unwrap().pos);
                j += size_of::<usize>();
            } else {
                debug_assert!(child.is_none());
            }
        }
    }

    fn decode_flexible_data<'a, M: ArenaMemory>(
        &self,
        source: &ArenaSlice<'a, M>,
    ) -> ChildrenView<'a, M> {
        ChildrenView { mask: self.mask, children: source.clone() }
    }
}

/// Efficient view of the encoded children data.
#[derive_where(Debug, Clone)]
pub struct ChildrenView<'a, M: ArenaMemory> {
    mask: u16,
    children: ArenaSlice<'a, M>,
}

impl<'a, M: ArenaMemory> ChildrenView<'a, M> {
    /// Gets the child at a specific index (0 to 15).
    pub fn get(&self, i: usize) -> Option<MemTrieNodePtr<'a, M>> {
        assert!(i < 16);
        let bit = 1u16 << (i as u16);
        if self.mask & bit == 0 {
            None
        } else {
            let lower_mask = self.mask & (bit - 1);
            let index = lower_mask.count_ones() as usize;
            Some(MemTrieNodePtr::from(self.children.read_ptr_at(index * size_of::<usize>())))
        }
    }

    /// Converts to a Children struct used in RawTrieNode.
    pub fn to_children(&self) -> Children {
        let mut nodes = [None; 16];
        if self.mask == 0 {
            return Children(nodes);
        };

        // cspell:words ptrs
        let mut node_ptrs = [None; 16];
        let mut j = size_of::<usize>() * self.mask.count_ones() as usize;
        // Execute all `read_ptr_at` in reverse to avoid repeat bound checks.
        // Additionally, issue reads for the node kinds before moving on to compute sha256 hashes,
        // thus hopefully giving CPU more time to load the relevant lines into the cache.
        for i in (0..16).rev() {
            let bit = self.mask & (1 << i);
            if bit != 0 {
                j -= size_of::<usize>();
                let ptr = MemTrieNodePtr::from(self.children.read_ptr_at(j));
                let kind = ptr.get_kind();
                node_ptrs[i] = Some((ptr, kind));
            }
        }

        for (node, node_ptr) in std::iter::zip(nodes.iter_mut().rev(), node_ptrs.into_iter().rev())
        {
            if let Some((node_ptr, kind)) = node_ptr {
                let node_view = node_ptr.view_kind(kind);
                *node = Some(node_view.node_hash());
            }
        }

        Children(nodes)
    }

    /// Iterates only through existing children.
    pub fn iter<'b>(&'b self) -> impl Iterator<Item = MemTrieNodePtr<'a, M>> + 'b {
        (0..self.mask.count_ones() as usize)
            .map(|i| MemTrieNodePtr::from(self.children.read_ptr_at(i * size_of::<usize>())))
    }
}

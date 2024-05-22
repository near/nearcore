use std::mem::size_of;

use borsh::{BorshDeserialize, BorshSerialize};

use super::encoding::BorshFixedSize;
use super::FlexibleDataHeader;
use crate::trie::mem::arena::{ArenaSlice, ArenaSliceMut};
use crate::trie::mem::node::{MemTrieNodeId, MemTrieNodePtr};
use crate::trie::Children;

/// Flexibly-sized data header for a variable-sized list of children trie nodes.
/// The header contains a 16-bit mask of which children are present, and the
/// flexible part is one pointer for each present child.
#[derive(Clone, Copy, BorshSerialize, BorshDeserialize)]
pub struct EncodedChildrenHeader {
    mask: u16,
}

impl BorshFixedSize for EncodedChildrenHeader {
    const SERIALIZED_SIZE: usize = std::mem::size_of::<u16>();
}

impl FlexibleDataHeader for EncodedChildrenHeader {
    type InputData = [Option<MemTrieNodeId>; 16];
    type View<'a> = ChildrenView<'a>;

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

    fn encode_flexible_data(
        &self,
        children: &[Option<MemTrieNodeId>; 16],
        target: &mut ArenaSliceMut<'_>,
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

    fn decode_flexible_data<'a>(&self, source: &ArenaSlice<'a>) -> ChildrenView<'a> {
        ChildrenView { mask: self.mask, children: source.clone() }
    }
}

/// Efficient view of the encoded children data.
#[derive(Debug, Clone)]
pub struct ChildrenView<'a> {
    mask: u16,
    children: ArenaSlice<'a>,
}

impl<'a> ChildrenView<'a> {
    /// Gets the child at a specific index (0 to 15).
    pub fn get(&self, i: usize) -> Option<MemTrieNodePtr<'a>> {
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
        let mut children = Children::default();
        let mut j = 0;
        for i in 0..16 {
            if self.mask & (1 << i) != 0 {
                let child = MemTrieNodePtr::from(self.children.read_ptr_at(j));
                children.0[i] = Some(child.view().node_hash());
                j += size_of::<usize>();
            }
        }
        children
    }

    /// Iterates only through existing children.
    pub fn iter<'b>(&'b self) -> impl Iterator<Item = MemTrieNodePtr<'a>> + 'b {
        (0..self.mask.count_ones() as usize)
            .map(|i| MemTrieNodePtr::from(self.children.read_ptr_at(i * size_of::<usize>())))
    }
}

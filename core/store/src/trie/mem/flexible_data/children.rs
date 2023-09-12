use super::FlexibleDataHeader;
use crate::trie::mem::node::MemTrieNode;
use crate::trie::Children;
use std::mem::MaybeUninit;

/// Flexibly-sized data header for a variable-sized list of children trie nodes.
/// The header contains a 16-bit mask of which children are present, and the
/// flexible part is one pointer for each present child.
#[repr(C, packed(1))]
#[derive(Clone, Copy)]
pub struct EncodedChildrenHeader {
    mask: u16,
}

impl FlexibleDataHeader for EncodedChildrenHeader {
    type InputData = Vec<Option<MemTrieNode>>;
    type View<'a> = ChildrenView<'a>;

    fn from_input(children: &Vec<Option<MemTrieNode>>) -> EncodedChildrenHeader {
        let mut mask = 0u16;
        for i in 0..16 {
            if children[i].is_some() {
                mask |= 1 << i;
            }
        }
        EncodedChildrenHeader { mask }
    }

    fn flexible_data_length(&self) -> usize {
        self.mask.count_ones() as usize * std::mem::size_of::<MemTrieNode>()
    }

    unsafe fn encode_flexible_data(&self, children: Vec<Option<MemTrieNode>>, mut ptr: *mut u8) {
        assert_eq!(children.len(), 16);
        for (i, child) in children.into_iter().enumerate() {
            if self.mask & (1 << i) != 0 {
                // Note: we need to use MaybeUninit here, because the memory
                // we're writing into is uninitialized. If we do not use
                // MaybeUninit, the compiler would insert a drop call on the
                // uninitialized memory as a MemTrieNode, which would segfault.
                (*(ptr as *mut MaybeUninit<MemTrieNode>)).write(child.unwrap());
                ptr = ptr.offset(std::mem::size_of::<MemTrieNode>() as isize);
            }
        }
    }

    unsafe fn decode_flexible_data<'a>(&'a self, ptr: *const u8) -> ChildrenView<'a> {
        ChildrenView {
            children: std::slice::from_raw_parts(
                ptr as *const MemTrieNode,
                self.mask.count_ones() as usize,
            ),
            mask: self.mask,
        }
    }

    unsafe fn drop_flexible_data(&self, ptr: *mut u8) {
        let num_children = self.mask.count_ones() as usize;
        for i in 0..num_children {
            let child_ptr = ptr.offset((i * std::mem::size_of::<MemTrieNode>()) as isize);
            (*(child_ptr as *mut MaybeUninit<MemTrieNode>)).assume_init_drop();
        }
    }
}

/// Efficient view of the encoded children data.
#[derive(PartialEq, Eq, Debug, Clone)]
pub struct ChildrenView<'a> {
    mask: u16,
    children: &'a [MemTrieNode],
}

impl<'a> ChildrenView<'a> {
    /// Gets the child at a specific index (0 to 15).
    pub fn get(&self, i: usize) -> Option<&'a MemTrieNode> {
        assert!(i < 16);
        let bit = 1u16 << (i as u16);
        if self.mask & bit == 0 {
            None
        } else {
            let lower_mask = self.mask & (bit - 1);
            let index = lower_mask.count_ones() as usize;
            Some(&self.children[index])
        }
    }

    /// Converts to a Children struct used in RawTrieNode.
    pub fn to_children(&self) -> Children {
        let mut children = Children::default();
        let mut j = 0;
        for i in 0..16 {
            if self.mask & (1 << i) != 0 {
                children.0[i] = Some(self.children[j].hash());
                j += 1;
            }
        }
        children
    }

    pub fn iter(&self) -> impl Iterator<Item = &'a MemTrieNode> {
        self.children.iter()
    }
}

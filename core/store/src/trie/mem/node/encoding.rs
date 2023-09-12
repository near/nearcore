use near_primitives::hash::CryptoHash;

use crate::trie::mem::flexible_data::children::EncodedChildrenHeader;
use crate::trie::mem::flexible_data::encoding::{RawDecoder, RawEncoder};
use crate::trie::mem::flexible_data::extension::EncodedExtensionHeader;
use crate::trie::mem::flexible_data::value::EncodedValueHeader;
use crate::trie::mem::flexible_data::FlexibleDataHeader;

use super::{InputMemTrieNode, MemTrieNode, MemTrieNodeView};

#[repr(u8)]
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub(crate) enum NodeKind {
    Leaf,
    Extension,
    Branch,
    BranchWithValue,
}

#[repr(C, packed(1))]
pub(crate) struct CommonHeader {
    refcount: u32,
    pub(crate) kind: NodeKind,
}

#[repr(C, packed(1))]
#[derive(Default)]
pub(crate) struct NonLeafHeader {
    pub(crate) hash: CryptoHash,
    pub(crate) memory_usage: u64,
}

#[repr(C, packed(1))]
pub(crate) struct LeafHeader {
    common: CommonHeader,
    value: EncodedValueHeader,
    extension: EncodedExtensionHeader,
}

#[repr(C, packed(1))]
pub(crate) struct ExtensionHeader {
    common: CommonHeader,
    nonleaf: NonLeafHeader,
    child: MemTrieNode,
    extension: EncodedExtensionHeader,
}

#[repr(C, packed(1))]
pub(crate) struct BranchHeader {
    common: CommonHeader,
    nonleaf: NonLeafHeader,
    children: EncodedChildrenHeader,
}

#[repr(C, packed(1))]
pub(crate) struct BranchWithValueHeader {
    common: CommonHeader,
    nonleaf: NonLeafHeader,
    value: EncodedValueHeader,
    children: EncodedChildrenHeader,
}

impl MemTrieNode {
    /// Encodes the data.
    pub(crate) unsafe fn new_impl(node: InputMemTrieNode) -> Self {
        let data = match node {
            InputMemTrieNode::Leaf { value, extension } => {
                let extension_header = EncodedExtensionHeader::from_input(&extension);
                let value_header = EncodedValueHeader::from_input(&value);
                let mut data = RawEncoder::new(
                    std::mem::size_of::<LeafHeader>()
                        + extension_header.flexible_data_length()
                        + value_header.flexible_data_length(),
                );
                data.encode(LeafHeader {
                    common: CommonHeader { refcount: 1, kind: NodeKind::Leaf },
                    extension: extension_header,
                    value: value_header,
                });
                data.encode_flexible(&extension_header, extension);
                data.encode_flexible(&value_header, value);
                data.finish()
            }
            InputMemTrieNode::Extension { extension, child } => {
                let extension_header = EncodedExtensionHeader::from_input(&extension);
                let mut data = RawEncoder::new(
                    std::mem::size_of::<ExtensionHeader>()
                        + extension_header.flexible_data_length(),
                );
                data.encode(ExtensionHeader {
                    common: CommonHeader { refcount: 1, kind: NodeKind::Extension },
                    nonleaf: NonLeafHeader::default(),
                    child,
                    extension: extension_header,
                });
                data.encode_flexible(&extension_header, extension);
                data.finish()
            }
            InputMemTrieNode::Branch { children } => {
                let children_header = EncodedChildrenHeader::from_input(&children);
                let mut data = RawEncoder::new(
                    std::mem::size_of::<BranchHeader>() + children_header.flexible_data_length(),
                );
                data.encode(BranchHeader {
                    common: CommonHeader { refcount: 1, kind: NodeKind::Branch },
                    nonleaf: NonLeafHeader::default(),
                    children: children_header,
                });
                data.encode_flexible(&children_header, children);
                data.finish()
            }
            InputMemTrieNode::BranchWithValue { children, value } => {
                let children_header = EncodedChildrenHeader::from_input(&children);
                let value_header = EncodedValueHeader::from_input(&value);
                let mut data = RawEncoder::new(
                    std::mem::size_of::<BranchWithValueHeader>()
                        + children_header.flexible_data_length()
                        + value_header.flexible_data_length(),
                );
                data.encode(BranchWithValueHeader {
                    common: CommonHeader { refcount: 1, kind: NodeKind::BranchWithValue },
                    nonleaf: NonLeafHeader::default(),
                    children: children_header,
                    value: value_header,
                });
                data.encode_flexible(&children_header, children);
                data.encode_flexible(&value_header, value);
                data.finish()
            }
        };
        Self { data }
    }

    pub(crate) fn decoder<'a>(&'a self) -> RawDecoder<'a> {
        RawDecoder::new(self.data)
    }

    /// Decodes the data.
    pub(crate) unsafe fn view_impl<'a>(&'a self) -> MemTrieNodeView<'a> {
        let mut decoder = self.decoder();
        let kind = decoder.peek::<CommonHeader>().kind;
        match kind {
            NodeKind::Leaf => {
                let header = decoder.decode::<LeafHeader>();
                let extension = decoder.decode_flexible(&header.extension);
                let value = decoder.decode_flexible(&header.value);
                MemTrieNodeView::Leaf { extension, value }
            }
            NodeKind::Extension => {
                let header = decoder.decode::<ExtensionHeader>();
                let extension = decoder.decode_flexible(&header.extension);
                MemTrieNodeView::Extension {
                    hash: &header.nonleaf.hash,
                    memory_usage: header.nonleaf.memory_usage,
                    extension,
                    child: &header.child,
                }
            }
            NodeKind::Branch => {
                let header = decoder.decode::<BranchHeader>();
                let children = decoder.decode_flexible(&header.children);
                MemTrieNodeView::Branch {
                    hash: &header.nonleaf.hash,
                    memory_usage: header.nonleaf.memory_usage,
                    children,
                }
            }
            NodeKind::BranchWithValue => {
                let header = decoder.decode::<BranchWithValueHeader>();
                let children = decoder.decode_flexible(&header.children);
                let value = decoder.decode_flexible(&header.value);
                MemTrieNodeView::BranchWithValue {
                    hash: &header.nonleaf.hash,
                    memory_usage: header.nonleaf.memory_usage,
                    children,
                    value,
                }
            }
        }
    }

    /// Deallocates memory and drops references to children.
    pub(crate) unsafe fn delete(&self) {
        let mut decoder = self.decoder();
        match decoder.peek::<CommonHeader>().kind {
            NodeKind::Leaf => {
                let header = decoder.take_fixed::<LeafHeader>();
                decoder.drop_flexible(&header.extension);
                decoder.drop_flexible(&header.value);
            }
            NodeKind::Extension => {
                let header = decoder.take_fixed::<ExtensionHeader>();
                decoder.drop_flexible(&header.extension);
            }
            NodeKind::Branch => {
                let header = decoder.take_fixed::<BranchHeader>();
                decoder.drop_flexible(&header.children);
            }
            NodeKind::BranchWithValue => {
                let header = decoder.take_fixed::<BranchWithValueHeader>();
                decoder.drop_flexible(&header.children);
                decoder.drop_flexible(&header.value);
            }
        }
        decoder.take_encoded_data(); // drops the memory allocation
    }

    pub(crate) unsafe fn clone_impl(&self) -> Self {
        let mut decoder = self.decoder();
        let header = decoder.decode_as_mut::<CommonHeader>();
        header.refcount += 1;
        Self { data: self.data }
    }

    pub(crate) unsafe fn drop_impl(&mut self) {
        let mut decoder = self.decoder();
        let header = decoder.decode_as_mut::<CommonHeader>();
        header.refcount -= 1;
        if header.refcount == 0 {
            self.delete();
        }
    }
}

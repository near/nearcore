use crate::trie::mem::arena::{ArenaSlice, ArenaSliceMut};

use super::encoding::BorshFixedSize;
use super::FlexibleDataHeader;
use borsh::{BorshDeserialize, BorshSerialize};

/// Flexibly-sized data header for a trie extension path (which is simply
/// a byte array).
#[derive(Clone, Copy, BorshSerialize, BorshDeserialize)]
pub struct EncodedExtensionHeader {
    length: u16,
}

impl BorshFixedSize for EncodedExtensionHeader {
    const SERIALIZED_SIZE: usize = std::mem::size_of::<u16>();
}

impl FlexibleDataHeader for EncodedExtensionHeader {
    type InputData = [u8];
    type View<'a> = ArenaSlice<'a>;
    fn from_input(extension: &[u8]) -> EncodedExtensionHeader {
        EncodedExtensionHeader { length: extension.len() as u16 }
    }

    fn flexible_data_length(&self) -> usize {
        self.length as usize
    }

    fn encode_flexible_data(&self, extension: &[u8], target: &mut ArenaSliceMut<'_>) {
        target.raw_slice_mut().copy_from_slice(&extension);
    }

    fn decode_flexible_data<'a>(&self, source: &ArenaSlice<'a>) -> ArenaSlice<'a> {
        source.clone()
    }
}

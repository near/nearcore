use super::FlexibleDataHeader;
use super::encoding::BorshFixedSize;
use crate::trie::mem::arena::{ArenaMemory, ArenaMemoryMut, ArenaSlice, ArenaSliceMut};
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
    type View<'a, M: ArenaMemory> = &'a [u8];
    fn from_input(extension: &[u8]) -> EncodedExtensionHeader {
        EncodedExtensionHeader { length: extension.len() as u16 }
    }

    fn flexible_data_length(&self) -> usize {
        self.length as usize
    }

    fn encode_flexible_data<M: ArenaMemoryMut>(
        &self,
        extension: &[u8],
        target: &mut ArenaSliceMut<M>,
    ) {
        target.raw_slice_mut().copy_from_slice(&extension);
    }

    fn decode_flexible_data<'a, M: ArenaMemory>(&self, source: &ArenaSlice<'a, M>) -> &'a [u8] {
        source.raw_slice()
    }
}

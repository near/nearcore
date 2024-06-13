use super::FlexibleDataHeader;
use crate::trie::mem::arena::{Arena, ArenaMemory, ArenaPtr, ArenaSliceMut};
use borsh::{BorshDeserialize, BorshSerialize};
use std::io::Write;

/// Implementing this trait indicates that the type can be borsh serialized and
/// the length of the serialization is a constant.
///
/// TODO: it would be nice to generate this, or test the correctness of the
/// `SERIALIZED_SIZE` specification. For now, we rely on higher-level testing.
pub trait BorshFixedSize {
    const SERIALIZED_SIZE: usize;
}

/// Facilitates allocation and encoding of flexibly-sized data.
pub struct RawEncoder<'a, A: Arena> {
    data: ArenaSliceMut<'a, A::Memory>,
    pos: usize,
}

// To make it easier to use borsh serialization.
impl<'a, A: Arena> Write for RawEncoder<'a, A> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.data.raw_slice_mut()[self.pos..self.pos + buf.len()].copy_from_slice(buf);
        self.pos += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a, A: Arena> RawEncoder<'a, A> {
    /// Creates a new arena allocation of the given size, returning an encoder
    /// that can be used to initialize the allocated memory.
    pub fn new(arena: &'a mut A, n: usize) -> Self {
        let data = arena.alloc(n);
        RawEncoder { data, pos: 0 }
    }

    /// Encodes the given fixed-size field to the current encoder position,
    /// and then advances the position by the encoded size of the field.
    pub fn encode<T: BorshSerialize>(&mut self, data: T) {
        data.serialize(self).unwrap();
    }

    /// Encodes the given flexibly-sized part of the data to the current
    /// encoder position, and then advances the position by the size of the
    /// flexibly-sized part, as returned by `header.flexible_data_length()`.
    /// Note that the header itself is NOT encoded; only the flexible part is.
    /// The header is expected to have been encoded earlier.
    pub fn encode_flexible<T: FlexibleDataHeader>(&mut self, header: &T, data: &T::InputData) {
        let length = header.flexible_data_length();
        header.encode_flexible_data(data, &mut self.data.subslice_mut(self.pos, length));
        self.pos += length;
    }

    /// Finishes the encoding process and returns a pointer to the allocated
    /// memory. The caller is responsible for freeing the pointer later.
    pub fn finish(self) -> ArenaSliceMut<'a, A::Memory> {
        assert_eq!(self.pos, self.data.len());
        self.data
    }
}

/// Facilitates the decoding of flexibly-sized data.
pub struct RawDecoder<'a, M: ArenaMemory> {
    data: ArenaPtr<'a, M>,
    pos: usize,
}

impl<'a, M: ArenaMemory> RawDecoder<'a, M> {
    /// Starts decoding from the given memory position. The position should be
    /// the beginning of an earlier slice returned by `RawEncoder::finish`.
    pub fn new(data: ArenaPtr<'a, M>) -> Self {
        RawDecoder { data, pos: 0 }
    }

    /// Decodes a fixed-size field at the current decoder position, and then
    /// advances the position by the size of the field.
    pub fn decode<T: BorshDeserialize + BorshFixedSize>(&mut self) -> T {
        let slice = self.data.slice(self.pos, T::SERIALIZED_SIZE);
        let result = T::try_from_slice(slice.raw_slice()).unwrap();
        self.pos += T::SERIALIZED_SIZE;
        result
    }

    /// Decodes a fixed-sized field at the current position, but does not
    /// advance the position.
    pub fn peek<T: BorshDeserialize + BorshFixedSize>(&mut self) -> T {
        let slice = self.data.slice(self.pos, T::SERIALIZED_SIZE);
        T::try_from_slice(slice.raw_slice()).unwrap()
    }

    /// Decodes a flexibly-sized part of the data at the current position,
    /// and then advances the position by the size of the flexibly-sized part,
    /// as returned by `header.flexible_data_length()`.
    pub fn decode_flexible<T: FlexibleDataHeader>(&mut self, header: &T) -> T::View<'a, M> {
        let length = header.flexible_data_length();
        let view = header.decode_flexible_data(&self.data.slice(self.pos, length));
        self.pos += length;
        view
    }
}

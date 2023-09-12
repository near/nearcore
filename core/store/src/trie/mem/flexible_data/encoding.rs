use std::mem::MaybeUninit;

use super::FlexibleDataHeader;

/// Facilitates allocation and encoding of flexibly-sized data.
pub struct RawEncoder {
    data: Box<[u8]>,
    pos: usize,
}

impl RawEncoder {
    /// Creates a new memory allocation of the given size, returning an encoder
    /// that can be used to initialize the allocated memory.
    pub fn new(n: usize) -> RawEncoder {
        let mut data = Vec::<u8>::new();
        data.reserve_exact(n);
        unsafe {
            data.set_len(n);
        }
        RawEncoder { data: data.into_boxed_slice(), pos: 0 }
    }

    unsafe fn ptr(&mut self) -> *mut u8 {
        self.data.as_mut_ptr().offset(self.pos as isize)
    }

    /// Encodes the given fixed-size field to the current encoder position,
    /// and then advance the position by the size of the field.
    pub unsafe fn encode<T>(&mut self, data: T) {
        assert!(self.pos + std::mem::size_of::<T>() <= self.data.len());
        (*(self.ptr() as *mut MaybeUninit<T>)).write(data);
        self.pos += std::mem::size_of::<T>();
    }

    /// Encodes the given flexibly-sized part of the data to the current
    /// encoder position, and then advance the position by the size of the
    /// flexibly-sized part, as returned by `header.flexible_data_length()`.
    pub unsafe fn encode_flexible<T: FlexibleDataHeader>(
        &mut self,
        header: &T,
        data: T::InputData,
    ) {
        let length = header.flexible_data_length();
        assert!(self.pos + length <= self.data.len());
        header.encode_flexible_data(data, self.ptr());
        self.pos += length;
    }

    /// Finishes the encoding process and returns a pointer to the allocated
    /// memory. The caller is responsible for freeing the pointer later.
    pub unsafe fn finish(self) -> *const u8 {
        assert_eq!(self.pos, self.data.len());
        Box::into_raw(self.data) as *const u8
    }
}

/// Facilitates the decoding of flexibly-sized data.
/// The lifetime 'a should be a lifetime that guarantees the availability of
/// the memory being decoded from.
pub struct RawDecoder<'a> {
    data: *const u8,
    pos: usize,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> RawDecoder<'a> {
    /// Starts decoding from the given memory location. The location should be
    /// a pointer returned by RawEncoder::finish.
    pub fn new(data: *const u8) -> RawDecoder<'a> {
        RawDecoder { data, _marker: std::marker::PhantomData, pos: 0 }
    }

    unsafe fn ptr(&mut self) -> *const u8 {
        self.data.offset(self.pos as isize)
    }

    /// Decodes a fixed-size field at the current decoder position, and then
    /// advances the position by the size of the field.
    pub unsafe fn decode<T>(&mut self) -> &'a T {
        let result = &*(self.ptr() as *const T);
        self.pos += std::mem::size_of::<T>();
        result
    }

    /// Like decode, but returns the reference as mutable.
    pub unsafe fn decode_as_mut<T>(&mut self) -> &'a mut T {
        let result = &mut *(self.ptr() as *mut T);
        self.pos += std::mem::size_of::<T>();
        result
    }

    /// Decodes a fixed-sized field at the current position, but does not
    /// advance the position.
    pub unsafe fn peek<T>(&mut self) -> &'a T {
        &*(self.ptr() as *const T)
    }

    /// Decodes a flexibly-sized part of the data at the current position,
    /// and then advances the position by the size of the flexibly-sized part,
    /// as returned by `header.flexible_data_length()`.
    pub unsafe fn decode_flexible<T: FlexibleDataHeader>(&mut self, header: &'a T) -> T::View<'a> {
        let length = header.flexible_data_length();
        let view = header.decode_flexible_data(self.ptr());
        self.pos += length;
        view
    }

    /// Takes a fixed field from the current position, and then advances the
    /// position by the size of the field. This is useful for dropping the fixed
    /// field when the overall structure is being dropped.
    pub unsafe fn take_fixed<T>(&mut self) -> T {
        let field = &mut *(self.ptr() as *mut MaybeUninit<T>);
        let result = std::mem::replace(field, MaybeUninit::uninit()).assume_init();
        self.pos += std::mem::size_of::<T>();
        result
    }

    /// Drops a flexibly-sized part of the data at the current position, and
    /// then advances the position by the size of the flexibly-sized part.
    pub unsafe fn drop_flexible<T: FlexibleDataHeader>(&mut self, header: &T) {
        let length = header.flexible_data_length();
        header.drop_flexible_data(self.ptr() as *mut u8);
        self.pos += length;
    }

    /// At the end of decoding, converts the memory buffer to a Box so that the
    /// memory itself can be deallocated. Requires that the decoding be complete
    /// (so that we have the right size for the memory allocation).
    pub unsafe fn take_encoded_data(&self) -> Box<[u8]> {
        Box::from_raw(std::ptr::slice_from_raw_parts_mut(self.data as *mut u8, self.pos))
    }
}

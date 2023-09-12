use super::FlexibleDataHeader;

/// Flexibly-sized data header for a trie extension path (which is simply
/// a byte array).
#[repr(C, packed(1))]
#[derive(Clone, Copy)]
pub struct EncodedExtensionHeader {
    length: u16,
}

impl FlexibleDataHeader for EncodedExtensionHeader {
    type InputData = Box<[u8]>;
    type View<'a> = &'a [u8];
    fn from_input(extension: &Box<[u8]>) -> EncodedExtensionHeader {
        EncodedExtensionHeader { length: extension.len() as u16 }
    }

    fn flexible_data_length(&self) -> usize {
        self.length as usize
    }

    unsafe fn encode_flexible_data(&self, extension: Box<[u8]>, ptr: *mut u8) {
        std::ptr::copy_nonoverlapping(extension.as_ptr(), ptr, self.length as usize);
    }

    unsafe fn decode_flexible_data<'a>(&'a self, ptr: *const u8) -> &'a [u8] {
        std::slice::from_raw_parts(ptr, self.length as usize)
    }

    unsafe fn drop_flexible_data(&self, _ptr: *mut u8) {}
}

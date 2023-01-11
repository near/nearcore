use crate::{MemSlice, MemoryLike};

use std::borrow::Cow;

#[derive(Default)]
pub struct MockedMemory {}

impl MemoryLike for MockedMemory {
    fn fits_memory(&self, _slice: MemSlice) -> Result<(), ()> {
        Ok(())
    }

    fn view_memory(&self, slice: MemSlice) -> Result<Cow<[u8]>, ()> {
        let view = unsafe { std::slice::from_raw_parts(slice.ptr as *const u8, slice.len()?) };
        Ok(Cow::Borrowed(view))
    }

    fn read_memory(&self, offset: u64, buffer: &mut [u8]) -> Result<(), ()> {
        let src = unsafe { std::slice::from_raw_parts(offset as *const u8, buffer.len()) };
        buffer.copy_from_slice(src);
        Ok(())
    }

    fn write_memory(&mut self, offset: u64, buffer: &[u8]) -> Result<(), ()> {
        let dest = unsafe { std::slice::from_raw_parts_mut(offset as *mut u8, buffer.len()) };
        dest.copy_from_slice(buffer);
        Ok(())
    }
}

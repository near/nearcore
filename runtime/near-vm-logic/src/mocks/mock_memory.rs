use crate::MemoryLike;

#[derive(Default)]
pub struct MockedMemory {}

impl MemoryLike for MockedMemory {
    fn fits_memory(&self, _offset: u64, _len: u64) -> bool {
        true
    }

    fn read_memory(&self, offset: u64, buffer: &mut [u8]) {
        let src = unsafe { std::slice::from_raw_parts(offset as *const u8, buffer.len() as usize) };
        buffer.copy_from_slice(src);
    }

    fn read_memory_u8(&self, offset: u64) -> u8 {
        let offset = offset as *const u8;
        unsafe { *offset }
    }

    fn write_memory(&mut self, offset: u64, buffer: &[u8]) {
        let dest =
            unsafe { std::slice::from_raw_parts_mut(offset as *mut u8, buffer.len() as usize) };
        dest.copy_from_slice(buffer);
    }
}

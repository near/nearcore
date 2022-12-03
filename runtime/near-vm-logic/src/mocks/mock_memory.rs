use crate::MemoryLike;

#[derive(Default)]
pub struct MockedMemory {}

impl MemoryLike for MockedMemory {
    fn read_memory(&self, offset: u64, buffer: &mut [u8]) -> Result<(), ()> {
        let src = unsafe { std::slice::from_raw_parts(offset as *const u8, buffer.len() as usize) };
        buffer.copy_from_slice(src);
        Ok(())
    }

    fn write_memory(&mut self, offset: u64, buffer: &[u8]) -> Result<(), ()> {
        let dest =
            unsafe { std::slice::from_raw_parts_mut(offset as *mut u8, buffer.len() as usize) };
        dest.copy_from_slice(buffer);
        Ok(())
    }
}

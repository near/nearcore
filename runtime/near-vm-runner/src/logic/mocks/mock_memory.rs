use crate::logic::{MemSlice, MemoryLike};

use std::borrow::Cow;

pub struct MockedMemory(Box<[u8]>);

impl MockedMemory {
    pub const MEMORY_SIZE: u64 = 64 * 1024;
}

impl Default for MockedMemory {
    fn default() -> Self {
        Self(vec![0; Self::MEMORY_SIZE as usize].into())
    }
}

fn make_range(ptr: u64, len: usize) -> Result<core::ops::Range<usize>, ()> {
    let start = usize::try_from(ptr).map_err(|_| ())?;
    let end = start.checked_add(len).ok_or(())?;
    Ok(start..end)
}

impl MemoryLike for MockedMemory {
    fn fits_memory(&self, slice: MemSlice) -> Result<(), ()> {
        match self.0.get(slice.range::<usize>()?) {
            Some(_) => Ok(()),
            None => Err(()),
        }
    }

    fn view_memory(&self, slice: MemSlice) -> Result<Cow<[u8]>, ()> {
        self.0.get(slice.range::<usize>()?).map(Cow::Borrowed).ok_or(())
    }

    fn read_memory(&self, ptr: u64, buffer: &mut [u8]) -> Result<(), ()> {
        let slice = self.0.get(make_range(ptr, buffer.len())?).ok_or(())?;
        buffer.copy_from_slice(slice);
        Ok(())
    }

    fn write_memory(&mut self, ptr: u64, buffer: &[u8]) -> Result<(), ()> {
        let slice = self.0.get_mut(make_range(ptr, buffer.len())?).ok_or(())?;
        slice.copy_from_slice(buffer);
        Ok(())
    }
}

#[test]
fn test_memory_like() {
    crate::logic::test_utils::test_memory_like(|| Box::new(MockedMemory::default()));
}

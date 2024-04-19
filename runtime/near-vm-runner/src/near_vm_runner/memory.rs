use crate::logic::{MemSlice, MemoryLike};
use near_vm_types::{MemoryType, Pages};
use near_vm_vm::{LinearMemory, Memory, MemoryStyle, VMMemory};
use std::borrow::Cow;
use std::sync::Arc;

#[derive(Clone)]
pub struct NearVmMemory(Arc<LinearMemory>);

impl NearVmMemory {
    pub fn new(
        initial_memory_pages: u32,
        max_memory_pages: u32,
    ) -> Result<Self, near_vm_vm::MemoryError> {
        let max_pages = Pages(max_memory_pages);
        Ok(NearVmMemory(Arc::new(LinearMemory::new(
            &MemoryType::new(Pages(initial_memory_pages), Some(max_pages), false),
            &MemoryStyle::Static {
                bound: max_pages,
                offset_guard_size: near_vm_types::WASM_PAGE_SIZE as u64,
            },
        )?)))
    }

    /// Returns pointer to memory at the specified offset provided that there’s
    /// enough space in the buffer starting at the returned pointer.
    ///
    /// Safety: Caller must guarantee that the returned pointer is not used
    /// after guest memory mapping is changed (e.g. grown).
    unsafe fn get_ptr(&self, offset: u64, len: usize) -> Result<*mut u8, ()> {
        let offset = usize::try_from(offset).map_err(|_| ())?;
        // SAFETY: Caller promisses memory mapping won’t change.
        let vmmem = unsafe { self.0.vmmemory().as_ref() };
        // `checked_sub` here verifies that offsetting the buffer by offset
        // still lands us in-bounds of the allocated object.
        let remaining = vmmem.current_length.checked_sub(offset).ok_or(())?;
        if len <= remaining {
            Ok(vmmem.base.add(offset))
        } else {
            Err(())
        }
    }

    /// Returns shared reference to slice in guest memory at given offset.
    ///
    /// Safety: Caller must guarantee that guest memory mapping is not changed
    /// (e.g. grown) while the slice is held.
    unsafe fn get(&self, offset: u64, len: usize) -> Result<&[u8], ()> {
        // SAFETY: Caller promisses memory mapping won’t change.
        let ptr = unsafe { self.get_ptr(offset, len)? };
        // SAFETY: get_ptr verifies that [ptr, ptr+len) is valid slice.
        Ok(unsafe { core::slice::from_raw_parts(ptr, len) })
    }

    /// Returns shared reference to slice in guest memory at given offset.
    ///
    /// Safety: Caller must guarantee that guest memory mapping is not changed
    /// (e.g. grown) while the slice is held.
    unsafe fn get_mut(&mut self, offset: u64, len: usize) -> Result<&mut [u8], ()> {
        // SAFETY: Caller promisses memory mapping won’t change.
        let ptr = unsafe { self.get_ptr(offset, len)? };
        // SAFETY: get_ptr verifies that [ptr, ptr+len) is valid slice and since
        // we’re holding exclusive self reference another mut reference won’t be
        // created
        Ok(unsafe { core::slice::from_raw_parts_mut(ptr, len) })
    }

    pub(super) fn vm(&self) -> VMMemory {
        VMMemory::new(self.0.clone(), None)
    }
}

impl MemoryLike for NearVmMemory {
    fn fits_memory(&self, slice: MemSlice) -> Result<(), ()> {
        // SAFETY: Contracts are executed on a single thread thus we know no one
        // will change guest memory mapping under us.
        unsafe { self.get_ptr(slice.ptr, slice.len()?) }.map(|_| ())
    }

    fn view_memory(&self, slice: MemSlice) -> Result<Cow<[u8]>, ()> {
        // SAFETY: Firstly, contracts are executed on a single thread thus we
        // know no one will change guest memory mapping under us.  Secondly, the
        // way MemoryLike interface is used we know the memory mapping won’t be
        // changed by the caller while it holds the slice reference.
        unsafe { self.get(slice.ptr, slice.len()?) }.map(Cow::Borrowed)
    }

    fn read_memory(&self, offset: u64, buffer: &mut [u8]) -> Result<(), ()> {
        // SAFETY: Contracts are executed on a single thread thus we know no one
        // will change guest memory mapping under us.
        Ok(buffer.copy_from_slice(unsafe { self.get(offset, buffer.len())? }))
    }

    fn write_memory(&mut self, offset: u64, buffer: &[u8]) -> Result<(), ()> {
        // SAFETY: Contracts are executed on a single thread thus we know no one
        // will change guest memory mapping under us.
        Ok(unsafe { self.get_mut(offset, buffer.len())? }.copy_from_slice(buffer))
    }
}

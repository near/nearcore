// This file contains code from external sources.
// Attributions: https://github.com/wasmerio/wasmer/blob/master/ATTRIBUTIONS.md

//! Memory management for executable code.
use rustix::mm::{self, MapFlags, MprotectFlags, ProtFlags};
use std::sync::Arc;
use near_vm_compiler::CompileError;

/// The optimal alignment for functions.
///
/// On x86-64, this is 16 since it's what the optimizations assume.
/// When we add support for other architectures, we should also figure out their
/// optimal alignment values.
pub(crate) const ARCH_FUNCTION_ALIGNMENT: u16 = 16;

/// The optimal alignment for data.
///
pub(crate) const DATA_SECTION_ALIGNMENT: u16 = 64;

fn round_up(size: usize, multiple: usize) -> usize {
    debug_assert!(multiple.is_power_of_two());
    (size + (multiple - 1)) & !(multiple - 1)
}

pub struct CodeMemoryWriter<'a> {
    memory: &'a mut CodeMemory,
    offset: usize,
}

impl<'a> CodeMemoryWriter<'a> {
    /// Write the contents from the provided buffer into the location of `memory` aligned to
    /// provided `alignment`.
    ///
    /// Returns the position within the mapping at which the buffer was written.
    pub fn write_aligned(&mut self, alignment: u16, input: &[u8]) -> Result<usize, CompileError> {
        let entry_offset = self.offset;
        let aligned_offset = round_up(entry_offset, usize::from(alignment));
        let final_offset = aligned_offset + input.len();
        let out_buffer = self.memory.as_slice_mut();
        // Fill out the padding with zeroes, if only to make sure there are no gadgets in there.
        out_buffer
            .get_mut(entry_offset..aligned_offset)
            .ok_or_else(|| CompileError::Resource("out of code memory space".into()))?
            .fill(0);
        let out_buffer = out_buffer
            .get_mut(aligned_offset..final_offset)
            .ok_or_else(|| CompileError::Resource("out of code memory space".into()))?;
        out_buffer.copy_from_slice(input);
        self.offset = final_offset;
        Ok(aligned_offset)
    }

    pub fn write_executable(
        &mut self,
        alignment: u16,
        input: &[u8],
    ) -> Result<usize, CompileError> {
        let result = self.write_aligned(alignment, input);
        self.memory.executable_size = self.offset;
        result
    }

    /// The current position of the writer.
    pub fn position(&self) -> usize {
        self.offset
    }
}

/// Mappings to regions of memory storing the executable JIT code.
pub struct CodeMemory {
    /// Where to return this memory to when dropped.
    source_pool: Option<Arc<crossbeam_queue::ArrayQueue<CodeMemory>>>,

    /// The mapping
    map: *mut u8,

    /// Mapping size
    size: usize,

    /// Size of the executable portion that comes first.
    executable_size: usize,
}

impl CodeMemory {
    fn create(size: usize) -> rustix::io::Result<CodeMemory> {
        // Make sure callers don’t pass in a 0-sized map request. That is most likely a bug.
        assert!(size != 0);
        let size = round_up(size, rustix::param::page_size());
        let map = unsafe {
            mm::mmap_anonymous(
                std::ptr::null_mut(),
                size,
                ProtFlags::WRITE | ProtFlags::READ,
                MapFlags::SHARED,
            )?
        };
        Ok(CodeMemory {
            source_pool: None,
            map: map.cast(),
            executable_size: 0,
            size,
        })
    }

    fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe {
            // SAFETY: We have made sure that this is the only reference to the memory region by
            // requiring a mutable self reference.
            std::slice::from_raw_parts_mut(self.map, self.size)
        }
    }

    /// Ensure this CodeMemory is at least of the requested size.
    ///
    /// This may invalidate any data previously written into the mapping.
    pub fn resize(mut self, size: usize) -> rustix::io::Result<Self> {
        if self.size < size {
            let source_pool = self.source_pool.take();
            // Ideally we would use mremap, but see
            // https://bugzilla.kernel.org/show_bug.cgi?id=8691
            unsafe {
                mm::munmap(self.map.cast(), self.size)?;
                std::mem::forget(self);
            }
            Self::create(size).map(|mut m| {
                m.source_pool = source_pool;
                m
            })
        } else {
            Ok(self)
        }
    }

    /// Write to this code memory from the beginning of the mapping.
    ///
    /// # Safety
    ///
    /// At the time this method is called, there should remain no dangling readable/executable
    /// references to this `CodeMemory`, for the original code memory that those references point
    /// to are invalidated as soon as this method is invoked.
    pub unsafe fn writer(&mut self) -> CodeMemoryWriter<'_> {
        self.executable_size = 0;
        CodeMemoryWriter {
            memory: self,
            offset: 0,
        }
    }

    /// Publish the specified number of bytes as executable code.
    ///
    /// # Safety
    ///
    /// Calling this requires that no mutable references to the code memory remain.
    pub unsafe fn publish(&mut self) -> Result<(), CompileError> {
        mm::mprotect(
            self.map.cast(),
            self.executable_size,
            MprotectFlags::EXEC | MprotectFlags::READ,
        )
        .map_err(|e| {
            CompileError::Resource(format!("could not make code memory executable: {}", e))
        })
    }

    /// Remap the offset into an absolute address within a read-execute mapping.
    pub fn executable_address(&self, offset: usize) -> *const u8 {
        unsafe {
            // TODO: encapsulate offsets so that this `offset` is guaranteed to be sound.
            self.map.offset(offset as isize)
        }
    }

    /// Remap the offset into an absolute address within a read-write mapping.
    pub fn writable_address(&self, offset: usize) -> *mut u8 {
        unsafe {
            // TODO: encapsulate offsets so that this `offset` is guaranteed to be sound.
            self.map.offset(offset as isize)
        }
    }
}

impl Drop for CodeMemory {
    fn drop(&mut self) {
        if let Some(source_pool) = self.source_pool.take() {
            unsafe {
                let result = mm::mprotect(
                    self.map.cast(),
                    self.size,
                    MprotectFlags::WRITE | MprotectFlags::READ,
                );
                if let Err(e) = result {
                    tracing::error!(
                        message="could not mprotect mapping before returning it to the memory pool",
                        map=?self.map, size=self.size, error=%e
                    );
                }
            }
            drop(source_pool.push(CodeMemory {
                source_pool: None,
                map: self.map,
                size: self.size,
                executable_size: 0,
            }));
        } else {
            unsafe {
                if let Err(e) = mm::munmap(self.map.cast(), self.size) {
                    tracing::error!(
                        message="could not unmap mapping",
                        map=?self.map, size=self.size, error=%e
                    );
                }
            }
        }
    }
}

unsafe impl Send for CodeMemory {}

/// The pool of preallocated memory maps for storing the code.
///
/// This pool cannot grow and will only allow up to a number of code mappings that were specified
/// at construction time.
///
/// However it is possible for the mappings inside to grow to accomodate larger code (TBD)
#[derive(Clone)]
pub struct LimitedMemoryPool {
    pool: Arc<crossbeam_queue::ArrayQueue<CodeMemory>>,
}

impl LimitedMemoryPool {
    /// Create a new pool with `count` mappings initialized to `default_memory_size` each.
    pub fn new(count: usize, default_memory_size: usize) -> rustix::io::Result<Self> {
        let pool = Arc::new(crossbeam_queue::ArrayQueue::new(count));
        let this = Self { pool };
        for _ in 0..count {
            drop(this.pool.push(CodeMemory::create(default_memory_size)?));
        }
        Ok(this)
    }

    /// Get a memory mapping, at least `size` bytes large.
    pub fn get(&self, size: usize) -> rustix::io::Result<CodeMemory> {
        let mut memory = self.pool.pop();
        if let Some(memory) = memory.as_mut() {
            memory.source_pool = Some(Arc::clone(&self.pool));
        }
        let memory = memory.ok_or(rustix::io::Errno::NOMEM)?;
        if memory.size < size {
            Ok(memory.resize(size)?)
        } else {
            Ok(memory)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CodeMemory;
    fn _assert() {
        fn _assert_send<T: Send>() {}
        _assert_send::<CodeMemory>();
    }
}

// This file contains code from external sources.
// Attributions: https://github.com/wasmerio/wasmer/blob/master/ATTRIBUTIONS.md

//! Low-level abstraction for allocating and managing zero-filled pages
//! of memory.

use more_asserts::assert_le;
use more_asserts::assert_lt;
use std::io;
use std::ptr;
use std::slice;

/// Round `size` up to the nearest multiple of `page_size`.
fn round_up_to_page_size(size: usize, page_size: usize) -> usize {
    (size + (page_size - 1)) & !(page_size - 1)
}

/// A simple struct consisting of a page-aligned pointer to page-aligned
/// and initially-zeroed memory and a length.
#[derive(Debug)]
pub struct Mmap {
    // Note that this is stored as a `usize` instead of a `*const` or `*mut`
    // pointer to allow this structure to be natively `Send` and `Sync` without
    // `unsafe impl`. This type is sendable across threads and shareable since
    // the coordination all happens at the OS layer.
    ptr: usize,
    len: usize,
    accessible_len: usize,
}

impl Mmap {
    /// Construct a new empty instance of `Mmap`.
    pub fn new() -> Self {
        // Rust's slices require non-null pointers, even when empty. `Vec`
        // contains code to create a non-null dangling pointer value when
        // constructed empty, so we reuse that here.
        let empty = Vec::<u8>::new();
        Self { ptr: empty.as_ptr() as usize, len: 0, accessible_len: 0 }
    }

    /// Create a new `Mmap` pointing to at least `size` bytes of page-aligned accessible memory.
    pub fn with_at_least(size: usize) -> Result<Self, String> {
        let page_size = region::page::size();
        let rounded_size = round_up_to_page_size(size, page_size);
        Self::accessible_reserved(rounded_size, rounded_size)
    }

    /// Create a new `Mmap` pointing to `accessible_size` bytes of page-aligned accessible memory,
    /// within a reserved mapping of `mapping_size` bytes. `accessible_size` and `mapping_size`
    /// must be native page-size multiples.
    #[cfg(not(target_os = "windows"))]
    pub fn accessible_reserved(
        accessible_size: usize,
        mapping_size: usize,
    ) -> Result<Self, String> {
        let page_size = region::page::size();
        assert_le!(accessible_size, mapping_size);
        assert_eq!(mapping_size & (page_size - 1), 0);
        assert_eq!(accessible_size & (page_size - 1), 0);

        // Mmap may return EINVAL if the size is zero, so just
        // special-case that.
        if mapping_size == 0 {
            return Ok(Self::new());
        }

        Ok(if accessible_size == mapping_size {
            // Allocate a single read-write region at once.
            let ptr = unsafe {
                libc::mmap(
                    ptr::null_mut(),
                    mapping_size,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANON,
                    -1,
                    0,
                )
            };
            if ptr as isize == -1_isize {
                return Err(io::Error::last_os_error().to_string());
            }

            Self { ptr: ptr as usize, len: mapping_size, accessible_len: accessible_size }
        } else {
            // Reserve the mapping size.
            let ptr = unsafe {
                libc::mmap(
                    ptr::null_mut(),
                    mapping_size,
                    libc::PROT_NONE,
                    libc::MAP_PRIVATE | libc::MAP_ANON,
                    -1,
                    0,
                )
            };
            if ptr as isize == -1_isize {
                return Err(io::Error::last_os_error().to_string());
            }

            let mut result = Self { ptr: ptr as usize, len: mapping_size, accessible_len: 0 };

            if accessible_size != 0 {
                // Commit the accessible size.
                result.make_accessible(accessible_size)?;
            }

            result
        })
    }

    /// Create a new `Mmap` pointing to `accessible_size` bytes of page-aligned accessible memory,
    /// within a reserved mapping of `mapping_size` bytes. `accessible_size` and `mapping_size`
    /// must be native page-size multiples.
    #[cfg(target_os = "windows")]
    pub fn accessible_reserved(
        accessible_size: usize,
        mapping_size: usize,
    ) -> Result<Self, String> {
        use winapi::um::memoryapi::VirtualAlloc;
        use winapi::um::winnt::{MEM_COMMIT, MEM_RESERVE, PAGE_NOACCESS, PAGE_READWRITE};

        let page_size = region::page::size();
        assert_le!(accessible_size, mapping_size);
        assert_eq!(mapping_size & (page_size - 1), 0);
        assert_eq!(accessible_size & (page_size - 1), 0);

        // VirtualAlloc may return ERROR_INVALID_PARAMETER if the size is zero,
        // so just special-case that.
        if mapping_size == 0 {
            return Ok(Self::new());
        }

        Ok(if accessible_size == mapping_size {
            // Allocate a single read-write region at once.
            let ptr = unsafe {
                VirtualAlloc(
                    ptr::null_mut(),
                    mapping_size,
                    MEM_RESERVE | MEM_COMMIT,
                    PAGE_READWRITE,
                )
            };
            if ptr.is_null() {
                return Err(io::Error::last_os_error().to_string());
            }

            Self { ptr: ptr as usize, len: mapping_size }
        } else {
            // Reserve the mapping size.
            let ptr =
                unsafe { VirtualAlloc(ptr::null_mut(), mapping_size, MEM_RESERVE, PAGE_NOACCESS) };
            if ptr.is_null() {
                return Err(io::Error::last_os_error().to_string());
            }

            let mut result = Self { ptr: ptr as usize, len: mapping_size, accessible_len: 0 };

            if accessible_size != 0 {
                // Commit the accessible size.
                result.make_accessible(accessible_size)?;
            }

            result
        })
    }

    /// Make the memory accessible for `len` bytes.
    /// `len` must be a native page-size multiple and describe a range within
    /// `self`'s reserved memory.
    #[cfg(not(target_os = "windows"))]
    pub fn make_accessible(&mut self, new_len: usize) -> Result<(), String> {
        let page_size = region::page::size();
        assert_eq!(new_len & (page_size - 1), 0);
        assert_lt!(new_len, self.len);
        let Some(additional_len) = new_len.checked_sub(self.accessible_len) else {
            return Ok(());
        };

        // Commit the accessible size.
        unsafe {
            region::protect(
                self.as_ptr().add(self.accessible_len),
                additional_len,
                region::Protection::READ_WRITE,
            )
        }
        .map_err(|e| e.to_string())?;
        self.accessible_len = new_len;
        Ok(())
    }

    /// Make the memory accessible for `len` bytes.
    /// `len` must be a native page-size multiple and describe a range within
    /// `self`'s reserved memory.
    #[cfg(target_os = "windows")]
    pub fn make_accessible(&mut self, new_len: usize) -> Result<(), String> {
        use winapi::ctypes::c_void;
        use winapi::um::memoryapi::VirtualAlloc;
        use winapi::um::winnt::{MEM_COMMIT, PAGE_READWRITE};
        let page_size = region::page::size();
        assert_eq!(new_len & (page_size - 1), 0);
        assert_lt!(new_len, self.len);
        let Some(additional_len) = new_len.checked_sub(self.accessible_len) else {
            return Ok(());
        };

        // Commit the accessible size.
        if unsafe {
            VirtualAlloc(
                self.as_ptr().add(self.accessible_len) as *mut c_void,
                new_len,
                MEM_COMMIT,
                PAGE_READWRITE,
            )
        }
        .is_null()
        {
            return Err(io::Error::last_os_error().to_string());
        }

        self.accessible_len = new_len;
        Ok(())
    }

    /// Resets the mmap, putting all byte values back to 0 and resetting the accessible length
    #[cfg(not(target_os = "windows"))]
    pub fn reset(&mut self) -> Result<(), String> {
        unsafe {
            self.as_mut_ptr().write_bytes(0, self.accessible_len);
            region::protect(self.as_ptr(), self.accessible_len, region::Protection::NONE)
                .map_err(|e| e.to_string())?;
            self.accessible_len = 0;
            Ok(())
        }
    }

    /// Resets the mmap, putting all byte values back to 0 and resetting the accessible length
    #[cfg(target_os = "windows")]
    pub fn reset(&mut self) -> Result<(), String> {
        use winapi::ctypes::c_void;
        use winapi::um::memoryapi::VirtualAlloc;
        use winapi::um::winnt::{MEM_COMMIT, PAGE_NOACCESS};

        // Commit the accessible size.
        unsafe {
            self.as_mut_ptr().write_bytes(0, self.accessible_len);
            if VirtualAlloc(
                self.as_ptr() as *mut c_void,
                self.accessible_len,
                MEM_COMMIT,
                PAGE_NOACCESS,
            )
            .is_null()
            {
                return Err(io::Error::last_os_error().to_string());
            }
            self.accessible_len = 0;
            Ok(())
        }
    }

    /// Return the allocated memory as a slice of u8.
    pub fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.as_ptr(), self.len) }
    }

    /// Return the allocated memory as a mutable slice of u8.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr(), self.len) }
    }

    /// Return the allocated memory as a pointer to u8.
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr as *const u8
    }

    /// Return the allocated memory as a mutable pointer to u8.
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr as *mut u8
    }

    /// Return the length of the allocated memory.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Return whether any memory has been allocated.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Drop for Mmap {
    #[cfg(not(target_os = "windows"))]
    fn drop(&mut self) {
        if self.len != 0 {
            let r = unsafe { libc::munmap(self.ptr as *mut libc::c_void, self.len) };
            assert_eq!(r, 0, "munmap failed: {}", io::Error::last_os_error());
        }
    }

    #[cfg(target_os = "windows")]
    fn drop(&mut self) {
        if self.len != 0 {
            use winapi::ctypes::c_void;
            use winapi::um::memoryapi::VirtualFree;
            use winapi::um::winnt::MEM_RELEASE;
            let r = unsafe { VirtualFree(self.ptr as *mut c_void, 0, MEM_RELEASE) };
            assert_ne!(r, 0);
        }
    }
}

fn _assert() {
    fn _assert_send_sync<T: Send + Sync>() {}
    _assert_send_sync::<Mmap>();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_up_to_page_size() {
        assert_eq!(round_up_to_page_size(0, 4096), 0);
        assert_eq!(round_up_to_page_size(1, 4096), 4096);
        assert_eq!(round_up_to_page_size(4096, 4096), 4096);
        assert_eq!(round_up_to_page_size(4097, 4096), 8192);
    }
}

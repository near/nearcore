//! Types for a reusable pointer abstraction for accessing Wasm linear memory.
//!
//! This abstraction is safe: it ensures the memory is in bounds and that the pointer
//! is aligned (avoiding undefined behavior).
//!
//! Therefore, you should use this abstraction whenever possible to avoid memory
//! related bugs when implementing an ABI.

use crate::sys::cell::WasmCell;
use crate::sys::{externals::Memory, FromToNativeWasmType};
use std::{cell::Cell, marker::PhantomData, mem};
use wasmer_types::ValueType;

/// The `Array` marker type. This type can be used like `WasmPtr<T, Array>`
/// to get access to methods
pub struct Array;
/// The `Item` marker type. This is the default and does not usually need to be
/// specified.
pub struct Item;

/// A zero-cost type that represents a pointer to something in Wasm linear
/// memory.
#[repr(transparent)]
pub struct WasmPtr<T: Copy, Ty = Item> {
    offset: u32,
    _phantom: PhantomData<(T, Ty)>,
}

/// Methods relevant to all types of `WasmPtr`.
impl<T: Copy, Ty> WasmPtr<T, Ty> {
    /// Create a new `WasmPtr` at the given offset.
    #[inline]
    pub fn new(offset: u32) -> Self {
        Self {
            offset,
            _phantom: PhantomData,
        }
    }

    /// Get the offset into Wasm linear memory for this `WasmPtr`.
    #[inline]
    pub fn offset(self) -> u32 {
        self.offset
    }
}

#[inline(always)]
fn align_pointer(ptr: usize, align: usize) -> usize {
    // clears bits below aligment amount (assumes power of 2) to align pointer
    debug_assert!(align.count_ones() == 1);
    ptr & !(align - 1)
}

/// Methods for `WasmPtr`s to arrays of data that can be dereferenced, namely to
/// types that implement [`ValueType`], meaning that they're valid for all
/// possible bit patterns.
impl<T: Copy + ValueType> WasmPtr<T, Array> {
    /// Dereference the `WasmPtr` getting access to a `&[Cell<T>]` allowing for
    /// reading and mutating of the inner values.
    ///
    /// This method is unsound if used with unsynchronized shared memory.
    /// If you're unsure what that means, it likely does not apply to you.
    /// This invariant will be enforced in the future.
    #[inline]
    pub fn deref<'a>(
        self,
        memory: &'a Memory,
        index: u32,
        length: u32,
    ) -> Option<Vec<WasmCell<'a, T>>> {
        // gets the size of the item in the array with padding added such that
        // for any index, we will always result an aligned memory access
        let item_size = mem::size_of::<T>();
        let slice_full_len = index as usize + length as usize;
        let memory_size = memory.size().bytes().0;

        if (self.offset as usize) + (item_size * slice_full_len) > memory_size
            || (self.offset as usize) >= memory_size
            || item_size == 0
        {
            return None;
        }
        let cell_ptrs = unsafe {
            let cell_ptr = align_pointer(
                memory.view::<u8>().as_ptr().add(self.offset as usize) as usize,
                mem::align_of::<T>(),
            ) as *const Cell<T>;
            &std::slice::from_raw_parts(cell_ptr, slice_full_len)[index as usize..slice_full_len]
        };

        let wasm_cells = cell_ptrs
            .iter()
            .map(|ptr| WasmCell::new(ptr))
            .collect::<Vec<_>>();
        Some(wasm_cells)
    }

    /// Get a UTF-8 `String` from the `WasmPtr` with the given length.
    ///
    /// an aliasing `WasmPtr` is used to mutate memory.
    pub fn get_utf8_string(self, memory: &Memory, str_len: u32) -> Option<String> {
        let memory_size = memory.size().bytes().0;
        if self.offset as usize + str_len as usize > memory.size().bytes().0
            || self.offset as usize >= memory_size
        {
            return None;
        }

        // TODO: benchmark the internals of this function: there is likely room for
        // micro-optimization here and this may be a fairly common function in user code.
        let view = memory.view::<u8>();

        let mut vec: Vec<u8> = Vec::with_capacity(str_len as usize);
        let base = self.offset as usize;
        for i in 0..(str_len as usize) {
            let byte = view[base + i].get();
            vec.push(byte);
        }

        String::from_utf8(vec).ok()
    }
}

unsafe impl<T: Copy, Ty> FromToNativeWasmType for WasmPtr<T, Ty> {
    type Native = i32;

    fn to_native(self) -> Self::Native {
        self.offset as i32
    }
    fn from_native(n: Self::Native) -> Self {
        Self {
            offset: n as u32,
            _phantom: PhantomData,
        }
    }
}

unsafe impl<T: Copy, Ty> ValueType for WasmPtr<T, Ty> {}

impl<T: Copy, Ty> Clone for WasmPtr<T, Ty> {
    fn clone(&self) -> Self {
        Self {
            offset: self.offset,
            _phantom: PhantomData,
        }
    }
}

impl<T: Copy, Ty> Copy for WasmPtr<T, Ty> {}

//! Types for a reusable pointer abstraction for accessing Wasm linear memory.
//!
//! This abstraction is safe: it ensures the memory is in bounds and that the pointer
//! is aligned (avoiding undefined behavior).
//!
//! Therefore, you should use this abstraction whenever possible to avoid memory
//! related bugs when implementing an ABI.

use super::externals::FromToNativeWasmType;
use near_vm_types::ValueType;
use std::marker::PhantomData;

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

unsafe impl<T: Copy, Ty> FromToNativeWasmType for WasmPtr<T, Ty> {
    type Native = i32;

    fn to_native(self) -> Self::Native {
        self.offset as i32
    }
    fn from_native(n: Self::Native) -> Self {
        Self { offset: n as u32, _phantom: PhantomData }
    }
}

unsafe impl<T: Copy, Ty> ValueType for WasmPtr<T, Ty> {}

impl<T: Copy, Ty> Clone for WasmPtr<T, Ty> {
    fn clone(&self) -> Self {
        Self { offset: self.offset, _phantom: PhantomData }
    }
}

impl<T: Copy, Ty> Copy for WasmPtr<T, Ty> {}

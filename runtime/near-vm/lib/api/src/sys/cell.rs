pub use std::cell::Cell;

/// A mutable Wasm-memory location.
#[repr(transparent)]
pub struct WasmCell<'a, T: ?Sized> {
    inner: &'a Cell<T>,
}

unsafe impl<T: ?Sized> Send for WasmCell<'_, T> where T: Send {}

unsafe impl<T: ?Sized> Sync for WasmCell<'_, T> {}

impl<'a, T> WasmCell<'a, T> {
    /// Creates a new `WasmCell` containing the given value.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::cell::Cell;
    /// use near_vm::WasmCell;
    ///
    /// let cell = Cell::new(5);
    /// let wasm_cell = WasmCell::new(&cell);
    /// ```
    #[inline]
    pub const fn new(cell: &'a Cell<T>) -> WasmCell<'a, T> {
        WasmCell { inner: cell }
    }
}

impl<T: Sized> WasmCell<'_, T> {
    /// Sets the contained value.
    #[inline]
    pub fn set(&self, val: T) {
        self.inner.set(val);
    }
}

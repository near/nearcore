//! Allocation counter for tests.
//!
//! Wraps the system allocator. While enabled on a thread, it adds up the bytes
//! that thread allocates. Frees are ignored, so the result is the total number
//! of allocated bytes, not peak live memory. The counter is per thread, so
//! tests running in parallel do not affect each other.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    static ENABLED: Cell<bool> = const { Cell::new(false) };
    static ALLOCATED: Cell<usize> = const { Cell::new(0) };
}

pub(crate) struct CountingAllocator;

/// Runs `f` on the current thread and returns its result together with the
/// number of bytes allocated while it ran. Calls must not be nested.
pub(crate) fn measure_allocated<R>(f: impl FnOnce() -> R) -> (R, usize) {
    struct DisableOnDrop;
    impl Drop for DisableOnDrop {
        fn drop(&mut self) {
            ENABLED.set(false);
        }
    }

    assert!(!ENABLED.get(), "measure_allocated calls must not be nested");
    ALLOCATED.set(0);
    ENABLED.set(true);
    let guard = DisableOnDrop;
    let result = f();
    drop(guard);
    (result, ALLOCATED.get())
}

// The hooks below must never allocate, log or panic, hence `try_with`.
fn record(size: usize) {
    let _ = ENABLED.try_with(|enabled| {
        if enabled.get() {
            let _ =
                ALLOCATED.try_with(|allocated| allocated.set(allocated.get().saturating_add(size)));
        }
    });
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            record(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() && new_size > layout.size() {
            record(new_size - layout.size());
        }
        new_ptr
    }
}

mod tests {
    use super::{ENABLED, measure_allocated};
    use std::hint::black_box;
    use std::panic::catch_unwind;
    use std::thread;

    #[test]
    fn counts_allocations_and_growth() {
        let (_, allocated) = measure_allocated(|| black_box(Vec::<u8>::with_capacity(1000)));
        assert!(allocated >= 1000, "allocated {allocated} bytes");

        let (_, allocated) = measure_allocated(|| {
            let mut buffer = black_box(Vec::<u8>::with_capacity(1000));
            buffer.reserve_exact(3000);
            buffer
        });
        assert!(allocated >= 3000, "allocated {allocated} bytes");
    }

    #[test]
    fn ignores_other_threads() {
        let (_, allocated) = measure_allocated(|| {
            thread::spawn(|| black_box(vec![0u8; 1 << 20])).join().unwrap();
        });
        assert!(allocated < 1 << 20, "allocated {allocated} bytes");
    }

    #[test]
    fn stops_counting_after_panic() {
        catch_unwind(|| measure_allocated(|| panic!("panic inside measurement"))).unwrap_err();
        assert!(!ENABLED.get());
        let (_, allocated) = measure_allocated(|| ());
        assert_eq!(allocated, 0);
    }
}

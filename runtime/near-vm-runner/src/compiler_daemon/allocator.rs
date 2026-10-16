//! Global allocator adapter for compiler-worker memory exhaustion.
//!
//! Rust's allocation error hook is unstable, so binaries which can become a
//! compiler worker install [`ExitOnWorkerMemoryExhaustion`] as their global
//! allocator. The adapter is inert in the parent process. [`enable`] activates
//! it after the process has selected compiler-worker mode.

#[cfg(unix)]
use libc::_exit;
use std::alloc::{GlobalAlloc, Layout};
#[cfg(not(unix))]
use std::process::abort;
#[cfg(unix)]
use std::sync::atomic::{AtomicBool, Ordering};

/// Exit status reserved for an instrumented compiler-worker allocation failure.
#[doc(hidden)]
pub const WORKER_MEMORY_EXHAUSTED_EXIT_CODE: i32 = 86;

#[cfg(unix)]
static ENABLED: AtomicBool = AtomicBool::new(false);

/// A global allocator adapter which terminates an enabled compiler worker when
/// its underlying allocator cannot satisfy a nonempty allocation.
///
/// This deliberately makes even allocations made through fallible collection
/// APIs terminal in a worker. The failure path takes no locks, allocates no
/// memory, and skips unwinding and destructors.
#[doc(hidden)]
pub struct ExitOnWorkerMemoryExhaustion<A> {
    inner: A,
}

impl<A> ExitOnWorkerMemoryExhaustion<A> {
    pub const fn new(inner: A) -> Self {
        Self { inner }
    }
}

/// Enable allocation-failure interception for the rest of this process.
///
/// This is called only after the executable has selected compiler-worker mode.
pub(super) fn enable() {
    #[cfg(unix)]
    ENABLED.store(true, Ordering::Relaxed);
}

/// Terminate after a typed resource-exhaustion error which bypassed the global
/// allocator, such as Wasmtime's fallible allocation or mmap paths.
pub(super) fn exit_for_memory_exhaustion() -> ! {
    immediate_exit()
}

#[inline]
fn check_allocation(ptr: *mut u8, size: usize) {
    #[cfg(unix)]
    if ptr.is_null() && size != 0 && ENABLED.load(Ordering::Relaxed) {
        immediate_exit();
    }

    #[cfg(not(unix))]
    let _ = (ptr, size);
}

#[cold]
fn immediate_exit() -> ! {
    #[cfg(unix)]
    unsafe {
        _exit(WORKER_MEMORY_EXHAUSTED_EXIT_CODE);
    }

    #[cfg(not(unix))]
    abort();
}

// SAFETY: every operation is forwarded to `inner` with the original arguments.
// The only added behavior is immediate process termination after a null result
// for a nonzero allocation while compiler-worker interception is enabled.
unsafe impl<A: GlobalAlloc> GlobalAlloc for ExitOnWorkerMemoryExhaustion<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.inner.alloc(layout) };
        check_allocation(ptr, layout.size());
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.inner.alloc_zeroed(layout) };
        check_allocation(ptr, layout.size());
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { self.inner.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { self.inner.realloc(ptr, layout, new_size) };
        check_allocation(new_ptr, new_size);
        new_ptr
    }
}

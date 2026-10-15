//! Accounts for the heap allocated while decoding peer messages, and stops
//! decoding once a per-message budget is used up.
//!
//! [`BudgetedAlloc`] wraps the global allocator and, while a [`with_budget`]
//! scope is active on the current thread, adds every allocation's size to a
//! thread-local counter. [`decode_borsh`] reads through a reader that fails once
//! the counter exceeds the budget, checking every few reads. Borsh reads at
//! least one byte per decoded element, so decoding stops shortly after the
//! budget is exceeded.
//!
//! The bound is soft: peak usage can exceed the budget by what is allocated
//! between two checks, i.e. a vector doubling plus a few KiB per read.
//! Allocations are charged cumulatively and frees are not refunded.
//!
//! Enforcement requires `BudgetedAlloc` to be the global allocator. Without it,
//! nothing is counted and decoding is unbounded; see [`is_enforced`].

use borsh::BorshDeserialize;
#[cfg(test)]
use std::alloc::System;
use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::hint::black_box;
use std::io::{self, Read};
use std::sync::OnceLock;

thread_local! {
    /// Bytes allocated on this thread since the active budget scope began.
    /// `None` when no scope is active.
    static USED: Cell<Option<usize>> = const { Cell::new(None) };
    /// Budget of the active scope.
    static LIMIT: Cell<usize> = const { Cell::new(usize::MAX) };
}

/// Global allocator wrapper that charges allocations to the active budget.
pub struct BudgetedAlloc<A>(pub A);

#[inline]
fn charge(bytes: usize) {
    // `try_with` because the allocator can run during thread-local teardown.
    let _ = USED.try_with(|used| {
        if let Some(n) = used.get() {
            used.set(Some(n.saturating_add(bytes)));
        }
    });
}

unsafe impl<A: GlobalAlloc> GlobalAlloc for BudgetedAlloc<A> {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        charge(layout.size());
        unsafe { self.0.alloc(layout) }
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        charge(layout.size());
        unsafe { self.0.alloc_zeroed(layout) }
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { self.0.dealloc(ptr, layout) }
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        charge(new_size.saturating_sub(layout.size()));
        unsafe { self.0.realloc(ptr, layout, new_size) }
    }
}

fn exceeded() -> io::Error {
    io::Error::new(io::ErrorKind::OutOfMemory, "decode memory budget exceeded")
}

/// Whether the current thread's active budget has been exceeded.
fn over_budget() -> bool {
    USED.get().is_some_and(|used| used > LIMIT.get())
}

/// Runs `f` with a budget of `limit` bytes for allocations on this thread.
///
/// Returns an error instead of `f`'s result if `f` allocated more than `limit`
/// bytes. A nested scope cannot raise the budget of an outer one.
pub fn with_budget<T>(limit: usize, f: impl FnOnce() -> T) -> io::Result<T> {
    struct Restore {
        used: Option<usize>,
        limit: usize,
    }

    impl Drop for Restore {
        fn drop(&mut self) {
            // Charges made in the inner scope also count against the outer one.
            let inner = USED.get().unwrap_or(0);
            USED.set(self.used.map(|outer| outer.saturating_add(inner)));
            LIMIT.set(self.limit);
        }
    }

    let outer_used = USED.get();
    let outer_limit = LIMIT.get();
    let remaining = match outer_used {
        Some(used) => outer_limit.saturating_sub(used),
        None => usize::MAX,
    };
    let _restore = Restore { used: outer_used, limit: outer_limit };
    USED.set(Some(0));
    LIMIT.set(limit.min(remaining));
    let result = f();
    if over_budget() {
        return Err(exceeded());
    }
    Ok(result)
}

/// Reader that fails once the active budget is exceeded.
struct BudgetedReader<'a> {
    inner: &'a [u8],
    reads: u32,
}

/// Reads between budget checks. Checking on every read made decoding about a
/// third slower in benchmarks.
const READS_PER_CHECK: u32 = 64;

impl BudgetedReader<'_> {
    #[inline]
    fn check(&mut self) -> io::Result<()> {
        self.reads = self.reads.wrapping_add(1);
        if self.reads % READS_PER_CHECK == 0 && over_budget() {
            return Err(exceeded());
        }
        Ok(())
    }
}

impl Read for BudgetedReader<'_> {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.check()?;
        self.inner.read(buf)
    }

    // Borsh decodes fixed-size values with `read_exact`, and the default
    // implementation is a loop over `read`.
    #[inline]
    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.check()?;
        self.inner.read_exact(buf)
    }
}

/// Like `T::try_from_slice`, but stops once the active budget is exceeded.
/// Outside a [`with_budget`] scope it behaves like `T::try_from_slice`.
pub fn decode_borsh<T: BorshDeserialize>(bytes: &[u8]) -> io::Result<T> {
    let mut reader = BudgetedReader { inner: bytes, reads: 0 };
    let value = T::deserialize_reader(&mut reader)?;
    if !reader.inner.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "not all bytes read"));
    }
    Ok(value)
}

/// Whether allocations are being charged, i.e. whether [`BudgetedAlloc`] is
/// the global allocator.
pub fn is_enforced() -> bool {
    static ENFORCED: OnceLock<bool> = OnceLock::new();
    *ENFORCED.get_or_init(|| {
        let outer = USED.replace(Some(0));
        drop(black_box(Vec::<u8>::with_capacity(1)));
        USED.replace(outer).is_some_and(|used| used > 0)
    })
}

/// Unit tests in this crate run with budgets enforced, as `neard` does.
#[cfg(test)]
#[global_allocator]
static ALLOC: BudgetedAlloc<System> = BudgetedAlloc(System);

#[cfg(test)]
mod tests {
    use super::*;
    use borsh::{BorshSerialize, to_vec};
    use std::collections::HashMap;
    use std::panic;
    use std::rc::Rc;

    const BUDGET: usize = 1 << 20;

    /// Encodes `value` with borsh.
    fn encode<T: BorshSerialize>(value: &T) -> Vec<u8> {
        to_vec(value).unwrap()
    }

    /// Decodes `bytes` as `T` within `BUDGET`.
    fn decode_in_budget<T: BorshDeserialize>(bytes: &[u8]) -> io::Result<T> {
        with_budget(BUDGET, || decode_borsh::<T>(bytes)).and_then(|r| r)
    }

    fn assert_exceeded<T>(result: io::Result<T>) {
        assert_eq!(result.err().unwrap().kind(), io::ErrorKind::OutOfMemory);
    }

    #[test]
    fn enforced_in_tests() {
        assert!(is_enforced());
    }

    #[test]
    fn bytes_over_budget_are_rejected() {
        let bytes = encode(&vec![7u8; 2 * BUDGET]);
        assert_exceeded(decode_in_budget::<Vec<u8>>(&bytes));
    }

    #[test]
    fn boxes_over_budget_are_rejected() {
        let bytes = encode(&vec![[7u8; 1024]; 2 * BUDGET / 1024]);
        assert_exceeded(decode_in_budget::<Vec<Box<[u8; 1024]>>>(&bytes));
        assert_exceeded(decode_in_budget::<Vec<Rc<[u8; 1024]>>>(&bytes));
    }

    #[test]
    fn map_over_budget_is_rejected() {
        let map: HashMap<u32, Vec<u8>> =
            (0..2 * BUDGET as u32 / 1024).map(|i| (i, vec![7; 1024])).collect();
        assert_exceeded(decode_in_budget::<HashMap<u32, Vec<u8>>>(&encode(&map)));
    }

    #[test]
    fn value_within_budget_decodes() {
        let bytes = encode(&vec![7u8; 1000]);
        assert_eq!(decode_in_budget::<Vec<u8>>(&bytes).unwrap().len(), 1000);
    }

    #[test]
    fn decode_without_scope_is_unbounded() {
        let bytes = encode(&vec![7u8; 2 * BUDGET]);
        assert_eq!(decode_borsh::<Vec<u8>>(&bytes).unwrap().len(), 2 * BUDGET);
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut bytes = encode(&vec![7u8; 10]);
        bytes.push(0);
        let err = decode_borsh::<Vec<u8>>(&bytes).err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn allocations_outside_decoding_are_charged() {
        assert_exceeded(with_budget(1024, || Vec::<u8>::with_capacity(4096)));
    }

    #[test]
    fn nested_scope_cannot_raise_budget_and_charges_outer() {
        let result = with_budget(10_000, || {
            let inner = with_budget(usize::MAX, || Vec::<u8>::with_capacity(6_000));
            assert!(inner.is_ok());
            // The inner 6 KB count against the outer 10 KB budget.
            with_budget(usize::MAX, || Vec::<u8>::with_capacity(6_000))
        });
        assert_exceeded(result);
        assert_eq!(USED.get(), None);
    }

    #[test]
    fn scope_is_reset_after_panic() {
        let result = panic::catch_unwind(|| with_budget(1024, || -> () { panic!("boom") }));
        assert!(result.is_err());
        assert_eq!(USED.get(), None);
        assert_eq!(LIMIT.get(), usize::MAX);
    }
}

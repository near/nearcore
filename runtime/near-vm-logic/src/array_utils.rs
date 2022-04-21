//! Polyfils for missing const-generic array APIs in the std.

use std::slice::ChunksExact;

/// Splits `&[u8; A + B]` into `(&[u8; A], &[u8; B])`.
pub(crate) fn split_array<const N: usize, const L: usize, const R: usize>(
    xs: &[u8; N],
) -> (&[u8; L], &[u8; R]) {
    let () = AssertEqSum::<N, L, R>::OK;

    let (left, right) = xs.split_at(L);
    (left.try_into().unwrap(), right.try_into().unwrap())
}

/// Joins `[u8; A]` and `[u8; B]` into `[u8; A + B]`.
pub(crate) fn join_array<const N: usize, const L: usize, const R: usize>(
    left: [u8; L],
    right: [u8; R],
) -> [u8; N] {
    let () = AssertEqSum::<N, L, R>::OK;

    let mut res = [0; N];
    let (l, r) = res.split_at_mut(L);
    l.copy_from_slice(&left);
    r.copy_from_slice(&right);
    res
}

/// Converts an `&[u8]` slice of length `N * k` into iterator of `k` `[u8; N]`
/// chunks.
pub(crate) struct ArrayChunks<'a, const N: usize> {
    inner: ChunksExact<'a, u8>,
}

impl<'a, const N: usize> ArrayChunks<'a, N> {
    pub(crate) fn new(bytes: &'a [u8]) -> Result<ArrayChunks<'a, N>, ()> {
        let inner = bytes.chunks_exact(N);
        if !inner.remainder().is_empty() {
            return Err(());
        }
        Ok(ArrayChunks { inner })
    }
}

impl<'a, const N: usize> Iterator for ArrayChunks<'a, N> {
    type Item = &'a [u8; N];

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|it| it.try_into().unwrap())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<'a, const N: usize> ExactSizeIterator for ArrayChunks<'a, N> {}

/// Asserts, at compile time, that `S == A + B`.
struct AssertEqSum<const S: usize, const A: usize, const B: usize>;
impl<const S: usize, const A: usize, const B: usize> AssertEqSum<S, A, B> {
    const OK: () = [()][A + B - S];
}

//! Polyfils for missing const-generic array APIs in the std.

use std::slice::ChunksExact;

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

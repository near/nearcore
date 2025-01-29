//! `stdx` crate contains polyfills which should really be in std,
//! but currently aren't for one reason or another.
#![deny(clippy::arithmetic_side_effects)]

// TODO(mina86): Replace usage of the split functions by split_array_ref et al
// methods of array and slice types once those are stabilized.

/// Splits `&[u8; L + R]` into `(&[u8; L], &[u8; R])`.
pub fn split_array<const N: usize, const L: usize, const R: usize>(
    xs: &[u8; N],
) -> (&[u8; L], &[u8; R]) {
    const {
        if N != L + R {
            panic!()
        }
    };
    let (left, right) = xs.split_at(L);
    (left.try_into().unwrap(), right.try_into().unwrap())
}

/// Splits `&mut [u8; L + R]` into `(&mut [u8; L], &mut [u8; R])`.
pub fn split_array_mut<const N: usize, const L: usize, const R: usize>(
    xs: &mut [u8; N],
) -> (&mut [u8; L], &mut [u8; R]) {
    const {
        if N != L + R {
            panic!()
        }
    };

    let (left, right) = xs.split_at_mut(L);
    (left.try_into().unwrap(), right.try_into().unwrap())
}

#[test]
fn test_split() {
    assert_eq!((&[0, 1], &[2, 3, 4]), split_array(&[0, 1, 2, 3, 4]));
    assert_eq!((&mut [0, 1], &mut [2, 3, 4]), split_array_mut(&mut [0, 1, 2, 3, 4]));
}

/// Joins `[u8; L]` and `[u8; R]` into `[u8; L + R]`.
pub fn join_array<const N: usize, const L: usize, const R: usize>(
    left: [u8; L],
    right: [u8; R],
) -> [u8; N] {
    const {
        if N != L + R {
            panic!()
        }
    };

    let mut res = [0; N];
    let (l, r) = res.split_at_mut(L);
    l.copy_from_slice(&left);
    r.copy_from_slice(&right);
    res
}

#[test]
fn test_join() {
    assert_eq!([0, 1, 2, 3], join_array([0, 1], [2, 3]));
}

/// Splits a slice into a slice of N-element arrays.
// TODO(mina86): Replace with [T]::as_chunks once that’s stabilized.
pub fn as_chunks<const N: usize, T>(slice: &[T]) -> (&[[T; N]], &[T]) {
    const {
        if N == 0 {
            panic!()
        }
    };

    let len = slice.len().checked_div(N).expect("static assert above ensures N ≠ 0");
    let (head, tail) = slice
        .split_at(len.checked_mul(N).expect("len * N ≤ slice.len() hence can't overflow here"));

    // SAFETY: We cast a slice of `len * N` elements into a slice of `len` many
    // `N` elements chunks.
    let head = unsafe { std::slice::from_raw_parts(head.as_ptr().cast(), len) };
    (head, tail)
}

#[derive(Debug, Eq, PartialEq)]
pub struct InexactChunkingError {
    slice_len: usize,
    chunk_size: usize,
}
impl std::error::Error for InexactChunkingError {}
impl std::fmt::Display for InexactChunkingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "slice of size {} cannot be precisely split into chunks of size {}",
            self.slice_len, self.chunk_size
        )
    }
}

/// Like `as_chunks` but returns an error if there’s a remainder.
pub fn as_chunks_exact<const N: usize, T>(slice: &[T]) -> Result<&[[T; N]], InexactChunkingError> {
    let (chunks, remainder) = as_chunks(slice);
    if remainder.is_empty() {
        Ok(chunks)
    } else {
        Err(InexactChunkingError { slice_len: slice.len(), chunk_size: N })
    }
}

#[test]
fn test_as_chunks() {
    assert_eq!((&[[0, 1], [2, 3]][..], &[4][..]), as_chunks::<2, _>(&[0, 1, 2, 3, 4]));
    assert_eq!(Ok(&[[0, 1], [2, 3]][..]), as_chunks_exact::<2, _>(&[0, 1, 2, 3]));
    assert_eq!(
        Err(InexactChunkingError { slice_len: 5, chunk_size: 2 }),
        as_chunks_exact::<2, _>(&[0, 1, 2, 3, 4])
    );
}

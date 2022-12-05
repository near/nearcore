//! `stdx` crate contains polyfills which should really be in std,
//! but currently aren't for one reason or another.

// TODO(mina86): Replace usage of the split functions by split_array_ref et al
// methods of array and slice types once those are stabilised.

/// Splits `&[u8; L + R]` into `(&[u8; L], &[u8; R])`.
pub fn split_array<const N: usize, const L: usize, const R: usize>(
    xs: &[u8; N],
) -> (&[u8; L], &[u8; R]) {
    #[allow(clippy::let_unit_value)]
    let () = AssertEqSum::<N, L, R>::OK;

    let (left, right) = xs.split_at(L);
    (left.try_into().unwrap(), right.try_into().unwrap())
}

/// Splits `&mut [u8; L + R]` into `(&mut [u8; L], &mut [u8; R])`.
pub fn split_array_mut<const N: usize, const L: usize, const R: usize>(
    xs: &mut [u8; N],
) -> (&mut [u8; L], &mut [u8; R]) {
    #[allow(clippy::let_unit_value)]
    let () = AssertEqSum::<N, L, R>::OK;

    let (left, right) = xs.split_at_mut(L);
    (left.try_into().unwrap(), right.try_into().unwrap())
}

/// Splits `&[u8]` into `(&[u8; N], &[u8])`.  **Panics** if slice is shorter
/// than `N`.
pub fn split_slice<const N: usize>(slice: &[u8]) -> (&[u8; N], &[u8]) {
    let (head, tail) = slice.split_at(N);
    (head.try_into().unwrap(), tail)
}

/// Splits `&[u8]` into `(&[u8], &[u8; N])`.  **Panics** if slice is shorter
/// than `N`.
pub fn rsplit_slice<const N: usize>(slice: &[u8]) -> (&[u8], &[u8; N]) {
    let index = slice.len().checked_sub(N).expect("len to be ≥ N");
    let (head, tail) = slice.split_at(index);
    (head, tail.try_into().unwrap())
}

/// Splits `&[u8]` into `(&[u8; N], &[u8])`.  **Panics** if slice is shorter
/// than `N`.
pub fn split_slice_mut<const N: usize>(slice: &mut [u8]) -> (&mut [u8; N], &mut [u8]) {
    let (head, tail) = slice.split_at_mut(N);
    (head.try_into().unwrap(), tail)
}

/// Splits `&[u8]` into `(&[u8], &[u8; N])`.  **Panics** if slice is shorter
/// than `N`.
pub fn rsplit_slice_mut<const N: usize>(slice: &mut [u8]) -> (&mut [u8], &mut [u8; N]) {
    let index = slice.len().checked_sub(N).expect("len to be ≥ N");
    let (head, tail) = slice.split_at_mut(index);
    (head, tail.try_into().unwrap())
}

#[test]
fn test_split() {
    assert_eq!((&[0, 1], &[2, 3, 4]), split_array(&[0, 1, 2, 3, 4]));
    assert_eq!((&mut [0, 1], &mut [2, 3, 4]), split_array_mut(&mut [0, 1, 2, 3, 4]));

    assert_eq!((&[0, 1], &[2, 3, 4][..]), split_slice(&[0, 1, 2, 3, 4]));
    assert_eq!((&[0, 1][..], &[2, 3, 4]), rsplit_slice(&[0, 1, 2, 3, 4]));
    assert_eq!((&mut [0, 1], &mut [2, 3, 4][..]), split_slice_mut(&mut [0, 1, 2, 3, 4]));
    assert_eq!((&mut [0, 1][..], &mut [2, 3, 4]), rsplit_slice_mut(&mut [0, 1, 2, 3, 4]));
}

/// Joins `[u8; L]` and `[u8; R]` into `[u8; L + R]`.
pub fn join_array<const N: usize, const L: usize, const R: usize>(
    left: [u8; L],
    right: [u8; R],
) -> [u8; N] {
    #[allow(clippy::let_unit_value)]
    let () = AssertEqSum::<N, L, R>::OK;

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
// TODO(mina86): Replace with [T]::as_chunks once that’s stabilised.
pub fn as_chunks<const N: usize, T>(slice: &[T]) -> (&[[T; N]], &[T]) {
    #[allow(clippy::let_unit_value)]
    let () = AssertNonZero::<N>::OK;

    let len = slice.len() / N;
    let (head, tail) = slice.split_at(len * N);

    // SAFETY: We cast a slice of `len * N` elements into a slice of `len` many
    // `N` elements chunks.
    let head = unsafe { std::slice::from_raw_parts(head.as_ptr().cast(), len) };
    (head, tail)
}

/// Chunks if `as_chunks` has no remainder.
pub fn as_chunks_exact<const N: usize, T>(slice: &[T]) -> Option<&[[T; N]]> {
    let (chunks, remainder) = as_chunks(slice);
    if remainder.is_empty() {
        Some(chunks)
    } else {
        None
    }
}

#[test]
fn test_as_chunks() {
    assert_eq!((&[[0, 1], [2, 3]][..], &[4][..]), as_chunks::<2, _>(&[0, 1, 2, 3, 4]));
    assert_eq!(Some(&[[0, 1], [2, 3]][..]), as_chunks_exact::<2, _>(&[0, 1, 2, 3]));
    assert_eq!(None, as_chunks_exact::<2, _>(&[0, 1, 2, 3, 4]));
}

/// Asserts, at compile time, that `S == A + B`.
struct AssertEqSum<const S: usize, const A: usize, const B: usize>;
impl<const S: usize, const A: usize, const B: usize> AssertEqSum<S, A, B> {
    const OK: () = static_assert(S == A + B);
}

/// Asserts, at compile time, that `N` is non-zero.
struct AssertNonZero<const N: usize>;
impl<const N: usize> AssertNonZero<N> {
    const OK: () = static_assert(N > 0);
}

const fn static_assert(ok: bool) {
    // Array with a single unit element
    let arr = [()];
    // index == 0 ↔ ok == true
    let index = !ok as usize;
    // the following expression will fail with
    // "index out of bounds" if ok is false
    arr[index]
}

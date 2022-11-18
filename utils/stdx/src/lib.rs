//! `stdx` crate contains polyfills which should really be in std,
//! but currently aren't for one reason or another.

// TODO(mina86): Replace usage of the split functions by split_array_ref et al
// methods of array and slice types once those are stabilised.

/// Splits `&[u8; L + R]` into `(&[u8; L], &[u8; R])`.
pub fn split_array<const N: usize, const L: usize, const R: usize>(
    xs: &[u8; N],
) -> (&[u8; L], &[u8; R]) {
    let () = AssertEqSum::<N, L, R>::OK;

    let (left, right) = xs.split_at(L);
    (left.try_into().unwrap(), right.try_into().unwrap())
}

/// Splits `&mut [u8; L + R]` into `(&mut [u8; L], &mut [u8; R])`.
pub fn split_array_mut<const N: usize, const L: usize, const R: usize>(
    xs: &mut [u8; N],
) -> (&mut [u8; L], &mut [u8; R]) {
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

/// Asserts, at compile time, that `S == A + B`.
struct AssertEqSum<const S: usize, const A: usize, const B: usize>;
impl<const S: usize, const A: usize, const B: usize> AssertEqSum<S, A, B> {
    const OK: () = [()][A + B - S];
}

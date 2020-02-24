use crate::util::{Packable, Point, Scalar};
use arrayref::array_ref;
use blake2::VarBlake2b;
use digest::generic_array::{typenum::U32, GenericArray};
use digest::{BlockInput, FixedOutput, Input, Reset, VariableOutput};

pub use blake2::Blake2b as Hash512;

#[derive(Clone)]
pub struct Hash256(VarBlake2b);

impl Default for Hash256 {
    fn default() -> Self {
        Hash256(VarBlake2b::new(32).unwrap())
    }
}

impl Input for Hash256 {
    fn input<B: AsRef<[u8]>>(&mut self, data: B) {
        self.0.input(data);
    }
}

impl BlockInput for Hash256 {
    type BlockSize = <VarBlake2b as BlockInput>::BlockSize;
}

impl FixedOutput for Hash256 {
    type OutputSize = U32;

    fn fixed_result(self) -> GenericArray<u8, U32> {
        let mut r = [0; 32];
        self.0.variable_result(|s| {
            r = *array_ref!(s, 0, 32);
        });
        r.into()
    }
}

impl Reset for Hash256 {
    fn reset(&mut self) {
        self.0.reset();
    }
}

mod hashable_trait {
    pub trait Hashable {
        fn hash_into<D: super::Input>(self, digest: D) -> D;
    }
}

use hashable_trait::*;

impl<T: AsRef<[u8]> + ?Sized> Hashable for &T {
    fn hash_into<D: Input>(self, digest: D) -> D {
        digest.chain(self.as_ref())
    }
}

impl Hashable for Point {
    fn hash_into<D: Input>(self, digest: D) -> D {
        digest.chain(&self.pack())
    }
}

impl Hashable for Scalar {
    fn hash_into<D: Input>(self, digest: D) -> D {
        digest.chain(&self.pack())
    }
}

pub fn _hash_new<D: Default>() -> D {
    D::default()
}

pub fn _hash_chain<D: Input, T: Hashable>(digest: D, data: T) -> D {
    data.hash_into(digest)
}

pub fn _hash_result<D: FixedOutput<OutputSize = U32>>(digest: D) -> [u8; 32] {
    digest.fixed_result().into()
}

pub fn _hash_to_scalar(hash: [u8; 32]) -> Scalar {
    Scalar::from_bytes_mod_order(hash)
}

macro_rules! hash_chain {
    ($h:expr, $d:expr $(, $dd:expr)*) => {
        hash_chain!($crate::hash::_hash_chain($h, $d) $(, $dd)*)
    };
    ($h:expr) => {
        $h
    };
}

macro_rules! hash {
    ($($d:expr),*) => {
        $crate::hash::_hash_result(hash_chain!($crate::hash::_hash_new::<$crate::hash::Hash256>() $(, $d)*))
    };
}

macro_rules! hash_s {
    ($($d:expr),*) => {
        $crate::hash::_hash_to_scalar(hash!($($d),*))
    };
}

pub fn _prs_result(digest: Hash512) -> Scalar {
    let res = digest.fixed_result();
    Scalar::from_bytes_mod_order_wide(array_ref!(res, 0, 64))
}

macro_rules! prs {
    ($($d:expr),*) => {
        $crate::hash::_prs_result(hash_chain!($crate::hash::_hash_new::<$crate::hash::Hash512>() $(, $d)*))
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use curve25519_dalek::constants::RISTRETTO_BASEPOINT_POINT as G;
    use hex_literal::hex;

    #[test]
    fn test_hashes() {
        assert_eq!(
            hash!(),
            hex!("0e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a8")
        );
        assert_eq!(hash!(b""), hash!());
        assert_eq!(hash!(b"", b""), hash!());
        assert_eq!(
            hash_s!(),
            Scalar::from_canonical_bytes(hex!(
                "cc0fb71e1f068c41898b8252aed624d1d0e5df47778f7787faab45cdf12fe308"
            ))
            .unwrap()
        );
        assert_eq!(
            prs!(),
            Scalar::from_canonical_bytes(hex!(
                "4d31ff252ec727ffb194a0557482c659e4376e76e8148134678460cb24223e06"
            ))
            .unwrap()
        );
        assert_eq!(
            hash!(hash_s!()),
            hex!("f32d6e5c532a11cee4ce38370622441ad181b72e3d68f042736e6ba3434c5b77")
        );
        assert_eq!(
            hash!(G),
            hex!("0993bca60aa601325f1dc1959caf9ab0453cd395a2ad8229c7221d70d0904f0f")
        );
    }
}

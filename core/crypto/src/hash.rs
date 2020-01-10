use crate::curve::{Packable, Point, Scalar};
use blake2::VarBlake2b;
use digest::{BlockInput, FixedOutput, Input, Reset, VariableOutput};
use generic_array::GenericArray;
use typenum::U32;

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

mod hash_traits {
    pub trait Chainable {
        fn chain_into<D: super::Input>(self, digest: D) -> D;
    }

    pub trait Result {
        fn from_value(value: [u8; 32]) -> Self;
    }
}

use hash_traits::*;

pub fn _hash_new<D: Default>() -> D {
    D::default()
}

impl<T: AsRef<[u8]> + ?Sized> Chainable for &T {
    fn chain_into<D: Input>(self, digest: D) -> D {
        digest.chain(self.as_ref())
    }
}

impl Chainable for Point {
    fn chain_into<D: Input>(self, digest: D) -> D {
        digest.chain(&self.pack())
    }
}

impl Chainable for Scalar {
    fn chain_into<D: Input>(self, digest: D) -> D {
        digest.chain(&self.pack())
    }
}

pub fn _hash_chain<D: Input, T: Chainable>(digest: D, data: T) -> D {
    data.chain_into(digest)
}

impl Result for [u8; 32] {
    fn from_value(value: [u8; 32]) -> Self {
        value
    }
}

impl Result for Scalar {
    fn from_value(value: [u8; 32]) -> Self {
        Scalar::from_bytes_mod_order(value)
    }
}

pub fn _hash_result<D: FixedOutput<OutputSize = U32>, R: Result>(digest: D) -> R {
    R::from_value(digest.fixed_result().into())
}

pub fn _hash_s_result<D: FixedOutput<OutputSize = U32>>(digest: D) -> Scalar {
    _hash_result(digest)
}

macro_rules! hash_chain {
    ($h:expr, $d:expr $(, $dd:expr)*) => {
        hash_chain!($crate::hash::_hash_chain($h, $d) $(, $dd)*)
    };
    ($h:expr) => {
        $h
    };
}

macro_rules! hasher {
    ($h:ty $(, $d:expr)*) => {
        hash_chain!($crate::hash::_hash_new::<$h>() $(, $d)*)
    };
}

macro_rules! hash {
    ($($d:expr),*) => {
        $crate::hash::_hash_result(hasher!($crate::hash::Hash256 $(, $d)*))
    };
}

macro_rules! hash_s {
    ($($d:expr),*) => {
        $crate::hash::_hash_s_result(hasher!($crate::hash::Hash256 $(, $d)*))
    };
}

pub fn _prs_result(digest: Hash512) -> Scalar {
    let res = digest.fixed_result();
    Scalar::from_bytes_mod_order_wide(array_ref!(res, 0, 64))
}

macro_rules! prs {
    ($($d:expr),*) => {{
        $crate::hash::_prs_result(hasher!($crate::hash::Hash512, $($d),*))
    }};
}

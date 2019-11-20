use blake2::VarBlake2b;
use curve25519_dalek::scalar::Scalar;
use digest::{BlockInput, FixedOutput, Input, Reset, VariableOutput};
use generic_array::GenericArray;
use typenum::U32;

pub use blake2::Blake2b as Blake2b512;

#[derive(Clone)]
pub struct Blake2b256(VarBlake2b);

impl Default for Blake2b256 {
    fn default() -> Self {
        Blake2b256(VarBlake2b::new(32).unwrap())
    }
}

impl Input for Blake2b256 {
    fn input<B: AsRef<[u8]>>(&mut self, data: B) {
        self.0.input(data);
    }
}

impl BlockInput for Blake2b256 {
    type BlockSize = <VarBlake2b as BlockInput>::BlockSize;
}

impl FixedOutput for Blake2b256 {
    type OutputSize = U32;

    fn fixed_result(self) -> GenericArray<u8, U32> {
        let mut r = [0; 32];
        self.0.variable_result(|s| {
            r = *array_ref!(s, 0, 32);
        });
        r.into()
    }
}

impl Reset for Blake2b256 {
    fn reset(&mut self) {
        self.0.reset();
    }
}

pub trait ToScalar {
    fn result_scalar(self) -> Scalar;
}

impl ToScalar for Blake2b256 {
    fn result_scalar(self) -> Scalar {
        Scalar::from_bytes_mod_order(self.fixed_result().into())
    }
}

impl ToScalar for Blake2b512 {
    fn result_scalar(self) -> Scalar {
        Scalar::from_hash(self)
    }
}

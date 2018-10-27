use super::types;

pub trait VerifierLike {
   fn compute_state(&mut self, transactions: &[types::StatedTransaction]) -> types::State;
}

// encode a type to byte array
pub trait Encode {
    fn encode(&self) -> Option<Vec<u8>>;
}

// decode from byte array
pub trait Decode: Sized {
    fn decode(data: &[u8]) -> Option<Self>;
}
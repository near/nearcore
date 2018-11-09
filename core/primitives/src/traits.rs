use super::types;
use std::hash;

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

/// trait that abstracts ``Header"
pub trait Header: Clone + Send + Sync + Encode + Decode + Eq + 'static {
    type Hash: hash::Hash;
    // TODO: add methods
    fn hash(&self) -> Self::Hash;
}

/// trait that abstracts ``block", ideally could be used for both beacon-chain blocks
/// and shard-chain blocks
pub trait Block: Clone + Send + Sync + Encode + Decode + Eq + 'static {
    type Header;
    type Body;
    type Hash: hash::Hash;

    fn header(&self) -> &Self::Header;
    fn body(&self) -> &Self::Body;
    fn deconstruct(self) -> (Self::Header, Self::Body);
    fn new(header: Self::Header, body: Self::Body) -> Self;
    fn hash(&self) -> Self::Hash;
}
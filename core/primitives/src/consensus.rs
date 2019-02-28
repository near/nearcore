use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;

pub use super::serialize::{Decode, Encode};
use super::types;

pub trait WitnessSelector: 'static {
    fn epoch_witnesses(&self, epoch: u64) -> &HashSet<types::UID>;
    fn epoch_leader(&self, epoch: u64) -> types::UID;
    /// Random sample of witnesses. Should exclude the current witness.
    fn random_witnesses(&self, epoch: u64, sample_size: usize) -> HashSet<types::UID>;
}

pub type GenericResult = Result<(), &'static str>;

/// General payload that can be stored on TxFlow. Should either not have references,
/// or the references should live for static lifetime.
pub trait Payload: Clone + Send + Hash + Debug + Encode + Decode + 'static {
    fn verify(&self) -> GenericResult;
    // Merge content from another payload into this one.
    fn union_update(&mut self, other: Self);
    fn is_empty(&self) -> bool;
    // Creates empty payload.
    fn new() -> Self;
}

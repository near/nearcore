use std::collections::HashSet;
use std::hash::Hash;

use super::types;

pub trait Verifier {
    fn compute_state(&mut self, transactions: &[types::StatedTransaction]) -> types::State;
}

pub trait WitnessSelector {
    fn epoch_witnesses(&self, epoch: u64) -> &HashSet<u64>;
    fn epoch_leader(&self, epoch: u64) -> u64;
}

pub trait Payload: Hash {
    fn verify(&self) -> Result<(), &'static str>;
}
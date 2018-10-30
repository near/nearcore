use std::collections::HashSet;

use super::types;

pub trait VerifierLike {
   fn compute_state(&mut self, transactions: &[types::StatedTransaction]) -> types::State;
}

pub trait WitnessSelectorLike {
   fn epoch_witnesses(&self, epoch: u64) -> &HashSet<u64>;
   fn epoch_leader(&self, epoch: u64) -> u64;
}
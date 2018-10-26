use std::collections::HashSet;

use super::types;

pub trait VerifierLike {
   fn compute_state(&mut self, transactions: &[types::StatedTransaction]) -> types::State;
}

pub trait WitnessSelector {
   fn epoch_witnesses(&self) -> &HashSet<u64>;
}
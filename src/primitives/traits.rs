use super::types;

pub trait VerifierLike {
   fn compute_state(&mut self, transactions: &[types::StatedTransaction]) -> types::State;
}
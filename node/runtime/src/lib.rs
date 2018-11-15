extern crate primitives;

use primitives::types::{MerkleHash, StatedTransaction};
use primitives::traits::{StateDbView, StateTransitionRuntime};

// TODO: waiting for storage::state_view
pub struct StateDbViewMock {}

impl StateDbView for StateDbViewMock {
    fn merkle_root(&self) -> MerkleHash { 0 }
    fn get(&self, key: String) -> String { "".to_string() }
    fn set(&mut self, key: String, value: String) {}
    fn delete(&mut self, key: String) {}
    fn finish(&self) -> Self { StateDbViewMock {} }
}

pub struct Runtime {}

/// TODO: runtime must include balance / staking / WASM modules.
impl Runtime {
    fn apply_transaction(&self, state_view: &mut StateDbViewMock, transaction: &StatedTransaction) -> bool {
        // TODO: validate if transaction is correctly signed.
        let sender_value = state_view.get(transaction.transaction.body.sender_uid.to_string()).parse::<u64>().unwrap();
        let receiver_value = state_view.get(transaction.transaction.body.receiver_uid.to_string()).parse::<u64>().unwrap();
        if sender_value >= transaction.transaction.body.amount {
            state_view.set(
                transaction.transaction.body.sender_uid.to_string(),
                (sender_value - transaction.transaction.body.amount).to_string());
            state_view.set(
                transaction.transaction.body.receiver_uid.to_string(),
                (receiver_value + transaction.transaction.body.amount).to_string());
            true
        } else {
            false
        }
    }
}

impl StateTransitionRuntime for Runtime {
    type StateDbView = StateDbViewMock;
    fn apply(&self, state_view: &mut StateDbViewMock, transactions: &Vec<StatedTransaction>) -> (Vec<StatedTransaction>, StateDbViewMock) {
        let mut filtered_transactions: Vec<StatedTransaction> = Vec::new();
        for t in transactions {
            if self.apply_transaction(state_view, t) {
                filtered_transactions.push((*t).clone());
            }
        }
        (filtered_transactions, state_view.finish())
    }
}
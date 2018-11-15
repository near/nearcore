extern crate primitives;

use primitives::types::StatedTransaction;
use primitives::traits::{StateDbView, StateTransitionRuntime};

// TODO: waiting for storage::state_view
pub struct StateDbViewMock {}

impl StateDbView for StateDbViewMock {
    fn get(&self, key: String) -> String { "".to_string() }
    fn set(&mut self, key: String, value: String) {}
    fn delete(&mut self, key: String) {}
    fn finish(&self) -> Self { StateDbViewMock {} }
}

pub struct Runtime {}

impl Runtime {
}

impl StateTransitionRuntime for Runtime {
    type StateDbView = StateDbViewMock;
    fn apply(&self, state_view: &StateDbViewMock, transactions: &Vec<StatedTransaction>) -> (Vec<StatedTransaction>, StateDbViewMock) {
        // TODO: runtime must include balance / staking / WASM modules.
        let mut filtered_transactions: Vec<StatedTransaction> = Vec::new();
        for t in transactions {
            let value = state_view.get(t.transaction.body.sender_uid.to_string());
            filtered_transactions.push((*t).clone());
        }
        (filtered_transactions, state_view.finish())
    }
}
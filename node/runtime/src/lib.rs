extern crate primitives;

use primitives::types::StatedTransaction;
use primitives::traits::{StateDbView, StateTransitionRuntime};

pub struct Runtime {}

impl Runtime {
}

pub struct StateDbViewMock {}

impl StateDbView for StateDbViewMock {
    fn get(&self, key: String) -> String { "".to_string() }
    fn set(&mut self, key: String, value: String) {}
    fn delete(&mut self, key: String) {}
    fn finish(&self) -> Self { StateDbViewMock {} }
}

impl StateTransitionRuntime for Runtime {
    type StateDbView = StateDbViewMock;
    fn apply(&self, state_view: &StateDbViewMock, transactions: &[StatedTransaction]) -> StateDbViewMock {
        // TODO: runtime must include balance / staking / WASM modules.
        for t in transactions {
            let value = state_view.get(t.transaction.body.sender_uid.to_string());
        }
        state_view.finish()
    }
}
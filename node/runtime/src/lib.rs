extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate bincode;
#[macro_use]
extern crate log;

extern crate primitives;

use primitives::signature::{PublicKey};
use primitives::types::{DBValue, AccountId, MerkleHash, StatedTransaction};
use primitives::traits::{GenericResult, StateDbView, StateTransitionRuntime};

// TODO: waiting for storage::state_view
pub struct StateDbViewMock {}

impl StateDbView for StateDbViewMock {
    fn merkle_root(&self) -> MerkleHash { 0 }
    fn get(&self, key: String) -> Option<DBValue> { Some(vec![]) }
    fn set(&mut self, key: String, value: DBValue) {}
    fn delete(&mut self, key: String) {}
    fn finish(&self) -> Self { StateDbViewMock {} }
}

#[derive(Serialize, Deserialize)]
struct Account {
    pub public_keys: Vec<PublicKey>,
    pub nonce: u64,
    pub amount: u64,
}

pub struct Runtime {}

/// TODO: runtime must include balance / staking / WASM modules.
impl Runtime {
    fn get_account(&self, state_view: &StateDbViewMock, account_key: AccountId) -> Option<Account> {
        match state_view.get(account_key.to_string()) {
            Some(data) => match bincode::deserialize(&data) {
                Ok(s) => Some(s),
                Err(e) => {
                    error!("error occurred while decoding: {:?}", e);
                    None
                }
            }
            None => None
        }
    }
    fn set_account(&self, state_view: &mut StateDbViewMock, account_key: AccountId, account: Account) {
        match bincode::serialize(&account) {
            Ok(data) => state_view.set(account_key.to_string(), data),
            Err(e) => {
                error!("error occurred while encoding: {:?}", e);
            }
        }
    }
    fn apply_transaction(&self, state_view: &mut StateDbViewMock, transaction: &StatedTransaction) -> bool {
        match self.get_account(state_view, transaction.transaction.body.sender) {
            Some(mut sender) => match self.get_account(state_view, transaction.transaction.body.receiver) {
                Some(mut receiver) => {
                    sender.amount -= transaction.transaction.body.amount;
                    sender.nonce += 1;
                    receiver.amount += transaction.transaction.body.amount;
                    self.set_account(state_view, transaction.transaction.body.sender, sender);
                    self.set_account(state_view, transaction.transaction.body.sender, receiver);
                    true
                }
                None => false
            }
            None => false
        }
    }
}

impl StateTransitionRuntime for Runtime {
    type StateDbView = StateDbViewMock;
    fn apply(&self, state_view: &mut StateDbViewMock, transactions: &Vec<StatedTransaction>) -> (Vec<StatedTransaction>, StateDbViewMock) {
        let mut filtered_transactions = vec![];
        for t in transactions {
            if self.apply_transaction(state_view, t) {
                filtered_transactions.push((*t).clone());
            }
        }
        (filtered_transactions, state_view.finish())
    }
}
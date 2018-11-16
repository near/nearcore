extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate bincode;
#[macro_use]
extern crate log;

extern crate byteorder;
extern crate primitives;

use byteorder::{LittleEndian, WriteBytesExt};
use primitives::signature::PublicKey;
use primitives::traits::{StateDbView, StateTransitionRuntime};
use primitives::types::{AccountId, DBValue, MerkleHash, StatedTransaction};

// TODO: waiting for storage::state_view
pub struct StateDbViewMock {}

impl StateDbView for StateDbViewMock {
    fn merkle_root(&self) -> MerkleHash {
        0
    }
    fn get(&self, _key: &[u8]) -> Option<DBValue> {
        Some(vec![])
    }
    fn set(&mut self, _key: &[u8], _value: DBValue) {}
    fn delete(&mut self, _key: &[u8]) {}
    fn commit(&mut self) {}
    fn rollback(&mut self) {}
    fn finish(&self) -> Self {
        StateDbViewMock {}
    }
}

#[derive(Serialize, Deserialize)]
struct Account {
    pub public_keys: Vec<PublicKey>,
    pub nonce: u64,
    pub amount: u64,
}

#[derive(Default)]
pub struct Runtime {}

fn account_id_to_bytes(account_key: AccountId) -> Vec<u8> {
    let mut bytes = vec![];
    bytes
        .write_u64::<LittleEndian>(account_key)
        .expect("writing to bytes failed");
    bytes
}

/// TODO: runtime must include balance / staking / WASM modules.
impl Runtime {
    fn get_account<S: StateDbView>(
        &self,
        state_view: &S,
        account_key: AccountId,
    ) -> Option<Account> {
        state_view
            .get(&account_id_to_bytes(account_key))
            .and_then(|data| bincode::deserialize(&data).ok())
    }

    fn set_account<S: StateDbView>(
        &self,
        state_view: &mut S,
        account_key: AccountId,
        account: &Account,
    ) {
        match bincode::serialize(&account) {
            Ok(data) => state_view.set(&account_id_to_bytes(account_key), data),
            Err(e) => {
                error!("error occurred while encoding: {:?}", e);
            }
        }
    }
    fn apply_transaction<S: StateDbView>(
        &self,
        state_view: &mut S,
        transaction: &StatedTransaction,
    ) -> bool {
        let sender = self.get_account(state_view, transaction.transaction.body.sender);
        let receiver = self.get_account(state_view, transaction.transaction.body.receiver);
        match (sender, receiver) {
            (Some(mut sender), Some(mut receiver)) => {
                sender.amount -= transaction.transaction.body.amount;
                sender.nonce = transaction.transaction.body.nonce;
                receiver.amount += transaction.transaction.body.amount;
                self.set_account(state_view, transaction.transaction.body.sender, &sender);
                self.set_account(state_view, transaction.transaction.body.sender, &receiver);
                true
            }
            _ => false,
        }
    }
}

impl StateTransitionRuntime for Runtime {
    type StateDbView = StateDbViewMock;
    fn apply(
        &self,
        state_view: &mut Self::StateDbView,
        transactions: Vec<StatedTransaction>,
    ) -> (Vec<StatedTransaction>, StateDbViewMock) {
        let mut filtered_transactions = vec![];
        for t in transactions.into_iter() {
            if self.apply_transaction(state_view, &t) {
                state_view.commit();
                filtered_transactions.push(t);
            } else {
                state_view.rollback();
            }
        }
        (filtered_transactions, state_view.finish())
    }
}

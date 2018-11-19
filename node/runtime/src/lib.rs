extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate bincode;
#[macro_use]
extern crate log;

extern crate byteorder;
extern crate primitives;
extern crate storage;

use byteorder::{LittleEndian, WriteBytesExt};
use primitives::signature::PublicKey;
use primitives::types::{AccountId, SignedTransaction};
use storage::{StateDb, StateDbView};

#[derive(Serialize, Deserialize)]
pub struct Account {
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
    pub fn get_account(
        &self,
        state_db: &StateDb,
        state_view: &StateDbView,
        account_key: AccountId,
    ) -> Option<Account> {
        state_view
            .get(state_db, &account_id_to_bytes(account_key))
            .and_then(|data| bincode::deserialize(&data).ok())
    }

    pub fn set_account(
        &self,
        state_db: &StateDb,
        state_view: &mut storage::StateDbView,
        account_key: AccountId,
        account: &Account,
    ) {
        match bincode::serialize(&account) {
            Ok(data) => state_view.set(state_db, &account_id_to_bytes(account_key), data),
            Err(e) => {
                error!("error occurred while encoding: {:?}", e);
            }
        }
    }
    fn apply_transaction(
        &self,
        state_db: &StateDb,
        state_view: &mut storage::StateDbView,
        transaction: &SignedTransaction,
    ) -> bool {
        let sender = self.get_account(state_db, state_view, transaction.body.sender);
        let receiver = self.get_account(state_db, state_view, transaction.body.receiver);
        match (sender, receiver) {
            (Some(mut sender), Some(mut receiver)) => {
                sender.amount -= transaction.body.amount;
                sender.nonce = transaction.body.nonce;
                receiver.amount += transaction.body.amount;
                self.set_account(state_db, state_view, transaction.body.sender, &sender);
                self.set_account(state_db, state_view, transaction.body.sender, &receiver);
                true
            }
            _ => false,
        }
    }

    pub fn apply(
        &self,
        state_db: &mut StateDb,
        state_view: &mut StateDbView,
        transactions: Vec<SignedTransaction>,
    ) -> (Vec<SignedTransaction>, StateDbView) {
        let mut filtered_transactions = vec![];
        for t in transactions.into_iter() {
            if self.apply_transaction(state_db, state_view, &t) {
                state_view.commit();
                filtered_transactions.push(t);
            } else {
                state_view.rollback();
            }
        }
        (filtered_transactions, state_view.finish(state_db))
    }
}

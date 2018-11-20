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
use primitives::types::{AccountId, MerkleHash, SignedTransaction, ViewCall, ViewCallResult};
use storage::{StateDb, StateDbUpdate};

#[derive(Serialize, Deserialize)]
pub struct Account {
    pub public_keys: Vec<PublicKey>,
    pub nonce: u64,
    pub amount: u64,
}

#[derive(Default)]
pub struct Runtime;

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
        state_update: &mut StateDbUpdate,
        account_key: AccountId,
    ) -> Option<Account> {
        state_update
            .get(&account_id_to_bytes(account_key))
            .and_then(|data| bincode::deserialize(&data).ok())
    }

    pub fn set_account(
        &self,
        state_update: &mut StateDbUpdate,
        account_key: AccountId,
        account: &Account,
    ) {
        match bincode::serialize(&account) {
            Ok(data) => state_update.set(&account_id_to_bytes(account_key), data),
            Err(e) => {
                error!("error occurred while encoding: {:?}", e);
            }
        }
    }
    fn apply_transaction(
        &self,
        state_update: &mut StateDbUpdate,
        transaction: &SignedTransaction,
    ) -> bool {
        let sender = self.get_account(state_update, transaction.body.sender);
        let receiver = self.get_account(state_update, transaction.body.receiver);
        match (sender, receiver) {
            (Some(mut sender), Some(mut receiver)) => {
                sender.amount -= transaction.body.amount;
                sender.nonce = transaction.body.nonce;
                receiver.amount += transaction.body.amount;
                self.set_account(state_update, transaction.body.sender, &sender);
                self.set_account(state_update, transaction.body.sender, &receiver);
                true
            }
            _ => false,
        }
    }

    pub fn apply(
        &self,
        state_db: &mut StateDb,
        root: &MerkleHash,
        transactions: Vec<SignedTransaction>,
    ) -> (Vec<SignedTransaction>, MerkleHash) {
        let mut filtered_transactions = vec![];
        let mut state_update = StateDbUpdate::new(state_db, root);
        for t in transactions.into_iter() {
            if self.apply_transaction(&mut state_update, &t) {
                state_update.commit();
                filtered_transactions.push(t);
            } else {
                state_update.rollback();
            }
        }
        let state_view = state_update.finalize();
        (filtered_transactions, state_view)
    }

    pub fn view(
        &self,
        state_db: &mut StateDb,
        root: &MerkleHash,
        view_call: &ViewCall,
    ) -> ViewCallResult {
        let mut state_update = StateDbUpdate::new(state_db, root);
        match self.get_account(&mut state_update, view_call.account) {
            Some(account) => ViewCallResult {
                account: view_call.account,
                amount: account.amount,
            },
            None => ViewCallResult {
                account: view_call.account,
                amount: 0,
            },
        }
    }
}

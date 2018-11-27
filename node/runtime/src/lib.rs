extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate bincode;
#[macro_use]
extern crate log;

extern crate byteorder;
extern crate network;
extern crate primitives;
extern crate storage;
extern crate beacon;

use std::sync::Arc;
use std::collections::HashMap;
use serde::{Serialize, de::DeserializeOwned};
use byteorder::{LittleEndian, WriteBytesExt};
use primitives::signature::PublicKey;
use primitives::types::{AccountId, MerkleHash, SignedTransaction, ViewCall, ViewCallResult};
use primitives::hash::CryptoHash;
use storage::{StateDb, StateDbUpdate};
use beacon::authority::AuthorityChangeSet;

const RUNTIME_DATA : &[u8] = b"runtime";

/// Runtime data that is stored in the state.
/// TODO: Look into how to store this not in a single element of the StateDb.
#[derive(Serialize, Deserialize)]
pub struct RuntimeData {
    /// Currently staked money.
    pub stake: HashMap<AccountId, u64>,
}

impl RuntimeData {
    pub fn at_stake(&self, account_key: AccountId) -> u64 {
        self.stake.get(&account_key).cloned().unwrap_or(0)
    }
    pub fn put_stake(&mut self, account_key: AccountId, amount: u64) {
        self.stake.insert(account_key, amount);
    }
}

/// Per account information stored in the state.
#[derive(Serialize, Deserialize)]
pub struct Account {
    pub public_keys: Vec<PublicKey>,
    pub nonce: u64,
    pub amount: u64,
}

pub struct ApplyState {
    pub root: MerkleHash,
    pub block_index: u64,
    pub parent_block_hash: CryptoHash,
}

pub struct ApplyResult {
    pub root: MerkleHash,
    pub transaction: storage::TrieBackendTransaction,
    pub authority_change_set: AuthorityChangeSet,
}

#[derive(Default)]
pub struct Runtime;

pub fn account_id_to_bytes(account_key: AccountId) -> Vec<u8> {
    let mut bytes = vec![];
    bytes
        .write_u64::<LittleEndian>(account_key)
        .expect("writing to bytes failed");
    bytes
}

/// TODO: runtime must include balance / staking / WASM modules.
impl Runtime {
    pub fn get<T: DeserializeOwned>(&self, state_update: &mut StateDbUpdate, key: &[u8]) -> Option<T> {
        state_update.get(key).and_then(|data| bincode::deserialize(&data).ok())
    }
    pub fn set<T: Serialize>(&self, state_update: &mut StateDbUpdate, key: &[u8], value: &T) {
        match bincode::serialize(value) {
            Ok(data) => state_update.set(key, storage::DBValue::from_slice(&data)),
            Err(e) => error!("Error occurred while encoding {:?}", e)
        }
    }
    fn apply_transaction(
        &self,
        state_update: &mut StateDbUpdate,
        transaction: &SignedTransaction,
        authority_change_set: &mut AuthorityChangeSet,
    ) -> bool {
        let runtime_data: Option<RuntimeData> = self.get(state_update, RUNTIME_DATA);
        let sender: Option<Account> = self.get(state_update, &account_id_to_bytes(transaction.body.sender));
        let receiver: Option<Account> = self.get(state_update, &account_id_to_bytes(transaction.body.receiver));
        match (runtime_data, sender, receiver) {
            (Some(mut runtime_data), Some(mut sender), Some(mut receiver)) => {
                // Transaction is staking transaction.
                if transaction.body.sender == transaction.body.receiver {
                    if sender.amount >= transaction.body.amount && sender.public_keys.is_empty() {
                        runtime_data.put_stake(transaction.body.sender, transaction.body.amount);
                        authority_change_set.proposed.insert(transaction.body.sender, (sender.public_keys[0], transaction.body.amount));
                        self.set(state_update, RUNTIME_DATA, &runtime_data);
                        true
                    } else {
                        false
                    }
                } else {
                    if sender.amount - runtime_data.at_stake(transaction.body.sender) >= transaction.body.amount {
                        sender.amount -= transaction.body.amount;
                        sender.nonce = transaction.body.nonce;
                        receiver.amount += transaction.body.amount;
                        self.set(state_update, &account_id_to_bytes(transaction.body.sender), &sender);
                        self.set(state_update, &account_id_to_bytes(transaction.body.sender), &receiver);
                        true
                    } else {
                        false
                    }
                }
            }
            _ => false,
        }
    }

    pub fn apply(
        &self,
        state_db: Arc<StateDb>,
        apply_state: &ApplyState,
        transactions: Vec<SignedTransaction>,
    ) -> (Vec<SignedTransaction>, ApplyResult) {
        let mut filtered_transactions = vec![];
        let mut state_update = StateDbUpdate::new(state_db, apply_state.root);
        let mut authority_change_set = AuthorityChangeSet::default();
        for t in transactions {
            if self.apply_transaction(&mut state_update, &t, &mut authority_change_set) {
                state_update.commit();
                filtered_transactions.push(t);
            } else {
                state_update.rollback();
            }
        }
        let (transaction, new_root) = state_update.finalize();
        (filtered_transactions, ApplyResult { root: new_root, transaction, authority_change_set })
    }

    pub fn view(
        &self,
        state_db: Arc<StateDb>,
        root: MerkleHash,
        view_call: &ViewCall,
    ) -> ViewCallResult {
        let mut state_update = StateDbUpdate::new(state_db, root);
        match self.get::<Account>(&mut state_update, &account_id_to_bytes(view_call.account)) {
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


#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use primitives::types::*;
    use storage::*;


    // TODO: Need state db to initialize with specific state.
    #[test]
    #[ignore]
    fn test_genesis_state() {
        let rt = Runtime {};
        let storage = Arc::new(MemoryStorage::default());
        let mut state_db = StateDb::new(storage);
        let root = state_db.get_state_view();
        let view_call = ViewCall { account: 1 };
        let result = rt.view(&mut state_db, &root, &view_call);
        assert_eq!(result, ViewCallResult {account: 1, amount: 0});
    }

    #[test]
    #[ignore]
    fn test_transfer_stake() {
        let rt = Runtime {};
        let t = SignedTransaction::new(123, TransactionBody::new(1, 1, 2, 100));
        let storage = Arc::new(MemoryStorage::default());
        let mut state_db = StateDb::new(storage);
        let root = state_db.get_state_view();
        let apply_state = ApplyState { root, parent_block_hash: CryptoHash::default(), block_index: 0 };
        let (filtered_tx, _apply_result) = rt.apply(&mut state_db, &apply_state, [t].to_vec());
        assert_eq!(filtered_tx.len(), 1);
    }
}
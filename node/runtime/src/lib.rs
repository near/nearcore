extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate bincode;
#[macro_use]
extern crate log;

extern crate beacon;
extern crate byteorder;
extern crate primitives;
extern crate storage;

use beacon::authority::AuthorityChangeSet;
use byteorder::{LittleEndian, WriteBytesExt};
use primitives::hash::CryptoHash;
use primitives::signature::PublicKey;
use primitives::types::{AccountId, MerkleHash, SignedTransaction, ViewCall, ViewCallResult};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use storage::{StateDb, StateDbUpdate};

const RUNTIME_DATA: &[u8] = b"runtime";

/// Runtime data that is stored in the state.
/// TODO: Look into how to store this not in a single element of the StateDb.
#[derive(Default, Serialize, Deserialize)]
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
    bytes.write_u64::<LittleEndian>(account_key).expect("writing to bytes failed");
    bytes
}

/// TODO: runtime must include balance / staking / WASM modules.
impl Runtime {
    fn get<T: DeserializeOwned>(&self, state_update: &mut StateDbUpdate, key: &[u8]) -> Option<T> {
        state_update.get(key).and_then(|data| bincode::deserialize(&data).ok())
    }
    fn set<T: Serialize>(&self, state_update: &mut StateDbUpdate, key: &[u8], value: &T) {
        match bincode::serialize(value) {
            Ok(data) => state_update.set(key, &storage::DBValue::from_slice(&data)),
            Err(e) => error!("Error occurred while encoding {:?}", e),
        }
    }
    fn apply_transaction(
        &self,
        state_update: &mut StateDbUpdate,
        transaction: &SignedTransaction,
        authority_change_set: &mut AuthorityChangeSet,
    ) -> bool {
        let runtime_data: Option<RuntimeData> = self.get(state_update, RUNTIME_DATA);
        let sender: Option<Account> =
            self.get(state_update, &account_id_to_bytes(transaction.body.sender));
        let receiver: Option<Account> =
            self.get(state_update, &account_id_to_bytes(transaction.body.receiver));
        match (runtime_data, sender, receiver) {
            (Some(mut runtime_data), Some(mut sender), Some(mut receiver)) => {
                // Transaction is staking transaction.
                if transaction.body.sender == transaction.body.receiver {
                    if sender.amount >= transaction.body.amount && sender.public_keys.is_empty() {
                        runtime_data.put_stake(transaction.body.sender, transaction.body.amount);
                        authority_change_set.proposed.insert(
                            transaction.body.sender,
                            (sender.public_keys[0], transaction.body.amount),
                        );
                        self.set(state_update, RUNTIME_DATA, &runtime_data);
                        true
                    } else {
                        false
                    }
                } else {
                    if sender.amount - runtime_data.at_stake(transaction.body.sender)
                        >= transaction.body.amount
                    {
                        sender.amount -= transaction.body.amount;
                        sender.nonce = transaction.body.nonce;
                        receiver.amount += transaction.body.amount;
                        self.set(
                            state_update,
                            &account_id_to_bytes(transaction.body.sender),
                            &sender,
                        );
                        self.set(
                            state_update,
                            &account_id_to_bytes(transaction.body.receiver),
                            &receiver,
                        );
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
                // state_update.commit();
                filtered_transactions.push(t);
            } else {
                // state_update.rollback();
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
            Some(account) => ViewCallResult { account: view_call.account, amount: account.amount },
            None => ViewCallResult { account: view_call.account, amount: 0 },
        }
    }

    pub fn genesis_state(&self, state_db: &Arc<StateDb>) -> MerkleHash {
        let mut state_db_update =
            storage::StateDbUpdate::new(state_db.clone(), MerkleHash::default());
        self.set(&mut state_db_update, RUNTIME_DATA, &RuntimeData::default());
        self.set(
            &mut state_db_update,
            &account_id_to_bytes(1),
            &Account { public_keys: vec![], amount: 100, nonce: 0 },
        );
        self.set(
            &mut state_db_update,
            &account_id_to_bytes(2),
            &Account { public_keys: vec![], amount: 0, nonce: 0 },
        );
        self.set(
            &mut state_db_update,
            &account_id_to_bytes(3),
            &Account { public_keys: vec![], amount: 0, nonce: 0 },
        );
        let (mut transaction, genesis_root) = state_db_update.finalize();
        // TODO: check that genesis_root is not yet in the state_db? Also may be can check before doing this?
        state_db.commit(&mut transaction).ok();
        genesis_root
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use primitives::types::*;
    use std::sync::Arc;
    use storage::test_utils::create_state_db;

    #[test]
    fn test_genesis_state() {
        let rt = Runtime {};
        let state_db = Arc::new(create_state_db());
        let root = rt.genesis_state(&state_db);
        let result = rt.view(state_db, root, &ViewCall { account: 1 });
        assert_eq!(result, ViewCallResult { account: 1, amount: 100 });
    }

    #[test]
    fn test_transfer_stake() {
        let rt = Runtime {};
        let t = SignedTransaction::new(123, TransactionBody::new(1, 1, 2, 100));
        let state_db = Arc::new(create_state_db());
        let root = rt.genesis_state(&state_db);
        let apply_state =
            ApplyState { root, parent_block_hash: CryptoHash::default(), block_index: 0 };
        let (filtered_tx, mut apply_result) =
            rt.apply(state_db.clone(), &apply_state, [t].to_vec());
        assert_ne!(root, apply_result.root);
        state_db.commit(&mut apply_result.transaction).ok();
        assert_eq!(filtered_tx.len(), 1);
        let result1 = rt.view(state_db.clone(), apply_result.root, &ViewCall { account: 1 });
        assert_eq!(result1, ViewCallResult { account: 1, amount: 0 });
        let result2 = rt.view(state_db, apply_result.root, &ViewCall { account: 2 });
        assert_eq!(result2, ViewCallResult { account: 2, amount: 100 });
    }
}

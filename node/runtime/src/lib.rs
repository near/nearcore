extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate bincode;
#[macro_use]
extern crate log;
extern crate kvdb;

extern crate beacon;
extern crate byteorder;
extern crate primitives;
extern crate storage;
extern crate wasm;

use kvdb::DBValue;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use beacon::authority::AuthorityChangeSet;
use byteorder::{LittleEndian, WriteBytesExt};
use primitives::hash::CryptoHash;
use primitives::signature::PublicKey;
use primitives::traits::{Decode, Encode};
use primitives::types::{AccountId, MerkleHash, SignedTransaction, ViewCall, ViewCallResult};
use primitives::utils::concat;
use std::fs;
use storage::{StateDb, StateDbUpdate};
use wasm::executor;
use wasm::ext::{External, Result};

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
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct Account {
    pub public_keys: Vec<PublicKey>,
    pub nonce: u64,
    pub amount: u64,
    pub code: Vec<u8>,
}

impl Account {
    pub fn new(public_keys: Vec<PublicKey>, amount: u64, code: Vec<u8>) -> Self {
        Account { public_keys, nonce: 0, amount, code }
    }
}

pub fn account_id_to_bytes(account_key: AccountId) -> Vec<u8> {
    let mut bytes = vec![];
    bytes.write_u64::<LittleEndian>(account_key).expect("writing to bytes failed");
    bytes
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

struct RuntimeExt<'a, 'b: 'a> {
    state_db_update: &'a mut StateDbUpdate<'b>,
    storage_prefix: Vec<u8>,
}

impl<'a, 'b: 'a> RuntimeExt<'a, 'b> {
    fn new(state_db_update: &'a mut StateDbUpdate<'b>, receiver: AccountId) -> Self {
        let mut prefix = account_id_to_bytes(receiver);
        prefix.append(&mut b",".to_vec());
        RuntimeExt { state_db_update, storage_prefix: prefix }
    }

    fn create_storage_key(&self, key: &[u8]) -> Vec<u8> {
        let mut storage_key = self.storage_prefix.clone();
        storage_key.extend_from_slice(key);
        storage_key
    }
}

impl<'a, 'b> External for RuntimeExt<'a, 'b> {
    fn storage_set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let storage_key = self.create_storage_key(key);
        self.state_db_update.set(&storage_key, &DBValue::from_slice(value));
        Ok(())
    }

    fn storage_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let storage_key = self.create_storage_key(key);
        let value = self.state_db_update.get(&storage_key);
        Ok(value.map(|buf| buf.to_vec()))
    }
}

#[derive(Default)]
pub struct Runtime {}

/// TODO: runtime must include balance / staking / WASM modules.
impl Runtime {
    pub fn get<T: DeserializeOwned>(
        &self,
        state_update: &mut StateDbUpdate,
        key: &[u8],
    ) -> Option<T> {
        state_update.get(key).and_then(|data| Decode::decode(&data))
    }

    pub fn set<T: Serialize>(&self, state_update: &mut StateDbUpdate, key: &[u8], value: &T) {
        value
            .encode()
            .map(|data| state_update.set(key, &storage::DBValue::from_slice(&data)))
            .unwrap_or(debug!("set value failed"))
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
                // temporary check
                if !transaction.body.method_name.is_empty() {
                    let mut runtime_ext = RuntimeExt::new(state_update, transaction.body.receiver);
                    let wasm_res = executor::execute(
                        &receiver.code,
                        transaction.body.method_name.as_bytes(),
                        &concat(transaction.body.args.clone()),
                        &mut vec![],
                        &mut runtime_ext,
                        &wasm::types::Config::default(),
                    );
                    match wasm_res {
                        Ok(res) => {
                            debug!(target: "runtime", "result of execution: {:?}", res);
                        }
                        Err(e) => {
                            debug!(target: "runtime", "wasm execution failed with error: {:?}", e);
                            return false;
                        }
                    }
                }
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
            (_, Some(_), receiver) => {
                if transaction.body.method_name == "deploy" {
                    match receiver {
                        Some(mut account) => {
                            account.code = transaction.body.args[0].clone();
                            self.set(
                                state_update,
                                &account_id_to_bytes(transaction.body.receiver),
                                &account,
                            );
                        }
                        _ => {
                            let account = Account::new(vec![], 0, transaction.body.args[0].clone());
                            self.set(
                                state_update,
                                &account_id_to_bytes(transaction.body.receiver),
                                &account,
                            );
                        }
                    };
                    true
                } else {
                    false
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
            Some(account) => ViewCallResult { account: view_call.account, amount: account.amount },
            None => ViewCallResult { account: view_call.account, amount: 0 },
        }
    }

    pub fn genesis_state(&self, state_db: &Arc<StateDb>) -> MerkleHash {
        let mut state_db_update =
            storage::StateDbUpdate::new(state_db.clone(), MerkleHash::default());
        let wasm_binary = fs::read("../../core/wasm/runtest/res/wasm_with_mem.wasm")
            .expect("Unable to read file");
        self.set(&mut state_db_update, RUNTIME_DATA, &RuntimeData::default());
        self.set(
            &mut state_db_update,
            &account_id_to_bytes(1),
            &Account { public_keys: vec![], amount: 100, nonce: 0, code: wasm_binary.clone() },
        );
        self.set(
            &mut state_db_update,
            &account_id_to_bytes(2),
            &Account { public_keys: vec![], amount: 0, nonce: 0, code: wasm_binary.clone() },
        );
        self.set(
            &mut state_db_update,
            &account_id_to_bytes(3),
            &Account { public_keys: vec![], amount: 0, nonce: 0, code: wasm_binary.clone() },
        );
        let (mut transaction, genesis_root) = state_db_update.finalize();
        // TODO: check that genesis_root is not yet in the state_db? Also may be can check before doing this?
        state_db.commit(&mut transaction).expect("Failed to commit genesis state");
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
        let t =
            SignedTransaction::new(123, TransactionBody::new(1, 1, 2, 100, String::new(), vec![]));
        let state_db = Arc::new(create_state_db());
        let root = rt.genesis_state(&state_db);
        let apply_state =
            ApplyState { root, parent_block_hash: CryptoHash::default(), block_index: 0 };
        let (filtered_tx, mut apply_result) = rt.apply(state_db.clone(), &apply_state, vec![t]);
        assert_ne!(root, apply_result.root);
        state_db.commit(&mut apply_result.transaction).ok();
        assert_eq!(filtered_tx.len(), 1);
        let result1 = rt.view(state_db.clone(), apply_result.root, &ViewCall { account: 1 });
        assert_eq!(result1, ViewCallResult { account: 1, amount: 0 });
        let result2 = rt.view(state_db, apply_result.root, &ViewCall { account: 2 });
        assert_eq!(result2, ViewCallResult { account: 2, amount: 100 });
    }

    #[test]
    fn test_get_and_set_accounts() {
        let state_db = Arc::new(create_state_db());
        let mut state_update = StateDbUpdate::new(state_db, MerkleHash::default());
        let test_account = Account { public_keys: vec![], nonce: 0, amount: 10, code: vec![] };
        let account_id = 0;
        let runtime = Runtime {};
        runtime.set(&mut state_update, &account_id_to_bytes(account_id), &test_account);
        let get_res = runtime.get(&mut state_update, &account_id_to_bytes(account_id)).unwrap();
        assert_eq!(test_account, get_res);
    }

    #[test]
    fn test_get_account_from_state_db() {
        let state_db = Arc::new(create_state_db());
        let root = MerkleHash::default();
        let mut state_update = StateDbUpdate::new(state_db.clone(), root);
        let test_account = Account::new(vec![], 10, vec![]);
        let account_id = 1;
        let runtime = Runtime {};
        runtime.set(&mut state_update, &account_id_to_bytes(account_id), &test_account);
        let (mut transaction, new_root) = state_update.finalize();
        state_db.commit(&mut transaction).unwrap();
        let mut new_state_update = StateDbUpdate::new(state_db.clone(), new_root);
        let get_res = runtime.get(&mut new_state_update, &account_id_to_bytes(account_id)).unwrap();
        assert_eq!(test_account, get_res);
    }

    #[test]
    fn test_smart_contract() {
        let state_db = Arc::new(create_state_db());
        let runtime = Runtime {};
        let root = runtime.genesis_state(&state_db);
        let tx_body = TransactionBody {
            nonce: 0,
            sender: 1,
            receiver: 2,
            amount: 0,
            method_name: "run_test".to_string(),
            args: vec![],
        };
        let transaction = SignedTransaction::new(123, tx_body);
        let tx_copy = transaction.clone();
        let apply_state =
            ApplyState { root, parent_block_hash: CryptoHash::default(), block_index: 0 };
        let (filtered_tx, _) = runtime.apply(state_db.clone(), &apply_state, vec![transaction]);
        assert_eq!(filtered_tx, vec![tx_copy]);
    }

    #[test]
    fn test_upload_contract() {
        let wasm_binary = fs::read("../../core/wasm/runtest/res/wasm_with_mem.wasm")
            .expect("Unable to read file");
        let state_db = Arc::new(create_state_db());
        let runtime = Runtime {};
        let root = runtime.genesis_state(&state_db);
        let tx_body = TransactionBody {
            nonce: 0,
            sender: 1,
            receiver: 10,
            amount: 0,
            method_name: "deploy".to_string(),
            args: vec![wasm_binary.clone()],
        };
        let transaction = SignedTransaction::new(123, tx_body);
        let tx_copy = transaction.clone();
        let apply_state =
            ApplyState { root, parent_block_hash: CryptoHash::default(), block_index: 0 };
        let (filtered_tx, mut apply_result) =
            runtime.apply(state_db.clone(), &apply_state, vec![transaction]);
        assert_eq!(filtered_tx, vec![tx_copy]);
        assert_ne!(root, apply_result.root);
        state_db.commit(&mut apply_result.transaction).unwrap();
        let mut new_state_update = StateDbUpdate::new(state_db, apply_result.root);
        let new_account = runtime.get(&mut new_state_update, &account_id_to_bytes(10)).unwrap();
        assert_eq!(Account::new(vec![], 0, wasm_binary), new_account);
    }
}

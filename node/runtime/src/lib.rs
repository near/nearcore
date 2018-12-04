extern crate beacon;
extern crate bincode;
extern crate byteorder;
extern crate chain;
extern crate kvdb;
#[macro_use]
extern crate log;
extern crate primitives;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate storage;
extern crate wasm;

use std::collections::HashMap;
use std::sync::Arc;

use kvdb::DBValue;
use serde::{de::DeserializeOwned, Serialize};

use beacon::types::AuthorityProposal;
use primitives::hash::CryptoHash;
use primitives::signature::PublicKey;
use primitives::traits::{Decode, Encode};
use primitives::types::{
    AccountAlias, AccountId, MerkleHash, PublicKeyAlias, SignedTransaction, ViewCall,
    ViewCallResult,
};
use primitives::utils::concat;
use storage::{StateDb, StateDbUpdate};
use wasm::executor;
use wasm::ext::{External, Result};
use chain::BlockChain;
use beacon::types::BeaconBlock;
use primitives::traits::Block;

pub mod chain_spec;
#[cfg(feature = "test-utils")]
pub mod test_utils;

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
    account_key.as_ref().to_vec()
}

pub struct ApplyState {
    pub root: MerkleHash,
    pub block_index: u64,
    pub parent_block_hash: CryptoHash,
}

pub struct ApplyResult {
    pub root: MerkleHash,
    pub transaction: storage::TrieBackendTransaction,
    pub authority_proposals: Vec<AuthorityProposal>,
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

fn get<T: DeserializeOwned>(state_update: &mut StateDbUpdate, key: &[u8]) -> Option<T> {
    state_update.get(key).and_then(|data| Decode::decode(&data))
}

fn set<T: Serialize>(state_update: &mut StateDbUpdate, key: &[u8], value: &T) {
    value
        .encode()
        .map(|data| state_update.set(key, &storage::DBValue::from_slice(&data)))
        .unwrap_or(debug!("set value failed"))
}

pub struct Runtime {
    state_db: Arc<StateDb>,
}

impl Runtime {
    pub fn new(state_db: Arc<StateDb>) -> Self {
        Runtime { state_db }
    }

    fn apply_transaction(
        &self,
        state_update: &mut StateDbUpdate,
        transaction: &SignedTransaction,
        authority_proposals: &mut Vec<AuthorityProposal>,
    ) -> bool {
        let runtime_data: Option<RuntimeData> = get(state_update, RUNTIME_DATA);
        let sender: Option<Account> =
            get(state_update, &account_id_to_bytes(transaction.body.sender));
        let receiver: Option<Account> =
            get(state_update, &account_id_to_bytes(transaction.body.receiver));
        match (runtime_data, sender, receiver) {
            (Some(mut runtime_data), Some(mut sender), Some(mut receiver)) => {
                // Check that transaction has valid nonce.
                if transaction.body.nonce <= sender.nonce {
                    debug!(target: "runtime", "Transaction nonce {} is invalid", transaction.body.nonce);
                    return false;
                }
                // Transaction contains call to smart contract
                if !transaction.body.method_name.is_empty() {
                    if transaction.body.method_name == "deploy" {
                        // re-deploy contract code for receiver
                        if transaction.body.args.is_empty() {
                            debug!(target: "runtime", "deploy requires at least 1 argument");
                            return false;
                        }
                        receiver.code = transaction.body.args[0].clone();
                        set(
                            state_update,
                            &account_id_to_bytes(transaction.body.receiver),
                            &receiver,
                        );
                    } else {
                        let mut runtime_ext =
                            RuntimeExt::new(state_update, transaction.body.receiver);
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
                }

                // Transaction is staking transaction.
                if transaction.body.sender == transaction.body.receiver {
                    if sender.amount >= transaction.body.amount && sender.public_keys.is_empty() {
                        runtime_data.put_stake(transaction.body.sender, transaction.body.amount);
                        authority_proposals.push(AuthorityProposal {
                            public_key: sender.public_keys[0],
                            amount: transaction.body.amount,
                        });
                        set(state_update, RUNTIME_DATA, &runtime_data);
                        true
                    } else {
                        if sender.amount < transaction.body.amount {
                            debug!(
                                target: "runtime",
                                "Account {:?} tries to stake {:?}, but only has {}",
                                transaction.body.sender,
                                transaction.body.amount,
                                sender.amount
                            );
                        } else {
                            debug!(target: "runtime", "Account {:?} already staked", transaction.body.sender);
                        }
                        false
                    }
                } else {
                    let staked = runtime_data.at_stake(transaction.body.sender);
                    if sender.amount - staked >= transaction.body.amount {
                        sender.amount -= transaction.body.amount;
                        sender.nonce = transaction.body.nonce;
                        receiver.amount += transaction.body.amount;
                        set(state_update, &account_id_to_bytes(transaction.body.sender), &sender);
                        set(
                            state_update,
                            &account_id_to_bytes(transaction.body.receiver),
                            &receiver,
                        );
                        true
                    } else {
                        debug!(
                            target: "runtime",
                            "Account {:?} tries to send {:?}, but has staked {} and has {} in the account",
                            transaction.body.sender,
                            transaction.body.amount,
                            staked,
                            sender.amount
                        );
                        false
                    }
                }
            }
            (_, Some(_), None) => {
                if transaction.body.method_name == "deploy" {
                    let account = Account::new(vec![], 0, transaction.body.args[0].clone());
                    set(state_update, &account_id_to_bytes(transaction.body.receiver), &account);
                    true
                } else {
                    debug!(
                        target: "runtime",
                        "Receiver {:?} does not exist",
                        transaction.body.receiver,
                    );
                    false
                }
            }
            _ => {
                debug!(
                    "Neither sender {:?} nor receiver {:?} exists",
                    transaction.body.sender, transaction.body.receiver
                );
                false
            }
        }
    }

    pub fn apply(
        &self,
        apply_state: &ApplyState,
        transactions: Vec<SignedTransaction>,
    ) -> (Vec<SignedTransaction>, ApplyResult) {
        let mut filtered_transactions = vec![];
        let mut state_update = StateDbUpdate::new(self.state_db.clone(), apply_state.root);
        let mut authority_proposals = vec![];
        for t in transactions {
            if self.apply_transaction(&mut state_update, &t, &mut authority_proposals) {
                state_update.commit();
                filtered_transactions.push(t);
            } else {
                state_update.rollback();
            }
        }
        let (transaction, new_root) = state_update.finalize();
        (filtered_transactions, ApplyResult { root: new_root, transaction, authority_proposals })
    }

    pub fn view(&self, root: MerkleHash, view_call: &ViewCall) -> ViewCallResult {
        let mut state_update = StateDbUpdate::new(self.state_db.clone(), root);
        match get::<Account>(&mut state_update, &account_id_to_bytes(view_call.account)) {
            Some(account) => {
                let mut result = vec![];
                if !view_call.method_name.is_empty() {
                    let mut runtime_ext = RuntimeExt::new(&mut state_update, view_call.account);
                    let wasm_res = executor::execute(
                        &account.code,
                        view_call.method_name.as_bytes(),
                        &concat(view_call.args.clone()),
                        &mut result,
                        &mut runtime_ext,
                        &wasm::types::Config::default(),
                    );
                    match wasm_res {
                        Ok(res) => {
                            debug!(target: "runtime", "result of execution: {:?}", res);
                        }
                        Err(e) => {
                            debug!(target: "runtime", "wasm execution failed with error: {:?}", e);
                        }
                    }
                }
                ViewCallResult {
                    account: view_call.account,
                    amount: account.amount,
                    nonce: account.nonce,
                    result,
                }
            }
            None => {
                ViewCallResult { account: view_call.account, amount: 0, nonce: 0, result: vec![] }
            }
        }
    }

    pub fn apply_genesis_state(
        &self,
        balances: &[(AccountAlias, PublicKeyAlias, u64)],
        wasm_binary: &[u8],
    ) -> MerkleHash {
        let mut state_db_update =
            storage::StateDbUpdate::new(self.state_db.clone(), MerkleHash::default());
        set(&mut state_db_update, RUNTIME_DATA, &RuntimeData::default());
        balances.iter().for_each(|(account_alias, public_key, balance)| {
            set(
                &mut state_db_update,
                &account_id_to_bytes(AccountId::from(account_alias)),
                &Account {
                    public_keys: vec![PublicKey::from(public_key)],
                    amount: *balance,
                    nonce: 0,
                    code: wasm_binary.to_vec(),
                },
            );
        });
        let (mut transaction, genesis_root) = state_db_update.finalize();
        // TODO: check that genesis_root is not yet in the state_db? Also may be can check before doing this?
        self.state_db.commit(&mut transaction).expect("Failed to commit genesis state");
        genesis_root
    }
}

pub struct StateDbViewer {
    beacon_chain: BlockChain<BeaconBlock>,
    runtime: Runtime,
}

impl StateDbViewer {
    pub fn new(beacon_chain: BlockChain<BeaconBlock>, runtime: Runtime) -> Self {
        StateDbViewer {
            beacon_chain,
            runtime,
        }
    }

    pub fn view(&self, view_call: &ViewCall) -> ViewCallResult {
        let root = self.beacon_chain.best_block().header().body.merkle_root_state;
        self.runtime.view(root, &view_call)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use primitives::hash::hash;
    use primitives::signature::get_keypair;
    use primitives::types::TransactionBody;
    use storage::test_utils::create_state_db;

    use super::*;
    use primitives::signature::DEFAULT_SIGNATURE;

    impl Default for Runtime {
        fn default() -> Runtime {
            Runtime { state_db: Arc::new(create_state_db()) }
        }
    }

    fn apply_default_genesis_state(runtime: &Runtime) -> MerkleHash {
        let (public_key, _) = get_keypair();
        let accounts = vec![
            ("alice".into(), public_key.to_string(), 0),
            ("bob".into(), public_key.to_string(), 100),
            ("john".into(), public_key.to_string(), 0),
        ];
        let wasm_binary = fs::read("../../core/wasm/runtest/res/wasm_with_mem.wasm")
            .expect("Unable to read file");
        runtime.apply_genesis_state(&accounts, &wasm_binary)
    }

    #[test]
    fn test_genesis_state() {
        let runtime = Runtime::default();
        let root = apply_default_genesis_state(&runtime);
        let result = runtime.view(root, &ViewCall::balance(hash(b"bob")));
        assert_eq!(
            result,
            ViewCallResult { account: hash(b"bob"), amount: 100, nonce: 0, result: vec![] }
        );
        let result2 =
            runtime.view(root, &ViewCall::func_call(hash(b"bob"), "run_test".to_string(), vec![]));
        assert_eq!(
            result2,
            ViewCallResult { account: hash(b"bob"), amount: 100, nonce: 0, result: vec![20, 0, 0, 0] }
        );
    }

    #[test]
    fn test_transfer_stake() {
        let runtime = Runtime::default();
        let root = apply_default_genesis_state(&runtime);
        let t = SignedTransaction::new(
            DEFAULT_SIGNATURE,
            TransactionBody::new(1, hash(b"bob"), hash(b"alice"), 100, String::new(), vec![]),
        );
        let apply_state =
            ApplyState { root, parent_block_hash: CryptoHash::default(), block_index: 0 };
        let (filtered_tx, mut apply_result) = runtime.apply(&apply_state, vec![t]);
        assert_ne!(root, apply_result.root);
        runtime.state_db.commit(&mut apply_result.transaction).ok();
        assert_eq!(filtered_tx.len(), 1);
        let result1 = runtime.view(apply_result.root, &ViewCall::balance(hash(b"bob")));
        assert_eq!(
            result1,
            ViewCallResult { account: hash(b"bob"), amount: 0, nonce: 1, result: vec![] }
        );
        let result2 = runtime.view(apply_result.root, &ViewCall::balance(hash(b"alice")));
        assert_eq!(
            result2,
            ViewCallResult { account: hash(b"alice"), amount: 100, nonce: 0, result: vec![] }
        );
    }

    #[test]
    fn test_get_and_set_accounts() {
        let state_db = Arc::new(create_state_db());
        let mut state_update = StateDbUpdate::new(state_db, MerkleHash::default());
        let test_account = Account { public_keys: vec![], nonce: 0, amount: 10, code: vec![] };
        let account_id = hash(b"bob");
        set(&mut state_update, &account_id_to_bytes(account_id), &test_account);
        let get_res = get(&mut state_update, &account_id_to_bytes(account_id)).unwrap();
        assert_eq!(test_account, get_res);
    }

    #[test]
    fn test_get_account_from_state_db() {
        let state_db = Arc::new(create_state_db());
        let root = MerkleHash::default();
        let mut state_update = StateDbUpdate::new(state_db.clone(), root);
        let test_account = Account::new(vec![], 10, vec![]);
        let account_id = hash(b"bob");
        set(&mut state_update, &account_id_to_bytes(account_id), &test_account);
        let (mut transaction, new_root) = state_update.finalize();
        state_db.commit(&mut transaction).unwrap();
        let mut new_state_update = StateDbUpdate::new(state_db.clone(), new_root);
        let get_res = get(&mut new_state_update, &account_id_to_bytes(account_id)).unwrap();
        assert_eq!(test_account, get_res);
    }

    #[test]
    fn test_smart_contract() {
        let runtime = Runtime::default();
        let root = apply_default_genesis_state(&runtime);
        let tx_body = TransactionBody {
            nonce: 1,
            sender: hash(b"bob"),
            receiver: hash(b"alice"),
            amount: 0,
            method_name: "run_test".to_string(),
            args: vec![],
        };
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state =
            ApplyState { root, parent_block_hash: CryptoHash::default(), block_index: 0 };
        let (filtered_tx, _) = runtime.apply(&apply_state, vec![transaction]);
        assert_eq!(filtered_tx.len(), 1);
    }

    #[test]
    fn test_upload_contract() {
        let runtime = Runtime::default();
        let root = apply_default_genesis_state(&runtime);
        let wasm_binary = fs::read("../../core/wasm/runtest/res/wasm_with_mem.wasm")
            .expect("Unable to read file");
        let tx_body = TransactionBody {
            nonce: 1,
            sender: hash(b"bob"),
            receiver: hash(b"xyz"),
            amount: 0,
            method_name: "deploy".to_string(),
            args: vec![wasm_binary.clone()],
        };
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state =
            ApplyState { root, parent_block_hash: CryptoHash::default(), block_index: 0 };
        let (filtered_tx, mut apply_result) = runtime.apply(&apply_state, vec![transaction]);
        assert_eq!(filtered_tx.len(), 1);
        assert_ne!(root, apply_result.root);
        runtime.state_db.commit(&mut apply_result.transaction).unwrap();
        let mut new_state_update = StateDbUpdate::new(runtime.state_db, apply_result.root);
        let new_account = get(&mut new_state_update, &account_id_to_bytes(hash(b"xyz"))).unwrap();
        assert_eq!(Account::new(vec![], 0, wasm_binary), new_account);
    }

    #[test]
    fn test_redeploy_contract() {
        let test_binary = b"test_binary";
        let runtime = Runtime::default();
        let root = apply_default_genesis_state(&runtime);
        let tx_body = TransactionBody {
            nonce: 1,
            sender: hash(b"bob"),
            receiver: hash(b"alice"),
            amount: 0,
            method_name: "deploy".to_string(),
            args: vec![test_binary.to_vec()],
        };
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state =
            ApplyState { root, parent_block_hash: CryptoHash::default(), block_index: 0 };
        let (filtered_tx, mut apply_result) = runtime.apply(&apply_state, vec![transaction]);
        assert_eq!(filtered_tx.len(), 1);
        assert_ne!(root, apply_result.root);
        runtime.state_db.commit(&mut apply_result.transaction).unwrap();
        let mut new_state_update = StateDbUpdate::new(runtime.state_db, apply_result.root);
        let new_account: Account =
            get(&mut new_state_update, &account_id_to_bytes(hash(b"alice"))).unwrap();
        assert_eq!(new_account.code, test_binary.to_vec())
    }

    #[test]
    fn test_send_money_and_execute_contract() {
        let runtime = Runtime::default();
        let root = apply_default_genesis_state(&runtime);
        let tx_body = TransactionBody {
            nonce: 1,
            sender: hash(b"bob"),
            receiver: hash(b"alice"),
            amount: 10,
            method_name: "run_test".to_string(),
            args: vec![],
        };
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state =
            ApplyState { root, parent_block_hash: CryptoHash::default(), block_index: 0 };
        let (filtered_tx, mut apply_result) = runtime.apply(&apply_state, vec![transaction]);
        assert_eq!(filtered_tx.len(), 1);
        assert_ne!(root, apply_result.root);
        runtime.state_db.commit(&mut apply_result.transaction).unwrap();
        let result1 = runtime.view(apply_result.root, &ViewCall::balance(hash(b"bob")));
        assert_eq!(
            result1,
            ViewCallResult { nonce: 1, account: hash(b"bob"), amount: 90, result: vec![] }
        );
        let result2 = runtime.view(apply_result.root, &ViewCall::balance(hash(b"alice")));
        assert_eq!(
            result2,
            ViewCallResult { nonce: 0, account: hash(b"alice"), amount: 10, result: vec![] }
        );
    }
}

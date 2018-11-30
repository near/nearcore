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
    AccountAlias, AccountId, MerkleHash, PublicKeyAlias, SignedTransaction, TransactionBody,
    ReceiptTransaction, ViewCall, ViewCallResult, PromiseId,
};
use primitives::utils::concat;
use storage::{StateDb, StateDbUpdate};
use wasm::executor;
use wasm::ext::{External, Result as ExtResult, Error as ExtError};
use wasm::types::ReturnData;
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
    fn storage_set(&mut self, key: &[u8], value: &[u8]) -> ExtResult<()> {
        let storage_key = self.create_storage_key(key);
        self.state_db_update.set(&storage_key, &DBValue::from_slice(value));
        Ok(())
    }

    fn storage_get(&self, key: &[u8]) -> ExtResult<Option<Vec<u8>>> {
        let storage_key = self.create_storage_key(key);
        let value = self.state_db_update.get(&storage_key);
        Ok(value.map(|buf| buf.to_vec()))
    }

    fn promise_create(
        &mut self,
        _account_alias: AccountAlias,
        _method_name: Vec<u8>,
        _arguments: Vec<u8>,
        _mana: u32,
        _amount: u64,
    ) -> ExtResult<PromiseId> {
        Err(ExtError::NotImplemented)
    }

    fn promise_then(
        &mut self,
        _promise_id: PromiseId,
        _method_name: Vec<u8>,
        _arguments: Vec<u8>,
        _mana: u32,
    ) -> ExtResult<PromiseId> {
        Err(ExtError::NotImplemented)
    }

    fn promise_and(
        &mut self,
        _promise_id1: PromiseId,
        _promise_id2: PromiseId,
    ) -> ExtResult<PromiseId> {
        Err(ExtError::NotImplemented)
    }

}

fn get<T: DeserializeOwned>(state_update: &mut StateDbUpdate, key: &[u8]) -> Option<T> {
    state_update.get(key).and_then(|data| Decode::decode(&data))
}

fn set<T: Serialize>(state_update: &mut StateDbUpdate, key: &[u8], value: &T) {
    value
        .encode()
        .map(|data| state_update.set(key, &storage::DBValue::from_slice(&data)))
        .unwrap_or_else(|| { debug!("set value failed"); })
}

/// executes code in wasm and updates state
fn wasm_execute(
    state_update: &mut StateDbUpdate,
    account_id: AccountId,
    code: &[u8],
    method_name: &[u8],
    input: &[u8],
    output: &mut Vec<u8>,
    config: &wasm::types::Config
) -> Result<(), String> {
    let mut runtime_ext = RuntimeExt::new(
        state_update,
        account_id,
    );
    executor::execute(
        code,
        method_name,
        input,
        output,
        &mut runtime_ext,
        config,
    ).map_err(|e| {
        format!("wasm execution failed with error: {:?}", e)
    })
}

pub struct Runtime {
    state_db: Arc<StateDb>,
}

impl Runtime {
    pub fn new(state_db: Arc<StateDb>) -> Self {
        Runtime { state_db }
    }

    fn send_money(
        &self,
        state_update: &mut StateDbUpdate,
        body: &TransactionBody,
        sender: &mut Account,
        runtime_data: &mut RuntimeData,
    ) -> Result<Option<ReceiptTransaction>, String> {
        let staked = runtime_data.at_stake(body.sender);
        if sender.amount - staked >= body.amount {
            sender.amount -= body.amount;
            sender.nonce = body.nonce;
            set(state_update, &account_id_to_bytes(body.sender), sender);
            let receipt = ReceiptTransaction::new(
                body.sender,
                body.receiver,
                body.amount,
                b"deposit".to_vec(),
                vec![],
                vec![],
            );
            Ok(Some(receipt))
        } else {
            Err(
                format!(
                    "Account {} tries to send {}, but has staked {} and only has {}",
                    body.sender, body.amount, staked, sender.amount
                )
            )
        }
    }

    fn staking(
        &self,
        state_update: &mut StateDbUpdate,
        body: &TransactionBody,
        sender: &mut Account,
        runtime_data: &mut RuntimeData,
        authority_change_set: &mut AuthorityChangeSet,
    ) -> Result<Option<ReceiptTransaction>, String>{
        if sender.amount >= body.amount && sender.public_keys.is_empty() {
            runtime_data.put_stake(body.sender, body.amount);
            authority_change_set.proposed.insert(
                body.sender,
                (sender.public_keys[0], body.amount),
            );
            set(state_update, RUNTIME_DATA, &runtime_data);
            Ok(None)
        } else if sender.amount < body.amount {
            let err_msg = format!(
                "Account {} tries to stake {}, but only has {}",
                body.sender,
                body.amount,
                sender.amount
            );
            Err(err_msg)
        } else {
            Err(format!("Account {} already staked", body.sender))
        }
    }

    // deploy smart contract
    fn deploy(
        &self,
        state_update: &mut StateDbUpdate,
        body: &TransactionBody,
        sender: &mut Account,
    ) -> Result<Option<ReceiptTransaction>, String> {
        if body.args.is_empty() {
            return Err("deploy requires at least 1 argument".to_string());
        }
        if body.sender == body.receiver {
            sender.code = body.args[0].clone();
            set(
                state_update,
                &account_id_to_bytes(body.sender),
                sender,
            );
            Ok(None)
        } else {
            let receipt = ReceiptTransaction::new(
                body.sender,
                body.receiver,
                body.amount,
                b"deploy".to_vec(),
                body.args.clone(),
                vec![],
            );
            Ok(Some(receipt))
        }
    }

    /// call to other contracts, only generates receipt
    fn apply_call(
        &self,
        body: &TransactionBody,
    ) -> Result<Option<ReceiptTransaction>, String> {
        if body.sender == body.receiver {
            return Err("target of call is self".to_string());
        }
        // Here we require the argument of call to be the 
        // method in the other contract, as well as arguments
        // to that method. Ideally this should be done inside wasm
        // i.e, the wasm executor will return the receipt
        if body.args.is_empty() {
            return Err("call has no arguments".to_string());
        }
        let mut args = body.args.clone();
        let method_name = args.remove(0);
        let receipt = ReceiptTransaction::new(
            body.sender,
            body.receiver,
            body.amount,
            method_name,
            args,
            vec![],
        );
        Ok(Some(receipt))
    }

    fn apply_call_with_callback(
        &self,
        body: &TransactionBody,
    ) -> Result<Option<ReceiptTransaction>, String> {
        if body.sender == body.receiver {
            return Err("target of call is self".to_string());
        }
        if body.args.is_empty() {
            return Err("call has no arguments".to_string());
        }
        let mut args = body.args.clone();
        let method_name = args.remove(0);
        let receipt = ReceiptTransaction::new(
            body.sender,
            body.receiver,
            body.amount,
            method_name,
            args,
            body.call_backs.clone(),
        );
        Ok(Some(receipt))
    }

    fn handle_special_calls(
        &self,
        state_update: &mut StateDbUpdate,
        transaction: &SignedTransaction,
        account: &mut Account,
        runtime_data: &mut RuntimeData,
        authority_change_set: &mut AuthorityChangeSet,
    ) -> (Result<Option<ReceiptTransaction>, String>, bool) {
        let lift = |res| { (res, true) };
        match transaction.body.method_name.as_str() {
            "send_money" => {
                lift(self.send_money(
                    state_update,
                    &transaction.body,
                    account,
                    runtime_data
                ))
            }
            "staking" => {
                lift(self.staking(
                    state_update,
                    &transaction.body,
                    account,
                    runtime_data,
                    authority_change_set,
                ))
            }
            "deploy" => {
                lift(self.deploy(
                    state_update,
                    &transaction.body,
                    account
                ))
            }
            "call" => {
                lift(self.apply_call(&transaction.body))
            }
            "call_with_call_back" => {
                lift(self.apply_call_with_callback(&transaction.body))
            }
            _ => (Err(String::new()), false)
        }
    }

    /// node receives signed_transaction, processes it
    /// and generates the receipt to send to receiver
    fn apply_signed_transaction(
        &self,
        state_update: &mut StateDbUpdate,
        transaction: &SignedTransaction,
        authority_change_set: &mut AuthorityChangeSet,
    ) -> Result<Option<ReceiptTransaction>, String> {
        let runtime_data: Option<RuntimeData> = get(state_update, RUNTIME_DATA);
        let sender: Option<Account> =
            get(state_update, &account_id_to_bytes(transaction.body.sender));
        match (runtime_data, sender) {
            (Some(mut runtime_data), Some(mut sender)) => {
                if transaction.body.nonce <= sender.nonce {
                    return Err(format!("Transaction nonce {} is invalid", transaction.body.nonce));
                }
                let (res, is_special) = self.handle_special_calls(
                    state_update, transaction, &mut sender, &mut runtime_data, authority_change_set
                );
                if is_special {
                    return res;
                }
                wasm_execute(
                    state_update,
                    transaction.body.sender,
                    &sender.code,
                    transaction.body.method_name.as_bytes(),
                    &concat(transaction.body.args.clone()),
                    &mut vec![],
                    &wasm::types::Config::default(),
                ).map(|_| None)
            }
            (None, _) => Err("runtime data does not exist".to_string()),
            _ => Err(format!("sender {} does not exist", transaction.body.sender))
        }
    }

    fn deposit(
        &self,
        state_update: &mut StateDbUpdate,
        receipt: &ReceiptTransaction,
        receiver: &mut Account
    ) -> Result<Option<ReceiptTransaction>, String> {
        receiver.amount += receipt.amount;
        set(
            state_update,
            &account_id_to_bytes(receipt.receiver),
            receiver
        );
        Ok(None)
    }

    fn apply_callback(
        &self,
        state_update: &mut StateDbUpdate,
        receipt: &ReceiptTransaction,
        receiver: &Account
    ) -> Result<Option<ReceiptTransaction>, String> {
        // We don't want to copy the entire call back stack in case
        // it is unnecessary (when the receipt returns to the original sender)
        // need to figure out the most efficient way to do this
        let mut start_copy = false;
        let mut new_call_backs = vec![];
        for call_back in receipt.call_backs.iter() {
            // need to figure out whether this is allowed
            if !start_copy && call_back.caller != receipt.receiver {
                start_copy = true;
            }
            if start_copy {
                new_call_backs.push(call_back.clone());
            } else {
                let res = wasm_execute(
                    state_update,
                    receipt.receiver,
                    &receiver.code,
                    call_back.method_name.as_bytes(),
                    &call_back.ret_val.clone().unwrap_or_default(),
                    &mut vec![],
                    &wasm::types::Config::default(),
                );
                if let Err(s) = res {
                    return Err(s);
                }
            }
        }
        if new_call_backs.is_empty() {
            Ok(None)
        } else {
            let next_receiver = new_call_backs[0].caller;
            let new_receipt = ReceiptTransaction::new(
                receipt.receiver,
                next_receiver,
                0,
                b"callback".to_vec(),
                vec![],
                new_call_backs,
            );
            Ok(Some(new_receipt))
        }
    }

    fn apply_receipt(
        &self,
        state_update: &mut StateDbUpdate,
        receipt: &ReceiptTransaction,
    ) -> Result<Option<ReceiptTransaction>, String> {
        let receiver_id = account_id_to_bytes(receipt.receiver);
        let receiver: Option<Account> = get(state_update, &receiver_id);
        match receiver {
            Some(mut receiver) => {
                if receipt.method_name == b"deposit".to_vec() {
                    self.deposit(state_update, receipt, &mut receiver)
                } else if receipt.method_name == b"callback".to_vec() {
                    self.apply_callback(state_update, receipt, &receiver)
                } else {
                    wasm_execute(
                        state_update,
                        receipt.receiver,
                        &receiver.code,
                        &receipt.method_name,
                        &concat(receipt.args.clone()),
                        &mut vec![],
                        &wasm::types::Config::default(),
                    ).map(|_| {
                        if receipt.call_backs.is_empty() {
                            // No callbacks, we end here
                            None
                        } else {
                            // TODO: put return value in the first callback
                            let new_receipt = ReceiptTransaction::new(
                                receipt.receiver,
                                receipt.sender,
                                0, // what should the amount be?
                                b"callback".to_vec(),
                                vec![],
                                receipt.call_backs.clone()
                            );
                            Some(new_receipt)
                        }
                    })
                }
            }
            _ => Err(format!("receiver {} does not exist", receipt.receiver))
        }
    }

    pub fn apply(
        &self,
        apply_state: &ApplyState,
        transactions: Vec<SignedTransaction>,
        receipts: &mut Vec<ReceiptTransaction>,
    ) -> (Vec<SignedTransaction>, ApplyResult) {
        let mut filtered_transactions = vec![];
        let mut state_update = StateDbUpdate::new(self.state_db.clone(), apply_state.root);
        let mut authority_proposals = vec![];
        for t in transactions {
            match self.apply_signed_transaction(&mut state_update, &t, &mut authority_change_set) {
                Ok(Some(receipt)) => {
                    receipts.push(receipt);
                    state_update.commit();
                    filtered_transactions.push(t);
                }
                Ok(None) => {
                    state_update.commit();
                    filtered_transactions.push(t);
                },
                Err(s) => {
                    debug!(target: "runtime", "{}", s);
                    state_update.rollback();
                }
            }
        }
        // receipts to be recorded in the block
        // for now it is not useful as we don't have shards
        let mut filtered_receipts = vec![];
        while !receipts.is_empty() {
            let receipt = receipts.remove(0);
            match self.apply_receipt(&mut state_update, &receipt) {
                Ok(Some(new_receipt)) => {
                    state_update.commit();
                    receipts.push(new_receipt);
                    filtered_receipts.push(receipt);
                }
                Ok(None) => {
                    state_update.commit();
                    filtered_receipts.push(receipt);
                }
                Err(s) => {
                    debug!(target: "runtime", "{}", s);
                    state_update.rollback();
                }
            }
        }
        let (transaction, new_root) = state_update.finalize();
        // Since we only have one shard, all receipts will be executed,
        // so no receipts is recorded here. Will change once we add
        // sharding support
        (
            filtered_transactions,
            ApplyResult { root: new_root, transaction, authority_change_set },
        )
    }

    pub fn apply_genesis_state(
        &self,
        balances: &[(AccountAlias, ReadablePublicKey, u64)],
        wasm_binary: &[u8],
        initial_authorities: &[(ReadablePublicKey, u64)]
    ) -> MerkleHash {
        let mut state_db_update =
            storage::StateDbUpdate::new(self.state_db.clone(), MerkleHash::default());
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
        let pk_to_acc_id: HashMap<ReadablePublicKey, AccountId> = balances.iter().map(|(account_alias, public_key, _)| (public_key.to_string(), AccountId::from(account_alias))).collect();
        let stake = initial_authorities.iter().map(|(pk, amount)| (*pk_to_acc_id.get(pk).expect("Missing account for public key"), *amount)).collect();
        let runtime_data = RuntimeData {
            stake
        };
        set(&mut state_db_update, RUNTIME_DATA, &runtime_data);
        let (mut transaction, genesis_root) = state_db_update.finalize();
        // TODO: check that genesis_root is not yet in the state_db? Also may be can check before doing this?
        self.state_db.commit(&mut transaction).expect("Failed to commit genesis state");
        genesis_root
    }
}

pub struct StateDbViewer {
    beacon_chain: Arc<BlockChain<BeaconBlock>>,
    state_db: Arc<StateDb>,
}

impl StateDbViewer {
    pub fn new(beacon_chain: Arc<BlockChain<BeaconBlock>>, state_db: Arc<StateDb>) -> Self {
        StateDbViewer {
            beacon_chain,
            state_db,
        }
    }

    pub fn view(&self, view_call: &ViewCall) -> ViewCallResult {
        let root = self.beacon_chain.best_block().header().body.merkle_root_state;
        self.view_at(view_call, root)
    }

    fn view_at(&self, view_call: &ViewCall, root: MerkleHash) -> ViewCallResult {
        let mut state_update = StateDbUpdate::new(self.state_db.clone(), root);
        let runtime_data: RuntimeData = get(&mut state_update, RUNTIME_DATA).expect("Runtime data is missing");
        match get::<Account>(&mut state_update, &account_id_to_bytes(view_call.account)) {
            Some(account) => {
                let mut result = vec![];
                if !view_call.method_name.is_empty() {
                    let mut runtime_ext = RuntimeExt::new(&mut state_update, view_call.account);
                    let wasm_res = executor::execute(
                        &account.code,
                        view_call.method_name.as_bytes(),
                        &concat(view_call.args.clone()),
                        &[],
                        &mut runtime_ext,
                        &wasm::types::Config::default(),
                    );
                    match wasm_res {
                        Ok(res) => {
                            debug!(target: "runtime", "result of execution: {:?}", res);
                            // TODO: Handle other ExecutionOutcome results
                            if let ReturnData::Value(buf) = res.return_data {
                                result.extend(&buf);
                            }
                        }
                        Err(e) => {
                            debug!(target: "runtime", "wasm execution failed with error: {:?}", e);
                        }
                    }
                }
                ViewCallResult {
                    account: view_call.account,
                    amount: account.amount,
                    stake: runtime_data.at_stake(view_call.account),
                    nonce: account.nonce,
                    result,
                }
            }
            None => {
                ViewCallResult { account: view_call.account, amount: 0, stake: 0, nonce: 0, result: vec![] }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use primitives::hash::hash;
    use primitives::types::{TransactionBody, CallBack};
    use primitives::signature::DEFAULT_SIGNATURE;

    use storage::test_utils::create_state_db;

    use test_utils::get_test_state_db_viewer;
    use test_utils::get_runtime_and_state_db_viewer;
    use std::fs;

    impl Default for Runtime {
        fn default() -> Runtime {
            Runtime { state_db: Arc::new(create_state_db()) }
        }
    }

    #[test]
    fn test_genesis_state() {
        let viewer = get_test_state_db_viewer();
        let result = viewer.view(&ViewCall::balance(hash(b"alice")));
        assert_eq!(
            result,
            ViewCallResult { account: hash(b"alice"), amount: 100, nonce: 0, stake: 50, result: vec![] }
        );
        let result2 =
            viewer.view(&ViewCall::func_call(hash(b"alice"), "run_test".to_string(), vec![]));
        assert_eq!(
            result2,
            ViewCallResult { account: hash(b"alice"), amount: 100, nonce: 0, stake: 50, result: vec![20, 0, 0, 0] }
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
        let (runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.beacon_chain.best_block().header().body.merkle_root_state;
        let tx_body = TransactionBody {
            nonce: 1,
            sender: hash(b"alice"),
            receiver: hash(b"bob"),
            amount: 0,
            method_name: "run_test".to_string(),
            args: vec![],
            call_backs: vec![],
        };
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state = ApplyState { 
            root, parent_block_hash: CryptoHash::default(), block_index: 0
        };
        let (filtered_tx, apply_result) = runtime.apply(&apply_state, vec![transaction], &mut vec![]);
        assert_eq!(filtered_tx.len(), 1);
        assert_ne!(root, apply_result.root);
    }

    #[test]
    #[should_panic]
    // we need to figure out how to deal with the case where account does not exist
    // especially in the context of sharding
    fn test_upload_contract() {
        let (runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.beacon_chain.best_block().header().body.merkle_root_state;
        let wasm_binary = fs::read("../../core/wasm/runtest/res/wasm_with_mem.wasm")
            .expect("Unable to read file");
        let tx_body = TransactionBody {
            nonce: 1,
            sender: hash(b"alice"),
            receiver: hash(b"xyz"),
            amount: 0,
            method_name: "deploy".to_string(),
            args: vec![wasm_binary.clone()],
            call_backs: vec![],
        };
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state =
            ApplyState { root, parent_block_hash: CryptoHash::default(), block_index: 0 };
        let (filtered_tx, mut apply_result) = runtime.apply(
            &apply_state, vec![transaction], &mut vec![]
        );
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
        let (runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.beacon_chain.best_block().header().body.merkle_root_state;
        let tx_body = TransactionBody {
            nonce: 1,
            sender: hash(b"bob"),
            receiver: hash(b"bob"),
            amount: 0,
            method_name: "deploy".to_string(),
            args: vec![test_binary.to_vec()],
            call_backs: vec![],
        };
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state =
            ApplyState { root, parent_block_hash: CryptoHash::default(), block_index: 0 };
        let (filtered_tx, mut apply_result) = runtime.apply(
            &apply_state, vec![transaction], &mut vec![]
        );
        assert_eq!(filtered_tx.len(), 1);
        assert_ne!(root, apply_result.root);
        runtime.state_db.commit(&mut apply_result.transaction).unwrap();
        let mut new_state_update = StateDbUpdate::new(runtime.state_db, apply_result.root);
        let new_account: Account = get(
            &mut new_state_update,
            &account_id_to_bytes(hash(b"bob"))
        ).unwrap();
        assert_eq!(new_account.code, test_binary.to_vec())
    }

    #[test]
    fn test_send_money() {
        let runtime = Runtime::default();
        let root = apply_default_genesis_state(&runtime);
        let tx_body = TransactionBody {
            nonce: 1,
            sender: hash(b"alice"),
            receiver: hash(b"bob"),
            amount: 10,
            method_name: "send_money".to_string(),
            args: vec![],
            call_backs: vec![],
        };
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state =
            ApplyState { root, parent_block_hash: CryptoHash::default(), block_index: 0 };
        let (filtered_tx, mut apply_result) = runtime.apply(
            &apply_state, vec![transaction], &mut vec![]
        );
        assert_eq!(filtered_tx.len(), 1);
        assert_ne!(root, apply_result.root);
        runtime.state_db.commit(&mut apply_result.transaction).unwrap();
        let result1 = viewer.view_at(
            &ViewCall::balance(hash(b"alice")),
            apply_result.root,
        );
        assert_eq!(
            result1,
            ViewCallResult {
                nonce: 1,
                account: hash(b"alice"),
                amount: 90,
                stake: 50,
                result: vec![],
            }
        );
        let result2 = viewer.view_at(
            &ViewCall::balance(hash(b"bob")),
            apply_result.root,
        );
        assert_eq!(
            result2,
            ViewCallResult {
                nonce: 0,
                account: hash(b"bob"),
                amount: 10,
                stake: 0,
                result: vec![],
            }
        );
    }

    #[test]
    fn test_simple_call() {
        let runtime = Runtime::default();
        let root = apply_default_genesis_state(&runtime);
        let tx_body = TransactionBody {
            nonce: 1,
            sender: hash(b"bob"),
            receiver: hash(b"alice"),
            amount: 10,
            method_name: "call".to_string(),
            args: vec![b"run_test".to_vec()],
            call_backs: vec![],
        };
        let transaction = SignedTransaction::new(123, tx_body);
        let apply_state =
            ApplyState { root, parent_block_hash: CryptoHash::default(), block_index: 0 };
        let (filtered_tx, apply_result) = runtime.apply(
            &apply_state, vec![transaction], &mut vec![]
        );
        assert_eq!(filtered_tx.len(), 1);
        assert_ne!(root, apply_result.root);
    }

    #[test]
    fn test_call_with_callback() {
        let runtime = Runtime::default();
        let root = apply_default_genesis_state(&runtime);
        let call_back = CallBack {
            method_name: "run_test".to_string(),
            ret_val: None,
            caller: hash(b"bob"),
        };
        let tx_body = TransactionBody {
            nonce: 1,
            sender: hash(b"bob"),
            receiver: hash(b"alice"),
            amount: 10,
            method_name: "call_with_call_back".to_string(),
            args: vec![b"run_test".to_vec()],
            call_backs: vec![call_back],
        };
        let transaction = SignedTransaction::new(123, tx_body);
        let apply_state =
            ApplyState { root, parent_block_hash: CryptoHash::default(), block_index: 0 };
        let (filtered_tx, apply_result) = runtime.apply(
            &apply_state, vec![transaction], &mut vec![]
        );
        assert_eq!(filtered_tx.len(), 1);
        assert_ne!(root, apply_result.root);
    }
}

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
use primitives::traits::{Decode, Encode, Block};
use primitives::types::{
    AccountAlias, AccountId, MerkleHash, ReadablePublicKey, SignedTransaction, TransactionBody,
    ReceiptTransaction, ReceiptBody, AsyncCall, CallbackResult, CallbackInfo, Callback,
    ViewCall, ViewCallResult, PromiseId, CallbackId,
};
use primitives::utils::{
    concat, index_to_bytes, account_to_shard_id
};
use storage::{StateDb, StateDbUpdate};
use wasm::executor;
use wasm::ext::{External, Result as ExtResult, Error as ExtError};
use wasm::types::ReturnData;
use chain::BlockChain;
use beacon::types::BeaconBlock;

pub mod chain_spec;
pub mod test_utils;

const RUNTIME_DATA: &[u8] = b"runtime";
const DEFAULT_MANA_LIMIT: u32 = 20;

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
    receipts: HashMap<PromiseId, ReceiptTransaction>,
    callbacks: HashMap<CallbackId, Callback>,
    account_id: AccountId,
    nonce: u64,
    transaction_hash: Vec<u8>,
}

impl<'a, 'b: 'a> RuntimeExt<'a, 'b> {
    fn new(
        state_db_update: &'a mut StateDbUpdate<'b>,
        account_id: AccountId,
        transaction_hash: Vec<u8>
    ) -> Self {
        let mut prefix = account_id_to_bytes(account_id);
        prefix.append(&mut b",".to_vec());
        RuntimeExt { 
            state_db_update,
            storage_prefix: prefix,
            receipts: HashMap::new(),
            callbacks: HashMap::new(),
            account_id,
            nonce: 0,
            transaction_hash,
        }
    }

    fn create_storage_key(&self, key: &[u8]) -> Vec<u8> {
        let mut storage_key = self.storage_prefix.clone();
        storage_key.extend_from_slice(key);
        storage_key
    }

    fn create_nonce(&mut self) -> Vec<u8> {
        let mut nonce: Vec<u8> = self.transaction_hash.clone();
        nonce.append(&mut index_to_bytes(self.nonce));
        self.nonce += 1;
        nonce
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
        account_alias: AccountAlias,
        method_name: Vec<u8>,
        arguments: Vec<u8>,
        mana: u32,
        amount: u64,
    ) -> ExtResult<PromiseId> {
        let nonce = self.create_nonce();
        let receipt = ReceiptTransaction::new(
            self.account_id,
            (&account_alias).into(),
            nonce.clone(),
            ReceiptBody::NewCall(AsyncCall::new(
                method_name,
                arguments,
                amount,
                mana,
            )),
        );
        let promise_id = PromiseId::Receipt(nonce);
        self.receipts.insert(promise_id.clone(), receipt);
        Ok(promise_id)
    }

    fn promise_then(
        &mut self,
        promise_id: PromiseId,
        method_name: Vec<u8>,
        arguments: Vec<u8>,
        mana: u32,
    ) -> ExtResult<PromiseId> {
        let callback_id = self.create_nonce();
        let receipt = match self.receipts.get_mut(&promise_id) {
            Some(r) => r,
            _ => return Err(ExtError::PromiseIdNotFound)
        };
        match receipt.body {
            ReceiptBody::NewCall(ref mut async_call) => {
                let shard_id = account_to_shard_id(self.account_id);
                let result_index = match promise_id {
                    PromiseId::Receipt(_) => 0,
                    PromiseId::Callback(_) => 0,
                    PromiseId::Joiner(v) => v.len(),
                };
                let callback_info = CallbackInfo::new(callback_id.clone(), 0, shard_id);
                async_call.callback = Some(callback_info);
                let mut callback = Callback::new(method_name, arguments, mana);
                callback.results.resize(result_index + 1, None);
                self.callbacks.insert(callback_id.clone(), callback);
                Ok(PromiseId::Callback(callback_id))
            }
            _ => {
                Err(ExtError::WrongPromise)
            }
        }
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

pub struct Runtime {
    state_db: Arc<StateDb>,
    callbacks: HashMap<CallbackId, Callback>,
}

impl Runtime {
    pub fn new(state_db: Arc<StateDb>) -> Self {
        Runtime { state_db, callbacks: HashMap::new() }
    }

    fn send_money(
        &self,
        state_update: &mut StateDbUpdate,
        transaction: &SignedTransaction,
        sender: &mut Account,
        runtime_data: &mut RuntimeData,
    ) -> Result<Vec<ReceiptTransaction>, String> {
        let staked = runtime_data.at_stake(transaction.body.sender);
        if sender.amount - staked >= transaction.body.amount {
            sender.amount -= transaction.body.amount;
            sender.nonce = transaction.body.nonce;
            set(state_update, &account_id_to_bytes(transaction.body.sender), sender);
            let receipt = ReceiptTransaction::new(
                transaction.body.sender,
                transaction.body.receiver,
                transaction.hash.into(),
                ReceiptBody::NewCall(AsyncCall::new(
                    b"deposit".to_vec(),
                    vec![],
                    transaction.body.amount,
                    1,
                ))
            );
            Ok(vec![receipt])
        } else {
            Err(
                format!(
                    "Account {} tries to send {}, but has staked {} and only has {}",
                    transaction.body.sender,
                    transaction.body.amount,
                    staked,
                    sender.amount
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
        authority_proposals: &mut Vec<AuthorityProposal>,
    ) -> Result<Vec<ReceiptTransaction>, String>{
        if sender.amount >= body.amount && sender.public_keys.is_empty() {
            runtime_data.put_stake(body.sender, body.amount);
            authority_proposals.push(AuthorityProposal {
                public_key: sender.public_keys[0],
                amount: body.amount,
            });
            set(state_update, RUNTIME_DATA, &runtime_data);
            Ok(vec![])
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

    fn handle_special_async_call(
        &self,
        state_update: &mut StateDbUpdate,
        transaction: &SignedTransaction,
        account: &mut Account,
        runtime_data: &mut RuntimeData,
        authority_proposals: &mut Vec<AuthorityProposal>
    ) -> Result<Vec<ReceiptTransaction>, String> {
        if transaction.body.method_name == b"send_money".to_vec() {
            self.send_money(state_update, transaction, account, runtime_data)
        } else if transaction.body.method_name == b"staking".to_vec() {
            self.staking(
                state_update,
                &transaction.body,
                account,
                runtime_data,
                authority_proposals
            )
        } else {
            // not a special call
            Err(String::new())
        }
    }

    /// node receives signed_transaction, processes it
    /// and generates the receipt to send to receiver
    fn apply_signed_transaction(
        &mut self,
        state_update: &mut StateDbUpdate,
        transaction: &SignedTransaction,
        authority_proposals: &mut Vec<AuthorityProposal>,
    ) -> Result<Vec<ReceiptTransaction>, String> {
        let runtime_data: Option<RuntimeData> = get(state_update, RUNTIME_DATA);
        let sender: Option<Account> =
            get(state_update, &account_id_to_bytes(transaction.body.sender));
        match (runtime_data, sender) {
            (Some(mut runtime_data), Some(mut sender)) => {
                if transaction.body.nonce <= sender.nonce {
                    return Err(format!("Transaction nonce {} is invalid", transaction.body.nonce));
                }
                let special_call_res = self.handle_special_async_call(
                    state_update,
                    transaction,
                    &mut sender,
                    &mut runtime_data,
                    authority_proposals
                );
                match special_call_res {
                    Ok(res) => return Ok(res),
                    Err(s) => {
                        if !s.is_empty() {
                            return Err(s);
                        }
                    }
                }
                let mut runtime_ext = RuntimeExt::new(
                    state_update,
                    transaction.body.sender,
                    transaction.hash.into()
                );
                // the result of this execution is not used for now
                // TODO: Use rate limiter for MANA
                executor::execute(
                    &sender.code,
                    &transaction.body.method_name,
                    &transaction.body.args,
                    &[],
                    &mut runtime_ext,
                    &wasm::types::Config::default(),
                    DEFAULT_MANA_LIMIT,
                ).map_err(|e| format!("wasm exeuction failed with error: {:?}", e))?;
                self.callbacks.extend(runtime_ext.callbacks);
                let receipts: Vec<ReceiptTransaction> = 
                    runtime_ext.receipts.drain().map(|(_, v)| v).collect();
                Ok(receipts)
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
    ) -> Result<Vec<ReceiptTransaction>, String> {
        match receipt.body {
            ReceiptBody::NewCall(ref async_call) => {
                receiver.amount += async_call.amount;
            }
            _ => return Err("Deposit does not come from an async call".to_string())
        }
        set(
            state_update,
            &account_id_to_bytes(receipt.receiver),
            receiver
        );
        Ok(vec![])
    }

    fn apply_receipt(
        &mut self,
        state_update: &mut StateDbUpdate,
        receipt: &ReceiptTransaction,
    ) -> Result<Vec<ReceiptTransaction>, String> {
        let receiver_id = account_id_to_bytes(receipt.receiver);
        let receiver: Option<Account> = get(state_update, &receiver_id);
        match receiver {
            Some(mut receiver) => {
                match &receipt.body {
                    ReceiptBody::NewCall(async_call) => {
                        if async_call.method_name == b"deposit".to_vec() {
                            return self.deposit(state_update, receipt, &mut receiver);
                        }
                        let mut runtime_ext = RuntimeExt::new(
                            state_update,
                            receipt.sender,
                            receipt.nonce.clone()
                        );
                        let wasm_result = executor::execute(
                            &receiver.code,
                            &async_call.method_name,
                            &async_call.args,
                            &[],
                            &mut runtime_ext,
                            &wasm::types::Config::default(),
                            async_call.mana,
                        ).map_err(|e| format!("wasm exeuction failed with error: {:?}", e))?;
                        let mut gen_receipt = |callback_id: &CallbackId, return_data, result_index| {
                            let callback_res = match return_data {
                                ReturnData::Value(v) => {
                                    CallbackResult::new(callback_id.clone(), Some(v), result_index)
                                }
                                ReturnData::None => {
                                    CallbackResult::new(callback_id.clone(), None, result_index)
                                }
                                ReturnData::Promise(PromiseId::Callback(id)) => {
                                    CallbackResult::new(id, None, result_index)
                                }
                                _ => return Err("return data is a non-callback promise".to_string())
                            };
                            let new_receipt = ReceiptTransaction::new(
                                receipt.receiver,
                                receipt.sender,
                                runtime_ext.create_nonce(),
                                ReceiptBody::Callback(callback_res),
                            );
                            Ok(vec![new_receipt])
                        };
                        match &async_call.callback {
                            Some(callback_info) => {
                                let result_index = callback_info.result_index;
                                gen_receipt(
                                    &callback_info.id,
                                    wasm_result.return_data,
                                    result_index
                                )
                            } 
                            None => {
                                match wasm_result.return_data {
                                    ReturnData::Promise(_) => {
                                        Err("No callback but return value is a promise".to_string())
                                    }
                                    _ => Ok(vec![])
                                }
                            }
                        }
                    },
                    ReceiptBody::Callback(callback_res) => {
                        let mut runtime_ext = RuntimeExt::new(
                            state_update,
                            receipt.sender,
                            receipt.nonce.clone()
                        );
                        let mut needs_removal = false;
                        let receipts = match self.callbacks.get_mut(&callback_res.id) {
                            Some(callback) => {
                                let expected_num_results = callback.results.len();
                                callback.results[callback_res.result_index] = callback_res.result.clone();
                                // if we have gathered all results, execute the callback
                                if callback_res.result_index == expected_num_results - 1 {
                                    executor::execute(
                                        &receiver.code,
                                        &callback.method_name,
                                        &callback.args,
                                        &[],
                                        &mut runtime_ext,
                                        &wasm::types::Config::default(),
                                        callback.mana,
                                    ).map_err(|e| format!("wasm exeuction failed with error: {:?}", e))?;
                                    needs_removal = true;
                                    runtime_ext.receipts.drain().map(|(_, v)| v).collect()
                                } else {
                                    // otherwise no receipt is generated
                                    vec![]
                                }
                            },
                            _ => {
                                return Err(format!("callback id: {:?} not found", callback_res.id));
                            }
                        };
                        if needs_removal {
                            self.callbacks.remove(&callback_res.id);
                        }
                        Ok(receipts)
                    }
                    // TODO: handle refund
                    ReceiptBody::Refund => unimplemented!()
                }
            }
            _ => Err(format!("receiver {} does not exist", receipt.receiver))
        }
    }

    pub fn apply(
        &mut self,
        apply_state: &ApplyState,
        transactions: Vec<SignedTransaction>,
        receipts: &mut Vec<ReceiptTransaction>,
    ) -> (Vec<SignedTransaction>, Vec<ReceiptTransaction>, ApplyResult) {
        let mut filtered_transactions = vec![];
        let mut state_update = StateDbUpdate::new(self.state_db.clone(), apply_state.root);
        let mut authority_proposals = vec![];
        for t in transactions {
            match self.apply_signed_transaction(&mut state_update, &t, &mut authority_proposals) {
                Ok(mut new_receipts) => {
                    receipts.append(&mut new_receipts);
                    state_update.commit();
                    filtered_transactions.push(t);
                }
                Err(s) => {
                    debug!(target: "runtime", "{}", s);
                    state_update.rollback();
                }
            }
        }
        // receipts to be recorded in the block
        // for now it is not useful as we don't have shards
        let mut filtered_receipts = vec![];
        while let Some(receipt) = receipts.pop() {
            // execute same shard receipts
            if account_to_shard_id(receipt.sender) == account_to_shard_id(receipt.receiver) {
                match self.apply_receipt(&mut state_update, &receipt) {
                    Ok(mut new_receipts) => {
                        state_update.commit();
                        receipts.append(&mut new_receipts);
                    }
                    Err(s) => {
                        debug!(target: "runtime", "{}", s);
                        state_update.rollback();
                    }
                }
            } else {
                filtered_receipts.push(receipt);
            }
        }
        let (transaction, new_root) = state_update.finalize();
        // Since we only have one shard, all receipts will be executed,
        // so no receipts is recorded here. Will change once we add
        // sharding support
        (
            filtered_transactions,
            filtered_receipts,
            ApplyResult { root: new_root, transaction, authority_proposals },
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
        let pk_to_acc_id: HashMap<ReadablePublicKey, AccountId> = 
            balances
                .iter()
                .map(|(account_alias, public_key, _)| (public_key.to_string(), AccountId::from(account_alias)))
                .collect();
        let stake = initial_authorities
            .iter()
            .map(|(pk, amount)| (*pk_to_acc_id.get(pk).expect("Missing account for public key"), *amount))
            .collect();
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
                    let mut runtime_ext = RuntimeExt::new(&mut state_update, view_call.account, vec![]);
                    let wasm_res = executor::execute(
                        &account.code,
                        view_call.method_name.as_bytes(),
                        &concat(view_call.args.clone()),
                        &[],
                        &mut runtime_ext,
                        &wasm::types::Config::default(),
                        DEFAULT_MANA_LIMIT,
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
                ViewCallResult { 
                    account: view_call.account,
                    amount: 0,
                    stake: 0,
                    nonce: 0,
                    result: vec![]
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use primitives::hash::hash;
    use primitives::types::TransactionBody;
    use primitives::signature::DEFAULT_SIGNATURE;

    use storage::test_utils::create_state_db;

    use test_utils::get_test_state_db_viewer;
    use test_utils::get_runtime_and_state_db_viewer;
    use std::fs;

    impl Default for Runtime {
        fn default() -> Runtime {
            Runtime { 
                state_db: Arc::new(create_state_db()),
                callbacks: HashMap::new()
            }
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
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.beacon_chain.best_block().header().body.merkle_root_state;
        let tx_body = TransactionBody {
            nonce: 1,
            sender: hash(b"alice"),
            receiver: hash(b"bob"),
            amount: 0,
            method_name: b"run_test".to_vec(),
            args: vec![],
        };
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state = ApplyState { 
            root, parent_block_hash: CryptoHash::default(), block_index: 0
        };
        let (filtered_tx, filtered_receipts, apply_result) = runtime.apply(
            &apply_state, vec![transaction], &mut vec![]
        );
        assert_eq!(filtered_tx.len(), 1);
        assert_eq!(filtered_receipts.len(), 0);
        assert_ne!(root, apply_result.root);
    }

    #[test]
    #[should_panic]
    // we need to figure out how to deal with the case where account does not exist
    // especially in the context of sharding
    fn test_upload_contract() {
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.beacon_chain.best_block().header().body.merkle_root_state;
        let wasm_binary = fs::read("../../core/wasm/runtest/res/wasm_with_mem.wasm")
            .expect("Unable to read file");
        let tx_body = TransactionBody {
            nonce: 1,
            sender: hash(b"alice"),
            receiver: hash(b"xyz"),
            amount: 0,
            method_name: b"deploy".to_vec(),
            args: wasm_binary.clone(),
        };
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state =
            ApplyState { root, parent_block_hash: CryptoHash::default(), block_index: 0 };
        let (filtered_tx, filtered_receipts, mut apply_result) = runtime.apply(
            &apply_state, vec![transaction], &mut vec![]
        );
        assert_eq!(filtered_tx.len(), 1);
        assert_eq!(filtered_receipts.len(), 0);
        assert_ne!(root, apply_result.root);
        runtime.state_db.commit(&mut apply_result.transaction).unwrap();
        let mut new_state_update = StateDbUpdate::new(runtime.state_db, apply_result.root);
        let new_account = get(&mut new_state_update, &account_id_to_bytes(hash(b"xyz"))).unwrap();
        assert_eq!(Account::new(vec![], 0, wasm_binary), new_account);
    }

    #[test]
    #[should_panic]
    fn test_redeploy_contract() {
        let test_binary = b"test_binary";
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.beacon_chain.best_block().header().body.merkle_root_state;
        let tx_body = TransactionBody {
            nonce: 1,
            sender: hash(b"bob"),
            receiver: hash(b"bob"),
            amount: 0,
            method_name: b"deploy".to_vec(),
            args: test_binary.to_vec(),
        };
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state =
            ApplyState { root, parent_block_hash: CryptoHash::default(), block_index: 0 };
        let (filtered_tx, filtered_receipts, mut apply_result) = runtime.apply(
            &apply_state, vec![transaction], &mut vec![]
        );
        assert_eq!(filtered_tx.len(), 1);
        assert_eq!(filtered_receipts.len(), 0);
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
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.beacon_chain.best_block().header().body.merkle_root_state;
        let tx_body = TransactionBody {
            nonce: 1,
            sender: hash(b"alice"),
            receiver: hash(b"bob"),
            amount: 10,
            method_name: b"send_money".to_vec(),
            args: vec![],
        };
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state =
            ApplyState { root, parent_block_hash: CryptoHash::default(), block_index: 0 };
        let (filtered_tx, filtered_receipts, mut apply_result) = runtime.apply(
            &apply_state, vec![transaction], &mut vec![]
        );
        assert_eq!(filtered_tx.len(), 1);
        assert_eq!(filtered_receipts.len(), 0);
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
}

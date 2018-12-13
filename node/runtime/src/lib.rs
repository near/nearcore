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
extern crate shard;
extern crate storage;
extern crate wasm;

use std::collections::HashMap;
use std::sync::Arc;

use serde::{de::DeserializeOwned, Serialize};

use beacon::types::AuthorityProposal;
use ext::RuntimeExt;
use primitives::hash::{CryptoHash, hash};
use primitives::signature::{PublicKey, Signature, verify};
use primitives::traits::{Decode, Encode};
use primitives::types::{
    AccountAlias, AccountId, MerkleHash, ReadablePublicKey, SignedTransaction, TransactionBody,
    ReceiptTransaction, ReceiptBody, AsyncCall, CallbackResult, CallbackInfo, Callback,
    PromiseId, CallbackId, StakeTransaction, SendMoneyTransaction, CreateAccountTransaction,
    SwapKeyTransaction, DeployContractTransaction, Balance, Transaction, ShardId,
};
use primitives::utils::{
    account_to_shard_id, index_to_bytes
};
use storage::{StateDb, StateDbUpdate};
use wasm::executor;
use wasm::types::{RuntimeContext, ReturnData};

pub mod chain_spec;
pub mod test_utils;
pub mod state_viewer;
mod ext;

const RUNTIME_DATA: &[u8] = b"runtime";
const DEFAULT_MANA_LIMIT: u32 = 20;

// const does not allow function call, so have to resort to this
fn system_account() -> AccountId {
    hash(b"system")
}

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
    pub fn new(public_keys: Vec<PublicKey>, amount: Balance, code: Vec<u8>) -> Self {
        Account { public_keys, nonce: 0, amount, code }
    }
}

fn account_id_to_bytes(account_key: AccountId) -> Vec<u8> {
    account_key.as_ref().to_vec()
}

fn create_nonce_with_nonce(base: &[u8], salt: u64) -> Vec<u8> {
    let mut nonce: Vec<u8> = base.to_owned();
    nonce.append(&mut index_to_bytes(salt));
    hash(&nonce).into()
}

pub struct ApplyState {
    pub root: MerkleHash,
    pub shard_id: ShardId,
    pub block_index: u64,
    pub parent_block_hash: CryptoHash,
}

pub struct ApplyResult {
    pub root: MerkleHash,
    pub shard_id: ShardId,
    pub transaction: storage::TrieBackendTransaction,
    pub authority_proposals: Vec<AuthorityProposal>,
    pub filtered_transactions: Vec<Transaction>,
    pub new_receipts: Vec<Transaction>,
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
        transaction: &SendMoneyTransaction,
        hash: CryptoHash,
        sender: &mut Account,
        runtime_data: &mut RuntimeData,
    ) -> Result<Vec<Transaction>, String> {
        let staked = runtime_data.at_stake(transaction.sender);
        if sender.amount - staked >= transaction.amount {
            sender.amount -= transaction.amount;
            set(state_update, &account_id_to_bytes(transaction.sender), sender);
            let receipt = ReceiptTransaction::new(
                transaction.sender,
                transaction.receiver,
                hash.into(),
                ReceiptBody::NewCall(AsyncCall::new(
                    b"deposit".to_vec(),
                    vec![],
                    transaction.amount,
                    1,
                ))
            );
            Ok(vec![Transaction::Receipt(receipt)])
        } else {
            Err(
                format!(
                    "Account {} tries to send {}, but has staked {} and only has {}",
                    transaction.sender,
                    transaction.amount,
                    staked,
                    sender.amount
                )
            )
        }
    }

    fn staking(
        &self,
        state_update: &mut StateDbUpdate,
        body: &StakeTransaction,
        sender: &mut Account,
        runtime_data: &mut RuntimeData,
        authority_proposals: &mut Vec<AuthorityProposal>,
    ) -> Result<Vec<Transaction>, String>{
        if sender.amount >= body.amount && sender.public_keys.is_empty() {
            runtime_data.put_stake(body.staker, body.amount);
            authority_proposals.push(AuthorityProposal {
                public_key: sender.public_keys[0],
                amount: body.amount,
            });
            set(state_update, RUNTIME_DATA, &runtime_data);
            Ok(vec![])
        } else if sender.amount < body.amount {
            let err_msg = format!(
                "Account {} tries to stake {}, but only has {}",
                body.staker,
                body.amount,
                sender.amount
            );
            Err(err_msg)
        } else {
            Err(format!("Account {} already staked", body.staker))
        }
    }

    fn create_account(
        &self,
        state_update: &mut StateDbUpdate,
        body: &CreateAccountTransaction,
        hash: CryptoHash,
        sender: &mut Account,
        runtime_data: &mut RuntimeData,
    ) -> Result<Vec<Transaction>, String> {
        let staked = runtime_data.at_stake(body.sender);
        if sender.amount >= staked + body.amount {
            sender.amount -= body.amount;
            set(
                state_update,
                &account_id_to_bytes(body.sender),
                &sender
            );
            let new_nonce = create_nonce_with_nonce(hash.as_ref(), 0);
            let receipt = ReceiptTransaction::new(
                body.sender,
                body.new_account_id,
                new_nonce,
                ReceiptBody::NewCall(AsyncCall::new(
                    b"create_account".to_vec(),
                    body.public_key.clone(),
                    body.amount,
                    0
                ))
            );
            Ok(vec![Transaction::Receipt(receipt)])
        } else {
            Err(
                format!(
                    "Account {} tries to create new account with {}, but has staked {} and only has {}",
                    body.sender,
                    body.amount,
                    staked,
                    sender.amount
                )
            )
        }
    }

    fn swap_key(
        &self,
        state_update: &mut StateDbUpdate,
        body: &SwapKeyTransaction,
        signature: &Signature,
        data: &[u8],
        account: &mut Account,
    ) -> Result<Vec<Transaction>, String> {
        // TODO: verify signature
        let cur_key = Decode::decode(&body.cur_key).ok_or("cannot decode public key")?;
        if !verify(data, signature, &cur_key) {
            return Err("Invalid signature. Cannot swap key".to_string());
        }
        let new_key = Decode::decode(&body.new_key).ok_or("cannot decode public key")?;
        let num_keys = account.public_keys.len();
        account.public_keys.retain(|&x| x != cur_key);
        if account.public_keys.len() == num_keys {
            return Err(format!("account {} does not have public key {}", body.sender, cur_key));
        }
        account.public_keys.push(new_key);
        set(
            state_update,
            &account_id_to_bytes(body.sender),
            &account
        );
        Ok(vec![])
    }

    fn deploy(
        &self,
        state_update: &mut StateDbUpdate,
        body: &DeployContractTransaction,
        account: &mut Account,
    ) -> Result<Vec<Transaction>, String> {
        // TODO: check signature
        let pub_key = Decode::decode(&body.public_key).ok_or("cannot decode public key")?;
        if account.public_keys.contains(&pub_key) {
            account.code = body.wasm_byte_array.clone();
            set(
                state_update,
                &account_id_to_bytes(body.contract_id),
                &account,
            );
            Ok(vec![])
        } else {
            Err(format!("account {} does not contain key {}", body.contract_id, pub_key))
        }
    }

    fn call_function(
        &mut self,
        state_update: &mut StateDbUpdate,
        runtime_data: &RuntimeData,
        sender: &mut Account,
        sender_account_id: AccountId,
        hash: CryptoHash,
        method_name: &[u8],
        args: &[u8],
    ) -> Result<Vec<Transaction>, String> {
        let staked = runtime_data.at_stake(sender_account_id);
        // sender.amount cannot be less than staked
        // otherwise staking would have failed
        assert!(sender.amount >= staked);
        let receipts = {
            let mut runtime_ext = RuntimeExt::new(
                state_update,
                sender_account_id,
                hash.into()
            );
            let wasm_res = executor::execute(
                &sender.code,
                &method_name,
                args,
                &[],
                &mut runtime_ext,
                &wasm::types::Config::default(),
                &RuntimeContext::new(
                    sender.amount - staked,
                    0,
                    sender_account_id,
                    sender_account_id,
                    DEFAULT_MANA_LIMIT,
                ),
            ).map_err(|e| format!("wasm execution failed with error: {:?}", e))?;
            sender.amount = wasm_res.balance + staked;
            let receipts = runtime_ext.get_receipts();
            self.callbacks.extend(runtime_ext.callbacks);
            receipts
        };
        set(
            state_update,
            &account_id_to_bytes(sender_account_id),
            &sender
        );        
        Ok(receipts)
    }   

    /// node receives signed_transaction, processes it
    /// and generates the receipt to send to receiver
    fn apply_signed_transaction(
        &mut self,
        state_update: &mut StateDbUpdate,
        transaction: &SignedTransaction,
        authority_proposals: &mut Vec<AuthorityProposal>,
    ) -> Result<Vec<Transaction>, String> {
        let runtime_data: Option<RuntimeData> = get(state_update, RUNTIME_DATA);
        let sender: Option<Account> =
            get(state_update, &account_id_to_bytes(transaction.body.get_sender()));
        match (runtime_data, sender) {
            (Some(mut runtime_data), Some(mut sender)) => {
                if transaction.body.get_nonce() <= sender.nonce {
                    return Err(format!(
                        "Transaction nonce {} is invalid",
                        transaction.body.get_nonce()
                    ));
                }
                sender.nonce = transaction.body.get_nonce();
                match transaction.body {
                    TransactionBody::SendMoney(ref t) => {
                        self.send_money(
                            state_update,
                            &t,
                            transaction.hash,
                            &mut sender,
                            &mut runtime_data,
                        )
                    },
                    TransactionBody::Stake(ref t) => {
                        self.staking(
                            state_update,
                            &t,
                            &mut sender,
                            &mut runtime_data,
                            authority_proposals,
                        )
                    },
                    TransactionBody::FunctionCall(ref t) => {
                        self.call_function(
                            state_update,
                            &runtime_data,
                            &mut sender,
                            transaction.body.get_sender(),
                            transaction.hash,
                            &t.method_name,
                            &t.args,
                        )
                    },
                    TransactionBody::DeployContract(ref t) => {
                        self.deploy(
                            state_update,
                            t,
                            &mut sender,
                        )
                    },
                    TransactionBody::CreateAccount(ref t) => {
                        self.create_account(
                            state_update,
                            t,
                            transaction.hash,
                            &mut sender,
                            &mut runtime_data
                        )
                    },
                    TransactionBody::SwapKey(ref t) => {
                        // this is super redundant. need to change when we add signature checks
                        let data = transaction.body.encode().ok_or("cannot encode body")?;
                        self.swap_key(
                            state_update,
                            t,
                            &transaction.sender_sig,
                            &data,
                            &mut sender,
                        )
                    }
                }
            }
            (None, _) => Err("runtime data does not exist".to_string()),
            _ => Err(format!("sender {} does not exist", transaction.body.get_sender()))
        }
    }

    fn deposit(
        &self,
        state_update: &mut StateDbUpdate,
        amount: u64,
        receiver_id: AccountId,
        receiver: &mut Account
    ) -> Result<Vec<Transaction>, String> {
        receiver.amount += amount;
        set(
            state_update,
            &account_id_to_bytes(receiver_id),
            receiver
        );
        Ok(vec![])
    }

    fn system_create_account(
        &self,
        state_update: &mut StateDbUpdate,
        call: &AsyncCall,
        account_id: AccountId,
    ) -> Result<Vec<Transaction>, String> {
        let account_id_bytes = account_id_to_bytes(account_id);
        match get::<Account>(state_update, &account_id_bytes) {
            Some(_) => {
                // this case should already be handled
                unreachable!()
            }
            _ => {
                let public_key = Decode::decode(&call.args).ok_or("cannot decode public key")?;
                let new_account = Account::new(
                    vec![public_key],
                    call.amount,
                    vec![]
                );
                set(
                    state_update,
                    &account_id_bytes,
                    &new_account
                );
                Ok(vec![])
            }
        }
    }

    fn return_data_to_receipts(
        runtime_ext: &mut RuntimeExt,
        return_data: ReturnData,
        callback_info: &Option<CallbackInfo>,
        sender_id: AccountId,
        receiver_id: AccountId,
    ) -> Result<Vec<Transaction>, String> {
        let callback_info = match callback_info {
            Some(info) => info,
            _ => {
                let receipts = runtime_ext.get_receipts();
                return Ok(receipts);
            }
        };
        let callback_res = match return_data {
            ReturnData::Value(v) => {
                let res = CallbackResult::new(
                    callback_info.clone(),
                    Some(v),
                );
                Some(res)
            }
            ReturnData::None => {
                let res = CallbackResult::new(
                    callback_info.clone(),
                    Some(vec![]),
                );
                Some(res)
            }
            ReturnData::Promise(PromiseId::Callback(id)) => {
                let callback = runtime_ext.callbacks.get_mut(&id).expect("callback must exist");
                if callback.callback.is_some() {
                    unreachable!("callback already has callback");
                } else {
                    callback.callback = Some(callback_info.clone());
                }
                None
            }
            ReturnData::Promise(PromiseId::Receipt(id)) => {
                let receipt = runtime_ext.receipts.get_mut(&id).expect("receipt must exist");
                match receipt.body {
                    ReceiptBody::NewCall(ref mut call) => {
                        if call.callback.is_some() {
                            return Err("receipt already has callback".to_string());
                        } else {
                            call.callback = Some(callback_info.clone());
                        }
                    }
                    _ => unreachable!("receipt body is not new call")
                }
                None
            }
            _ => return Err("return data is a non-callback promise".to_string())
        };
        let mut receipts = runtime_ext.get_receipts();
        if let Some(callback_res) = callback_res {
            let new_receipt = ReceiptTransaction::new(
                receiver_id,
                sender_id,
                runtime_ext.create_nonce(),
                ReceiptBody::Callback(callback_res),
            );
            receipts.push(Transaction::Receipt(new_receipt));
        }
        Ok(receipts)
    }

    fn apply_async_call(
        &mut self,
        state_update: &mut StateDbUpdate,
        runtime_data: &RuntimeData,
        async_call: &AsyncCall,
        sender_id: AccountId,
        receiver_id: AccountId,
        nonce: Vec<u8>,
        receiver: &mut Account,
    ) -> Result<Vec<Transaction>, String> {
        let staked = runtime_data.at_stake(receiver_id);
        assert!(receiver.amount >= staked);
        let result = {
            let mut runtime_ext = RuntimeExt::new(
                state_update,
                sender_id,
                nonce,
            );
            let wasm_res = executor::execute(
                &receiver.code,
                &async_call.method_name,
                &async_call.args,
                &[],
                &mut runtime_ext,
                &wasm::types::Config::default(),
                &RuntimeContext::new(
                    receiver.amount - staked,
                    async_call.amount,
                    sender_id,
                    receiver_id,
                    async_call.mana,
                ),
            ).map_err(|e| format!("wasm exeuction failed with error: {:?}", e))?;
            let result = Self::return_data_to_receipts(
                &mut runtime_ext,
                wasm_res.return_data,                    
                &async_call.callback,
                sender_id,
                receiver_id,
            );
            if result.is_ok() {
                receiver.amount = wasm_res.balance + staked;
            }
            result
        };
        set(
            state_update,
            &account_id_to_bytes(receiver_id),
            receiver,
        );
        result
    }

    fn apply_callback(
        &mut self,
        state_update: &mut StateDbUpdate,
        runtime_data: &RuntimeData,
        callback_res: &CallbackResult,
        sender_id: AccountId,
        receiver_id: AccountId,
        nonce: Vec<u8>,
        receiver: &mut Account,
    ) -> Result<Vec<Transaction>, String> {
        let staked = runtime_data.at_stake(receiver_id);
        assert!(receiver.amount >= staked);
        let mut needs_removal = false;
        let receipts = {
            let mut runtime_ext = RuntimeExt::new(
                state_update,
                sender_id,
                nonce,
            );
        
            match self.callbacks.get_mut(&callback_res.info.id) {
                Some(callback) => {
                    callback.results[callback_res.info.result_index] = callback_res.result.clone();
                    callback.result_counter += 1;
                    // if we have gathered all results, execute the callback
                    if callback.result_counter == callback.results.len() {
                        let wasm_res = executor::execute(
                            &receiver.code,
                            &callback.method_name,
                            &callback.args,
                            &callback.results,
                            &mut runtime_ext,
                            &wasm::types::Config::default(),
                            &RuntimeContext::new(
                                receiver.amount - staked,
                                0,
                                sender_id,
                                receiver_id,
                                callback.mana,
                            ),
                        ).map_err(|e| format!("wasm exeuction failed with error: {:?}", e))?;
                        needs_removal = true;
                        let balance = wasm_res.balance;
                        Self::return_data_to_receipts(
                            &mut runtime_ext,
                            wasm_res.return_data,
                            &callback.callback,
                            sender_id,
                            receiver_id,
                        ).and_then(|receipts| {
                            receiver.amount = balance + staked;
                            Ok(receipts)
                        })?
                    } else {
                        // otherwise no receipt is generated
                        vec![]
                    }
                },
                _ => {
                    return Err(format!("callback id: {:?} not found", callback_res.info.id));
                }
            }
        };
        
        if needs_removal {
            self.callbacks.remove(&callback_res.info.id);
            set(
                state_update,
                &account_id_to_bytes(receiver_id),
                receiver
            );
        }
        Ok(receipts)
    }

    fn apply_receipt(
        &mut self,
        state_update: &mut StateDbUpdate,
        receipt: &ReceiptTransaction,
        new_receipts: &mut Vec<Transaction>,
    ) -> Result<(), String> {
        let receiver_id = account_id_to_bytes(receipt.receiver);
        let receiver: Option<Account> = get(state_update, &receiver_id);
        let runtime_data: RuntimeData = 
            get(state_update, RUNTIME_DATA).ok_or("runtime data does not exist")?;
        let mut amount = 0;
        let mut callback_info = None;
        let mut receiver_exists = true;
        let result = match receiver {
            Some(mut receiver) => {
                match &receipt.body {
                    ReceiptBody::NewCall(async_call) => {
                        amount = async_call.amount;
                        if async_call.method_name == b"deposit".to_vec() {
                            self.deposit(
                                state_update,
                                async_call.amount,
                                receipt.receiver,
                                &mut receiver
                            )
                        } else if async_call.method_name == b"create_account".to_vec() {
                            debug!(target: "runtime", "account {} already exists", receipt.receiver);
                            let receipt = ReceiptTransaction::new(
                                system_account(),
                                receipt.sender,
                                create_nonce_with_nonce(&receipt.nonce, 0),
                                ReceiptBody::Refund(async_call.amount)
                            );
                            Ok(vec![Transaction::Receipt(receipt)])
                        } else {
                            callback_info = async_call.callback.clone();
                            self.apply_async_call(
                                state_update,
                                &runtime_data,
                                &async_call,
                                receipt.sender,
                                receipt.receiver,
                                receipt.nonce.clone(),
                                &mut receiver,
                            )
                        }
                    },
                    ReceiptBody::Callback(callback_res) => {
                        callback_info = Some(callback_res.info.clone());
                        self.apply_callback(
                            state_update,
                            &runtime_data,
                            &callback_res,
                            receipt.sender,
                            receipt.receiver,
                            receipt.nonce.clone(),
                            &mut receiver,
                        )
                    }
                    ReceiptBody::Refund(amount) => {
                        receiver.amount += amount;
                        set(
                            state_update,
                            &receiver_id,
                            &receiver,
                        );
                        Ok(vec![])
                    }
                }
            }
            _ => {
                receiver_exists = false;
                let err = Err(format!("receiver {} does not exist", receipt.receiver));
                if let ReceiptBody::NewCall(call) = &receipt.body {
                    amount = call.amount;
                    if call.method_name == b"create_account".to_vec() {
                        self.system_create_account(
                            state_update,
                            &call,
                            receipt.receiver,
                        )
                    } else {
                        err
                    }
                } else {
                    err
                }
            }
        };
        match result {
            Ok(mut receipts) => {
                new_receipts.append(&mut receipts);
                Ok(())
            }
            Err(s) => {
                if amount > 0 {
                    let receiver = if receiver_exists {
                        receipt.receiver
                    } else {
                        system_account()
                    };
                    let new_receipt = ReceiptTransaction::new(
                        receiver,
                        receipt.sender,
                        create_nonce_with_nonce(&receipt.nonce, 0),
                        ReceiptBody::Refund(amount)
                    );
                    new_receipts.push(Transaction::Receipt(new_receipt));
                }
                if let Some(callback_info) = callback_info {
                    let new_receipt = ReceiptTransaction::new(
                        receipt.receiver,
                        callback_info.receiver,
                        create_nonce_with_nonce(&receipt.nonce, 1),
                        ReceiptBody::Callback(CallbackResult::new(
                            callback_info,
                            None,
                        ))
                    );
                    new_receipts.push(Transaction::Receipt(new_receipt));
                }
                Err(s)
            }
        }
    }

    fn filter_transaction(
        runtime: &mut Self,
        state_update: &mut StateDbUpdate,
        shard_id: ShardId,
        transaction: &Transaction,
        new_receipts: &mut Vec<Transaction>,
        authority_proposals: &mut Vec<AuthorityProposal>,
    ) -> bool {
        match transaction {
            Transaction::SignedTransaction(ref tx) => {
                match runtime.apply_signed_transaction(
                    state_update,
                    tx,
                    authority_proposals
                ) {
                    Ok(mut receipts) => {
                        new_receipts.append(&mut receipts);
                        state_update.commit();
                        true
                    }
                    Err(s) => {
                        debug!(target: "runtime", "{}", s);
                        state_update.rollback();
                        false
                    }
                }
            }
            Transaction::Receipt(ref r) => {
                if account_to_shard_id(r.receiver) == shard_id {
                    let mut tmp_new_receipts = vec![];
                    match runtime.apply_receipt(state_update, r, &mut tmp_new_receipts) {
                        Ok(()) => {
                            state_update.commit();
                            new_receipts.append(&mut tmp_new_receipts);
                            true
                        }
                        Err(s) => {
                            debug!(target: "runtime", "{}", s);
                            state_update.rollback();
                            new_receipts.append(&mut tmp_new_receipts);
                            false
                        }
                    }
                } else {
                    // wrong receipt
                    debug!(target: "runtime", "receipt sent to the wrong shard");
                    false
                }
            }
        }
    }

    /// apply receipts from previous block and transactions and receipts from this block
    pub fn apply(
        &mut self,
        apply_state: &ApplyState,
        prev_receipts: &[Transaction],
        mut transactions: Vec<Transaction>,
    ) -> ApplyResult {
        let mut new_receipts = vec![];
        let mut state_update = StateDbUpdate::new(self.state_db.clone(), apply_state.root);
        let mut authority_proposals = vec![];
        let shard_id = apply_state.shard_id;
        for receipt in prev_receipts.iter() {
            Self::filter_transaction(
                self,
                &mut state_update,
                shard_id,
                receipt,
                &mut new_receipts,
                &mut authority_proposals
            );
        }
        transactions.retain(|t| {
            Self::filter_transaction(
                self,
                &mut state_update,
                shard_id,
                t,
                &mut new_receipts,
                &mut authority_proposals
            )
        });
        let (transaction, new_root) = state_update.finalize();
        ApplyResult { 
            root: new_root, 
            transaction,
            authority_proposals,
            shard_id,
            filtered_transactions: transactions,
            new_receipts,
        }
    }

    pub fn apply_genesis_state(
        &self,
        balances: &[(AccountAlias, ReadablePublicKey, u64)],
        wasm_binary: &[u8],
        initial_authorities: &[(ReadablePublicKey, u64)]
    ) -> MerkleHash {
        let mut state_db_update =
            StateDbUpdate::new(self.state_db.clone(), MerkleHash::default());
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use primitives::hash::hash;
    use primitives::types::{
        DeployContractTransaction, FunctionCallTransaction,
        TransactionBody, ViewCall, ViewCallResult
    };
    use primitives::signature::{DEFAULT_SIGNATURE, get_keypair, sign};
    use primitives::utils::concat;
    use storage::test_utils::create_state_db;
    use test_utils::{
        encode_int, get_runtime_and_state_db_viewer, get_test_state_db_viewer
    };

    use super::*;

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
    fn test_simple_smart_contract() {
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.get_root();
        let tx_body = TransactionBody::FunctionCall(FunctionCallTransaction {
            nonce: 1,
            originator: hash(b"alice"),
            contract_id: hash(b"bob"),
            method_name: b"run_test".to_vec(),
            args: vec![],
        });
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state = ApplyState {
            root,
            shard_id: 0,
            parent_block_hash: CryptoHash::default(),
            block_index: 0
        };
        let apply_result = runtime.apply_all(
            apply_state, vec![Transaction::SignedTransaction(transaction)]
        );
        assert_eq!(apply_result.filtered_transactions.len(), 1);
        assert_eq!(apply_result.new_receipts.len(), 0);
        assert_ne!(root, apply_result.root);
    }

    #[test]
    fn test_simple_smart_contract_with_args() {
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.get_root();
        let tx_body = TransactionBody::FunctionCall(FunctionCallTransaction {
            nonce: 1,
            originator: hash(b"alice"),
            contract_id: hash(b"bob"),
            method_name: b"run_test".to_vec(),
            args: concat((2..4).into_iter().map(|x| encode_int(x).to_vec()).collect()),
        });
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state = ApplyState { 
            root,
            shard_id: 0,
            parent_block_hash: CryptoHash::default(),
            block_index: 0
        };
        let apply_result = runtime.apply_all(
            apply_state, vec![Transaction::SignedTransaction(transaction)],
        );
        assert_eq!(apply_result.filtered_transactions.len(), 1);
        assert_eq!(apply_result.new_receipts.len(), 0);
        assert_ne!(root, apply_result.root);
    }

    #[test]
    // we need to figure out how to deal with the case where account does not exist
    // especially in the context of sharding
    fn test_upload_contract() {
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.get_root();
        let (pub_key, _) = get_keypair();
        // first create a new account with no contract
        let tx_body = TransactionBody::CreateAccount(CreateAccountTransaction {
            nonce: 1,
            sender: hash(b"alice"),
            new_account_id: hash(b"eve"),
            amount: 10,
            public_key: pub_key.encode().unwrap(),
        });
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state = ApplyState { 
            root,
            shard_id: 0,
            parent_block_hash: CryptoHash::default(),
            block_index: 0
        };
        let mut apply_result = runtime.apply_all(
            apply_state, vec![Transaction::SignedTransaction(transaction)]
        );
        assert_eq!(apply_result.filtered_transactions.len(), 1);
        assert_eq!(apply_result.new_receipts.len(), 0);
        assert_ne!(root, apply_result.root);
        runtime.state_db.commit(&mut apply_result.transaction).unwrap();
        // deploy contract
        let wasm_binary = include_bytes!("../../../core/wasm/runtest/res/wasm_with_mem.wasm");
        let tx_body = TransactionBody::DeployContract(DeployContractTransaction{
            nonce: 1,
            contract_id: hash(b"eve"),
            wasm_byte_array: wasm_binary.to_vec(),
            public_key: pub_key.encode().unwrap(),
        });
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state = ApplyState {
            shard_id: 0,
            root: apply_result.root,
            parent_block_hash: CryptoHash::default(),
            block_index: 0 
        };
        let mut apply_result = runtime.apply(
            &apply_state, &[], vec![Transaction::SignedTransaction(transaction)],
        );
        runtime.state_db.commit(&mut apply_result.transaction).unwrap();
        let mut new_state_update = StateDbUpdate::new(runtime.state_db, apply_result.root);
        let new_account: Account = get(
            &mut new_state_update,
            &account_id_to_bytes(hash(b"eve"))
        ).unwrap();
        assert_eq!(new_account.code, wasm_binary.to_vec());
    }

    #[test]
    fn test_redeploy_contract() {
        let test_binary = b"test_binary";
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.get_root();
        let mut state_update = StateDbUpdate::new(runtime.state_db.clone(), root);
        let account: Account = get(
            &mut state_update,
            &account_id_to_bytes(hash(b"bob"))
        ).unwrap();
        let tx_body = TransactionBody::DeployContract(DeployContractTransaction{
            nonce: 1,
            contract_id: hash(b"bob"),
            wasm_byte_array: test_binary.to_vec(),
            public_key: account.public_keys[0].encode().unwrap(),
        });
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state = ApplyState {
            root,
            shard_id: 0,
            parent_block_hash: CryptoHash::default(),
            block_index: 0
        };
        let mut apply_result = runtime.apply(
            &apply_state, &[], vec![Transaction::SignedTransaction(transaction)],
        );
        assert_eq!(apply_result.filtered_transactions.len(), 1);
        assert_eq!(apply_result.new_receipts.len(), 0);
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
        let root = viewer.get_root();
        let tx_body = TransactionBody::SendMoney(SendMoneyTransaction {
            nonce: 1,
            sender: hash(b"alice"),
            receiver: hash(b"bob"),
            amount: 10,
        });
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state = ApplyState {
            root,
            shard_id: 0,
            parent_block_hash: CryptoHash::default(),
            block_index: 0
        };
        let mut apply_result = runtime.apply_all(
            apply_state, vec![Transaction::SignedTransaction(transaction)]
        );
        assert_eq!(apply_result.filtered_transactions.len(), 1);
        assert_eq!(apply_result.new_receipts.len(), 0);
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
    fn test_send_money_failure() {
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.get_root();
        let tx_body = TransactionBody::SendMoney(SendMoneyTransaction {
            nonce: 1,
            sender: hash(b"alice"),
            receiver: hash(b"bob"),
            amount: 1000,
        });
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state = ApplyState {
            root,
            shard_id: 0,
            parent_block_hash: CryptoHash::default(),
            block_index: 0
        };
        let mut apply_result = runtime.apply(
            &apply_state, &[], vec![Transaction::SignedTransaction(transaction)]
        );
        assert_eq!(apply_result.filtered_transactions.len(), 0);
        assert_eq!(apply_result.new_receipts.len(), 0);
        assert_eq!(root, apply_result.root);
        runtime.state_db.commit(&mut apply_result.transaction).unwrap();
        let result1 = viewer.view_at(
            &ViewCall::balance(hash(b"alice")),
            apply_result.root,
        );
        assert_eq!(
            result1,
            ViewCallResult {
                nonce: 0,
                account: hash(b"alice"),
                amount: 100,
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
                amount: 0,
                stake: 0,
                result: vec![],
            }
        );
    }

    #[test]
    fn test_send_money_refund() {
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.get_root();
        let tx_body = TransactionBody::SendMoney(SendMoneyTransaction {
            nonce: 1,
            sender: hash(b"alice"),
            receiver: hash(b"eve"),
            amount: 10,
        });
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state = ApplyState {
            root,
            shard_id: 0,
            parent_block_hash: CryptoHash::default(),
            block_index: 0
        };
        let mut apply_result = runtime.apply_all(
            apply_state, vec![Transaction::SignedTransaction(transaction)]
        );
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
                amount: 100,
                stake: 50,
                result: vec![],
            }
        );
        let result2 = viewer.view_at(
            &ViewCall::balance(hash(b"eve")),
            apply_result.root,
        );
        assert_eq!(
            result2,
            ViewCallResult {
                nonce: 0,
                account: hash(b"eve"),
                amount: 0,
                stake: 0,
                result: vec![],
            }
        );
    }

    #[test]
    fn test_create_account() {
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.get_root();
        let (pub_key, _) = get_keypair();
        let tx_body = TransactionBody::CreateAccount(CreateAccountTransaction {
            nonce: 1,
            sender: hash(b"alice"),
            new_account_id: hash(b"eve"),
            amount: 10,
            public_key: pub_key.encode().unwrap()
        });
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state = ApplyState {
            root,
            shard_id: 0,
            parent_block_hash: CryptoHash::default(),
            block_index: 0
        };
        let mut apply_result = runtime.apply_all(
            apply_state, vec![Transaction::SignedTransaction(transaction)]
        );
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
            &ViewCall::balance(hash(b"eve")),
            apply_result.root,
        );
        assert_eq!(
            result2,
            ViewCallResult {
                nonce: 0,
                account: hash(b"eve"),
                amount: 10,
                stake: 0,
                result: vec![],
            }
        );
    }

    #[test]
    fn test_create_account_failure() {
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.get_root();
        let (pub_key, _) = get_keypair();
        let tx_body = TransactionBody::CreateAccount(CreateAccountTransaction {
            nonce: 1,
            sender: hash(b"alice"),
            new_account_id: hash(b"bob"),
            amount: 10,
            public_key: pub_key.encode().unwrap()
        });
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state = ApplyState {
            root,
            shard_id: 0,
            parent_block_hash: CryptoHash::default(),
            block_index: 0
        };
        let mut apply_result = runtime.apply_all(
            apply_state, vec![Transaction::SignedTransaction(transaction)]
        );
        assert_ne!(root, apply_result.root);
        runtime.state_db.commit(&mut apply_result.transaction).unwrap();
        //let apply_state = ApplyState {
        //    root: apply_result.root,
        //    shard_id: 0,
        //    parent_block_hash: CryptoHash::default(),
        //    block_index: 0
        //};
        //let mut apply_result = runtime.apply(
        //    &apply_state, apply_result.new_receipts
        //);
        //assert_eq!(apply_result.filtered_transactions.len(), 0);
        //assert_eq!(apply_result.new_receipts.len(), 0);
        //assert_ne!(root, apply_result.root);
        //runtime.state_db.commit(&mut apply_result.transaction).unwrap();
        let result1 = viewer.view_at(
            &ViewCall::balance(hash(b"alice")),
            apply_result.root,
        );
        assert_eq!(
            result1,
            ViewCallResult {
                nonce: 1,
                account: hash(b"alice"),
                amount: 100,
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
                amount: 0,
                stake: 0,
                result: vec![],
            }
        );
    }

    #[test]
    fn test_swap_key() {
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.get_root();
        let (pub_key1, secret_key1) = get_keypair();
        let (pub_key2, _) = get_keypair();
        let tx_body = TransactionBody::CreateAccount(CreateAccountTransaction {
            nonce: 1,
            sender: hash(b"alice"),
            new_account_id: hash(b"eve"),
            amount: 10,
            public_key: pub_key1.encode().unwrap()
        });
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state = ApplyState {
            root,
            shard_id: 0,
            parent_block_hash: CryptoHash::default(),
            block_index: 0
        };
        let mut apply_result = runtime.apply_all(
            apply_state, vec![Transaction::SignedTransaction(transaction)]
        );
        assert_eq!(apply_result.filtered_transactions.len(), 1);
        assert_eq!(apply_result.new_receipts.len(), 0);
        assert_ne!(root, apply_result.root);
        runtime.state_db.commit(&mut apply_result.transaction).unwrap();
        let tx_body = TransactionBody::SwapKey(SwapKeyTransaction {
            nonce: 2,
            sender: hash(b"eve"),
            cur_key: pub_key1.encode().unwrap(),
            new_key: pub_key2.encode().unwrap(),
        });
        let data = tx_body.encode().unwrap();
        let signature = sign(&data, &secret_key1);
        let transaction1 = SignedTransaction::new(signature, tx_body);
        let apply_state = ApplyState {
            shard_id: 0,
            root: apply_result.root,
            parent_block_hash: CryptoHash::default(),
            block_index: 0,
        };
        let mut apply_result = runtime.apply(
            &apply_state, &[], vec![Transaction::SignedTransaction(transaction1)],
        );
        runtime.state_db.commit(&mut apply_result.transaction).unwrap();
        let mut new_state_update = StateDbUpdate::new(runtime.state_db.clone(), apply_result.root);
        let account = get::<Account>(
            &mut new_state_update,
            &account_id_to_bytes(hash(b"eve")),
        ).unwrap();
        assert_eq!(account.public_keys, vec![pub_key2]);
    }

    #[test]
    fn test_async_call_with_no_callback() {
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.get_root();
        let receipt = ReceiptTransaction::new(
            hash(b"alice"),
            hash(b"bob"),
            hash(&[1, 2, 3]).into(),
            ReceiptBody::NewCall(AsyncCall::new(
                b"run_test".to_vec(),
                vec![],
                0,
                0,
            ))
        );
        let apply_state = ApplyState {
            root,
            shard_id: 0,
            parent_block_hash: CryptoHash::default(),
            block_index: 0
        };
        let apply_result = runtime.apply_all(
            apply_state, vec![Transaction::Receipt(receipt)]
        );
        assert_eq!(apply_result.filtered_transactions.len(), 1);
        assert_eq!(apply_result.new_receipts.len(), 0);
        assert_ne!(root, apply_result.root);
    }

    #[test]
    fn test_async_call_with_callback() {
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.get_root();
        let args = concat((7..9).into_iter().map(|x| encode_int(x).to_vec()).collect());
        let mut callback = Callback::new(b"sum_with_input".to_vec(), args, 0);
        callback.results.resize(1, None);
        let callback_id = [0; 32].to_vec();
        let mut async_call = AsyncCall::new(b"run_test".to_vec(), vec![], 0, 0);
        let callback_info = CallbackInfo::new(callback_id.clone(), 0, hash(b"alice"));
        async_call.callback = Some(callback_info.clone());
        let receipt = ReceiptTransaction::new(
            hash(b"alice"),
            hash(b"bob"),
            hash(&[1, 2, 3]).into(),
            ReceiptBody::NewCall(async_call),
        );
        let mut state_update = StateDbUpdate::new(runtime.state_db.clone(), root);
        let mut new_receipts = vec![];
        runtime.apply_receipt(&mut state_update, &receipt, &mut new_receipts).unwrap();
        assert_eq!(new_receipts.len(), 1);
        if let Transaction::Receipt(new_receipt) = &new_receipts[0] {
            assert_eq!(new_receipt.sender, hash(b"bob"));
            assert_eq!(new_receipt.receiver, hash(b"alice"));
            let callback_res = CallbackResult::new(
                callback_info.clone(), Some(encode_int(20).to_vec())
            );
            assert_eq!(new_receipt.body, ReceiptBody::Callback(callback_res));
        } else {
            assert!(false);
        }
        
    }

    #[test]
    fn test_callback() {
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.get_root();
        let args = concat((7..9).into_iter().map(|x| encode_int(x).to_vec()).collect());
        let mut callback = Callback::new(b"sum_with_input".to_vec(), args, 0);
        callback.results.resize(1, None);
        let callback_id = [0; 32].to_vec();
        runtime.callbacks.insert(callback_id.clone(), callback);
        let receipt = ReceiptTransaction::new(
            hash(b"alice"),
            hash(b"bob"),
            hash(&[1, 2, 3]).into(),
            ReceiptBody::Callback(CallbackResult::new(
                CallbackInfo::new(callback_id.clone(), 0, hash(b"alice")),
                None,
            ))
        );
        let apply_state = ApplyState {
            root,
            shard_id: 0,
            parent_block_hash: CryptoHash::default(),
            block_index: 0
        };
        let apply_result = runtime.apply_all(
            apply_state, vec![Transaction::Receipt(receipt)]
        );
        assert_eq!(runtime.callbacks.len(), 0);
        assert_eq!(root, apply_result.root);
    }
}

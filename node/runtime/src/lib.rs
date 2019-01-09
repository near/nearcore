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

use beacon::authority::AuthorityStake;
use ext::RuntimeExt;
use primitives::hash::{CryptoHash, hash};
use primitives::signature::{PublicKey, Signature, verify};
use primitives::traits::{Decode, Encode};
use primitives::types::{
    AccountId, MerkleHash, ReadablePublicKey, SignedTransaction, TransactionBody,
    ReceiptTransaction, ReceiptBody, AsyncCall, CallbackResult, CallbackInfo, Callback,
    PromiseId, StakeTransaction, SendMoneyTransaction, CreateAccountTransaction,
    SwapKeyTransaction, DeployContractTransaction, Balance, Transaction, ShardId,
    FunctionCallTransaction, AccountingInfo, ManaAccounting, Mana, BlockIndex,
};
use primitives::utils::{
    account_to_shard_id, index_to_bytes, is_valid_account_id
};
use storage::{StateDb, StateDbUpdate};
use wasm::executor;
use wasm::types::{RuntimeContext, ReturnData};

pub mod chain_spec;
pub mod test_utils;
pub mod state_viewer;
mod tx_stakes;
use tx_stakes::{TxStakeConfig, TxTotalStake, get_tx_stake_key};
mod ext;

const COL_ACCOUNT: &[u8] = &[0];
const COL_CALLBACK: &[u8] = &[1];
const COL_CODE: &[u8] = &[2];
const COL_TX_STAKE: &[u8] = &[3];
const COL_TX_STAKE_SEPARATOR: &[u8] = &[4];

// const does not allow function call, so have to resort to this
fn system_account() -> AccountId {
    "system".to_string()
}

/// Per account information stored in the state.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct Account {
    pub public_keys: Vec<PublicKey>,
    pub nonce: u64,
    // amount + staked is the total value of the account
    pub amount: u64,
    pub staked: u64,
    pub code_hash: CryptoHash,
}

impl Account {
    pub fn new(public_keys: Vec<PublicKey>, amount: Balance, code_hash: CryptoHash) -> Self {
        Account { public_keys, nonce: 0, amount, staked: 0, code_hash }
    }
}

fn account_id_to_bytes(col: &[u8], account_key: &AccountId) -> Vec<u8> {
    let mut key = col.to_vec();
    key.append(&mut account_key.clone().into_bytes());
    key
}

fn callback_id_to_bytes(id: &[u8]) -> Vec<u8> {
    let mut key = COL_CALLBACK.to_vec();
    key.extend_from_slice(id);
    key
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
    pub transaction: storage::DBChanges,
    pub authority_proposals: Vec<AuthorityStake>,
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
    pub state_db: Arc<StateDb>,
}

impl Runtime {
    pub fn new(state_db: Arc<StateDb>) -> Self {
        Runtime { state_db }
    }

    fn try_charge_mana(
        &self,
        state_update: &mut StateDbUpdate,
        block_index: BlockIndex,
        originator: &AccountId,
        contract_id: &Option<AccountId>,
        mana: Mana,
    ) -> Option<AccountingInfo> {
        let config = TxStakeConfig::default();
        let mut acc_info_options = Vec::new();
        // Trying to use contract specific quota first
        if let Some(ref contract_id) = contract_id {
            acc_info_options.push(AccountingInfo{
                originator: originator.clone(),
                contract_id: Some(contract_id.clone()),
            });
        }
        // Trying to use global quota
        acc_info_options.push(AccountingInfo{
            originator: originator.clone(),
            contract_id: None,
        });
        for accounting_info in acc_info_options {
            let key = get_tx_stake_key(
                &accounting_info.originator,
                &accounting_info.contract_id,
            );
            let tx_total_stake: Option<TxTotalStake> = get(state_update, &key);
            if let Some(mut tx_total_stake) = tx_total_stake {
                tx_total_stake.update(block_index, &config);
                if tx_total_stake.available_mana(&config) >= mana {
                    tx_total_stake.charge_mana(mana, &config);
                    set(state_update, &key, &tx_total_stake);
                    return Some(accounting_info)
                }
            }
        }
        None
    }

    fn send_money(
        &self,
        state_update: &mut StateDbUpdate,
        transaction: &SendMoneyTransaction,
        hash: CryptoHash,
        sender: &mut Account,
        accounting_info: AccountingInfo,
    ) -> Result<Vec<Transaction>, String> {
        if sender.amount >= transaction.amount {
            sender.amount -= transaction.amount;
            set(state_update, &account_id_to_bytes(COL_ACCOUNT, &transaction.originator), sender);
            let receipt = ReceiptTransaction::new(
                transaction.originator.clone(),
                transaction.receiver.clone(),
                hash.into(),
                ReceiptBody::NewCall(AsyncCall::new(
                    b"deposit".to_vec(),
                    vec![],
                    transaction.amount,
                    0,
                    accounting_info,
                ))
            );
            Ok(vec![Transaction::Receipt(receipt)])
        } else {
            Err(
                format!(
                    "Account {} tries to send {}, but has staked {} and only has {}",
                    transaction.originator,
                    transaction.amount,
                    sender.staked,
                    sender.amount,
                )
            )
        }
    }

    fn staking(
        &self,
        state_update: &mut StateDbUpdate,
        body: &StakeTransaction,
        sender_account_id: &AccountId,
        sender: &mut Account,
        authority_proposals: &mut Vec<AuthorityStake>,
    ) -> Result<Vec<Transaction>, String> {
        if sender.amount >= body.amount && sender.public_keys.is_empty() {
            authority_proposals.push(AuthorityStake {
                account_id: sender_account_id.clone(),
                public_key: sender.public_keys[0],
                amount: body.amount,
            });
            sender.amount -= body.amount;
            sender.staked += body.amount;
            set(state_update, &account_id_to_bytes(COL_ACCOUNT, sender_account_id), &sender);
            Ok(vec![])
        } else if sender.amount < body.amount {
            let err_msg = format!(
                "Account {} tries to stake {}, but has staked {} and only has {}",
                body.originator,
                body.amount,
                sender.staked,
                sender.amount,
            );
            Err(err_msg)
        } else {
            Err(format!("Account {} already staked", body.originator))
        }
    }

    fn create_account(
        &self,
        state_update: &mut StateDbUpdate,
        body: &CreateAccountTransaction,
        hash: CryptoHash,
        sender: &mut Account,
        accounting_info: AccountingInfo,
    ) -> Result<Vec<Transaction>, String> {
        if !is_valid_account_id(&body.new_account_id) {
            return Err(format!("Account {} does not match requirements", body.new_account_id));
        }
        if sender.amount >= body.amount {
            sender.amount -= body.amount;
            set(
                state_update,
                &account_id_to_bytes(COL_ACCOUNT, &body.originator),
                &sender
            );
            let new_nonce = create_nonce_with_nonce(hash.as_ref(), 0);
            let receipt = ReceiptTransaction::new(
                body.originator.clone(),
                body.new_account_id.clone(),
                new_nonce,
                ReceiptBody::NewCall(AsyncCall::new(
                    b"create_account".to_vec(),
                    body.public_key.clone(),
                    body.amount,
                    0,
                    accounting_info,
                ))
            );
            Ok(vec![Transaction::Receipt(receipt)])
        } else {
            Err(
                format!(
                    "Account {} tries to create new account with {}, but only has {}",
                    body.originator,
                    body.amount,
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
            return Err(format!("Account {} does not have public key {}", body.originator, cur_key));
        }
        account.public_keys.push(new_key);
        set(
            state_update,
            &account_id_to_bytes(COL_ACCOUNT, &body.originator),
            &account
        );
        Ok(vec![])
    }

    fn deploy(
        &self,
        body: &DeployContractTransaction,
        hash: CryptoHash,
        accounting_info: AccountingInfo,
    ) -> Result<Vec<Transaction>, String> {
        // TODO: check signature
        
        let new_nonce = create_nonce_with_nonce(hash.as_ref(), 0);
        let args = Encode::encode(&(&body.public_key, &body.wasm_byte_array))
            .ok_or("cannot encode args")?;
        let receipt = ReceiptTransaction::new(
            body.originator.clone(),
            body.contract_id.clone(),
            new_nonce,
            ReceiptBody::NewCall(AsyncCall::new(
                b"deploy".to_vec(),
                args,
                0,
                0,
                accounting_info,
            ))
        );
        Ok(vec![Transaction::Receipt(receipt)])
    }

    fn call_function(
        &self,
        state_update: &mut StateDbUpdate,
        transaction: &FunctionCallTransaction,
        hash: CryptoHash,
        sender: &mut Account,
        accounting_info: AccountingInfo,
        mana: Mana,
    ) -> Result<Vec<Transaction>, String> {
        if sender.amount >= transaction.amount {
            sender.amount -= transaction.amount;
            set(state_update, &account_id_to_bytes(COL_ACCOUNT, &transaction.originator), sender);
            let receipt = ReceiptTransaction::new(
                transaction.originator.clone(),
                transaction.contract_id.clone(),
                hash.into(),
                ReceiptBody::NewCall(AsyncCall::new(
                    transaction.method_name.clone(),
                    transaction.args.clone(),
                    transaction.amount,
                    mana - 1,
                    accounting_info,
                ))
            );
            Ok(vec![Transaction::Receipt(receipt)])
        } else {
            Err(
                format!(
                    "Account {} tries to call some contract with the amount {}, but has staked {} and only has {}",
                    transaction.originator,
                    transaction.amount,
                    sender.staked,
                    sender.amount
                )
            )
        }
    }

    /// node receives signed_transaction, processes it
    /// and generates the receipt to send to receiver
    fn apply_signed_transaction(
        &mut self,
        state_update: &mut StateDbUpdate,
        block_index: BlockIndex,
        transaction: &SignedTransaction,
        authority_proposals: &mut Vec<AuthorityStake>,
    ) -> Result<Vec<Transaction>, String> {
        let sender_account_id = transaction.body.get_originator();
        let sender: Option<Account> =
            get(state_update, &account_id_to_bytes(COL_ACCOUNT, &sender_account_id));
        match sender {
            Some(mut sender) => {
                if transaction.body.get_nonce() <= sender.nonce {
                    return Err(format!(
                        "Transaction nonce {} must be larger than sender nonce {}",
                        transaction.body.get_nonce(),
                        sender.nonce,
                    ));
                }
                sender.nonce = transaction.body.get_nonce();
                set(
                    state_update,
                    &account_id_to_bytes(COL_ACCOUNT, &sender_account_id),
                    &sender
                );
                let contract_id = transaction.body.get_contract_id();
                let mana = transaction.body.get_mana();
                let accounting_info = self.try_charge_mana(
                    state_update,
                    block_index,
                    &sender_account_id,
                    &contract_id,
                    mana,
                ).ok_or_else(|| format!("sender {} does not have enough mana {}", sender_account_id, mana))?;
                match transaction.body {
                    TransactionBody::SendMoney(ref t) => {
                        self.send_money(
                            state_update,
                            &t,
                            transaction.transaction_hash(),
                            &mut sender,
                            accounting_info,
                        )
                    },
                    TransactionBody::Stake(ref t) => {
                        self.staking(
                            state_update,
                            &t,
                            &sender_account_id,
                            &mut sender,
                            authority_proposals,
                        )
                    },
                    TransactionBody::FunctionCall(ref t) => {
                        self.call_function(
                            state_update,
                            &t,
                            transaction.transaction_hash(),
                            &mut sender,
                            accounting_info,
                            mana,
                        )
                    },
                    TransactionBody::DeployContract(ref t) => {
                        self.deploy(
                            t,
                            transaction.transaction_hash(),
                            accounting_info,
                        )
                    },
                    TransactionBody::CreateAccount(ref t) => {
                        self.create_account(
                            state_update,
                            t,
                            transaction.transaction_hash(),
                            &mut sender,
                            accounting_info,
                        )
                    },
                    TransactionBody::SwapKey(ref t) => {
                        // this is super redundant. need to change when we add signature checks
                        let data = transaction.body.encode().ok_or("cannot encode body")?;
                        self.swap_key(
                            state_update,
                            t,
                            &transaction.signature,
                            &data,
                            &mut sender,
                        )
                    }
                }
            }
            _ => Err(format!("sender {} does not exist", sender_account_id))
        }
    }

    fn deposit(
        &self,
        state_update: &mut StateDbUpdate,
        amount: u64,
        receiver_id: &AccountId,
        receiver: &mut Account
    ) -> Result<Vec<Transaction>, String> {
        receiver.amount += amount;
        set(
            state_update,
            &account_id_to_bytes(COL_ACCOUNT, &receiver_id),
            receiver
        );
        Ok(vec![])
    }

    fn system_create_account(
        &self,
        state_update: &mut StateDbUpdate,
        call: &AsyncCall,
        account_id: &AccountId,
    ) -> Result<Vec<Transaction>, String> {
        if !is_valid_account_id(account_id) {
            return Err(format!("Account {} does not match requirements", account_id));
        }
        let account_id_bytes = account_id_to_bytes(COL_ACCOUNT, &account_id);
       
        let public_key = Decode::decode(&call.args).ok_or("cannot decode public key")?;
        let new_account = Account::new(
            vec![public_key],
            call.amount,
            hash(&[])
        );
        set(
            state_update,
            &account_id_bytes,
            &new_account
        );
        // TODO(#347): Remove default TX staking once tx staking is properly implemented
        let mut tx_total_stake = TxTotalStake::new(0);
        tx_total_stake.add_active_stake(100);
        set(
            state_update,
            &get_tx_stake_key(&account_id, &None),
            &tx_total_stake,
        );

        Ok(vec![])
    }

    fn system_deploy(
        &self,
        state_update: &mut StateDbUpdate,
        call: &AsyncCall,
        account_id: &AccountId,
    ) -> Result<Vec<Transaction>, String> {
        let (public_key, code): (Vec<u8>, Vec<u8>) = 
            Decode::decode(&call.args).ok_or("cannot decode public key")?;
        let public_key = Decode::decode(&public_key).ok_or("cannot decode public key")?;
        let new_account = Account::new(
            vec![public_key],
            call.amount,
            hash(&code),
        );
        set(
            state_update,
            &account_id_to_bytes(COL_ACCOUNT, account_id),
            &new_account
        );
        set(
            state_update,
            &account_id_to_bytes(COL_CODE, account_id),
            &code
        );
        Ok(vec![])
    }

    fn return_data_to_receipts(
        runtime_ext: &mut RuntimeExt,
        return_data: ReturnData,
        callback_info: &Option<CallbackInfo>,
        sender_id: &AccountId,
        receiver_id: &AccountId,
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
                receiver_id.clone(),
                sender_id.clone(),
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
        async_call: &AsyncCall,
        sender_id: &AccountId,
        receiver_id: &AccountId,
        nonce: &[u8],
        receiver: &mut Account,
        mana_accounting: &mut ManaAccounting,
        block_index: BlockIndex,
    ) -> Result<Vec<Transaction>, String> {
        let code: Vec<u8> = get(state_update, &account_id_to_bytes(COL_CODE, receiver_id))
            .ok_or_else(|| format!("cannot find contract code for account {}", receiver_id.clone()))?;
        mana_accounting.gas_used = 0;
        mana_accounting.mana_refund = async_call.mana;
        mana_accounting.accounting_info = async_call.accounting_info.clone();
        let result = {
            let mut runtime_ext = RuntimeExt::new(
                state_update,
                receiver_id,
                &async_call.accounting_info,
                nonce,
            );
            let wasm_res = executor::execute(
                &code,
                &async_call.method_name,
                &async_call.args,
                &[],
                &mut runtime_ext,
                &wasm::types::Config::default(),
                &RuntimeContext::new(
                    receiver.amount,
                    async_call.amount,
                    sender_id,
                    receiver_id,
                    async_call.mana,
                    block_index,
                    nonce.to_vec(),
                ),
            ).map_err(|e| format!("wasm async call preparation failed with error: {:?}", e))?;
            mana_accounting.gas_used = wasm_res.gas_used;
            mana_accounting.mana_refund = wasm_res.mana_left;
            let balance = wasm_res.balance;
            let return_data = wasm_res.return_data
                .map_err(|e| format!("wasm async call execution failed with error: {:?}", e))?;
            Self::return_data_to_receipts(
                &mut runtime_ext,
                return_data,
                &async_call.callback,
                sender_id,
                receiver_id,
            ).and_then(|receipts| {
                receiver.amount = balance;
                Ok(receipts)
            })
        };
        set(
            state_update,
            &account_id_to_bytes(COL_ACCOUNT, &receiver_id),
            receiver,
        );
        result
    }

    fn apply_callback(
        &mut self,
        state_update: &mut StateDbUpdate,
        callback_res: &CallbackResult,
        sender_id: &AccountId,
        receiver_id: &AccountId,
        nonce: &[u8],
        receiver: &mut Account,
        mana_accounting: &mut ManaAccounting,
        block_index: BlockIndex,
    ) -> Result<Vec<Transaction>, String> {
        let mut needs_removal = false;
        let callback: Option<Callback> = 
                get(state_update, &callback_id_to_bytes(&callback_res.info.id));
        let code: Vec<u8> = get(state_update, &account_id_to_bytes(COL_CODE, receiver_id))
            .ok_or_else(|| format!("account {} does not have contract code", receiver_id.clone()))?;
        mana_accounting.gas_used = 0;
        mana_accounting.mana_refund = 0;
        let receipts = match callback {
            Some(mut callback) => {
                callback.results[callback_res.info.result_index] = callback_res.result.clone();
                callback.result_counter += 1;
                // if we have gathered all results, execute the callback
                if callback.result_counter == callback.results.len() {
                    let mut runtime_ext = RuntimeExt::new(
                        state_update,
                        receiver_id,
                        &callback.accounting_info,
                        nonce,
                    );

                    mana_accounting.accounting_info = callback.accounting_info.clone();
                    mana_accounting.mana_refund = callback.mana;
                    needs_removal = true;
                    let wasm_res = executor::execute(
                        &code,
                        &callback.method_name,
                        &callback.args,
                        &callback.results,
                        &mut runtime_ext,
                        &wasm::types::Config::default(),
                        &RuntimeContext::new(
                            receiver.amount,
                            0,
                            sender_id,
                            receiver_id,
                            callback.mana,
                            block_index,
                            nonce.to_vec(),
                        ),
                    ).map_err(|e| format!("wasm callback preparation failed with error: {:?}", e))?;
                    mana_accounting.gas_used = wasm_res.gas_used;
                    mana_accounting.mana_refund = wasm_res.mana_left;
                    let balance = wasm_res.balance;
                    let return_data = wasm_res.return_data
                        .map_err(|e| format!("wasm callback execution failed with error: {:?}", e))?;
                    Self::return_data_to_receipts(
                        &mut runtime_ext,
                        return_data,
                        &callback.callback,
                        sender_id,
                        receiver_id,
                    ).and_then(|receipts| {
                        receiver.amount = balance;
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
        };
        
        if needs_removal {
            state_update.delete(&callback_id_to_bytes(&callback_res.info.id));
            set(
                state_update,
                &account_id_to_bytes(COL_ACCOUNT, &receiver_id),
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
        block_index: BlockIndex,
    ) -> Result<(), String> {
        let receiver: Option<Account> = 
            get(state_update, &account_id_to_bytes(COL_ACCOUNT, &receipt.receiver));
        let mut amount = 0;
        let mut callback_info = None;
        let mut receiver_exists = true;
        let mut mana_accounting = ManaAccounting::default();
        let result = match receiver {
            Some(mut receiver) => {
                match &receipt.body {
                    ReceiptBody::NewCall(async_call) => {
                        amount = async_call.amount;
                        if async_call.method_name == b"deposit".to_vec() {
                            self.deposit(
                                state_update,
                                async_call.amount,
                                &receipt.receiver,
                                &mut receiver
                            )
                        } else if async_call.method_name == b"create_account".to_vec() {
                            debug!(
                                target: "runtime",
                                "Account {} already exists",
                                receipt.receiver,
                            );
                            let receipt = ReceiptTransaction::new(
                                system_account(),
                                receipt.originator.clone(),
                                create_nonce_with_nonce(&receipt.nonce, 0),
                                ReceiptBody::Refund(async_call.amount)
                            );
                            Ok(vec![Transaction::Receipt(receipt)])
                        } else if async_call.method_name == b"deploy".to_vec() {
                            let (pub_key, code): (Vec<u8>, Vec<u8>) = Decode::decode(&async_call.args).ok_or("cannot decode args")?;
                            let pub_key = Decode::decode(&pub_key).ok_or("cannot decode public key")?;
                            if receiver.public_keys.contains(&pub_key) {
                                receiver.code_hash = hash(&code);
                                set(
                                    state_update,
                                    &account_id_to_bytes(COL_CODE, &receipt.receiver),
                                    &code,
                                );
                                set(
                                    state_update,
                                    &account_id_to_bytes(COL_ACCOUNT, &receipt.receiver),
                                    &receiver,
                                );
                                Ok(vec![])
                            } else {
                                Err(format!("Account {} does not contain key {}", receipt.receiver, pub_key))
                            }
                        } else {
                            callback_info = async_call.callback.clone();
                            self.apply_async_call(
                                state_update,
                                &async_call,
                                &receipt.originator,
                                &receipt.receiver,
                                &receipt.nonce,
                                &mut receiver,
                                &mut mana_accounting,
                                block_index,
                            )
                        }
                    },
                    ReceiptBody::Callback(callback_res) => {
                        callback_info = Some(callback_res.info.clone());
                        self.apply_callback(
                            state_update,
                            &callback_res,
                            &receipt.originator,
                            &receipt.receiver,
                            &receipt.nonce,
                            &mut receiver,
                            &mut mana_accounting,
                            block_index,
                        )
                    }
                    ReceiptBody::Refund(amount) => {
                        receiver.amount += amount;
                        set(
                            state_update,
                            &account_id_to_bytes(COL_ACCOUNT, &receipt.receiver),
                            &receiver,
                        );
                        Ok(vec![])
                    },
                    ReceiptBody::ManaAccounting(_mana_accounting) => {
                        // TODO(#259): Refund mana and charge gas
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
                            &receipt.receiver,
                        )
                    } else if call.method_name == b"deploy".to_vec() {
                        self.system_deploy(
                            state_update,
                            &call,
                            &receipt.receiver,
                        )
                    } else {
                        err
                    }
                } else {
                    err
                }
            }
        };
        let res = match result {
            Ok(mut receipts) => {
                new_receipts.append(&mut receipts);
                Ok(())
            }
            Err(s) => {
                if amount > 0 {
                    let receiver = if receiver_exists {
                        receipt.receiver.clone()
                    } else {
                        system_account()
                    };
                    let new_receipt = ReceiptTransaction::new(
                        receiver,
                        receipt.originator.clone(),
                        create_nonce_with_nonce(&receipt.nonce, new_receipts.len() as u64),
                        ReceiptBody::Refund(amount)
                    );
                    new_receipts.push(Transaction::Receipt(new_receipt));
                }
                if let Some(callback_info) = callback_info {
                    let new_receipt = ReceiptTransaction::new(
                        receipt.receiver.clone(),
                        callback_info.receiver.clone(),
                        create_nonce_with_nonce(&receipt.nonce, new_receipts.len() as u64),
                        ReceiptBody::Callback(CallbackResult::new(
                            callback_info,
                            None,
                        ))
                    );
                    new_receipts.push(Transaction::Receipt(new_receipt));
                }
                Err(s)
            }
        };
        if mana_accounting.mana_refund > 0 || mana_accounting.gas_used > 0 {
            let new_receipt = ReceiptTransaction::new(
                mana_accounting.accounting_info.originator.clone(),
                receipt.receiver.clone(),
                create_nonce_with_nonce(&receipt.nonce, new_receipts.len() as u64),
                ReceiptBody::ManaAccounting(mana_accounting),
            );
            new_receipts.push(Transaction::Receipt(new_receipt));
        }
        res
    }

    fn filter_transaction(
        runtime: &mut Self,
        state_update: &mut StateDbUpdate,
        shard_id: ShardId,
        block_index: BlockIndex,
        transaction: &Transaction,
        new_receipts: &mut Vec<Transaction>,
        authority_proposals: &mut Vec<AuthorityStake>,
    ) -> bool {
        match transaction {
            Transaction::SignedTransaction(ref tx) => {
                match runtime.apply_signed_transaction(
                    state_update,
                    block_index,
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
                if account_to_shard_id(&r.receiver) == shard_id {
                    let mut tmp_new_receipts = vec![];
                    match runtime.apply_receipt(state_update, r, &mut tmp_new_receipts, block_index) {
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

    /// check whether transactions in a block are valid and return the new root
    /// if they are
    pub fn check(
        &mut self,
        apply_state: &ApplyState,
        prev_receipts: &[Transaction],
        transactions: &[Transaction],
    ) -> Option<(storage::DBChanges, MerkleHash)> {
        let mut new_receipts = vec![];
        let mut state_update = StateDbUpdate::new(self.state_db.clone(), apply_state.root);
        let mut authority_proposals = vec![];
        let shard_id = apply_state.shard_id;
        let block_index = apply_state.block_index;
        for tx in prev_receipts.iter().chain(transactions) {
            let filter_res = Self::filter_transaction(
                self,
                &mut state_update,
                shard_id,
                block_index,
                tx,
                &mut new_receipts,
                &mut authority_proposals
            );
            if !filter_res {
                return None;
            }
        }
        let (db_transaction, new_root) = state_update.finalize();
        Some((db_transaction, new_root))
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
        let block_index = apply_state.block_index;
        for receipt in prev_receipts.iter() {
            Self::filter_transaction(
                self,
                &mut state_update,
                shard_id,
                block_index,
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
                block_index,
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

    /// Balances are account, publickey, initial_balance, initial_tx_stake
    pub fn apply_genesis_state(
        &self,
        balances: &[(AccountId, ReadablePublicKey, Balance, Balance)],
        wasm_binary: &[u8],
        initial_authorities: &[(AccountId, ReadablePublicKey, u64)]
    ) -> MerkleHash {
        let mut state_db_update =
            StateDbUpdate::new(self.state_db.clone(), MerkleHash::default());
        let mut pk_to_acc_id = HashMap::new();
        balances.iter().for_each(|(account_id, public_key, balance, initial_tx_stake)| {
            pk_to_acc_id.insert(public_key.clone(), account_id.clone());
            set(
                &mut state_db_update,
                &account_id_to_bytes(COL_ACCOUNT, &account_id),
                &Account {
                    public_keys: vec![PublicKey::from(public_key)],
                    amount: *balance,
                    nonce: 0,
                    staked: 0,
                    code_hash: hash(wasm_binary),
                },
            );
            // Default code
            set(
                &mut state_db_update,
                &account_id_to_bytes(COL_CODE, &account_id),
                &wasm_binary.to_vec(),
            );
            // Default transaction stake
            let key = get_tx_stake_key(
                &account_id,
                &None,
            );
            let mut tx_total_stake = TxTotalStake::new(0);
            tx_total_stake.add_active_stake(*initial_tx_stake);
            set(
                &mut state_db_update,
                &key,
                &tx_total_stake,
            );
            // TODO(#345): Add system TX stake
        });
        for (_, pk, amount) in initial_authorities {
            let account_id = pk_to_acc_id.get(pk).expect("Missing account for public key");
            let account_id_bytes = account_id_to_bytes(COL_ACCOUNT, account_id);
            let mut account: Account = get(
                &mut state_db_update,
                &account_id_bytes,
            ).expect("account must exist");
            account.staked = *amount;
            set(
                &mut state_db_update,
                &account_id_bytes,
                &account
            );
        }
        let (transaction, genesis_root) = state_db_update.finalize();
        // TODO: check that genesis_root is not yet in the state_db? Also may be can check before doing this?
        self.state_db.commit(transaction).expect("Failed to commit genesis state");
        genesis_root
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use primitives::hash::hash;
    use primitives::types::{
        DeployContractTransaction, FunctionCallTransaction,
        TransactionBody,
    };
    use primitives::signature::{DEFAULT_SIGNATURE, get_keypair, sign};
    use state_viewer::AccountViewCallResult;
    use storage::test_utils::create_state_db;
    use test_utils::*;
    use super::*;

    fn alice_account() -> AccountId {
        "alice.near".to_string()
    }
    fn bob_account() -> AccountId {
        "bob.near".to_string()
    }
    fn eve_account() -> AccountId {
        "eve.near".to_string()
    }

    impl Default for Runtime {
        fn default() -> Runtime {
            Runtime {
                state_db: Arc::new(create_state_db()),
            }
        }
    }

    fn default_code_hash() -> CryptoHash {
        let genesis_wasm = include_bytes!("../../../core/wasm/runtest/res/wasm_with_mem.wasm");
        hash(genesis_wasm)
    }

    // TODO(#348): Add tests for TX staking, mana charging and regeneration

    #[test]
    fn test_genesis_state() {
        let viewer = get_test_state_db_viewer();
        let result = viewer.view_account(&alice_account());
        assert_eq!(
            result.unwrap(),
            AccountViewCallResult {
                account: alice_account(),
                amount: 100,
                nonce: 0,
                stake: 50,
                code_hash: default_code_hash(),
            }
        );
    }

    #[test]
    fn test_get_and_set_accounts() {
        let state_db = Arc::new(create_state_db());
        let mut state_update = StateDbUpdate::new(state_db, MerkleHash::default());
        let test_account = Account::new(vec![], 10, hash(&[]));
        let account_id = bob_account();
        set(&mut state_update, &account_id_to_bytes(COL_ACCOUNT, &account_id), &test_account);
        let get_res = get(&mut state_update, &account_id_to_bytes(COL_ACCOUNT, &account_id)).unwrap();
        assert_eq!(test_account, get_res);
    }

    #[test]
    fn test_get_account_from_state_db() {
        let state_db = Arc::new(create_state_db());
        let root = MerkleHash::default();
        let mut state_update = StateDbUpdate::new(state_db.clone(), root);
        let test_account = Account::new(vec![], 10, hash(&[]));
        let account_id = bob_account();
        set(&mut state_update, &account_id_to_bytes(COL_ACCOUNT, &account_id), &test_account);
        let (transaction, new_root) = state_update.finalize();
        state_db.commit(transaction).unwrap();
        let mut new_state_update = StateDbUpdate::new(state_db.clone(), new_root);
        let get_res = get(&mut new_state_update, &account_id_to_bytes(COL_ACCOUNT, &account_id)).unwrap();
        assert_eq!(test_account, get_res);
    }

    #[test]
    fn test_simple_smart_contract() {
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.get_root();
        let tx_body = TransactionBody::FunctionCall(FunctionCallTransaction {
            nonce: 1,
            originator: alice_account(),
            contract_id: bob_account(),
            method_name: b"run_test".to_vec(),
            args: vec![],
            amount: 0,
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
            originator: alice_account(),
            contract_id: bob_account(),
            method_name: b"run_test".to_vec(),
            args: (2..4).flat_map(|x| encode_int(x).to_vec()).collect(),
            amount: 0,
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
    fn test_upload_contract() {
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.get_root();
        let (pub_key, _) = get_keypair();
        let wasm_binary = include_bytes!("../../../core/wasm/runtest/res/wasm_with_mem.wasm");
        let tx_body = TransactionBody::DeployContract(DeployContractTransaction {
            nonce: 1,
            originator: alice_account(),
            contract_id: eve_account(),
            public_key: pub_key.encode().unwrap(),
            wasm_byte_array: wasm_binary.to_vec(),
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
        runtime.state_db.commit(apply_result.transaction).unwrap();
        let mut new_state_update = StateDbUpdate::new(runtime.state_db, apply_result.root);
        let code: Vec<u8> = get(
            &mut new_state_update,
            &account_id_to_bytes(COL_CODE, &eve_account())
        ).unwrap();
        assert_eq!(code, wasm_binary.to_vec());
    }

    #[test]
    fn test_redeploy_contract() {
        let test_binary = b"test_binary";
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.get_root();
        let mut state_update = StateDbUpdate::new(runtime.state_db.clone(), root);
        let account: Account = get(
            &mut state_update,
            &account_id_to_bytes(COL_ACCOUNT, &bob_account())
        ).unwrap();
        let tx_body = TransactionBody::DeployContract(DeployContractTransaction{
            nonce: 1,
            originator: bob_account(),
            contract_id: bob_account(),
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
        let apply_result = runtime.apply_all(
            apply_state, vec![Transaction::SignedTransaction(transaction)],
        );
        assert_eq!(apply_result.filtered_transactions.len(), 1);
        assert_eq!(apply_result.new_receipts.len(), 0);
        assert_ne!(root, apply_result.root);
        runtime.state_db.commit(apply_result.transaction).unwrap();
        let mut new_state_update = StateDbUpdate::new(runtime.state_db, apply_result.root);
        let code: Vec<u8> = get(
            &mut new_state_update,
            &account_id_to_bytes(COL_CODE, &bob_account())
        ).unwrap();
        assert_eq!(code, test_binary.to_vec())
    }

    #[test]
    fn test_send_money() {
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.get_root();
        let tx_body = TransactionBody::SendMoney(SendMoneyTransaction {
            nonce: 1,
            originator: alice_account(),
            receiver: bob_account(),
            amount: 10,
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
        runtime.state_db.commit(apply_result.transaction).unwrap();
        let result1 = viewer.view_account_at(&alice_account(), apply_result.root);
        assert_eq!(
            result1.unwrap(),
            AccountViewCallResult {
                nonce: 1,
                account: alice_account(),
                amount: 90,
                stake: 50,
                code_hash: default_code_hash(),
            }
        );
        let result2 = viewer.view_account_at(&bob_account(), apply_result.root);
        assert_eq!(
            result2.unwrap(),
            AccountViewCallResult {
                nonce: 0,
                account: bob_account(),
                amount: 10,
                stake: 0,
                code_hash: default_code_hash(),
            }
        );
    }

    #[test]
    fn test_send_money_over_balance() {
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.get_root();
        let tx_body = TransactionBody::SendMoney(SendMoneyTransaction {
            nonce: 1,
            originator: alice_account(),
            receiver: bob_account(),
            amount: 1000,
        });
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state = ApplyState {
            root,
            shard_id: 0,
            parent_block_hash: CryptoHash::default(),
            block_index: 0
        };
        let apply_result = runtime.apply(
            &apply_state, &[], vec![Transaction::SignedTransaction(transaction)]
        );
        assert_eq!(apply_result.filtered_transactions.len(), 0);
        assert_eq!(apply_result.new_receipts.len(), 0);
        assert_eq!(root, apply_result.root);
        runtime.state_db.commit(apply_result.transaction).unwrap();
        let result1 = viewer.view_account_at(&alice_account(), apply_result.root);
        assert_eq!(
            result1.unwrap(),
            AccountViewCallResult {
                nonce: 0,
                account: alice_account(),
                amount: 100,
                stake: 50,
                code_hash: default_code_hash(),
            }
        );
        let result2 = viewer.view_account_at(&bob_account(), apply_result.root);
        assert_eq!(
            result2.unwrap(),
            AccountViewCallResult {
                nonce: 0,
                account: bob_account(),
                amount: 0,
                stake: 0,
                code_hash: default_code_hash(),
            }
        );
    }

    #[test]
    fn test_refund_on_send_money_to_non_existent_account() {
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.get_root();
        let tx_body = TransactionBody::SendMoney(SendMoneyTransaction {
            nonce: 1,
            originator: alice_account(),
            // Account should not exist
            receiver: eve_account(),
            amount: 10,
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
        assert_ne!(root, apply_result.root);
        runtime.state_db.commit(apply_result.transaction).unwrap();
        let result1 = viewer.view_account_at(&alice_account(), apply_result.root);
        assert_eq!(
            result1.unwrap(),
            AccountViewCallResult {
                nonce: 1,
                account: alice_account(),
                amount: 100,
                stake: 50,
                code_hash: default_code_hash(),
            }
        );
        let result2 = viewer.view_account_at(&eve_account(), apply_result.root);
        assert!(result2.is_err());
    }

    #[test]
    fn test_create_account() {
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.get_root();
        let (pub_key, _) = get_keypair();
        let tx_body = TransactionBody::CreateAccount(CreateAccountTransaction {
            nonce: 1,
            originator: alice_account(),
            new_account_id: eve_account(),
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
        let apply_result = runtime.apply_all(
            apply_state, vec![Transaction::SignedTransaction(transaction)]
        );
        assert_ne!(root, apply_result.root);
        runtime.state_db.commit(apply_result.transaction).unwrap();
        let result1 = viewer.view_account_at(&alice_account(), apply_result.root);
        assert_eq!(
            result1.unwrap(),
            AccountViewCallResult {
                nonce: 1,
                account: alice_account(),
                amount: 90,
                stake: 50,
                code_hash: default_code_hash(),
            }
        );
        let result2 = viewer.view_account_at(&eve_account(), apply_result.root);
        assert_eq!(
            result2.unwrap(),
            AccountViewCallResult {
                nonce: 0,
                account: eve_account(),
                amount: 10,
                stake: 0,
                code_hash: hash(b""),
            }
        );
    }

    #[test]
    fn test_create_account_failure_invalid_name() {
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.get_root();
        let (pub_key, _) = get_keypair();
        for invalid_account_name in vec![
                "eve", // too short
                "Alice.near", // capital letter
                "alice(near)", // brackets are invalid
                "long_of_the_name_for_real_is_hard", // too long
                "qq@qq*qq" // * is invalid
        ] {
            let tx_body = TransactionBody::CreateAccount(CreateAccountTransaction {
                nonce: 1,
                originator: alice_account(),
                new_account_id: invalid_account_name.to_string(),
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
            let apply_result = runtime.apply_all(
                apply_state, vec![Transaction::SignedTransaction(transaction)]
            );
            // Transaction failed, roots are the same and nonce on the account is 0.
            assert_eq!(root, apply_result.root);
            let result1 = viewer.view_account_at(&alice_account(), apply_result.root);
            assert_eq!(
                result1.unwrap(),
                AccountViewCallResult {
                    nonce: 0,
                    account: alice_account(),
                    amount: 100,
                    stake: 50,
                    code_hash: default_code_hash(),
                }
            );
        }
    }

    #[test]
    fn test_create_account_failure_already_exists() {
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.get_root();
        let (pub_key, _) = get_keypair();
        let tx_body = TransactionBody::CreateAccount(CreateAccountTransaction {
            nonce: 1,
            originator: alice_account(),
            new_account_id: bob_account(),
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
        let apply_result = runtime.apply_all(
            apply_state, vec![Transaction::SignedTransaction(transaction)]
        );
        assert_ne!(root, apply_result.root);
        runtime.state_db.commit(apply_result.transaction).unwrap();
        let result1 = viewer.view_account_at(&alice_account(), apply_result.root);
        assert_eq!(
            result1.unwrap(),
            AccountViewCallResult {
                nonce: 1,
                account: alice_account(),
                amount: 100,
                stake: 50,
                code_hash: default_code_hash(),
            }
        );
        let result2 = viewer.view_account_at(&bob_account(), apply_result.root);
        assert_eq!(
            result2.unwrap(),
            AccountViewCallResult {
                nonce: 0,
                account: bob_account(),
                amount: 0,
                stake: 0,
                code_hash: default_code_hash(),
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
            originator: alice_account(),
            new_account_id: eve_account(),
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
        let apply_result = runtime.apply_all(
            apply_state, vec![Transaction::SignedTransaction(transaction)]
        );
        assert_eq!(apply_result.filtered_transactions.len(), 1);
        assert_eq!(apply_result.new_receipts.len(), 0);
        assert_ne!(root, apply_result.root);
        runtime.state_db.commit(apply_result.transaction).unwrap();
        let tx_body = TransactionBody::SwapKey(SwapKeyTransaction {
            nonce: 2,
            originator: eve_account(),
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
        let apply_result = runtime.apply(
            &apply_state, &[], vec![Transaction::SignedTransaction(transaction1)],
        );
        runtime.state_db.commit(apply_result.transaction).unwrap();
        let mut new_state_update = StateDbUpdate::new(runtime.state_db.clone(), apply_result.root);
        let account = get::<Account>(
            &mut new_state_update,
            &account_id_to_bytes(COL_ACCOUNT, &eve_account()),
        ).unwrap();
        assert_eq!(account.public_keys, vec![pub_key2]);
    }

    #[test]
    fn test_async_call_with_no_callback() {
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.get_root();
        let receipt = ReceiptTransaction::new(
            alice_account(),
            bob_account(),
            hash(&[1, 2, 3]).into(),
            ReceiptBody::NewCall(AsyncCall::new(
                b"run_test".to_vec(),
                vec![],
                0,
                0,
                AccountingInfo {
                    originator: alice_account(),
                    contract_id: Some(bob_account()),
                },
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
        assert_eq!(root, apply_result.root);
    }

    #[test]
    fn test_async_call_with_callback() {
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.get_root();
        let args = (7..9).flat_map(|x| encode_int(x).to_vec()).collect();
        let accounting_info = AccountingInfo {
            originator: alice_account(),
            contract_id: Some(bob_account()),
        };
        let mut callback = Callback::new(
            b"sum_with_input".to_vec(),
            args,
            0,
            accounting_info.clone(),
        );
        callback.results.resize(1, None);
        let callback_id = [0; 32].to_vec();
        let mut async_call = AsyncCall::new(
            b"run_test".to_vec(),
            vec![],
            0,
            0,
            accounting_info,
        );
        let callback_info = CallbackInfo::new(callback_id.clone(), 0, alice_account());
        async_call.callback = Some(callback_info.clone());
        let receipt = ReceiptTransaction::new(
            alice_account(),
            bob_account(),
            hash(&[1, 2, 3]).into(),
            ReceiptBody::NewCall(async_call),
        );
        let block_index = 1;
        let mut state_update = StateDbUpdate::new(runtime.state_db.clone(), root);
        let mut new_receipts = vec![];
        runtime.apply_receipt(&mut state_update, &receipt, &mut new_receipts, block_index).unwrap();
        assert_eq!(new_receipts.len(), 1);
        if let Transaction::Receipt(new_receipt) = &new_receipts[0] {
            assert_eq!(new_receipt.originator, bob_account());
            assert_eq!(new_receipt.receiver, alice_account());
            let callback_res = CallbackResult::new(
                callback_info.clone(), Some(encode_int(10).to_vec())
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
        let args = (7..9).flat_map(|x| encode_int(x).to_vec()).collect();
        let mut callback = Callback::new(
            b"sum_with_input".to_vec(),
            args,
            0,
            AccountingInfo {
                originator: alice_account(),
                contract_id: Some(bob_account()),
            },
        );
        callback.results.resize(1, None);
        let callback_id = [0; 32].to_vec();
        let mut state_update = StateDbUpdate::new(runtime.state_db.clone(), root);
        set(
            &mut state_update,
            &callback_id_to_bytes(&callback_id.clone()),
            &callback
        );
        let (transaction, new_root) = state_update.finalize();
        runtime.state_db.commit(transaction).unwrap();
        let receipt = ReceiptTransaction::new(
            alice_account(),
            bob_account(),
            hash(&[1, 2, 3]).into(),
            ReceiptBody::Callback(CallbackResult::new(
                CallbackInfo::new(callback_id.clone(), 0, alice_account()),
                None,
            ))
        );
        let apply_state = ApplyState {
            root: new_root,
            shard_id: 0,
            parent_block_hash: CryptoHash::default(),
            block_index: 0
        };
        let apply_result = runtime.apply(
            &apply_state, &[], vec![Transaction::Receipt(receipt)]
        );
        runtime.state_db.commit(apply_result.transaction).unwrap();
        let mut state_update = StateDbUpdate::new(runtime.state_db.clone(), apply_result.root);
        let callback: Option<Callback> = get(&mut state_update, &callback_id_to_bytes(&callback_id));
        assert!(callback.is_none());
    }

    #[test]
    fn test_nonce_update_when_deploying_contract() {
        let (mut runtime, viewer) = get_runtime_and_state_db_viewer();
        let root = viewer.get_root();
        let (pub_key, _) = get_keypair();
        let wasm_binary = include_bytes!("../../../core/wasm/runtest/res/wasm_with_mem.wasm");
        let tx_body = TransactionBody::DeployContract(DeployContractTransaction {
            nonce: 1,
            originator: alice_account(),
            contract_id: eve_account(),
            public_key: pub_key.encode().unwrap(),
            wasm_byte_array: wasm_binary.to_vec(),
        });
        let transaction = SignedTransaction::new(DEFAULT_SIGNATURE, tx_body);
        let apply_state = ApplyState {
            root,
            shard_id: 0,
            parent_block_hash: CryptoHash::default(),
            block_index: 0
        };
        let apply_result = runtime.apply(
            &apply_state, &[], vec![Transaction::SignedTransaction(transaction)]
        );
        runtime.state_db.commit(apply_result.transaction).unwrap();
        let mut state_update = StateDbUpdate::new(runtime.state_db.clone(), apply_result.root);
        let account: Account = get(
            &mut state_update,
            &account_id_to_bytes(COL_ACCOUNT, &alice_account())
        ).unwrap();
        assert_eq!(account.nonce, 1);
    }

    #[test]
    fn test_100_accounts() {
        let mut chain_spec = generate_test_chain_spec();
        let public_key = get_keypair().0;
        for i in 0..100 {
            chain_spec.accounts.push((format!("account{}", i), public_key.to_string(), 10000, 0));
        }
        let (_, viewer) = get_runtime_and_state_db_viewer_from_chain_spec(&chain_spec);
        for i in 0..100 {
            assert_eq!(viewer.view_account(&format!("account{}", i)).unwrap().amount, 10000)
        }
    }
}

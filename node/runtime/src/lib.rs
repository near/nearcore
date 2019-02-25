extern crate bincode;
extern crate byteorder;
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

use serde::{de::DeserializeOwned, Serialize};

use primitives::aggregate_signature::BlsPublicKey;
use primitives::hash::{CryptoHash, hash};
use primitives::signature::{bs58_serializer, PublicKey};
use primitives::traits::{Decode, Encode};
use primitives::types::{
    AccountId, AccountingInfo, AuthorityStake,
    Balance, BlockIndex, Mana,
    ManaAccounting, MerkleHash, PromiseId, ReadablePublicKey, ReadableBlsPublicKey, ShardId,
};
use primitives::utils::{
    account_to_shard_id, index_to_bytes, is_valid_account_id
};
use primitives::transaction::{
    AsyncCall, Callback, CallbackInfo, CallbackResult,
    FunctionCallTransaction, LogEntry, ReceiptBody,
    ReceiptTransaction, SignedTransaction,
    TransactionBody, TransactionResult, TransactionStatus,
    verify_transaction_signature
};
use wasm::executor;
use wasm::types::{ReturnData, RuntimeContext};
use primitives::chain::ReceiptBlock;

use crate::ext::RuntimeExt;
use crate::tx_stakes::{get_tx_stake_key, TxStakeConfig, TxTotalStake};
use crate::system::{
    SYSTEM_METHOD_CREATE_ACCOUNT, system_account,
    system_create_account
};
use storage::TrieUpdate;

pub mod test_utils;
pub mod state_viewer;
mod tx_stakes;
mod ext;
mod system;

const COL_ACCOUNT: &[u8] = &[0];
const COL_CALLBACK: &[u8] = &[1];
const COL_CODE: &[u8] = &[2];
const COL_TX_STAKE: &[u8] = &[3];
const COL_TX_STAKE_SEPARATOR: &[u8] = &[4];

/// Per account information stored in the state.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct Account {
    pub public_keys: Vec<PublicKey>,
    // TODO: Multiple bls keys associated with the same account
    #[serde(with = "bs58_serializer")]
    pub bls_public_key: BlsPublicKey,
    pub nonce: u64,
    // amount + staked is the total value of the account
    pub amount: u64,
    pub staked: u64,
    pub code_hash: CryptoHash,
}

impl Account {
    pub fn new(public_keys: Vec<PublicKey>, amount: Balance, code_hash: CryptoHash) -> Self {
        Account { public_keys, bls_public_key: BlsPublicKey::empty(), nonce: 0, amount, staked: 0, code_hash }
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

fn create_nonce_with_nonce(base: &CryptoHash, salt: u64) -> CryptoHash {
    let mut nonce: Vec<u8> = base.as_ref().to_owned();
    nonce.append(&mut index_to_bytes(salt));
    hash(&nonce)
}

#[derive(Debug)]
pub struct ApplyState {
    pub root: MerkleHash,
    pub shard_id: ShardId,
    pub block_index: u64,
    pub parent_block_hash: CryptoHash,
}

#[derive(Clone, Debug)]
pub struct ApplyResult {
    pub root: MerkleHash,
    pub shard_id: ShardId,
    pub db_changes: storage::DBChanges,
    pub authority_proposals: Vec<AuthorityStake>,
    pub new_receipts: HashMap<ShardId, Vec<ReceiptTransaction>>,
    pub tx_result: Vec<TransactionResult>,
}

fn get<T: DeserializeOwned>(state_update: &mut TrieUpdate, key: &[u8]) -> Option<T> {
    state_update.get(key).and_then(|data| Decode::decode(&data).ok())
}

fn set<T: Serialize>(state_update: &mut TrieUpdate, key: &[u8], value: &T) {
    value
        .encode().ok()
        .map(|data| state_update.set(key, &storage::DBValue::from_slice(&data)))
        .unwrap_or_else(|| { debug!("set value failed"); })
}

#[derive(Clone, Copy, Default)]
pub struct Runtime {}

impl Runtime {

    fn try_charge_mana(
        self,
        state_update: &mut TrieUpdate,
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

    fn call_function(
        self,
        state_update: &mut TrieUpdate,
        transaction: &FunctionCallTransaction,
        hash: CryptoHash,
        sender: &mut Account,
        accounting_info: AccountingInfo,
        mana: Mana,
    ) -> Result<Vec<ReceiptTransaction>, String> {
        match transaction.method_name.get(0) {
            Some(b'_') => return Err(format!("Account {} tries to call a private method {}",
                transaction.originator,
                std::str::from_utf8(&transaction.method_name).unwrap_or_else(|_| "NON_UTF8_METHOD_NAME"),
            )),
            None if transaction.amount == 0 => return Err(format!("Account {} tries to send 0 tokens",
                transaction.originator,
            )),
            _ => (),
        };
        if sender.amount >= transaction.amount {
            sender.amount -= transaction.amount;
            set(state_update, &account_id_to_bytes(COL_ACCOUNT, &transaction.originator), sender);
            let receipt = ReceiptTransaction::new(
                transaction.originator.clone(),
                transaction.contract_id.clone(),
                create_nonce_with_nonce(&hash, 0),
                ReceiptBody::NewCall(AsyncCall::new(
                    transaction.method_name.clone(),
                    transaction.args.clone(),
                    transaction.amount,
                    mana - 1,
                    accounting_info,
                ))
            );
            Ok(vec![receipt])
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
        self,
        state_update: &mut TrieUpdate,
        block_index: BlockIndex,
        transaction: &SignedTransaction,
        authority_proposals: &mut Vec<AuthorityStake>
    ) -> Result<Vec<ReceiptTransaction>, String> {
        let sender_account_id = transaction.body.get_originator();
        if !is_valid_account_id(&sender_account_id) {
            return Err("Invalid originator account_id".to_string());
        }
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
                if !verify_transaction_signature(&transaction, &sender.public_keys) {
                    return Err(format!(
                        "transaction not signed with a public key of originator {:?}",
                        transaction.body.get_originator()
                    ));
                }
                sender.nonce = transaction.body.get_nonce();
                set(
                    state_update,
                    &account_id_to_bytes(COL_ACCOUNT, &sender_account_id),
                    &sender
                );
                let contract_id = transaction.body.get_contract_id();
                if let Some(ref contract_id) = contract_id {
                    if !is_valid_account_id(&contract_id) {
                        return Err("Invalid contract_id".to_string());
                    }
                }
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
                        system::send_money(
                            state_update,
                            &t,
                            transaction.get_hash(),
                            &mut sender,
                            accounting_info,
                        )
                    },
                    TransactionBody::Stake(ref t) => {
                        system::staking(
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
                            transaction.get_hash(),
                            &mut sender,
                            accounting_info,
                            mana,
                        )
                    },
                    TransactionBody::DeployContract(ref t) => {
                        system::deploy(
                            state_update,
                            &t.contract_id,
                            &t.wasm_byte_array,
                            &mut sender,
                        )
                    },
                    TransactionBody::CreateAccount(ref t) => {
                        system::create_account(
                            state_update,
                            t,
                            transaction.get_hash(),
                            &mut sender,
                            accounting_info,
                        )
                    },
                    TransactionBody::SwapKey(ref t) => {
                        system::swap_key(
                            state_update,
                            t,
                            &mut sender,
                        )
                    }
                    TransactionBody::AddKey(ref t) => {
                        system::add_key(
                            state_update,
                            t,
                            &mut sender
                        )
                    }
                    TransactionBody::DeleteKey(ref t) => {
                        system::delete_key(
                            state_update,
                            t,
                            &mut sender
                        )
                    }
                    TransactionBody::AddBlsKey(ref t) => {
                        system::add_bls_key(
                            state_update,
                            t,
                            &mut sender
                        )
                    }
                }
            }
            _ => Err(format!("sender {} does not exist", sender_account_id))
        }
    }

    fn return_data_to_receipts(
        runtime_ext: &mut RuntimeExt,
        return_data: ReturnData,
        callback_info: &Option<CallbackInfo>,
        sender_id: &AccountId,
        receiver_id: &AccountId,
    ) -> Result<Vec<ReceiptTransaction>, String> {
        let callback_info = match callback_info {
            Some(info) => info,
            _ => {
                let receipts = runtime_ext.get_receipts();
                runtime_ext.flush_callbacks();
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
            receipts.push(new_receipt);
        }
        runtime_ext.flush_callbacks();
        Ok(receipts)
    }

    fn apply_async_call(
        self,
        state_update: &mut TrieUpdate,
        async_call: &AsyncCall,
        sender_id: &AccountId,
        receiver_id: &AccountId,
        nonce: &CryptoHash,
        receiver: &mut Account,
        mana_accounting: &mut ManaAccounting,
        block_index: BlockIndex,
        logs: &mut Vec<LogEntry>,
    ) -> Result<Vec<ReceiptTransaction>, String> {
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
            let mut wasm_res = executor::execute(
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
                    nonce.as_ref().to_vec(),
                ),
            ).map_err(|e| format!("wasm async call preparation failed with error: {:?}", e))?;
            mana_accounting.gas_used = wasm_res.gas_used;
            mana_accounting.mana_refund = wasm_res.mana_left;
            logs.append(&mut wasm_res.logs);
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
        self,
        state_update: &mut TrieUpdate,
        callback_res: &CallbackResult,
        sender_id: &AccountId,
        receiver_id: &AccountId,
        nonce: &CryptoHash,
        receiver: &mut Account,
        mana_accounting: &mut ManaAccounting,
        block_index: BlockIndex,
        logs: &mut Vec<String>,
    ) -> Result<Vec<ReceiptTransaction>, String> {
        let mut needs_removal = false;
        let mut callback: Option<Callback> = 
                get(state_update, &callback_id_to_bytes(&callback_res.info.id));
        let code: Vec<u8> = get(state_update, &account_id_to_bytes(COL_CODE, receiver_id))
            .ok_or_else(|| format!("account {} does not have contract code", receiver_id.clone()))?;
        mana_accounting.gas_used = 0;
        mana_accounting.mana_refund = 0;
        let receipts = match callback {
            Some(ref mut callback) => {
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
                    executor::execute(
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
                            nonce.as_ref().to_vec(),
                        ),
                    )
                    .map_err(|e| format!("wasm callback execution failed with error: {:?}", e))
                    .and_then(|mut res| {
                        mana_accounting.gas_used = res.gas_used;
                        mana_accounting.mana_refund = res.mana_left;
                        logs.append(&mut res.logs);
                        let balance = res.balance;
                        res.return_data
                            .map_err(|e| format!("wasm callback execution failed with error: {:?}", e))
                            .and_then(|data|
                                Self::return_data_to_receipts(
                                    &mut runtime_ext,
                                    data,
                                    &callback.callback,
                                    sender_id,
                                    receiver_id,
                                )
                            )
                            .and_then(|receipts| {
                                receiver.amount = balance;
                                Ok(receipts)
                            })
                    })
                } else {
                    // otherwise no receipt is generated
                    Ok(vec![])
                }
            },
            _ => {
                return Err(format!("callback id: {:?} not found", callback_res.info.id));
            }
        };
        if needs_removal {
            if receipts.is_err() {
                // On error, we rollback previous changes and then commit the deletion
                state_update.rollback();
                state_update.remove(&callback_id_to_bytes(&callback_res.info.id));
                state_update.commit();
            } else {
                state_update.remove(&callback_id_to_bytes(&callback_res.info.id));
                set(
                    state_update,
                    &account_id_to_bytes(COL_ACCOUNT, &receiver_id),
                    receiver
                );
            }
        } else {
            // if we don't need to remove callback, since it is updated, we need
            // to update the storage.
            let callback = callback.expect("Cannot be none");
            set(
                state_update,
                &callback_id_to_bytes(&callback_res.info.id),
                &callback
            );
        }
        receipts
    }

    fn apply_receipt(
        self,
        state_update: &mut TrieUpdate,
        receipt: &ReceiptTransaction,
        new_receipts: &mut Vec<ReceiptTransaction>,
        block_index: BlockIndex,
        logs: &mut Vec<String>,
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
                        if async_call.method_name.is_empty() {
                            if amount > 0 {
                                mana_accounting.mana_refund = async_call.mana;
                                mana_accounting.accounting_info = async_call.accounting_info.clone();
                                system::deposit(
                                    state_update,
                                    async_call.amount,
                                    &receipt.receiver,
                                    &mut receiver
                                )
                            } else {
                                // Transferred amount is 0. Weird.
                                Ok(vec![])
                            }
                        } else if async_call.method_name == SYSTEM_METHOD_CREATE_ACCOUNT {
                            Err(format!("Account {} already exists", receipt.receiver))
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
                                logs,
                            )
                        }
                    },
                    ReceiptBody::Callback(callback_res) => {
                        self.apply_callback(
                            state_update,
                            &callback_res,
                            &receipt.originator,
                            &receipt.receiver,
                            &receipt.nonce,
                            &mut receiver,
                            &mut mana_accounting,
                            block_index,
                            logs,
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
                    ReceiptBody::ManaAccounting(mana_accounting) => {
                        let key = get_tx_stake_key(
                            &mana_accounting.accounting_info.originator,
                            &mana_accounting.accounting_info.contract_id,
                        );
                        let tx_total_stake: Option<TxTotalStake> = get(state_update, &key);
                        if let Some(mut tx_total_stake) = tx_total_stake {
                            let config = TxStakeConfig::default();
                            tx_total_stake.update(block_index, &config);
                            tx_total_stake.refund_mana_and_charge_gas(
                                mana_accounting.mana_refund,
                                mana_accounting.gas_used,
                                &config,
                            );
                            set(state_update, &key, &tx_total_stake);
                        } else {
                            // TODO(#445): Figure out what to do when the TxStake doesn't exist during mana accounting
                            panic!("TX stake doesn't exist when mana accounting arrived");
                        }
                        Ok(vec![])
                    }
                }
            }
            _ => {
                receiver_exists = false;
                let err = Err(format!("receiver {} does not exist", receipt.receiver));
                if let ReceiptBody::NewCall(call) = &receipt.body {
                    amount = call.amount;
                    if call.method_name == SYSTEM_METHOD_CREATE_ACCOUNT {
                        system_create_account(
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
                    new_receipts.push(new_receipt);
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
                    new_receipts.push(new_receipt);
                }
                Err(s)
            }
        };
        if mana_accounting.mana_refund > 0 || mana_accounting.gas_used > 0 {
            let new_receipt = ReceiptTransaction::new(
                receipt.receiver.clone(),
                mana_accounting.accounting_info.originator.clone(),
                create_nonce_with_nonce(&receipt.nonce, new_receipts.len() as u64),
                ReceiptBody::ManaAccounting(mana_accounting),
            );
            new_receipts.push(new_receipt);
        }
        res
    }

    fn print_log(log: &[LogEntry]) {
        if log.is_empty() {
            return;
        }
        let log_str = log.iter().fold(String::new(), |acc, s| {
            if acc.is_empty() {
                s.to_string()
            } else {
                acc + "\n" + s
            }
        });
        debug!(target: "runtime", "{}", log_str);
    }

    fn process_transaction(
        runtime: Self,
        state_update: &mut TrieUpdate,
        block_index: BlockIndex,
        transaction: &SignedTransaction,
        new_receipts: &mut HashMap<ShardId, Vec<ReceiptTransaction>>,
        authority_proposals: &mut Vec<AuthorityStake>,
    ) -> TransactionResult {
        let mut result = TransactionResult::default();
        match runtime.apply_signed_transaction(
            state_update,
            block_index,
            transaction,
            authority_proposals
        ) {
            Ok(receipts) => {
                for receipt in receipts {
                    result.receipts.push(receipt.nonce);
                    let shard_id = receipt.shard_id();
                    if new_receipts.contains_key(&shard_id) {
                        new_receipts
                        .entry(shard_id)
                        .and_modify(|e| e.push(receipt));
                    } else {
                        new_receipts.insert(shard_id, vec![receipt]);
                    }
                }
                state_update.commit();
                result.status = TransactionStatus::Completed;
            }
            Err(s) => {
                state_update.rollback();
                result.logs.push(format!("Runtime error: {}", s));
                result.status = TransactionStatus::Failed;
            }
        };
        Self::print_log(&result.logs);
        result
    }

    fn process_receipt(
        runtime: Self,
        state_update: &mut TrieUpdate,
        shard_id: ShardId,
        block_index: BlockIndex,
        receipt: &ReceiptTransaction,
        new_receipts: &mut HashMap<ShardId, Vec<ReceiptTransaction>>,
    ) -> TransactionResult {
        let mut result = TransactionResult::default();
        if account_to_shard_id(&receipt.receiver) == shard_id {
            let mut tmp_new_receipts = vec![];
            let apply_result = runtime.apply_receipt(
                state_update, 
                receipt,
                &mut tmp_new_receipts,
                block_index,
                &mut result.logs
            );
            for receipt in tmp_new_receipts {
                result.receipts.push(receipt.nonce);
                let shard_id = receipt.shard_id();
                if new_receipts.contains_key(&shard_id) {
                    new_receipts
                    .entry(shard_id)
                    .and_modify(|e| e.push(receipt));
                } else {
                    new_receipts.insert(shard_id, vec![receipt]);
                }
            }
            match apply_result {
                Ok(()) => {
                    state_update.commit();
                    result.status = TransactionStatus::Completed;
                }
                Err(s) => {
                    state_update.rollback();
                    result.logs.push(format!("Runtime error: {}", s));
                    result.status = TransactionStatus::Failed;
                }
            };
        } else {
            // wrong receipt
            result.status = TransactionStatus::Failed;
            result.logs.push("receipt sent to the wrong shard".to_string());
        };
        Self::print_log(&result.logs);
        result
    }

    /// apply receipts from previous block and transactions from this block
    pub fn apply(
        self,
        mut state_update: TrieUpdate,
        apply_state: &ApplyState,
        prev_receipts: &[ReceiptBlock],
        transactions: &[SignedTransaction],
    ) -> ApplyResult {
        let mut new_receipts = HashMap::new();
        let mut authority_proposals = vec![];
        let shard_id = apply_state.shard_id;
        let block_index = apply_state.block_index;
        let mut tx_result = vec![];
        for receipt in prev_receipts.iter().flat_map(|b| &b.receipts) {
            tx_result.push(Self::process_receipt(
                self,
                &mut state_update,
                shard_id,
                block_index,
                receipt,
                &mut new_receipts,
            ));
        }
        for transaction in transactions {
            tx_result.push(Self::process_transaction(
                self,
                &mut state_update,
                block_index,
                transaction,
                &mut new_receipts,
                &mut authority_proposals
            ));
        }
        let (root, db_changes) = state_update.finalize();
        ApplyResult { 
            root,
            db_changes,
            authority_proposals,
            shard_id,
            new_receipts,
            tx_result,
        }
    }

    /// Balances are account, publickey, initial_balance, initial_tx_stake
    pub fn apply_genesis_state(
        self,
        mut state_update: TrieUpdate,
        balances: &[(AccountId, ReadablePublicKey, Balance, Balance)],
        wasm_binary: &[u8],
        initial_authorities: &[(AccountId, ReadableBlsPublicKey, u64)]
    ) -> (MerkleHash, storage::DBChanges) {
        balances.iter().for_each(|(account_id, public_key, balance, initial_tx_stake)| {
            set(
                &mut state_update,
                &account_id_to_bytes(COL_ACCOUNT, &account_id),
                &Account {
                    public_keys: vec![PublicKey::from(&public_key.0)],
                    bls_public_key: BlsPublicKey::empty(),
                    amount: *balance,
                    nonce: 0,
                    staked: 0,
                    code_hash: hash(wasm_binary),
                },
            );
            // Default code
            set(
                &mut state_update,
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
                &mut state_update,
                &key,
                &tx_total_stake,
            );
            // TODO(#345): Add system TX stake
        });
        for (account_id, _pk, amount) in initial_authorities {
            let account_id_bytes = account_id_to_bytes(COL_ACCOUNT, account_id);
            let mut account: Account = get(
                &mut state_update,
                &account_id_bytes,
            ).expect("account must exist");
            account.staked = *amount;
            set(
                &mut state_update,
                &account_id_bytes,
                &account
            );
        }
        state_update.finalize()
    }
}

#[cfg(test)]
mod tests {
    use primitives::hash::hash;
    use primitives::signature::{get_key_pair};
    use storage::test_utils::create_trie;

    use crate::state_viewer::{AccountViewCallResult, TrieViewer};
    use crate::test_utils::*;

    use super::*;

    // TODO(#348): Add tests for TX staking, mana charging and regeneration

    #[test]
    fn test_genesis_state() {
        let (viewer, mut state_update) = get_test_trie_viewer();
        let result = viewer.view_account(&mut state_update, &alice_account());
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
        let trie = create_trie();
        let mut state_update = TrieUpdate::new(trie, MerkleHash::default());
        let test_account = Account::new(vec![], 10, hash(&[]));
        let account_id = bob_account();
        set(&mut state_update, &account_id_to_bytes(COL_ACCOUNT, &account_id), &test_account);
        let get_res = get(&mut state_update, &account_id_to_bytes(COL_ACCOUNT, &account_id)).unwrap();
        assert_eq!(test_account, get_res);
    }

    #[test]
    fn test_get_account_from_trie() {
        let trie = create_trie();
        let root = MerkleHash::default();
        let mut state_update = TrieUpdate::new(trie.clone(), root);
        let test_account = Account::new(vec![], 10, hash(&[]));
        let account_id = bob_account();
        set(&mut state_update, &account_id_to_bytes(COL_ACCOUNT, &account_id), &test_account);
        let (new_root, transaction) = state_update.finalize();
        trie.apply_changes(transaction).unwrap();
        let mut new_state_update = TrieUpdate::new(trie.clone(), new_root);
        let get_res = get(&mut new_state_update, &account_id_to_bytes(COL_ACCOUNT, &account_id)).unwrap();
        assert_eq!(test_account, get_res);
    }

    #[test]
    fn test_smart_contract_simple() {
        let (runtime, trie, root) = get_runtime_and_trie();
        let (mut alice, root) = User::new(runtime, &alice_account(), trie.clone(), root);
        let (new_root, apply_results) = alice.call_function(
            root, &bob_account(), "run_test", vec![], 0
        );
        // 3 results: signedTx, It's Receipt, Mana receipt
        assert_eq!(apply_results.len(), 3);
        // Signed TX successfully generated
        assert_eq!(apply_results[0].tx_result[0].status, TransactionStatus::Completed);
        assert_eq!(apply_results[0].new_receipts.len(), 1);
        // Receipt successfully executed
        assert_eq!(apply_results[1].tx_result[0].status, TransactionStatus::Completed);
        assert_eq!(apply_results[1].new_receipts.len(), 1);
        // Mana successfully executed
        assert_eq!(apply_results[2].tx_result[0].status, TransactionStatus::Completed);
        // Checking final root
        assert_ne!(root, new_root);
    }

    #[test]
    fn test_smart_contract_bad_method_name() {
        let (runtime, trie, root) = get_runtime_and_trie();
        let (mut alice, root) = User::new(runtime, &alice_account(), trie.clone(), root);
        let (_, apply_results) = alice.call_function(
            root, &bob_account(), "_run_test", vec![], 0
        );
        // Only 1 results: signedTx
        assert_eq!(apply_results.len(), 1);
        // Signed TX successfully generated
        assert_eq!(apply_results[0].tx_result[0].status, TransactionStatus::Failed);
        assert_eq!(root, apply_results[0].root);
    }

    #[test]
    fn test_smart_contract_empty_method_name_with_no_tokens() {
        let (runtime, trie, root) = get_runtime_and_trie();
        let (mut alice, root) = User::new(runtime, &alice_account(), trie.clone(), root);
        let (_, apply_results) = alice.call_function(
            root, &bob_account(), "", vec![], 0
        );
        // Only 1 results: signedTx
        assert_eq!(apply_results.len(), 1);
        // Signed TX successfully generated
        assert_eq!(apply_results[0].tx_result[0].status, TransactionStatus::Failed);
        assert_eq!(root, apply_results[0].root);
    }

    #[test]
    fn test_smart_contract_empty_method_name_with_tokens() {
        let (runtime, trie, root) = get_runtime_and_trie();
        let (mut alice, root) = User::new(runtime, &alice_account(), trie.clone(), root);
        let (new_root, apply_results) = alice.call_function(
            root, &bob_account(), "", vec![], 10
        );
        // 3 results: signedTx, It's Receipt, Mana receipt
        assert_eq!(apply_results.len(), 3);
        // Signed TX successfully generated
        assert_eq!(apply_results[0].tx_result[0].status, TransactionStatus::Completed);
        assert_eq!(apply_results[0].new_receipts.len(), 1);
        // Receipt successfully executed
        assert_eq!(apply_results[1].tx_result[0].status, TransactionStatus::Completed);
        assert_eq!(apply_results[1].new_receipts.len(), 1);
        // Mana successfully executed
        assert_eq!(apply_results[2].tx_result[0].status, TransactionStatus::Completed);
        // Checking final root
        assert_ne!(root, new_root);
    }

    #[test]
    fn test_smart_contract_with_args() {
        let (runtime, trie, root) = get_runtime_and_trie();
        let (mut alice, root) = User::new(runtime, &alice_account(), trie.clone(), root);
        let (new_root, apply_results) = alice.call_function(
            root,
            &bob_account(),
            "run_test",
            (2..4).flat_map(|x| encode_int(x).to_vec()).collect(),
            0
        );
        // 3 results: signedTx, It's Receipt, Mana receipt
        assert_eq!(apply_results.len(), 3);
        // Signed TX successfully generated
        assert_eq!(apply_results[0].tx_result[0].status, TransactionStatus::Completed);
        assert_eq!(apply_results[0].new_receipts.len(), 1);
        // Receipt successfully executed
        assert_eq!(apply_results[1].tx_result[0].status, TransactionStatus::Completed);
        assert_eq!(apply_results[1].new_receipts.len(), 1);
        // Mana successfully executed
        assert_eq!(apply_results[2].tx_result[0].status, TransactionStatus::Completed);
        // Checking final root
        assert_ne!(root, new_root);
    }

    #[test]
    fn test_async_call_with_no_callback() {
        let (runtime, trie, root) = get_runtime_and_trie();
        let (mut alice, root) = User::new(runtime, &alice_account(), trie.clone(), root);
        let (_, apply_results) = alice.async_call(root, &bob_account(), "run_test", vec![]);
        // 2 results: Receipt, Mana receipt
        assert_eq!(apply_results.len(), 2);
        // Signed TX successfully generated
        assert_eq!(apply_results[0].tx_result[0].status, TransactionStatus::Completed);
        assert_eq!(apply_results[0].new_receipts.len(), 1);
        assert_eq!(root, apply_results[0].root);
        // Receipt successfully executed
        assert_eq!(apply_results[1].tx_result[0].status, TransactionStatus::Completed);
        // Change in mana and gas
        assert_ne!(root, apply_results[1].root);
    }

    #[test]
    fn test_async_call_with_logs() {
        let (runtime, trie, root) = get_runtime_and_trie();
        let (mut alice, root) = User::new(runtime, &alice_account(), trie, root);
        let (_, apply_results) = alice.async_call(root, &bob_account(), "log_something", vec![]);
        // 2 results: Receipt, Mana receipt
        assert_eq!(apply_results.len(), 2);
        // Signed TX successfully generated
        assert_eq!(apply_results[0].tx_result[0].status, TransactionStatus::Completed);
        assert_eq!(apply_results[0].new_receipts.len(), 1);
        // Receipt successfully executed and contains logs
        assert_eq!(apply_results[1].tx_result[0].status, TransactionStatus::Completed);
        assert_eq!(apply_results[0].tx_result[0].logs[0], "LOG: hello".to_string());
        // Change in mana and gas
        assert_ne!(apply_results[0].root, apply_results[1].root);
    }

    #[test]
    fn test_async_call_with_callback() {
        let (runtime, trie, root) = get_runtime_and_trie();
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
            accounting_info.clone(),
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
        let mut state_update = TrieUpdate::new(trie.clone(), root);
        let mut new_receipts = vec![];
        let mut logs = vec![];
        runtime.apply_receipt(
            &mut state_update,
            &receipt,
            &mut new_receipts,
            block_index,
            &mut logs,
        ).unwrap();
        assert_eq!(new_receipts.len(), 2);

        assert_eq!(new_receipts[0].originator, bob_account());
        assert_eq!(new_receipts[0].receiver, alice_account());
        let callback_res = CallbackResult::new(
            callback_info.clone(), Some(encode_int(10).to_vec())
        );
        assert_eq!(new_receipts[0].body, ReceiptBody::Callback(callback_res));

        assert_eq!(new_receipts[1].originator, bob_account());
        assert_eq!(new_receipts[1].receiver, alice_account());
        if let ReceiptBody::ManaAccounting(ref mana_accounting) = new_receipts[1].body {
            assert_eq!(mana_accounting.mana_refund, 0);
            assert!(mana_accounting.gas_used > 0);
            assert_eq!(mana_accounting.accounting_info, accounting_info);
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_callback() {
        let (runtime, trie, root) = get_runtime_and_trie();
        let (mut alice, root) = User::new(runtime.clone(), &alice_account(), trie.clone(), root);
        let callback_id = [0; 32].to_vec();
        let (new_root, _) = alice.callback(
            root,
            &bob_account(),
            "run_test_with_storage_change",
            vec![],
            callback_id.clone()
        );
        assert_ne!(root, new_root);
        let mut state_update = TrieUpdate::new(trie.clone(), new_root);
        let callback: Option<Callback> = get(&mut state_update, &callback_id_to_bytes(&callback_id));
        assert!(callback.is_none());
    }

    #[test]
    // if the callback failed, it should still be removed
    fn test_callback_failure() {
        let (runtime, trie, root) = get_runtime_and_trie();
        let (mut alice, root) = User::new(runtime.clone(), &alice_account(), trie.clone(), root);
        let callback_id = [0; 32].to_vec();
        let (new_root, _) = alice.callback(
            root,
            &bob_account(),
            "a_function_that_does_not_exist",
            vec![],
            callback_id.clone()
        );
        // the callback should be removed
        assert_eq!(root, new_root);
        let mut state_update = TrieUpdate::new(trie, new_root);
        let callback: Option<Callback> = get(&mut state_update, &callback_id_to_bytes(&callback_id));
        assert!(callback.is_none());
    }

    #[test]
    fn test_nonce_update_when_deploying_contract() {
        let (runtime, trie, root) = get_runtime_and_trie();
        let (mut alice, root) = User::new(runtime.clone(), &alice_account(), trie.clone(), root);
        let wasm_binary = include_bytes!("../../../core/wasm/runtest/res/wasm_with_mem.wasm");
        let (new_root, _) = alice.deploy_contract(root, &alice_account(), wasm_binary);
        let mut state_update = TrieUpdate::new(trie, new_root);
        let account: Account = get(
            &mut state_update,
            &account_id_to_bytes(COL_ACCOUNT, &alice_account())
        ).unwrap();
        assert_eq!(account.nonce, 1);
    }

    #[test]
    fn test_100_accounts() {
        let (mut chain_spec, _, _) = generate_test_chain_spec();
        let public_key = get_key_pair().0;
        for i in 0..100 {
            chain_spec.accounts.push((format!("account{}", i), public_key.to_readable(), 10000, 0));
        }
        let (_, trie, root) = get_runtime_and_trie_from_chain_spec(&chain_spec);
        let viewer = TrieViewer {};
        let mut state_update = TrieUpdate::new(trie, root);
        for i in 0..100 {
            assert_eq!(
                viewer.view_account(&mut state_update, &format!("account{}", i)).unwrap().amount, 
                10000
            )
        }
    }
}

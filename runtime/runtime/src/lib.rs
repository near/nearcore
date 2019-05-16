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

use std::collections::{hash_map::Entry, HashMap};
use std::convert::TryFrom;

use primitives::account::Account;
use primitives::chain::ReceiptBlock;
use primitives::crypto::signature::PublicKey;
use primitives::hash::CryptoHash;
use primitives::transaction::{
    AsyncCall, Callback, CallbackInfo, CallbackResult, FunctionCallTransaction, LogEntry,
    ReceiptBody, ReceiptTransaction, SignedTransaction, TransactionBody, TransactionResult,
    TransactionStatus,
};
use primitives::types::{
    AccountId, AccountingInfo, AuthorityStake, Balance, BlockIndex, Mana, ManaAccounting,
    MerkleHash, PromiseId, ReadableBlsPublicKey, ReadablePublicKey, ShardId,
};
use primitives::utils::{
    account_to_shard_id, create_nonce_with_nonce, key_for_account, key_for_callback, key_for_code,
    key_for_tx_stake,
};
use wasm::executor;
use wasm::types::{ContractCode, ReturnData, RuntimeContext};

use crate::ethereum::EthashProvider;
use crate::ext::RuntimeExt;
use crate::system::{system_account, system_create_account, SYSTEM_METHOD_CREATE_ACCOUNT};
use crate::tx_stakes::{TxStakeConfig, TxTotalStake};
use std::sync::{Arc, Mutex};
use storage::{get, set, TrieUpdate};
use verifier::{TransactionVerifier, VerificationData};

pub mod adapter;
pub mod chain_spec;
mod system;

pub mod ethereum;
pub mod ext;
pub mod state_viewer;
pub mod tx_stakes;

pub const ETHASH_CACHE_PATH: &str = "ethash_cache";
pub(crate) const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

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
    pub largest_tx_nonce: HashMap<AccountId, u64>,
}

pub struct Runtime {
    ethash_provider: Arc<Mutex<EthashProvider>>,
}

impl Runtime {
    pub fn new(ethash_provider: Arc<Mutex<EthashProvider>>) -> Self {
        Runtime { ethash_provider }
    }

    fn try_charge_mana(
        &self,
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
            acc_info_options.push(AccountingInfo {
                originator: originator.clone(),
                contract_id: Some(contract_id.clone()),
            });
        }
        // Trying to use global quota
        acc_info_options.push(AccountingInfo { originator: originator.clone(), contract_id: None });
        for accounting_info in acc_info_options {
            let key = key_for_tx_stake(&accounting_info.originator, &accounting_info.contract_id);
            let tx_total_stake: Option<TxTotalStake> = get(state_update, &key);
            if let Some(mut tx_total_stake) = tx_total_stake {
                tx_total_stake.update(block_index, &config);
                if tx_total_stake.available_mana(&config) >= mana {
                    tx_total_stake.charge_mana(mana, &config);
                    set(state_update, key, &tx_total_stake);
                    return Some(accounting_info);
                }
            }
        }
        None
    }

    fn call_function(
        &self,
        state_update: &mut TrieUpdate,
        transaction: &FunctionCallTransaction,
        hash: CryptoHash,
        sender: &mut Account,
        accounting_info: AccountingInfo,
        mana: Mana,
    ) -> Result<Vec<ReceiptTransaction>, String> {
        match transaction.method_name.get(0) {
            Some(b'_') => {
                return Err(format!(
                    "Account {} tries to call a private method {}",
                    transaction.originator,
                    std::str::from_utf8(&transaction.method_name)
                        .unwrap_or_else(|_| "NON_UTF8_METHOD_NAME"),
                ))
            }
            None if transaction.amount == 0 => {
                return Err(format!("Account {} tries to send 0 tokens", transaction.originator,))
            }
            _ => (),
        };
        if sender.amount >= transaction.amount {
            sender.amount -= transaction.amount;
            set(state_update, key_for_account(&transaction.originator), sender);
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
                )),
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

    /// Subtracts the storage rent from the given account balance.
    fn apply_rent(account_id: &AccountId, account: &mut Account, block_index: BlockIndex) {
        use primitives::serialize::Encode;
        use primitives::types::StorageUsage;
        // Cost of storing a single byte per block.
        const STORAGE_COST: u64 = 1;

        // The number of bytes the account occupies in the Trie.
        let meta_storage = key_for_account(account_id).len() as StorageUsage
            + account.encode().unwrap().len() as StorageUsage;
        let total_storage = account.storage_usage + meta_storage;
        let charge = (block_index - account.storage_paid_at) * total_storage * STORAGE_COST;
        account.amount = if charge <= account.amount { account.amount - charge } else { 0 };
        account.storage_paid_at = block_index;
    }

    /// node receives signed_transaction, processes it
    /// and generates the receipt to send to receiver
    fn apply_signed_transaction(
        &self,
        state_update: &mut TrieUpdate,
        block_index: BlockIndex,
        transaction: &SignedTransaction,
        authority_proposals: &mut Vec<AuthorityStake>,
    ) -> Result<Vec<ReceiptTransaction>, String> {
        let VerificationData { originator_id, mut originator, .. } = {
            let verifier = TransactionVerifier::new(state_update);
            verifier.verify_transaction(transaction)?
        };
        originator.nonce = transaction.body.get_nonce();
        Self::apply_rent(&originator_id, &mut originator, block_index);
        set(state_update, key_for_account(&originator_id), &originator);
        state_update.commit();
        let contract_id = transaction.body.get_contract_id();
        let mana = transaction.body.get_mana();
        let accounting_info = self
            .try_charge_mana(state_update, block_index, &originator_id, &contract_id, mana)
            .ok_or_else(|| {
                format!("sender {} does not have enough mana {}", originator_id, mana)
            })?;
        match transaction.body {
            TransactionBody::SendMoney(ref t) => system::send_money(
                state_update,
                &t,
                transaction.get_hash(),
                &mut originator,
                accounting_info,
            ),
            TransactionBody::Stake(ref t) => system::staking(
                state_update,
                &t,
                &originator_id,
                &mut originator,
                authority_proposals,
            ),
            TransactionBody::FunctionCall(ref t) => self.call_function(
                state_update,
                &t,
                transaction.get_hash(),
                &mut originator,
                accounting_info,
                mana,
            ),
            TransactionBody::DeployContract(ref t) => {
                system::deploy(state_update, &t.contract_id, &t.wasm_byte_array, &mut originator)
            }
            TransactionBody::CreateAccount(ref t) => system::create_account(
                state_update,
                t,
                transaction.get_hash(),
                &mut originator,
                accounting_info,
            ),
            TransactionBody::SwapKey(ref t) => system::swap_key(state_update, t, &mut originator),
            TransactionBody::AddKey(ref t) => system::add_key(state_update, t, &mut originator),
            TransactionBody::DeleteKey(ref t) => {
                system::delete_key(state_update, t, &mut originator, transaction.get_hash())
            }
        }
    }

    fn return_data_to_receipts(
        runtime_ext: &mut RuntimeExt,
        return_data: ReturnData,
        callback_info: &Option<CallbackInfo>,
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
                let res = CallbackResult::new(callback_info.clone(), Some(v));
                Some(res)
            }
            ReturnData::None => {
                let res = CallbackResult::new(callback_info.clone(), Some(vec![]));
                Some(res)
            }
            ReturnData::Promise(PromiseId::Callback(id)) => {
                let callback = runtime_ext.callbacks.get_mut(&id).expect("callback must exist");
                if callback.callback.is_some() {
                    unreachable!("callback already has a callback");
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
                            return Err(
                                "don't return original promise that already has a callback"
                                    .to_string(),
                            );
                        } else {
                            call.callback = Some(callback_info.clone());
                        }
                    }
                    _ => unreachable!("receipt body is not a new call"),
                }
                None
            }
            ReturnData::Promise(PromiseId::Joiner(_)) => {
                return Err(
                    "don't return a joined promise (using promise_and or Promise.all)".to_string()
                )
            }
        };
        let mut receipts = runtime_ext.get_receipts();
        if let Some(callback_res) = callback_res {
            let new_receipt = ReceiptTransaction::new(
                receiver_id.clone(),
                callback_info.receiver.clone(),
                runtime_ext.create_nonce(),
                ReceiptBody::Callback(callback_res),
            );
            receipts.push(new_receipt);
        }
        runtime_ext.flush_callbacks();
        Ok(receipts)
    }

    fn get_code(
        state_update: &TrieUpdate,
        receiver_id: &AccountId,
    ) -> Result<Arc<ContractCode>, String> {
        debug!(target:"runtime", "Calling the contract at account {}", receiver_id);
        let account = get::<Account>(state_update, &key_for_account(receiver_id))
            .ok_or_else(|| format!("cannot find account for account_id {}", receiver_id.clone()))?;
        let code_hash = account.code_hash;
        let code = || {
            get::<ContractCode>(state_update, &key_for_code(receiver_id)).ok_or_else(|| {
                format!("cannot find contract code for account {}", receiver_id.clone())
            })
        };
        wasm::cache::get_code_with_cache(code_hash, code)
    }

    fn apply_async_call(
        &mut self,
        state_update: &mut TrieUpdate,
        async_call: &AsyncCall,
        sender_id: &AccountId,
        receiver_id: &AccountId,
        nonce: &CryptoHash,
        receiver: &mut Account,
        mana_accounting: &mut ManaAccounting,
        block_index: BlockIndex,
        transaction_result: &mut TransactionResult,
    ) -> Result<Vec<ReceiptTransaction>, String> {
        let code = Self::get_code(state_update, receiver_id)?;
        let result = {
            let mut runtime_ext = RuntimeExt::new(
                state_update,
                receiver_id,
                &async_call.accounting_info,
                nonce,
                self.ethash_provider.clone(),
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
                    receiver.storage_usage,
                    block_index,
                    nonce.as_ref().to_vec(),
                ),
            )
            .map_err(|e| format!("wasm async call preparation failed with error: {:?}", e))?;
            mana_accounting.gas_used = wasm_res.gas_used;
            mana_accounting.mana_refund = wasm_res.mana_left;
            transaction_result.logs.append(&mut wasm_res.logs);
            let balance = wasm_res.balance;
            let storage_usage = wasm_res.storage_usage;
            let return_data = wasm_res
                .return_data
                .map_err(|e| format!("wasm async call execution failed with error: {:?}", e))?;
            transaction_result.result = return_data.to_result();
            Self::return_data_to_receipts(
                &mut runtime_ext,
                return_data,
                &async_call.callback,
                receiver_id,
            )
            .and_then(|receipts| {
                receiver.amount = balance;
                receiver.storage_usage = storage_usage;
                Ok(receipts)
            })
        };
        set(state_update, key_for_account(&receiver_id), receiver);
        result
    }

    fn apply_callback(
        &mut self,
        state_update: &mut TrieUpdate,
        callback_res: &CallbackResult,
        sender_id: &AccountId,
        receiver_id: &AccountId,
        nonce: &CryptoHash,
        receiver: &mut Account,
        mana_accounting: &mut ManaAccounting,
        block_index: BlockIndex,
        transaction_result: &mut TransactionResult,
    ) -> Result<Vec<ReceiptTransaction>, String> {
        let mut needs_removal = false;
        let mut callback: Option<Callback> =
            get(state_update, &key_for_callback(&callback_res.info.id));
        let code = Self::get_code(state_update, receiver_id)?;
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
                        self.ethash_provider.clone(),
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
                            receiver.storage_usage,
                            block_index,
                            nonce.as_ref().to_vec(),
                        ),
                    )
                    .map_err(|e| format!("wasm callback execution failed with error: {:?}", e))
                    .and_then(|mut res| {
                        mana_accounting.gas_used = res.gas_used;
                        mana_accounting.mana_refund = res.mana_left;
                        transaction_result.logs.append(&mut res.logs);
                        let balance = res.balance;
                        let storage_usage = res.storage_usage;
                        res.return_data
                            .map_err(|e| {
                                format!("wasm callback execution failed with error: {:?}", e)
                            })
                            .and_then(|data| {
                                transaction_result.result = data.to_result();
                                Self::return_data_to_receipts(
                                    &mut runtime_ext,
                                    data,
                                    &callback.callback,
                                    receiver_id,
                                )
                            })
                            .and_then(|receipts| {
                                receiver.amount = balance;
                                receiver.storage_usage = storage_usage;
                                Ok(receipts)
                            })
                    })
                } else {
                    // otherwise no receipt is generated
                    Ok(vec![])
                }
            }
            _ => {
                return Err(format!("callback id: {:?} not found", callback_res.info.id));
            }
        };
        if needs_removal {
            if receipts.is_err() {
                // On error, we rollback previous changes and then commit the deletion
                state_update.rollback();
                state_update.remove(&key_for_callback(&callback_res.info.id));
                state_update.commit();
            } else {
                state_update.remove(&key_for_callback(&callback_res.info.id));
                set(state_update, key_for_account(&receiver_id), receiver);
            }
        } else {
            // if we don't need to remove callback, since it is updated, we need
            // to update the storage.
            let callback = callback.expect("Cannot be none");
            set(state_update, key_for_callback(&callback_res.info.id), &callback);
        }
        receipts
    }

    fn apply_receipt(
        &mut self,
        state_update: &mut TrieUpdate,
        receipt: &ReceiptTransaction,
        new_receipts: &mut Vec<ReceiptTransaction>,
        block_index: BlockIndex,
        transaction_result: &mut TransactionResult,
    ) -> Result<(), String> {
        let receiver: Option<Account> = get(state_update, &key_for_account(&receipt.receiver));
        let mut amount = 0;
        let mut callback_info = None;
        let mut receiver_exists = true;
        let mut mana_accounting = ManaAccounting::default();
        let result = match receiver {
            Some(mut receiver) => {
                match &receipt.body {
                    ReceiptBody::NewCall(async_call) => {
                        amount = async_call.amount;
                        mana_accounting.mana_refund = async_call.mana;
                        mana_accounting.accounting_info = async_call.accounting_info.clone();
                        callback_info = async_call.callback.clone();
                        if async_call.method_name.is_empty() {
                            transaction_result.result = Some(vec![]);
                            system::deposit(
                                state_update,
                                async_call.amount,
                                &async_call.callback,
                                &receipt.receiver,
                                &receipt.nonce,
                                &mut receiver,
                            )
                        } else if async_call.method_name == SYSTEM_METHOD_CREATE_ACCOUNT {
                            Err(format!("Account {} already exists", receipt.receiver))
                        } else {
                            self.apply_async_call(
                                state_update,
                                &async_call,
                                &receipt.originator,
                                &receipt.receiver,
                                &receipt.nonce,
                                &mut receiver,
                                &mut mana_accounting,
                                block_index,
                                transaction_result,
                            )
                        }
                    }
                    ReceiptBody::Callback(callback_res) => self.apply_callback(
                        state_update,
                        &callback_res,
                        &receipt.originator,
                        &receipt.receiver,
                        &receipt.nonce,
                        &mut receiver,
                        &mut mana_accounting,
                        block_index,
                        transaction_result,
                    ),
                    ReceiptBody::Refund(amount) => {
                        receiver.amount += amount;
                        set(state_update, key_for_account(&receipt.receiver), &receiver);
                        Ok(vec![])
                    }
                    ReceiptBody::ManaAccounting(mana_accounting) => {
                        let key = key_for_tx_stake(
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
                            set(state_update, key, &tx_total_stake);
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
                        system_create_account(state_update, &call, &receipt.receiver)
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
                    let receiver =
                        if receiver_exists { receipt.receiver.clone() } else { system_account() };
                    let new_receipt = ReceiptTransaction::new(
                        receiver,
                        receipt.originator.clone(),
                        create_nonce_with_nonce(&receipt.nonce, new_receipts.len() as u64),
                        ReceiptBody::Refund(amount),
                    );
                    new_receipts.push(new_receipt);
                }
                if let Some(callback_info) = callback_info {
                    let new_receipt = ReceiptTransaction::new(
                        receipt.receiver.clone(),
                        callback_info.receiver.clone(),
                        create_nonce_with_nonce(&receipt.nonce, new_receipts.len() as u64),
                        ReceiptBody::Callback(CallbackResult::new(callback_info, None)),
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

    pub fn process_transaction(
        runtime: &Self,
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
            authority_proposals,
        ) {
            Ok(receipts) => {
                for receipt in receipts {
                    result.receipts.push(receipt.nonce);
                    let shard_id = receipt.shard_id();
                    new_receipts.entry(shard_id).or_insert_with(|| vec![]).push(receipt);
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

    pub fn process_receipt(
        runtime: &mut Self,
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
                &mut result,
            );
            for receipt in tmp_new_receipts {
                result.receipts.push(receipt.nonce);
                let shard_id = receipt.shard_id();
                new_receipts.entry(shard_id).or_insert_with(|| vec![]).push(receipt);
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
            unreachable!("receipt sent to the wrong shard");
        };
        Self::print_log(&result.logs);
        result
    }

    /// apply receipts from previous block and transactions from this block
    pub fn apply(
        &mut self,
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
        let mut largest_tx_nonce = HashMap::new();
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
            let sender = transaction.body.get_originator();
            let nonce = transaction.body.get_nonce();
            match largest_tx_nonce.entry(sender) {
                Entry::Occupied(mut e) => {
                    let largest_nonce = e.get_mut();
                    if *largest_nonce < nonce {
                        *largest_nonce = nonce;
                    }
                }
                Entry::Vacant(e) => {
                    e.insert(nonce);
                }
            };

            tx_result.push(Self::process_transaction(
                self,
                &mut state_update,
                block_index,
                transaction,
                &mut new_receipts,
                &mut authority_proposals,
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
            largest_tx_nonce,
        }
    }

    /// Balances are account, publickey, initial_balance, initial_tx_stake
    pub fn apply_genesis_state(
        &self,
        mut state_update: TrieUpdate,
        balances: &[(AccountId, ReadablePublicKey, Balance, Balance)],
        wasm_binary: &[u8],
        initial_authorities: &[(AccountId, ReadablePublicKey, ReadableBlsPublicKey, u64)],
    ) -> (MerkleHash, storage::DBChanges) {
        balances.iter().for_each(|(account_id, public_key, balance, initial_tx_stake)| {
            let code = ContractCode::new(wasm_binary.to_vec());
            set(
                &mut state_update,
                key_for_account(&account_id),
                &Account {
                    public_keys: vec![PublicKey::try_from(public_key.0.as_str()).unwrap()],
                    amount: *balance,
                    nonce: 0,
                    staked: 0,
                    code_hash: code.get_hash(),
                    storage_usage: 0,
                    storage_paid_at: 0,
                },
            );
            // Default code
            set(&mut state_update, key_for_code(&account_id), &code);
            // Default transaction stake
            let key = key_for_tx_stake(&account_id, &None);
            let mut tx_total_stake = TxTotalStake::new(0);
            tx_total_stake.add_active_stake(*initial_tx_stake);
            set(&mut state_update, key, &tx_total_stake);
            // TODO(#345): Add system TX stake
        });
        for (account_id, _, _, amount) in initial_authorities {
            let account_id_bytes = key_for_account(account_id);
            let mut account: Account =
                get(&state_update, &account_id_bytes).expect("account must exist");
            account.staked = *amount;
            set(&mut state_update, account_id_bytes, &account);
        }
        state_update.finalize()
    }
}

#[cfg(test)]
mod tests {
    use primitives::hash::hash;
    use primitives::types::MerkleHash;
    use storage::test_utils::create_trie;

    use super::*;
    use testlib::runtime_utils::bob_account;

    // TODO(#348): Add tests for TX staking, mana charging and regeneration

    #[test]
    fn test_get_and_set_accounts() {
        let trie = create_trie();
        let mut state_update = TrieUpdate::new(trie, MerkleHash::default());
        let test_account = Account::new(vec![], 10, hash(&[]));
        let account_id = bob_account();
        set(&mut state_update, key_for_account(&account_id), &test_account);
        let get_res = get(&state_update, &key_for_account(&account_id)).unwrap();
        assert_eq!(test_account, get_res);
    }

    #[test]
    fn test_get_account_from_trie() {
        let trie = create_trie();
        let root = MerkleHash::default();
        let mut state_update = TrieUpdate::new(trie.clone(), root);
        let test_account = Account::new(vec![], 10, hash(&[]));
        let account_id = bob_account();
        set(&mut state_update, key_for_account(&account_id), &test_account);
        let (new_root, transaction) = state_update.finalize();
        trie.apply_changes(transaction).unwrap();
        let new_state_update = TrieUpdate::new(trie.clone(), new_root);
        let get_res = get(&new_state_update, &key_for_account(&account_id)).unwrap();
        assert_eq!(test_account, get_res);
    }
}

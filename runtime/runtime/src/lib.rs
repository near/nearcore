#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

use std::collections::{hash_map::Entry, HashMap};
use std::convert::TryFrom;
use std::sync::{Arc, Mutex};

use near_primitives::account::Account;
use near_primitives::crypto::signature::PublicKey;
use near_primitives::hash::CryptoHash;
use near_primitives::serialize::{from_base, Encode};
use near_primitives::transaction::{
    AsyncCall, Callback, CallbackInfo, CallbackResult, FunctionCallTransaction, LogEntry,
    ReceiptBody, ReceiptTransaction, SignedTransaction, TransactionBody, TransactionResult,
    TransactionStatus,
};
use near_primitives::types::StorageUsage;
use near_primitives::types::{
    AccountId, Balance, BlockIndex, MerkleHash, PromiseId, ReadablePublicKey, ShardId,
    ValidatorStake,
};
use near_primitives::utils::{
    account_to_shard_id, create_nonce_with_nonce, key_for_account, key_for_callback, key_for_code,
    system_account,
};
use near_store::{get, set, StoreUpdate, TrieChanges, TrieUpdate};
use near_verifier::{TransactionVerifier, VerificationData};
use wasm::executor;
use wasm::types::{ContractCode, ReturnData, RuntimeContext};

use crate::economics_config::EconomicsConfig;
use crate::ethereum::EthashProvider;
use crate::ext::RuntimeExt;
use crate::system::{system_create_account, SYSTEM_METHOD_CREATE_ACCOUNT};

pub mod adapter;
pub mod economics_config;
pub mod ethereum;
pub mod ext;
pub mod state_viewer;
mod system;

pub const ETHASH_CACHE_PATH: &str = "ethash_cache";
pub(crate) const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

#[derive(Debug)]
pub struct ApplyState {
    pub root: MerkleHash,
    pub shard_id: ShardId,
    pub block_index: u64,
    pub parent_block_hash: CryptoHash,
}

pub struct ApplyResult {
    pub root: MerkleHash,
    pub shard_id: ShardId,
    pub trie_changes: TrieChanges,
    pub validator_proposals: Vec<ValidatorStake>,
    pub new_receipts: HashMap<ShardId, Vec<ReceiptTransaction>>,
    pub tx_result: Vec<TransactionResult>,
    pub largest_tx_nonce: HashMap<AccountId, u64>,
}

pub struct Runtime {
    ethash_provider: Arc<Mutex<EthashProvider>>,
    economics_config: EconomicsConfig,
}

impl Runtime {
    pub fn new(ethash_provider: Arc<Mutex<EthashProvider>>) -> Self {
        Runtime { ethash_provider, economics_config: Default::default() }
    }

    fn call_function(
        &self,
        state_update: &mut TrieUpdate,
        transaction: &FunctionCallTransaction,
        hash: CryptoHash,
        sender: &mut Account,
        refund_account_id: &AccountId,
        public_key: PublicKey,
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
                    refund_account_id.clone(),
                    transaction.originator.clone(),
                    public_key,
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

    fn self_function_call(
        &self,
        state_update: &mut TrieUpdate,
        transaction: &FunctionCallTransaction,
        hash: CryptoHash,
        account: &mut Account,
        refund_account_id: &AccountId,
        public_key: PublicKey,
        block_index: BlockIndex,
        transaction_result: &mut TransactionResult,
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
            None => {
                return Err(format!(
                    "Account {} tries to call itself with empty method name",
                    transaction.originator,
                ))
            }
            _ => (),
        };
        if account.amount >= transaction.amount {
            account.amount -= transaction.amount;
            set(state_update, key_for_account(&transaction.originator), account);
        } else {
            return Err(
                format!(
                    "Account {} tries to call itself with the amount {}, but has staked {} and only has {}",
                    transaction.originator,
                    transaction.amount,
                    account.staked,
                    account.amount
                )
            );
        }

        let mut leftover_balance = 0;

        let res = self.apply_async_call(
            state_update,
            &AsyncCall::new(
                transaction.method_name.clone(),
                transaction.args.clone(),
                transaction.amount,
                refund_account_id.clone(),
                transaction.originator.clone(),
                public_key.clone(),
            ),
            &transaction.originator,
            &transaction.originator,
            &hash,
            account,
            &mut leftover_balance,
            block_index,
            transaction_result,
        );

        if leftover_balance > 0 {
            account.amount += leftover_balance;
            set(state_update, key_for_account(&transaction.originator), account);
        }
        res
    }

    /// Subtracts the storage rent from the given account balance.
    fn apply_rent(&self, account_id: &AccountId, account: &mut Account, block_index: BlockIndex) {
        // The number of bytes the account occupies in the Trie.
        let meta_storage = key_for_account(account_id).len() as StorageUsage
            + account.encode().unwrap().len() as StorageUsage;
        let total_storage = (account.storage_usage + meta_storage) as u128;
        let charge = ((block_index - account.storage_paid_at) as u128)
            * total_storage
            * self.economics_config.storage_cost_byte_per_block;
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
        validator_proposals: &mut Vec<ValidatorStake>,
        transaction_result: &mut TransactionResult,
    ) -> Result<Vec<ReceiptTransaction>, String> {
        let VerificationData { originator_id, mut originator, public_key, .. } = {
            let verifier = TransactionVerifier::new(state_update);
            verifier.verify_transaction(transaction)?
        };
        originator.nonce = transaction.body.get_nonce();
        let transaction_cost = self.economics_config.transactions_costs.cost(&transaction.body);
        originator.checked_sub(transaction_cost)?;
        self.apply_rent(&originator_id, &mut originator, block_index);
        set(state_update, key_for_account(&originator_id), &originator);
        state_update.commit();

        let refund_account_id = &originator_id;
        match transaction.body {
            TransactionBody::SendMoney(ref t) => system::send_money(
                state_update,
                &t,
                transaction.get_hash(),
                &mut originator,
                refund_account_id,
                public_key,
            ),
            TransactionBody::Stake(ref t) => system::staking(
                state_update,
                &t,
                &originator_id,
                &mut originator,
                validator_proposals,
            ),
            TransactionBody::FunctionCall(ref t) if originator_id == t.contract_id => self
                .self_function_call(
                    state_update,
                    &t,
                    transaction.get_hash(),
                    &mut originator,
                    refund_account_id,
                    public_key,
                    block_index,
                    transaction_result,
                ),
            TransactionBody::FunctionCall(ref t) => self.call_function(
                state_update,
                &t,
                transaction.get_hash(),
                &mut originator,
                refund_account_id,
                public_key,
            ),
            TransactionBody::DeployContract(ref t) => {
                system::deploy(state_update, &t.contract_id, &t.wasm_byte_array, &mut originator)
            }
            TransactionBody::CreateAccount(ref t) => system::create_account(
                state_update,
                t,
                transaction.get_hash(),
                &mut originator,
                refund_account_id,
                public_key,
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
        &self,
        state_update: &mut TrieUpdate,
        async_call: &AsyncCall,
        sender_id: &AccountId,
        receiver_id: &AccountId,
        nonce: &CryptoHash,
        receiver: &mut Account,
        leftover_balance: &mut Balance,
        block_index: BlockIndex,
        transaction_result: &mut TransactionResult,
    ) -> Result<Vec<ReceiptTransaction>, String> {
        *leftover_balance = async_call.amount;
        let code = Self::get_code(state_update, receiver_id)?;
        let result = {
            let mut runtime_ext = RuntimeExt::new(
                state_update,
                receiver_id,
                &async_call.refund_account,
                nonce,
                self.ethash_provider.clone(),
                &async_call.originator_id,
                &async_call.public_key,
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
                    receiver.storage_usage,
                    block_index,
                    nonce.as_ref().to_vec(),
                    false,
                    &async_call.originator_id,
                    &async_call.public_key,
                ),
            )
            .map_err(|e| format!("wasm async call preparation failed with error: {:?}", e))?;
            transaction_result.logs.append(&mut wasm_res.logs);
            let balance = wasm_res.frozen_balance;
            *leftover_balance = wasm_res.liquid_balance;
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
        &self,
        state_update: &mut TrieUpdate,
        callback_res: &CallbackResult,
        sender_id: &AccountId,
        receiver_id: &AccountId,
        nonce: &CryptoHash,
        receiver: &mut Account,
        leftover_balance: &mut Balance,
        refund_account: &mut AccountId,
        block_index: BlockIndex,
        transaction_result: &mut TransactionResult,
    ) -> Result<Vec<ReceiptTransaction>, String> {
        let mut needs_removal = false;
        let mut callback: Option<Callback> =
            get(state_update, &key_for_callback(&callback_res.info.id));
        let code = Self::get_code(state_update, receiver_id)?;
        let receipts = match callback {
            Some(ref mut callback) => {
                callback.results[callback_res.info.result_index] = callback_res.result.clone();
                callback.result_counter += 1;
                // if we have gathered all results, execute the callback
                if callback.result_counter == callback.results.len() {
                    *leftover_balance = callback.amount;
                    let mut runtime_ext = RuntimeExt::new(
                        state_update,
                        receiver_id,
                        &callback.refund_account,
                        nonce,
                        self.ethash_provider.clone(),
                        &callback.originator_id,
                        &callback.public_key,
                    );

                    *refund_account = callback.refund_account.clone();
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
                            callback.amount,
                            sender_id,
                            receiver_id,
                            receiver.storage_usage,
                            block_index,
                            nonce.as_ref().to_vec(),
                            false,
                            &callback.originator_id,
                            &callback.public_key,
                        ),
                    )
                    .map_err(|e| format!("wasm callback execution failed with error: {:?}", e))
                    .and_then(|mut res| {
                        transaction_result.logs.append(&mut res.logs);
                        let balance = res.frozen_balance;
                        *leftover_balance = res.liquid_balance;
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
        &self,
        state_update: &mut TrieUpdate,
        receipt: &ReceiptTransaction,
        new_receipts: &mut Vec<ReceiptTransaction>,
        block_index: BlockIndex,
        transaction_result: &mut TransactionResult,
    ) -> Result<(), String> {
        let receiver: Option<Account> = get(state_update, &key_for_account(&receipt.receiver));
        let receiver_exists = receiver.is_some();
        let mut amount = 0;
        let mut callback_info = None;
        // Un-utilized leftover liquid balance that we can refund back to the originator.
        let mut leftover_balance = 0;
        let mut refund_account: String = Default::default();
        let result = match receiver {
            Some(mut receiver) => match &receipt.body {
                ReceiptBody::NewCall(async_call) => {
                    amount = async_call.amount;
                    refund_account = async_call.refund_account.clone();
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
                            &mut leftover_balance,
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
                    &mut leftover_balance,
                    &mut refund_account,
                    block_index,
                    transaction_result,
                ),
                ReceiptBody::Refund(amount) => {
                    receiver.amount += amount;
                    set(state_update, key_for_account(&receipt.receiver), &receiver);
                    Ok(vec![])
                }
            },
            _ => {
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
        if leftover_balance > 0 {
            let new_receipt = ReceiptTransaction::new(
                receipt.receiver.clone(),
                refund_account,
                create_nonce_with_nonce(&receipt.nonce, new_receipts.len() as u64),
                ReceiptBody::Refund(leftover_balance),
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
        &self,
        state_update: &mut TrieUpdate,
        block_index: BlockIndex,
        transaction: &SignedTransaction,
        new_receipts: &mut HashMap<ShardId, Vec<ReceiptTransaction>>,
        validator_proposals: &mut Vec<ValidatorStake>,
    ) -> TransactionResult {
        let mut result = TransactionResult::default();
        match self.apply_signed_transaction(
            state_update,
            block_index,
            transaction,
            validator_proposals,
            &mut result,
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
        &self,
        state_update: &mut TrieUpdate,
        shard_id: ShardId,
        block_index: BlockIndex,
        receipt: &ReceiptTransaction,
        new_receipts: &mut HashMap<ShardId, Vec<ReceiptTransaction>>,
    ) -> TransactionResult {
        let mut result = TransactionResult::default();
        if account_to_shard_id(&receipt.receiver) == shard_id {
            let mut tmp_new_receipts = vec![];
            let apply_result = self.apply_receipt(
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
        &self,
        mut state_update: TrieUpdate,
        apply_state: &ApplyState,
        prev_receipts: &[Vec<ReceiptTransaction>],
        transactions: &[SignedTransaction],
    ) -> Result<ApplyResult, Box<dyn std::error::Error>> {
        let mut new_receipts = HashMap::new();
        let mut validator_proposals = vec![];
        let shard_id = apply_state.shard_id;
        let block_index = apply_state.block_index;
        let mut tx_result = vec![];
        let mut largest_tx_nonce = HashMap::new();
        for receipt in prev_receipts.iter().flatten() {
            tx_result.push(self.process_receipt(
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

            tx_result.push(self.process_transaction(
                &mut state_update,
                block_index,
                transaction,
                &mut new_receipts,
                &mut validator_proposals,
            ));
        }
        let trie_changes = state_update.finalize()?;
        Ok(ApplyResult {
            root: trie_changes.new_root,
            trie_changes,
            validator_proposals: validator_proposals,
            shard_id,
            new_receipts,
            tx_result,
            largest_tx_nonce,
        })
    }

    /// Balances are account, publickey, initial_balance, initial_tx_stake
    pub fn apply_genesis_state(
        &self,
        mut state_update: TrieUpdate,
        balances: &[(AccountId, ReadablePublicKey, Balance)],
        validators: &[(AccountId, ReadablePublicKey, Balance)],
        contracts: &[(AccountId, String)],
    ) -> (StoreUpdate, MerkleHash) {
        let mut code_hash: HashMap<String, CryptoHash> = HashMap::default();
        for (account_id, wasm) in contracts {
            let code =
                ContractCode::new(from_base(wasm).expect("Failed to decode wasm from base58"));
            code_hash.insert(account_id.clone(), code.get_hash());
            // TODO: why do we need code hash if we store code per account? should bee 1:n mapping.
            set(&mut state_update, key_for_code(&account_id), &code);
        }
        for (account_id, public_key, balance) in balances {
            set(
                &mut state_update,
                key_for_account(&account_id),
                &Account {
                    public_keys: vec![PublicKey::try_from(public_key.0.as_str()).unwrap()],
                    amount: *balance,
                    nonce: 0,
                    staked: 0,
                    code_hash: code_hash.remove(account_id).unwrap_or(CryptoHash::default()),
                    storage_usage: 0,
                    storage_paid_at: 0,
                },
            );
        }
        for (account_id, _, amount) in validators {
            let account_id_bytes = key_for_account(account_id);
            let mut account: Account =
                get(&state_update, &account_id_bytes).expect("account must exist");
            account.staked = *amount;
            set(&mut state_update, account_id_bytes, &account);
        }
        let trie = state_update.trie.clone();
        state_update
            .finalize()
            .expect("Genesis state update failed")
            .into(trie)
            .expect("Genesis state update failed")
    }
}

#[cfg(test)]
mod tests {
    use near_primitives::hash::hash;
    use near_primitives::types::MerkleHash;
    use near_store::test_utils::create_trie;
    use testlib::runtime_utils::bob_account;

    use super::*;

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
        let (store_update, new_root) = state_update.finalize().unwrap().into(trie.clone()).unwrap();
        store_update.commit().unwrap();
        let new_state_update = TrieUpdate::new(trie.clone(), new_root);
        let get_res = get(&new_state_update, &key_for_account(&account_id)).unwrap();
        assert_eq!(test_account, get_res);
    }
}

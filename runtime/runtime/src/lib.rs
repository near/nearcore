use std::cmp::max;
use std::collections::{HashMap, HashSet};

use borsh::{BorshDeserialize, BorshSerialize};
use log::debug;

use near_crypto::PublicKey;
use near_primitives::account::{AccessKey, Account};
use near_primitives::contract::ContractCode;
use near_primitives::errors::{ActionError, ActionErrorKind, RuntimeError, TxExecutionError};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{ActionReceipt, DataReceipt, Receipt, ReceiptEnum, ReceivedData};
use near_primitives::state_record::StateRecord;
use near_primitives::transaction::{
    Action, ExecutionOutcome, ExecutionOutcomeWithId, ExecutionStatus, LogEntry, SignedTransaction,
};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{
    AccountId, Balance, BlockHeight, EpochHeight, EpochId, EpochInfoProvider, Gas, MerkleHash,
    Nonce, RawStateChangesWithTrieKey, ShardId, StateChangeCause, StateRoot, ValidatorStake,
};
use near_primitives::utils::{create_nonce_with_nonce, system_account};
use near_store::{
    get, get_account, get_postponed_receipt, get_received_data, remove_postponed_receipt, set,
    set_access_key, set_account, set_code, set_postponed_receipt, set_received_data,
    PartialStorage, ShardTries, StorageError, StoreUpdate, Trie, TrieChanges, TrieUpdate,
};
use near_vm_logic::types::PromiseResult;
use near_vm_logic::ReturnData;
#[cfg(feature = "costs_counting")]
pub use near_vm_runner::EXT_COSTS_COUNTER;

use crate::actions::*;
use crate::balance_checker::check_balance;
use crate::config::{
    exec_fee, safe_add_balance, safe_add_gas, safe_gas_to_balance, total_deposit, total_exec_fees,
    total_prepaid_gas, RuntimeConfig,
};
use crate::verifier::validate_receipt;
pub use crate::verifier::{validate_transaction, verify_and_charge_transaction};
use near_primitives::version::ProtocolVersion;
use std::rc::Rc;

mod actions;
pub mod adapter;
mod balance_checker;
pub mod cache;
pub mod config;
pub mod ext;
mod metrics;
pub mod state_viewer;
mod verifier;

const EXPECT_ACCOUNT_EXISTS: &str = "account exists, checked above";

#[derive(Debug)]
pub struct ApplyState {
    /// Currently building block height.
    // TODO #1903 pub block_height: BlockHeight,
    pub block_index: BlockHeight,
    /// Prev block hash
    pub last_block_hash: CryptoHash,
    /// Current epoch id
    pub epoch_id: EpochId,
    /// Current epoch height
    pub epoch_height: EpochHeight,
    /// Price for the gas.
    pub gas_price: Balance,
    /// The current block timestamp (number of non-leap-nanoseconds since January 1, 1970 0:00:00 UTC).
    pub block_timestamp: u64,
    /// Gas limit for a given chunk.
    /// If None is given, assumes there is no gas limit.
    pub gas_limit: Option<Gas>,
    /// Current random seed (from current block vrf output).
    pub random_seed: CryptoHash,
    /// Current Protocol version when we apply the state transition
    pub current_protocol_version: ProtocolVersion,
}

/// Contains information to update validators accounts at the first block of a new epoch.
#[derive(Debug)]
pub struct ValidatorAccountsUpdate {
    /// Maximum stake across last 3 epochs.
    pub stake_info: HashMap<AccountId, Balance>,
    /// Rewards to distribute to validators.
    pub validator_rewards: HashMap<AccountId, Balance>,
    /// Stake proposals from the last chunk.
    pub last_proposals: HashMap<AccountId, Balance>,
    /// The ID of the protocol treasure account if it belongs to the current shard.
    pub protocol_treasury_account_id: Option<AccountId>,
    /// Accounts to slash and the slashed amount (None means everything)
    pub slashing_info: HashMap<AccountId, Option<Balance>>,
}

#[derive(Debug)]
pub struct VerificationResult {
    /// The amount gas that was burnt to convert the transaction into a receipt and send it.
    pub gas_burnt: Gas,
    /// The remaining amount of gas in the receipt.
    pub gas_remaining: Gas,
    /// The gas price at which the gas was purchased in the receipt.
    pub receipt_gas_price: Balance,
    /// The balance that was burnt to convert the transaction into a receipt and send it.
    pub burnt_amount: Balance,
}

#[derive(Debug, Default)]
pub struct ApplyStats {
    pub tx_burnt_amount: Balance,
    pub slashed_burnt_amount: Balance,
    pub other_burnt_amount: Balance,
    /// This is a negative amount. This amount was not charged from the account that issued
    /// the transaction. It's likely due to the delayed queue of the receipts.
    pub gas_deficit_amount: Balance,
}

pub struct ApplyResult {
    pub state_root: StateRoot,
    pub trie_changes: TrieChanges,
    pub validator_proposals: Vec<ValidatorStake>,
    pub outgoing_receipts: Vec<Receipt>,
    pub outcomes: Vec<ExecutionOutcomeWithId>,
    pub state_changes: Vec<RawStateChangesWithTrieKey>,
    pub stats: ApplyStats,
    pub proof: Option<PartialStorage>,
}

/// Stores indices for a persistent queue for delayed receipts that didn't fit into a block.
#[derive(Default, BorshSerialize, BorshDeserialize, Clone, PartialEq)]
pub struct DelayedReceiptIndices {
    // First inclusive index in the queue.
    first_index: u64,
    // Exclusive end index of the queue
    next_available_index: u64,
}

#[derive(Debug)]
pub struct ActionResult {
    pub gas_burnt: Gas,
    pub gas_burnt_for_function_call: Gas,
    pub gas_used: Gas,
    pub result: Result<ReturnData, ActionError>,
    pub logs: Vec<LogEntry>,
    pub new_receipts: Vec<Receipt>,
    pub validator_proposals: Vec<ValidatorStake>,
}

impl ActionResult {
    pub fn merge(&mut self, mut next_result: ActionResult) -> Result<(), RuntimeError> {
        assert!(next_result.gas_burnt_for_function_call <= next_result.gas_burnt);
        assert!(
            next_result.gas_burnt <= next_result.gas_used,
            "Gas burnt {} <= Gas used {}",
            next_result.gas_burnt,
            next_result.gas_used
        );
        self.gas_burnt = safe_add_gas(self.gas_burnt, next_result.gas_burnt)?;
        self.gas_burnt_for_function_call = safe_add_gas(
            self.gas_burnt_for_function_call,
            next_result.gas_burnt_for_function_call,
        )?;
        self.gas_used = safe_add_gas(self.gas_used, next_result.gas_used)?;
        self.result = next_result.result;
        self.logs.append(&mut next_result.logs);
        if let Ok(ReturnData::ReceiptIndex(ref mut receipt_index)) = self.result {
            // Shifting local receipt index to be global receipt index.
            *receipt_index += self.new_receipts.len() as u64;
        }
        if self.result.is_ok() {
            self.new_receipts.append(&mut next_result.new_receipts);
            self.validator_proposals.append(&mut next_result.validator_proposals);
        } else {
            self.new_receipts.clear();
            self.validator_proposals.clear();
        }
        Ok(())
    }
}

impl Default for ActionResult {
    fn default() -> Self {
        Self {
            gas_burnt: 0,
            gas_burnt_for_function_call: 0,
            gas_used: 0,
            result: Ok(ReturnData::None),
            logs: vec![],
            new_receipts: vec![],
            validator_proposals: vec![],
        }
    }
}

pub struct Runtime {
    pub config: RuntimeConfig,
}

impl Runtime {
    pub fn new(config: RuntimeConfig) -> Self {
        Runtime { config }
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

    /// Takes one signed transaction, verifies it and converts it to a receipt. Add this receipt
    /// either to the new local receipts if the signer is the same as receiver or to the new
    /// outgoing receipts.
    /// When transaction is converted to a receipt, the account is charged for the full value of
    /// the generated receipt.
    /// In case of successful verification (expected for valid chunks), returns the receipt and
    /// `ExecutionOutcomeWithId` for the transaction.
    /// In case of an error, returns either `InvalidTxError` if the transaction verification failed
    /// or a `StorageError` wrapped into `RuntimeError`.
    fn process_transaction(
        &self,
        state_update: &mut TrieUpdate,
        apply_state: &ApplyState,
        signed_transaction: &SignedTransaction,
        stats: &mut ApplyStats,
    ) -> Result<(Receipt, ExecutionOutcomeWithId), RuntimeError> {
        near_metrics::inc_counter(&metrics::TRANSACTION_PROCESSED_TOTAL);
        match verify_and_charge_transaction(
            &self.config,
            state_update,
            apply_state.gas_price,
            signed_transaction,
            true,
        ) {
            Ok(verification_result) => {
                near_metrics::inc_counter(&metrics::TRANSACTION_PROCESSED_SUCCESSFULLY_TOTAL);
                state_update.commit(StateChangeCause::TransactionProcessing {
                    tx_hash: signed_transaction.get_hash(),
                });
                let transaction = &signed_transaction.transaction;
                let receipt = Receipt {
                    predecessor_id: transaction.signer_id.clone(),
                    receiver_id: transaction.receiver_id.clone(),
                    receipt_id: create_nonce_with_nonce(&signed_transaction.get_hash(), 0),
                    receipt: ReceiptEnum::Action(ActionReceipt {
                        signer_id: transaction.signer_id.clone(),
                        signer_public_key: transaction.public_key.clone(),
                        gas_price: verification_result.receipt_gas_price,
                        output_data_receivers: vec![],
                        input_data_ids: vec![],
                        actions: transaction.actions.clone(),
                    }),
                };
                stats.tx_burnt_amount =
                    safe_add_balance(stats.tx_burnt_amount, verification_result.burnt_amount)?;
                let outcome = ExecutionOutcomeWithId {
                    id: signed_transaction.get_hash(),
                    outcome: ExecutionOutcome {
                        status: ExecutionStatus::SuccessReceiptId(receipt.receipt_id),
                        logs: vec![],
                        receipt_ids: vec![receipt.receipt_id],
                        gas_burnt: verification_result.gas_burnt,
                        tokens_burnt: verification_result.burnt_amount,
                        executor_id: transaction.signer_id.clone(),
                    },
                };
                Ok((receipt, outcome))
            }
            Err(e) => {
                near_metrics::inc_counter(&metrics::TRANSACTION_PROCESSED_FAILED_TOTAL);
                state_update.rollback();
                return Err(e);
            }
        }
    }

    fn apply_action(
        &self,
        action: &Action,
        state_update: &mut TrieUpdate,
        apply_state: &ApplyState,
        account: &mut Option<Account>,
        actor_id: &mut AccountId,
        receipt: &Receipt,
        action_receipt: &ActionReceipt,
        promise_results: &[PromiseResult],
        action_hash: &CryptoHash,
        is_last_action: bool,
        epoch_info_provider: &dyn EpochInfoProvider,
    ) -> Result<ActionResult, RuntimeError> {
        let mut result = ActionResult::default();
        let exec_fees = exec_fee(&self.config.transaction_costs, action);
        result.gas_burnt += exec_fees;
        result.gas_used += exec_fees;
        let account_id = &receipt.receiver_id;
        // Account validation
        if let Err(e) = check_account_existence(action, account, account_id) {
            result.result = Err(e);
            return Ok(result);
        }
        // Permission validation
        if let Err(e) = check_actor_permissions(action, account, &actor_id, account_id) {
            result.result = Err(e);
            return Ok(result);
        }
        match action {
            Action::CreateAccount(_) => {
                near_metrics::inc_counter(&metrics::ACTION_CREATE_ACCOUNT_TOTAL);
                action_create_account(
                    &self.config.transaction_costs,
                    &self.config.account_creation_config,
                    account,
                    actor_id,
                    &receipt.receiver_id,
                    &receipt.predecessor_id,
                    &mut result,
                );
            }
            Action::DeployContract(deploy_contract) => {
                near_metrics::inc_counter(&metrics::ACTION_DEPLOY_CONTRACT_TOTAL);
                action_deploy_contract(
                    state_update,
                    account.as_mut().expect(EXPECT_ACCOUNT_EXISTS),
                    &account_id,
                    deploy_contract,
                )?;
            }
            Action::FunctionCall(function_call) => {
                near_metrics::inc_counter(&metrics::ACTION_FUNCTION_CALL_TOTAL);
                action_function_call(
                    state_update,
                    apply_state,
                    account.as_mut().expect(EXPECT_ACCOUNT_EXISTS),
                    receipt,
                    action_receipt,
                    promise_results,
                    &mut result,
                    account_id,
                    function_call,
                    action_hash,
                    &self.config,
                    is_last_action,
                    epoch_info_provider,
                )?;
            }
            Action::Transfer(transfer) => {
                near_metrics::inc_counter(&metrics::ACTION_TRANSFER_TOTAL);
                action_transfer(account.as_mut().expect(EXPECT_ACCOUNT_EXISTS), transfer)?;
                // Check if this is a gas refund, then try to refund the access key allowance.
                if receipt.predecessor_id == system_account()
                    && action_receipt.signer_id == receipt.receiver_id
                {
                    try_refund_allowance(
                        state_update,
                        &receipt.receiver_id,
                        &action_receipt.signer_public_key,
                        transfer,
                    )?;
                }
            }
            Action::Stake(stake) => {
                near_metrics::inc_counter(&metrics::ACTION_STAKE_TOTAL);
                action_stake(
                    account.as_mut().expect(EXPECT_ACCOUNT_EXISTS),
                    &mut result,
                    account_id,
                    stake,
                    &apply_state.last_block_hash,
                    epoch_info_provider,
                )?;
            }
            Action::AddKey(add_key) => {
                near_metrics::inc_counter(&metrics::ACTION_ADD_KEY_TOTAL);
                action_add_key(
                    &self.config.transaction_costs,
                    state_update,
                    account.as_mut().expect(EXPECT_ACCOUNT_EXISTS),
                    &mut result,
                    account_id,
                    add_key,
                )?;
            }
            Action::DeleteKey(delete_key) => {
                near_metrics::inc_counter(&metrics::ACTION_DELETE_KEY_TOTAL);
                action_delete_key(
                    &self.config.transaction_costs,
                    state_update,
                    account.as_mut().expect(EXPECT_ACCOUNT_EXISTS),
                    &mut result,
                    account_id,
                    delete_key,
                )?;
            }
            Action::DeleteAccount(delete_account) => {
                near_metrics::inc_counter(&metrics::ACTION_DELETE_ACCOUNT_TOTAL);
                action_delete_account(
                    state_update,
                    account,
                    actor_id,
                    receipt,
                    &mut result,
                    account_id,
                    delete_account,
                )?;
            }
        };
        Ok(result)
    }

    // Executes when all Receipt `input_data_ids` are in the state
    fn apply_action_receipt(
        &self,
        state_update: &mut TrieUpdate,
        apply_state: &ApplyState,
        receipt: &Receipt,
        outgoing_receipts: &mut Vec<Receipt>,
        validator_proposals: &mut Vec<ValidatorStake>,
        stats: &mut ApplyStats,
        epoch_info_provider: &dyn EpochInfoProvider,
    ) -> Result<ExecutionOutcomeWithId, RuntimeError> {
        let action_receipt = match receipt.receipt {
            ReceiptEnum::Action(ref action_receipt) => action_receipt,
            _ => unreachable!("given receipt should be an action receipt"),
        };
        let account_id = &receipt.receiver_id;
        // Collecting input data and removing it from the state
        let promise_results = action_receipt
            .input_data_ids
            .iter()
            .map(|data_id| {
                let ReceivedData { data } = get_received_data(state_update, account_id, *data_id)?
                    .ok_or_else(|| {
                        StorageError::StorageInconsistentState(
                            "received data should be in the state".to_string(),
                        )
                    })?;
                state_update.remove(TrieKey::ReceivedData {
                    receiver_id: account_id.clone(),
                    data_id: *data_id,
                });
                match data {
                    Some(value) => Ok(PromiseResult::Successful(value)),
                    None => Ok(PromiseResult::Failed),
                }
            })
            .collect::<Result<Vec<PromiseResult>, RuntimeError>>()?;

        // state_update might already have some updates so we need to make sure we commit it before
        // executing the actual receipt
        state_update.commit(StateChangeCause::ActionReceiptProcessingStarted {
            receipt_hash: receipt.get_hash(),
        });

        let mut account = get_account(state_update, account_id)?;
        let mut actor_id = receipt.predecessor_id.clone();
        let mut result = ActionResult::default();
        let exec_fee = self.config.transaction_costs.action_receipt_creation_config.exec_fee();
        result.gas_used = exec_fee;
        result.gas_burnt = exec_fee;
        // Executing actions one by one
        for (action_index, action) in action_receipt.actions.iter().enumerate() {
            let is_last_action = action_index + 1 == action_receipt.actions.len();
            let mut new_result = self.apply_action(
                action,
                state_update,
                apply_state,
                &mut account,
                &mut actor_id,
                receipt,
                action_receipt,
                &promise_results,
                &create_nonce_with_nonce(
                    &receipt.receipt_id,
                    u64::max_value() - action_index as u64,
                ),
                is_last_action,
                epoch_info_provider,
            )?;
            if new_result.result.is_ok() {
                if let Err(e) = new_result.new_receipts.iter().try_for_each(|receipt| {
                    validate_receipt(&self.config.wasm_config.limit_config, receipt)
                }) {
                    new_result.result = Err(ActionErrorKind::NewReceiptValidationError(e).into());
                }
            }
            result.merge(new_result)?;
            // TODO storage error
            if let Err(ref mut res) = result.result {
                res.index = Some(action_index as u64);
                break;
            }
        }

        // Going to check balance covers account's storage.
        if result.result.is_ok() {
            if let Some(ref mut account) = account {
                if let Some(amount) = get_insufficient_storage_stake(account, &self.config)? {
                    result.merge(ActionResult {
                        result: Err(ActionError {
                            index: None,
                            kind: ActionErrorKind::LackBalanceForState {
                                account_id: account_id.clone(),
                                amount,
                            },
                        }),
                        ..Default::default()
                    })?;
                } else {
                    set_account(state_update, account_id.clone(), account);
                }
            }
        }

        // If the receipt is a refund, then we consider it free without burnt gas.
        let gas_deficit_amount = if receipt.predecessor_id == system_account() {
            result.gas_burnt = 0;
            result.gas_used = 0;
            // If the refund fails tokens are burned.
            if result.result.is_err() {
                stats.other_burnt_amount = safe_add_balance(
                    stats.other_burnt_amount,
                    total_deposit(&action_receipt.actions)?,
                )?
            }
            0
        } else {
            // Calculating and generating refunds
            self.generate_refund_receipts(
                apply_state.gas_price,
                receipt,
                action_receipt,
                &mut result,
            )?
        };
        stats.gas_deficit_amount = safe_add_balance(stats.gas_deficit_amount, gas_deficit_amount)?;

        // Moving validator proposals
        validator_proposals.append(&mut result.validator_proposals);

        // Committing or rolling back state.
        match &result.result {
            Ok(_) => {
                state_update.commit(StateChangeCause::ReceiptProcessing {
                    receipt_hash: receipt.get_hash(),
                });
            }
            Err(_) => {
                state_update.rollback();
            }
        };

        // `gas_deficit_amount` is strictly less than `gas_price * gas_burnt`.
        let mut tx_burnt_amount =
            safe_gas_to_balance(apply_state.gas_price, result.gas_burnt)? - gas_deficit_amount;
        // The amount of tokens burnt for the execution of this receipt. It's used in the execution
        // outcome.
        let tokens_burnt = tx_burnt_amount;

        // Adding burnt gas reward for function calls if the account exists.
        let receiver_gas_reward = result.gas_burnt_for_function_call
            * *self.config.transaction_costs.burnt_gas_reward.numer() as u64
            / *self.config.transaction_costs.burnt_gas_reward.denom() as u64;
        // The balance that the current account should receive as a reward for function call
        // execution.
        let receiver_reward = safe_gas_to_balance(apply_state.gas_price, receiver_gas_reward)?
            .saturating_sub(gas_deficit_amount);
        if receiver_reward > 0 {
            let mut account = get_account(state_update, account_id)?;
            if let Some(ref mut account) = account {
                // Validators receive the remaining execution reward that was not given to the
                // account holder. If the account doesn't exist by the end of the execution, the
                // validators receive the full reward.
                tx_burnt_amount -= receiver_reward;
                account.amount = safe_add_balance(account.amount, receiver_reward)?;
                set_account(state_update, account_id.clone(), account);
                state_update.commit(StateChangeCause::ActionReceiptGasReward {
                    receipt_hash: receipt.get_hash(),
                });
            }
        }

        stats.tx_burnt_amount = safe_add_balance(stats.tx_burnt_amount, tx_burnt_amount)?;

        // Generating outgoing data
        // A {
        // B().then(C())}  B--data receipt->C

        // A {
        // B(); 42}
        if !action_receipt.output_data_receivers.is_empty() {
            if let Ok(ReturnData::ReceiptIndex(receipt_index)) = result.result {
                // Modifying a new receipt instead of sending data
                match result
                    .new_receipts
                    .get_mut(receipt_index as usize)
                    .expect("the receipt for the given receipt index should exist")
                    .receipt
                {
                    ReceiptEnum::Action(ref mut new_action_receipt) => new_action_receipt
                        .output_data_receivers
                        .extend_from_slice(&action_receipt.output_data_receivers),
                    _ => unreachable!("the receipt should be an action receipt"),
                }
            } else {
                let data = match result.result {
                    Ok(ReturnData::Value(ref data)) => Some(data.clone()),
                    Ok(_) => Some(vec![]),
                    Err(_) => None,
                };
                result.new_receipts.extend(action_receipt.output_data_receivers.iter().map(
                    |data_receiver| Receipt {
                        predecessor_id: account_id.clone(),
                        receiver_id: data_receiver.receiver_id.clone(),
                        receipt_id: CryptoHash::default(),
                        receipt: ReceiptEnum::Data(DataReceipt {
                            data_id: data_receiver.data_id,
                            data: data.clone(),
                        }),
                    },
                ));
            };
        }

        // Generating receipt IDs
        let receipt_ids = result
            .new_receipts
            .into_iter()
            .enumerate()
            .filter_map(|(receipt_index, mut new_receipt)| {
                let receipt_id =
                    create_nonce_with_nonce(&receipt.receipt_id, receipt_index as Nonce);
                new_receipt.receipt_id = receipt_id;
                let is_action = match &new_receipt.receipt {
                    ReceiptEnum::Action(_) => true,
                    _ => false,
                };
                outgoing_receipts.push(new_receipt);
                if is_action {
                    Some(receipt_id)
                } else {
                    None
                }
            })
            .collect();

        let status = match result.result {
            Ok(ReturnData::ReceiptIndex(receipt_index)) => ExecutionStatus::SuccessReceiptId(
                create_nonce_with_nonce(&receipt.receipt_id, receipt_index as Nonce),
            ),
            Ok(ReturnData::Value(data)) => ExecutionStatus::SuccessValue(data),
            Ok(ReturnData::None) => ExecutionStatus::SuccessValue(vec![]),
            Err(e) => ExecutionStatus::Failure(TxExecutionError::ActionError(e)),
        };

        Self::print_log(&result.logs);

        Ok(ExecutionOutcomeWithId {
            id: receipt.receipt_id,
            outcome: ExecutionOutcome {
                status,
                logs: result.logs,
                receipt_ids,
                gas_burnt: result.gas_burnt,
                tokens_burnt,
                executor_id: account_id.clone(),
            },
        })
    }

    fn generate_refund_receipts(
        &self,
        current_gas_price: Balance,
        receipt: &Receipt,
        action_receipt: &ActionReceipt,
        result: &mut ActionResult,
    ) -> Result<Balance, RuntimeError> {
        let total_deposit = total_deposit(&action_receipt.actions)?;
        let prepaid_gas = total_prepaid_gas(&action_receipt.actions)?;
        let exec_gas = safe_add_gas(
            total_exec_fees(&self.config.transaction_costs, &action_receipt.actions)?,
            self.config.transaction_costs.action_receipt_creation_config.exec_fee(),
        )?;
        let deposit_refund = if result.result.is_err() { total_deposit } else { 0 };
        let gas_refund = if result.result.is_err() {
            safe_add_gas(prepaid_gas, exec_gas)? - result.gas_burnt
        } else {
            safe_add_gas(prepaid_gas, exec_gas)? - result.gas_used
        };
        // Refund for the unused portion of the gas at the price at which this gas was purchased.
        let mut gas_balance_refund = safe_gas_to_balance(action_receipt.gas_price, gas_refund)?;
        let mut gas_deficit_amount = 0;
        if current_gas_price > action_receipt.gas_price {
            // In a rare scenario, when the current gas price is higher than the purchased gas
            // price, the difference is subtracted from the refund. If the refund doesn't have
            // enough balance to cover the difference, then the remaining balance is considered
            // the deficit and it's reported in the stats for the balance checker.
            gas_deficit_amount = safe_gas_to_balance(
                current_gas_price - action_receipt.gas_price,
                result.gas_burnt,
            )?;
            if gas_balance_refund >= gas_deficit_amount {
                gas_balance_refund -= gas_deficit_amount;
                gas_deficit_amount = 0;
            } else {
                gas_deficit_amount -= gas_balance_refund;
                gas_balance_refund = 0;
            }
        } else {
            // Refund for the difference of the purchased gas price and the the current gas price.
            gas_balance_refund = safe_add_balance(
                gas_balance_refund,
                safe_gas_to_balance(
                    action_receipt.gas_price - current_gas_price,
                    result.gas_burnt,
                )?,
            )?;
        }
        if deposit_refund > 0 {
            result
                .new_receipts
                .push(Receipt::new_balance_refund(&receipt.predecessor_id, deposit_refund));
        }
        if gas_balance_refund > 0 {
            // Gas refunds refund the allowance of the access key, so if the key exists on the
            // account it will increase the allowance by the refund amount.
            result.new_receipts.push(Receipt::new_gas_refund(
                &action_receipt.signer_id,
                gas_balance_refund,
                action_receipt.signer_public_key.clone(),
            ));
        }
        Ok(gas_deficit_amount)
    }

    fn process_receipt(
        &self,
        state_update: &mut TrieUpdate,
        apply_state: &ApplyState,
        receipt: &Receipt,
        outgoing_receipts: &mut Vec<Receipt>,
        validator_proposals: &mut Vec<ValidatorStake>,
        stats: &mut ApplyStats,
        epoch_info_provider: &dyn EpochInfoProvider,
    ) -> Result<Option<ExecutionOutcomeWithId>, RuntimeError> {
        let account_id = &receipt.receiver_id;
        match receipt.receipt {
            ReceiptEnum::Data(ref data_receipt) => {
                // Received a new data receipt.
                // Saving the data into the state keyed by the data_id.
                set_received_data(
                    state_update,
                    account_id.clone(),
                    data_receipt.data_id,
                    &ReceivedData { data: data_receipt.data.clone() },
                );
                // Check if there is already a receipt that was postponed and was awaiting for the
                // given data_id.
                // If we don't have a postponed receipt yet, we don't need to do anything for now.
                if let Some(receipt_id) = get(
                    state_update,
                    &TrieKey::PostponedReceiptId {
                        receiver_id: account_id.clone(),
                        data_id: data_receipt.data_id,
                    },
                )? {
                    // There is already a receipt that is awaiting for the just received data.
                    // Removing this pending data_id for the receipt from the state.
                    state_update.remove(TrieKey::PostponedReceiptId {
                        receiver_id: account_id.clone(),
                        data_id: data_receipt.data_id,
                    });
                    // Checking how many input data items is pending for the receipt.
                    let pending_data_count: u32 = get(
                        state_update,
                        &TrieKey::PendingDataCount { receiver_id: account_id.clone(), receipt_id },
                    )?
                    .ok_or_else(|| {
                        StorageError::StorageInconsistentState(
                            "pending data count should be in the state".to_string(),
                        )
                    })?;
                    if pending_data_count == 1 {
                        // It was the last input data pending for this receipt. We'll cleanup
                        // some receipt related fields from the state and execute the receipt.

                        // Removing pending data count from the state.
                        state_update.remove(TrieKey::PendingDataCount {
                            receiver_id: account_id.clone(),
                            receipt_id,
                        });
                        // Fetching the receipt itself.
                        let ready_receipt =
                            get_postponed_receipt(state_update, account_id, receipt_id)?
                                .ok_or_else(|| {
                                    StorageError::StorageInconsistentState(
                                        "pending receipt should be in the state".to_string(),
                                    )
                                })?;
                        // Removing the receipt from the state.
                        remove_postponed_receipt(state_update, account_id, receipt_id);
                        // Executing the receipt. It will read all the input data and clean it up
                        // from the state.
                        return self
                            .apply_action_receipt(
                                state_update,
                                apply_state,
                                &ready_receipt,
                                outgoing_receipts,
                                validator_proposals,
                                stats,
                                epoch_info_provider,
                            )
                            .map(Some);
                    } else {
                        // There is still some pending data for the receipt, so we update the
                        // pending data count in the state.
                        set(
                            state_update,
                            TrieKey::PendingDataCount {
                                receiver_id: account_id.clone(),
                                receipt_id,
                            },
                            &(pending_data_count.checked_sub(1).ok_or_else(|| {
                                StorageError::StorageInconsistentState(
                                    "pending data count is 0, but there is a new DataReceipt"
                                        .to_string(),
                                )
                            })?),
                        );
                    }
                }
            }
            ReceiptEnum::Action(ref action_receipt) => {
                // Received a new action receipt. We'll first check how many input data items
                // were already received before and saved in the state.
                // And if we have all input data, then we can immediately execute the receipt.
                // If not, then we will postpone this receipt for later.
                let mut pending_data_count: u32 = 0;
                for data_id in &action_receipt.input_data_ids {
                    if get_received_data(state_update, account_id, *data_id)?.is_none() {
                        pending_data_count += 1;
                        // The data for a given data_id is not available, so we save a link to this
                        // receipt_id for the pending data_id into the state.
                        set(
                            state_update,
                            TrieKey::PostponedReceiptId {
                                receiver_id: account_id.clone(),
                                data_id: *data_id,
                            },
                            &receipt.receipt_id,
                        )
                    }
                }
                if pending_data_count == 0 {
                    // All input data is available. Executing the receipt. It will cleanup
                    // input data from the state.
                    return self
                        .apply_action_receipt(
                            state_update,
                            apply_state,
                            receipt,
                            outgoing_receipts,
                            validator_proposals,
                            stats,
                            epoch_info_provider,
                        )
                        .map(Some);
                } else {
                    // Not all input data is available now.
                    // Save the counter for the number of pending input data items into the state.
                    set(
                        state_update,
                        TrieKey::PendingDataCount {
                            receiver_id: account_id.clone(),
                            receipt_id: receipt.receipt_id,
                        },
                        &pending_data_count,
                    );
                    // Save the receipt itself into the state.
                    set_postponed_receipt(state_update, &receipt);
                }
            }
        };
        // We didn't trigger execution, so we need to commit the state.
        state_update
            .commit(StateChangeCause::PostponedReceipt { receipt_hash: receipt.get_hash() });
        Ok(None)
    }

    /// Iterates over the validators in the current shard and updates their accounts to return stake
    /// and allocate rewards. Also updates protocol treasure account if it belongs to the current
    /// shard.
    fn update_validator_accounts(
        &self,
        state_update: &mut TrieUpdate,
        validator_accounts_update: &ValidatorAccountsUpdate,
        stats: &mut ApplyStats,
    ) -> Result<(), RuntimeError> {
        for (account_id, max_of_stakes) in &validator_accounts_update.stake_info {
            if let Some(mut account) = get_account(state_update, account_id)? {
                if let Some(reward) = validator_accounts_update.validator_rewards.get(account_id) {
                    debug!(target: "runtime", "account {} adding reward {} to stake {}", account_id, reward, account.locked);
                    account.locked = account
                        .locked
                        .checked_add(*reward)
                        .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?;
                }

                debug!(target: "runtime",
                       "account {} stake {} max_of_stakes: {}",
                       account_id, account.locked, max_of_stakes
                );
                if account.locked < *max_of_stakes {
                    return Err(StorageError::StorageInconsistentState(format!(
                        "FATAL: staking invariant does not hold. \
                         Account stake {} is less than maximum of stakes {} in the past three epochs",
                        account.locked,
                        max_of_stakes)).into());
                }
                let last_proposal =
                    *validator_accounts_update.last_proposals.get(account_id).unwrap_or(&0);
                let return_stake = account
                    .locked
                    .checked_sub(max(*max_of_stakes, last_proposal))
                    .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?;
                debug!(target: "runtime", "account {} return stake {}", account_id, return_stake);
                account.locked = account
                    .locked
                    .checked_sub(return_stake)
                    .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?;
                account.amount = account
                    .amount
                    .checked_add(return_stake)
                    .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?;

                set_account(state_update, account_id.clone(), &account);
            } else if *max_of_stakes > 0 {
                // if max_of_stakes > 0, it means that the account must have locked balance
                // and therefore must exist
                return Err(StorageError::StorageInconsistentState(format!(
                    "Account {} with max of stakes {} is not found",
                    account_id, max_of_stakes
                ))
                .into());
            }
        }

        for (account_id, stake) in validator_accounts_update.slashing_info.iter() {
            if let Some(mut account) = get_account(state_update, &account_id)? {
                let amount_to_slash = stake.unwrap_or(account.locked);
                debug!(target: "runtime", "slashing {} of {} from {}", amount_to_slash, account.locked, account_id);
                if account.locked < amount_to_slash {
                    return Err(StorageError::StorageInconsistentState(format!(
                        "FATAL: staking invariant does not hold. Account locked {} is less than slashed {}",
                        account.locked, amount_to_slash)).into());
                }
                stats.slashed_burnt_amount = stats
                    .slashed_burnt_amount
                    .checked_add(amount_to_slash)
                    .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?;
                account.locked = account
                    .locked
                    .checked_sub(amount_to_slash)
                    .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?;
                set_account(state_update, account_id.clone(), &account);
            } else {
                return Err(StorageError::StorageInconsistentState(format!(
                    "Account {} to slash is not found",
                    account_id
                ))
                .into());
            }
        }

        if let Some(account_id) = &validator_accounts_update.protocol_treasury_account_id {
            // If protocol treasury stakes, then the rewards was already distributed above.
            if !validator_accounts_update.stake_info.contains_key(account_id) {
                let mut account = get_account(state_update, account_id)?.ok_or_else(|| {
                    StorageError::StorageInconsistentState(format!(
                        "Protocol treasury account {} is not found",
                        account_id
                    ))
                })?;
                let treasury_reward = *validator_accounts_update
                    .validator_rewards
                    .get(account_id)
                    .ok_or_else(|| {
                        StorageError::StorageInconsistentState(format!(
                            "Validator reward for the protocol treasury account {} is not found",
                            account_id
                        ))
                    })?;
                account.amount = account
                    .amount
                    .checked_add(treasury_reward)
                    .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?;
                set_account(state_update, account_id.clone(), &account);
            }
        }
        state_update.commit(StateChangeCause::ValidatorAccountsUpdate);

        Ok(())
    }

    /// Applies new singed transactions and incoming receipts for some chunk/shard on top of
    /// given trie and the given state root.
    /// If the validator accounts update is provided, updates validators accounts.
    /// All new signed transactions should be valid and already verified by the chunk producer.
    /// If any transaction is invalid, it would return an `InvalidTxError`.
    /// Returns an `ApplyResult` that contains the new state root, trie changes,
    /// new outgoing receipts, execution outcomes for
    /// all transactions, local action receipts (generated from transactions with signer ==
    /// receivers) and incoming action receipts.
    pub fn apply(
        &self,
        trie: Trie,
        root: CryptoHash,
        validator_accounts_update: &Option<ValidatorAccountsUpdate>,
        apply_state: &ApplyState,
        incoming_receipts: &[Receipt],
        transactions: &[SignedTransaction],
        epoch_info_provider: &dyn EpochInfoProvider,
    ) -> Result<ApplyResult, RuntimeError> {
        let trie = Rc::new(trie);
        let initial_state = TrieUpdate::new(trie.clone(), root);
        let mut state_update = TrieUpdate::new(trie.clone(), root);

        let mut stats = ApplyStats::default();

        if let Some(validator_accounts_update) = validator_accounts_update {
            self.update_validator_accounts(
                &mut state_update,
                validator_accounts_update,
                &mut stats,
            )?;
        }

        let mut outgoing_receipts = Vec::new();
        let mut validator_proposals = vec![];
        let mut local_receipts = vec![];
        let mut outcomes = vec![];
        let mut total_gas_burnt = 0;

        for signed_transaction in transactions {
            let (receipt, outcome_with_id) = self.process_transaction(
                &mut state_update,
                apply_state,
                signed_transaction,
                &mut stats,
            )?;
            if receipt.receiver_id == signed_transaction.transaction.signer_id {
                local_receipts.push(receipt);
            } else {
                outgoing_receipts.push(receipt);
            }

            total_gas_burnt += outcome_with_id.outcome.gas_burnt;

            outcomes.push(outcome_with_id);
        }

        let mut delayed_receipts_indices: DelayedReceiptIndices =
            get(&state_update, &TrieKey::DelayedReceiptIndices)?.unwrap_or_default();
        let initial_delayed_receipt_indices = delayed_receipts_indices.clone();

        let mut process_receipt = |receipt: &Receipt,
                                   state_update: &mut TrieUpdate,
                                   total_gas_burnt: &mut Gas|
         -> Result<_, RuntimeError> {
            self.process_receipt(
                state_update,
                apply_state,
                receipt,
                &mut outgoing_receipts,
                &mut validator_proposals,
                &mut stats,
                epoch_info_provider,
            )?
            .into_iter()
            .try_for_each(
                |outcome_with_id: ExecutionOutcomeWithId| -> Result<(), RuntimeError> {
                    *total_gas_burnt =
                        safe_add_gas(*total_gas_burnt, outcome_with_id.outcome.gas_burnt)?;
                    outcomes.push(outcome_with_id);
                    Ok(())
                },
            )?;
            Ok(())
        };

        let gas_limit = apply_state.gas_limit.unwrap_or(Gas::max_value());

        // We first process local receipts. They contain staking, local contract calls, etc.
        for receipt in local_receipts.iter() {
            if total_gas_burnt < gas_limit {
                // NOTE: We don't need to validate the local receipt, because it's just validated in
                // the `verify_and_charge_transaction`.
                process_receipt(&receipt, &mut state_update, &mut total_gas_burnt)?;
            } else {
                Self::delay_receipt(&mut state_update, &mut delayed_receipts_indices, receipt)?;
            }
        }

        // Then we process the delayed receipts. It's a backlog of receipts from the past blocks.
        while delayed_receipts_indices.first_index < delayed_receipts_indices.next_available_index {
            if total_gas_burnt >= gas_limit {
                break;
            }
            let key = TrieKey::DelayedReceipt { index: delayed_receipts_indices.first_index };
            let receipt: Receipt = get(&state_update, &key)?.ok_or_else(|| {
                StorageError::StorageInconsistentState(format!(
                    "Delayed receipt #{} should be in the state",
                    delayed_receipts_indices.first_index
                ))
            })?;

            // Validating the delayed receipt. If it fails, it's likely the state is inconsistent.
            validate_receipt(&self.config.wasm_config.limit_config, &receipt).map_err(|e| {
                StorageError::StorageInconsistentState(format!(
                    "Delayed receipt #{} in the state is invalid: {}",
                    delayed_receipts_indices.first_index, e
                ))
            })?;

            state_update.remove(key);
            // Math checked above: first_index is less than next_available_index
            delayed_receipts_indices.first_index += 1;
            process_receipt(&receipt, &mut state_update, &mut total_gas_burnt)?;
        }

        // And then we process the new incoming receipts. These are receipts from other shards.
        for receipt in incoming_receipts.iter() {
            // Validating new incoming no matter whether we have available gas or not. We don't
            // want to store invalid receipts in state as delayed.
            validate_receipt(&self.config.wasm_config.limit_config, &receipt)
                .map_err(RuntimeError::ReceiptValidationError)?;
            if total_gas_burnt < gas_limit {
                process_receipt(&receipt, &mut state_update, &mut total_gas_burnt)?;
            } else {
                Self::delay_receipt(&mut state_update, &mut delayed_receipts_indices, receipt)?;
            }
        }

        if delayed_receipts_indices != initial_delayed_receipt_indices {
            set(&mut state_update, TrieKey::DelayedReceiptIndices, &delayed_receipts_indices);
        }

        check_balance(
            &self.config.transaction_costs,
            &initial_state,
            &state_update,
            validator_accounts_update,
            incoming_receipts,
            transactions,
            &outgoing_receipts,
            &stats,
        )?;

        state_update.commit(StateChangeCause::UpdatedDelayedReceipts);

        let (trie_changes, state_changes) = state_update.finalize()?;

        // Dedup proposals from the same account.
        // The order is deterministically changed.
        let mut unique_proposals = vec![];
        let mut account_ids = HashSet::new();
        for proposal in validator_proposals.into_iter().rev() {
            if !account_ids.contains(&proposal.account_id) {
                account_ids.insert(proposal.account_id.clone());
                unique_proposals.push(proposal);
            }
        }

        let state_root = trie_changes.new_root;
        let proof = trie.recorded_storage();
        Ok(ApplyResult {
            state_root,
            trie_changes,
            validator_proposals: unique_proposals,
            outgoing_receipts,
            outcomes,
            state_changes,
            stats,
            proof,
        })
    }

    // Adds the given receipt into the end of the delayed receipt queue in the state.
    fn delay_receipt(
        state_update: &mut TrieUpdate,
        delayed_receipts_indices: &mut DelayedReceiptIndices,
        receipt: &Receipt,
    ) -> Result<(), StorageError> {
        set(
            state_update,
            TrieKey::DelayedReceipt { index: delayed_receipts_indices.next_available_index },
            receipt,
        );
        delayed_receipts_indices.next_available_index =
            delayed_receipts_indices.next_available_index.checked_add(1).ok_or_else(|| {
                StorageError::StorageInconsistentState(
                    "Next available index for delayed receipt exceeded the integer limit"
                        .to_string(),
                )
            })?;
        Ok(())
    }

    /// It's okay to use unsafe math here, because this method should only be called on the trusted
    /// state records (e.g. at launch from genesis)
    pub fn compute_storage_usage(&self, records: &[StateRecord]) -> HashMap<AccountId, u64> {
        let mut result = HashMap::new();
        let config = &self.config.transaction_costs.storage_usage_config;
        for record in records {
            let account_and_storage = match record {
                StateRecord::Account { account_id, .. } => {
                    Some((account_id.clone(), config.num_bytes_account))
                }
                StateRecord::Data { account_id, data_key, value } => {
                    let storage_usage =
                        config.num_extra_bytes_record + data_key.len() as u64 + value.len() as u64;
                    Some((account_id.clone(), storage_usage))
                }
                StateRecord::Contract { account_id, code } => {
                    Some((account_id.clone(), code.len() as u64))
                }
                StateRecord::AccessKey { account_id, public_key, access_key } => {
                    let public_key: PublicKey = public_key.clone();
                    let access_key: AccessKey = access_key.clone().into();
                    let storage_usage = config.num_extra_bytes_record
                        + public_key.try_to_vec().unwrap().len() as u64
                        + access_key.try_to_vec().unwrap().len() as u64;
                    Some((account_id.clone(), storage_usage))
                }
                StateRecord::PostponedReceipt(_) => None,
                StateRecord::ReceivedData { .. } => None,
                StateRecord::DelayedReceipt(_) => None,
            };
            if let Some((account, storage_usage)) = account_and_storage {
                *result.entry(account).or_default() += storage_usage;
            }
        }
        result
    }

    /// Balances are account, publickey, initial_balance, initial_tx_stake
    pub fn apply_genesis_state(
        &self,
        tries: ShardTries,
        shard_id: ShardId,
        validators: &[(AccountId, PublicKey, Balance)],
        records: &[StateRecord],
    ) -> (StoreUpdate, StateRoot) {
        let mut state_update = tries.new_trie_update(shard_id, MerkleHash::default());
        let mut postponed_receipts: Vec<Receipt> = vec![];
        let mut delayed_receipts_indices = DelayedReceiptIndices::default();
        for record in records {
            match record.clone() {
                StateRecord::Account { account_id, account } => {
                    set_account(&mut state_update, account_id, &account);
                }
                StateRecord::Data { account_id, data_key, value } => {
                    state_update.set(TrieKey::ContractData { key: data_key, account_id }, value);
                }
                StateRecord::Contract { account_id, code } => {
                    let acc = get_account(&state_update, &account_id).expect("Failed to read state").expect("Code state record should be preceded by the corresponding account record");
                    // Recompute contract code hash.
                    let code = ContractCode::new(code, None);
                    set_code(&mut state_update, account_id, &code);
                    assert_eq!(code.get_hash(), acc.code_hash);
                }
                StateRecord::AccessKey { account_id, public_key, access_key } => {
                    set_access_key(&mut state_update, account_id, public_key, &access_key);
                }
                StateRecord::PostponedReceipt(receipt) => {
                    // Delaying processing postponed receipts, until we process all data first
                    postponed_receipts.push(*receipt);
                }
                StateRecord::ReceivedData { account_id, data_id, data } => {
                    set_received_data(
                        &mut state_update,
                        account_id,
                        data_id,
                        &ReceivedData { data },
                    );
                }
                StateRecord::DelayedReceipt(receipt) => {
                    Self::delay_receipt(
                        &mut state_update,
                        &mut delayed_receipts_indices,
                        &*receipt,
                    )
                    .unwrap();
                }
            }
        }
        for (account_id, storage_usage) in self.compute_storage_usage(records) {
            let mut account = get_account(&state_update, &account_id)
                .expect("Genesis storage error")
                .expect("Account must exist");
            account.storage_usage = storage_usage;
            set_account(&mut state_update, account_id, &account);
        }
        // Processing postponed receipts after we stored all received data
        for receipt in postponed_receipts {
            let account_id = &receipt.receiver_id;
            let action_receipt = match &receipt.receipt {
                ReceiptEnum::Action(a) => a,
                _ => panic!("Expected action receipt"),
            };
            // Logic similar to `apply_receipt`
            let mut pending_data_count: u32 = 0;
            for data_id in &action_receipt.input_data_ids {
                if get_received_data(&state_update, account_id, *data_id)
                    .expect("Genesis storage error")
                    .is_none()
                {
                    pending_data_count += 1;
                    set(
                        &mut state_update,
                        TrieKey::PostponedReceiptId {
                            receiver_id: account_id.clone(),
                            data_id: *data_id,
                        },
                        &receipt.receipt_id,
                    )
                }
            }
            if pending_data_count == 0 {
                panic!("Postponed receipt should have pending data")
            } else {
                set(
                    &mut state_update,
                    TrieKey::PendingDataCount {
                        receiver_id: account_id.clone(),
                        receipt_id: receipt.receipt_id,
                    },
                    &pending_data_count,
                );
                set_postponed_receipt(&mut state_update, &receipt);
            }
        }
        if delayed_receipts_indices != DelayedReceiptIndices::default() {
            set(&mut state_update, TrieKey::DelayedReceiptIndices, &delayed_receipts_indices);
        }

        for (account_id, _, amount) in validators {
            let mut account: Account = get_account(&state_update, account_id)
                .expect("Genesis storage error")
                .expect("account must exist");
            account.locked = *amount;
            set_account(&mut state_update, account_id.clone(), &account);
        }
        state_update.commit(StateChangeCause::InitialState);
        let trie_changes = state_update.finalize().expect("Genesis state update failed").0;

        let (store_update, state_root) =
            tries.apply_all(&trie_changes, shard_id).expect("Genesis state update failed");
        (store_update, state_root)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use near_crypto::{InMemorySigner, KeyType, Signer};
    use near_primitives::errors::ReceiptValidationError;
    use near_primitives::hash::hash;
    use near_primitives::test_utils::{account_new, MockEpochInfoProvider};
    use near_primitives::transaction::{FunctionCallAction, TransferAction};
    use near_primitives::types::MerkleHash;
    use near_store::test_utils::create_tries;
    use std::sync::Arc;
    use testlib::runtime_utils::{alice_account, bob_account};

    const GAS_PRICE: Balance = 5000;

    fn to_yocto(near: Balance) -> Balance {
        near * 10u128.pow(24)
    }

    #[test]
    fn test_get_and_set_accounts() {
        let tries = create_tries();
        let mut state_update = tries.new_trie_update(0, MerkleHash::default());
        let test_account = account_new(to_yocto(10), hash(&[]));
        let account_id = bob_account();
        set_account(&mut state_update, account_id.clone(), &test_account);
        let get_res = get_account(&state_update, &account_id).unwrap().unwrap();
        assert_eq!(test_account, get_res);
    }

    #[test]
    fn test_get_account_from_trie() {
        let tries = create_tries();
        let root = MerkleHash::default();
        let mut state_update = tries.new_trie_update(0, root);
        let test_account = account_new(to_yocto(10), hash(&[]));
        let account_id = bob_account();
        set_account(&mut state_update, account_id.clone(), &test_account);
        state_update.commit(StateChangeCause::InitialState);
        let trie_changes = state_update.finalize().unwrap().0;
        let (store_update, new_root) = tries.apply_all(&trie_changes, 0).unwrap();
        store_update.commit().unwrap();
        let new_state_update = tries.new_trie_update(0, new_root);
        let get_res = get_account(&new_state_update, &account_id).unwrap().unwrap();
        assert_eq!(test_account, get_res);
    }

    /***************/
    /* Apply tests */
    /***************/

    fn setup_runtime(
        initial_balance: Balance,
        initial_locked: Balance,
        gas_limit: Gas,
    ) -> (Runtime, ShardTries, CryptoHash, ApplyState, Arc<InMemorySigner>, impl EpochInfoProvider)
    {
        let tries = create_tries();
        let root = MerkleHash::default();
        let runtime = Runtime::new(RuntimeConfig::default());

        let account_id = alice_account();
        let signer =
            Arc::new(InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id));

        let mut initial_state = tries.new_trie_update(0, root);
        let mut initial_account = account_new(initial_balance, hash(&[]));
        initial_account.locked = initial_locked;
        set_account(&mut initial_state, account_id.clone(), &initial_account);
        set_access_key(
            &mut initial_state,
            account_id.clone(),
            signer.public_key(),
            &AccessKey::full_access(),
        );
        initial_state.commit(StateChangeCause::InitialState);
        let trie_changes = initial_state.finalize().unwrap().0;
        let (store_update, root) = tries.apply_all(&trie_changes, 0).unwrap();
        store_update.commit().unwrap();

        let apply_state = ApplyState {
            block_index: 0,
            last_block_hash: Default::default(),
            epoch_id: Default::default(),
            epoch_height: 0,
            gas_price: GAS_PRICE,
            block_timestamp: 100,
            gas_limit: Some(gas_limit),
            random_seed: Default::default(),
            current_protocol_version: 0,
        };

        (runtime, tries, root, apply_state, signer, MockEpochInfoProvider::default())
    }

    #[test]
    fn test_apply_no_op() {
        let (runtime, tries, root, apply_state, _, epoch_info_provider) =
            setup_runtime(to_yocto(1_000_000), 0, 10u64.pow(15));
        runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &[],
                &[],
                &epoch_info_provider,
            )
            .unwrap();
    }

    #[test]
    fn test_apply_check_balance_validation_rewards() {
        let initial_locked = to_yocto(500_000);
        let reward = to_yocto(10_000_000);
        let small_refund = to_yocto(500);
        let (runtime, tries, root, apply_state, _, epoch_info_provider) =
            setup_runtime(to_yocto(1_000_000), initial_locked, 10u64.pow(15));

        let validator_accounts_update = ValidatorAccountsUpdate {
            stake_info: vec![(alice_account(), initial_locked)].into_iter().collect(),
            validator_rewards: vec![(alice_account(), reward)].into_iter().collect(),
            last_proposals: Default::default(),
            protocol_treasury_account_id: None,
            slashing_info: HashMap::default(),
        };

        runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &Some(validator_accounts_update),
                &apply_state,
                &[Receipt::new_balance_refund(&alice_account(), small_refund)],
                &[],
                &epoch_info_provider,
            )
            .unwrap();
    }

    #[test]
    fn test_apply_delayed_receipts_feed_all_at_once() {
        let initial_balance = to_yocto(1_000_000);
        let initial_locked = to_yocto(500_000);
        let small_transfer = to_yocto(10_000);
        let gas_limit = 1;
        let (runtime, tries, mut root, apply_state, _, epoch_info_provider) =
            setup_runtime(initial_balance, initial_locked, gas_limit);

        let n = 10;
        let receipts = generate_receipts(small_transfer, n);

        // Checking n receipts delayed by 1 + 3 extra
        for i in 1..=n + 3 {
            let prev_receipts: &[Receipt] = if i == 1 { &receipts } else { &[] };
            let apply_result = runtime
                .apply(
                    tries.get_trie_for_shard(0),
                    root,
                    &None,
                    &apply_state,
                    prev_receipts,
                    &[],
                    &epoch_info_provider,
                )
                .unwrap();
            let (store_update, new_root) = tries.apply_all(&apply_result.trie_changes, 0).unwrap();
            root = new_root;
            store_update.commit().unwrap();
            let state = tries.new_trie_update(0, root);
            let account = get_account(&state, &alice_account()).unwrap().unwrap();
            let capped_i = std::cmp::min(i, n);
            assert_eq!(
                account.amount,
                initial_balance
                    + small_transfer * Balance::from(capped_i)
                    + Balance::from(capped_i * (capped_i - 1) / 2)
            );
        }
    }

    #[test]
    fn test_apply_delayed_receipts_add_more_using_chunks() {
        let initial_balance = to_yocto(1_000_000);
        let initial_locked = to_yocto(500_000);
        let small_transfer = to_yocto(10_000);
        let (runtime, tries, mut root, mut apply_state, _, epoch_info_provider) =
            setup_runtime(initial_balance, initial_locked, 1);

        let receipt_gas_cost =
            runtime.config.transaction_costs.action_receipt_creation_config.exec_fee()
                + runtime.config.transaction_costs.action_creation_config.transfer_cost.exec_fee();
        apply_state.gas_limit = Some(receipt_gas_cost * 3);

        let n = 40;
        let receipts = generate_receipts(small_transfer, n);
        let mut receipt_chunks = receipts.chunks_exact(4);

        // Every time we'll process 3 receipts, so we need n / 3 rounded up. Then we do 3 extra.
        for i in 1..=n / 3 + 3 {
            let prev_receipts: &[Receipt] = receipt_chunks.next().unwrap_or_default();
            let apply_result = runtime
                .apply(
                    tries.get_trie_for_shard(0),
                    root,
                    &None,
                    &apply_state,
                    prev_receipts,
                    &[],
                    &epoch_info_provider,
                )
                .unwrap();
            let (store_update, new_root) = tries.apply_all(&apply_result.trie_changes, 0).unwrap();
            root = new_root;
            store_update.commit().unwrap();
            let state = tries.new_trie_update(0, root);
            let account = get_account(&state, &alice_account()).unwrap().unwrap();
            let capped_i = std::cmp::min(i * 3, n);
            assert_eq!(
                account.amount,
                initial_balance
                    + small_transfer * Balance::from(capped_i)
                    + Balance::from(capped_i * (capped_i - 1) / 2)
            );
        }
    }

    #[test]
    fn test_apply_delayed_receipts_adjustable_gas_limit() {
        let initial_balance = to_yocto(1_000_000);
        let initial_locked = to_yocto(500_000);
        let small_transfer = to_yocto(10_000);
        let (runtime, tries, mut root, mut apply_state, _, epoch_info_provider) =
            setup_runtime(initial_balance, initial_locked, 1);

        let receipt_gas_cost =
            runtime.config.transaction_costs.action_receipt_creation_config.exec_fee()
                + runtime.config.transaction_costs.action_creation_config.transfer_cost.exec_fee();

        let n = 120;
        let receipts = generate_receipts(small_transfer, n);
        let mut receipt_chunks = receipts.chunks_exact(4);

        let mut num_receipts_given = 0;
        let mut num_receipts_processed = 0;
        let mut num_receipts_per_block = 1;
        // Test adjusts gas limit based on the number of receipt given and number of receipts processed.
        while num_receipts_processed < n {
            if num_receipts_given > num_receipts_processed {
                num_receipts_per_block += 1;
            } else if num_receipts_per_block > 1 {
                num_receipts_per_block -= 1;
            }
            apply_state.gas_limit = Some(num_receipts_per_block * receipt_gas_cost);
            let prev_receipts: &[Receipt] = receipt_chunks.next().unwrap_or_default();
            num_receipts_given += prev_receipts.len() as u64;
            let apply_result = runtime
                .apply(
                    tries.get_trie_for_shard(0),
                    root,
                    &None,
                    &apply_state,
                    prev_receipts,
                    &[],
                    &epoch_info_provider,
                )
                .unwrap();
            let (store_update, new_root) = tries.apply_all(&apply_result.trie_changes, 0).unwrap();
            root = new_root;
            store_update.commit().unwrap();
            let state = tries.new_trie_update(0, root);
            num_receipts_processed += apply_result.outcomes.len() as u64;
            let account = get_account(&state, &alice_account()).unwrap().unwrap();
            assert_eq!(
                account.amount,
                initial_balance
                    + small_transfer * Balance::from(num_receipts_processed)
                    + Balance::from(num_receipts_processed * (num_receipts_processed - 1) / 2)
            );
            println!(
                "{} processed out of {} given. With limit {} receipts per block",
                num_receipts_processed, num_receipts_given, num_receipts_per_block
            );
        }
    }

    fn generate_receipts(small_transfer: u128, n: u64) -> Vec<Receipt> {
        (0..n)
            .map(|i| Receipt {
                predecessor_id: bob_account(),
                receiver_id: alice_account(),
                receipt_id: create_nonce_with_nonce(&CryptoHash::default(), i),
                receipt: ReceiptEnum::Action(ActionReceipt {
                    signer_id: bob_account(),
                    signer_public_key: PublicKey::empty(KeyType::ED25519),
                    gas_price: GAS_PRICE,
                    output_data_receivers: vec![],
                    input_data_ids: vec![],
                    actions: vec![Action::Transfer(TransferAction {
                        deposit: small_transfer + Balance::from(i),
                    })],
                }),
            })
            .collect()
    }

    #[test]
    fn test_apply_delayed_receipts_local_tx() {
        let initial_balance = to_yocto(1_000_000);
        let initial_locked = to_yocto(500_000);
        let small_transfer = to_yocto(10_000);
        let (mut runtime, tries, root, mut apply_state, signer, epoch_info_provider) =
            setup_runtime(initial_balance, initial_locked, 1);

        let receipt_exec_gas_fee = 1000;
        runtime.config = RuntimeConfig::free();
        runtime.config.transaction_costs.action_receipt_creation_config.execution =
            receipt_exec_gas_fee;
        // This allows us to execute 3 receipts per apply.
        apply_state.gas_limit = Some(receipt_exec_gas_fee * 3);

        let num_receipts = 6;
        let receipts = generate_receipts(small_transfer, num_receipts);

        let num_transactions = 9;
        let local_transactions = (0..num_transactions)
            .map(|i| {
                SignedTransaction::send_money(
                    i + 1,
                    alice_account(),
                    alice_account(),
                    &*signer,
                    small_transfer,
                    CryptoHash::default(),
                )
            })
            .collect::<Vec<_>>();

        // STEP #1. Pass 4 new local transactions + 2 receipts.
        // We can process only 3 local TX receipts TX#0, TX#1, TX#2.
        // TX#3 receipt and R#0, R#1 are delayed.
        // The new delayed queue is TX#3, R#0, R#1.
        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &receipts[0..2],
                &local_transactions[0..4],
                &epoch_info_provider,
            )
            .unwrap();
        let (store_update, root) = tries.apply_all(&apply_result.trie_changes, 0).unwrap();
        store_update.commit().unwrap();

        assert_eq!(
            apply_result.outcomes.iter().map(|o| o.id).collect::<Vec<_>>(),
            vec![
                local_transactions[0].get_hash(), // tx 0
                local_transactions[1].get_hash(), // tx 1
                local_transactions[2].get_hash(), // tx 2
                local_transactions[3].get_hash(), // tx 3 - the TX is processed, but the receipt is delayed
                create_nonce_with_nonce(&local_transactions[0].get_hash(), 0), // receipt for tx 0
                create_nonce_with_nonce(&local_transactions[1].get_hash(), 0), // receipt for tx 1
                create_nonce_with_nonce(&local_transactions[2].get_hash(), 0), // receipt for tx 2
            ],
            "STEP #1 failed",
        );

        // STEP #2. Pass 1 new local transaction (TX#4) + 1 receipts R#2.
        // We process 1 local receipts for TX#4, then delayed TX#3 receipt and then receipt R#0.
        // R#2 is added to delayed queue.
        // The new delayed queue is R#1, R#2
        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &receipts[2..3],
                &local_transactions[4..5],
                &epoch_info_provider,
            )
            .unwrap();
        let (store_update, root) = tries.apply_all(&apply_result.trie_changes, 0).unwrap();
        store_update.commit().unwrap();

        assert_eq!(
            apply_result.outcomes.iter().map(|o| o.id).collect::<Vec<_>>(),
            vec![
                local_transactions[4].get_hash(), // tx 4
                create_nonce_with_nonce(&local_transactions[4].get_hash(), 0), // receipt for tx 4
                create_nonce_with_nonce(&local_transactions[3].get_hash(), 0), // receipt for tx 3
                receipts[0].receipt_id,           // receipt #0
            ],
            "STEP #2 failed",
        );

        // STEP #3. Pass 4 new local transaction (TX#5, TX#6, TX#7, TX#8) and 1 new receipt R#3.
        // We process 3 local receipts for TX#5, TX#6, TX#7.
        // TX#8 and R#3 are added to delayed queue.
        // The new delayed queue is R#1, R#2, TX#8, R#3
        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &receipts[3..4],
                &local_transactions[5..9],
                &epoch_info_provider,
            )
            .unwrap();
        let (store_update, root) = tries.apply_all(&apply_result.trie_changes, 0).unwrap();
        store_update.commit().unwrap();

        assert_eq!(
            apply_result.outcomes.iter().map(|o| o.id).collect::<Vec<_>>(),
            vec![
                local_transactions[5].get_hash(), // tx 5
                local_transactions[6].get_hash(), // tx 6
                local_transactions[7].get_hash(), // tx 7
                local_transactions[8].get_hash(), // tx 8
                create_nonce_with_nonce(&local_transactions[5].get_hash(), 0), // receipt for tx 5
                create_nonce_with_nonce(&local_transactions[6].get_hash(), 0), // receipt for tx 6
                create_nonce_with_nonce(&local_transactions[7].get_hash(), 0), // receipt for tx 7
            ],
            "STEP #3 failed",
        );

        // STEP #4. Pass no new TXs and 1 receipt R#4.
        // We process R#1, R#2, TX#8.
        // R#4 is added to delayed queue.
        // The new delayed queue is R#3, R#4
        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &receipts[4..5],
                &[],
                &epoch_info_provider,
            )
            .unwrap();
        let (store_update, root) = tries.apply_all(&apply_result.trie_changes, 0).unwrap();
        store_update.commit().unwrap();

        assert_eq!(
            apply_result.outcomes.iter().map(|o| o.id).collect::<Vec<_>>(),
            vec![
                receipts[1].receipt_id,                                        // receipt #1
                receipts[2].receipt_id,                                        // receipt #2
                create_nonce_with_nonce(&local_transactions[8].get_hash(), 0), // receipt for tx 8
            ],
            "STEP #4 failed",
        );

        // STEP #5. Pass no new TXs and 1 receipt R#5.
        // We process R#3, R#4, R#5.
        // The new delayed queue is empty.
        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &receipts[5..6],
                &[],
                &epoch_info_provider,
            )
            .unwrap();

        assert_eq!(
            apply_result.outcomes.iter().map(|o| o.id).collect::<Vec<_>>(),
            vec![
                receipts[3].receipt_id, // receipt #3
                receipts[4].receipt_id, // receipt #4
                receipts[5].receipt_id, // receipt #5
            ],
            "STEP #5 failed",
        );
    }

    #[test]
    fn test_apply_invalid_incoming_receipts() {
        let initial_balance = to_yocto(1_000_000);
        let initial_locked = to_yocto(500_000);
        let small_transfer = to_yocto(10_000);
        let gas_limit = 1;
        let (runtime, tries, root, apply_state, _, epoch_info_provider) =
            setup_runtime(initial_balance, initial_locked, gas_limit);

        let n = 1;
        let mut receipts = generate_receipts(small_transfer, n);
        let invalid_account_id = "Invalid".to_string();
        receipts.get_mut(0).unwrap().predecessor_id = invalid_account_id.clone();

        let err = runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &receipts,
                &[],
                &epoch_info_provider,
            )
            .err()
            .unwrap();
        assert_eq!(
            err,
            RuntimeError::ReceiptValidationError(ReceiptValidationError::InvalidPredecessorId {
                account_id: invalid_account_id
            })
        )
    }

    #[test]
    fn test_apply_invalid_delayed_receipts() {
        let initial_balance = to_yocto(1_000_000);
        let initial_locked = to_yocto(500_000);
        let small_transfer = to_yocto(10_000);
        let gas_limit = 1;
        let (runtime, tries, root, apply_state, _, epoch_info_provider) =
            setup_runtime(initial_balance, initial_locked, gas_limit);

        let n = 1;
        let mut invalid_receipt = generate_receipts(small_transfer, n).pop().unwrap();
        let invalid_account_id = "Invalid".to_string();
        invalid_receipt.predecessor_id = invalid_account_id.clone();

        // Saving invalid receipt to the delayed receipts.
        let mut state_update = tries.new_trie_update(0, root);
        let mut delayed_receipts_indices = DelayedReceiptIndices::default();
        Runtime::delay_receipt(&mut state_update, &mut delayed_receipts_indices, &invalid_receipt)
            .unwrap();
        set(&mut state_update, TrieKey::DelayedReceiptIndices, &delayed_receipts_indices);
        state_update.commit(StateChangeCause::UpdatedDelayedReceipts);
        let trie_changes = state_update.finalize().unwrap().0;
        let (store_update, root) = tries.apply_all(&trie_changes, 0).unwrap();
        store_update.commit().unwrap();

        let err = runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &[],
                &[],
                &epoch_info_provider,
            )
            .err()
            .unwrap();
        assert_eq!(
            err,
            RuntimeError::StorageError(StorageError::StorageInconsistentState(format!(
                "Delayed receipt #0 in the state is invalid: {}",
                ReceiptValidationError::InvalidPredecessorId { account_id: invalid_account_id }
            )))
        )
    }

    #[test]
    fn test_apply_deficit_gas_for_transfer() {
        let initial_balance = to_yocto(1_000_000);
        let initial_locked = to_yocto(500_000);
        let small_transfer = to_yocto(10_000);
        let gas_limit = 10u64.pow(15);
        let (runtime, tries, root, apply_state, _, epoch_info_provider) =
            setup_runtime(initial_balance, initial_locked, gas_limit);

        let n = 1;
        let mut receipts = generate_receipts(small_transfer, n);
        if let ReceiptEnum::Action(action_receipt) = &mut receipts.get_mut(0).unwrap().receipt {
            action_receipt.gas_price = GAS_PRICE / 10;
        }

        let result = runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &receipts,
                &[],
                &epoch_info_provider,
            )
            .unwrap();
        assert_eq!(result.stats.gas_deficit_amount, result.stats.tx_burnt_amount * 9)
    }

    #[test]
    fn test_apply_deficit_gas_for_function_call_covered() {
        let initial_balance = to_yocto(1_000_000);
        let initial_locked = to_yocto(500_000);
        let gas_limit = 10u64.pow(15);
        let (runtime, tries, root, apply_state, _, epoch_info_provider) =
            setup_runtime(initial_balance, initial_locked, gas_limit);

        let gas = 2 * 10u64.pow(14);
        let gas_price = GAS_PRICE / 10;
        let actions = vec![Action::FunctionCall(FunctionCallAction {
            method_name: "hello".to_string(),
            args: b"world".to_vec(),
            gas,
            deposit: 0,
        })];

        let expected_gas_burnt = safe_add_gas(
            runtime.config.transaction_costs.action_receipt_creation_config.exec_fee(),
            total_exec_fees(&runtime.config.transaction_costs, &actions).unwrap(),
        )
        .unwrap();
        let receipts = vec![Receipt {
            predecessor_id: bob_account(),
            receiver_id: alice_account(),
            receipt_id: create_nonce_with_nonce(&CryptoHash::default(), 0),
            receipt: ReceiptEnum::Action(ActionReceipt {
                signer_id: bob_account(),
                signer_public_key: PublicKey::empty(KeyType::ED25519),
                gas_price,
                output_data_receivers: vec![],
                input_data_ids: vec![],
                actions,
            }),
        }];
        let total_receipt_cost = Balance::from(gas + expected_gas_burnt) * gas_price;
        let expected_gas_burnt_amount = Balance::from(expected_gas_burnt) * GAS_PRICE;
        let expected_refund = total_receipt_cost - expected_gas_burnt_amount;

        let result = runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &receipts,
                &[],
                &epoch_info_provider,
            )
            .unwrap();
        // We used part of the prepaid gas to paying extra fees.
        assert_eq!(result.stats.gas_deficit_amount, 0);
        // The refund is less than the received amount.
        match &result.outgoing_receipts[0].receipt {
            ReceiptEnum::Action(ActionReceipt { actions, .. }) => {
                assert!(
                    matches!(actions[0], Action::Transfer(TransferAction { deposit }) if deposit == expected_refund)
                );
            }
            _ => unreachable!(),
        };
    }

    #[test]
    fn test_apply_deficit_gas_for_function_call_partial() {
        let initial_balance = to_yocto(1_000_000);
        let initial_locked = to_yocto(500_000);
        let gas_limit = 10u64.pow(15);
        let (runtime, tries, root, apply_state, _, epoch_info_provider) =
            setup_runtime(initial_balance, initial_locked, gas_limit);

        let gas = 1_000_000;
        let gas_price = GAS_PRICE / 10;
        let actions = vec![Action::FunctionCall(FunctionCallAction {
            method_name: "hello".to_string(),
            args: b"world".to_vec(),
            gas,
            deposit: 0,
        })];

        let expected_gas_burnt = safe_add_gas(
            runtime.config.transaction_costs.action_receipt_creation_config.exec_fee(),
            total_exec_fees(&runtime.config.transaction_costs, &actions).unwrap(),
        )
        .unwrap();
        let receipts = vec![Receipt {
            predecessor_id: bob_account(),
            receiver_id: alice_account(),
            receipt_id: create_nonce_with_nonce(&CryptoHash::default(), 0),
            receipt: ReceiptEnum::Action(ActionReceipt {
                signer_id: bob_account(),
                signer_public_key: PublicKey::empty(KeyType::ED25519),
                gas_price,
                output_data_receivers: vec![],
                input_data_ids: vec![],
                actions,
            }),
        }];
        let total_receipt_cost = Balance::from(gas + expected_gas_burnt) * gas_price;
        let expected_gas_burnt_amount = Balance::from(expected_gas_burnt) * GAS_PRICE;
        let expected_deficit = expected_gas_burnt_amount - total_receipt_cost;

        let result = runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &receipts,
                &[],
                &epoch_info_provider,
            )
            .unwrap();
        // Used full prepaid gas, but it still not enough to cover deficit.
        assert_eq!(result.stats.gas_deficit_amount, expected_deficit);
        // Burnt all the fees + all prepaid gas.
        assert_eq!(result.stats.tx_burnt_amount, total_receipt_cost);
    }
}

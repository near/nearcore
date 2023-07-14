use crate::actions::*;
use crate::balance_checker::check_balance;
use crate::config::{
    exec_fee, safe_add_balance, safe_add_compute, safe_add_gas, safe_gas_to_balance, total_deposit,
    total_prepaid_exec_fees, total_prepaid_gas,
};
use crate::prefetch::TriePrefetcher;
use crate::verifier::{check_storage_stake, validate_receipt, StorageStakingError};
pub use crate::verifier::{
    validate_transaction, verify_and_charge_transaction, ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT,
};
use config::total_prepaid_send_fees;
pub use near_crypto;
pub use near_primitives;
use near_primitives::account::Account;
use near_primitives::checked_feature;
use near_primitives::contract::ContractCode;
use near_primitives::errors::{ActionError, ActionErrorKind, RuntimeError, TxExecutionError};
use near_primitives::hash::CryptoHash;
use near_primitives::profile::ProfileDataV3;
use near_primitives::receipt::{
    ActionReceipt, DataReceipt, DelayedReceiptIndices, Receipt, ReceiptEnum, ReceivedData,
};
pub use near_primitives::runtime::apply_state::ApplyState;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::runtime::migration_data::{MigrationData, MigrationFlags};
use near_primitives::sandbox::state_patch::SandboxStatePatch;
use near_primitives::state_record::StateRecord;
use near_primitives::transaction::ExecutionMetadata;
use near_primitives::transaction::{
    Action, ExecutionOutcome, ExecutionOutcomeWithId, ExecutionStatus, LogEntry, SignedTransaction,
};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{
    validator_stake::ValidatorStake, AccountId, Balance, Compute, EpochInfoProvider, Gas,
    RawStateChangesWithTrieKey, StateChangeCause, StateRoot,
};
use near_primitives::utils::{
    create_action_hash, create_receipt_id_from_receipt, create_receipt_id_from_transaction,
};
use near_primitives::version::{ProtocolFeature, ProtocolVersion};
use near_store::{
    get, get_account, get_postponed_receipt, get_received_data, remove_postponed_receipt, set,
    set_account, set_delayed_receipt, set_postponed_receipt, set_received_data, PartialStorage,
    StorageError, Trie, TrieChanges, TrieUpdate,
};
use near_store::{set_access_key, set_code};
use near_vm_runner::logic::types::PromiseResult;
use near_vm_runner::logic::{ActionCosts, ReturnData};
pub use near_vm_runner::with_ext_cost_counter;
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::debug;

mod actions;
pub mod adapter;
mod balance_checker;
pub mod config;
pub mod ext;
mod metrics;
mod prefetch;
pub mod state_viewer;
mod verifier;

const EXPECT_ACCOUNT_EXISTS: &str = "account exists, checked above";

/// Contains information to update validators accounts at the first block of a new epoch.
#[derive(Debug)]
pub struct ValidatorAccountsUpdate {
    /// Maximum stake across last 3 epochs.
    pub stake_info: HashMap<AccountId, Balance>,
    /// Rewards to distribute to validators.
    pub validator_rewards: HashMap<AccountId, Balance>,
    /// Stake proposals from the last chunk.
    pub last_proposals: HashMap<AccountId, Balance>,
    /// The ID of the protocol treasury account if it belongs to the current shard.
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

#[derive(Debug)]
pub struct ApplyResult {
    pub state_root: StateRoot,
    pub trie_changes: TrieChanges,
    pub validator_proposals: Vec<ValidatorStake>,
    pub outgoing_receipts: Vec<Receipt>,
    pub outcomes: Vec<ExecutionOutcomeWithId>,
    pub state_changes: Vec<RawStateChangesWithTrieKey>,
    pub stats: ApplyStats,
    pub processed_delayed_receipts: Vec<Receipt>,
    pub proof: Option<PartialStorage>,
    pub delayed_receipts_count: u64,
    pub metrics: Option<metrics::ApplyMetrics>,
}

#[derive(Debug)]
pub struct ActionResult {
    pub gas_burnt: Gas,
    pub gas_burnt_for_function_call: Gas,
    pub gas_used: Gas,
    pub compute_usage: Compute,
    pub result: Result<ReturnData, ActionError>,
    pub logs: Vec<LogEntry>,
    pub new_receipts: Vec<Receipt>,
    pub validator_proposals: Vec<ValidatorStake>,
    pub profile: ProfileDataV3,
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
        self.compute_usage = safe_add_compute(self.compute_usage, next_result.compute_usage)?;
        self.profile.merge(&next_result.profile);
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
            compute_usage: 0,
            result: Ok(ReturnData::None),
            logs: vec![],
            new_receipts: vec![],
            validator_proposals: vec![],
            profile: Default::default(),
        }
    }
}

pub struct Runtime {}

impl Runtime {
    pub fn new() -> Self {
        Self {}
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
        let _span = tracing::debug_span!(target: "runtime", "process_transaction", tx_hash = %signed_transaction.get_hash()).entered();
        metrics::TRANSACTION_PROCESSED_TOTAL.inc();

        match verify_and_charge_transaction(
            &apply_state.config,
            state_update,
            apply_state.gas_price,
            signed_transaction,
            true,
            Some(apply_state.block_height),
            apply_state.current_protocol_version,
        ) {
            Ok(verification_result) => {
                metrics::TRANSACTION_PROCESSED_SUCCESSFULLY_TOTAL.inc();
                state_update.commit(StateChangeCause::TransactionProcessing {
                    tx_hash: signed_transaction.get_hash(),
                });
                let transaction = &signed_transaction.transaction;
                let receipt_id = create_receipt_id_from_transaction(
                    apply_state.current_protocol_version,
                    signed_transaction,
                    &apply_state.prev_block_hash,
                    &apply_state.block_hash,
                );
                let receipt = Receipt {
                    predecessor_id: transaction.signer_id.clone(),
                    receiver_id: transaction.receiver_id.clone(),
                    receipt_id,
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
                        // TODO(#8806): Support compute costs for actions. For now they match burnt gas.
                        compute_usage: Some(verification_result.gas_burnt),
                        tokens_burnt: verification_result.burnt_amount,
                        executor_id: transaction.signer_id.clone(),
                        // TODO: profile data is only counted in apply_action, which only happened at process_receipt
                        // VerificationResult needs updates to incorporate profile data to support profile data of txns
                        metadata: ExecutionMetadata::V1,
                    },
                };
                Ok((receipt, outcome))
            }
            Err(e) => {
                metrics::TRANSACTION_PROCESSED_FAILED_TOTAL.inc();
                state_update.rollback();
                Err(e)
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
        action_index: usize,
        actions: &[Action],
        epoch_info_provider: &dyn EpochInfoProvider,
    ) -> Result<ActionResult, RuntimeError> {
        let exec_fees = exec_fee(
            &apply_state.config.fees,
            action,
            &receipt.receiver_id,
            apply_state.current_protocol_version,
        );
        let mut result = ActionResult::default();
        result.gas_used = exec_fees;
        result.gas_burnt = exec_fees;
        // TODO(#8806): Support compute costs for actions. For now they match burnt gas.
        result.compute_usage = exec_fees;
        let account_id = &receipt.receiver_id;
        let is_the_only_action = actions.len() == 1;
        let is_refund = AccountId::is_system(&receipt.predecessor_id);
        // Account validation
        if let Err(e) = check_account_existence(
            action,
            account,
            account_id,
            apply_state.current_protocol_version,
            is_the_only_action,
            is_refund,
        ) {
            result.result = Err(e);
            return Ok(result);
        }
        // Permission validation
        if let Err(e) = check_actor_permissions(action, account, actor_id, account_id) {
            result.result = Err(e);
            return Ok(result);
        }
        metrics::ACTION_CALLED_COUNT.with_label_values(&[action.as_ref()]).inc();
        match action {
            Action::CreateAccount(_) => {
                action_create_account(
                    &apply_state.config.fees,
                    &apply_state.config.account_creation_config,
                    account,
                    actor_id,
                    &receipt.receiver_id,
                    &receipt.predecessor_id,
                    &mut result,
                );
            }
            Action::DeployContract(deploy_contract) => {
                action_deploy_contract(
                    state_update,
                    account.as_mut().expect(EXPECT_ACCOUNT_EXISTS),
                    account_id,
                    deploy_contract,
                    apply_state,
                    apply_state.current_protocol_version,
                )?;
            }
            Action::FunctionCall(function_call) => {
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
                    &apply_state.config,
                    action_index + 1 == actions.len(),
                    epoch_info_provider,
                )?;
            }
            Action::Transfer(transfer) => {
                if let Some(account) = account.as_mut() {
                    action_transfer(account, transfer)?;
                    // Check if this is a gas refund, then try to refund the access key allowance.
                    if is_refund && action_receipt.signer_id == receipt.receiver_id {
                        try_refund_allowance(
                            state_update,
                            &receipt.receiver_id,
                            &action_receipt.signer_public_key,
                            transfer,
                        )?;
                    }
                } else {
                    // Implicit account creation
                    debug_assert!(checked_feature!(
                        "stable",
                        ImplicitAccountCreation,
                        apply_state.current_protocol_version
                    ));
                    debug_assert!(!is_refund);
                    action_implicit_account_creation_transfer(
                        state_update,
                        &apply_state.config.fees,
                        account,
                        actor_id,
                        &receipt.receiver_id,
                        transfer,
                        apply_state.block_height,
                        apply_state.current_protocol_version,
                    );
                }
            }
            Action::Stake(stake) => {
                action_stake(
                    account.as_mut().expect(EXPECT_ACCOUNT_EXISTS),
                    &mut result,
                    account_id,
                    stake,
                    &apply_state.prev_block_hash,
                    epoch_info_provider,
                )?;
            }
            Action::AddKey(add_key) => {
                action_add_key(
                    apply_state,
                    state_update,
                    account.as_mut().expect(EXPECT_ACCOUNT_EXISTS),
                    &mut result,
                    account_id,
                    add_key,
                )?;
            }
            Action::DeleteKey(delete_key) => {
                action_delete_key(
                    &apply_state.config.fees,
                    state_update,
                    account.as_mut().expect(EXPECT_ACCOUNT_EXISTS),
                    &mut result,
                    account_id,
                    delete_key,
                    apply_state.current_protocol_version,
                )?;
            }
            Action::DeleteAccount(delete_account) => {
                action_delete_account(
                    state_update,
                    account,
                    actor_id,
                    receipt,
                    &mut result,
                    account_id,
                    delete_account,
                    apply_state.current_protocol_version,
                )?;
            }
            Action::Delegate(signed_delegate_action) => {
                apply_delegate_action(
                    state_update,
                    apply_state,
                    action_receipt,
                    account_id,
                    signed_delegate_action,
                    &mut result,
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
        let action_receipt = match &receipt.receipt {
            ReceiptEnum::Action(action_receipt) => action_receipt,
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
        let exec_fees = apply_state.config.fees.fee(ActionCosts::new_action_receipt).exec_fee();
        result.gas_used = exec_fees;
        result.gas_burnt = exec_fees;
        // TODO(#8806): Support compute costs for actions. For now they match burnt gas.
        result.compute_usage = exec_fees;
        // Executing actions one by one
        for (action_index, action) in action_receipt.actions.iter().enumerate() {
            let action_hash = create_action_hash(
                apply_state.current_protocol_version,
                receipt,
                &apply_state.prev_block_hash,
                &apply_state.block_hash,
                action_index,
            );
            let mut new_result = self.apply_action(
                action,
                state_update,
                apply_state,
                &mut account,
                &mut actor_id,
                receipt,
                action_receipt,
                &promise_results,
                &action_hash,
                action_index,
                &action_receipt.actions,
                epoch_info_provider,
            )?;
            if new_result.result.is_ok() {
                if let Err(e) = new_result.new_receipts.iter().try_for_each(|receipt| {
                    validate_receipt(
                        &apply_state.config.wasm_config.limit_config,
                        receipt,
                        apply_state.current_protocol_version,
                    )
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
                match check_storage_stake(
                    account,
                    &apply_state.config,
                    apply_state.current_protocol_version,
                ) {
                    Ok(()) => {
                        set_account(state_update, account_id.clone(), account);
                    }
                    Err(StorageStakingError::LackBalanceForStorageStaking(amount)) => {
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
                    }
                    Err(StorageStakingError::StorageError(err)) => {
                        return Err(RuntimeError::StorageError(
                            StorageError::StorageInconsistentState(err),
                        ))
                    }
                }
            }
        }

        let gas_deficit_amount = if AccountId::is_system(&receipt.predecessor_id) {
            // We will set gas_burnt for refund receipts to be 0 when we calculate tx_burnt_amount
            // Here we don't set result.gas_burnt to be zero if CountRefundReceiptsInGasLimit is
            // enabled because we want it to be counted in gas limit calculation later
            if !checked_feature!(
                "stable",
                CountRefundReceiptsInGasLimit,
                apply_state.current_protocol_version
            ) {
                result.gas_burnt = 0;
                result.compute_usage = 0;
                result.gas_used = 0;
            }

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
                apply_state.current_protocol_version,
                &apply_state.config.fees,
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

        // If the receipt is a refund, then we consider it free without burnt gas.
        let gas_burnt: Gas =
            if AccountId::is_system(&receipt.predecessor_id) { 0 } else { result.gas_burnt };
        // `gas_deficit_amount` is strictly less than `gas_price * gas_burnt`.
        let mut tx_burnt_amount =
            safe_gas_to_balance(apply_state.gas_price, gas_burnt)? - gas_deficit_amount;
        // The amount of tokens burnt for the execution of this receipt. It's used in the execution
        // outcome.
        let tokens_burnt = tx_burnt_amount;

        // Adding burnt gas reward for function calls if the account exists.
        let receiver_gas_reward = result.gas_burnt_for_function_call
            * *apply_state.config.fees.burnt_gas_reward.numer() as u64
            / *apply_state.config.fees.burnt_gas_reward.denom() as u64;
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
                account.set_amount(safe_add_balance(account.amount(), receiver_reward)?);
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
                let receipt_id = create_receipt_id_from_receipt(
                    apply_state.current_protocol_version,
                    receipt,
                    &apply_state.prev_block_hash,
                    &apply_state.block_hash,
                    receipt_index,
                );

                new_receipt.receipt_id = receipt_id;
                let is_action = matches!(&new_receipt.receipt, ReceiptEnum::Action(_));
                outgoing_receipts.push(new_receipt);
                if is_action {
                    Some(receipt_id)
                } else {
                    None
                }
            })
            .collect();

        let status = match result.result {
            Ok(ReturnData::ReceiptIndex(receipt_index)) => {
                ExecutionStatus::SuccessReceiptId(create_receipt_id_from_receipt(
                    apply_state.current_protocol_version,
                    receipt,
                    &apply_state.prev_block_hash,
                    &apply_state.block_hash,
                    receipt_index as usize,
                ))
            }
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
                compute_usage: Some(result.compute_usage),
                tokens_burnt,
                executor_id: account_id.clone(),
                metadata: ExecutionMetadata::V3(result.profile),
            },
        })
    }

    fn generate_refund_receipts(
        &self,
        current_gas_price: Balance,
        receipt: &Receipt,
        action_receipt: &ActionReceipt,
        result: &mut ActionResult,
        current_protocol_version: ProtocolVersion,
        transaction_costs: &RuntimeFeesConfig,
    ) -> Result<Balance, RuntimeError> {
        let total_deposit = total_deposit(&action_receipt.actions)?;
        let prepaid_gas = safe_add_gas(
            total_prepaid_gas(&action_receipt.actions)?,
            total_prepaid_send_fees(
                transaction_costs,
                &action_receipt.actions,
                current_protocol_version,
            )?,
        )?;
        let prepaid_exec_gas = safe_add_gas(
            total_prepaid_exec_fees(
                transaction_costs,
                &action_receipt.actions,
                &receipt.receiver_id,
                current_protocol_version,
            )?,
            transaction_costs.fee(ActionCosts::new_action_receipt).exec_fee(),
        )?;
        let deposit_refund = if result.result.is_err() { total_deposit } else { 0 };
        let gas_refund = if result.result.is_err() {
            safe_add_gas(prepaid_gas, prepaid_exec_gas)? - result.gas_burnt
        } else {
            safe_add_gas(prepaid_gas, prepaid_exec_gas)? - result.gas_used
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
                    set_postponed_receipt(state_update, receipt);
                }
            }
        };
        // We didn't trigger execution, so we need to commit the state.
        state_update
            .commit(StateChangeCause::PostponedReceipt { receipt_hash: receipt.get_hash() });
        Ok(None)
    }

    /// Iterates over the validators in the current shard and updates their accounts to return stake
    /// and allocate rewards. Also updates protocol treasury account if it belongs to the current
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
                    debug!(target: "runtime", "account {} adding reward {} to stake {}", account_id, reward, account.locked());
                    account.set_locked(
                        account
                            .locked()
                            .checked_add(*reward)
                            .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?,
                    );
                }

                debug!(target: "runtime",
                       "account {} stake {} max_of_stakes: {}",
                       account_id, account.locked(), max_of_stakes
                );
                if account.locked() < *max_of_stakes {
                    return Err(StorageError::StorageInconsistentState(format!(
                        "FATAL: staking invariant does not hold. \
                         Account stake {} is less than maximum of stakes {} in the past three epochs",
                        account.locked(),
                        max_of_stakes)).into());
                }
                let last_proposal =
                    *validator_accounts_update.last_proposals.get(account_id).unwrap_or(&0);
                let return_stake = account
                    .locked()
                    .checked_sub(max(*max_of_stakes, last_proposal))
                    .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?;
                debug!(target: "runtime", "account {} return stake {}", account_id, return_stake);
                account.set_locked(
                    account
                        .locked()
                        .checked_sub(return_stake)
                        .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?,
                );
                account.set_amount(
                    account
                        .amount()
                        .checked_add(return_stake)
                        .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?,
                );

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
            if let Some(mut account) = get_account(state_update, account_id)? {
                let amount_to_slash = stake.unwrap_or(account.locked());
                debug!(target: "runtime", "slashing {} of {} from {}", amount_to_slash, account.locked(), account_id);
                if account.locked() < amount_to_slash {
                    return Err(StorageError::StorageInconsistentState(format!(
                        "FATAL: staking invariant does not hold. Account locked {} is less than slashed {}",
                        account.locked(), amount_to_slash)).into());
                }
                stats.slashed_burnt_amount = stats
                    .slashed_burnt_amount
                    .checked_add(amount_to_slash)
                    .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?;
                account.set_locked(
                    account
                        .locked()
                        .checked_sub(amount_to_slash)
                        .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?,
                );
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
                account.set_amount(
                    account
                        .amount()
                        .checked_add(treasury_reward)
                        .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?,
                );
                set_account(state_update, account_id.clone(), &account);
            }
        }
        state_update.commit(StateChangeCause::ValidatorAccountsUpdate);

        Ok(())
    }

    pub fn apply_migrations(
        &self,
        state_update: &mut TrieUpdate,
        migration_data: &Arc<MigrationData>,
        migration_flags: &MigrationFlags,
        protocol_version: ProtocolVersion,
    ) -> Result<(Gas, Vec<Receipt>), StorageError> {
        let mut gas_used: Gas = 0;
        if ProtocolFeature::FixStorageUsage.protocol_version() == protocol_version
            && migration_flags.is_first_block_of_version
        {
            for (account_id, delta) in &migration_data.storage_usage_delta {
                // Account could have been deleted in the meantime, so we check if it is still Some
                if let Some(mut account) = get_account(state_update, account_id)? {
                    // Storage usage is saved in state, hence it is nowhere close to max value
                    // of u64, and maximal delta is 4196, se we can add here without checking
                    // for overflow
                    account.set_storage_usage(account.storage_usage() + delta);
                    set_account(state_update, account_id.clone(), &account);
                }
            }
            gas_used += migration_data.storage_usage_fix_gas;
            state_update.commit(StateChangeCause::Migration);
        }

        // Re-introduce receipts lost because of a bug in apply_chunks.
        // We take the first block with existing chunk in the first epoch in which protocol feature
        // RestoreReceiptsAfterFixApplyChunks was enabled, and put the restored receipts there.
        // See https://github.com/near/nearcore/pull/4248/ for more details.
        let receipts_to_restore = if ProtocolFeature::RestoreReceiptsAfterFixApplyChunks
            .protocol_version()
            == protocol_version
            && migration_flags.is_first_block_with_chunk_of_version
        {
            // Note that receipts are restored only on mainnet so restored_receipts will be empty on
            // other chains.
            migration_data.restored_receipts.get(&0u64).cloned().unwrap_or_default()
        } else {
            vec![]
        };

        Ok((gas_used, receipts_to_restore))
    }

    /// Applies new signed transactions and incoming receipts for some chunk/shard on top of
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
        validator_accounts_update: &Option<ValidatorAccountsUpdate>,
        apply_state: &ApplyState,
        incoming_receipts: &[Receipt],
        transactions: &[SignedTransaction],
        epoch_info_provider: &dyn EpochInfoProvider,
        state_patch: SandboxStatePatch,
    ) -> Result<ApplyResult, RuntimeError> {
        // state_patch must be empty unless this is sandbox build.  Thanks to
        // conditional compilation this always resolves to true so technically
        // the check is not necessary.  It’s defence in depth to make sure any
        // future refactoring won’t break the condition.
        assert!(cfg!(feature = "sandbox") || state_patch.is_empty());

        let _span = tracing::debug_span!(
            target: "runtime",
            "apply",
            num_transactions = transactions.len())
        .entered();

        let mut prefetcher = TriePrefetcher::new_if_enabled(&trie);
        let mut state_update = TrieUpdate::new(trie);

        if let Some(prefetcher) = &mut prefetcher {
            // Prefetcher is allowed to fail
            _ = prefetcher.prefetch_transactions_data(transactions);
        }

        let mut stats = ApplyStats::default();

        if let Some(validator_accounts_update) = validator_accounts_update {
            self.update_validator_accounts(
                &mut state_update,
                validator_accounts_update,
                &mut stats,
            )?;
        }

        let (gas_used_for_migrations, mut receipts_to_restore) = self
            .apply_migrations(
                &mut state_update,
                &apply_state.migration_data,
                &apply_state.migration_flags,
                apply_state.current_protocol_version,
            )
            .map_err(RuntimeError::StorageError)?;
        // If we have receipts that need to be restored, prepend them to the list of incoming receipts
        let incoming_receipts = if receipts_to_restore.is_empty() {
            incoming_receipts
        } else {
            receipts_to_restore.extend_from_slice(incoming_receipts);
            receipts_to_restore.as_slice()
        };

        let mut delayed_receipts_indices: DelayedReceiptIndices =
            get(&state_update, &TrieKey::DelayedReceiptIndices)?.unwrap_or_default();
        let initial_delayed_receipt_indices = delayed_receipts_indices.clone();

        if !apply_state.is_new_chunk
            && apply_state.current_protocol_version
                >= ProtocolFeature::FixApplyChunks.protocol_version()
        {
            let (trie, trie_changes, state_changes) = state_update.finalize()?;
            let proof = trie.recorded_storage();
            return Ok(ApplyResult {
                state_root: trie_changes.new_root,
                trie_changes,
                validator_proposals: vec![],
                outgoing_receipts: vec![],
                outcomes: vec![],
                state_changes,
                stats,
                processed_delayed_receipts: vec![],
                proof,
                delayed_receipts_count: delayed_receipts_indices.len(),
                metrics: None,
            });
        }

        let mut outgoing_receipts = Vec::new();
        let mut validator_proposals = vec![];
        let mut local_receipts = vec![];
        let mut outcomes = vec![];
        let mut processed_delayed_receipts = vec![];
        // This contains the gas "burnt" for refund receipts. Even though we don't actually
        // charge any gas for refund receipts, we still count the gas use towards the block gas
        // limit
        let mut total_gas_burnt = gas_used_for_migrations;
        let mut total_compute_usage = total_gas_burnt;
        let mut metrics = metrics::ApplyMetrics::default();

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

            total_gas_burnt = safe_add_gas(total_gas_burnt, outcome_with_id.outcome.gas_burnt)?;
            total_compute_usage = safe_add_compute(
                total_compute_usage,
                outcome_with_id
                    .outcome
                    .compute_usage
                    .expect("`process_transaction` must populate compute usage"),
            )?;

            if !checked_feature!("stable", ComputeCosts, apply_state.current_protocol_version) {
                assert_eq!(
                    total_compute_usage, total_gas_burnt,
                    "Compute usage must match burnt gas"
                );
            }

            outcomes.push(outcome_with_id);
        }
        metrics.tx_processing_done(total_gas_burnt, total_compute_usage);

        let mut process_receipt = |receipt: &Receipt,
                                   state_update: &mut TrieUpdate,
                                   total_gas_burnt: &mut Gas,
                                   total_compute_usage: &mut Compute|
         -> Result<_, RuntimeError> {
            let _span = tracing::debug_span!(
                target: "runtime",
                "process_receipt",
                receipt_id = %receipt.receipt_id,
                predecessor = %receipt.predecessor_id,
                receiver = %receipt.receiver_id,
                id = %receipt.receipt_id,
            )
            .entered();
            let node_counter_before = state_update.trie().get_trie_nodes_count();
            let result = self.process_receipt(
                state_update,
                apply_state,
                receipt,
                &mut outgoing_receipts,
                &mut validator_proposals,
                &mut stats,
                epoch_info_provider,
            );
            let node_counter_after = state_update.trie().get_trie_nodes_count();
            tracing::trace!(target: "runtime", ?node_counter_before, ?node_counter_after);

            if let Some(outcome_with_id) = result? {
                *total_gas_burnt =
                    safe_add_gas(*total_gas_burnt, outcome_with_id.outcome.gas_burnt)?;
                *total_compute_usage = safe_add_compute(
                    *total_compute_usage,
                    outcome_with_id
                        .outcome
                        .compute_usage
                        .expect("`process_receipt` must populate compute usage"),
                )?;

                if !checked_feature!("stable", ComputeCosts, apply_state.current_protocol_version) {
                    assert_eq!(
                        total_compute_usage, total_gas_burnt,
                        "Compute usage must match burnt gas"
                    );
                }
                outcomes.push(outcome_with_id);
            }
            Ok(())
        };

        // TODO(#8859): Introduce a dedicated `compute_limit` for the chunk.
        // For now compute limit always matches the gas limit.
        let compute_limit = apply_state.gas_limit.unwrap_or(Gas::max_value());

        // We first process local receipts. They contain staking, local contract calls, etc.
        if let Some(prefetcher) = &mut prefetcher {
            prefetcher.clear();
            // Prefetcher is allowed to fail
            _ = prefetcher.prefetch_receipts_data(&local_receipts);
        }
        for receipt in local_receipts.iter() {
            if total_compute_usage < compute_limit {
                // NOTE: We don't need to validate the local receipt, because it's just validated in
                // the `verify_and_charge_transaction`.
                process_receipt(
                    receipt,
                    &mut state_update,
                    &mut total_gas_burnt,
                    &mut total_compute_usage,
                )?;
            } else {
                set_delayed_receipt(&mut state_update, &mut delayed_receipts_indices, receipt);
            }
        }
        metrics.local_receipts_done(total_gas_burnt, total_compute_usage);

        // Then we process the delayed receipts. It's a backlog of receipts from the past blocks.
        while delayed_receipts_indices.first_index < delayed_receipts_indices.next_available_index {
            if total_compute_usage >= compute_limit {
                break;
            }
            let key = TrieKey::DelayedReceipt { index: delayed_receipts_indices.first_index };
            let receipt: Receipt = get(&state_update, &key)?.ok_or_else(|| {
                StorageError::StorageInconsistentState(format!(
                    "Delayed receipt #{} should be in the state",
                    delayed_receipts_indices.first_index
                ))
            })?;

            if let Some(prefetcher) = &mut prefetcher {
                prefetcher.clear();
                // Prefetcher is allowed to fail
                _ = prefetcher.prefetch_receipts_data(std::slice::from_ref(&receipt));
            }

            // Validating the delayed receipt. If it fails, it's likely the state is inconsistent.
            validate_receipt(
                &apply_state.config.wasm_config.limit_config,
                &receipt,
                apply_state.current_protocol_version,
            )
            .map_err(|e| {
                StorageError::StorageInconsistentState(format!(
                    "Delayed receipt #{} in the state is invalid: {}",
                    delayed_receipts_indices.first_index, e
                ))
            })?;

            state_update.remove(key);
            // Math checked above: first_index is less than next_available_index
            delayed_receipts_indices.first_index += 1;
            process_receipt(
                &receipt,
                &mut state_update,
                &mut total_gas_burnt,
                &mut total_compute_usage,
            )?;
            processed_delayed_receipts.push(receipt);
        }
        metrics.delayed_receipts_done(total_gas_burnt, total_compute_usage);

        // And then we process the new incoming receipts. These are receipts from other shards.
        if let Some(prefetcher) = &mut prefetcher {
            prefetcher.clear();
            // Prefetcher is allowed to fail
            _ = prefetcher.prefetch_receipts_data(&incoming_receipts);
        }
        for receipt in incoming_receipts.iter() {
            // Validating new incoming no matter whether we have available gas or not. We don't
            // want to store invalid receipts in state as delayed.
            validate_receipt(
                &apply_state.config.wasm_config.limit_config,
                receipt,
                apply_state.current_protocol_version,
            )
            .map_err(RuntimeError::ReceiptValidationError)?;
            if total_compute_usage < compute_limit {
                process_receipt(
                    receipt,
                    &mut state_update,
                    &mut total_gas_burnt,
                    &mut total_compute_usage,
                )?;
            } else {
                set_delayed_receipt(&mut state_update, &mut delayed_receipts_indices, receipt);
            }
        }
        metrics.incoming_receipts_done(total_gas_burnt, total_compute_usage);

        // No more receipts are executed on this trie, stop any pending prefetches on it.
        if let Some(prefetcher) = &prefetcher {
            prefetcher.clear();
        }

        if delayed_receipts_indices != initial_delayed_receipt_indices {
            set(&mut state_update, TrieKey::DelayedReceiptIndices, &delayed_receipts_indices);
        }

        check_balance(
            &apply_state.config.fees,
            &state_update,
            validator_accounts_update,
            incoming_receipts,
            transactions,
            &outgoing_receipts,
            &stats,
            apply_state.current_protocol_version,
        )?;

        state_update.commit(StateChangeCause::UpdatedDelayedReceipts);
        self.apply_state_patch(&mut state_update, state_patch);
        let (trie, trie_changes, state_changes) = state_update.finalize()?;

        // Dedup proposals from the same account.
        // The order is deterministically changed.
        let mut unique_proposals = vec![];
        let mut account_ids = HashSet::new();
        for proposal in validator_proposals.into_iter().rev() {
            let account_id = proposal.account_id();
            if !account_ids.contains(account_id) {
                account_ids.insert(account_id.clone());
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
            processed_delayed_receipts,
            proof,
            delayed_receipts_count: delayed_receipts_indices.len(),
            metrics: Some(metrics),
        })
    }

    fn apply_state_patch(&self, state_update: &mut TrieUpdate, state_patch: SandboxStatePatch) {
        if state_patch.is_empty() {
            return;
        }
        for record in state_patch {
            match record {
                StateRecord::Account { account_id, account } => {
                    set_account(state_update, account_id, &account);
                }
                StateRecord::Data { account_id, data_key, value } => {
                    state_update.set(TrieKey::ContractData { key: data_key.into(), account_id }, value.into());
                }
                StateRecord::Contract { account_id, code } => {
                    let acc = get_account(state_update, &account_id).expect("Failed to read state").expect("Code state record should be preceded by the corresponding account record");
                    // Recompute contract code hash.
                    let code = ContractCode::new(code, None);
                    set_code(state_update, account_id, &code);
                    assert_eq!(*code.hash(), acc.code_hash());
                }
                StateRecord::AccessKey { account_id, public_key, access_key } => {
                    set_access_key(state_update, account_id, public_key, &access_key);
                }
                _ => unimplemented!("patch_state can only patch Account, AccessKey, Contract and Data kind of StateRecord")
            }
        }
        state_update.commit(StateChangeCause::Migration);
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use near_crypto::{InMemorySigner, KeyType, PublicKey, Signer};
    use near_primitives::account::AccessKey;
    use near_primitives::hash::hash;
    use near_primitives::runtime::config::RuntimeConfig;
    use near_primitives::shard_layout::ShardUId;
    use near_primitives::test_utils::{account_new, MockEpochInfoProvider};
    use near_primitives::transaction::{
        AddKeyAction, DeleteKeyAction, DeployContractAction, FunctionCallAction, TransferAction,
    };
    use near_primitives::types::MerkleHash;
    use near_primitives::version::PROTOCOL_VERSION;
    use near_store::test_utils::create_tries;
    use near_store::{set_access_key, ShardTries, StoreCompiledContractCache};
    use near_vm_runner::logic::{ExtCosts, ParameterCost};
    use testlib::runtime_utils::{alice_account, bob_account};

    use super::*;

    const GAS_PRICE: Balance = 5000;

    fn to_yocto(near: Balance) -> Balance {
        near * 10u128.pow(24)
    }

    fn create_receipt_with_actions(
        account_id: AccountId,
        signer: Arc<InMemorySigner>,
        actions: Vec<Action>,
    ) -> Receipt {
        Receipt {
            predecessor_id: account_id.clone(),
            receiver_id: account_id.clone(),
            receipt_id: CryptoHash::hash_borsh(actions.clone()),
            receipt: ReceiptEnum::Action(ActionReceipt {
                signer_id: account_id,
                signer_public_key: signer.public_key(),
                gas_price: GAS_PRICE,
                output_data_receivers: vec![],
                input_data_ids: vec![],
                actions,
            }),
        }
    }

    #[test]
    fn test_get_and_set_accounts() {
        let tries = create_tries();
        let mut state_update =
            tries.new_trie_update(ShardUId::single_shard(), MerkleHash::default());
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
        let mut state_update = tries.new_trie_update(ShardUId::single_shard(), root);
        let test_account = account_new(to_yocto(10), hash(&[]));
        let account_id = bob_account();
        set_account(&mut state_update, account_id.clone(), &test_account);
        state_update.commit(StateChangeCause::InitialState);
        let trie_changes = state_update.finalize().unwrap().1;
        let mut store_update = tries.store_update();
        let new_root = tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
        store_update.commit().unwrap();
        let new_state_update = tries.new_trie_update(ShardUId::single_shard(), new_root);
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
        let runtime = Runtime::new();
        let account_id = alice_account();
        let signer = Arc::new(InMemorySigner::from_seed(
            account_id.clone(),
            KeyType::ED25519,
            account_id.as_ref(),
        ));

        let mut initial_state = tries.new_trie_update(ShardUId::single_shard(), root);
        let mut initial_account = account_new(initial_balance, hash(&[]));
        // For the account and a full access key
        initial_account.set_storage_usage(182);
        initial_account.set_locked(initial_locked);
        set_account(&mut initial_state, account_id.clone(), &initial_account);
        set_access_key(
            &mut initial_state,
            account_id,
            signer.public_key(),
            &AccessKey::full_access(),
        );
        initial_state.commit(StateChangeCause::InitialState);
        let trie_changes = initial_state.finalize().unwrap().1;
        let mut store_update = tries.store_update();
        let root = tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
        store_update.commit().unwrap();

        let apply_state = ApplyState {
            block_height: 1,
            prev_block_hash: Default::default(),
            block_hash: Default::default(),
            epoch_id: Default::default(),
            epoch_height: 0,
            gas_price: GAS_PRICE,
            block_timestamp: 100,
            gas_limit: Some(gas_limit),
            random_seed: Default::default(),
            current_protocol_version: PROTOCOL_VERSION,
            config: Arc::new(RuntimeConfig::test()),
            cache: Some(Box::new(StoreCompiledContractCache::new(&tries.get_store()))),
            is_new_chunk: true,
            migration_data: Arc::new(MigrationData::default()),
            migration_flags: MigrationFlags::default(),
        };

        (runtime, tries, root, apply_state, signer, MockEpochInfoProvider::default())
    }

    #[test]
    fn test_apply_no_op() {
        let (runtime, tries, root, apply_state, _, epoch_info_provider) =
            setup_runtime(to_yocto(1_000_000), 0, 10u64.pow(15));
        runtime
            .apply(
                tries.get_trie_for_shard(ShardUId::single_shard(), root),
                &None,
                &apply_state,
                &[],
                &[],
                &epoch_info_provider,
                Default::default(),
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
                tries.get_trie_for_shard(ShardUId::single_shard(), root),
                &Some(validator_accounts_update),
                &apply_state,
                &[Receipt::new_balance_refund(&alice_account(), small_refund)],
                &[],
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();
    }

    #[test]
    fn test_apply_refund_receipts() {
        let initial_balance = to_yocto(1_000_000);
        let initial_locked = to_yocto(500_000);
        let small_transfer = to_yocto(10_000);
        let gas_limit = 1;
        let (runtime, tries, mut root, apply_state, _, epoch_info_provider) =
            setup_runtime(initial_balance, initial_locked, gas_limit);

        let n = 10;
        let receipts = generate_refund_receipts(small_transfer, n);

        // Checking n receipts delayed
        for i in 1..=n + 3 {
            let prev_receipts: &[Receipt] = if i == 1 { &receipts } else { &[] };
            let apply_result = runtime
                .apply(
                    tries.get_trie_for_shard(ShardUId::single_shard(), root),
                    &None,
                    &apply_state,
                    prev_receipts,
                    &[],
                    &epoch_info_provider,
                    Default::default(),
                )
                .unwrap();
            let mut store_update = tries.store_update();
            root = tries.apply_all(
                &apply_result.trie_changes,
                ShardUId::single_shard(),
                &mut store_update,
            );
            store_update.commit().unwrap();
            let state = tries.new_trie_update(ShardUId::single_shard(), root);
            let account = get_account(&state, &alice_account()).unwrap().unwrap();
            let capped_i = std::cmp::min(i, n);
            assert_eq!(
                account.amount(),
                initial_balance
                    + small_transfer * Balance::from(capped_i)
                    + Balance::from(capped_i * (capped_i - 1) / 2)
            );
        }
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
                    tries.get_trie_for_shard(ShardUId::single_shard(), root),
                    &None,
                    &apply_state,
                    prev_receipts,
                    &[],
                    &epoch_info_provider,
                    Default::default(),
                )
                .unwrap();
            let mut store_update = tries.store_update();
            root = tries.apply_all(
                &apply_result.trie_changes,
                ShardUId::single_shard(),
                &mut store_update,
            );
            store_update.commit().unwrap();
            let state = tries.new_trie_update(ShardUId::single_shard(), root);
            let account = get_account(&state, &alice_account()).unwrap().unwrap();
            let capped_i = std::cmp::min(i, n);
            assert_eq!(
                account.amount(),
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
            apply_state.config.fees.fee(ActionCosts::new_action_receipt).exec_fee()
                + apply_state.config.fees.fee(ActionCosts::transfer).exec_fee();
        apply_state.gas_limit = Some(receipt_gas_cost * 3);

        let n = 40;
        let receipts = generate_receipts(small_transfer, n);
        let mut receipt_chunks = receipts.chunks_exact(4);

        // Every time we'll process 3 receipts, so we need n / 3 rounded up. Then we do 3 extra.
        for i in 1..=n / 3 + 3 {
            let prev_receipts: &[Receipt] = receipt_chunks.next().unwrap_or_default();
            let apply_result = runtime
                .apply(
                    tries.get_trie_for_shard(ShardUId::single_shard(), root),
                    &None,
                    &apply_state,
                    prev_receipts,
                    &[],
                    &epoch_info_provider,
                    Default::default(),
                )
                .unwrap();
            let mut store_update = tries.store_update();
            let new_root = tries.apply_all(
                &apply_result.trie_changes,
                ShardUId::single_shard(),
                &mut store_update,
            );
            root = new_root;
            store_update.commit().unwrap();
            let state = tries.new_trie_update(ShardUId::single_shard(), root);
            let account = get_account(&state, &alice_account()).unwrap().unwrap();
            let capped_i = std::cmp::min(i * 3, n);
            assert_eq!(
                account.amount(),
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
            apply_state.config.fees.fee(ActionCosts::new_action_receipt).exec_fee()
                + apply_state.config.fees.fee(ActionCosts::transfer).exec_fee();

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
                    tries.get_trie_for_shard(ShardUId::single_shard(), root),
                    &None,
                    &apply_state,
                    prev_receipts,
                    &[],
                    &epoch_info_provider,
                    Default::default(),
                )
                .unwrap();
            let mut store_update = tries.store_update();
            root = tries.apply_all(
                &apply_result.trie_changes,
                ShardUId::single_shard(),
                &mut store_update,
            );
            store_update.commit().unwrap();
            let state = tries.new_trie_update(ShardUId::single_shard(), root);
            num_receipts_processed += apply_result.outcomes.len() as u64;
            let account = get_account(&state, &alice_account()).unwrap().unwrap();
            assert_eq!(
                account.amount(),
                initial_balance
                    + small_transfer * Balance::from(num_receipts_processed)
                    + Balance::from(num_receipts_processed * (num_receipts_processed - 1) / 2)
            );
            let expected_queue_length = num_receipts_given - num_receipts_processed;
            println!(
                "{} processed out of {} given. With limit {} receipts per block. The expected delayed_receipts_count is {}. The delayed_receipts_count is {}.",
                num_receipts_processed,
                num_receipts_given,
                num_receipts_per_block,
                expected_queue_length,
                apply_result.delayed_receipts_count,
            );
            assert_eq!(apply_result.delayed_receipts_count, expected_queue_length);
        }
    }

    fn generate_receipts(small_transfer: u128, n: u64) -> Vec<Receipt> {
        let mut receipt_id = CryptoHash::default();
        (0..n)
            .map(|i| {
                receipt_id = hash(receipt_id.as_ref());
                Receipt {
                    predecessor_id: bob_account(),
                    receiver_id: alice_account(),
                    receipt_id,
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
                }
            })
            .collect()
    }

    fn generate_refund_receipts(small_transfer: u128, n: u64) -> Vec<Receipt> {
        let mut receipt_id = CryptoHash::default();
        (0..n)
            .map(|i| {
                receipt_id = hash(receipt_id.as_ref());
                Receipt::new_balance_refund(&alice_account(), small_transfer + Balance::from(i))
            })
            .collect()
    }

    #[test]
    fn test_apply_delayed_receipts_local_tx() {
        let initial_balance = to_yocto(1_000_000);
        let initial_locked = to_yocto(500_000);
        let small_transfer = to_yocto(10_000);
        let (runtime, tries, root, mut apply_state, signer, epoch_info_provider) =
            setup_runtime(initial_balance, initial_locked, 1);

        let receipt_exec_gas_fee = 1000;
        let mut free_config = RuntimeConfig::free();
        free_config.fees.action_fees[ActionCosts::new_action_receipt].execution =
            receipt_exec_gas_fee;
        apply_state.config = Arc::new(free_config);
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
                tries.get_trie_for_shard(ShardUId::single_shard(), root),
                &None,
                &apply_state,
                &receipts[0..2],
                &local_transactions[0..4],
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        let mut store_update = tries.store_update();
        let root = tries.apply_all(
            &apply_result.trie_changes,
            ShardUId::single_shard(),
            &mut store_update,
        );
        store_update.commit().unwrap();

        assert_eq!(
            apply_result.outcomes.iter().map(|o| o.id).collect::<Vec<_>>(),
            vec![
                local_transactions[0].get_hash(), // tx 0
                local_transactions[1].get_hash(), // tx 1
                local_transactions[2].get_hash(), // tx 2
                local_transactions[3].get_hash(), // tx 3 - the TX is processed, but the receipt is delayed
                create_receipt_id_from_transaction(
                    PROTOCOL_VERSION,
                    &local_transactions[0],
                    &apply_state.prev_block_hash,
                    &apply_state.block_hash
                ), // receipt for tx 0
                create_receipt_id_from_transaction(
                    PROTOCOL_VERSION,
                    &local_transactions[1],
                    &apply_state.prev_block_hash,
                    &apply_state.block_hash
                ), // receipt for tx 1
                create_receipt_id_from_transaction(
                    PROTOCOL_VERSION,
                    &local_transactions[2],
                    &apply_state.prev_block_hash,
                    &apply_state.block_hash
                ), // receipt for tx 2
            ],
            "STEP #1 failed",
        );

        // STEP #2. Pass 1 new local transaction (TX#4) + 1 receipts R#2.
        // We process 1 local receipts for TX#4, then delayed TX#3 receipt and then receipt R#0.
        // R#2 is added to delayed queue.
        // The new delayed queue is R#1, R#2
        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(ShardUId::single_shard(), root),
                &None,
                &apply_state,
                &receipts[2..3],
                &local_transactions[4..5],
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        let mut store_update = tries.store_update();
        let root = tries.apply_all(
            &apply_result.trie_changes,
            ShardUId::single_shard(),
            &mut store_update,
        );
        store_update.commit().unwrap();

        assert_eq!(
            apply_result.outcomes.iter().map(|o| o.id).collect::<Vec<_>>(),
            vec![
                local_transactions[4].get_hash(), // tx 4
                create_receipt_id_from_transaction(
                    PROTOCOL_VERSION,
                    &local_transactions[4],
                    &apply_state.prev_block_hash,
                    &apply_state.block_hash,
                ), // receipt for tx 4
                create_receipt_id_from_transaction(
                    PROTOCOL_VERSION,
                    &local_transactions[3],
                    &apply_state.prev_block_hash,
                    &apply_state.block_hash,
                ), // receipt for tx 3
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
                tries.get_trie_for_shard(ShardUId::single_shard(), root),
                &None,
                &apply_state,
                &receipts[3..4],
                &local_transactions[5..9],
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        let mut store_update = tries.store_update();
        let root = tries.apply_all(
            &apply_result.trie_changes,
            ShardUId::single_shard(),
            &mut store_update,
        );
        store_update.commit().unwrap();

        assert_eq!(
            apply_result.outcomes.iter().map(|o| o.id).collect::<Vec<_>>(),
            vec![
                local_transactions[5].get_hash(), // tx 5
                local_transactions[6].get_hash(), // tx 6
                local_transactions[7].get_hash(), // tx 7
                local_transactions[8].get_hash(), // tx 8
                create_receipt_id_from_transaction(
                    PROTOCOL_VERSION,
                    &local_transactions[5],
                    &apply_state.prev_block_hash,
                    &apply_state.block_hash,
                ), // receipt for tx 5
                create_receipt_id_from_transaction(
                    PROTOCOL_VERSION,
                    &local_transactions[6],
                    &apply_state.prev_block_hash,
                    &apply_state.block_hash,
                ), // receipt for tx 6
                create_receipt_id_from_transaction(
                    PROTOCOL_VERSION,
                    &local_transactions[7],
                    &apply_state.prev_block_hash,
                    &apply_state.block_hash,
                ), // receipt for tx 7
            ],
            "STEP #3 failed",
        );

        // STEP #4. Pass no new TXs and 1 receipt R#4.
        // We process R#1, R#2, TX#8.
        // R#4 is added to delayed queue.
        // The new delayed queue is R#3, R#4
        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(ShardUId::single_shard(), root),
                &None,
                &apply_state,
                &receipts[4..5],
                &[],
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        let mut store_update = tries.store_update();
        let root = tries.apply_all(
            &apply_result.trie_changes,
            ShardUId::single_shard(),
            &mut store_update,
        );
        store_update.commit().unwrap();

        assert_eq!(
            apply_result.outcomes.iter().map(|o| o.id).collect::<Vec<_>>(),
            vec![
                receipts[1].receipt_id, // receipt #1
                receipts[2].receipt_id, // receipt #2
                create_receipt_id_from_transaction(
                    PROTOCOL_VERSION,
                    &local_transactions[8],
                    &apply_state.prev_block_hash,
                    &apply_state.block_hash,
                ), // receipt for tx 8
            ],
            "STEP #4 failed",
        );

        // STEP #5. Pass no new TXs and 1 receipt R#5.
        // We process R#3, R#4, R#5.
        // The new delayed queue is empty.
        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(ShardUId::single_shard(), root),
                &None,
                &apply_state,
                &receipts[5..6],
                &[],
                &epoch_info_provider,
                Default::default(),
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
                tries.get_trie_for_shard(ShardUId::single_shard(), root),
                &None,
                &apply_state,
                &receipts,
                &[],
                &epoch_info_provider,
                Default::default(),
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
            apply_state.config.fees.fee(ActionCosts::new_action_receipt).exec_fee(),
            total_prepaid_exec_fees(
                &apply_state.config.fees,
                &actions,
                &alice_account(),
                PROTOCOL_VERSION,
            )
            .unwrap(),
        )
        .unwrap();
        let receipts = vec![Receipt {
            predecessor_id: bob_account(),
            receiver_id: alice_account(),
            receipt_id: CryptoHash::default(),
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
                tries.get_trie_for_shard(ShardUId::single_shard(), root),
                &None,
                &apply_state,
                &receipts,
                &[],
                &epoch_info_provider,
                Default::default(),
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
            apply_state.config.fees.fee(ActionCosts::new_action_receipt).exec_fee(),
            total_prepaid_exec_fees(
                &apply_state.config.fees,
                &actions,
                &alice_account(),
                PROTOCOL_VERSION,
            )
            .unwrap(),
        )
        .unwrap();
        let receipts = vec![Receipt {
            predecessor_id: bob_account(),
            receiver_id: alice_account(),
            receipt_id: CryptoHash::default(),
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
                tries.get_trie_for_shard(ShardUId::single_shard(), root),
                &None,
                &apply_state,
                &receipts,
                &[],
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        // Used full prepaid gas, but it still not enough to cover deficit.
        assert_eq!(result.stats.gas_deficit_amount, expected_deficit);
        // Burnt all the fees + all prepaid gas.
        assert_eq!(result.stats.tx_burnt_amount, total_receipt_cost);
    }

    #[test]
    fn test_delete_key_add_key() {
        let initial_locked = to_yocto(500_000);
        let (runtime, tries, root, apply_state, signer, epoch_info_provider) =
            setup_runtime(to_yocto(1_000_000), initial_locked, 10u64.pow(15));

        let state_update = tries.new_trie_update(ShardUId::single_shard(), root);
        let initial_account_state = get_account(&state_update, &alice_account()).unwrap().unwrap();

        let actions = vec![
            Action::DeleteKey(DeleteKeyAction { public_key: signer.public_key() }),
            Action::AddKey(AddKeyAction {
                public_key: signer.public_key(),
                access_key: AccessKey::full_access(),
            }),
        ];

        let receipts = vec![create_receipt_with_actions(alice_account(), signer, actions)];

        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(ShardUId::single_shard(), root),
                &None,
                &apply_state,
                &receipts,
                &[],
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        let mut store_update = tries.store_update();
        let root = tries.apply_all(
            &apply_result.trie_changes,
            ShardUId::single_shard(),
            &mut store_update,
        );
        store_update.commit().unwrap();

        let state_update = tries.new_trie_update(ShardUId::single_shard(), root);
        let final_account_state = get_account(&state_update, &alice_account()).unwrap().unwrap();

        assert_eq!(initial_account_state.storage_usage(), final_account_state.storage_usage());
    }

    #[test]
    fn test_delete_key_underflow() {
        let initial_locked = to_yocto(500_000);
        let (runtime, tries, root, apply_state, signer, epoch_info_provider) =
            setup_runtime(to_yocto(1_000_000), initial_locked, 10u64.pow(15));

        let mut state_update = tries.new_trie_update(ShardUId::single_shard(), root);
        let mut initial_account_state =
            get_account(&state_update, &alice_account()).unwrap().unwrap();
        initial_account_state.set_storage_usage(10);
        set_account(&mut state_update, alice_account(), &initial_account_state);
        state_update.commit(StateChangeCause::InitialState);
        let trie_changes = state_update.finalize().unwrap().1;
        let mut store_update = tries.store_update();
        let root = tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
        store_update.commit().unwrap();

        let actions = vec![Action::DeleteKey(DeleteKeyAction { public_key: signer.public_key() })];

        let receipts = vec![create_receipt_with_actions(alice_account(), signer, actions)];

        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(ShardUId::single_shard(), root),
                &None,
                &apply_state,
                &receipts,
                &[],
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        let mut store_update = tries.store_update();
        let root = tries.apply_all(
            &apply_result.trie_changes,
            ShardUId::single_shard(),
            &mut store_update,
        );
        store_update.commit().unwrap();

        let state_update = tries.new_trie_update(ShardUId::single_shard(), root);
        let final_account_state = get_account(&state_update, &alice_account()).unwrap().unwrap();

        assert_eq!(final_account_state.storage_usage(), 0);
    }

    // This test only works on platforms that support wasmer2.
    #[test]
    #[cfg(target_arch = "x86_64")]
    fn test_contract_precompilation() {
        let initial_balance = to_yocto(1_000_000);
        let initial_locked = to_yocto(500_000);
        let gas_limit = 10u64.pow(15);
        let (runtime, tries, root, apply_state, signer, epoch_info_provider) =
            setup_runtime(initial_balance, initial_locked, gas_limit);

        let wasm_code = near_test_contracts::rs_contract().to_vec();
        let actions =
            vec![Action::DeployContract(DeployContractAction { code: wasm_code.clone() })];

        let receipts = vec![create_receipt_with_actions(alice_account(), signer, actions)];

        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(ShardUId::single_shard(), root),
                &None,
                &apply_state,
                &receipts,
                &[],
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        let mut store_update = tries.store_update();
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
        store_update.commit().unwrap();

        let contract_code = near_primitives::contract::ContractCode::new(wasm_code, None);
        let vm_kind = near_vm_runner::internal::VMKind::for_protocol_version(
            apply_state.current_protocol_version,
        );
        let key = near_vm_runner::get_contract_cache_key(
            &contract_code,
            vm_kind,
            &apply_state.config.wasm_config,
        );
        apply_state
            .cache
            .unwrap()
            .get(&key)
            .expect("Compiled contract should be cached")
            .expect("Compilation result should be non-empty");
    }

    #[test]
    fn test_compute_usage_limit() {
        let (runtime, tries, root, mut apply_state, signer, epoch_info_provider) =
            setup_runtime(to_yocto(1_000_000), to_yocto(500_000), 1);

        let mut free_config = RuntimeConfig::free();
        let sha256_cost = ParameterCost {
            gas: Gas::from(1_000_000u64),
            compute: Compute::from(10_000_000_000_000u64),
        };
        free_config.wasm_config.ext_costs.costs[ExtCosts::sha256_base] = sha256_cost.clone();
        apply_state.config = Arc::new(free_config);
        // This allows us to execute 1 receipt with a function call per apply.
        apply_state.gas_limit = Some(sha256_cost.compute);

        let deploy_contract_receipt = create_receipt_with_actions(
            alice_account(),
            signer.clone(),
            vec![Action::DeployContract(DeployContractAction {
                code: near_test_contracts::rs_contract().to_vec(),
            })],
        );

        let first_call_receipt = create_receipt_with_actions(
            alice_account(),
            signer.clone(),
            vec![Action::FunctionCall(FunctionCallAction {
                method_name: "ext_sha256".to_string(),
                args: b"first".to_vec(),
                gas: sha256_cost.gas,
                deposit: 0,
            })],
        );

        let second_call_receipt = create_receipt_with_actions(
            alice_account(),
            signer,
            vec![Action::FunctionCall(FunctionCallAction {
                method_name: "ext_sha256".to_string(),
                args: b"second".to_vec(),
                gas: sha256_cost.gas,
                deposit: 0,
            })],
        );

        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(ShardUId::single_shard(), root),
                &None,
                &apply_state,
                &vec![
                    deploy_contract_receipt.clone(),
                    first_call_receipt.clone(),
                    second_call_receipt.clone(),
                ],
                &[],
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        let mut store_update = tries.store_update();
        let root = tries.apply_all(
            &apply_result.trie_changes,
            ShardUId::single_shard(),
            &mut store_update,
        );
        store_update.commit().unwrap();

        // Only first two receipts should fit into the chunk due to the compute usage limit.
        assert_matches!(&apply_result.outcomes[..], [first, second] => {
            assert_eq!(first.id, deploy_contract_receipt.receipt_id);
            assert_matches!(first.outcome.status, ExecutionStatus::SuccessValue(_));

            assert_eq!(second.id, first_call_receipt.receipt_id);
            assert_eq!(second.outcome.compute_usage.unwrap(), sha256_cost.compute);
            assert_matches!(second.outcome.status, ExecutionStatus::SuccessValue(_));
        });

        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(ShardUId::single_shard(), root),
                &None,
                &apply_state,
                &[],
                &[],
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();

        assert_matches!(&apply_result.outcomes[..], [ExecutionOutcomeWithId { id, outcome }] => {
            assert_eq!(*id, second_call_receipt.receipt_id);
            assert_eq!(outcome.compute_usage.unwrap(), sha256_cost.compute);
            assert_matches!(outcome.status, ExecutionStatus::SuccessValue(_));
        });
    }

    #[test]
    fn test_compute_usage_limit_with_failed_receipt() {
        let (runtime, tries, root, apply_state, signer, epoch_info_provider) =
            setup_runtime(to_yocto(1_000_000), to_yocto(500_000), 10u64.pow(15));

        let deploy_contract_receipt = create_receipt_with_actions(
            alice_account(),
            signer.clone(),
            vec![Action::DeployContract(DeployContractAction {
                code: near_test_contracts::rs_contract().to_vec(),
            })],
        );

        let first_call_receipt = create_receipt_with_actions(
            alice_account(),
            signer,
            vec![Action::FunctionCall(FunctionCallAction {
                method_name: "ext_sha256".to_string(),
                args: b"first".to_vec(),
                gas: 1,
                deposit: 0,
            })],
        );

        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(ShardUId::single_shard(), root),
                &None,
                &apply_state,
                &vec![deploy_contract_receipt.clone(), first_call_receipt.clone()],
                &[],
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();

        assert_matches!(&apply_result.outcomes[..], [first, second] => {
            assert_eq!(first.id, deploy_contract_receipt.receipt_id);
            assert_matches!(first.outcome.status, ExecutionStatus::SuccessValue(_));

            assert_eq!(second.id, first_call_receipt.receipt_id);
            assert_matches!(second.outcome.status, ExecutionStatus::Failure(_));
        });
    }
}

/// Interface provided for gas cost estimations.
pub mod estimator {
    use near_primitives::errors::RuntimeError;
    use near_primitives::receipt::Receipt;
    use near_primitives::runtime::apply_state::ApplyState;
    use near_primitives::transaction::ExecutionOutcomeWithId;
    use near_primitives::types::validator_stake::ValidatorStake;
    use near_primitives::types::EpochInfoProvider;
    use near_store::TrieUpdate;

    use crate::ApplyStats;

    use super::Runtime;

    pub fn apply_action_receipt(
        state_update: &mut TrieUpdate,
        apply_state: &ApplyState,
        receipt: &Receipt,
        outgoing_receipts: &mut Vec<Receipt>,
        validator_proposals: &mut Vec<ValidatorStake>,
        stats: &mut ApplyStats,
        epoch_info_provider: &dyn EpochInfoProvider,
    ) -> Result<ExecutionOutcomeWithId, RuntimeError> {
        Runtime {}.apply_action_receipt(
            state_update,
            apply_state,
            receipt,
            outgoing_receipts,
            validator_proposals,
            stats,
            epoch_info_provider,
        )
    }
}

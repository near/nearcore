use crate::actions::*;
use crate::balance_checker::check_balance;
use crate::config::{
    exec_fee, safe_add_balance, safe_add_compute, safe_add_gas, safe_gas_to_balance, total_deposit,
    total_prepaid_exec_fees, total_prepaid_gas,
};
use crate::congestion_control::DelayedReceiptQueueWrapper;
use crate::prefetch::TriePrefetcher;
use crate::verifier::{check_storage_stake, validate_receipt, StorageStakingError};
pub use crate::verifier::{
    validate_transaction, verify_and_charge_transaction, ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT,
};
use config::total_prepaid_send_fees;
pub use congestion_control::bootstrap_congestion_info;
use congestion_control::ReceiptSink;
use metrics::ApplyMetrics;
pub use near_crypto;
use near_parameters::{ActionCosts, RuntimeConfig};
pub use near_primitives;
use near_primitives::account::Account;
use near_primitives::checked_feature;
use near_primitives::congestion_info::{BlockCongestionInfo, CongestionInfo};
use near_primitives::errors::{
    ActionError, ActionErrorKind, IntegerOverflowError, InvalidTxError, RuntimeError,
    TxExecutionError,
};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    ActionReceipt, DataReceipt, DelayedReceiptIndices, PromiseYieldIndices, PromiseYieldTimeout,
    Receipt, ReceiptEnum, ReceiptV0, ReceivedData,
};
use near_primitives::runtime::migration_data::{MigrationData, MigrationFlags};
use near_primitives::sandbox::state_patch::SandboxStatePatch;
use near_primitives::state_record::StateRecord;
#[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
use near_primitives::transaction::NonrefundableStorageTransferAction;
use near_primitives::transaction::{
    Action, ExecutionMetadata, ExecutionOutcome, ExecutionOutcomeWithId, ExecutionStatus, LogEntry,
    SignedTransaction, TransferAction,
};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{
    validator_stake::ValidatorStake, AccountId, Balance, BlockHeight, Compute, EpochHeight,
    EpochId, EpochInfoProvider, Gas, RawStateChangesWithTrieKey, ShardId, StateChangeCause,
    StateRoot,
};
use near_primitives::utils::{
    create_action_hash_from_receipt_id, create_receipt_id_from_receipt_id,
    create_receipt_id_from_transaction,
};
use near_primitives::version::{ProtocolFeature, ProtocolVersion};
use near_primitives_core::apply::ApplyChunkReason;
use near_store::trie::receipts_column_helper::DelayedReceiptQueue;
use near_store::{
    get, get_account, get_postponed_receipt, get_promise_yield_receipt, get_received_data,
    has_received_data, remove_account, remove_postponed_receipt, remove_promise_yield_receipt, set,
    set_access_key, set_account, set_code, set_postponed_receipt, set_promise_yield_receipt,
    set_received_data, PartialStorage, StorageError, Trie, TrieAccess, TrieChanges, TrieUpdate,
};
use near_vm_runner::logic::types::PromiseResult;
use near_vm_runner::logic::ReturnData;
pub use near_vm_runner::with_ext_cost_counter;
use near_vm_runner::ContractCode;
use near_vm_runner::ContractRuntimeCache;
use near_vm_runner::ProfileDataV3;
use std::cmp::max;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tracing::{debug, instrument};

mod actions;
pub mod adapter;
mod balance_checker;
pub mod config;
mod congestion_control;
mod conversions;
pub mod ext;
mod metrics;
mod prefetch;
pub mod receipt_manager;
pub mod state_viewer;
#[cfg(test)]
mod tests;
mod verifier;

const EXPECT_ACCOUNT_EXISTS: &str = "account exists, checked above";

#[derive(Debug)]
pub struct ApplyState {
    /// Represents a phase of the chain lifecycle that we want to run apply for.
    /// This is currently represented as a static string and used as dimension in some metrics.
    pub apply_reason: Option<ApplyChunkReason>,
    /// Currently building block height.
    pub block_height: BlockHeight,
    /// Prev block hash
    pub prev_block_hash: CryptoHash,
    /// Current block hash
    pub block_hash: CryptoHash,
    /// To which shard the applied chunk belongs.
    pub shard_id: ShardId,
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
    /// The Runtime config to use for the current transition.
    pub config: Arc<RuntimeConfig>,
    /// Cache for compiled contracts.
    pub cache: Option<Box<dyn ContractRuntimeCache>>,
    /// Whether the chunk being applied is new.
    pub is_new_chunk: bool,
    /// Data for migrations that may need to be applied at the start of an epoch when protocol
    /// version changes
    pub migration_data: Arc<MigrationData>,
    /// Flags for migrations indicating whether they can be applied at this block
    pub migration_flags: MigrationFlags,
    /// Congestion level on each shard based on the latest known chunk header of each shard.
    ///
    /// The map must be empty if congestion control is disabled in the previous
    /// chunk. If the next chunks is the first with congestion control enabled,
    /// the congestion info needs to be computed while applying receipts.
    /// TODO(congestion_info) - verify performance of initialization when congested
    pub congestion_info: BlockCongestionInfo,
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
    pub processed_yield_timeouts: Vec<PromiseYieldTimeout>,
    pub proof: Option<PartialStorage>,
    pub delayed_receipts_count: u64,
    pub metrics: Option<metrics::ApplyMetrics>,
    pub congestion_info: Option<CongestionInfo>,
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
    pub profile: Box<ProfileDataV3>,
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
    #[instrument(target = "runtime", level = "debug", "process_transaction", skip_all, fields(
        tx_hash = %signed_transaction.get_hash(),
        gas_burnt = tracing::field::Empty,
        compute_usage = tracing::field::Empty,
    ))]
    fn process_transaction(
        &self,
        state_update: &mut TrieUpdate,
        apply_state: &ApplyState,
        signed_transaction: &SignedTransaction,
        stats: &mut ApplyStats,
    ) -> Result<(Receipt, ExecutionOutcomeWithId), InvalidTxError> {
        let span = tracing::Span::current();
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
                let receipt = Receipt::V0(ReceiptV0 {
                    predecessor_id: transaction.signer_id().clone(),
                    receiver_id: transaction.receiver_id().clone(),
                    receipt_id,
                    receipt: ReceiptEnum::Action(ActionReceipt {
                        signer_id: transaction.signer_id().clone(),
                        signer_public_key: transaction.public_key().clone(),
                        gas_price: verification_result.receipt_gas_price,
                        output_data_receivers: vec![],
                        input_data_ids: vec![],
                        actions: transaction.actions().to_vec(),
                    }),
                });
                stats.tx_burnt_amount =
                    safe_add_balance(stats.tx_burnt_amount, verification_result.burnt_amount)
                        .map_err(|_| InvalidTxError::CostOverflow)?;
                let gas_burnt = verification_result.gas_burnt;
                let compute_usage = verification_result.gas_burnt;
                let outcome = ExecutionOutcomeWithId {
                    id: signed_transaction.get_hash(),
                    outcome: ExecutionOutcome {
                        status: ExecutionStatus::SuccessReceiptId(*receipt.receipt_id()),
                        logs: vec![],
                        receipt_ids: vec![*receipt.receipt_id()],
                        gas_burnt,
                        // TODO(#8806): Support compute costs for actions. For now they match burnt gas.
                        compute_usage: Some(compute_usage),
                        tokens_burnt: verification_result.burnt_amount,
                        executor_id: transaction.signer_id().clone(),
                        // TODO: profile data is only counted in apply_action, which only happened at process_receipt
                        // VerificationResult needs updates to incorporate profile data to support profile data of txns
                        metadata: ExecutionMetadata::V1,
                    },
                };
                span.record("gas_burnt", gas_burnt);
                span.record("compute_usage", compute_usage);
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
        promise_results: Arc<[PromiseResult]>,
        action_hash: &CryptoHash,
        action_index: usize,
        actions: &[Action],
        epoch_info_provider: &(dyn EpochInfoProvider),
    ) -> Result<ActionResult, RuntimeError> {
        let _span = tracing::debug_span!(
            target: "runtime",
            "apply_action",
        )
        .entered();
        let exec_fees = exec_fee(&apply_state.config, action, receipt.receiver_id());
        let mut result = ActionResult::default();
        result.gas_used = exec_fees;
        result.gas_burnt = exec_fees;
        // TODO(#8806): Support compute costs for actions. For now they match burnt gas.
        result.compute_usage = exec_fees;
        let account_id = receipt.receiver_id();
        let is_refund = receipt.predecessor_id().is_system();
        let is_the_only_action = actions.len() == 1;
        let implicit_account_creation_eligible = is_the_only_action && !is_refund;

        let receipt_starts_with_create_account =
            matches!(actions.get(0), Some(Action::CreateAccount(_)));
        // Account validation
        if let Err(e) = check_account_existence(
            action,
            account,
            account_id,
            &apply_state.config,
            implicit_account_creation_eligible,
            receipt_starts_with_create_account,
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
                    receipt.receiver_id(),
                    receipt.predecessor_id(),
                    &mut result,
                    apply_state.current_protocol_version,
                );
            }
            Action::DeployContract(deploy_contract) => {
                action_deploy_contract(
                    state_update,
                    account.as_mut().expect(EXPECT_ACCOUNT_EXISTS),
                    account_id,
                    deploy_contract,
                    apply_state,
                )?;
            }
            Action::FunctionCall(function_call) => {
                let account = account.as_mut().expect(EXPECT_ACCOUNT_EXISTS);
                let contract = prepare_function_call(
                    state_update,
                    apply_state,
                    account,
                    account_id,
                    function_call,
                    &apply_state.config,
                    epoch_info_provider,
                    None,
                );
                let is_last_action = action_index + 1 == actions.len();
                action_function_call(
                    state_update,
                    apply_state,
                    account,
                    receipt,
                    action_receipt,
                    promise_results,
                    &mut result,
                    account_id,
                    function_call,
                    action_hash,
                    &apply_state.config,
                    is_last_action,
                    epoch_info_provider,
                    contract,
                )?;
            }
            Action::Transfer(TransferAction { deposit }) => {
                action_transfer_or_implicit_account_creation(
                    account,
                    *deposit,
                    false,
                    is_refund,
                    action_receipt,
                    receipt,
                    state_update,
                    apply_state,
                    actor_id,
                    epoch_info_provider,
                )?;
            }
            #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
            Action::NonrefundableStorageTransfer(NonrefundableStorageTransferAction {
                deposit,
            }) => {
                action_transfer_or_implicit_account_creation(
                    account,
                    *deposit,
                    true,
                    is_refund,
                    action_receipt,
                    receipt,
                    state_update,
                    apply_state,
                    actor_id,
                    epoch_info_provider,
                )?;
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
                    receipt.priority(),
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
        receipt_sink: &mut ReceiptSink,
        validator_proposals: &mut Vec<ValidatorStake>,
        stats: &mut ApplyStats,
        epoch_info_provider: &(dyn EpochInfoProvider),
    ) -> Result<ExecutionOutcomeWithId, RuntimeError> {
        let _span = tracing::debug_span!(
            target: "runtime",
            "apply_action_receipt",
        )
        .entered();
        let action_receipt = match receipt.receipt() {
            ReceiptEnum::Action(action_receipt) | ReceiptEnum::PromiseYield(action_receipt) => {
                action_receipt
            }
            _ => unreachable!("given receipt should be an action receipt"),
        };
        let account_id = receipt.receiver_id();
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
            .collect::<Result<Arc<[PromiseResult]>, RuntimeError>>()?;

        // state_update might already have some updates so we need to make sure we commit it before
        // executing the actual receipt
        state_update.commit(StateChangeCause::ActionReceiptProcessingStarted {
            receipt_hash: receipt.get_hash(),
        });

        let mut account = get_account(state_update, account_id)?;
        let mut actor_id = receipt.predecessor_id().clone();
        let mut result = ActionResult::default();
        let exec_fees = apply_state.config.fees.fee(ActionCosts::new_action_receipt).exec_fee();
        result.gas_used = exec_fees;
        result.gas_burnt = exec_fees;
        // TODO(#8806): Support compute costs for actions. For now they match burnt gas.
        result.compute_usage = exec_fees;
        #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
        let mut nonrefundable_amount_burnt: Balance = 0;

        // Executing actions one by one
        for (action_index, action) in action_receipt.actions.iter().enumerate() {
            let action_hash = create_action_hash_from_receipt_id(
                apply_state.current_protocol_version,
                receipt.receipt_id(),
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
                Arc::clone(&promise_results),
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

            #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
            if let Action::NonrefundableStorageTransfer(NonrefundableStorageTransferAction {
                deposit,
            }) = action
            {
                nonrefundable_amount_burnt = safe_add_balance(nonrefundable_amount_burnt, *deposit)?
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

        let gas_deficit_amount = if receipt.predecessor_id().is_system() {
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
                &apply_state.config,
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
        // If the receipt was successfully applied, we update `other_burnt_amount` statistic with the non-refundable amount burnt.
        #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
        if result.result.is_ok() {
            stats.other_burnt_amount =
                safe_add_balance(stats.other_burnt_amount, nonrefundable_amount_burnt)?;
        }

        // If the receipt is a refund, then we consider it free without burnt gas.
        let gas_burnt: Gas =
            if receipt.predecessor_id().is_system() { 0 } else { result.gas_burnt };
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
                    .receipt_mut()
                {
                    ReceiptEnum::Action(ref mut new_action_receipt)
                    | ReceiptEnum::PromiseYield(ref mut new_action_receipt) => new_action_receipt
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
                    |data_receiver| {
                        Receipt::V0(ReceiptV0 {
                            predecessor_id: account_id.clone(),
                            receiver_id: data_receiver.receiver_id.clone(),
                            receipt_id: CryptoHash::default(),
                            receipt: ReceiptEnum::Data(DataReceipt {
                                data_id: data_receiver.data_id,
                                data: data.clone(),
                            }),
                        })
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
                let receipt_id = create_receipt_id_from_receipt_id(
                    apply_state.current_protocol_version,
                    receipt.receipt_id(),
                    &apply_state.prev_block_hash,
                    &apply_state.block_hash,
                    receipt_index,
                );

                new_receipt.set_receipt_id(receipt_id);
                let is_action = matches!(
                    new_receipt.receipt(),
                    ReceiptEnum::Action(_) | ReceiptEnum::PromiseYield(_)
                );

                let res = receipt_sink.forward_or_buffer_receipt(
                    new_receipt,
                    apply_state,
                    state_update,
                    epoch_info_provider,
                );
                if let Err(e) = res {
                    Some(Err(e))
                } else if is_action {
                    Some(Ok(receipt_id))
                } else {
                    None
                }
            })
            .collect::<Result<_, _>>()?;

        let status = match result.result {
            Ok(ReturnData::ReceiptIndex(receipt_index)) => {
                ExecutionStatus::SuccessReceiptId(create_receipt_id_from_receipt_id(
                    apply_state.current_protocol_version,
                    receipt.receipt_id(),
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
            id: *receipt.receipt_id(),
            outcome: ExecutionOutcome {
                status,
                logs: result.logs,
                receipt_ids,
                gas_burnt: result.gas_burnt,
                compute_usage: Some(result.compute_usage),
                tokens_burnt,
                executor_id: account_id.clone(),
                metadata: ExecutionMetadata::V3(Box::new(conversions::Convert::convert(
                    *result.profile,
                ))),
            },
        })
    }

    fn generate_refund_receipts(
        &self,
        current_gas_price: Balance,
        receipt: &Receipt,
        action_receipt: &ActionReceipt,
        result: &mut ActionResult,
        config: &RuntimeConfig,
    ) -> Result<Balance, RuntimeError> {
        let total_deposit = total_deposit(&action_receipt.actions)?;
        let prepaid_gas = safe_add_gas(
            total_prepaid_gas(&action_receipt.actions)?,
            total_prepaid_send_fees(config, &action_receipt.actions)?,
        )?;
        let prepaid_exec_gas = safe_add_gas(
            total_prepaid_exec_fees(config, &action_receipt.actions, receipt.receiver_id())?,
            config.fees.fee(ActionCosts::new_action_receipt).exec_fee(),
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
            // Refund for the difference of the purchased gas price and the current gas price.
            gas_balance_refund = safe_add_balance(
                gas_balance_refund,
                safe_gas_to_balance(
                    action_receipt.gas_price - current_gas_price,
                    result.gas_burnt,
                )?,
            )?;
        }

        if deposit_refund > 0 {
            result.new_receipts.push(Receipt::new_balance_refund(
                receipt.predecessor_id(),
                deposit_refund,
                receipt.priority(),
            ));
        }
        if gas_balance_refund > 0 {
            // Gas refunds refund the allowance of the access key, so if the key exists on the
            // account it will increase the allowance by the refund amount.
            result.new_receipts.push(Receipt::new_gas_refund(
                &action_receipt.signer_id,
                gas_balance_refund,
                action_receipt.signer_public_key.clone(),
                receipt.priority(),
            ));
        }
        Ok(gas_deficit_amount)
    }

    fn process_receipt(
        &self,
        state_update: &mut TrieUpdate,
        apply_state: &ApplyState,
        receipt: &Receipt,
        receipt_sink: &mut ReceiptSink,
        validator_proposals: &mut Vec<ValidatorStake>,
        stats: &mut ApplyStats,
        epoch_info_provider: &(dyn EpochInfoProvider),
    ) -> Result<Option<ExecutionOutcomeWithId>, RuntimeError> {
        let account_id = receipt.receiver_id();
        match receipt.receipt() {
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
                                receipt_sink,
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
                    if !has_received_data(state_update, account_id, *data_id)? {
                        pending_data_count += 1;
                        // The data for a given data_id is not available, so we save a link to this
                        // receipt_id for the pending data_id into the state.
                        set(
                            state_update,
                            TrieKey::PostponedReceiptId {
                                receiver_id: account_id.clone(),
                                data_id: *data_id,
                            },
                            receipt.receipt_id(),
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
                            receipt_sink,
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
                            receipt_id: *receipt.receipt_id(),
                        },
                        &pending_data_count,
                    );
                    // Save the receipt itself into the state.
                    set_postponed_receipt(state_update, receipt);
                }
            }
            ReceiptEnum::PromiseYield(_) => {
                // Received a new PromiseYield receipt. We simply store it and await
                // the corresponding PromiseResume receipt.
                set_promise_yield_receipt(state_update, receipt);
            }
            ReceiptEnum::PromiseResume(ref data_receipt) => {
                // Received a new PromiseResume receipt delivering input data for a PromiseYield.
                // It is guaranteed that the PromiseYield has exactly one input data dependency
                // and that it arrives first, so we can simply find and execute it.
                if let Some(yield_receipt) =
                    get_promise_yield_receipt(state_update, account_id, data_receipt.data_id)?
                {
                    // Remove the receipt from the state
                    remove_promise_yield_receipt(state_update, account_id, data_receipt.data_id);

                    // Save the data into the state keyed by the data_id
                    set_received_data(
                        state_update,
                        account_id.clone(),
                        data_receipt.data_id,
                        &ReceivedData { data: data_receipt.data.clone() },
                    );

                    // Execute the PromiseYield receipt. It will read the input data and clean it
                    // up from the state.
                    return self
                        .apply_action_receipt(
                            state_update,
                            apply_state,
                            &yield_receipt,
                            receipt_sink,
                            validator_proposals,
                            stats,
                            epoch_info_provider,
                        )
                        .map(Some);
                } else {
                    // If the user happens to call `promise_yield_resume` multiple times, it may so
                    // happen that multiple PromiseResume receipts are delivered. We can safely
                    // ignore all but the first.
                    return Ok(None);
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
                    account.set_locked(account.locked().checked_add(*reward).ok_or_else(|| {
                        RuntimeError::UnexpectedIntegerOverflow("update_validator_accounts".into())
                    })?);
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
                    .ok_or_else(|| {
                        RuntimeError::UnexpectedIntegerOverflow(
                            "update_validator_accounts - return stake".into(),
                        )
                    })?;
                debug!(target: "runtime", "account {} return stake {}", account_id, return_stake);
                account.set_locked(account.locked().checked_sub(return_stake).ok_or_else(
                    || {
                        RuntimeError::UnexpectedIntegerOverflow(
                            "update_validator_accounts - set_locked".into(),
                        )
                    },
                )?);
                account.set_amount(account.amount().checked_add(return_stake).ok_or_else(
                    || {
                        RuntimeError::UnexpectedIntegerOverflow(
                            "update_validator_accounts - set_amount".into(),
                        )
                    },
                )?);

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
                stats.slashed_burnt_amount =
                    stats.slashed_burnt_amount.checked_add(amount_to_slash).ok_or_else(|| {
                        RuntimeError::UnexpectedIntegerOverflow(
                            "update_validator_accounts - slashed".into(),
                        )
                    })?;
                account.set_locked(account.locked().checked_sub(amount_to_slash).ok_or_else(
                    || {
                        RuntimeError::UnexpectedIntegerOverflow(
                            "update_validator_accounts - slash locked".into(),
                        )
                    },
                )?);
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
                account.set_amount(account.amount().checked_add(treasury_reward).ok_or_else(
                    || {
                        RuntimeError::UnexpectedIntegerOverflow(
                            "update_validator_accounts - treasure_reward".into(),
                        )
                    },
                )?);
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

        // Remove the only testnet account with large storage key.
        if ProtocolFeature::RemoveAccountWithLongStorageKey.protocol_version() == protocol_version
            && migration_flags.is_first_block_with_chunk_of_version
        {
            let account_id = "contractregistry.testnet".parse().unwrap();
            if get_account(state_update, &account_id)?.is_some() {
                remove_account(state_update, &account_id)?;
                state_update.commit(StateChangeCause::Migration);
            }
        }

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
    #[instrument(target = "runtime", level = "debug", "apply", skip_all, fields(
        protocol_version = apply_state.current_protocol_version,
        num_transactions = transactions.len(),
        gas_burnt = tracing::field::Empty,
        compute_usage = tracing::field::Empty,
    ))]
    pub fn apply(
        &self,
        trie: Trie,
        validator_accounts_update: &Option<ValidatorAccountsUpdate>,
        apply_state: &ApplyState,
        incoming_receipts: &[Receipt],
        transactions: &[SignedTransaction],
        epoch_info_provider: &(dyn EpochInfoProvider),
        state_patch: SandboxStatePatch,
    ) -> Result<ApplyResult, RuntimeError> {
        // state_patch must be empty unless this is sandbox build.  Thanks to
        // conditional compilation this always resolves to true so technically
        // the check is not necessary.  It’s defence in depth to make sure any
        // future refactoring won’t break the condition.
        assert!(cfg!(feature = "sandbox") || state_patch.is_empty());

        // What this function does can be broken down conceptually into the following steps:
        // 1. Update validator accounts.
        // 2. Apply migrations.
        // 3. Process transactions.
        // 4. Process receipts.
        // 5. Validate and apply the state update.

        let mut processing_state =
            ApplyProcessingState::new(&apply_state, trie, epoch_info_provider, transactions);

        if let Some(prefetcher) = &mut processing_state.prefetcher {
            // Prefetcher is allowed to fail
            _ = prefetcher.prefetch_transactions_data(transactions);
        }

        // Step 1: update validator accounts.
        if let Some(validator_accounts_update) = validator_accounts_update {
            self.update_validator_accounts(
                &mut processing_state.state_update,
                validator_accounts_update,
                &mut processing_state.stats,
            )?;
        }

        // Step 2: apply migrations.
        let (gas_used_for_migrations, mut receipts_to_restore) = self
            .apply_migrations(
                &mut processing_state.state_update,
                &apply_state.migration_data,
                &apply_state.migration_flags,
                processing_state.protocol_version,
            )
            .map_err(RuntimeError::StorageError)?;
        processing_state.total.add(gas_used_for_migrations, gas_used_for_migrations)?;

        let delayed_receipts = DelayedReceiptQueue::load(&processing_state.state_update)?;
        let delayed_receipts = DelayedReceiptQueueWrapper::new(delayed_receipts);

        // If the chunk is missing, exit early and don't process any receipts.
        if !apply_state.is_new_chunk
            && processing_state.protocol_version
                >= ProtocolFeature::FixApplyChunks.protocol_version()
        {
            return missing_chunk_apply_result(&delayed_receipts, processing_state);
        }

        // If we have receipts that need to be restored, prepend them to the list of incoming receipts
        let incoming_receipts = if receipts_to_restore.is_empty() {
            incoming_receipts
        } else {
            receipts_to_restore.extend_from_slice(incoming_receipts);
            receipts_to_restore.as_slice()
        };

        let mut outgoing_receipts = Vec::new();

        let mut processing_state =
            processing_state.into_processing_receipt_state(incoming_receipts, delayed_receipts);
        let mut own_congestion_info = apply_state.own_congestion_info(
            processing_state.protocol_version,
            &processing_state.state_update,
        )?;
        let mut receipt_sink = ReceiptSink::new(
            processing_state.protocol_version,
            &processing_state.state_update.trie,
            apply_state,
            &mut own_congestion_info,
            &mut outgoing_receipts,
        )?;
        // Forward buffered receipts from previous chunks.
        receipt_sink.forward_from_buffer(&mut processing_state.state_update, apply_state)?;

        // Step 3: process transactions.
        self.process_transactions(&mut processing_state, &mut receipt_sink)?;

        // Step 4: process receipts.
        let process_receipts_result =
            self.process_receipts(&mut processing_state, &mut receipt_sink)?;

        // After receipt processing is done, report metrics on outgoing buffers
        // and on congestion indicators.
        metrics::report_congestion_metrics(
            &receipt_sink,
            apply_state.shard_id,
            &apply_state.config.congestion_control_config,
        );

        // Step 5: validate and apply the state update.
        self.validate_apply_state_update(
            processing_state,
            process_receipts_result,
            own_congestion_info,
            validator_accounts_update,
            state_patch,
            outgoing_receipts,
        )
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

    /// Processes a collection of transactions.
    ///
    /// Fills the `processing_state` with local receipts generated during processing of the
    /// transactions.
    fn process_transactions<'a>(
        &self,
        processing_state: &mut ApplyProcessingReceiptState<'a>,
        receipt_sink: &mut ReceiptSink,
    ) -> Result<(), RuntimeError> {
        let total = &mut processing_state.total;
        let apply_state = &mut processing_state.apply_state;
        let state_update = &mut processing_state.state_update;
        for signed_transaction in processing_state.transactions {
            let (receipt, outcome_with_id) = self.process_transaction(
                state_update,
                apply_state,
                signed_transaction,
                &mut processing_state.stats,
            )?;
            if receipt.receiver_id() == signed_transaction.transaction.signer_id() {
                processing_state.local_receipts.push_back(receipt);
            } else {
                receipt_sink.forward_or_buffer_receipt(
                    receipt,
                    apply_state,
                    state_update,
                    processing_state.epoch_info_provider,
                )?;
            }
            let compute = outcome_with_id.outcome.compute_usage;
            let compute = compute.expect("`process_transaction` must populate compute usage");
            total.add(outcome_with_id.outcome.gas_burnt, compute)?;
            if !checked_feature!("stable", ComputeCosts, processing_state.protocol_version) {
                assert_eq!(total.compute, total.gas, "Compute usage must match burnt gas");
            }
            processing_state.outcomes.push(outcome_with_id);
        }
        processing_state.metrics.tx_processing_done(total.gas, total.compute);
        Ok(())
    }

    /// This function wraps [Runtime::process_receipt]. It adds a tracing span around the latter
    /// and populates various metrics.
    fn process_receipt_with_metrics<'a>(
        &self,
        receipt: &Receipt,
        processing_state: &mut ApplyProcessingReceiptState<'a>,
        mut receipt_sink: &mut ReceiptSink,
        mut validator_proposals: &mut Vec<ValidatorStake>,
    ) -> Result<(), RuntimeError> {
        let span = tracing::debug_span!(
            target: "runtime",
            "process_receipt",
            receipt_id = %receipt.receipt_id(),
            predecessor = %receipt.predecessor_id(),
            receiver = %receipt.receiver_id(),
            gas_burnt = tracing::field::Empty,
            compute_usage = tracing::field::Empty,
        )
        .entered();
        let total = &mut processing_state.total;
        let state_update = &mut processing_state.state_update;
        let node_counter_before = state_update.trie().get_trie_nodes_count();
        let recorded_storage_size_before = state_update.trie().recorded_storage_size();
        let storage_proof_size_upper_bound_before =
            state_update.trie().recorded_storage_size_upper_bound();
        let result = self.process_receipt(
            state_update,
            processing_state.apply_state,
            receipt,
            &mut receipt_sink,
            &mut validator_proposals,
            &mut processing_state.stats,
            processing_state.epoch_info_provider,
        );
        let node_counter_after = state_update.trie().get_trie_nodes_count();
        tracing::trace!(target: "runtime", ?node_counter_before, ?node_counter_after);

        let recorded_storage_diff = state_update
            .trie()
            .recorded_storage_size()
            .saturating_sub(recorded_storage_size_before)
            as f64;
        let recorded_storage_upper_bound_diff = state_update
            .trie()
            .recorded_storage_size_upper_bound()
            .saturating_sub(storage_proof_size_upper_bound_before)
            as f64;
        let shard_id_str = processing_state.apply_state.shard_id.to_string();
        metrics::RECEIPT_RECORDED_SIZE
            .with_label_values(&[shard_id_str.as_str()])
            .observe(recorded_storage_diff);
        metrics::RECEIPT_RECORDED_SIZE_UPPER_BOUND
            .with_label_values(&[shard_id_str.as_str()])
            .observe(recorded_storage_upper_bound_diff);
        let recorded_storage_proof_ratio =
            recorded_storage_upper_bound_diff / f64::max(1.0, recorded_storage_diff);
        // Record the ratio only for large receipts, small receipts can have a very high ratio,
        // but the ratio is not that important for them.
        if recorded_storage_upper_bound_diff > 100_000. {
            metrics::RECEIPT_RECORDED_SIZE_UPPER_BOUND_RATIO
                .with_label_values(&[shard_id_str.as_str()])
                .observe(recorded_storage_proof_ratio);
        }
        if let Some(outcome_with_id) = result? {
            let gas_burnt = outcome_with_id.outcome.gas_burnt;
            let compute_usage = outcome_with_id
                .outcome
                .compute_usage
                .expect("`process_receipt` must populate compute usage");
            total.add(gas_burnt, compute_usage)?;
            span.record("gas_burnt", gas_burnt);
            span.record("compute_usage", compute_usage);

            if !checked_feature!("stable", ComputeCosts, processing_state.protocol_version) {
                assert_eq!(total.compute, total.gas, "Compute usage must match burnt gas");
            }
            processing_state.outcomes.push(outcome_with_id);
        }
        Ok(())
    }

    fn process_local_receipts<'a>(
        &self,
        mut processing_state: &mut ApplyProcessingReceiptState<'a>,
        receipt_sink: &mut ReceiptSink,
        compute_limit: u64,
        proof_size_limit: Option<usize>,
        validator_proposals: &mut Vec<ValidatorStake>,
    ) -> Result<(), RuntimeError> {
        let local_processing_start = std::time::Instant::now();
        let local_receipt_count = processing_state.local_receipts.len();
        if let Some(prefetcher) = &mut processing_state.prefetcher {
            // Prefetcher is allowed to fail
            let (front, back) = processing_state.local_receipts.as_slices();
            _ = prefetcher.prefetch_receipts_data(front);
            _ = prefetcher.prefetch_receipts_data(back);
        }
        while let Some(receipt) = processing_state.next_local_receipt() {
            if processing_state.total.compute >= compute_limit
                || proof_size_limit.is_some_and(|limit| {
                    processing_state.state_update.trie.recorded_storage_size_upper_bound() > limit
                })
            {
                processing_state.delayed_receipts.push(
                    &mut processing_state.state_update,
                    &receipt,
                    &processing_state.apply_state.config,
                )?;
            } else {
                // NOTE: We don't need to validate the local receipt, because it's just validated in
                // the `verify_and_charge_transaction`.
                self.process_receipt_with_metrics(
                    &receipt,
                    &mut processing_state,
                    receipt_sink,
                    validator_proposals,
                )?
            }
        }
        processing_state.metrics.local_receipts_done(
            local_receipt_count as u64,
            local_processing_start.elapsed(),
            processing_state.total.gas,
            processing_state.total.compute,
        );
        Ok(())
    }

    fn process_delayed_receipts<'a>(
        &self,
        mut processing_state: &mut ApplyProcessingReceiptState<'a>,
        receipt_sink: &mut ReceiptSink,
        compute_limit: u64,
        proof_size_limit: Option<usize>,
        validator_proposals: &mut Vec<ValidatorStake>,
    ) -> Result<Vec<Receipt>, RuntimeError> {
        let delayed_processing_start = std::time::Instant::now();
        let protocol_version = processing_state.protocol_version;
        let mut delayed_receipt_count = 0;
        let mut processed_delayed_receipts = vec![];
        while processing_state.delayed_receipts.len() > 0 {
            if processing_state.total.compute >= compute_limit
                || proof_size_limit.is_some_and(|limit| {
                    processing_state.state_update.trie.recorded_storage_size_upper_bound() > limit
                })
            {
                break;
            }
            delayed_receipt_count += 1;
            let receipt = processing_state
                .delayed_receipts
                .pop(&mut processing_state.state_update, &processing_state.apply_state.config)?
                .expect("queue is not empty");

            if let Some(prefetcher) = &mut processing_state.prefetcher {
                // Prefetcher is allowed to fail
                _ = prefetcher.prefetch_receipts_data(std::slice::from_ref(&receipt));
            }

            // Validating the delayed receipt. If it fails, it's likely the state is inconsistent.
            validate_receipt(
                &processing_state.apply_state.config.wasm_config.limit_config,
                &receipt,
                protocol_version,
            )
            .map_err(|e| {
                StorageError::StorageInconsistentState(format!(
                    "Delayed receipt {:?} in the state is invalid: {}",
                    receipt, e
                ))
            })?;

            self.process_receipt_with_metrics(
                &receipt,
                &mut processing_state,
                receipt_sink,
                validator_proposals,
            )?;
            processed_delayed_receipts.push(receipt);
        }
        processing_state.metrics.delayed_receipts_done(
            delayed_receipt_count,
            delayed_processing_start.elapsed(),
            processing_state.total.gas,
            processing_state.total.compute,
        );

        Ok(processed_delayed_receipts)
    }

    fn process_incoming_receipts<'a>(
        &self,
        mut processing_state: &mut ApplyProcessingReceiptState<'a>,
        receipt_sink: &mut ReceiptSink,
        compute_limit: u64,
        proof_size_limit: Option<usize>,
        validator_proposals: &mut Vec<ValidatorStake>,
    ) -> Result<(), RuntimeError> {
        let incoming_processing_start = std::time::Instant::now();
        let protocol_version = processing_state.protocol_version;
        if let Some(prefetcher) = &mut processing_state.prefetcher {
            // Prefetcher is allowed to fail
            _ = prefetcher.prefetch_receipts_data(&processing_state.incoming_receipts);
        }
        for receipt in processing_state.incoming_receipts.iter() {
            // Validating new incoming no matter whether we have available gas or not. We don't
            // want to store invalid receipts in state as delayed.
            validate_receipt(
                &processing_state.apply_state.config.wasm_config.limit_config,
                receipt,
                protocol_version,
            )
            .map_err(RuntimeError::ReceiptValidationError)?;
            if processing_state.total.compute >= compute_limit
                || proof_size_limit.is_some_and(|limit| {
                    processing_state.state_update.trie.recorded_storage_size_upper_bound() > limit
                })
            {
                processing_state.delayed_receipts.push(
                    &mut processing_state.state_update,
                    receipt,
                    &processing_state.apply_state.config,
                )?;
            } else {
                self.process_receipt_with_metrics(
                    &receipt,
                    &mut processing_state,
                    receipt_sink,
                    validator_proposals,
                )?;
            }
        }
        processing_state.metrics.incoming_receipts_done(
            processing_state.incoming_receipts.len() as u64,
            incoming_processing_start.elapsed(),
            processing_state.total.gas,
            processing_state.total.compute,
        );
        Ok(())
    }

    /// Processes all receipts (local, delayed and incoming).
    /// Returns a structure containing the result of the processing.
    fn process_receipts<'a>(
        &self,
        processing_state: &mut ApplyProcessingReceiptState<'a>,
        receipt_sink: &mut ReceiptSink,
    ) -> Result<ProcessReceiptsResult, RuntimeError> {
        let mut validator_proposals = vec![];
        let protocol_version = processing_state.protocol_version;
        let apply_state = &processing_state.apply_state;

        // TODO(#8859): Introduce a dedicated `compute_limit` for the chunk.
        // For now compute limit always matches the gas limit.
        let compute_limit = apply_state.gas_limit.unwrap_or(Gas::max_value());
        let proof_size_limit = if ProtocolFeature::StatelessValidation.enabled(protocol_version) {
            Some(apply_state.config.witness_config.main_storage_proof_size_soft_limit)
        } else {
            None
        };

        // We first process local receipts. They contain staking, local contract calls, etc.
        self.process_local_receipts(
            processing_state,
            receipt_sink,
            compute_limit,
            proof_size_limit,
            &mut validator_proposals,
        )?;

        // Then we process the delayed receipts. It's a backlog of receipts from the past blocks.
        let processed_delayed_receipts = self.process_delayed_receipts(
            processing_state,
            receipt_sink,
            compute_limit,
            proof_size_limit,
            &mut validator_proposals,
        )?;

        // And then we process the new incoming receipts. These are receipts from other shards.
        self.process_incoming_receipts(
            processing_state,
            receipt_sink,
            compute_limit,
            proof_size_limit,
            &mut validator_proposals,
        )?;

        // Resolve timed-out PromiseYield receipts
        let promise_yield_result = resolve_promise_yield_timeouts(
            processing_state,
            receipt_sink,
            compute_limit,
            proof_size_limit,
        )?;

        let shard_id_str = processing_state.apply_state.shard_id.to_string();
        if processing_state.total.compute >= compute_limit {
            metrics::CHUNK_RECEIPTS_LIMITED_BY
                .with_label_values(&[shard_id_str.as_str(), "compute_limit"])
                .inc();
        } else if proof_size_limit.is_some_and(|limit| {
            processing_state.state_update.trie.recorded_storage_size_upper_bound() > limit
        }) {
            metrics::CHUNK_RECEIPTS_LIMITED_BY
                .with_label_values(&[shard_id_str.as_str(), "storage_proof_size_limit"])
                .inc();
        } else {
            metrics::CHUNK_RECEIPTS_LIMITED_BY
                .with_label_values(&[shard_id_str.as_str(), "unlimited"])
                .inc();
        }

        Ok(ProcessReceiptsResult {
            promise_yield_result,
            validator_proposals,
            processed_delayed_receipts,
        })
    }

    fn validate_apply_state_update<'a>(
        &self,
        processing_state: ApplyProcessingReceiptState<'a>,
        process_receipts_result: ProcessReceiptsResult,
        mut own_congestion_info: Option<CongestionInfo>,
        validator_accounts_update: &Option<ValidatorAccountsUpdate>,
        state_patch: SandboxStatePatch,
        outgoing_receipts: Vec<Receipt>,
    ) -> Result<ApplyResult, RuntimeError> {
        let _span = tracing::debug_span!(target: "runtime", "apply_commit").entered();
        let apply_state = processing_state.apply_state;
        let mut state_update = processing_state.state_update;
        let delayed_receipts = processing_state.delayed_receipts;
        let promise_yield_result = process_receipts_result.promise_yield_result;

        if promise_yield_result.promise_yield_indices
            != promise_yield_result.initial_promise_yield_indices
        {
            set(
                &mut state_update,
                TrieKey::PromiseYieldIndices,
                &promise_yield_result.promise_yield_indices,
            );
        }

        // Congestion info needs a final touch to select an allowed shard if
        // this shard is fully congested.

        let delayed_receipts_count = delayed_receipts.len();
        if let Some(congestion_info) = &mut own_congestion_info {
            delayed_receipts.apply_congestion_changes(congestion_info)?;
            let all_shards = apply_state.congestion_info.all_shards();

            let congestion_seed = apply_state.block_height.wrapping_add(apply_state.shard_id);
            congestion_info.finalize_allowed_shard(
                apply_state.shard_id,
                all_shards.as_slice(),
                congestion_seed,
            );
        }

        check_balance(
            &apply_state.config,
            &state_update,
            validator_accounts_update,
            processing_state.incoming_receipts,
            &promise_yield_result.timeout_receipts,
            processing_state.transactions,
            &outgoing_receipts,
            &processing_state.stats,
        )?;

        state_update.commit(StateChangeCause::UpdatedDelayedReceipts);
        self.apply_state_patch(&mut state_update, state_patch);
        let chunk_recorded_size_upper_bound =
            state_update.trie.recorded_storage_size_upper_bound() as f64;
        let shard_id_str = apply_state.shard_id.to_string();
        metrics::CHUNK_RECORDED_SIZE_UPPER_BOUND
            .with_label_values(&[shard_id_str.as_str()])
            .observe(chunk_recorded_size_upper_bound);
        let (trie, trie_changes, state_changes) = state_update.finalize()?;

        if let Some(prefetcher) = &processing_state.prefetcher {
            // Only clear the prefetcher queue after finalize is done because as part of receipt
            // processing we also prefetch account data and access keys that are accessed in
            // finalize. This data can take a very long time otherwise if not prefetched.
            //
            // (This probably results in more data being accessed than strictly necessary and
            // prefetcher may touch data that is no longer relevant as a result but...)
            //
            // In the future it may make sense to have prefetcher have a mode where it has two
            // queues: one for data that is going to be required soon, and the other that it would
            // only work when otherwise idle.
            let discarded_prefetch_requests = prefetcher.clear();
            tracing::debug!(target: "runtime", discarded_prefetch_requests);
        }

        // Dedup proposals from the same account.
        // The order is deterministically changed.
        let mut unique_proposals = vec![];
        let mut account_ids = HashSet::new();
        for proposal in process_receipts_result.validator_proposals.into_iter().rev() {
            let account_id = proposal.account_id();
            if !account_ids.contains(account_id) {
                account_ids.insert(account_id.clone());
                unique_proposals.push(proposal);
            }
        }

        let state_root = trie_changes.new_root;
        let chunk_recorded_size = trie.recorded_storage_size() as f64;
        metrics::CHUNK_RECORDED_SIZE
            .with_label_values(&[shard_id_str.as_str()])
            .observe(chunk_recorded_size);
        metrics::CHUNK_RECORDED_SIZE_UPPER_BOUND_RATIO
            .with_label_values(&[shard_id_str.as_str()])
            .observe(chunk_recorded_size_upper_bound / f64::max(1.0, chunk_recorded_size));
        metrics::report_recorded_column_sizes(&trie, &apply_state);
        let proof = trie.recorded_storage();
        let processed_delayed_receipts = process_receipts_result.processed_delayed_receipts;
        let processed_yield_timeouts = promise_yield_result.processed_yield_timeouts;
        Ok(ApplyResult {
            state_root,
            trie_changes,
            validator_proposals: unique_proposals,
            outgoing_receipts,
            outcomes: processing_state.outcomes,
            state_changes,
            stats: processing_state.stats,
            processed_delayed_receipts,
            processed_yield_timeouts,
            proof,
            delayed_receipts_count,
            metrics: Some(processing_state.metrics),
            congestion_info: own_congestion_info,
        })
    }
}

impl ApplyState {
    fn own_congestion_info(
        &self,
        protocol_version: ProtocolVersion,
        trie: &dyn TrieAccess,
    ) -> Result<Option<CongestionInfo>, RuntimeError> {
        if !ProtocolFeature::CongestionControl.enabled(protocol_version) {
            debug_assert!(self.congestion_info.is_empty());
            return Ok(None);
        }

        if let Some(congestion_info) = self.congestion_info.get(&self.shard_id) {
            return Ok(Some(congestion_info.congestion_info));
        }

        tracing::warn!(target: "runtime", "starting to bootstrap congestion info, this might take a while");
        let start = std::time::Instant::now();
        let result = bootstrap_congestion_info(trie, &self.config, self.shard_id);
        let time = start.elapsed();
        tracing::warn!(target: "runtime","bootstrapping congestion info done after {time:#.1?}");
        let computed = result?;
        Ok(Some(computed))
    }
}

fn action_transfer_or_implicit_account_creation(
    account: &mut Option<Account>,
    deposit: u128,
    nonrefundable: bool,
    is_refund: bool,
    action_receipt: &ActionReceipt,
    receipt: &Receipt,
    state_update: &mut TrieUpdate,
    apply_state: &ApplyState,
    actor_id: &mut AccountId,
    epoch_info_provider: &dyn EpochInfoProvider,
) -> Result<(), RuntimeError> {
    Ok(if let Some(account) = account.as_mut() {
        if nonrefundable {
            assert!(cfg!(feature = "protocol_feature_nonrefundable_transfer_nep491"));
            #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
            action_nonrefundable_storage_transfer(
                account,
                deposit,
                apply_state.config.storage_amount_per_byte(),
            )?;
        } else {
            action_transfer(account, deposit)?;
        }
        // Check if this is a gas refund, then try to refund the access key allowance.
        if is_refund && &action_receipt.signer_id == receipt.receiver_id() {
            try_refund_allowance(
                state_update,
                receipt.receiver_id(),
                &action_receipt.signer_public_key,
                deposit,
            )?;
        }
    } else {
        // Implicit account creation
        debug_assert!(apply_state.config.wasm_config.implicit_account_creation);
        debug_assert!(!is_refund);
        action_implicit_account_creation_transfer(
            state_update,
            &apply_state,
            &apply_state.config.fees,
            account,
            actor_id,
            receipt.receiver_id(),
            deposit,
            apply_state.block_height,
            apply_state.current_protocol_version,
            nonrefundable,
            epoch_info_provider,
        );
    })
}

fn missing_chunk_apply_result(
    delayed_receipts: &DelayedReceiptQueueWrapper,
    processing_state: ApplyProcessingState,
) -> Result<ApplyResult, RuntimeError> {
    let (trie, trie_changes, state_changes) = processing_state.state_update.finalize()?;
    let proof = trie.recorded_storage();

    // For old chunks, copy the congestion info exactly as it came in,
    // potentially returning `None` even if the congestion control
    // feature is enabled for the protocol version.
    let congestion_info = processing_state
        .apply_state
        .congestion_info
        .get(&processing_state.apply_state.shard_id)
        .map(|extended_info| extended_info.congestion_info);

    return Ok(ApplyResult {
        state_root: trie_changes.new_root,
        trie_changes,
        validator_proposals: vec![],
        outgoing_receipts: vec![],
        outcomes: vec![],
        state_changes,
        stats: processing_state.stats,
        processed_delayed_receipts: vec![],
        processed_yield_timeouts: vec![],
        proof,
        delayed_receipts_count: delayed_receipts.len(),
        metrics: None,
        congestion_info,
    });
}

fn resolve_promise_yield_timeouts(
    processing_state: &mut ApplyProcessingReceiptState,
    receipt_sink: &mut ReceiptSink,
    compute_limit: u64,
    proof_size_limit: Option<usize>,
) -> Result<ResolvePromiseYieldTimeoutsResult, RuntimeError> {
    let mut state_update = &mut processing_state.state_update;
    let total = &mut processing_state.total;
    let apply_state = &processing_state.apply_state;

    let mut promise_yield_indices: PromiseYieldIndices =
        get(state_update, &TrieKey::PromiseYieldIndices)?.unwrap_or_default();
    let initial_promise_yield_indices = promise_yield_indices.clone();
    let mut new_receipt_index: usize = 0;

    let mut processed_yield_timeouts = vec![];
    let mut timeout_receipts = vec![];
    let yield_processing_start = std::time::Instant::now();
    while promise_yield_indices.first_index < promise_yield_indices.next_available_index {
        if total.compute >= compute_limit
            || proof_size_limit
                .is_some_and(|limit| state_update.trie.recorded_storage_size_upper_bound() > limit)
        {
            break;
        }

        let queue_entry_key =
            TrieKey::PromiseYieldTimeout { index: promise_yield_indices.first_index };

        let queue_entry =
            get::<PromiseYieldTimeout>(state_update, &queue_entry_key)?.ok_or_else(|| {
                StorageError::StorageInconsistentState(format!(
                    "PromiseYield timeout queue entry #{} should be in the state",
                    promise_yield_indices.first_index
                ))
            })?;

        // Queue entries are ordered by expires_at
        if queue_entry.expires_at > apply_state.block_height {
            break;
        }

        // Check if the yielded promise still needs to be resolved
        let promise_yield_key = TrieKey::PromiseYieldReceipt {
            receiver_id: queue_entry.account_id.clone(),
            data_id: queue_entry.data_id,
        };
        if state_update.contains_key(&promise_yield_key)? {
            let new_receipt_id = create_receipt_id_from_receipt_id(
                processing_state.protocol_version,
                &queue_entry.data_id,
                &apply_state.prev_block_hash,
                &apply_state.block_hash,
                new_receipt_index,
            );
            new_receipt_index += 1;

            // Create a PromiseResume receipt to resolve the timed-out yield.
            let resume_receipt = Receipt::V0(ReceiptV0 {
                predecessor_id: queue_entry.account_id.clone(),
                receiver_id: queue_entry.account_id.clone(),
                receipt_id: new_receipt_id,
                receipt: ReceiptEnum::PromiseResume(DataReceipt {
                    data_id: queue_entry.data_id,
                    data: None,
                }),
            });

            // The receipt is destined for the local shard and will be placed in the outgoing
            // receipts buffer. It is possible that there is already an outgoing receipt resolving
            // this yield if `yield_resume` was invoked by some receipt which was processed in
            // the current chunk. The ordering will be maintained because the receipts are
            // destined for the same shard; the timeout will be processed second and discarded.
            receipt_sink.forward_or_buffer_receipt(
                resume_receipt.clone(),
                apply_state,
                &mut state_update,
                processing_state.epoch_info_provider,
            )?;
            timeout_receipts.push(resume_receipt);
        }

        processed_yield_timeouts.push(queue_entry);
        state_update.remove(queue_entry_key);
        // Math checked above: first_index is less than next_available_index
        promise_yield_indices.first_index += 1;
    }
    processing_state.metrics.yield_timeouts_done(
        processed_yield_timeouts.len() as u64,
        yield_processing_start.elapsed(),
        total.gas,
        total.compute,
    );
    Ok(ResolvePromiseYieldTimeoutsResult {
        timeout_receipts,
        initial_promise_yield_indices,
        promise_yield_indices,
        processed_yield_timeouts,
    })
}

struct TotalResourceGuard {
    gas: u64,
    compute: u64,
    span: tracing::Span,
}

impl Drop for TotalResourceGuard {
    fn drop(&mut self) {
        self.span.record("gas_burnt", self.gas);
        self.span.record("compute_usage", self.compute);
    }
}

impl TotalResourceGuard {
    fn add(&mut self, gas: u64, compute: u64) -> Result<(), IntegerOverflowError> {
        self.gas = safe_add_gas(self.gas, gas)?;
        self.compute = safe_add_compute(self.compute, compute)?;
        Ok(())
    }
}

struct ProcessReceiptsResult {
    promise_yield_result: ResolvePromiseYieldTimeoutsResult,
    validator_proposals: Vec<ValidatorStake>,
    processed_delayed_receipts: Vec<Receipt>,
}

struct ResolvePromiseYieldTimeoutsResult {
    timeout_receipts: Vec<Receipt>,
    initial_promise_yield_indices: PromiseYieldIndices,
    promise_yield_indices: PromiseYieldIndices,
    processed_yield_timeouts: Vec<PromiseYieldTimeout>,
}

/// This struct is a convenient way to hold the processing state during [Runtime::apply].
struct ApplyProcessingState<'a> {
    protocol_version: ProtocolVersion,
    apply_state: &'a ApplyState,
    prefetcher: Option<TriePrefetcher>,
    state_update: TrieUpdate,
    epoch_info_provider: &'a (dyn EpochInfoProvider),
    transactions: &'a [SignedTransaction],
    total: TotalResourceGuard,
    stats: ApplyStats,
}

impl<'a> ApplyProcessingState<'a> {
    fn new(
        apply_state: &'a ApplyState,
        trie: Trie,
        epoch_info_provider: &'a (dyn EpochInfoProvider),
        transactions: &'a [SignedTransaction],
    ) -> Self {
        let protocol_version = apply_state.current_protocol_version;
        let prefetcher = TriePrefetcher::new_if_enabled(&trie);
        let state_update = TrieUpdate::new(trie);
        let total = TotalResourceGuard {
            span: tracing::Span::current(),
            // This contains the gas "burnt" for refund receipts. Even though we don't actually
            // charge any gas for refund receipts, we still count the gas use towards the block gas
            // limit
            gas: 0,
            compute: 0,
        };
        let stats = ApplyStats::default();
        Self {
            protocol_version,
            apply_state,
            prefetcher,
            state_update,
            epoch_info_provider,
            transactions,
            total,
            stats,
        }
    }

    fn into_processing_receipt_state(
        self,
        incoming_receipts: &'a [Receipt],
        delayed_receipts: DelayedReceiptQueueWrapper,
    ) -> ApplyProcessingReceiptState<'a> {
        ApplyProcessingReceiptState {
            protocol_version: self.protocol_version,
            apply_state: self.apply_state,
            prefetcher: self.prefetcher,
            state_update: self.state_update,
            epoch_info_provider: self.epoch_info_provider,
            transactions: self.transactions,
            total: self.total,
            stats: self.stats,
            outcomes: Vec::new(),
            metrics: metrics::ApplyMetrics::default(),
            local_receipts: VecDeque::new(),
            incoming_receipts,
            delayed_receipts,
        }
    }
}

/// Similar to [ApplyProcessingState], with the difference that this contains extra state used
/// by receipt processing.
struct ApplyProcessingReceiptState<'a> {
    protocol_version: ProtocolVersion,
    apply_state: &'a ApplyState,
    prefetcher: Option<TriePrefetcher>,
    state_update: TrieUpdate,
    epoch_info_provider: &'a (dyn EpochInfoProvider),
    transactions: &'a [SignedTransaction],
    total: TotalResourceGuard,
    stats: ApplyStats,
    outcomes: Vec<ExecutionOutcomeWithId>,
    metrics: ApplyMetrics,
    local_receipts: VecDeque<Receipt>,
    incoming_receipts: &'a [Receipt],
    delayed_receipts: DelayedReceiptQueueWrapper,
}

impl<'a> ApplyProcessingReceiptState<'a> {
    /// Obtain the next receipt that should be executed.
    fn next_local_receipt(&mut self) -> Option<Receipt> {
        self.local_receipts.pop_front()
    }
}

/// Interface provided for gas cost estimations.
pub mod estimator {
    use super::{ReceiptSink, Runtime};
    use crate::congestion_control::ReceiptSinkV2;
    use crate::{ApplyState, ApplyStats};
    use near_primitives::congestion_info::CongestionInfo;
    use near_primitives::errors::RuntimeError;
    use near_primitives::receipt::Receipt;
    use near_primitives::transaction::ExecutionOutcomeWithId;
    use near_primitives::types::validator_stake::ValidatorStake;
    use near_primitives::types::EpochInfoProvider;
    use near_store::trie::receipts_column_helper::ShardsOutgoingReceiptBuffer;
    use near_store::TrieUpdate;
    use std::collections::HashMap;

    pub fn apply_action_receipt(
        state_update: &mut TrieUpdate,
        apply_state: &ApplyState,
        receipt: &Receipt,
        outgoing_receipts: &mut Vec<Receipt>,
        validator_proposals: &mut Vec<ValidatorStake>,
        stats: &mut ApplyStats,
        epoch_info_provider: &(dyn EpochInfoProvider),
    ) -> Result<ExecutionOutcomeWithId, RuntimeError> {
        // TODO(congestion_control - edit runtime config parameters for limitless estimator runs
        let mut congestion_info = CongestionInfo::default();
        // no limits set for any shards => limitless
        let outgoing_limit = HashMap::new();

        let mut receipt_sink = ReceiptSink::V2(ReceiptSinkV2 {
            own_congestion_info: &mut congestion_info,
            outgoing_limit,
            outgoing_buffers: ShardsOutgoingReceiptBuffer::load(&state_update.trie)?,
            outgoing_receipts,
        });
        Runtime {}.apply_action_receipt(
            state_update,
            apply_state,
            receipt,
            &mut receipt_sink,
            validator_proposals,
            stats,
            epoch_info_provider,
        )
    }
}

// cspell:ignore contractregistry

use crate::actions::*;
use crate::config::{
    exec_fee, safe_add_balance, safe_add_compute, safe_add_gas, safe_gas_to_balance, total_deposit,
    total_prepaid_exec_fees, total_prepaid_gas,
};
use crate::congestion_control::DelayedReceiptQueueWrapper;
use crate::prefetch::TriePrefetcher;
pub use crate::types::SignedValidPeriodTransactions;
use crate::verifier::{StorageStakingError, check_storage_stake, validate_receipt};
pub use crate::verifier::{
    ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT, get_signer_and_access_key, set_tx_state_changes,
    validate_transaction, verify_and_charge_tx_ephemeral,
};
use bandwidth_scheduler::{BandwidthSchedulerOutput, run_bandwidth_scheduler};
use config::{TransactionCost, total_prepaid_send_fees, tx_cost};
use congestion_control::ReceiptSink;
pub use congestion_control::bootstrap_congestion_info;
use global_contracts::{
    action_deploy_global_contract, action_use_global_contract,
    apply_global_contract_distribution_receipt,
};
use itertools::Itertools;
use metrics::ApplyMetrics;
pub use near_crypto;
use near_parameters::{ActionCosts, RuntimeConfig};
pub use near_primitives;
use near_primitives::account::Account;
use near_primitives::bandwidth_scheduler::{BandwidthRequests, BlockBandwidthRequests};
use near_primitives::chunk_apply_stats::ChunkApplyStatsV0;
use near_primitives::congestion_info::{BlockCongestionInfo, CongestionInfo};
use near_primitives::errors::{
    ActionError, ActionErrorKind, EpochError, IntegerOverflowError, InvalidTxError, RuntimeError,
    TxExecutionError,
};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    ActionReceipt, DataReceipt, PromiseYieldIndices, PromiseYieldTimeout, Receipt, ReceiptEnum,
    ReceiptOrStateStoredReceipt, ReceiptV0, ReceivedData,
};
use near_primitives::sandbox::state_patch::SandboxStatePatch;
use near_primitives::state_record::StateRecord;
use near_primitives::stateless_validation::contract_distribution::ContractUpdates;
use near_primitives::transaction::{
    Action, ExecutionMetadata, ExecutionOutcome, ExecutionOutcomeWithId, ExecutionStatus, LogEntry,
    SignedTransaction, TransferAction, ValidatedTransaction,
};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{
    AccountId, Balance, BlockHeight, Compute, EpochHeight, EpochId, EpochInfoProvider, Gas,
    RawStateChangesWithTrieKey, ShardId, StateChangeCause, StateRoot,
    validator_stake::ValidatorStake,
};
use near_primitives::utils::{
    create_action_hash_from_receipt_id, create_receipt_id_from_receipt_id,
    create_receipt_id_from_transaction,
};
use near_primitives::version::ProtocolVersion;
use near_primitives_core::apply::ApplyChunkReason;
use near_primitives_core::version::ProtocolFeature;
use near_store::trie::AccessOptions;
use near_store::trie::receipts_column_helper::DelayedReceiptQueue;
use near_store::trie::update::TrieUpdateResult;
use near_store::{
    PartialStorage, StorageError, Trie, TrieAccess, TrieChanges, TrieUpdate, get, get_account,
    get_postponed_receipt, get_promise_yield_receipt, get_pure, get_received_data,
    has_received_data, remove_postponed_receipt, remove_promise_yield_receipt, set, set_access_key,
    set_account, set_postponed_receipt, set_promise_yield_receipt, set_received_data,
};
use near_vm_runner::ContractCode;
use near_vm_runner::ContractRuntimeCache;
use near_vm_runner::ProfileDataV3;
use near_vm_runner::logic::ReturnData;
use near_vm_runner::logic::types::PromiseResult;
pub use near_vm_runner::with_ext_cost_counter;
use pipelining::ReceiptPreparationPipeline;
use rayon::prelude::*;
use std::cmp::max;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tracing::{debug, instrument};
use verifier::ValidateReceiptMode;

mod actions;
pub mod adapter;
mod bandwidth_scheduler;
pub mod config;
mod congestion_control;
mod conversions;
pub mod ext;
mod global_contracts;
pub mod metrics;
mod pipelining;
mod prefetch;
pub mod receipt_manager;
pub mod state_viewer;
#[cfg(test)]
mod tests;
mod types;
mod verifier;

const EXPECT_ACCOUNT_EXISTS: &str = "account exists, checked above";

#[derive(Debug)]
pub struct ApplyState {
    /// Points to a phase of the chain lifecycle that we want to run apply for.
    pub apply_reason: ApplyChunkReason,
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
    /// Cache for trie node accesses.
    pub trie_access_tracker_state: Arc<ext::AccountingState>,
    /// Whether the chunk being applied is new.
    pub is_new_chunk: bool,
    /// Congestion level on each shard based on the latest known chunk header of each shard.
    ///
    /// The map must be empty if congestion control is disabled in the previous
    /// chunk. If the next chunks is the first with congestion control enabled,
    /// the congestion info needs to be computed while applying receipts.
    /// TODO(congestion_info) - verify performance of initialization when congested
    pub congestion_info: BlockCongestionInfo,
    /// Bandwidth requests from all shards, generated at the previous height.
    /// Each shard requests some bandwidth to other shards and then the bandwidth scheduler
    /// decides how much each shard is allowed to send.
    pub bandwidth_requests: BlockBandwidthRequests,
}

impl ApplyState {
    pub fn create_receipt_id(
        &self,
        parent_receipt_id: &CryptoHash,
        receipt_index: usize,
    ) -> CryptoHash {
        create_receipt_id_from_receipt_id(
            self.current_protocol_version,
            parent_receipt_id,
            &self.block_hash,
            self.block_height,
            receipt_index,
        )
    }
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

#[derive(Debug)]
pub struct ApplyResult {
    pub state_root: StateRoot,
    pub trie_changes: TrieChanges,
    pub validator_proposals: Vec<ValidatorStake>,
    pub outgoing_receipts: Vec<Receipt>,
    pub outcomes: Vec<ExecutionOutcomeWithId>,
    pub state_changes: Vec<RawStateChangesWithTrieKey>,
    pub stats: ChunkApplyStatsV0,
    pub processed_delayed_receipts: Vec<Receipt>,
    pub processed_yield_timeouts: Vec<PromiseYieldTimeout>,
    pub proof: Option<PartialStorage>,
    pub delayed_receipts_count: u64,
    pub metrics: Option<metrics::ApplyMetrics>,
    pub congestion_info: Option<CongestionInfo>,
    pub bandwidth_requests: BandwidthRequests,
    /// Used only for a sanity check.
    pub bandwidth_scheduler_state_hash: CryptoHash,
    /// Contracts accessed and deployed while applying the chunk.
    pub contract_updates: ContractUpdates,
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

/// Lists the balance differences between
#[derive(Debug, Default)]
pub struct GasRefundResult {
    /// The deficit due to increased gas prices since receipt creation.
    pub price_deficit: Balance,
    /// The surplus due to decreased gas prices since receipt creation.
    pub price_surplus: Balance,
    /// The penalty paid for left over gas
    pub refund_penalty: Balance,
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
            if acc.is_empty() { s.to_string() } else { acc + "\n" + s }
        });
        debug!(target: "runtime", "{}", log_str);
    }

    fn parallel_validate_transactions(
        config: &RuntimeConfig,
        gas_price: Balance,
        signed_txs: impl IntoParallelIterator<Item = SignedTransaction>,
        current_protocol_version: ProtocolVersion,
    ) -> Vec<(CryptoHash, Result<(ValidatedTransaction, TransactionCost), InvalidTxError>)> {
        signed_txs
            .into_par_iter()
            .map(|signed_tx| {
                (
                    signed_tx.get_hash(),
                    match validate_transaction(config, signed_tx, current_protocol_version) {
                        Ok(validated_tx) => {
                            match tx_cost(
                                config,
                                &validated_tx.to_tx(),
                                gas_price,
                                current_protocol_version,
                            ) {
                                Ok(cost) => Ok((validated_tx, cost)),
                                Err(e) => Err(InvalidTxError::from(e)),
                            }
                        }
                        Err((e, _tx)) => Err(e),
                    },
                )
            })
            .collect()
    }

    /// Takes one signed transaction, verifies it and converts it to a receipt.
    ///
    /// Add the produced receipt either to the new local receipts if the signer is the same
    /// as receiver or to the new outgoing receipts.
    ///
    /// When transaction is converted to a receipt, the account is charged for the full value of
    /// the generated receipt.
    ///
    /// In case of successful verification, returns the receipt and `ExecutionOutcomeWithId` for
    /// the transaction.
    ///
    /// In case of an error, returns either `InvalidTxError` if the transaction verification failed
    /// or a `StorageError` wrapped into `RuntimeError`.
    #[instrument(target = "runtime", level = "debug", "process_transaction", skip_all, fields(
        tx_hash = %validated_tx.get_hash(),
        gas_burnt = tracing::field::Empty,
        compute_usage = tracing::field::Empty,
    ))]
    fn process_transaction(
        &self,
        state_update: &mut TrieUpdate,
        apply_state: &ApplyState,
        validated_tx: &ValidatedTransaction,
        transaction_cost: &TransactionCost,
        stats: &mut ChunkApplyStatsV0,
    ) -> Result<(Receipt, ExecutionOutcomeWithId), InvalidTxError> {
        let span = tracing::Span::current();
        metrics::TRANSACTION_PROCESSED_TOTAL.inc();
        let (mut signer, mut access_key) = get_signer_and_access_key(state_update, &validated_tx)?;

        let verification_result = verify_and_charge_tx_ephemeral(
            &apply_state.config,
            &mut signer,
            &mut access_key,
            validated_tx,
            transaction_cost,
            Some(apply_state.block_height),
        );
        let verification_result = match verification_result {
            Ok(ok) => ok,
            Err(e) => {
                metrics::TRANSACTION_PROCESSED_FAILED_TOTAL.inc();
                state_update.rollback();
                return Err(e);
            }
        };

        metrics::TRANSACTION_PROCESSED_SUCCESSFULLY_TOTAL.inc();
        set_tx_state_changes(state_update, validated_tx, &signer, &access_key);
        state_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: validated_tx.get_hash() });
        let receipt_id = create_receipt_id_from_transaction(
            apply_state.current_protocol_version,
            validated_tx.to_hash(),
            &apply_state.block_hash,
            apply_state.block_height,
        );
        let receipt = Receipt::V0(ReceiptV0 {
            predecessor_id: validated_tx.signer_id().clone(),
            receiver_id: validated_tx.receiver_id().clone(),
            receipt_id,
            receipt: ReceiptEnum::Action(ActionReceipt {
                signer_id: validated_tx.signer_id().clone(),
                signer_public_key: validated_tx.public_key().clone(),
                gas_price: verification_result.receipt_gas_price,
                output_data_receivers: vec![],
                input_data_ids: vec![],
                actions: validated_tx.actions().to_vec(),
            }),
        });

        match safe_add_balance(stats.balance.tx_burnt_amount, verification_result.burnt_amount) {
            Ok(new) => stats.balance.tx_burnt_amount = new,
            Err(_) => return Err(InvalidTxError::CostOverflow),
        }
        let gas_burnt = verification_result.gas_burnt;
        let compute_usage = verification_result.gas_burnt;
        let outcome = ExecutionOutcomeWithId {
            id: validated_tx.get_hash(),
            outcome: ExecutionOutcome {
                status: ExecutionStatus::SuccessReceiptId(*receipt.receipt_id()),
                logs: vec![],
                receipt_ids: vec![*receipt.receipt_id()],
                gas_burnt,
                // TODO(#8806): Support compute costs for actions. For now they match burnt gas.
                compute_usage: Some(compute_usage),
                tokens_burnt: verification_result.burnt_amount,
                executor_id: validated_tx.signer_id().clone(),
                // TODO: profile data is only counted in apply_action, which only happened at process_receipt
                // VerificationResult needs updates to incorporate profile data to support profile data of txns
                metadata: ExecutionMetadata::V1,
            },
        };
        span.record("gas_burnt", gas_burnt);
        span.record("compute_usage", compute_usage);
        Ok((receipt, outcome))
    }

    fn apply_action(
        &self,
        action: &Action,
        state_update: &mut TrieUpdate,
        apply_state: &ApplyState,
        preparation_pipeline: &ReceiptPreparationPipeline,
        account: &mut Option<Account>,
        actor_id: &mut AccountId,
        receipt: &Receipt,
        action_receipt: &ActionReceipt,
        promise_results: Arc<[PromiseResult]>,
        action_hash: &CryptoHash,
        action_index: usize,
        actions: &[Action],
        epoch_info_provider: &dyn EpochInfoProvider,
        stats: &mut ChunkApplyStatsV0,
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

        // Account validation
        if let Err(e) = check_account_existence(
            action,
            account,
            account_id,
            &apply_state.config,
            implicit_account_creation_eligible,
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
                );
            }
            Action::DeployContract(deploy_contract) => {
                action_deploy_contract(
                    state_update,
                    account.as_mut().expect(EXPECT_ACCOUNT_EXISTS),
                    account_id,
                    deploy_contract,
                    Arc::clone(&apply_state.config.wasm_config),
                    apply_state.cache.as_deref(),
                    apply_state.current_protocol_version,
                )?;
            }
            Action::DeployGlobalContract(deploy_global_contract) => {
                let account = account.as_mut().expect(EXPECT_ACCOUNT_EXISTS);
                action_deploy_global_contract(
                    account,
                    account_id,
                    apply_state,
                    deploy_global_contract,
                    &mut result,
                    stats,
                )?;
            }
            Action::UseGlobalContract(use_global_contract) => {
                let account = account.as_mut().expect(EXPECT_ACCOUNT_EXISTS);
                action_use_global_contract(
                    state_update,
                    account_id,
                    account,
                    use_global_contract,
                    apply_state.current_protocol_version,
                    &mut result,
                )?;
            }
            Action::FunctionCall(function_call) => {
                let account = account.as_mut().expect(EXPECT_ACCOUNT_EXISTS);
                let account_contract = account.contract();
                let code_hash =
                    state_update.get_account_contract_hash(account_contract.as_ref())?;
                let contract =
                    preparation_pipeline.get_contract(receipt, code_hash, action_index, None);
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
                    code_hash,
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
        preparation_pipeline: &ReceiptPreparationPipeline,
        receipt: &Receipt,
        receipt_sink: &mut ReceiptSink,
        validator_proposals: &mut Vec<ValidatorStake>,
        stats: &mut ChunkApplyStatsV0,
        epoch_info_provider: &dyn EpochInfoProvider,
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

        // Executing actions one by one
        for (action_index, action) in action_receipt.actions.iter().enumerate() {
            let action_hash = create_action_hash_from_receipt_id(
                apply_state.current_protocol_version,
                receipt.receipt_id(),
                &apply_state.block_hash,
                apply_state.block_height,
                action_index,
            );
            let mut new_result = self.apply_action(
                action,
                state_update,
                apply_state,
                preparation_pipeline,
                &mut account,
                &mut actor_id,
                receipt,
                action_receipt,
                Arc::clone(&promise_results),
                &action_hash,
                action_index,
                &action_receipt.actions,
                epoch_info_provider,
                stats,
            )?;
            if new_result.result.is_ok() {
                if let Err(e) = new_result.new_receipts.iter().try_for_each(|receipt| {
                    validate_receipt(
                        &apply_state.config.wasm_config.limit_config,
                        receipt,
                        apply_state.current_protocol_version,
                        ValidateReceiptMode::NewReceipt,
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
                match check_storage_stake(account, &apply_state.config) {
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
                        ));
                    }
                }
            }
        }

        let gas_refund_result = if receipt.predecessor_id().is_system() {
            // If the refund fails tokens are burned.
            if result.result.is_err() {
                stats.balance.other_burnt_amount = safe_add_balance(
                    stats.balance.other_burnt_amount,
                    total_deposit(&action_receipt.actions)?,
                )?
            }
            GasRefundResult::default()
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
        stats.balance.gas_deficit_amount =
            safe_add_balance(stats.balance.gas_deficit_amount, gas_refund_result.price_deficit)?;

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
            if receipt.predecessor_id().is_system() { 0 } else { result.gas_burnt };
        // `price_deficit` is strictly less than `gas_price * gas_burnt`.
        let mut tx_burnt_amount = safe_gas_to_balance(apply_state.gas_price, gas_burnt)?
            - gas_refund_result.price_deficit;
        tx_burnt_amount = safe_add_balance(tx_burnt_amount, gas_refund_result.price_surplus)?;
        tx_burnt_amount = safe_add_balance(tx_burnt_amount, gas_refund_result.refund_penalty)?;
        // The amount of tokens burnt for the execution of this receipt. It's used in the execution
        // outcome.
        let tokens_burnt = tx_burnt_amount;

        // Adding burnt gas reward for function calls if the account exists.
        let receiver_gas_reward = result.gas_burnt_for_function_call
            * *apply_state.config.fees.burnt_gas_reward.numer() as u64
            / *apply_state.config.fees.burnt_gas_reward.denom() as u64;
        // The balance that the current account should receive as a reward for function call
        // execution.
        let receiver_reward = if apply_state.config.fees.refund_gas_price_changes {
            // Use current gas price for reward calculation
            let full_reward = safe_gas_to_balance(apply_state.gas_price, receiver_gas_reward)?;
            // Pre NEP-536:
            // When refunding the gas price difference, if we run a deficit,
            // subtract it from contract rewards. This is a (arguably weird) bit
            // of cross-financing the missing funds to pay gas at the current
            // rate.
            // We should charge the caller more but we can't at this point. The
            // pessimistic gas pricing was not pessimistic enough, which may
            // happen when receipts are delayed.
            // To recover the losses, take as much as we can from the reward
            // that rightfully belongs to the contract owner.
            full_reward.saturating_sub(gas_refund_result.price_deficit)
        } else {
            // Use receipt gas price for reward calculation
            safe_gas_to_balance(action_receipt.gas_price, receiver_gas_reward)?
            // Post NEP-536:
            // No shenanigans here. We are not refunding gas price differences,
            // we just use the receipt gas price and call it the correct price.
            // No deficits to try and recover.
        };
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

        stats.balance.tx_burnt_amount =
            safe_add_balance(stats.balance.tx_burnt_amount, tx_burnt_amount)?;

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
                    ReceiptEnum::Action(new_action_receipt)
                    | ReceiptEnum::PromiseYield(new_action_receipt) => new_action_receipt
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
                let receipt_id = apply_state.create_receipt_id(receipt.receipt_id(), receipt_index);
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
            Ok(ReturnData::ReceiptIndex(receipt_index)) => ExecutionStatus::SuccessReceiptId(
                apply_state.create_receipt_id(receipt.receipt_id(), receipt_index as usize),
            ),
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
    ) -> Result<GasRefundResult, RuntimeError> {
        if config.fees.refund_gas_price_changes {
            let price_deficit = self.refund_unspent_gas_and_unspent_gas_and_deposits(
                current_gas_price,
                receipt,
                action_receipt,
                result,
                config,
            )?;
            Ok(GasRefundResult { price_deficit, price_surplus: 0, refund_penalty: 0 })
        } else {
            self.refund_unspent_gas_and_deposits(
                current_gas_price,
                receipt,
                action_receipt,
                result,
                config,
            )
        }
    }

    /// How we used to handle refunds, prior to NEP-536.
    ///
    /// In the old model, we tried to always bill the user the exact gas price
    /// of the block where the gas is spent. That means, a transaction uses a
    /// different gas price on every hop. But gas is purchased all at the start,
    /// at the receipt gas price.
    ///
    /// To deal with a price increase during execution, we charged a pessimistic
    /// gas price. The pessimistic price is an estimation of how expensive gas
    /// could realistically become while the transaction executes. It's not a
    /// guaranteed to stay below that limit, though.
    ///
    /// The pessimistic price is usually several times higher than the real
    /// execution price. Thus, it is important to refund the difference between
    /// the purchase price and the execution price.
    ///
    /// NEP-536 removes this concept because we no longer want to waste runtime
    /// throughput with a somewhat useless refund receipts for essentially every
    /// function call.
    fn refund_unspent_gas_and_unspent_gas_and_deposits(
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

    /// How we handle refunds since NEP-536.
    ///
    /// In this model, the user purchases gas at one price. This price stays the
    /// same for the entire execution of the transaction, even if the blockchain
    /// price changes.
    ///
    /// In this configuration, gas price changes do not affect refunds, either.
    /// Thus, we only create refunds for unspent gas and for deposits.
    fn refund_unspent_gas_and_deposits(
        &self,
        current_gas_price: Balance,
        receipt: &Receipt,
        action_receipt: &ActionReceipt,
        result: &mut ActionResult,
        config: &RuntimeConfig,
    ) -> Result<GasRefundResult, RuntimeError> {
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
        let gross_gas_refund = if result.result.is_err() {
            safe_add_gas(prepaid_gas, prepaid_exec_gas)? - result.gas_burnt
        } else {
            safe_add_gas(prepaid_gas, prepaid_exec_gas)? - result.gas_used
        };

        // NEP-536 also adds a penalty to gas refund.
        let refund_penalty: Gas = config.fees.gas_penalty_for_gas_refund(gross_gas_refund);
        let Some(net_gas_refund) = gross_gas_refund.checked_sub(refund_penalty) else {
            // violation of gas_penalty_for_gas_refund post condition
            panic!("returned larger penalty than input, {refund_penalty} > {gross_gas_refund}",);
        };

        // Refund for the unused portion of the gas at the price at which this gas was purchased.
        let gas_balance_refund = safe_gas_to_balance(action_receipt.gas_price, net_gas_refund)?;

        let mut gas_refund_result = GasRefundResult {
            price_deficit: 0,
            price_surplus: 0,
            refund_penalty: safe_gas_to_balance(action_receipt.gas_price, refund_penalty)?,
        };

        if current_gas_price > action_receipt.gas_price {
            // price increased, burning resulted in a deficit
            gas_refund_result.price_deficit = safe_gas_to_balance(
                current_gas_price - action_receipt.gas_price,
                result.gas_burnt,
            )?;
        } else {
            // price decreased, burning resulted in a surplus
            gas_refund_result.price_surplus = safe_gas_to_balance(
                action_receipt.gas_price - current_gas_price,
                result.gas_burnt,
            )?;
        };

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

        Ok(gas_refund_result)
    }

    fn process_receipt(
        &self,
        processing_state: &mut ApplyProcessingReceiptState,
        receipt: &Receipt,
        receipt_sink: &mut ReceiptSink,
        validator_proposals: &mut Vec<ValidatorStake>,
    ) -> Result<Option<ExecutionOutcomeWithId>, RuntimeError> {
        let ApplyProcessingReceiptState {
            ref mut state_update,
            apply_state,
            epoch_info_provider,
            ref pipeline_manager,
            ref mut stats,
            ..
        } = *processing_state;
        let account_id = receipt.receiver_id();
        match receipt.receipt() {
            ReceiptEnum::Data(data_receipt) => {
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
                                pipeline_manager,
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
            ReceiptEnum::Action(action_receipt) => {
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
                            pipeline_manager,
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
            ReceiptEnum::PromiseResume(data_receipt) => {
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
                            pipeline_manager,
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
            ReceiptEnum::GlobalContractDistribution(_) => {
                apply_global_contract_distribution_receipt(
                    receipt,
                    apply_state,
                    epoch_info_provider,
                    state_update,
                    receipt_sink,
                )?;
                return Ok(None);
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

    /// Applies new signed transactions and incoming receipts for some chunk/shard on top of
    /// given trie and the given state root.
    ///
    /// If the validator accounts update is provided, updates validators accounts.
    ///
    /// Returns an `ApplyResult` that contains the new state root, trie changes,
    /// new outgoing receipts, execution outcomes for all transactions, local action receipts
    /// (generated from transactions with signer == receivers) and incoming action receipts.
    ///
    /// Invalid transactions should have been filtered out by the chunk producer, but if a chunk
    /// containing invalid transactions does make it to here, these transactions are skipped. This
    /// does pollute the chain with junk data, but it also allows the protocol to make progress, as
    /// the only alternative way to handle these transactions is to make the entire chunk invalid.
    #[instrument(target = "runtime", level = "debug", "apply", skip_all, fields(
        protocol_version = apply_state.current_protocol_version,
        num_transactions = signed_txs.len(),
        gas_burnt = tracing::field::Empty,
        compute_usage = tracing::field::Empty,
    ))]
    pub fn apply(
        &self,
        trie: Trie,
        validator_accounts_update: &Option<ValidatorAccountsUpdate>,
        apply_state: &ApplyState,
        incoming_receipts: &[Receipt],
        signed_txs: SignedValidPeriodTransactions,
        epoch_info_provider: &dyn EpochInfoProvider,
        state_patch: SandboxStatePatch,
    ) -> Result<ApplyResult, RuntimeError> {
        metrics::TRANSACTION_APPLIED_TOTAL.inc_by(signed_txs.len() as u64);

        // state_patch must be empty unless this is sandbox build.  Thanks to
        // conditional compilation this always resolves to true so technically
        // the check is not necessary.  It’s defense in depth to make sure any
        // future refactoring won’t break the condition.
        assert!(cfg!(feature = "sandbox") || state_patch.is_empty());

        // What this function does can be broken down conceptually into the following steps:
        // 1. Update validator accounts.
        // 2. Process transactions.
        // 3. Process receipts.
        // 4. Validate and apply the state update.
        let mut processing_state =
            ApplyProcessingState::new(&apply_state, trie, epoch_info_provider);
        processing_state.stats.transactions_num = signed_txs.len().try_into().unwrap();
        processing_state.stats.incoming_receipts_num = incoming_receipts.len().try_into().unwrap();
        processing_state.stats.is_new_chunk = !apply_state.is_new_chunk;

        if let Some(prefetcher) = &mut processing_state.prefetcher {
            // Prefetcher is allowed to fail
            _ = prefetcher.prefetch_transactions_data(&signed_txs);
        }

        // Step 1: update validator accounts.
        if let Some(validator_accounts_update) = validator_accounts_update {
            self.update_validator_accounts(
                &mut processing_state.state_update,
                validator_accounts_update,
            )?;
        }

        let delayed_receipts = DelayedReceiptQueueWrapper::new(
            DelayedReceiptQueue::load(&processing_state.state_update)?,
            epoch_info_provider,
            apply_state.shard_id,
            apply_state.epoch_id,
        );

        // Bandwidth scheduler should be run for every chunk, including the missing ones.
        let bandwidth_scheduler_output = run_bandwidth_scheduler(
            apply_state,
            &mut processing_state.state_update,
            epoch_info_provider,
            &mut processing_state.stats.bandwidth_scheduler,
        )?;

        // If the chunk is missing, exit early and don't process any receipts.
        if !apply_state.is_new_chunk {
            return missing_chunk_apply_result(
                &delayed_receipts,
                processing_state,
                &bandwidth_scheduler_output,
            );
        }

        let mut processing_state =
            processing_state.into_processing_receipt_state(incoming_receipts, delayed_receipts);
        let own_congestion_info =
            apply_state.own_congestion_info(&processing_state.state_update)?;
        let mut receipt_sink = ReceiptSink::new(
            &processing_state.state_update.trie,
            apply_state,
            own_congestion_info,
            bandwidth_scheduler_output,
        )?;
        // Forward buffered receipts from previous chunks.
        receipt_sink.forward_from_buffer(
            &mut processing_state.state_update,
            apply_state,
            processing_state.epoch_info_provider,
        )?;

        // Step 2: process transactions.
        self.process_transactions(&mut processing_state, signed_txs, &mut receipt_sink)?;

        // Step 3: process receipts.
        let process_receipts_result =
            self.process_receipts(&mut processing_state, &mut receipt_sink)?;

        // After receipt processing is done, report metrics on outgoing buffers
        // and on congestion indicators.
        metrics::report_congestion_metrics(
            &receipt_sink,
            apply_state.shard_id,
            &apply_state.config.congestion_control_config,
        );

        // Step 4: validate and apply the state update.
        self.validate_apply_state_update(
            processing_state,
            process_receipts_result,
            receipt_sink,
            state_patch,
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
                    state_update.set(
                        TrieKey::ContractData { key: data_key.into(), account_id },
                        value.into(),
                    );
                }
                StateRecord::Contract { account_id, code } => {
                    let acc = get_account(state_update, &account_id).expect("Failed to read state").expect("Code state record should be preceded by the corresponding account record");
                    // Recompute contract code hash.
                    let code = ContractCode::new(code, None);
                    state_update.set_code(account_id, &code);
                    assert_eq!(*code.hash(), acc.contract().local_code().unwrap_or_default());
                }
                StateRecord::AccessKey { account_id, public_key, access_key } => {
                    set_access_key(state_update, account_id, public_key, &access_key);
                }
                _ => unimplemented!(
                    "patch_state can only patch Account, AccessKey, Contract and Data kind of StateRecord"
                ),
            }
        }
        state_update.commit(StateChangeCause::Migration);
    }

    /// Processes a collection of transactions.
    ///
    /// Fills the `processing_state` with local receipts generated during processing of the
    /// transactions.
    ///
    /// Any transactions that fail to validate (e.g. invalid nonces, unknown signing keys,
    /// insufficient NEAR balance, etc.) will be skipped, producing no receipts.
    fn process_transactions(
        &self,
        processing_state: &mut ApplyProcessingReceiptState,
        signed_txs: SignedValidPeriodTransactions,
        receipt_sink: &mut ReceiptSink,
    ) -> Result<(), RuntimeError> {
        let total = &mut processing_state.total;
        let apply_state = &mut processing_state.apply_state;
        let state_update = &mut processing_state.state_update;

        let signed_txs = signed_txs.into_par_iter_nonexpired_transactions();
        for (tx_hash, result) in Self::parallel_validate_transactions(
            &apply_state.config,
            apply_state.gas_price,
            signed_txs,
            apply_state.current_protocol_version,
        ) {
            match result {
                Ok((validated_tx, cost)) => {
                    let (receipt, outcome_with_id) = match self.process_transaction(
                        state_update,
                        apply_state,
                        &validated_tx,
                        &cost,
                        &mut processing_state.stats,
                    ) {
                        Ok(outcome) => outcome,
                        Err(err) => {
                            tracing::debug!(
                                target: "runtime",
                                ?tx_hash,
                                ?err,
                                "invalid transaction ignored (process_transaction error)",
                            );
                            continue;
                        }
                    };
                    if receipt.receiver_id() == validated_tx.signer_id() {
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
                    let compute =
                        compute.expect("`process_transaction` must populate compute usage");
                    total.add(outcome_with_id.outcome.gas_burnt, compute)?;
                    processing_state.outcomes.push(outcome_with_id);
                }
                Err(err) => {
                    tracing::debug!(
                        target: "runtime",
                        ?tx_hash,
                        ?err,
                        "invalid transaction ignored (parallel validation error)",
                    );
                }
            }
        }
        processing_state.metrics.tx_processing_done(total.gas, total.compute);
        Ok(())
    }

    /// This function wraps [Runtime::process_receipt]. It adds a tracing span around the latter
    /// and populates various metrics.
    fn process_receipt_with_metrics(
        &self,
        receipt: &Receipt,
        processing_state: &mut ApplyProcessingReceiptState,
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

        let state_update = &mut processing_state.state_update;
        let trie = state_update.trie();
        let recorded_storage_size_before = trie.recorded_storage_size();
        let storage_proof_size_upper_bound_before = trie.recorded_storage_size_upper_bound();

        // Main logic
        let result = self.process_receipt(
            processing_state,
            receipt,
            &mut receipt_sink,
            &mut validator_proposals,
        );

        let shard_id_str = processing_state.apply_state.shard_id.to_string();
        let trie = processing_state.state_update.trie();

        let recorded_storage_diff = trie.recorded_storage_size() - recorded_storage_size_before;
        let recorded_storage_upper_bound_diff =
            trie.recorded_storage_size_upper_bound() - storage_proof_size_upper_bound_before;
        metrics::RECEIPT_RECORDED_SIZE
            .with_label_values(&[shard_id_str.as_str()])
            .observe(recorded_storage_diff as f64);
        metrics::RECEIPT_RECORDED_SIZE_UPPER_BOUND
            .with_label_values(&[shard_id_str.as_str()])
            .observe(recorded_storage_upper_bound_diff as f64);
        let recorded_storage_proof_ratio =
            recorded_storage_upper_bound_diff as f64 / f64::max(1.0, recorded_storage_diff as f64);
        // Record the ratio only for large receipts, small receipts can have a very high ratio,
        // but the ratio is not that important for them.
        if recorded_storage_upper_bound_diff > 100_000 {
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
            let total = &mut processing_state.total;
            total.add(gas_burnt, compute_usage)?;
            span.record("gas_burnt", gas_burnt);
            span.record("compute_usage", compute_usage);

            processing_state.outcomes.push(outcome_with_id);
        }
        Ok(())
    }

    #[instrument(target = "runtime", level = "debug", "process_local_receipts", skip_all, fields(
        num_receipts = processing_state.local_receipts.len(),
        gas_burnt = tracing::field::Empty,
        compute_usage = tracing::field::Empty,
    ))]
    fn process_local_receipts(
        &self,
        mut processing_state: &mut ApplyProcessingReceiptState,
        receipt_sink: &mut ReceiptSink,
        compute_limit: u64,
        validator_proposals: &mut Vec<ValidatorStake>,
    ) -> Result<(), RuntimeError> {
        let local_processing_start = std::time::Instant::now();
        let local_receipt_count = processing_state.local_receipts.len();
        let local_receipts = std::mem::take(&mut processing_state.local_receipts);
        if let Some(prefetcher) = &mut processing_state.prefetcher {
            // Prefetcher is allowed to fail
            let (front, back) = local_receipts.as_slices();
            _ = prefetcher.prefetch_receipts_data(front);
            _ = prefetcher.prefetch_receipts_data(back);
        }

        let mut prep_lookahead_iter = local_receipts.iter();
        // Advance the preparation by one step (stagger it) so that we're preparing one interesting
        // receipt in advance.
        let mut next_schedule_after = schedule_contract_preparation(
            &mut processing_state.pipeline_manager,
            &processing_state.state_update,
            &mut prep_lookahead_iter,
        );

        for receipt in &local_receipts {
            if processing_state.total.compute >= compute_limit
                || processing_state.state_update.trie.check_proof_size_limit_exceed()
            {
                processing_state.delayed_receipts.push(
                    &mut processing_state.state_update,
                    &receipt,
                    &processing_state.apply_state,
                )?;
            } else {
                if let Some(nsi) = &mut next_schedule_after {
                    *nsi = nsi.saturating_sub(1);
                    if *nsi == 0 {
                        // We're about to process a receipt that has been submitted for
                        // preparation, so lets submit the next one in anticipation that it might
                        // be processed too (it might also be not if we run out of gas/compute.)
                        next_schedule_after = schedule_contract_preparation(
                            &mut processing_state.pipeline_manager,
                            &processing_state.state_update,
                            &mut prep_lookahead_iter,
                        );
                    }
                }
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

        let span = tracing::Span::current();
        span.record("gas_burnt", processing_state.total.gas);
        span.record("compute_usage", processing_state.total.compute);
        processing_state.metrics.local_receipts_done(
            local_receipt_count as u64,
            local_processing_start.elapsed(),
            processing_state.total.gas,
            processing_state.total.compute,
        );
        Ok(())
    }

    #[instrument(
        target = "runtime",
        level = "debug",
        "process_delayed_receipts",
        skip_all,
        fields(num_receipts = processing_state.delayed_receipts.upper_bound_len(), gas_burnt, compute_usage)
    )]
    fn process_delayed_receipts(
        &self,
        mut processing_state: &mut ApplyProcessingReceiptState,
        receipt_sink: &mut ReceiptSink,
        compute_limit: u64,
        validator_proposals: &mut Vec<ValidatorStake>,
    ) -> Result<Vec<Receipt>, RuntimeError> {
        let delayed_processing_start = std::time::Instant::now();
        let protocol_version = processing_state.protocol_version;
        let mut delayed_receipt_count = 0;
        let mut processed_delayed_receipts = vec![];

        let mut next_schedule_after = {
            let mut prep_lookahead_iter =
                processing_state.delayed_receipts.peek_iter(&processing_state.state_update);
            schedule_contract_preparation(
                &mut processing_state.pipeline_manager,
                &processing_state.state_update,
                &mut prep_lookahead_iter,
            )
        };

        loop {
            if processing_state.total.compute >= compute_limit
                || processing_state.state_update.trie.check_proof_size_limit_exceed()
            {
                break;
            }

            let receipt = if let Some(receipt) = processing_state
                .delayed_receipts
                .pop(&mut processing_state.state_update, &processing_state.apply_state.config)?
            {
                receipt.into_receipt()
            } else {
                // Break loop if there are no more receipts to be processed.
                break;
            };

            // TODO(resharding): Add metric for tracking number of
            delayed_receipt_count += 1;
            if let Some(nsi) = &mut next_schedule_after {
                *nsi = nsi.saturating_sub(1);
                if *nsi == 0 {
                    let mut prep_lookahead_iter =
                        processing_state.delayed_receipts.peek_iter(&processing_state.state_update);
                    next_schedule_after = schedule_contract_preparation(
                        &mut processing_state.pipeline_manager,
                        &processing_state.state_update,
                        &mut prep_lookahead_iter,
                    );
                }
            }

            if let Some(prefetcher) = &mut processing_state.prefetcher {
                // Prefetcher is allowed to fail
                _ = prefetcher.prefetch_receipts_data(std::slice::from_ref(&receipt));
            }

            // Validating the delayed receipt. If it fails, it's likely the state is inconsistent.
            validate_receipt(
                &processing_state.apply_state.config.wasm_config.limit_config,
                &receipt,
                protocol_version,
                ValidateReceiptMode::ExistingReceipt,
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
        let span = tracing::Span::current();
        span.record("gas_burnt", processing_state.total.gas);
        span.record("compute_usage", processing_state.total.compute);
        processing_state.metrics.delayed_receipts_done(
            delayed_receipt_count,
            delayed_processing_start.elapsed(),
            processing_state.total.gas,
            processing_state.total.compute,
        );

        Ok(processed_delayed_receipts)
    }

    #[instrument(target = "runtime", level = "debug", "process_incoming_receipts", skip_all, fields(
        num_receipts = processing_state.incoming_receipts.len(),
        gas_burnt = tracing::field::Empty,
        compute_usage = tracing::field::Empty,
    ))]
    fn process_incoming_receipts(
        &self,
        mut processing_state: &mut ApplyProcessingReceiptState,
        receipt_sink: &mut ReceiptSink,
        compute_limit: u64,
        validator_proposals: &mut Vec<ValidatorStake>,
    ) -> Result<(), RuntimeError> {
        let incoming_processing_start = std::time::Instant::now();
        let protocol_version = processing_state.protocol_version;
        if let Some(prefetcher) = &mut processing_state.prefetcher {
            // Prefetcher is allowed to fail
            _ = prefetcher.prefetch_receipts_data(&processing_state.incoming_receipts);
        }

        let mut prep_lookahead_iter = processing_state.incoming_receipts.iter();
        // Advance the preparation by one step (stagger it) so that we're preparing one interesting
        // receipt in advance.
        let mut next_schedule_after = schedule_contract_preparation(
            &mut processing_state.pipeline_manager,
            &processing_state.state_update,
            &mut prep_lookahead_iter,
        );

        for receipt in processing_state.incoming_receipts {
            // Validating new incoming no matter whether we have available gas or not. We don't
            // want to store invalid receipts in state as delayed.
            validate_receipt(
                &processing_state.apply_state.config.wasm_config.limit_config,
                receipt,
                protocol_version,
                ValidateReceiptMode::ExistingReceipt,
            )
            .map_err(RuntimeError::ReceiptValidationError)?;
            if processing_state.total.compute >= compute_limit
                || processing_state.state_update.trie.check_proof_size_limit_exceed()
            {
                processing_state.delayed_receipts.push(
                    &mut processing_state.state_update,
                    receipt,
                    &processing_state.apply_state,
                )?;
            } else {
                if let Some(nsi) = &mut next_schedule_after {
                    *nsi = nsi.saturating_sub(1);
                    if *nsi == 0 {
                        // We're about to process a receipt that has been submitted for
                        // preparation, so lets submit the next one in anticipation that it might
                        // be processed too (it might also be not if we run out of gas/compute.)
                        next_schedule_after = schedule_contract_preparation(
                            &mut processing_state.pipeline_manager,
                            &processing_state.state_update,
                            &mut prep_lookahead_iter,
                        );
                    }
                }

                self.process_receipt_with_metrics(
                    &receipt,
                    &mut processing_state,
                    receipt_sink,
                    validator_proposals,
                )?;
            }
        }
        let span = tracing::Span::current();
        span.record("gas_burnt", processing_state.total.gas);
        span.record("compute_usage", processing_state.total.compute);
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
    fn process_receipts(
        &self,
        processing_state: &mut ApplyProcessingReceiptState,
        receipt_sink: &mut ReceiptSink,
    ) -> Result<ProcessReceiptsResult, RuntimeError> {
        let mut validator_proposals = vec![];
        let apply_state = &processing_state.apply_state;

        // TODO(#8859): Introduce a dedicated `compute_limit` for the chunk.
        // For now compute limit always matches the gas limit.
        let compute_limit = apply_state.gas_limit.unwrap_or(Gas::max_value());

        // We first process local receipts. They contain staking, local contract calls, etc.
        self.process_local_receipts(
            processing_state,
            receipt_sink,
            compute_limit,
            &mut validator_proposals,
        )?;

        // Then we process the delayed receipts. It's a backlog of receipts from the past blocks.
        let processed_delayed_receipts = self.process_delayed_receipts(
            processing_state,
            receipt_sink,
            compute_limit,
            &mut validator_proposals,
        )?;

        // And then we process the new incoming receipts. These are receipts from other shards.
        self.process_incoming_receipts(
            processing_state,
            receipt_sink,
            compute_limit,
            &mut validator_proposals,
        )?;

        // Resolve timed-out PromiseYield receipts
        let promise_yield_result =
            resolve_promise_yield_timeouts(processing_state, receipt_sink, compute_limit)?;

        let shard_id_str = processing_state.apply_state.shard_id.to_string();
        if processing_state.total.compute >= compute_limit {
            metrics::CHUNK_RECEIPTS_LIMITED_BY
                .with_label_values(&[shard_id_str.as_str(), "compute_limit"])
                .inc();
        } else if processing_state.state_update.trie.check_proof_size_limit_exceed() {
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

    fn validate_apply_state_update(
        &self,
        processing_state: ApplyProcessingReceiptState,
        process_receipts_result: ProcessReceiptsResult,
        receipt_sink: ReceiptSink,
        state_patch: SandboxStatePatch,
    ) -> Result<ApplyResult, RuntimeError> {
        let _span = tracing::debug_span!(target: "runtime", "apply_commit").entered();
        let apply_state = processing_state.apply_state;
        let epoch_info_provider = processing_state.epoch_info_provider;
        let mut stats = processing_state.stats;
        let mut state_update = processing_state.state_update;
        let protocol_version = apply_state.current_protocol_version;
        let pending_delayed_receipts = processing_state.delayed_receipts;
        let processed_delayed_receipts = process_receipts_result.processed_delayed_receipts;
        let promise_yield_result = process_receipts_result.promise_yield_result;
        let shard_layout = epoch_info_provider.shard_layout(&apply_state.epoch_id)?;

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
        let delayed_receipts_count = pending_delayed_receipts.upper_bound_len();
        let mut own_congestion_info = receipt_sink.own_congestion_info();
        pending_delayed_receipts.apply_congestion_changes(&mut own_congestion_info)?;

        let (all_shards, shard_seed) =
            if ProtocolFeature::SimpleNightshadeV4.enabled(protocol_version) {
                let shard_ids = shard_layout.shard_ids().collect_vec();
                let shard_index = shard_layout
                    .get_shard_index(apply_state.shard_id)
                    .map_err(Into::<EpochError>::into)?
                    .try_into()
                    .expect("Shard Index must fit within u64");

                (shard_ids, shard_index)
            } else {
                (apply_state.congestion_info.all_shards(), apply_state.shard_id.into())
            };

        let congestion_seed = apply_state.block_height.wrapping_add(shard_seed);
        own_congestion_info.finalize_allowed_shard(
            apply_state.shard_id,
            &all_shards,
            congestion_seed,
        );

        let bandwidth_requests = receipt_sink.generate_bandwidth_requests(
            &state_update,
            &shard_layout,
            true,
            &mut stats,
        )?;

        state_update.commit(StateChangeCause::UpdatedDelayedReceipts);
        self.apply_state_patch(&mut state_update, state_patch);
        let chunk_recorded_size_upper_bound =
            state_update.trie.recorded_storage_size_upper_bound() as f64;
        let shard_id_str = apply_state.shard_id.to_string();
        metrics::CHUNK_RECORDED_SIZE_UPPER_BOUND
            .with_label_values(&[shard_id_str.as_str()])
            .observe(chunk_recorded_size_upper_bound);
        let TrieUpdateResult { trie, trie_changes, state_changes, contract_updates } =
            state_update.finalize()?;

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
        let processed_yield_timeouts = promise_yield_result.processed_yield_timeouts;
        let bandwidth_scheduler_state_hash =
            receipt_sink.bandwidth_scheduler_output().scheduler_state_hash;

        let outgoing_receipts =
            receipt_sink.finalize_stats_get_outgoing_receipts(&mut stats.receipt_sink);
        Ok(ApplyResult {
            state_root,
            trie_changes,
            validator_proposals: unique_proposals,
            outgoing_receipts,
            outcomes: processing_state.outcomes,
            state_changes,
            stats,
            processed_delayed_receipts,
            processed_yield_timeouts,
            proof,
            delayed_receipts_count,
            metrics: Some(processing_state.metrics),
            congestion_info: Some(own_congestion_info),
            bandwidth_requests,
            bandwidth_scheduler_state_hash,
            contract_updates,
        })
    }
}

impl ApplyState {
    fn own_congestion_info(&self, trie: &dyn TrieAccess) -> Result<CongestionInfo, RuntimeError> {
        if let Some(congestion_info) = self.congestion_info.get(&self.shard_id) {
            return Ok(congestion_info.congestion_info);
        }

        tracing::warn!(target: "runtime", "starting to bootstrap congestion info, this might take a while");
        let start = std::time::Instant::now();
        let result = bootstrap_congestion_info(trie, &self.config, self.shard_id);
        let time = start.elapsed();
        tracing::warn!(target: "runtime","bootstrapping congestion info done after {time:#.1?}");
        let computed = result?;
        Ok(computed)
    }
}

fn action_transfer_or_implicit_account_creation(
    account: &mut Option<Account>,
    deposit: u128,
    is_refund: bool,
    action_receipt: &ActionReceipt,
    receipt: &Receipt,
    state_update: &mut TrieUpdate,
    apply_state: &ApplyState,
    actor_id: &mut AccountId,
    epoch_info_provider: &dyn EpochInfoProvider,
) -> Result<(), RuntimeError> {
    Ok(if let Some(account) = account.as_mut() {
        action_transfer(account, deposit)?;
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
            epoch_info_provider,
        );
    })
}

fn missing_chunk_apply_result(
    delayed_receipts: &DelayedReceiptQueueWrapper,
    processing_state: ApplyProcessingState,
    bandwidth_scheduler_output: &BandwidthSchedulerOutput,
) -> Result<ApplyResult, RuntimeError> {
    let TrieUpdateResult { trie, trie_changes, state_changes, contract_updates } =
        processing_state.state_update.finalize()?;
    let proof = trie.recorded_storage();

    // For old chunks, copy the congestion info exactly as it came in,
    // potentially returning `None` even if the congestion control
    // feature is enabled for the protocol version.
    let congestion_info = processing_state
        .apply_state
        .congestion_info
        .get(&processing_state.apply_state.shard_id)
        .map(|extended_info| extended_info.congestion_info);

    // The chunk is missing and doesn't send out any receipts.
    // It still wants to send the same receipts to the same shards, the bandwidth requests are the same.
    let previous_bandwidth_requests = processing_state
        .apply_state
        .bandwidth_requests
        .shards_bandwidth_requests
        .get(&processing_state.apply_state.shard_id)
        .cloned()
        .unwrap_or_else(BandwidthRequests::empty);

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
        delayed_receipts_count: delayed_receipts.upper_bound_len(),
        metrics: None,
        congestion_info,
        bandwidth_requests: previous_bandwidth_requests,
        bandwidth_scheduler_state_hash: bandwidth_scheduler_output.scheduler_state_hash,
        contract_updates,
    });
}

fn resolve_promise_yield_timeouts(
    processing_state: &mut ApplyProcessingReceiptState,
    receipt_sink: &mut ReceiptSink,
    compute_limit: u64,
) -> Result<ResolvePromiseYieldTimeoutsResult, RuntimeError> {
    let mut state_update = &mut processing_state.state_update;
    let total = &mut processing_state.total;
    let apply_state = &processing_state.apply_state;

    let mut promise_yield_indices: PromiseYieldIndices =
        get(state_update, &TrieKey::PromiseYieldIndices)?.unwrap_or_default();
    let initial_promise_yield_indices = promise_yield_indices.clone();
    let mut new_receipt_index: usize = 0;

    let mut processed_yield_timeouts = vec![];
    let yield_processing_start = std::time::Instant::now();
    while promise_yield_indices.first_index < promise_yield_indices.next_available_index {
        if total.compute >= compute_limit || state_update.trie.check_proof_size_limit_exceed() {
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
        if state_update.contains_key(&promise_yield_key, AccessOptions::DEFAULT)? {
            let new_receipt_id = create_receipt_id_from_receipt_id(
                processing_state.protocol_version,
                &queue_entry.data_id,
                &apply_state.block_hash,
                apply_state.block_height,
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
                resume_receipt,
                apply_state,
                &mut state_update,
                processing_state.epoch_info_provider,
            )?;
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
    epoch_info_provider: &'a dyn EpochInfoProvider,
    total: TotalResourceGuard,
    stats: ChunkApplyStatsV0,
}

impl<'a> ApplyProcessingState<'a> {
    fn new(
        apply_state: &'a ApplyState,
        trie: Trie,
        epoch_info_provider: &'a dyn EpochInfoProvider,
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
        let stats = ChunkApplyStatsV0::new(apply_state.block_height, apply_state.shard_id);
        Self {
            protocol_version,
            apply_state,
            prefetcher,
            state_update,
            epoch_info_provider,
            total,
            stats,
        }
    }

    fn into_processing_receipt_state(
        self,
        incoming_receipts: &'a [Receipt],
        delayed_receipts: DelayedReceiptQueueWrapper<'a>,
    ) -> ApplyProcessingReceiptState<'a> {
        let pipeline_manager = pipelining::ReceiptPreparationPipeline::new(
            Arc::clone(&self.apply_state.config),
            self.apply_state.cache.as_ref().map(|v| v.handle()),
            self.state_update.contract_storage(),
        );
        ApplyProcessingReceiptState {
            pipeline_manager,
            protocol_version: self.protocol_version,
            apply_state: self.apply_state,
            prefetcher: self.prefetcher,
            state_update: self.state_update,
            epoch_info_provider: self.epoch_info_provider,
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
    epoch_info_provider: &'a dyn EpochInfoProvider,
    total: TotalResourceGuard,
    stats: ChunkApplyStatsV0,
    outcomes: Vec<ExecutionOutcomeWithId>,
    metrics: ApplyMetrics,
    local_receipts: VecDeque<Receipt>,
    incoming_receipts: &'a [Receipt],
    delayed_receipts: DelayedReceiptQueueWrapper<'a>,
    pipeline_manager: pipelining::ReceiptPreparationPipeline,
}

trait MaybeRefReceipt {
    fn as_ref(&self) -> &Receipt;
}

impl MaybeRefReceipt for Receipt {
    fn as_ref(&self) -> &Receipt {
        self
    }
}

impl<'a> MaybeRefReceipt for &'a Receipt {
    fn as_ref(&self) -> &Receipt {
        *self
    }
}

impl MaybeRefReceipt for ReceiptOrStateStoredReceipt<'_> {
    fn as_ref(&self) -> &Receipt {
        self.get_receipt()
    }
}

impl<'a> MaybeRefReceipt for &'a ReceiptOrStateStoredReceipt<'a> {
    fn as_ref(&self) -> &Receipt {
        self.get_receipt()
    }
}

/// Schedule a one receipt for contract preparation.
///
/// The caller should call this method again after the returned number of receipts from `iterator`
/// are processed.
fn schedule_contract_preparation<R: MaybeRefReceipt>(
    pipeline_manager: &mut pipelining::ReceiptPreparationPipeline,
    state_update: &TrieUpdate,
    mut iterator: impl Iterator<Item = R>,
) -> Option<usize> {
    let scheduled_receipt_offset = iterator.position(|peek| {
        let peek = peek.as_ref();
        let account_id = peek.receiver_id();
        // We need to inspect each receipt recursively in case these are data receipts, thus a
        // function.
        fn handle_receipt(
            mgr: &mut ReceiptPreparationPipeline,
            state_update: &TrieUpdate,
            account_id: &AccountId,
            receipt: &Receipt,
        ) -> bool {
            match receipt.receipt() {
                ReceiptEnum::Action(_) | ReceiptEnum::PromiseYield(_) => {
                    // This returns `true` if work may have been scheduled (thus we currently
                    // prepare actions in at most 2 "interesting" receipts in parallel due to
                    // staggering.)
                    mgr.submit(receipt, state_update, None)
                }
                ReceiptEnum::Data(dr) => {
                    let key = TrieKey::PostponedReceiptId {
                        receiver_id: account_id.clone(),
                        data_id: dr.data_id,
                    };
                    let Ok(Some(rid)) = get_pure::<CryptoHash>(state_update, &key) else {
                        return false;
                    };
                    let key = TrieKey::PendingDataCount {
                        receiver_id: account_id.clone(),
                        receipt_id: rid,
                    };
                    let Ok(Some(data_count)) = get_pure::<u32>(state_update, &key) else {
                        return false;
                    };
                    if data_count > 1 {
                        return false;
                    }
                    let key = TrieKey::PostponedReceipt {
                        receiver_id: account_id.clone(),
                        receipt_id: rid,
                    };
                    let Ok(Some(pr)) = get_pure::<Receipt>(state_update, &key) else {
                        return false;
                    };
                    return handle_receipt(mgr, state_update, account_id, &pr);
                }
                ReceiptEnum::PromiseResume(dr) => {
                    let key = TrieKey::PromiseYieldReceipt {
                        receiver_id: account_id.clone(),
                        data_id: dr.data_id,
                    };
                    let Ok(Some(yr)) = get_pure::<Receipt>(state_update, &key) else {
                        return false;
                    };
                    return handle_receipt(mgr, state_update, account_id, &yr);
                }
                ReceiptEnum::GlobalContractDistribution(_) => false,
            }
        }
        handle_receipt(pipeline_manager, state_update, account_id, peek)
    })?;
    Some(scheduled_receipt_offset.saturating_add(1))
}

#[cfg(feature = "estimator")]
/// Interface provided for gas cost estimations.
pub mod estimator {
    use super::{ReceiptSink, Runtime};
    use crate::ApplyState;
    use crate::BandwidthSchedulerOutput;
    use crate::congestion_control::ReceiptSinkV2;
    use crate::pipelining::ReceiptPreparationPipeline;
    use near_primitives::bandwidth_scheduler::BandwidthSchedulerParams;
    use near_primitives::chunk_apply_stats::{ChunkApplyStatsV0, ReceiptSinkStats};
    use near_primitives::congestion_info::CongestionInfo;
    use near_primitives::errors::RuntimeError;
    use near_primitives::receipt::Receipt;
    use near_primitives::transaction::ExecutionOutcomeWithId;
    use near_primitives::types::EpochInfoProvider;
    use near_primitives::types::validator_stake::ValidatorStake;
    use near_store::trie::outgoing_metadata::{OutgoingMetadatas, ReceiptGroupsConfig};
    use near_store::trie::receipts_column_helper::ShardsOutgoingReceiptBuffer;
    use near_store::{ShardUId, TrieUpdate};
    use std::collections::HashMap;
    use std::num::NonZeroU64;

    pub fn apply_action_receipt(
        state_update: &mut TrieUpdate,
        apply_state: &ApplyState,
        receipt: &Receipt,
        outgoing_receipts: &mut Vec<Receipt>,
        validator_proposals: &mut Vec<ValidatorStake>,
        stats: &mut ChunkApplyStatsV0,
        epoch_info_provider: &dyn EpochInfoProvider,
    ) -> Result<ExecutionOutcomeWithId, RuntimeError> {
        // TODO(congestion_control - edit runtime config parameters for limitless estimator runs
        let congestion_info = CongestionInfo::default();
        // no limits set for any shards => limitless
        // TODO(bandwidth_scheduler) - now empty map means all limits are zero, fix.
        let outgoing_limit = HashMap::new();

        // ShardId used in EstimatorContext::testbed
        let shard_uid: ShardUId = ShardUId::single_shard();
        let outgoing_metadatas = OutgoingMetadatas::load(
            state_update,
            std::iter::once(shard_uid.shard_id()),
            ReceiptGroupsConfig::default_config(),
        )?;

        let shard_layout = epoch_info_provider.shard_layout(&apply_state.epoch_id)?;
        let params = BandwidthSchedulerParams::new(
            NonZeroU64::new(shard_layout.num_shards()).expect("ShardLayout has zero shards!"),
            &apply_state.config,
        );

        let mut receipt_sink = ReceiptSink::V2(ReceiptSinkV2 {
            own_congestion_info: congestion_info,
            outgoing_limit,
            outgoing_buffers: ShardsOutgoingReceiptBuffer::load(&state_update.trie)?,
            outgoing_receipts: Vec::new(),
            outgoing_metadatas,
            bandwidth_scheduler_output: BandwidthSchedulerOutput::no_granted_bandwidth(params),
            stats: ReceiptSinkStats::default(),
        });
        let empty_pipeline = ReceiptPreparationPipeline::new(
            std::sync::Arc::clone(&apply_state.config),
            apply_state.cache.as_ref().map(|c| c.handle()),
            state_update.contract_storage(),
        );
        let apply_result = Runtime {}.apply_action_receipt(
            state_update,
            apply_state,
            &empty_pipeline,
            receipt,
            &mut receipt_sink,
            validator_proposals,
            stats,
            epoch_info_provider,
        );
        let new_outgoing_receipts =
            receipt_sink.finalize_stats_get_outgoing_receipts(&mut stats.receipt_sink);
        outgoing_receipts.extend(new_outgoing_receipts.into_iter());
        apply_result
    }
}

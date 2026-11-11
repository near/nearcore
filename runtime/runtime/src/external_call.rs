//! Running external contract calls (ECCs) while applying a chunk.
//!
//! An external call runs the contract's reserved method in `ExecutionMode::External`, with the
//! call's payload as input. A clean return accepts the call: the run's writes stay, its promises
//! become receipts, and the contract pays for the call's fees and the gas the run used. Any
//! failure refuses the call: the caller rolls the run's writes back and nobody pays.
//!
//! `process_external_call` takes a call through all its steps, from the check of the chunk's
//! shares for ECCs to counting what it used toward them. It doesn't depend on the chunk's
//! processing state, so the chunk producer can call it to run a call before packing it, and
//! make the same decision as apply.

use crate::config::{
    EccCost, ecc_cost, receipt_gas_price, safe_add_balance, safe_add_compute, safe_gas_to_balance,
};
use crate::contract_preparation::{
    ContractPreparation, prepare_contract_metadata, prepare_function_call,
};
use crate::conversions::Convert;
use crate::ext::RuntimeExt;
use crate::function_call::{
    NewReceiptSigner, execute_function_call, new_receipts, record_contract_call,
};
use crate::receipt_manager::ReceiptManager;
use crate::universal_account_id::install_universal_state_init;
use crate::verifier::{
    StorageStakingError, ValidateReceiptMode, check_storage_stake, ecc_cost_for_account,
    state_init_to_install, validate_external_call, validate_receipt, verify_ecc_ephemeral,
};
use crate::{ActionResult, ApplyState, PrevBlockContext};
use ahash::RandomState as AHashRandomState;
use bytesize::ByteSize;
use dashmap::DashMap;
use near_parameters::RuntimeConfig;
use near_primitives::account::{Account, AccountContract};
use near_primitives::errors::{
    ActionError, ActionErrorKind, ExternalCallChunkLimit, IntegerOverflowError, InvalidTxError,
    RuntimeError, StorageError, TxExecutionError,
};
use near_primitives::hash::CryptoHash;
use near_primitives::profile_data_v3::ProfileDataV3;
use near_primitives::receipt::{AuthorizationContext, Receipt, VersionedReceiptEnum};
use near_primitives::transaction::{
    ECC_METHOD_NAME, EccTransaction, ExecutionMetadata, ExecutionMetadataV4, ExecutionOutcome,
    ExecutionOutcomeWithId, ExecutionStatus,
};
use near_primitives::types::{
    AccountId, Balance, BlockHeight, Compute, EpochId, EpochInfoProvider, Gas, ShardId,
    StateChangeCause, VmGeneration,
};
use near_primitives::utils::{
    create_action_hash_from_receipt_id, create_ecc_outcome_id, create_receipt_id_from_receipt_id,
};
use near_primitives::version::ProtocolVersion;
use near_store::trie::AccessOptions;
use near_store::{TrieUpdate, get_account, set_account};
use near_vm_runner::logic::errors::FunctionCallError;
use near_vm_runner::logic::{ExecutionMode, HostError, ReturnData, VMContext, VMOutcome};
use near_vm_runner::{CompilePriority, ContractRuntimeCache};
use std::borrow::Cow;
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::Arc;

/// What the external calls of a chunk used so far, against their shares of the chunk's compute
/// and storage proof, which `process_external_call` checks before each call and adds to after
/// it. Only that function checks and adds, so apply and the chunk producer admit the same calls.
///
/// It also holds the contracts the chunk's calls went to, so that a second call to one of them
/// gets a failed outcome. The other rules on a chunk's external calls, a limit on their number and
/// a share of the chunk's transaction bytes, make a chunk invalid, and are checked before apply.
#[derive(Debug, Clone)]
pub struct ExternalCallsInChunk {
    compute: Compute,
    recorded_size: u64,
    /// The share of the chunk's compute limit. `Compute::MAX` when the chunk has no gas limit,
    /// as apply's own compute limit is then.
    compute_share: Compute,
    /// The share of `main_storage_proof_size_soft_limit`.
    recorded_size_share: u64,
    called_contracts: HashSet<AccountId>,
}

impl ExternalCallsInChunk {
    /// Nothing used yet, in a chunk with `chunk_gas_limit`, which is its compute limit.
    pub fn new(config: &RuntimeConfig, chunk_gas_limit: Option<Gas>) -> Self {
        let calls_config = &config.external_calls_config;
        let compute_share =
            chunk_gas_limit.map_or(Compute::MAX, |gas_limit| calls_config.compute_share(gas_limit));
        Self {
            compute: 0,
            recorded_size: 0,
            compute_share,
            recorded_size_share: calls_config.storage_proof_share(&config.witness_config),
            called_contracts: HashSet::new(),
        }
    }

    /// What the calls recorded so far.
    #[cfg(test)]
    pub(crate) fn recorded_size(&self) -> u64 {
        self.recorded_size
    }

    /// Whether the chunk has room left in every share, so that `process_external_call` takes the
    /// next call. What a call uses is known only once it ran, so a chunk can pass each share by
    /// at most one call.
    pub fn has_room(&self) -> bool {
        self.fits().is_ok()
    }

    fn fits(&self) -> Result<(), InvalidTxError> {
        let (used, limit) = (self.compute, self.compute_share);
        if used >= limit {
            let share = ExternalCallChunkLimit::Compute { used, limit };
            return Err(InvalidTxError::ExternalCallChunkShareReached { share });
        }
        let (used, limit) = (self.recorded_size, self.recorded_size_share);
        if used >= limit {
            let share = ExternalCallChunkLimit::StorageProof { used, limit };
            return Err(InvalidTxError::ExternalCallChunkShareReached { share });
        }
        Ok(())
    }

    /// Counts a call that passed `fits`, whatever came of it: the compute it used and the
    /// storage proof it recorded, the read of its contract account included.
    fn add(&mut self, compute: Compute, recorded_size: u64) {
        self.compute = self.compute.saturating_add(compute);
        self.recorded_size = self.recorded_size.saturating_add(recorded_size);
    }
}

/// What came of an external contract call, for the caller to record.
pub struct ProcessedExternalCall {
    /// The call's outcome. The gas, compute and tokens burnt it reports are what the call counts
    /// toward the chunk: gas and tokens only if the call is accepted, and compute in any case
    /// except a call refused past the chunk's shares for ECCs.
    pub outcome: ExecutionOutcomeWithId,
    /// Whether the contract accepted and paid for the call.
    pub accepted: bool,
    /// The receipts an accepted call sends, with their ids, for the caller to send out.
    pub receipts: Vec<Receipt>,
    /// Whether the call installed its state init, which it does only on an uninitialized account.
    pub installed_state_init: bool,
}

/// Processes an external contract call of a chunk: checks the chunk's shares for ECCs, reads
/// the contract account, runs the call, and counts what it used toward the shares. Apply calls
/// it for each call in the chunk, in order, and so is to the chunk producer, so that both make
/// the same decision. It records nothing about the chunk: the caller records the outcome and
/// counts it toward the chunk's gas, compute and burnt tokens.
///
/// A call that failed validation (`validation_error`) is refused without anything read for it,
/// and so is a call that finds the chunk's ECCs past one of their shares (`external_calls`), or a
/// second call to a contract the chunk called already.
/// Otherwise the contract account is read into `accounts`, the chunk's working copy of accounts,
/// unless it is there already. A call whose contract can't pay its fees, or whose account has no
/// contract, is refused without running. Otherwise the contract's reserved method runs. If the
/// contract accepts the call, the run's writes are committed and the contract's account (both in
/// `accounts` and in `state_update`) is charged. Otherwise the run's writes are rolled back.
/// Changes staged in `state_update` before the call are committed either way.
///
/// If the call is refused, before or after the run, nobody pays. The validators' work on it
/// still counts toward the chunk's compute, so it stays bounded, but not toward the chunk's gas:
/// since nobody pays for it, it shouldn't move the next gas price. The exceptions are a second
/// call to a contract and a call refused past a share, which count no compute: see below. Every call that passes the check of
/// the shares counts toward them, whatever comes of it.
///
/// A `RuntimeError`, such as a storage error, stops applying the chunk.
pub fn process_external_call<'a>(
    state_update: &mut TrieUpdate,
    apply_state: &ApplyState,
    epoch_info_provider: &dyn EpochInfoProvider,
    external_calls: &mut ExternalCallsInChunk,
    accounts: &DashMap<&'a AccountId, Result<Option<Account>, StorageError>, AHashRandomState>,
    ecc: &'a EccTransaction,
    validation_error: Option<InvalidTxError>,
) -> Result<ProcessedExternalCall, RuntimeError> {
    let outcome_id = create_ecc_outcome_id(ecc.hash_without_state_init(), apply_state.block_height);
    let refuse = |error: TxExecutionError, compute: Compute| ProcessedExternalCall {
        outcome: ExecutionOutcomeWithId::refused_ecc(outcome_id, ecc, error, compute),
        accepted: false,
        receipts: vec![],
        installed_state_init: false,
    };
    let cost = match ecc_cost(&apply_state.config, ecc, apply_state.gas_price) {
        Ok(cost) => cost,
        // Without fees there is nothing to count toward the chunk's limits.
        Err(err) => {
            let err = validation_error.unwrap_or_else(|| err.into());
            return Ok(refuse(err.into(), 0));
        }
    };
    if let Some(err) = validation_error {
        return Ok(refuse(err.into(), cost.compute_burnt));
    }
    // The check comes before anything is read for the call, so that a call past a share adds
    // nothing to the storage proof.
    //
    // Such a call counts no compute toward the chunk either, on purpose. Refusing it takes far
    // less work than its fees' compute stands for, less than reading an account. If it counted
    // that compute, a chunk producer could fill its chunk's compute limit with calls it knows
    // are past the shares, so that the chunk processes no receipts, at no cost to anyone and
    // with the chunk still produced. `max_external_calls_per_chunk` bounds the work they take.
    if let Err(err) = external_calls.fits() {
        return Ok(refuse(err.into(), 0));
    }
    // A second call to a contract is refused, before anything is read for it and with no
    // compute, as a call past a share is.
    if !external_calls.called_contracts.insert(ecc.contract_id().clone()) {
        let contract_id = ecc.contract_id().clone();
        return Ok(refuse(InvalidTxError::ContractCalledTwice { contract_id }.into(), 0));
    }
    // Everything recorded from here on, the read of the contract account included, counts toward
    // the share of the chunk's storage proof.
    let recorded_before = state_update.trie.recorded_storage_size_upper_bound();
    let processed = process_admitted_external_call(
        state_update,
        apply_state,
        epoch_info_provider,
        accounts,
        ecc,
        outcome_id,
        &cost,
    )?;
    let recorded_after = state_update.trie.recorded_storage_size_upper_bound();
    let recorded =
        u64::try_from(recorded_after.saturating_sub(recorded_before)).unwrap_or(u64::MAX);
    let compute =
        processed.outcome.outcome.compute_usage.expect("an ECC outcome reports its compute");
    external_calls.add(compute, recorded);
    Ok(processed)
}

/// What the chunk producer knows when it runs an ECC before packing it.
///
/// The run reads only these fields of the `ApplyState` that apply runs the call with, and the
/// other fields are left empty. A run that starts reading another field has to get it here too:
/// the dry run and apply have to reach the same verdict.
pub struct EccDryRunContext {
    pub shard_id: ShardId,
    /// The epoch of the block the chunk is for.
    pub epoch_id: EpochId,
    pub protocol_version: ProtocolVersion,
    pub config: Arc<RuntimeConfig>,
    pub vm_generation: VmGeneration,
    pub gas_price: Balance,
    /// The chunk's parent block, which the run sees.
    pub prev_block: PrevBlockContext,
    pub cache: Option<Box<dyn ContractRuntimeCache>>,
}

/// What a dry run of an ECC found.
#[derive(Debug)]
pub enum EccDryRun {
    /// The contract accepts the call and pays for it. `account` is its account after the call.
    Accepted {
        account: Account,
        gas_burnt: Gas,
        compute_usage: Compute,
        /// Whether the call installed its state init.
        installed_state_init: bool,
    },
    /// The call fails or the contract refuses it, and nobody pays.
    Refused(TxExecutionError),
}

/// Runs an ECC as apply does, with `process_external_call`, on top of the state in
/// `state_update`. The chunk producer uses it to pack only the calls that the contract accepts.
///
/// As in apply, an accepted call's writes are committed to `state_update`, so that the next
/// call runs on top of them. A refused call leaves nothing in it. `external_calls` counts the
/// call toward the shares whatever comes of it, so the caller keeps a copy to go back to if the
/// call is refused: a refused call is not in the chunk, and apply does not count it.
///
/// `loaded_account` is the contract's account if the chunk's signed transactions have read it
/// already, as apply does by then. Otherwise it is read from `state_update`, and the read counts
/// toward the shares, as it does in apply.
pub fn dry_run_external_call(
    state_update: &mut TrieUpdate,
    context: EccDryRunContext,
    epoch_info_provider: &dyn EpochInfoProvider,
    external_calls: &mut ExternalCallsInChunk,
    loaded_account: Option<Account>,
    ecc: &EccTransaction,
) -> Result<EccDryRun, RuntimeError> {
    let apply_state = ApplyState::for_ecc_dry_run(context);
    let contract_id = ecc.contract_id();
    let accounts = DashMap::with_hasher(AHashRandomState::new());
    if let Some(account) = loaded_account {
        accounts.insert(contract_id, Ok(Some(account)));
    }
    // A call can reach the pool without its validity checked, such as one put back after a
    // reorg. Apply checks it, so the dry run does too.
    let validation_error =
        validate_external_call(&apply_state.config, ecc, apply_state.current_protocol_version)
            .err();
    let ProcessedExternalCall { outcome, accepted, receipts: _, installed_state_init } =
        process_external_call(
            state_update,
            &apply_state,
            epoch_info_provider,
            external_calls,
            &accounts,
            ecc,
            validation_error,
        )?;
    let outcome = outcome.outcome;
    if !accepted {
        let ExecutionStatus::Failure(error) = outcome.status else {
            unreachable!("a refused external call has a failed outcome");
        };
        return Ok(EccDryRun::Refused(error));
    }
    let account = accounts
        .get(contract_id)
        .and_then(|entry| entry.value().as_ref().ok().cloned().flatten())
        .expect("an accepted call leaves its contract's account in the map");
    let compute_usage = outcome.compute_usage.expect("an ECC outcome reports its compute");
    let gas_burnt = outcome.gas_burnt;
    Ok(EccDryRun::Accepted { account, gas_burnt, compute_usage, installed_state_init })
}

/// Processes an external contract call that passed the check of the chunk's shares for ECCs,
/// as `process_external_call` describes.
fn process_admitted_external_call<'a>(
    state_update: &mut TrieUpdate,
    apply_state: &ApplyState,
    epoch_info_provider: &dyn EpochInfoProvider,
    accounts: &DashMap<&'a AccountId, Result<Option<Account>, StorageError>, AHashRandomState>,
    ecc: &'a EccTransaction,
    outcome_id: CryptoHash,
    cost: &EccCost,
) -> Result<ProcessedExternalCall, RuntimeError> {
    // Read and update the contract's account through the chunk's working copy, which signed
    // transactions later in the chunk start from. Otherwise a later transaction from the
    // contract would overwrite the charge and the storage usage of an accepted call. A signed
    // transaction from the contract in the chunk has read the account already.
    let contract_id = ecc.contract_id();
    if !accounts.contains_key(contract_id) {
        accounts.insert(contract_id, get_account(state_update, contract_id));
    }
    let mut entry = accounts.get_mut(contract_id).expect("the contract account was read above");
    let contract = entry.value_mut().as_mut().map_err(|err| err.clone())?;
    let refuse = |error: TxExecutionError, compute: Compute| ProcessedExternalCall {
        outcome: ExecutionOutcomeWithId::refused_ecc(outcome_id, ecc, error, compute),
        accepted: false,
        receipts: vec![],
        installed_state_init: false,
    };
    // A call that installs its state init pays the install's fees too, so its contract must
    // cover them before the run.
    let config = &apply_state.config;
    let call_compute = cost.compute_burnt;
    let cost = match ecc_cost_for_account(config, ecc, contract.as_ref(), apply_state.gas_price) {
        Ok(cost) => cost,
        Err(err) => return Ok(refuse(InvalidTxError::from(err).into(), call_compute)),
    };
    // The fees come out of the call's gas budget, so a call whose fees use it all can never run.
    // Its refusal counts the compute of the call without the install, as other refusals do.
    if cost.gas_burnt >= config.wasm_config.limit_config.max_gas_burnt_external {
        return Ok(refuse(gas_exceeded(), call_compute));
    }
    let state_init = contract.as_ref().and_then(|account| state_init_to_install(account, ecc));
    let account = match verify_ecc_ephemeral(contract.as_mut(), ecc, &cost) {
        Ok(account) => account,
        Err(err) => return Ok(refuse(err.into(), cost.compute_burnt)),
    };

    // The state update may hold changes staged before the call, such as the outgoing buffer pops
    // of `forward_from_buffer`. Commit them so that refusing the call rolls back only its install
    // and run.
    let cause = StateChangeCause::ExternalCallProcessing { transaction_id: outcome_id };
    state_update.commit(cause.clone());
    // The install and the run change a copy of the account, which replaces the chunk's working
    // copy only if the call is accepted.
    let mut run_account = account.clone();
    let read_budget =
        match prepare_account_for_call(state_update, apply_state, &mut run_account, ecc)? {
            Ok(read_budget) => read_budget,
            Err(error) => {
                state_update.rollback();
                return Ok(refuse(error, cost.compute_burnt));
            }
        };
    let mut run = run_external_call(
        state_update,
        apply_state,
        epoch_info_provider,
        &mut run_account,
        ecc,
        &outcome_id,
        &cost,
        read_budget,
    )?;
    let accepted = run.verdict.is_ok();
    if accepted {
        *account = run_account;
        set_account(state_update, contract_id.clone(), account);
        state_update.commit(cause);
    } else {
        state_update.rollback();
    }
    let mut receipts = std::mem::take(&mut run.new_receipts);
    let receipt_ids = set_ecc_receipt_ids(apply_state, &outcome_id, &mut receipts);
    let outcome = run.into_outcome(outcome_id, apply_state.block_height, ecc, account, receipt_ids);
    let installed_state_init = accepted && state_init.is_some();
    Ok(ProcessedExternalCall { outcome, accepted, receipts, installed_state_init })
}

/// Gives the receipts of an external call their ids, derived from the call's outcome id the way a
/// receipt's new receipts get theirs. Returns the ids of the action receipts.
fn set_ecc_receipt_ids(
    apply_state: &ApplyState,
    outcome_id: &CryptoHash,
    receipts: &mut [Receipt],
) -> Vec<CryptoHash> {
    let mut receipt_ids = vec![];
    for (receipt_index, receipt) in receipts.iter_mut().enumerate() {
        let receipt_id = apply_state.create_receipt_id(outcome_id, receipt_index);
        receipt.set_receipt_id(receipt_id);
        if matches!(
            receipt.versioned_receipt(),
            VersionedReceiptEnum::Action(_) | VersionedReceiptEnum::PromiseYield(_)
        ) {
            receipt_ids.push(receipt_id);
        }
    }
    receipt_ids
}

/// What an external call did, and whether the contract accepts and pays for it.
pub(crate) struct ExternalCallRun {
    /// Gas burnt by the call's fees and its run. It counts toward the chunk's gas only if the
    /// call is accepted.
    pub gas_burnt: Gas,
    /// Compute used by the call's fees and its run. It counts toward the chunk's compute whether
    /// or not the call is accepted.
    pub compute_usage: Compute,
    /// The amount the call burns if the contract accepts it, or why the call is refused. The
    /// contract also pays for the gas attached to the receipts, which goes with them.
    pub verdict: Result<Balance, TxExecutionError>,
    /// The receipts of an accepted call, without receipt ids.
    pub new_receipts: Vec<Receipt>,
    outcome: VMOutcome,
}

impl ExternalCallRun {
    /// The execution outcome of the call. `account` is the contract's account after the call, and
    /// `receipt_ids` the ids of the action receipts it created.
    pub fn into_outcome(
        self,
        id: CryptoHash,
        block_height: BlockHeight,
        ecc: &EccTransaction,
        account: &Account,
        receipt_ids: Vec<CryptoHash>,
    ) -> ExecutionOutcomeWithId {
        let burnt = match self.verdict {
            Ok(burnt) => burnt,
            // TODO(ecc): include the logs of a refused call in its outcome.
            Err(error) => {
                return ExecutionOutcomeWithId::refused_ecc(id, ecc, error, self.compute_usage);
            }
        };
        let status = match self.outcome.return_data {
            ReturnData::Value(data) => ExecutionStatus::SuccessValue(data),
            ReturnData::None => ExecutionStatus::SuccessValue(vec![]),
            ReturnData::ReceiptIndex(index) => ExecutionStatus::SuccessReceiptId(
                create_receipt_id_from_receipt_id(&id, block_height, index as usize),
            ),
        };
        let profile = ProfileDataV3::convert(self.outcome.profile);
        let metadata = {
            let contracts = vec![account.contract().into_owned()];
            ExecutionMetadata::V4(Box::new(ExecutionMetadataV4 { profile, contracts }))
        };
        ExecutionOutcomeWithId {
            id,
            outcome: ExecutionOutcome {
                status,
                logs: self.outcome.logs,
                receipt_ids,
                gas_burnt: self.gas_burnt,
                compute_usage: Some(self.compute_usage),
                tokens_burnt: burnt,
                executor_id: ecc.contract_id().clone(),
                metadata,
            },
        }
    }
}

/// Makes `account` ready for `ecc` to run: installs the state init the account takes, uncommitted,
/// then checks for a contract. Returns what the run may still record, or why the call is refused.
fn prepare_account_for_call(
    state_update: &mut TrieUpdate,
    apply_state: &ApplyState,
    account: &mut Account,
    ecc: &EccTransaction,
) -> Result<Result<usize, TxExecutionError>, RuntimeError> {
    let contract_id = ecc.contract_id();
    let max_recorded_size = apply_state.config.external_calls_config.max_call_recorded_size;
    let mut read_budget = max_recorded_size;
    if let Some(state_init) = state_init_to_install(account, ecc) {
        // Unlike the action, the call creates no account and carries no deposit to settle: the
        // check that ends the run covers the installed state's storage staking.
        let mut result = ActionResult::default();
        let recorded_before = state_update.trie.recorded_storage_size_upper_bound();
        let mut budgeted = state_update
            .with_recording_budget(max_recorded_size)
            .ok_or(RuntimeError::ExternalCallWithoutRecorder)?;
        let installed = install_universal_state_init(
            &mut budgeted,
            account,
            contract_id,
            state_init,
            &mut result,
            &apply_state.config.fees,
            apply_state.block_height,
            apply_state.current_protocol_version,
        );
        drop(budgeted);
        match installed {
            Err(RuntimeError::StorageError(StorageError::RecordingBudgetExceeded { limit })) => {
                return Ok(Err(recorded_storage_exceeded(limit)));
            }
            installed => installed?,
        }
        if let Err(error) = result.result {
            return Ok(Err(error.into()));
        }
        let recorded =
            state_update.trie.recorded_storage_size_upper_bound().saturating_sub(recorded_before);
        read_budget = max_recorded_size.saturating_sub(recorded);
    }
    // The contract is both the sender and the receiver of the call, so an account with no
    // contract is refused like a missing account, without loading anything.
    if matches!(account.contract().as_ref(), AccountContract::None) {
        let error = InvalidTxError::ContractNotFound { contract_id: contract_id.clone() }.into();
        return Ok(Err(error));
    }
    Ok(Ok(read_budget))
}

/// The error of a call whose state init install records more than a call may.
fn recorded_storage_exceeded(limit: u64) -> TxExecutionError {
    let limit = ByteSize::b(limit);
    let error = FunctionCallError::HostError(HostError::RecordedStorageExceeded { limit });
    let kind = ActionErrorKind::FunctionCallError(Convert::convert(error));
    ActionError { index: Some(0), kind }.into()
}

/// The error of a call whose fees use all of its gas budget before the run.
fn gas_exceeded() -> TxExecutionError {
    let error = FunctionCallError::HostError(HostError::GasExceeded);
    let kind = ActionErrorKind::FunctionCallError(Convert::convert(error));
    ActionError { index: Some(0), kind }.into()
}

/// Runs the reserved method of the contract of `ecc`, whose account is `account`, and decides
/// whether the contract accepts and pays for the call. `cost` holds the fees of the call, which
/// `verify_ecc_ephemeral` checked the contract can pay before the run.
///
/// The run's writes are left in `state_update` uncommitted: the caller commits them if the call
/// is accepted and rolls them back otherwise. `account` is updated with the contract's new
/// balance and storage usage only if the call is accepted.
///
/// A `RuntimeError`, such as a storage error, stops applying the chunk.
pub(crate) fn run_external_call(
    state_update: &mut TrieUpdate,
    apply_state: &ApplyState,
    epoch_info_provider: &dyn EpochInfoProvider,
    account: &mut Account,
    ecc: &EccTransaction,
    outcome_id: &CryptoHash,
    cost: &EccCost,
    read_budget: usize,
) -> Result<ExternalCallRun, RuntimeError> {
    let config = &apply_state.config;
    let contract_id = ecc.contract_id();
    let execution_mode = ExecutionMode::External { fee_gas: cost.gas_burnt };
    let preparation = prepare_contract_metadata(
        config,
        state_update.contract_storage(),
        &epoch_info_provider.chain_id(),
        contract_id,
        account,
        state_update,
        ECC_METHOD_NAME,
        // External calls have no prepaid gas; their limits are set by the protocol.
        Gas::ZERO,
        &execution_mode,
        AccessOptions::DEFAULT,
        apply_state.current_protocol_version,
    )?;
    let context = external_call_context(apply_state, account, ecc, execution_mode);

    // A run may record only so much of the trie in the storage proof. A read past the cap fails
    // the run, at the same trie node in the chunk producer's run and in a replay from the proof,
    // and a value past it is never fetched. The cap is lifted when `budgeted` is dropped.
    let mut budgeted = state_update
        .with_recording_budget(read_budget)
        .ok_or(RuntimeError::ExternalCallWithoutRecorder)?;
    let mut receipt_manager = ReceiptManager::default();
    let mut runtime_ext = RuntimeExt::new(
        &mut budgeted,
        &mut receipt_manager,
        contract_id.clone(),
        account.clone(),
        // Data ids are salted like the action hash of a receipt, so they differ from receipt ids.
        create_action_hash_from_receipt_id(outcome_id, apply_state.block_height, 0),
        // The validator stake host functions see the epoch of the chunk's parent block, as the
        // rest of the run does. Data ids are derived from the chunk's height, like the outcome id.
        apply_state.prev_block.epoch_id,
        apply_state.block_height,
        epoch_info_provider,
        apply_state.current_protocol_version,
        Arc::clone(&apply_state.trie_access_tracker_state),
        // The per-receipt storage proof limit counts from the start of the run. The recording
        // budget set around the run caps it far lower.
        None,
    );
    let outcome = match preparation {
        ContractPreparation::Ready { contract: code_ext, gas_counter } => {
            let contract_identifier = code_ext.identifier.clone();
            record_contract_call(
                runtime_ext.trie_update,
                &contract_identifier,
                &apply_state.apply_reason,
            )?;
            // External calls don't go through the receipt preparation pipeline, which is keyed
            // by receipts and prepares contracts with the limits of internal calls.
            let contract = prepare_function_call(
                &code_ext,
                apply_state.cache.as_deref(),
                Arc::clone(&config.wasm_config),
                gas_counter,
                ECC_METHOD_NAME,
                CompilePriority::Critical,
            );
            execute_function_call(
                contract,
                &contract_identifier,
                apply_state,
                &mut runtime_ext,
                &context,
                config,
            )?
        }
        ContractPreparation::Aborted(abort) => {
            abort.into_outcome(&context, Arc::clone(&config.wasm_config))
        }
    };
    drop(runtime_ext);
    drop(budgeted);
    // TODO(ecc): count external call runs and their failures in metrics, either in the
    // `FUNCTION_CALL_PROCESSED*` metrics that `action_function_call` reports or in metrics of
    // their own.

    let receipt_gas_price = receipt_gas_price(config, apply_state.gas_price);
    let mut receipts = vec![];
    if outcome.aborted.is_none() {
        let signer = NewReceiptSigner {
            signer_id: contract_id,
            authorization_context: Cow::Owned(AuthorizationContext::ExternalContractCall),
            gas_price: receipt_gas_price,
        };
        receipts = new_receipts(state_update, apply_state, contract_id, receipt_manager, &signer)?;
    }
    let verdict =
        verdict(apply_state, receipt_gas_price, contract_id, account, &outcome, &receipts, cost)?;
    if verdict.is_err() {
        receipts.clear();
    }
    Ok(ExternalCallRun {
        gas_burnt: cost.gas_burnt.checked_add_result(outcome.burnt_gas)?,
        compute_usage: safe_add_compute(cost.compute_burnt, outcome.compute_usage)?,
        verdict,
        new_receipts: receipts,
        outcome,
    })
}

/// The context of the run of an external call. The run sees the chunk's parent block, which the
/// chunk producer runs the call on top of when building the chunk.
fn external_call_context(
    apply_state: &ApplyState,
    account: &Account,
    ecc: &EccTransaction,
    execution_mode: ExecutionMode,
) -> VMContext {
    let contract_id = ecc.contract_id();
    VMContext {
        current_account_id: contract_id.clone(),
        // An external call has no signer, predecessor or refund receiver. In external mode the
        // host functions either return the current account instead or fail, so these fields
        // are never read.
        signer_account_id: contract_id.clone(),
        signer_account_pk: None,
        predecessor_account_id: contract_id.clone(),
        refund_to_account_id: contract_id.clone(),
        input: Rc::from(ecc.payload()),
        promise_results: [].into(),
        // TODO(spice): double check the context before Spice lands.
        // Under Spice the producer reads the last certified block's
        // state while this context is the parent's.
        block_height: apply_state.prev_block.height,
        block_timestamp: apply_state.prev_block.timestamp,
        epoch_height: apply_state.prev_block.epoch_height,
        account_balance: account.amount(),
        account_locked_balance: account.locked(),
        storage_usage: account.storage_usage(),
        account_contract: account.contract().into_owned(),
        attached_deposit: Balance::ZERO,
        prepaid_gas: Gas::ZERO,
        // External calls can't read the random seed: its value would be known before the call
        // is included in a chunk.
        random_seed: vec![],
        execution_mode,
        output_data_receivers: vec![],
    }
}

/// Decides whether `contract_id` accepts the call that produced `outcome` and `receipts`, and
/// charges it if so.
///
/// On acceptance, `account` gets the balance the run left minus the charge, and the storage
/// usage the run left, and the burnt amount is returned. On refusal `account` is unchanged.
fn verdict(
    apply_state: &ApplyState,
    receipt_gas_price: Balance,
    contract_id: &AccountId,
    account: &mut Account,
    outcome: &VMOutcome,
    receipts: &[Receipt],
    cost: &EccCost,
) -> Result<Result<Balance, TxExecutionError>, RuntimeError> {
    let config = &apply_state.config;
    if let Some(err) = &outcome.aborted {
        let kind = ActionErrorKind::FunctionCallError(Convert::convert(err.clone()));
        return Ok(Err(ActionError { index: Some(0), kind }.into()));
    }
    for receipt in receipts {
        if let Err(err) = validate_receipt(
            &config.wasm_config.limit_config,
            receipt,
            apply_state.current_protocol_version,
            ValidateReceiptMode::NewReceipt,
        ) {
            let kind = ActionErrorKind::NewReceiptValidationError(err);
            return Ok(Err(ActionError { index: Some(0), kind }.into()));
        }
    }

    // The used gas includes the gas attached to the receipts, which the contract pays for too.
    let attached_gas =
        outcome.used_gas.checked_sub(outcome.burnt_gas).ok_or(IntegerOverflowError)?;
    let burnt = safe_add_balance(
        cost.burnt_amount,
        safe_gas_to_balance(apply_state.gas_price, outcome.burnt_gas)?,
    )?;
    let charge = safe_add_balance(burnt, safe_gas_to_balance(receipt_gas_price, attached_gas)?)?;
    // The VM lets a run attach a deposit its balance cannot cover and reports it as subsidized.
    // An external call has nothing to cover it, so the contract lacks that amount too.
    let balance =
        outcome.balance.checked_sub(charge).filter(|_| outcome.subsidized_amount == Balance::ZERO);
    let Some(balance) = balance else {
        return Ok(Err(InvalidTxError::NotEnoughBalance {
            signer_id: contract_id.clone(),
            balance: outcome.balance,
            cost: safe_add_balance(charge, outcome.subsidized_amount)?,
        }
        .into()));
    };

    let mut charged = account.clone();
    charged.set_amount(balance);
    charged.set_storage_usage(outcome.storage_usage);
    match check_storage_stake(&charged, balance, config) {
        Ok(()) => {}
        Err(StorageStakingError::LackBalanceForStorageStaking(amount)) => {
            return Ok(Err(ActionError {
                index: None,
                kind: ActionErrorKind::LackBalanceForState {
                    account_id: contract_id.clone(),
                    amount,
                },
            }
            .into()));
        }
        Err(StorageStakingError::StorageError(err)) => {
            return Err(StorageError::StorageInconsistentState(err).into());
        }
    }
    *account = charged;
    Ok(Ok(burnt))
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_primitives::num_rational::Rational32;

    /// A config whose shares for ECCs are 2/5 of the chunk's compute and 1/20 of its storage
    /// proof, with a storage proof limit of 4 MB.
    fn config() -> RuntimeConfig {
        let mut config = RuntimeConfig::test();
        config.witness_config.main_storage_proof_size_soft_limit = 4_000_000;
        config.external_calls_config.chunk_compute_share = Rational32::new(2, 5);
        config.external_calls_config.chunk_storage_proof_share = Rational32::new(1, 20);
        config
    }

    fn compute_exceeded(used: Compute, limit: Compute) -> Result<(), InvalidTxError> {
        let share = ExternalCallChunkLimit::Compute { used, limit };
        Err(InvalidTxError::ExternalCallChunkShareReached { share })
    }

    fn storage_proof_exceeded(used: u64, limit: u64) -> Result<(), InvalidTxError> {
        let share = ExternalCallChunkLimit::StorageProof { used, limit };
        Err(InvalidTxError::ExternalCallChunkShareReached { share })
    }

    #[test]
    fn test_external_calls_in_chunk_shares() {
        let calls = ExternalCallsInChunk::new(&config(), Some(Gas::from_gas(1000)));
        assert_eq!(calls.compute_share, 400);
        assert_eq!(calls.recorded_size_share, 200_000);
        assert_eq!(calls.fits(), Ok(()));
    }

    /// Room is left while the chunk's ECCs used less than the compute share, so the call that
    /// reaches it is admitted. Once they used exactly the share, the next call is refused.
    #[test]
    fn test_external_calls_in_chunk_compute_share_boundary() {
        let mut calls = ExternalCallsInChunk::new(&config(), Some(Gas::from_gas(1000)));
        calls.add(399, 0);
        assert_eq!(calls.fits(), Ok(()));
        calls.add(1, 0);
        assert_eq!(calls.fits(), compute_exceeded(400, 400));
    }

    /// Room is left while the chunk's ECCs recorded less than the storage proof share, so the
    /// call that reaches it is admitted. Once they recorded exactly the share, the next call is
    /// refused.
    #[test]
    fn test_external_calls_in_chunk_storage_proof_share_boundary() {
        let mut calls = ExternalCallsInChunk::new(&config(), Some(Gas::from_gas(1000)));
        calls.add(0, 199_999);
        assert_eq!(calls.fits(), Ok(()));
        calls.add(0, 1);
        assert_eq!(calls.fits(), storage_proof_exceeded(200_000, 200_000));
    }

    /// A call can pass a share, as what it uses is known only once it ran. The refusal of the
    /// next call reports all that was used.
    #[test]
    fn test_external_calls_in_chunk_past_share() {
        let mut calls = ExternalCallsInChunk::new(&config(), Some(Gas::from_gas(1000)));
        calls.add(0, 250_000);
        assert_eq!(calls.fits(), storage_proof_exceeded(250_000, 200_000));
    }

    /// The compute share is checked first.
    #[test]
    fn test_external_calls_in_chunk_order_of_checks() {
        let mut calls = ExternalCallsInChunk::new(&config(), Some(Gas::from_gas(1000)));
        calls.add(500, 300_000);
        assert_eq!(calls.fits(), compute_exceeded(500, 400));
    }

    /// Without a gas limit of the chunk there is no compute share, but the storage proof share
    /// still applies.
    #[test]
    fn test_external_calls_in_chunk_without_gas_limit() {
        let mut calls = ExternalCallsInChunk::new(&config(), None);
        calls.add(u64::MAX / 2, 0);
        assert_eq!(calls.fits(), Ok(()));
        calls.add(0, 200_000);
        assert_eq!(calls.fits(), storage_proof_exceeded(200_000, 200_000));
    }
}

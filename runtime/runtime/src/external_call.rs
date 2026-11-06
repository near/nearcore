//! Running external contract calls (ECCs) while applying a chunk.
//!
//! An external call runs the contract's reserved method in `ExecutionMode::External`, with the
//! call's payload as input. A clean return accepts the call: the run's writes stay and the
//! contract pays for the call's fees and the gas the run used. Any failure refuses the call:
//! the caller rolls the run's writes back and nobody pays.
//!
//! The run and the verdict don't depend on the chunk's processing state, so the chunk producer
//! can reuse them to run a call before packing it.

use crate::ApplyState;
use crate::config::{EccCost, safe_add_balance, safe_add_compute, safe_gas_to_balance};
use crate::contract_preparation::{
    ContractPreparation, prepare_contract_metadata, prepare_function_call,
};
use crate::conversions::Convert;
use crate::ext::RuntimeExt;
use crate::function_call::{execute_function_call, record_contract_call};
use crate::receipt_manager::ReceiptManager;
use crate::verifier::{StorageStakingError, check_storage_stake};
use near_parameters::RuntimeConfig;
use near_primitives::account::Account;
use near_primitives::errors::{
    ActionError, ActionErrorKind, ExternalCallChunkLimit, InvalidTxError, RuntimeError,
    StorageError, TxExecutionError,
};
use near_primitives::hash::CryptoHash;
use near_primitives::profile_data_v3::ProfileDataV3;
use near_primitives::transaction::{
    ECC_METHOD_NAME, EccTransaction, ExecutionMetadata, ExecutionMetadataV4, ExecutionOutcome,
    ExecutionOutcomeWithId, ExecutionStatus,
};
use near_primitives::types::{AccountId, Balance, Compute, EpochInfoProvider, Gas};
use near_store::TrieUpdate;
use near_store::trie::AccessOptions;
use near_vm_runner::CompilePriority;
use near_vm_runner::logic::{ExecutionMode, ReturnData, VMContext, VMOutcome};
use std::rc::Rc;
use std::sync::Arc;

/// What the external calls of a chunk used so far, against their shares of the chunk's compute
/// and storage proof, which apply checks before each call. Apply uses it, and the chunk producer
/// will too, so both admit the same calls.
///
/// The other rules on a chunk's external calls, one call per contract and a share of the chunk's
/// transaction bytes, make a chunk invalid, and are checked before apply.
#[derive(Debug)]
pub struct ExternalCallsInChunk {
    compute: Compute,
    recorded_size: u64,
    /// The share of the chunk's compute limit. `Compute::MAX` when the chunk has no gas limit,
    /// as apply's own compute limit is then.
    compute_share: Compute,
    /// The share of `main_storage_proof_size_soft_limit`.
    recorded_size_share: u64,
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
        }
    }

    /// Whether the chunk has room left in every share. What a call uses is known only once it
    /// ran, so a chunk can pass each share by at most one call.
    pub fn fits(&self) -> Result<(), InvalidTxError> {
        let (used, limit) = (self.compute, self.compute_share);
        if used >= limit {
            let limit = ExternalCallChunkLimit::Compute { used, limit };
            return Err(InvalidTxError::ExternalCallChunkLimitExceeded { limit });
        }
        let (used, limit) = (self.recorded_size, self.recorded_size_share);
        if used >= limit {
            let limit = ExternalCallChunkLimit::StorageProof { used, limit };
            return Err(InvalidTxError::ExternalCallChunkLimitExceeded { limit });
        }
        Ok(())
    }

    /// Counts a call that passed `fits`, whatever came of it: the compute it used and the
    /// storage proof it recorded, the read of its contract account included.
    pub fn add(&mut self, compute: Compute, recorded_size: u64) {
        self.compute = self.compute.saturating_add(compute);
        self.recorded_size = self.recorded_size.saturating_add(recorded_size);
    }
}

/// What an external call did, and whether the contract accepts and pays for it.
pub(crate) struct ExternalCallRun {
    /// Gas burnt by the call's fees and its run. It counts toward the chunk's gas only if the
    /// call is accepted.
    pub gas_burnt: Gas,
    /// Compute used by the call's fees and its run. It counts toward the chunk's compute whether
    /// or not the call is accepted.
    pub compute_usage: Compute,
    /// The amount the contract pays if it accepts the call, or why the call is refused. The
    /// whole amount is reported as burnt, in the outcome and in the chunk's stats.
    // TODO(ecc): once external calls can create receipts, carry the burnt amount separately from
    // the charge. The contract still pays for the used gas, but only the fees and the burnt gas
    // are burnt: the gas attached to the receipts goes with them. Until then the two amounts are
    // the same.
    pub verdict: Result<Balance, TxExecutionError>,
    outcome: VMOutcome,
}

impl ExternalCallRun {
    /// The execution outcome of the call. `account` is the contract's account after the call.
    pub fn into_outcome(
        self,
        id: CryptoHash,
        ecc: &EccTransaction,
        account: &Account,
    ) -> ExecutionOutcomeWithId {
        let charge = match self.verdict {
            Ok(charge) => charge,
            // TODO(ecc): include the logs of a refused call in its outcome.
            Err(error) => {
                return ExecutionOutcomeWithId::refused_ecc(id, ecc, error, self.compute_usage);
            }
        };
        let status = match self.outcome.return_data {
            ReturnData::Value(data) => ExecutionStatus::SuccessValue(data),
            ReturnData::None => ExecutionStatus::SuccessValue(vec![]),
            ReturnData::ReceiptIndex(_) => {
                // TODO(ecc): allow ECC to create receipts
                unreachable!("an external call that returns a promise is refused")
            }
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
                receipt_ids: vec![],
                gas_burnt: self.gas_burnt,
                compute_usage: Some(self.compute_usage),
                tokens_burnt: charge,
                executor_id: ecc.contract_id().clone(),
                metadata,
            },
        }
    }
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

    let mut receipt_manager = ReceiptManager::default();
    let mut runtime_ext = RuntimeExt::new(
        state_update,
        &mut receipt_manager,
        contract_id.clone(),
        account.clone(),
        *outcome_id,
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
            // A run may record only so much of the trie in the storage proof. A read past the cap
            // fails the run, at the same trie node in the chunk producer's run and in a replay
            // from the proof, and a value past it is never fetched.
            let trie = runtime_ext.trie_update.trie();
            trie.set_recording_budget(config.external_calls_config.max_call_recorded_size);
            let outcome = execute_function_call(
                contract,
                &contract_identifier,
                apply_state,
                &mut runtime_ext,
                &context,
                config,
            );
            runtime_ext.trie_update.trie().clear_recording_budget();
            outcome?
        }
        ContractPreparation::Aborted(abort) => {
            abort.into_outcome(&context, Arc::clone(&config.wasm_config))
        }
    };
    drop(runtime_ext);
    // TODO(ecc): count external call runs and their failures in metrics, either in the
    // `FUNCTION_CALL_PROCESSED*` metrics that `action_function_call` reports or in metrics of
    // their own.

    let verdict = verdict(
        config,
        apply_state.gas_price,
        contract_id,
        account,
        &outcome,
        &receipt_manager,
        cost,
    )?;
    Ok(ExternalCallRun {
        gas_burnt: cost.gas_burnt.checked_add_result(outcome.burnt_gas)?,
        compute_usage: safe_add_compute(cost.compute_burnt, outcome.compute_usage)?,
        verdict,
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
        signer_account_pk: vec![],
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

/// Decides whether `contract_id` accepts the call that produced `outcome`, and charges it if so.
///
/// On acceptance, `account` gets the balance the run left minus the charge, and the storage
/// usage the run left, and the charge is returned. On refusal `account` is unchanged.
fn verdict(
    config: &RuntimeConfig,
    gas_price: Balance,
    contract_id: &AccountId,
    account: &mut Account,
    outcome: &VMOutcome,
    receipt_manager: &ReceiptManager,
    cost: &EccCost,
) -> Result<Result<Balance, TxExecutionError>, RuntimeError> {
    if let Some(err) = &outcome.aborted {
        let kind = ActionErrorKind::FunctionCallError(Convert::convert(err.clone()));
        return Ok(Err(ActionError { index: Some(0), kind }.into()));
    }

    // TODO(ecc): turn the receipts the run created into outgoing receipts (promises and yield
    // resumes) instead of refusing the call. A run that creates a yielded promise stays refused:
    // the yield's data id is derived from the height of the chunk that includes the call.
    let created_receipts = !receipt_manager.action_receipts.is_empty()
        || !receipt_manager.data_receipts.is_empty()
        || matches!(outcome.return_data, ReturnData::ReceiptIndex(_));
    if created_receipts {
        return Ok(Err(InvalidTxError::InvalidTransactionVersion.into()));
    }

    // The used gas includes the gas attached to the receipts the run created, which the contract
    // pays for too.
    let gas_cost = safe_gas_to_balance(gas_price, outcome.used_gas)?;
    let charge = safe_add_balance(cost.burnt_amount, gas_cost)?;
    let Some(balance) = outcome.balance.checked_sub(charge) else {
        return Ok(Err(InvalidTxError::NotEnoughBalance {
            signer_id: contract_id.clone(),
            balance: outcome.balance,
            cost: charge,
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
    Ok(Ok(charge))
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
        let limit = ExternalCallChunkLimit::Compute { used, limit };
        Err(InvalidTxError::ExternalCallChunkLimitExceeded { limit })
    }

    fn storage_proof_exceeded(used: u64, limit: u64) -> Result<(), InvalidTxError> {
        let limit = ExternalCallChunkLimit::StorageProof { used, limit };
        Err(InvalidTxError::ExternalCallChunkLimitExceeded { limit })
    }

    #[test]
    fn test_external_calls_in_chunk_shares() {
        let mut calls = ExternalCallsInChunk::new(&config(), Some(Gas::from_gas(1000)));
        assert_eq!(calls.compute_share, 400);
        assert_eq!(calls.recorded_size_share, 200_000);
        assert_eq!(calls.fits(), Ok(()));

        // Room is left until a share is reached, so the call that reaches it is admitted.
        calls.add(399, 199_999);
        assert_eq!(calls.fits(), Ok(()));
        calls.add(1, 0);
        assert_eq!(calls.fits(), compute_exceeded(400, 400));
    }

    #[test]
    fn test_external_calls_in_chunk_storage_proof_share() {
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

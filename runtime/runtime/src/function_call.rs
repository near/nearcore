use crate::config::safe_add_compute;
use crate::ext::{ExternalError, RuntimeExt};
use crate::receipt_manager::ReceiptManager;
use crate::{ActionResult, ApplyState, metrics};
use near_parameters::RuntimeConfig;
use near_primitives::account::Account;
use near_primitives::config::ViewConfig;
use near_primitives::errors::{ActionError, ActionErrorKind, RuntimeError};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    ActionReceipt, ActionReceiptV2, DataReceipt, Receipt, ReceiptEnum, ReceiptV0,
    VersionedActionReceipt,
};
use near_primitives::transaction::FunctionCallAction;
use near_primitives::types::{AccountId, EpochInfoProvider};
use near_primitives_core::version::ProtocolFeature;
use near_store::{
    StorageError, TrieUpdate, enqueue_promise_yield_timeout, get_promise_yield_indices,
    set_promise_yield_indices,
};
use near_vm_runner::PreparedContract;
use near_vm_runner::logic::errors::{
    CompilationError, FunctionCallError, InconsistentStateError, VMRunnerError,
};
use near_vm_runner::logic::{VMContext, VMOutcome};
use std::rc::Rc;
use std::sync::Arc;

pub(crate) fn action_function_call(
    state_update: &mut TrieUpdate,
    apply_state: &ApplyState,
    account: &mut Account,
    receipt: &Receipt,
    action_receipt: &VersionedActionReceipt,
    promise_results: Arc<[near_vm_runner::logic::types::PromiseResult]>,
    result: &mut ActionResult,
    account_id: &AccountId,
    function_call: &FunctionCallAction,
    action_hash: &CryptoHash,
    code_hash: CryptoHash,
    config: &RuntimeConfig,
    is_last_action: bool,
    epoch_info_provider: &dyn EpochInfoProvider,
    contract: Box<dyn PreparedContract>,
) -> Result<(), RuntimeError> {
    if account.amount().checked_add(function_call.deposit).is_none() {
        return Err(StorageError::StorageInconsistentState(
            "Account balance integer overflow during function call deposit".to_string(),
        )
        .into());
    }

    let account_contract = account.contract();
    state_update.record_contract_call(
        account_id.clone(),
        code_hash,
        account_contract.as_ref(),
        apply_state.apply_reason.clone(),
    )?;

    #[cfg(feature = "test_features")]
    apply_recorded_storage_garbage(function_call, state_update);

    let mut receipt_manager = ReceiptManager::default();
    let mut runtime_ext = RuntimeExt::new(
        state_update,
        &mut receipt_manager,
        account_id.clone(),
        account.clone(),
        *action_hash,
        apply_state.epoch_id,
        apply_state.block_height,
        epoch_info_provider,
        apply_state.current_protocol_version,
        config.wasm_config.storage_get_mode,
        Arc::clone(&apply_state.trie_access_tracker_state),
    );
    let outcome = execute_function_call(
        contract,
        apply_state,
        &mut runtime_ext,
        receipt.predecessor_id(),
        action_receipt,
        promise_results,
        function_call,
        action_hash,
        config,
        is_last_action,
        None,
    )?;

    match &outcome.aborted {
        None => {
            metrics::FUNCTION_CALL_PROCESSED.with_label_values(&["ok"]).inc();
        }
        Some(err) => {
            metrics::FUNCTION_CALL_PROCESSED.with_label_values(&[err.into()]).inc();
        }
    }

    let execution_succeeded = outcome.aborted.is_none();
    if let Some(err) = outcome.aborted {
        // collect metrics for failed function calls
        metrics::FUNCTION_CALL_PROCESSED_FUNCTION_CALL_ERRORS
            .with_label_values(&[(&err).into()])
            .inc();
        match &err {
            FunctionCallError::CompilationError(err) => {
                metrics::FUNCTION_CALL_PROCESSED_COMPILATION_ERRORS
                    .with_label_values(&[err.into()])
                    .inc();
            }
            FunctionCallError::LinkError { .. } => (),
            FunctionCallError::MethodResolveError(err) => {
                metrics::FUNCTION_CALL_PROCESSED_METHOD_RESOLVE_ERRORS
                    .with_label_values(&[err.into()])
                    .inc();
            }
            FunctionCallError::WasmTrap(inner_err) => {
                metrics::FUNCTION_CALL_PROCESSED_WASM_TRAP_ERRORS
                    .with_label_values(&[inner_err.into()])
                    .inc();
            }
            FunctionCallError::HostError(inner_err) => {
                metrics::FUNCTION_CALL_PROCESSED_HOST_ERRORS
                    .with_label_values(&[inner_err.into()])
                    .inc();
            }
        }
        // Update action result with the abort error converted to the
        // transaction runtime's format of errors.
        let action_err: ActionError =
            ActionErrorKind::FunctionCallError(crate::conversions::Convert::convert(err)).into();
        result.result = Err(action_err);
    }
    result.gas_burnt = result.gas_burnt.checked_add_result(outcome.burnt_gas)?;
    result.gas_burnt_for_function_call =
        result.gas_burnt_for_function_call.checked_add_result(outcome.burnt_gas)?;
    // Runtime in `generate_refund_receipts` takes care of using proper value for refunds.
    // It uses `gas_used` for success and `gas_burnt` for failures. So it's not an issue to
    // return a real `gas_used` instead of the `gas_burnt` into `ActionResult` even for
    // `FunctionCall`s error.
    result.gas_used = result.gas_used.checked_add_result(outcome.used_gas)?;
    result.compute_usage = safe_add_compute(result.compute_usage, outcome.compute_usage)?;
    result.logs.extend(outcome.logs);
    result.profile.merge(&outcome.profile);
    if execution_succeeded {
        // Fetch metadata for PromiseYield timeout queue
        let mut promise_yield_indices = get_promise_yield_indices(state_update).unwrap_or_default();
        let initial_promise_yield_indices = promise_yield_indices.clone();

        let mut new_receipts: Vec<_> = receipt_manager
            .action_receipts
            .into_iter()
            .map(|receipt| {
                // If the newly created receipt is a PromiseYield, enqueue a timeout for it
                if receipt.is_promise_yield {
                    enqueue_promise_yield_timeout(
                        state_update,
                        &mut promise_yield_indices,
                        account_id.clone(),
                        receipt.input_data_ids[0],
                        apply_state.block_height
                            + config.wasm_config.limit_config.yield_timeout_length_in_blocks,
                    );
                }

                let new_receipt = if ProtocolFeature::DeterministicAccountIds
                    .enabled(apply_state.current_protocol_version)
                {
                    let new_action_receipt = ActionReceiptV2 {
                        signer_id: action_receipt.signer_id().clone(),
                        signer_public_key: action_receipt.signer_public_key().clone(),
                        refund_to: receipt.refund_to,
                        gas_price: action_receipt.gas_price(),
                        output_data_receivers: receipt.output_data_receivers,
                        input_data_ids: receipt.input_data_ids,
                        actions: receipt.actions,
                    };
                    if receipt.is_promise_yield {
                        ReceiptEnum::PromiseYieldV2(new_action_receipt)
                    } else {
                        ReceiptEnum::ActionV2(new_action_receipt)
                    }
                } else {
                    let new_action_receipt = ActionReceipt {
                        signer_id: action_receipt.signer_id().clone(),
                        signer_public_key: action_receipt.signer_public_key().clone(),
                        gas_price: action_receipt.gas_price(),
                        output_data_receivers: receipt.output_data_receivers,
                        input_data_ids: receipt.input_data_ids,
                        actions: receipt.actions,
                    };
                    if receipt.is_promise_yield {
                        ReceiptEnum::PromiseYield(new_action_receipt)
                    } else {
                        ReceiptEnum::Action(new_action_receipt)
                    }
                };

                Receipt::V0(ReceiptV0 {
                    predecessor_id: account_id.clone(),
                    receiver_id: receipt.receiver_id,
                    // Actual receipt ID is set in the Runtime.apply_action_receipt(...) in the
                    // "Generating receipt IDs" section
                    receipt_id: CryptoHash::default(),
                    receipt: new_receipt,
                })
            })
            .collect();

        // Create data receipts for resumed yields
        new_receipts.extend(receipt_manager.data_receipts.into_iter().map(|receipt| {
            let new_data_receipt = DataReceipt { data_id: receipt.data_id, data: receipt.data };

            Receipt::V0(ReceiptV0 {
                predecessor_id: account_id.clone(),
                receiver_id: account_id.clone(),
                // Actual receipt ID is set in the Runtime.apply_action_receipt(...) in the
                // "Generating receipt IDs" section
                receipt_id: CryptoHash::default(),
                receipt: if receipt.is_promise_resume {
                    ReceiptEnum::PromiseResume(new_data_receipt)
                } else {
                    ReceiptEnum::Data(new_data_receipt)
                },
            })
        }));

        // Commit metadata for yielded promises queue
        if promise_yield_indices != initial_promise_yield_indices {
            set_promise_yield_indices(state_update, &promise_yield_indices);
        }

        account.set_amount(outcome.balance);
        account.set_storage_usage(outcome.storage_usage);
        result.result = Ok(outcome.return_data);
        result.new_receipts.extend(new_receipts);
    }

    Ok(())
}

/// Runs given function call with given context / apply state.
pub(crate) fn execute_function_call(
    contract: Box<dyn near_vm_runner::PreparedContract>,
    apply_state: &ApplyState,
    runtime_ext: &mut RuntimeExt,
    predecessor_id: &AccountId,
    action_receipt: &VersionedActionReceipt,
    promise_results: Arc<[near_vm_runner::logic::types::PromiseResult]>,
    function_call: &FunctionCallAction,
    action_hash: &CryptoHash,
    config: &RuntimeConfig,
    is_last_action: bool,
    view_config: Option<ViewConfig>,
) -> Result<VMOutcome, RuntimeError> {
    let account_id = runtime_ext.account_id().clone();
    tracing::debug!(%account_id, "calling the contract");
    // Output data receipts are ignored if the function call is not the last action in the batch.
    let output_data_receivers: Vec<_> = if is_last_action {
        action_receipt.output_data_receivers().iter().map(|r| r.receiver_id.clone()).collect()
    } else {
        vec![]
    };
    let random_seed =
        near_primitives::utils::create_random_seed(*action_hash, apply_state.random_seed);
    let context = VMContext {
        current_account_id: runtime_ext.account_id().clone(),
        signer_account_id: action_receipt.signer_id().clone(),
        signer_account_pk: borsh::to_vec(&action_receipt.signer_public_key())
            .expect("Failed to serialize"),
        predecessor_account_id: predecessor_id.clone(),
        refund_to_account_id: action_receipt.refund_to().as_ref().unwrap_or(predecessor_id).clone(),
        input: Rc::from(function_call.args.clone()),
        promise_results,
        block_height: apply_state.block_height,
        block_timestamp: apply_state.block_timestamp,
        epoch_height: apply_state.epoch_height,
        account_balance: runtime_ext.account().amount(),
        account_locked_balance: runtime_ext.account().locked(),
        storage_usage: runtime_ext.account().storage_usage(),
        account_contract: runtime_ext.account().contract().into_owned(),
        attached_deposit: function_call.deposit,
        prepaid_gas: function_call.gas,
        random_seed,
        view_config,
        output_data_receivers,
    };

    near_vm_runner::reset_metrics();
    let result = near_vm_runner::run(contract, runtime_ext, &context, Arc::clone(&config.fees));
    near_vm_runner::report_metrics(
        &apply_state.shard_id.to_string(),
        &apply_state.apply_reason.to_string(),
    );

    // There are many specific errors that the runtime can encounter.
    // Some can be translated to the more general `RuntimeError`, which allows to pass
    // the error up to the caller. For all other cases, panicking here is better
    // than leaking the exact details further up.
    // Note that this does not include errors caused by user code / input, those are
    // stored in outcome.aborted.
    let mut outcome = match result {
        Err(VMRunnerError::ContractCodeNotPresent) => {
            let error = FunctionCallError::CompilationError(CompilationError::CodeDoesNotExist {
                account_id: account_id.as_str().into(),
            });
            return Ok(VMOutcome::nop_outcome(error));
        }
        Err(VMRunnerError::ExternalError(any_err)) => {
            let err: ExternalError =
                any_err.downcast().expect("Downcasting AnyError should not fail");
            return Err(match err {
                ExternalError::StorageError(err) => err.into(),
                ExternalError::ValidatorError(err) => RuntimeError::ValidatorError(err),
            });
        }
        Err(VMRunnerError::InconsistentStateError(
            err @ InconsistentStateError::IntegerOverflow,
        )) => return Err(StorageError::StorageInconsistentState(err.to_string()).into()),
        Err(VMRunnerError::CacheError(err)) => {
            metrics::FUNCTION_CALL_PROCESSED_CACHE_ERRORS.with_label_values(&[(&err).into()]).inc();
            return Err(StorageError::StorageInconsistentState(err.to_string()).into());
        }
        Err(VMRunnerError::LoadingError(msg)) => {
            panic!("Contract runtime failed to load a contract: {msg}")
        }
        Err(VMRunnerError::Nondeterministic(msg)) => {
            panic!("Contract runner returned non-deterministic error '{}', aborting", msg)
        }
        Err(VMRunnerError::WasmUnknownError { debug_message }) => {
            panic!("Wasmer returned unknown message: {}", debug_message)
        }
        Ok(r) => r,
    };

    if !context.view_config.is_some() {
        let unused_gas = function_call.gas.saturating_sub(outcome.used_gas);
        let distributed = runtime_ext.receipt_manager.distribute_gas(unused_gas)?;
        outcome.used_gas = outcome.used_gas.checked_add_result(distributed)?;
    }

    Ok(outcome)
}

/// See #11703 for more details
#[cfg(feature = "test_features")]
fn apply_recorded_storage_garbage(function_call: &FunctionCallAction, state_update: &TrieUpdate) {
    if let Some(garbage_size_mbs) = function_call
        .method_name
        .strip_prefix("internal_record_storage_garbage_")
        .and_then(|suf| suf.parse::<usize>().ok())
    {
        if state_update.trie.record_storage_garbage(garbage_size_mbs) {
            tracing::warn!(%garbage_size_mbs, "generated storage proof garbage");
        }
    }
}

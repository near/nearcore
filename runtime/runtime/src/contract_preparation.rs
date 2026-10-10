//! Contract metadata preparation shared by receipt execution, pipelining, and view calls.

use crate::contract_code::RuntimeContractIdentifier;
use crate::ext::RuntimeContractExt;
use near_parameters::RuntimeConfig;
use near_parameters::vm::Config as VmConfig;
use near_primitives::account::AccountContract;
use near_primitives::config::ViewConfig;
use near_primitives::transaction::FunctionCallAction;
use near_primitives::types::{AccountId, ProtocolVersion};
use near_store::contract::ContractStorage;
use near_store::trie::AccessOptions;
use near_store::{StorageError, TrieUpdate};
use near_vm_runner::logic::{ContractLoadingAbort, GasCounter, PreparedContractGasCounter};
use near_vm_runner::{
    CompilePriority, ContractRuntimeCache, PreparedContract, prepare_with_priority,
};
use std::sync::Arc;

/// Metadata preparation either permits loading or produces an early abort.
pub(crate) enum ContractPreparation {
    Ready {
        contract: RuntimeContractExt,
        // The counter is ~2.5kB, keep it boxed for the compilation-queue handoff.
        gas_counter: Box<PreparedContractGasCounter>,
    },
    /// Empty method, loading fee not covered, or no code. Nothing is loaded or recorded.
    Aborted(ContractLoadingAbort),
}

/// Create a gas counter, pay the loading base, resolve identity/size metadata,
/// then pay the byte fee. With FixContractLoadingCost, ready preparations have
/// paid the entire loading fee.
///
/// Base-charge failures abort before metadata lookup. Byte-charge failures
/// abort before loading code. Storage failures propagate separately with the
/// caller's witness policy.
pub(crate) fn prepare_contract_metadata(
    config: &RuntimeConfig,
    storage: &ContractStorage,
    chain_id: &str,
    account_id: &AccountId,
    account_contract: AccountContract,
    state_update: &TrieUpdate,
    function_call: &FunctionCallAction,
    view_config: Option<&ViewConfig>,
    access: AccessOptions,
    protocol_version: ProtocolVersion,
) -> Result<ContractPreparation, StorageError> {
    let max_gas_burnt = match view_config {
        Some(ViewConfig { max_gas_burnt }) => *max_gas_burnt,
        None => config.wasm_config.limit_config.max_gas_burnt,
    };
    let gas_counter = GasCounter::new(
        config.wasm_config.ext_costs.clone(),
        max_gas_burnt,
        config.wasm_config.regular_op_cost,
        function_call.gas,
        view_config.is_some(),
    );
    if !config.wasm_config.fix_contract_loading_cost {
        let identifier = RuntimeContractIdentifier::resolve(
            account_id,
            account_contract,
            state_update,
            chain_id,
            access,
            protocol_version,
        )?;
        let contract = RuntimeContractExt { storage: storage.clone(), identifier };
        let gas_counter = Box::new(PreparedContractGasCounter::Legacy(gas_counter));
        return Ok(ContractPreparation::Ready { contract, gas_counter });
    }
    let base_charged = match gas_counter.charge_loading_base(&function_call.method_name) {
        Ok(base_charged) => base_charged,
        Err(abort) => return Ok(ContractPreparation::Aborted(abort)),
    };
    let identifier = RuntimeContractIdentifier::resolve(
        account_id,
        account_contract,
        state_update,
        chain_id,
        access,
        protocol_version,
    )?;
    let Some(code_len) =
        identifier.resolve_code_len(state_update, access, chain_id, protocol_version)?
    else {
        let abort = base_charged.without_code(account_id.as_str());
        return Ok(ContractPreparation::Aborted(abort));
    };
    let loading_fee_paid = match base_charged.charge_bytes(code_len) {
        Ok(loading_fee_paid) => loading_fee_paid,
        Err(abort) => return Ok(ContractPreparation::Aborted(abort)),
    };
    let contract = RuntimeContractExt { storage: storage.clone(), identifier };
    let gas_counter = Box::new(PreparedContractGasCounter::Paid(loading_fee_paid));
    Ok(ContractPreparation::Ready { contract, gas_counter })
}

pub(crate) fn prepare_function_call(
    code_ext: &RuntimeContractExt,
    cache: Option<&dyn ContractRuntimeCache>,
    config: Arc<VmConfig>,
    gas_counter: Box<PreparedContractGasCounter>,
    method_name: &str,
    priority: CompilePriority,
) -> Box<dyn PreparedContract> {
    prepare_with_priority(code_ext, config, cache, *gas_counter, method_name, priority)
}

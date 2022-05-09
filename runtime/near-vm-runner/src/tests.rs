mod cache;
mod compile_errors;
mod contract_preload;
mod rs_contract;
mod runtime_errors;
mod ts_contract;
mod wasm_validation;

use crate::vm_kind::VMKind;

use near_primitives::checked_feature;
use near_primitives::contract::ContractCode;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::version::ProtocolVersion;
use near_vm_errors::VMError;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::{VMConfig, VMContext, VMOutcome};

const CURRENT_ACCOUNT_ID: &str = "alice";
const SIGNER_ACCOUNT_ID: &str = "bob";
const SIGNER_ACCOUNT_PK: [u8; 3] = [0, 1, 2];
const PREDECESSOR_ACCOUNT_ID: &str = "carol";

const LATEST_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::MAX;

fn with_vm_variants(runner: fn(VMKind) -> ()) {
    #[cfg(all(feature = "wasmer0_vm", target_arch = "x86_64"))]
    runner(VMKind::Wasmer0);

    #[cfg(feature = "wasmtime_vm")]
    runner(VMKind::Wasmtime);

    #[cfg(all(feature = "wasmer2_vm", target_arch = "x86_64"))]
    runner(VMKind::Wasmer2);
}

fn create_context(input: Vec<u8>) -> VMContext {
    VMContext {
        current_account_id: CURRENT_ACCOUNT_ID.parse().unwrap(),
        signer_account_id: SIGNER_ACCOUNT_ID.parse().unwrap(),
        signer_account_pk: Vec::from(&SIGNER_ACCOUNT_PK[..]),
        predecessor_account_id: PREDECESSOR_ACCOUNT_ID.parse().unwrap(),
        input,
        block_index: 10,
        block_timestamp: 42,
        epoch_height: 1,
        account_balance: 2u128,
        account_locked_balance: 0,
        storage_usage: 12,
        attached_deposit: 2u128,
        prepaid_gas: 10_u64.pow(14),
        random_seed: vec![0, 1, 2],
        view_config: None,
        output_data_receivers: vec![],
    }
}

/// Exhaustive parameters for making a simple contract call.
/// Calling with no protocol version specified will use a test configuration.
fn make_simple_contract_call_ex(
    code: &[u8],
    method_name: &str,
    vm_kind: VMKind,
    protocol_version: Option<ProtocolVersion>,
    prepaid_gas: u64,
) -> (VMOutcome, Option<VMError>) {
    let mut fake_external = MockedExternal::new();
    let mut context = create_context(vec![]);
    context.prepaid_gas = prepaid_gas;
    let (config, fees) = if let Some(protocol_version) = protocol_version {
        let runtime_config_store = RuntimeConfigStore::new(None);
        let runtime_config = runtime_config_store.get_config(protocol_version);
        (runtime_config.wasm_config.clone(), runtime_config.transaction_costs.clone())
    } else {
        (VMConfig::test(), RuntimeFeesConfig::test())
    };

    let promise_results = vec![];
    let code = ContractCode::new(code.to_vec(), None);

    crate::runner::run_with_vm_kind(
        &code,
        method_name,
        &mut fake_external,
        context,
        &config,
        &fees,
        &promise_results,
        protocol_version.unwrap_or(LATEST_PROTOCOL_VERSION),
        None,
        vm_kind,
    )
    .outcome_error()
}

fn make_simple_contract_call_with_protocol_version_vm(
    code: &[u8],
    method_name: &str,
    protocol_version: ProtocolVersion,
    vm_kind: VMKind,
) -> (VMOutcome, Option<VMError>) {
    make_simple_contract_call_ex(code, method_name, vm_kind, Some(protocol_version), 10u64.pow(14))
}

fn make_simple_contract_call_with_gas_vm(
    code: &[u8],
    method_name: &str,
    prepaid_gas: u64,
    vm_kind: VMKind,
) -> (VMOutcome, Option<VMError>) {
    make_simple_contract_call_ex(code, method_name, vm_kind, None, prepaid_gas)
}

fn make_simple_contract_call_vm(
    code: &[u8],
    method_name: &str,
    vm_kind: VMKind,
) -> (VMOutcome, Option<VMError>) {
    make_simple_contract_call_ex(code, method_name, vm_kind, None, 10u64.pow(14))
}

#[track_caller]
fn gas_and_error_match(
    outcome_and_error: (VMOutcome, Option<VMError>),
    expected_gas: Option<u64>,
    expected_error: Option<VMError>,
) {
    match expected_gas {
        Some(gas) => {
            let outcome = outcome_and_error.0;
            assert_eq!(outcome.used_gas, gas, "used gas differs");
            assert_eq!(outcome.burnt_gas, gas, "burnt gas differs");
        }
        None => {
            assert_eq!(outcome_and_error.0.used_gas, 0, "used gas non-zero");
            assert_eq!(outcome_and_error.0.burnt_gas, 0, "burnt gas non-zero");
        }
    }

    assert_eq!(outcome_and_error.1, expected_error);
}

/// Small helper to compute expected loading gas cost charged before loading.
/// Includes some hard-coded parameter values that would have to be updated
/// if they change in the future.
fn prepaid_loading_gas(bytes: usize) -> Option<u64> {
    let expected_gas = if checked_feature!(
        "protocol_feature_fix_contract_loading_cost",
        FixContractLoadingCost,
        LATEST_PROTOCOL_VERSION
    ) {
        Some(35445963 + bytes as u64 * 216750)
    } else {
        None
    };
    expected_gas
}

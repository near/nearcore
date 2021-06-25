mod contract_preload;
mod error_cases;
mod invalid_contracts;
mod rs_contract;
mod ts_contract;

use near_primitives::contract::ContractCode;
use near_primitives::profile::ProfileData;
use wabt::Wat2Wasm;

use crate::{run_vm, VMKind};
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::CompiledContractCache;
use near_primitives::version::ProtocolVersion;
use near_vm_errors::VMError;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::{VMConfig, VMContext, VMOutcome};
#[cfg(feature = "wasmer0_vm")]
use once_cell::sync::OnceCell;

const CURRENT_ACCOUNT_ID: &str = "alice";
const SIGNER_ACCOUNT_ID: &str = "bob";
const SIGNER_ACCOUNT_PK: [u8; 3] = [0, 1, 2];
const PREDECESSOR_ACCOUNT_ID: &str = "carol";

const LATEST_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::MAX;

fn with_vm_variants(runner: fn(VMKind) -> ()) {
    #[cfg(feature = "wasmer0_vm")]
    static WASMER0_TRAP_HANDLER_SETUP: OnceCell<()> = OnceCell::new();
    #[cfg(feature = "wasmer0_vm")]
    WASMER0_TRAP_HANDLER_SETUP.get_or_init(|| {
        // This is a HACK. When wasmer2 is enabled, this test must be run once before bad_import tests to ensure wasmer 0 trap handler is setup
        // Otherwise tests of cargo test -p near-vm-runner --lib tests::error_cases fails. Possibly related to wasmer2 installed a different,
        // non compatible global trap handler in wasmer/lib/vm/src/trap/traphandlers.rs.
        // Alternative possible fix is have separate test binaries, one run all tests with wasmer0, another with wasmer2. But that's repetitive
        // If only one of wasmer0 and wasmer2 is enabled, tests pass without this hack, so should not be a problem after we fully move to wasmer2
        let _ =
            make_simple_contract_call_vm(&error_cases::trap_contract(), "hello", VMKind::Wasmer0);
    });

    #[cfg(feature = "wasmer0_vm")]
    runner(VMKind::Wasmer0);

    #[cfg(feature = "wasmtime_vm")]
    runner(VMKind::Wasmtime);

    #[cfg(feature = "wasmer2_vm")]
    runner(VMKind::Wasmer2);
}

fn create_context(input: Vec<u8>) -> VMContext {
    VMContext {
        current_account_id: CURRENT_ACCOUNT_ID.to_owned(),
        signer_account_id: SIGNER_ACCOUNT_ID.to_owned(),
        signer_account_pk: Vec::from(&SIGNER_ACCOUNT_PK[..]),
        predecessor_account_id: PREDECESSOR_ACCOUNT_ID.to_owned(),
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
        is_view: false,
        output_data_receivers: vec![],
    }
}

fn make_simple_contract_call_with_gas_vm(
    code: &[u8],
    method_name: &str,
    prepaid_gas: u64,
    vm_kind: VMKind,
) -> (Option<VMOutcome>, Option<VMError>) {
    let mut fake_external = MockedExternal::new();
    let mut context = create_context(vec![]);
    context.prepaid_gas = prepaid_gas;
    let config = VMConfig::default();
    let fees = RuntimeFeesConfig::default();

    let promise_results = vec![];

    let code = ContractCode::new(code.to_vec(), None);
    run_vm(
        &code,
        method_name,
        &mut fake_external,
        context,
        &config,
        &fees,
        &promise_results,
        vm_kind,
        LATEST_PROTOCOL_VERSION,
        None,
        ProfileData::new(),
    )
}

fn make_simple_contract_call_vm(
    code: &[u8],
    method_name: &str,
    vm_kind: VMKind,
) -> (Option<VMOutcome>, Option<VMError>) {
    make_simple_contract_call_with_gas_vm(code, method_name, 10u64.pow(14), vm_kind)
}

fn wat2wasm_no_validate(wat: &str) -> Vec<u8> {
    Wat2Wasm::new().validate(false).convert(wat).unwrap().as_ref().to_vec()
}

fn make_cached_contract_call_vm(
    cache: &mut dyn CompiledContractCache,
    code: &[u8],
    method_name: &str,
    prepaid_gas: u64,
    vm_kind: VMKind,
) -> (Option<VMOutcome>, Option<VMError>) {
    let mut fake_external = MockedExternal::new();
    let mut context = create_context(vec![]);
    let config = VMConfig::default();
    let fees = RuntimeFeesConfig::default();
    let promise_results = vec![];
    context.prepaid_gas = prepaid_gas;
    let code = ContractCode::new(code.to_vec(), None);

    run_vm(
        &code,
        method_name,
        &mut fake_external,
        context.clone(),
        &config,
        &fees,
        &promise_results,
        vm_kind,
        LATEST_PROTOCOL_VERSION,
        Some(cache),
        ProfileData::new(),
    )
}

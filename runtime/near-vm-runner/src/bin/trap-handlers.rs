use near_primitives::contract::ContractCode;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::version::ProtocolVersion;
use near_vm_errors::{CompilationError, FunctionCallError, PrepareError, WasmTrap};
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::profile::ProfileData;
use near_vm_logic::VMConfig;
use near_vm_logic::{VMContext, VMOutcome};
use near_vm_runner::{run_vm, VMError, VMKind};

const CURRENT_ACCOUNT_ID: &str = "alice";
const SIGNER_ACCOUNT_ID: &str = "bob";
const SIGNER_ACCOUNT_PK: [u8; 3] = [0, 1, 2];
const PREDECESSOR_ACCOUNT_ID: &str = "carol";

const LATEST_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::MAX;

fn with_vm_variants(runner: fn(VMKind) -> ()) {
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

pub fn trap_contract() -> Vec<u8> {
    wat::parse_str(
        r#"
            (module
              (type (;0;) (func))
              (func (;0;) (type 0) (unreachable))
              (export "hello" (func 0))
            )"#,
    )
    .unwrap()
}

fn test_trap_contract() {
    with_vm_variants(|vm_kind: VMKind| {
        match vm_kind {
            VMKind::Wasmer0 | VMKind::Wasmer2 => {}
            // All contracts leading to hardware traps can not run concurrently on Wasmtime and Wasmer,
            // Restore, once get rid of Wasmer 0.x.
            VMKind::Wasmtime => return,
        }
        make_simple_contract_call_vm(&trap_contract(), "hello", vm_kind);
    });
    println!("done call test_trap_contract");
}

fn div_by_zero_contract() -> Vec<u8> {
    wat::parse_str(
        r#"
            (module
              (type (;0;) (func))
              (func (;0;) (type 0)
                i32.const 1
                i32.const 0
                i32.div_s
                return
              )
              (export "hello" (func 0))
            )"#,
    )
    .unwrap()
}

fn test_div_by_zero_contract() {
    with_vm_variants(|vm_kind: VMKind| {
        match vm_kind {
            VMKind::Wasmer0 | VMKind::Wasmer2 => {}
            // All contracts leading to hardware traps can not run concurrently on Wasmtime and Wasmer,
            // Check if can restore, once get rid of Wasmer 0.x.
            VMKind::Wasmtime => return,
        }
        make_simple_contract_call_vm(&div_by_zero_contract(), "hello", vm_kind);
    });
    println!("done call test_div_by_zero_contract");
}

fn bad_import_global(env: &str) -> Vec<u8> {
    wat::parse_str(format!(
        r#"
            (module
              (type (;0;) (func))
              (import "{}" "input" (global (;0;) i32))
              (func (;0;) (type 0))
              (export "hello" (func 0))
            )"#,
        env
    ))
    .unwrap()
}

fn test_bad_import_1() {
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_vm(&bad_import_global("wtf"), "hello", vm_kind),
            (
                None,
                Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
                    CompilationError::PrepareError(PrepareError::Instantiate)
                )))
            )
        )
    });
    println!("done call test_bad_import_1");
}

fn main() {
    test_bad_import_1();
    test_trap_contract();
    test_trap_contract();
    test_div_by_zero_contract();
    test_div_by_zero_contract();
    test_trap_contract();
}

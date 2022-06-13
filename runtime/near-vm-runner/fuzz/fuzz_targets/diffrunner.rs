#![no_main]

use near_primitives::contract::ContractCode;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::version::PROTOCOL_VERSION;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::VMConfig;
use near_vm_runner::internal::VMKind;
use near_vm_runner::VMResult;
use near_vm_runner_fuzz::{ArbitraryModule, create_context, find_entry_point};

libfuzzer_sys::fuzz_target!(|module: ArbitraryModule| {
    let code = ContractCode::new(module.0.module.to_bytes(), None);
    let wasmer2 = run_fuzz(&code, VMKind::Wasmer2);
    let wasmtime = run_fuzz(&code, VMKind::Wasmtime);
    assert_eq!(wasmer2, wasmtime);
});

fn run_fuzz(code: &ContractCode, vm_kind: VMKind) -> VMResult {
    let mut fake_external = MockedExternal::new();
    let mut context = create_context(vec![]);
    context.prepaid_gas = 10u64.pow(14);
    let config = VMConfig::test();
    let fees = RuntimeFeesConfig::test();

    let promise_results = vec![];

    let method_name = find_entry_point(code).unwrap_or_else(|| "main".to_string());
    vm_kind.runtime(config).unwrap().run(
        code,
        &method_name,
        &mut fake_external,
        context,
        &fees,
        &promise_results,
        PROTOCOL_VERSION,
        None,
    )
}

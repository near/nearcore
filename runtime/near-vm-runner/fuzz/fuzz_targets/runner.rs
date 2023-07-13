#![no_main]

use near_primitives::contract::ContractCode;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::version::PROTOCOL_VERSION;
use near_vm_runner::internal::VMKind;
use near_vm_runner::logic::mocks::mock_external::MockedExternal;
use near_vm_runner::logic::{VMConfig, VMOutcome};
use near_vm_runner_fuzz::{create_context, find_entry_point, ArbitraryModule};

libfuzzer_sys::fuzz_target!(|module: ArbitraryModule| {
    let code = ContractCode::new(module.0.module.to_bytes(), None);
    let _result = run_fuzz(&code, VMKind::for_protocol_version(PROTOCOL_VERSION));
});

fn run_fuzz(code: &ContractCode, vm_kind: VMKind) -> VMOutcome {
    let mut fake_external = MockedExternal::new();
    let mut context = create_context(vec![]);
    context.prepaid_gas = 10u64.pow(14);
    let mut config = VMConfig::test();
    config.limit_config.wasmer2_stack_limit = i32::MAX; // If we can crash wasmer2 even without the secondary stack limit it's still good to know
    let fees = RuntimeFeesConfig::test();

    let promise_results = vec![];

    let method_name = find_entry_point(code).unwrap_or_else(|| "main".to_string());
    vm_kind
        .runtime(config)
        .unwrap()
        .run(
            code,
            &method_name,
            &mut fake_external,
            context,
            &fees,
            &promise_results,
            PROTOCOL_VERSION,
            None,
        )
        .unwrap_or_else(|err| panic!("fatal error: {err:?}"))
}

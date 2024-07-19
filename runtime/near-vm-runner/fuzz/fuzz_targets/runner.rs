#![no_main]

use near_parameters::{RuntimeConfig, RuntimeConfigStore};
use near_primitives::version::PROTOCOL_VERSION;
use near_vm_runner::internal::VMKindExt;
use near_vm_runner::logic::mocks::mock_external::MockedExternal;
use near_vm_runner::logic::VMOutcome;
use near_vm_runner::ContractCode;
use near_vm_runner_fuzz::{create_context, find_entry_point, ArbitraryModule};
use std::sync::Arc;

libfuzzer_sys::fuzz_target!(|module: ArbitraryModule| {
    let code = ContractCode::new(module.0.module.to_bytes(), None);
    let config_store = RuntimeConfigStore::new(None);
    let config = config_store.get_config(PROTOCOL_VERSION);
    let _result = run_fuzz(&code, Arc::clone(config));
});

fn run_fuzz(code: &ContractCode, config: Arc<RuntimeConfig>) -> VMOutcome {
    let mut fake_external = MockedExternal::with_code(code.clone_for_tests());
    let method_name = find_entry_point(code).unwrap_or_else(|| "main".to_string());
    let mut context = create_context(vec![]);
    context.prepaid_gas = 10u64.pow(14);
    let mut wasm_config = near_parameters::vm::Config::clone(&config.wasm_config);
    wasm_config.limit_config.wasmer2_stack_limit = i32::MAX; // If we can crash wasmer2 even without the secondary stack limit it's still good to know
    let vm_kind = config.wasm_config.vm_kind;
    let fees = Arc::clone(&config.fees);
    let gas_counter = context.make_gas_counter(&wasm_config);
    vm_kind
        .runtime(wasm_config.into())
        .unwrap()
        .prepare(&fake_external, None, gas_counter, &method_name)
        .run(&mut fake_external, &context, fees)
        .unwrap_or_else(|err| panic!("fatal error: {err:?}"))
}

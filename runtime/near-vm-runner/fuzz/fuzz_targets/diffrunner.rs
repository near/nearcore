#![no_main]

use near_primitives::contract::ContractCode;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::version::PROTOCOL_VERSION;
use near_vm_runner::internal::VMKind;
use near_vm_runner::logic::errors::FunctionCallError;
use near_vm_runner::logic::mocks::mock_external::MockedExternal;
use near_vm_runner::logic::{VMConfig, VMOutcome};
use near_vm_runner_fuzz::{create_context, find_entry_point, ArbitraryModule};

libfuzzer_sys::fuzz_target!(|module: ArbitraryModule| {
    let code = ContractCode::new(module.0.module.to_bytes(), None);
    let near_vm = run_fuzz(&code, VMKind::NearVm);
    let wasmtime = run_fuzz(&code, VMKind::Wasmtime);
    assert_eq!(near_vm, wasmtime);
});

fn run_fuzz(code: &ContractCode, vm_kind: VMKind) -> VMOutcome {
    let mut fake_external = MockedExternal::new();
    let mut context = create_context(vec![]);
    context.prepaid_gas = 10u64.pow(14);
    let mut config = VMConfig::test();
    config.limit_config.contract_prepare_version =
        near_vm_runner::logic::ContractPrepareVersion::V2;
    let fees = RuntimeFeesConfig::test();

    let promise_results = vec![];

    let method_name = find_entry_point(code).unwrap_or_else(|| "main".to_string());
    let res = vm_kind.runtime(config).unwrap().run(
        code,
        &method_name,
        &mut fake_external,
        context,
        &fees,
        &promise_results,
        PROTOCOL_VERSION,
        None,
    );

    // Remove the VMError message details as they can differ between runtimes
    // TODO: maybe there's actually things we could check for equality here too?
    match res {
        Ok(mut outcome) => {
            if outcome.aborted.is_some() {
                outcome.logs = vec!["[censored]".to_owned()];
                outcome.aborted =
                    Some(FunctionCallError::LinkError { msg: "[censored]".to_owned() });
            }
            outcome
        }
        Err(err) => panic!("fatal error: {err:?}"),
    }
}

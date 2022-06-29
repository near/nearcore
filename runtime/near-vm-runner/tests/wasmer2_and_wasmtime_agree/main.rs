use arbitrary::Arbitrary;
use bolero::check;
use near_primitives::contract::ContractCode;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::version::PROTOCOL_VERSION;
use near_vm_errors::{FunctionCallError, VMError};
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::VMConfig;
use near_vm_runner::internal::VMKind;
use near_vm_runner::VMResult;

#[path = "../common/mod.rs"]
mod common;
use common::{create_context, find_entry_point, ArbitraryModule};

fn main() {
    check!().for_each(|data: &[u8]| {
        let module = ArbitraryModule::arbitrary(&mut arbitrary::Unstructured::new(data));
        let module = match module {
            Ok(m) => m,
            Err(_) => return,
        };
        let code = ContractCode::new(module.0.module.to_bytes(), None);
        let wasmer2 = run_fuzz(&code, VMKind::Wasmer2);
        let wasmtime = run_fuzz(&code, VMKind::Wasmtime);
        assert_eq!(wasmer2, wasmtime);
    });
}

fn run_fuzz(code: &ContractCode, vm_kind: VMKind) -> VMResult {
    let mut fake_external = MockedExternal::new();
    let mut context = create_context(vec![]);
    context.prepaid_gas = 10u64.pow(14);
    let config = VMConfig::test();
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
        VMResult::Ok(err) => VMResult::Ok(err),
        VMResult::Aborted(mut outcome, _err) => {
            outcome.logs = vec!["[censored]".to_owned()];
            VMResult::Aborted(
                outcome,
                VMError::FunctionCallError(FunctionCallError::Nondeterministic(
                    "[censored]".to_owned(),
                )),
            )
        }
    }
}

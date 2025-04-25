use super::test_vm_config;
use crate::ContractCode;
use crate::logic::VMContext;
use crate::logic::errors::FunctionCallError;
use crate::logic::mocks::mock_external::MockedExternal;
use crate::runner::{VMKindExt, VMResult};
use near_parameters::RuntimeFeesConfig;
use near_parameters::vm::VMKind;
use near_test_contracts::ArbitraryModule;
use std::sync::Arc;

/// Finds a no-parameter exported function, something like `(func (export "entry-point"))`.
#[cfg(feature = "prepare")]
pub fn find_entry_point(contract: &ContractCode) -> Option<String> {
    use crate::internal::wasmparser::{Export, ExternalKind, Parser, Payload, TypeDef};
    let mut tys = Vec::new();
    let mut fns = Vec::new();
    for payload in Parser::default().parse_all(contract.code()) {
        match payload {
            Ok(Payload::FunctionSection(rdr)) => fns.extend(rdr),
            Ok(Payload::TypeSection(rdr)) => tys.extend(rdr),
            Ok(Payload::ExportSection(rdr)) => {
                for export in rdr {
                    if let Ok(Export { field, kind: ExternalKind::Function, index }) = export {
                        if let Some(&Ok(ty_index)) = fns.get(index as usize) {
                            if let Some(Ok(TypeDef::Func(func_type))) = tys.get(ty_index as usize) {
                                if func_type.params.is_empty() && func_type.returns.is_empty() {
                                    return Some(field.to_string());
                                }
                            }
                        }
                    }
                }
            }
            _ => (),
        }
    }
    None
}

pub fn create_context(input: Vec<u8>) -> VMContext {
    VMContext {
        current_account_id: "alice".parse().unwrap(),
        signer_account_id: "bob".parse().unwrap(),
        signer_account_pk: vec![0, 1, 2, 3, 4],
        predecessor_account_id: "carol".parse().unwrap(),
        input,
        promise_results: Vec::new().into(),
        block_height: 10,
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

#[cfg(feature = "prepare")]
fn run_fuzz(code: &ContractCode, vm_kind: VMKind) -> VMResult {
    let mut fake_external = MockedExternal::with_code(code.clone_for_tests());
    let method_name = find_entry_point(code).unwrap_or_else(|| "main".to_string());
    let mut context = create_context(vec![]);
    context.prepaid_gas = 10u64.pow(14);
    let config = test_vm_config();
    let fees = Arc::new(RuntimeFeesConfig::test());
    let gas_counter = context.make_gas_counter(&config);
    let mut res = vm_kind
        .runtime(config.into())
        .unwrap()
        .prepare(&fake_external, None, gas_counter, &method_name)
        .run(&mut fake_external, &context, Arc::clone(&fees));

    // Remove the VMError message details as they can differ between runtimes
    // TODO: maybe there's actually things we could check for equality here too?
    match res {
        Ok(ref mut outcome) => {
            if outcome.aborted.is_some() {
                outcome.logs = vec!["[censored]".to_owned()];
                outcome.aborted =
                    Some(FunctionCallError::LinkError { msg: "[censored]".to_owned() });
            }
        }
        Err(err) => panic!("fatal error: {err:?}"),
    }
    res
}

#[test]
#[cfg(feature = "prepare")]
fn slow_test_current_vm_does_not_crash_fuzzer() {
    let config = test_vm_config();
    if config.vm_kind.is_available() {
        bolero::check!().with_arbitrary::<ArbitraryModule>().for_each(
            |module: &ArbitraryModule| {
                let code = ContractCode::new(module.0.to_bytes(), None);
                let _result = run_fuzz(&code, config.vm_kind);
            },
        );
    }
}

#[test]
#[cfg_attr(not(all(feature = "wasmtime_vm", feature = "near_vm", target_arch = "x86_64")), ignore)]
#[cfg(feature = "prepare")]
fn slow_test_near_vm_and_wasmtime_agree_fuzzer() {
    bolero::check!().with_arbitrary::<ArbitraryModule>().for_each(|module: &ArbitraryModule| {
        let code = ContractCode::new(module.0.to_bytes(), None);
        let near_vm = run_fuzz(&code, VMKind::NearVm).expect("fatal failure");
        let wasmtime = run_fuzz(&code, VMKind::Wasmtime).expect("fatal failure");
        assert_eq!(near_vm, wasmtime);
    });
}

#[test]
#[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
fn slow_test_near_vm_is_reproducible_fuzzer() {
    use crate::near_vm_runner::NearVM;
    use near_primitives_core::hash::CryptoHash;

    bolero::check!().with_arbitrary::<ArbitraryModule>().for_each(|module: &ArbitraryModule| {
        let code = ContractCode::new(module.0.to_bytes(), None);
        let config = std::sync::Arc::new(test_vm_config());
        let mut first_hash = None;
        for _ in 0..3 {
            let vm = NearVM::new(config.clone());
            let exec = match vm.compile_uncached(&code) {
                Ok(e) => e,
                Err(_) => return,
            };
            let code = exec.serialize().unwrap();
            let hash = CryptoHash::hash_bytes(&code);
            match first_hash {
                None => first_hash = Some(hash),
                Some(h) => assert_eq!(h, hash),
            }
        }
    })
}

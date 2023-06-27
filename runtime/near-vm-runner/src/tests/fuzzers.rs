use crate::internal::wasmparser::{Export, ExternalKind, Parser, Payload, TypeDef};
use crate::internal::VMKind;
use crate::logic::errors::FunctionCallError;
use crate::logic::mocks::mock_external::MockedExternal;
use crate::logic::{VMConfig, VMContext};
use crate::runner::VMResult;
use arbitrary::Arbitrary;
use bolero::check;
use core::fmt;
use near_primitives_core::contract::ContractCode;
use near_primitives_core::runtime::fees::RuntimeFeesConfig;
use near_primitives_core::version::PROTOCOL_VERSION;

/// Finds a no-parameter exported function, something like `(func (export "entry-point"))`.
pub fn find_entry_point(contract: &ContractCode) -> Option<String> {
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

/// Define a configuration for which [`available_imports`] is implemented. This
/// allows to specify the imports available in a [`ConfiguredModule`].
///
/// [`available_imports`]: wasm_smith::Config::available_imports
/// [`ConfiguredModule`]: wasm_smith::ConfiguredModule
#[derive(arbitrary::Arbitrary, Debug)]
pub struct ModuleConfig {}

impl wasm_smith::Config for ModuleConfig {
    /// Returns a WebAssembly module which imports all near host functions. The
    /// imports are grabbed from a compiled [test contract] which calls every
    /// host function in its method `sanity_check`.
    ///
    /// [test contract]: near_test_contracts::rs_contract
    fn available_imports(&self) -> Option<std::borrow::Cow<'_, [u8]>> {
        Some(near_test_contracts::rs_contract().into())
    }

    /// Make sure to canonicalize the NaNs, as otherwise behavior differs
    /// between wasmtime (that does not canonicalize) and near-vm (that
    /// should canonicalize)
    fn canonicalize_nans(&self) -> bool {
        true
    }
}

/// Wrapper to get more useful Debug.
pub struct ArbitraryModule(pub wasm_smith::ConfiguredModule<ModuleConfig>);

impl<'a> Arbitrary<'a> for ArbitraryModule {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        wasm_smith::ConfiguredModule::<ModuleConfig>::arbitrary(u).map(ArbitraryModule)
    }
}

impl fmt::Debug for ArbitraryModule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.0.module.to_bytes();
        write!(f, "{:?}", bytes)?;
        if let Ok(wat) = wasmprinter::print_bytes(&bytes) {
            write!(f, "\n{}", wat)?;
        }
        Ok(())
    }
}

fn run_fuzz(code: &ContractCode, vm_kind: VMKind) -> VMResult {
    let mut fake_external = MockedExternal::new();

    let mut context = create_context(vec![]);
    context.prepaid_gas = 10u64.pow(14);

    let mut config = VMConfig::test();
    config.limit_config.wasmer2_stack_limit = i32::MAX; // If we can crash wasmer2 even without the secondary stack limit it's still good to know

    let fees = RuntimeFeesConfig::test();

    let promise_results = vec![];

    let method_name = find_entry_point(code).unwrap_or_else(|| "main".to_string());
    let mut res = vm_kind.runtime(config).unwrap().run(
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
fn current_vm_does_not_crash() {
    check!().for_each(|data: &[u8]| {
        let module = ArbitraryModule::arbitrary(&mut arbitrary::Unstructured::new(data));
        let module = match module {
            Ok(m) => m,
            Err(_) => return,
        };
        let code = ContractCode::new(module.0.module.to_bytes(), None);
        let _result = run_fuzz(&code, VMKind::for_protocol_version(PROTOCOL_VERSION));
    });
}

#[test]
fn wasmer2_and_wasmtime_agree() {
    check!().for_each(|data: &[u8]| {
        let module = ArbitraryModule::arbitrary(&mut arbitrary::Unstructured::new(data));
        let module = match module {
            Ok(m) => m,
            Err(_) => return,
        };
        let code = ContractCode::new(module.0.module.to_bytes(), None);
        let wasmer2 = run_fuzz(&code, VMKind::Wasmer2).expect("fatal failure");
        let wasmtime = run_fuzz(&code, VMKind::Wasmtime).expect("fatal failure");
        assert_eq!(wasmer2, wasmtime);
        assert_eq!(wasmer2, wasmtime);
    });
}

#[test]
fn near_vm_and_wasmtime_agree() {
    check!().for_each(|data: &[u8]| {
        let module = ArbitraryModule::arbitrary(&mut arbitrary::Unstructured::new(data));
        let module = match module {
            Ok(m) => m,
            Err(_) => return,
        };
        let code = ContractCode::new(module.0.module.to_bytes(), None);
        let near_vm = run_fuzz(&code, VMKind::NearVm).expect("fatal failure");
        let wasmtime = run_fuzz(&code, VMKind::Wasmtime).expect("fatal failure");
        assert_eq!(near_vm, wasmtime);
    });
}

#[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
#[test]
fn near_vm_is_reproducible() {
    use crate::near_vm_runner::NearVM;
    use near_primitives::hash::CryptoHash;

    bolero::check!().for_each(|data: &[u8]| {
        if let Ok(module) = ArbitraryModule::arbitrary(&mut arbitrary::Unstructured::new(data)) {
            let code = ContractCode::new(module.0.module.to_bytes(), None);
            let config = VMConfig::test();
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
        }
    })
}

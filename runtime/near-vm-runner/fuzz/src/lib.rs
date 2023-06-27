use core::fmt;
use near_primitives::contract::ContractCode;
use near_vm_runner::internal::wasmparser::{Export, ExternalKind, Parser, Payload, TypeDef};
use near_vm_runner::logic::VMContext;

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
}

/// Wrapper to get more useful Debug.
pub struct ArbitraryModule(pub wasm_smith::ConfiguredModule<ModuleConfig>);

impl<'a> arbitrary::Arbitrary<'a> for ArbitraryModule {
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

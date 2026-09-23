//! Tests in this module ensure our limits are stricter than Wasmtime's `max_core_instance_size`.
//!
//! Wasmtime's `max_core_instance_size` configuration option limits the size of
//! a compiler internal runtime instance metadata that increases with the number
//! of globals and the number of escaped functions.
//! We never want to hit that limit and instead filter out contracts based on
//! our own limits for `max_globals_per_contract` and
//! `max_functions_number_per_contract` (an upper bound on escaped functions).
//!
//! Upstream changes could violate that assumption but tests in this file should
//! fire in CI on the Wasmtime upgrade PR. However, we cannot anticipate all
//! possible compiler changes.
//! Ideally, we catch remaining cases while reviewing Wasmtime changes. But if we
//! miss it, some contracts may unexpectedly no longer compile on testnet and
//! mainnet. But it wouldn't be a security vulnerability.

use super::{Module, WasmtimeVM};
use crate::prepare::prepare_contract;
use near_parameters::RuntimeConfigStore;
use near_parameters::vm::{Config, LimitConfig, VMKind};
use near_primitives_core::version::PROTOCOL_VERSION;
use std::sync::Arc;
use wasm_encoder::{
    CodeSection, ConstExpr, EntityType, ExportKind, ExportSection, Function, FunctionSection,
    GlobalSection, GlobalType, ImportSection, Instruction, MemorySection, MemoryType,
    Module as WasmModule, RefType, TableSection, TableType, TypeSection, ValType,
};
use wasmtime::Result as WasmtimeResult;

#[test]
fn metadata_cap_activation() {
    let store = RuntimeConfigStore::new(None);
    let bounded = Arc::clone(&store.get_config(88).wasm_config);
    let legacy = Arc::clone(&store.get_config(87).wasm_config);
    assert!(bounded.limit_config.max_globals_per_contract.is_some());
    assert!(legacy.limit_config.max_globals_per_contract.is_none());

    let wasm = metadata_heavy_module(&bounded.as_ref().limit_config, FunctionKind::DefinedExports);
    // The input is preparation-valid on both sides of activation. Only the
    // backend cap should prevent loading it on the historical configuration.
    for (config, should_load) in [(legacy, false), (bounded, true)] {
        let result = compile_and_load(config, &wasm);

        if should_load {
            result.expect("bounded configuration must accept the high-water module");
        } else {
            let error = result.expect_err("historical configuration must retain its metadata cap");
            assert!(
                format!("{error:#}").contains("exceeds the configured maximum"),
                "expected the instance metadata limit, got: {error:#}"
            );
        }
    }
}

#[test]
fn prepared_imported_functions_high_water_loads() {
    assert_prepared_metadata_high_water_loads(FunctionKind::ImportedExports);
}

#[test]
fn prepared_defined_functions_high_water_loads() {
    assert_prepared_metadata_high_water_loads(FunctionKind::DefinedExports);
}

fn assert_prepared_metadata_high_water_loads(function_kind: FunctionKind) {
    let store = RuntimeConfigStore::new(None);
    let config = Arc::clone(&store.get_config(PROTOCOL_VERSION).wasm_config);
    let wasm = metadata_heavy_module(&config.as_ref().limit_config, function_kind);
    assert!(wasm.len() as u64 <= config.limit_config.max_contract_size);
    compile_and_load(config, &wasm)
        .expect("protocol metadata envelope must fit the pooling allocator");
}

fn compile_and_load(config: Arc<Config>, wasm: &[u8]) -> WasmtimeResult<Module> {
    let prepared = prepare_contract(wasm, config.as_ref(), VMKind::Wasmtime)
        .expect("module at the combined protocol limits must prepare");
    let vm = WasmtimeVM::new(config);

    // Compilation alone does not exercise the pooling allocator's module
    // validation. Serialization followed by deserialization is essential.
    let serialized = vm.engine.precompile_module(&prepared).expect("module must compile");
    // SAFETY: these bytes were just compiled by this same engine.
    unsafe { Module::deserialize(&vm.engine, &serialized) }
}

#[derive(Clone, Copy)]
enum FunctionKind {
    /// Normal exported functions, not escaped.
    DefinedExports,
    /// Exported functions that link to imports. These are
    /// escaped functions and use more meta data at runtime.
    ImportedExports,
}

/// Create a valid WASM module, maxing out the limits defined in the config for
/// number of functions, globals, and types.
///
/// The type of function generated (defined or imported) is decided by `function_kind`.
///
/// Note: Currently in Wasmtime, types do not contribute to
/// `max_core_instance_size` but it may do so in the future.
fn metadata_heavy_module(limits: &LimitConfig, function_kind: FunctionKind) -> Vec<u8> {
    let functions = u32::try_from(limits.max_functions_number_per_contract.unwrap()).unwrap();
    let globals = u32::try_from(limits.max_globals_per_contract.unwrap()).unwrap();
    let types = u32::try_from(limits.max_types_per_contract.unwrap()).unwrap();
    let mut module = WasmModule::new();
    let mut type_section = TypeSection::new();
    for _ in 0..types {
        type_section.ty().function([], []);
    }
    module.section(&type_section);
    if matches!(function_kind, FunctionKind::ImportedExports) {
        let mut section = ImportSection::new();
        for i in 0..functions {
            section.import("env", &format!("f{i}"), EntityType::Function(0));
        }
        module.section(&section);
    } else {
        let mut section = FunctionSection::new();
        for _ in 0..functions {
            section.function(0);
        }
        module.section(&section);
    }
    let mut tables = TableSection::new();
    for _ in 0..limits.max_tables_per_contract.unwrap() {
        tables.table(TableType {
            element_type: RefType::FUNCREF,
            table64: false,
            minimum: limits.max_elements_per_contract_table.unwrap() as u64,
            maximum: None,
            shared: false,
        });
    }
    module.section(&tables);
    let mut memories = MemorySection::new();
    memories.memory(MemoryType {
        minimum: u64::from(limits.initial_memory_pages),
        maximum: Some(u64::from(limits.max_memory_pages)),
        memory64: false,
        shared: false,
        page_size_log2: None,
    });
    module.section(&memories);
    let mut global_section = GlobalSection::new();
    for _ in 0..globals {
        global_section.global(
            GlobalType { val_type: ValType::I64, mutable: true, shared: false },
            &ConstExpr::i64_const(0),
        );
    }
    module.section(&global_section);
    let mut exports = ExportSection::new();
    for i in 0..functions {
        exports.export(&format!("f{i}"), ExportKind::Func, i);
    }
    module.section(&exports);
    if matches!(function_kind, FunctionKind::DefinedExports) {
        let mut code = CodeSection::new();
        for _ in 0..functions {
            let mut function = Function::new([]);
            function.instruction(&Instruction::End);
            code.function(&function);
        }
        module.section(&code);
    }
    module.finish()
}

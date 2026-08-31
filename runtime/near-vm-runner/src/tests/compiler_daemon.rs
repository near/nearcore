use super::test_vm_config;
use crate::ContractCode;
use crate::prepare::prepare_contract;
use crate::wasmtime_runner::{WasmtimeVM, create_compiler_engine};
use near_parameters::vm::VMKind;
use std::sync::Arc;

/// Verify that artifacts compiled by the daemon's non-pooled engine match
/// those compiled by the in-process pooled engine (via compile_uncached).
#[test]
fn test_artifact_compatible_with_pooled_engine() {
    let config = test_vm_config(Some(VMKind::Wasmtime));
    let contract = ContractCode::new(
        wat::parse_str(
            r#"(module
                (memory 1)
                (func $increment (param i64) (result i64)
                    local.get 0
                    i64.const 1
                    i64.add)
                (func (export "main") (result i64)
                    i64.const 41
                    call $increment))"#,
        )
        .unwrap(),
        None,
    );

    // Compile with the daemon's non-pooled engine (same as child process).
    let daemon_engine = create_compiler_engine(config.limit_config.max_memory_pages).unwrap();
    let prepared = prepare_contract(contract.code(), &config, VMKind::Wasmtime).unwrap();
    let daemon_artifact = daemon_engine.precompile_module(&prepared).unwrap();

    // Compile with the node's in-process pooled engine.
    let pooled_vm = WasmtimeVM::new_for_target(Arc::new(config), None).unwrap();
    let pooled_artifact = pooled_vm.compile_uncached(&contract).unwrap().unwrap();

    assert_eq!(daemon_artifact, pooled_artifact);
}

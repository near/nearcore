use super::test_vm_config;
use crate::ContractCode;
use crate::prepare::prepare_contract;
use crate::wasmtime_runner::{CachedArtifact, WasmtimeVM, create_compiler_engine};
use near_parameters::vm::VMKind;
use std::sync::Arc;

#[test]
fn test_compiled_bytes_same_as_in_process_engine() {
    let config = test_vm_config(Some(VMKind::Wasmtime));
    let contract = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);

    // Compile with the daemon's non-pooled engine (same as child process).
    let daemon_engine = create_compiler_engine(config.limit_config.max_memory_pages).unwrap();
    let prepared = prepare_contract(contract.code(), &config, VMKind::Wasmtime).unwrap();
    let daemon_artifact = daemon_engine.precompile_module(&prepared).unwrap();

    // Compile with the node's in-process pooled engine.
    let pooled_vm = WasmtimeVM::new_for_target(Arc::new(config), None).unwrap();
    let CachedArtifact::CompiledBytes(pooled_artifact) =
        pooled_vm.compile_uncached(&contract).unwrap()
    else {
        panic!("contract compilation failed");
    };

    assert_eq!(daemon_artifact, pooled_artifact);
}

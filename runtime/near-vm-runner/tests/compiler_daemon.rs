//! Integration test for the out-of-process compiler daemon.
//!
//! This binary serves double duty: when invoked with `compile-wasm` as the
//! first argument, it acts as the daemon subprocess. Otherwise, it runs
//! the tests. This lets us test the full subprocess flow (spawn, IPC,
//! compilation) without a separate binary.

use assert_matches::assert_matches;
use near_parameters::vm::VMKind;
use near_vm_runner::compiler_daemon;
use near_vm_runner::logic::errors::CompilationError;
use near_vm_runner::prepare;

fn main() {
    if std::env::args().nth(1).as_deref() == Some("compile-wasm") {
        compiler_daemon::daemon_main();
    }

    compiler_daemon::set_daemon_binary(std::env::current_exe().unwrap());

    test_basic_compilation();
    test_invalid_wasm();
}

fn test_config() -> near_parameters::vm::Config {
    let config_store = near_parameters::RuntimeConfigStore::new(None);
    let runtime_config = config_store.get_config(near_primitives_core::version::PROTOCOL_VERSION);
    (*runtime_config.wasm_config).clone()
}

fn test_basic_compilation() {
    let config = test_config();
    let wasm = wat::parse_str(r#"(module (func (export "main")))"#).unwrap();
    let prepared = prepare::prepare_contract(&wasm, &config, VMKind::Wasmtime).unwrap();

    let result = compiler_daemon::compile_in_subprocess(&prepared, &config.limit_config);
    let compiled = result.unwrap();
    assert!(!compiled.is_empty());
}

fn test_invalid_wasm() {
    let config = test_config();
    let result =
        compiler_daemon::compile_in_subprocess(b"this is not valid wasm", &config.limit_config);
    assert_matches!(result, Err(CompilationError::WasmtimeCompileError { .. }));
}

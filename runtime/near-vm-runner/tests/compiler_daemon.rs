//! Integration test for the out-of-process compiler daemon.
//!
//! The tests spawn the dedicated daemon binary Cargo builds for this package.

use assert_matches::assert_matches;
use near_parameters::vm::VMKind;
use near_vm_runner::CompilePriority;
use near_vm_runner::compiler_daemon;
use near_vm_runner::logic::errors::CompilationError;
#[cfg(feature = "test_features")]
use near_vm_runner::logic::errors::VMRunnerError;
use near_vm_runner::prepare;
use std::path::PathBuf;
use std::sync::Arc;

const TEST_POOL_SIZE: usize = 4;

fn main() {
    compiler_daemon::set_daemon_binary(PathBuf::from(env!(
        "CARGO_BIN_EXE_near-vm-compiler-daemon"
    )));
    compiler_daemon::set_daemon_pool_size(TEST_POOL_SIZE);

    test_basic_compilation();
    #[cfg(all(target_os = "linux", feature = "test_features"))]
    test_landlock_sandbox();
    test_invalid_wasm();
    test_parallel_compilation();
    test_mixed_priority_compilation();
    #[cfg(feature = "test_features")]
    test_worker_crash_is_unknown_compilation_error();
}

fn test_config() -> near_parameters::vm::Config {
    let config_store = near_parameters::RuntimeConfigStore::new(None);
    let runtime_config = config_store.get_config(near_primitives_core::version::PROTOCOL_VERSION);
    (*runtime_config.wasm_config).clone()
}

/// Build a distinct, non-trivial WASM module.
///
/// - Change `seed` makes unique artifacts.
/// - Increase `num_funcs` to make compilation take long enough that concurrent callers actually overlap.
fn prepared_module(config: &near_parameters::vm::Config, seed: usize, num_funcs: usize) -> Vec<u8> {
    let mut wat = String::from("(module\n");
    for i in 0..num_funcs {
        let value = (seed as i64) * 1_000_000 + i as i64;
        wat.push_str(&format!("(func (export \"f{i}\") (result i64) (i64.const {value}))\n"));
    }
    wat.push_str(")\n");
    let wasm = wat::parse_str(&wat).unwrap();
    prepare::prepare_contract(&wasm, config, VMKind::Wasmtime).unwrap()
}

fn test_basic_compilation() {
    let config = test_config();
    let wasm = wat::parse_str(r#"(module (func (export "main")))"#).unwrap();
    let prepared = prepare::prepare_contract(&wasm, &config, VMKind::Wasmtime).unwrap();

    let result = compiler_daemon::compile_in_subprocess(
        &prepared,
        &config.limit_config,
        CompilePriority::Critical,
    );
    let compiled = result.unwrap().unwrap();
    assert!(!compiled.is_empty());
}

fn test_invalid_wasm() {
    let config = test_config();
    let result = compiler_daemon::compile_in_subprocess(
        b"this is not valid wasm",
        &config.limit_config,
        CompilePriority::Critical,
    );
    assert_matches!(result, Ok(Err(CompilationError::WasmtimeCompileError { .. })));
}

/// Hammer the daemon from many threads compiling a mix of distinct modules.
/// Asserts every compile succeeds, that output is deterministic across workers,
/// and that more than one worker subprocess was actually spawned (i.e. real
/// parallelism occurred, not just serial reuse of a single worker).
fn test_parallel_compilation() {
    const VARIANTS: usize = 4;
    const THREADS: usize = 16;
    const ITERS: usize = 8;
    let config = Arc::new(test_config());

    // Reference artifacts: compile each variant once up front.
    let prepared: Vec<Vec<u8>> = (0..VARIANTS).map(|s| prepared_module(&config, s, 300)).collect();
    let reference: Vec<Vec<u8>> = prepared
        .iter()
        .map(|p| {
            compiler_daemon::compile_in_subprocess(
                p,
                &config.limit_config,
                CompilePriority::Critical,
            )
            .unwrap()
            .unwrap()
        })
        .collect();
    let prepared = Arc::new(prepared);
    let reference = Arc::new(reference);

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let config = Arc::clone(&config);
            let prepared = Arc::clone(&prepared);
            let reference = Arc::clone(&reference);
            std::thread::spawn(move || {
                for i in 0..ITERS {
                    let variant = (t + i) % VARIANTS;
                    let compiled = compiler_daemon::compile_in_subprocess(
                        &prepared[variant],
                        &config.limit_config,
                        CompilePriority::Critical,
                    )
                    .unwrap()
                    .unwrap();
                    assert!(!compiled.is_empty());
                    // Daemon output must be deterministic regardless of which
                    // worker served the request.
                    assert_eq!(compiled, reference[variant], "nondeterministic artifact");
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    let high_water = compiler_daemon::spawned_worker_high_water();
    assert!(high_water >= 2, "expected >1 worker to spawn under load, got {high_water}");
    assert!(high_water <= TEST_POOL_SIZE, "spawned more workers than the pool cap: {high_water}");
}

/// Concurrent compilations across all three priority classes must all succeed.
/// Exercises `checkout` with mixed priorities and contention without relying on
/// timing-sensitive ordering assertions (the ordering decision is unit-tested
/// separately in `parent.rs`).
fn test_mixed_priority_compilation() {
    const THREADS: usize = 12;
    let config = Arc::new(test_config());
    let prepared = Arc::new(prepared_module(&config, 42, 200));

    let priorities =
        [CompilePriority::Critical, CompilePriority::Interactive, CompilePriority::Background];

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let config = Arc::clone(&config);
            let prepared = Arc::clone(&prepared);
            let priority = priorities[t % priorities.len()];
            std::thread::spawn(move || {
                let compiled = compiler_daemon::compile_in_subprocess(
                    &prepared,
                    &config.limit_config,
                    priority,
                )
                .unwrap()
                .unwrap();
                assert!(!compiled.is_empty());
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}

/// Verify that a sandboxed worker cannot access filesystem paths and, on
/// kernels supporting Landlock ABI v4, cannot bind a TCP socket. Successful
/// compilation in the other tests proves that IPC and Wasmtime still work.
#[cfg(all(target_os = "linux", feature = "test_features"))]
fn test_landlock_sandbox() {
    let config = test_config();
    let result = compiler_daemon::compile_in_subprocess(
        compiler_daemon::protocol::TEST_LANDLOCK_PROBE_REQUEST,
        &config.limit_config,
        CompilePriority::Critical,
    )
    .unwrap()
    .unwrap();
    assert_eq!(result, compiler_daemon::protocol::TEST_LANDLOCK_PROBE_RESPONSE);
}

/// A worker crash is reported as an unknown compilation error. The runtime
/// handles this like an unknown execution error when producing the outcome.
#[cfg(feature = "test_features")]
fn test_worker_crash_is_unknown_compilation_error() {
    let config = test_config();
    let result = compiler_daemon::compile_in_subprocess(
        compiler_daemon::protocol::TEST_ABORT_REQUEST,
        &config.limit_config,
        CompilePriority::Critical,
    );
    assert_matches!(result, Err(VMRunnerError::WasmCompilationUnknownError { .. }));
}

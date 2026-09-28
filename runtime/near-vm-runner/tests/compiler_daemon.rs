//! Integration test for the out-of-process compiler daemon.
//!
//! This executable serves as both parent and child, matching how neard starts
//! compiler workers without requiring a second binary.

// cspell:words sandboxed

use assert_matches::assert_matches;
use near_parameters::vm::VMKind;
use near_vm_runner::CompilePriority;
use near_vm_runner::compiler_daemon;
use near_vm_runner::compiler_daemon::ExitOnWorkerMemoryExhaustion;
use near_vm_runner::logic::errors::CompilationError;
#[cfg(feature = "test_features")]
use near_vm_runner::logic::errors::VMRunnerError;
use near_vm_runner::prepare;
#[cfg(feature = "test_features")]
use near_vm_runner::{ContractCode, MockContractRuntimeCache, precompile_contract};
use std::alloc::System;
#[cfg(unix)]
use std::borrow::Cow;
use std::env;
#[cfg(unix)]
use std::process::{Command, Stdio};
use std::sync::Arc;
#[cfg(feature = "test_features")]
use std::thread::{sleep, spawn};
#[cfg(feature = "test_features")]
use std::time::{Duration, Instant};

const TEST_POOL_SIZE: usize = 4;
const TEST_MEMORY_LIMIT_BYTES: u64 = 1024 * 1024 * 1024;
const TEST_TOTAL_BUDGET_BYTES: u64 = 4 * TEST_MEMORY_LIMIT_BYTES;

#[global_allocator]
static ALLOC: ExitOnWorkerMemoryExhaustion<System> = ExitOnWorkerMemoryExhaustion::new(System);

fn main() {
    if env::args_os().nth(1).is_some_and(|arg| arg == "compile-wasm") {
        compiler_daemon::daemon_main();
    }

    compiler_daemon::set_daemon_binary(env::current_exe().unwrap());
    compiler_daemon::set_daemon_pool_size(TEST_POOL_SIZE);
    #[cfg(feature = "test_features")]
    compiler_daemon::set_test_memory_config(
        TEST_MEMORY_LIMIT_BYTES,
        TEST_TOTAL_BUDGET_BYTES,
        TEST_TOTAL_BUDGET_BYTES,
    );

    #[cfg(unix)]
    test_missing_memory_limit_is_startup_error();
    #[cfg(unix)]
    test_allocator_exhaustion_exit_code();
    test_startup_probe();
    test_basic_compilation();
    #[cfg(all(target_os = "linux", feature = "test_features"))]
    test_landlock_sandbox();
    test_invalid_wasm();
    test_parallel_compilation();
    test_mixed_priority_compilation();
    #[cfg(feature = "test_features")]
    test_worker_timeout_is_unknown_compilation_error();
    #[cfg(feature = "test_features")]
    test_worker_crash_is_unknown_compilation_error();
    #[cfg(all(unix, feature = "test_features"))]
    test_worker_memory_escalation_succeeds();
    #[cfg(all(unix, feature = "test_features"))]
    test_memory_escalation_evicts_active_sibling();
    #[cfg(all(unix, feature = "test_features"))]
    test_concurrent_memory_recoveries_are_serialized();
    #[cfg(all(unix, feature = "test_features"))]
    test_worker_memory_exhaustion_is_preserved();
    #[cfg(all(unix, feature = "test_features"))]
    test_unknown_sigkill_is_not_memory_exhaustion();
    #[cfg(all(unix, feature = "test_features"))]
    test_live_protocol_failure_has_bounded_cleanup();
    #[cfg(feature = "test_features")]
    test_engine_creation_failure_is_not_cached();
}

#[cfg(unix)]
fn test_missing_memory_limit_is_startup_error() {
    use compiler_daemon::protocol::{
        COMPILER_DAEMON_STACK_SIZE_ENV, COMPILER_DAEMON_THREADS_ENV, DaemonStartup, read_frame,
    };

    let mut child = Command::new(env::current_exe().unwrap())
        .arg("compile-wasm")
        .env_clear()
        .env(COMPILER_DAEMON_THREADS_ENV, "1")
        .env(COMPILER_DAEMON_STACK_SIZE_ENV, (8 * 1024 * 1024).to_string())
        .current_dir("/")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let startup = read_frame(child.stdout.as_mut().unwrap()).unwrap();
    let startup: DaemonStartup = borsh::from_slice(&startup).unwrap();
    let DaemonStartup::Err(err) = startup else {
        panic!("worker without a memory limit unexpectedly became ready");
    };
    assert!(err.contains("NEAR_COMPILER_DAEMON_MEMORY_LIMIT_BYTES"), "{err}");
    assert!(!child.wait().unwrap().success());
}

/// Exercise the real system allocator under the worker's RLIMIT_AS and check
/// that the adapter exits directly with the reserved, distinguishable status.
#[cfg(unix)]
fn test_allocator_exhaustion_exit_code() {
    use compiler_daemon::protocol::{
        COMPILER_DAEMON_MEMORY_LIMIT_ENV, COMPILER_DAEMON_STACK_SIZE_ENV,
        COMPILER_DAEMON_THREADS_ENV, CompileRequest, DaemonStartup, MemoryLimitStatus, TestAction,
        read_frame, write_frame,
    };

    let mut child = Command::new(env::current_exe().unwrap())
        .arg("compile-wasm")
        .env_clear()
        .env(COMPILER_DAEMON_THREADS_ENV, "1")
        .env(COMPILER_DAEMON_STACK_SIZE_ENV, (8 * 1024 * 1024).to_string())
        .env(COMPILER_DAEMON_MEMORY_LIMIT_ENV, TEST_MEMORY_LIMIT_BYTES.to_string())
        .current_dir("/")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();

    let startup = read_frame(&mut stdout).unwrap();
    let startup: DaemonStartup = borsh::from_slice(&startup).unwrap();
    let DaemonStartup::Ready(status) = startup else {
        panic!("compiler daemon failed to start");
    };
    assert_eq!(
        status.memory_limit,
        MemoryLimitStatus::Enforced { memory_limit_bytes: TEST_MEMORY_LIMIT_BYTES }
    );

    let request = CompileRequest {
        prepared_code: Cow::Borrowed(&[]),
        max_memory_pages: 1,
        test_action: Some(TestAction::AllocationFailure),
    };
    write_frame(&mut stdin, &borsh::to_vec(&request).unwrap()).unwrap();
    drop(stdin);
    assert!(read_frame(&mut stdout).is_err(), "worker unexpectedly returned a response");

    let status = child.wait().unwrap();
    assert_eq!(
        status.code(),
        Some(compiler_daemon::WORKER_MEMORY_EXHAUSTED_EXIT_CODE),
        "worker did not report instrumented memory exhaustion: {status}"
    );
}

fn test_startup_probe() {
    let status = compiler_daemon::start_daemon().unwrap();
    assert_ne!(status.compiler_compatibility_hash, 0);
    #[cfg(unix)]
    assert_eq!(
        status.memory_limit,
        compiler_daemon::protocol::MemoryLimitStatus::Enforced {
            memory_limit_bytes: TEST_MEMORY_LIMIT_BYTES,
        }
    );
    #[cfg(not(unix))]
    assert_eq!(status.memory_limit, compiler_daemon::protocol::MemoryLimitStatus::Unavailable);
    #[cfg(target_os = "linux")]
    assert_matches!(
        status.isolation,
        compiler_daemon::protocol::IsolationStatus::LinuxLandlock { abi: 1.. }
    );
    #[cfg(not(target_os = "linux"))]
    assert_eq!(status.isolation, compiler_daemon::protocol::IsolationStatus::Unavailable);
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
    compiler_daemon::set_test_action_for_next_request(
        compiler_daemon::protocol::TestAction::LandlockProbe,
    );
    let result = compiler_daemon::compile_in_subprocess(
        &[],
        &config.limit_config,
        CompilePriority::Critical,
    )
    .unwrap()
    .unwrap();
    assert!(result.is_empty());
}

/// A worker that stops responding is killed and its request is reported as an
/// unknown compilation error. A subsequent compile proves the pool remains usable.
#[cfg(feature = "test_features")]
fn test_worker_timeout_is_unknown_compilation_error() {
    let config = test_config();
    let started = Instant::now();
    compiler_daemon::set_test_action_for_next_request(
        compiler_daemon::protocol::TestAction::Timeout,
    );
    let result = compiler_daemon::compile_in_subprocess(
        &[],
        &config.limit_config,
        CompilePriority::Critical,
    );
    let Err(VMRunnerError::WasmCompilationUnknownError { debug_message }) = result else {
        panic!("expected unknown compilation error after daemon timeout");
    };
    assert!(debug_message.contains("timed out during compilation request"), "{debug_message}");
    assert!(started.elapsed() < Duration::from_secs(5), "daemon timeout took too long");
    let state = compiler_daemon::worker_pool_state();
    assert_eq!(state.live, state.idle, "timeout leaked a worker permit: {state:?}");

    let prepared = prepared_module(&config, 99, 1);
    let compiled = compiler_daemon::compile_in_subprocess(
        &prepared,
        &config.limit_config,
        CompilePriority::Critical,
    )
    .unwrap()
    .unwrap();
    assert!(!compiled.is_empty());
}

/// A worker crash is reported as an unknown compilation error. The runtime
/// handles this like an unknown execution error when producing the outcome.
#[cfg(feature = "test_features")]
fn test_worker_crash_is_unknown_compilation_error() {
    let config = test_config();
    compiler_daemon::set_test_action_for_next_request(compiler_daemon::protocol::TestAction::Abort);
    let result = compiler_daemon::compile_in_subprocess(
        &[],
        &config.limit_config,
        CompilePriority::Critical,
    );
    let Err(VMRunnerError::WasmCompilationUnknownError { debug_message }) = result else {
        panic!("expected unknown compilation error after daemon crash");
    };
    assert!(debug_message.contains("compiler daemon exited with"), "{debug_message}");
    assert!(!debug_message.contains("memory limit"), "{debug_message}");
    let state = compiler_daemon::worker_pool_state();
    assert_eq!(state.live, state.idle, "worker crash leaked a worker permit: {state:?}");
}

/// Confirmed local exhaustion retries in a fresh, larger worker and produces
/// the same artifact as an ordinary compilation.
#[cfg(all(unix, feature = "test_features"))]
fn test_worker_memory_escalation_succeeds() {
    let config = test_config();
    let prepared = prepared_module(&config, 101, 10);
    let expected = compiler_daemon::compile_in_subprocess(
        &prepared,
        &config.limit_config,
        CompilePriority::Critical,
    )
    .unwrap()
    .unwrap();

    compiler_daemon::set_test_action_for_next_request(
        compiler_daemon::protocol::TestAction::MemoryExhaustionBelow {
            memory_limit_bytes: 2 * TEST_MEMORY_LIMIT_BYTES,
        },
    );
    let compiled = compiler_daemon::compile_in_subprocess(
        &prepared,
        &config.limit_config,
        CompilePriority::Critical,
    )
    .unwrap()
    .unwrap();
    assert_eq!(compiled, expected, "memory escalation changed the artifact");
    let state = compiler_daemon::worker_pool_state();
    assert!(state.reserved_bytes <= TEST_TOTAL_BUDGET_BYTES, "{state:?}");
}

/// Under a constrained budget, a protected larger retry evicts an active
/// ordinary request and that displaced caller safely requeues at the same tier.
#[cfg(all(unix, feature = "test_features"))]
fn test_memory_escalation_evicts_active_sibling() {
    let config = Arc::new(test_config());
    let prepared = Arc::new(prepared_module(&config, 102, 10));
    let evictions_before = compiler_daemon::worker_pool_state().scheduler_evictions;
    let sleepers: Vec<_> = (0..3)
        .map(|_| {
            let config = Arc::clone(&config);
            let prepared = Arc::clone(&prepared);
            spawn(move || {
                compiler_daemon::set_test_action_for_next_request(
                    compiler_daemon::protocol::TestAction::SleepMillis(5_000),
                );
                compiler_daemon::compile_in_subprocess(
                    &prepared,
                    &config.limit_config,
                    CompilePriority::Background,
                )
                .unwrap()
                .unwrap()
            })
        })
        .collect();

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let state = compiler_daemon::worker_pool_state();
        if state.live >= 3 && state.idle == 0 {
            break;
        }
        assert!(Instant::now() < deadline, "sleeping workers did not become active: {state:?}");
        sleep(Duration::from_millis(10));
    }

    compiler_daemon::set_test_action_for_next_request(
        compiler_daemon::protocol::TestAction::MemoryExhaustionBelow {
            memory_limit_bytes: 2 * TEST_MEMORY_LIMIT_BYTES,
        },
    );
    let compiled = compiler_daemon::compile_in_subprocess(
        &prepared,
        &config.limit_config,
        CompilePriority::Critical,
    )
    .unwrap()
    .unwrap();
    assert!(!compiled.is_empty());
    assert!(
        compiler_daemon::worker_pool_state().scheduler_evictions > evictions_before,
        "larger recovery did not evict a sibling under the constrained budget"
    );

    for sleeper in sleepers {
        assert!(!sleeper.join().unwrap().is_empty());
    }
    let state = compiler_daemon::worker_pool_state();
    assert!(state.reserved_bytes <= TEST_TOTAL_BUDGET_BYTES, "{state:?}");
}

/// Concurrent local OOMs share one protected recovery owner and both callers
/// eventually make progress without exceeding the global reservation budget.
#[cfg(all(unix, feature = "test_features"))]
fn test_concurrent_memory_recoveries_are_serialized() {
    let config = Arc::new(test_config());
    let prepared = Arc::new(prepared_module(&config, 103, 10));
    let recoveries: Vec<_> = (0..2)
        .map(|_| {
            let config = Arc::clone(&config);
            let prepared = Arc::clone(&prepared);
            spawn(move || {
                compiler_daemon::set_test_action_for_next_request(
                    compiler_daemon::protocol::TestAction::MemoryExhaustionBelow {
                        memory_limit_bytes: 2 * TEST_MEMORY_LIMIT_BYTES,
                    },
                );
                compiler_daemon::compile_in_subprocess(
                    &prepared,
                    &config.limit_config,
                    CompilePriority::Interactive,
                )
                .unwrap()
                .unwrap()
            })
        })
        .collect();
    for recovery in recoveries {
        assert!(!recovery.join().unwrap().is_empty());
    }
    let state = compiler_daemon::worker_pool_state();
    assert!(state.reserved_bytes <= TEST_TOTAL_BUDGET_BYTES, "{state:?}");
}

/// The reserved allocator exit remains distinguishable after pipe teardown,
/// including after all larger local tiers have also failed.
#[cfg(all(unix, feature = "test_features"))]
fn test_worker_memory_exhaustion_is_preserved() {
    let config = test_config();
    compiler_daemon::set_test_action_for_next_request(
        compiler_daemon::protocol::TestAction::AllocationFailure,
    );
    let result = compiler_daemon::compile_in_subprocess(
        &[],
        &config.limit_config,
        CompilePriority::Critical,
    );
    let Err(VMRunnerError::WasmCompilationUnknownError { debug_message }) = result else {
        panic!("expected unknown compilation error after local memory exhaustion");
    };
    assert!(debug_message.contains("exhausted its local memory limit"), "{debug_message}");
    let state = compiler_daemon::worker_pool_state();
    assert_eq!(state.live, state.idle, "memory exhaustion leaked a worker permit: {state:?}");
}

/// SIGKILL without trustworthy per-worker evidence must stay an ordinary crash.
#[cfg(all(unix, feature = "test_features"))]
fn test_unknown_sigkill_is_not_memory_exhaustion() {
    let config = test_config();
    compiler_daemon::set_test_action_for_next_request(
        compiler_daemon::protocol::TestAction::UnknownSigkill,
    );
    let result = compiler_daemon::compile_in_subprocess(
        &[],
        &config.limit_config,
        CompilePriority::Critical,
    );
    let Err(VMRunnerError::WasmCompilationUnknownError { debug_message }) = result else {
        panic!("expected unknown compilation error after sigkill");
    };
    assert!(debug_message.contains("compiler daemon exited with"), "{debug_message}");
    assert!(!debug_message.contains("memory limit"), "{debug_message}");
}

/// A live child which closes its response pipe is killed and reaped without an
/// unbounded wait, while the cleanup signal remains distinct from the cause.
#[cfg(all(unix, feature = "test_features"))]
fn test_live_protocol_failure_has_bounded_cleanup() {
    let config = test_config();
    let started = Instant::now();
    compiler_daemon::set_test_action_for_next_request(
        compiler_daemon::protocol::TestAction::CloseOutputAndPark,
    );
    let result = compiler_daemon::compile_in_subprocess(
        &[],
        &config.limit_config,
        CompilePriority::Critical,
    );
    let Err(VMRunnerError::WasmCompilationUnknownError { debug_message }) = result else {
        panic!("expected unknown compilation error after broken protocol");
    };
    assert!(debug_message.contains("compiler daemon protocol failed"), "{debug_message}");
    assert!(!debug_message.contains("memory limit"), "{debug_message}");
    assert!(started.elapsed() < Duration::from_secs(5), "protocol cleanup took too long");
    let state = compiler_daemon::worker_pool_state();
    assert_eq!(state.live, state.idle, "protocol failure leaked a worker permit: {state:?}");
}

/// Engine construction depends on local process resources and configuration.
/// Its failure must remain an unavailable error and must not be persisted as a
/// deterministic contract compilation error.
#[cfg(feature = "test_features")]
fn test_engine_creation_failure_is_not_cached() {
    let config = Arc::new(test_config());
    let code =
        ContractCode::new(wat::parse_str(r#"(module (func (export "main")))"#).unwrap(), None);
    let cache = MockContractRuntimeCache::default();

    compiler_daemon::set_test_action_for_next_request(
        compiler_daemon::protocol::TestAction::EngineCreationFailure,
    );
    let result = precompile_contract(&code, config, Some(&cache));

    assert_matches!(result, Err(VMRunnerError::WasmCompilationUnknownError { .. }));
    assert_eq!(cache.len(), 0, "daemon-local failure was cached");
    assert_eq!(cache.put_count(), 0, "daemon-local failure attempted a cache write");
}

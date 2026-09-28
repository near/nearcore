#![cfg(unix)]

use near_vm_runner::compiler_daemon;
use near_vm_runner::compiler_daemon::protocol::{
    COMPILER_DAEMON_STACK_SIZE_ENV, COMPILER_DAEMON_THREADS_ENV, CompileRequest, DaemonStartup,
    TestAction, read_frame, write_frame,
};
use std::borrow::Cow;
use std::process::{Command, Stdio};

/// Verify the production Jemalloc adapter with a real allocation failure under
/// the compiler worker's address-space limit.
#[test]
fn jemalloc_exits_with_memory_exhaustion_status() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_neard"))
        .arg("compile-wasm")
        .env_clear()
        .env(COMPILER_DAEMON_THREADS_ENV, "1")
        .env(COMPILER_DAEMON_STACK_SIZE_ENV, (8 * 1024 * 1024).to_string())
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
    assert!(matches!(startup, DaemonStartup::Ready(_)));

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

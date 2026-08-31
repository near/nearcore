//! Compiler daemon subprocess entry point.
//!
//! Runs inside the child process spawned by the parent. Sets a virtual memory
//! limit (RLIMIT_AS), [landlocks](https://landlock.io/) itself to minimal
//! system access, and raises `oom_score_adj`.
//!
//! Limits and sandboxing are only implemented for Linux.

// cspell:words landlock landlocks sandboxing

use super::MIN_WORKER_MEMORY_LIMIT_BYTES;
use super::protocol::{
    CompileRequest, DaemonStartup, DaemonStatus, read_frame, write_compile_response, write_frame,
};
use super::sandbox::{self, SandboxStatus};
use crate::wasmtime_runner::{compiler_compatibility_hash, create_compiler_engine};
use std::collections::{HashMap, hash_map};
use std::fmt::Display;
use std::process::exit;
#[cfg(feature = "test_features")]
use std::thread::park;

/// Entry point for the dedicated compiler daemon binary.
pub fn daemon_main() -> ! {
    let stdout = std::io::stdout();
    let mut writer = stdout.lock();

    set_memory_limit();
    raise_oom_score_adj();
    let sandbox_status = match sandbox::apply() {
        Ok(status) => status,
        Err(err) => {
            let startup = DaemonStartup::Err(err);
            let _ = write_frame(&mut writer, &borsh::to_vec(&startup).unwrap());
            std::process::exit(1);
        }
    };
    let compiler_compatibility_hash = compiler_compatibility_hash().unwrap_or_else(|err| {
        abort_worker(format!("failed to create compatibility engine: {err}"))
    });
    let startup = DaemonStartup::Ready(DaemonStatus {
        compiler_compatibility_hash,
        isolation: sandbox_status.isolation_status(),
    });
    if write_frame(&mut writer, &borsh::to_vec(&startup).unwrap()).is_err() {
        std::process::exit(1);
    }

    let stdin = std::io::stdin();
    let mut reader = stdin.lock();
    let mut engines: HashMap<u32, wasmtime::Engine> = HashMap::new();

    loop {
        let frame = match read_frame(&mut reader) {
            Ok(f) => f,
            Err(_) => std::process::exit(0),
        };
        let request: CompileRequest<'_> = match borsh::from_slice(&frame) {
            Ok(r) => r,
            Err(err) => abort_worker(format!("failed to deserialize request: {err}")),
        };
        let response = handle_request(&mut engines, request, &sandbox_status);
        let response = response.as_ref().map(Vec::as_slice).map_err(String::as_str);
        if write_compile_response(&mut writer, response).is_err() {
            std::process::exit(0);
        }
    }
}

fn handle_request(
    engines: &mut HashMap<u32, wasmtime::Engine>,
    request: CompileRequest<'_>,
    sandbox_status: &SandboxStatus,
) -> Result<Vec<u8>, String> {
    #[cfg(feature = "test_features")]
    if let Some(action) = request.test_action {
        return match action {
            super::protocol::TestAction::Abort => std::process::abort(),
            super::protocol::TestAction::Timeout => loop {
                park();
            },
            super::protocol::TestAction::EngineCreationFailure => {
                abort_worker("failed to create engine: test engine creation failure")
            }
            #[cfg(target_os = "linux")]
            super::protocol::TestAction::LandlockProbe => {
                match sandbox::run_probe(sandbox_status) {
                    Ok(()) => Ok(Vec::new()),
                    Err(err) => Err(err),
                }
            }
        };
    }
    let _ = sandbox_status;
    handle_compile(engines, request)
}

fn handle_compile(
    engines: &mut HashMap<u32, wasmtime::Engine>,
    request: CompileRequest<'_>,
) -> Result<Vec<u8>, String> {
    let engine = match engines.entry(request.max_memory_pages) {
        hash_map::Entry::Occupied(e) => e.into_mut(),
        hash_map::Entry::Vacant(e) => e.insert(
            create_compiler_engine(request.max_memory_pages)
                .unwrap_or_else(|err| abort_worker(format!("failed to create engine: {err}"))),
        ),
    };
    engine.precompile_module(&request.prepared_code).map_err(|err| err.to_string())
}

/// Exit the worker process with a message to its local stderr.
///
/// The compilation worker experienced an unrecoverable failure. Exit without
/// sending a compilation response. The parent treats the closed IPC channel as
/// unavailable and never mistakes the message for a deterministic contract
/// compilation error.
fn abort_worker(err: impl Display) -> ! {
    eprintln!("{err}");
    exit(1)
}

#[cfg(unix)]
fn set_memory_limit() {
    let ret = unsafe {
        // cspell:words rlim
        let limit = libc::rlimit {
            rlim_cur: MIN_WORKER_MEMORY_LIMIT_BYTES,
            rlim_max: MIN_WORKER_MEMORY_LIMIT_BYTES,
        };
        // cspell:words setrlimit
        libc::setrlimit(libc::RLIMIT_AS, &limit)
    };
    if ret != 0 {
        eprintln!("warning: failed to set memory limit: {}", std::io::Error::last_os_error());
    }
}

#[cfg(not(unix))]
fn set_memory_limit() {}

/// Mark this worker as the kernel OOM killer's preferred victim.
///
/// Compiler workers are cheap to respawn: on the next request the parent pool
/// simply checks out a fresh one. Under global memory pressure we would much
/// rather lose a transient worker than the long-lived neard process. Writing
/// the maximum score to `/proc/self/oom_score_adj` asks the kernel to kill
/// this process first.
///
/// Best-effort: a failure here does not stop the daemon from compiling.
#[cfg(target_os = "linux")]
fn raise_oom_score_adj() {
    use std::fs;
    // 1000 is the maximum value `/proc/self/oom_score_adj` accepts: it marks
    // this process as the prime OOM-killer target.
    if let Err(err) = fs::write("/proc/self/oom_score_adj", b"1000") {
        eprintln!("warning: failed to set oom_score_adj: {err}");
    }
}

#[cfg(not(target_os = "linux"))]
fn raise_oom_score_adj() {}

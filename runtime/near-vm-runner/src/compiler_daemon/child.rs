//! Compiler daemon subprocess entry point.
//!
//! Runs inside the child process spawned by the parent. Sets a virtual memory
//! limit (RLIMIT_AS), [landlocks](https://landlock.io/) itself to minimal
//! system access, and raises `oom_score_adj`.
//!
//! Limits and sandboxing are only implemented for Linux.

use super::MIN_WORKER_MEMORY_LIMIT_BYTES;
use super::protocol::{CompileRequest, CompileResponse, DaemonStartup, read_frame, write_frame};
use super::sandbox::{self, SandboxStatus};
use crate::wasmtime_runner::create_compiler_engine;
use std::collections::{HashMap, hash_map};

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
    let startup = DaemonStartup::Ready;
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
        let request: CompileRequest = match borsh::from_slice(&frame) {
            Ok(r) => r,
            Err(e) => {
                let resp = CompileResponse::Err(format!("failed to deserialize request: {e}"));
                let _ = write_frame(&mut writer, &borsh::to_vec(&resp).unwrap());
                continue;
            }
        };
        let response = handle_request(&mut engines, request, &sandbox_status);
        if write_frame(&mut writer, &borsh::to_vec(&response).unwrap()).is_err() {
            std::process::exit(0);
        }
    }
}

fn handle_request(
    engines: &mut HashMap<u32, wasmtime::Engine>,
    request: CompileRequest,
    sandbox_status: &SandboxStatus,
) -> CompileResponse {
    #[cfg(all(target_os = "linux", feature = "test_features"))]
    if request.prepared_code == super::protocol::TEST_LANDLOCK_PROBE_REQUEST {
        return match sandbox::run_probe(sandbox_status) {
            Ok(()) => CompileResponse::Ok(super::protocol::TEST_LANDLOCK_PROBE_RESPONSE.to_vec()),
            Err(err) => CompileResponse::Err(err),
        };
    }
    let _ = sandbox_status;
    handle_compile(engines, request)
}

fn handle_compile(
    engines: &mut HashMap<u32, wasmtime::Engine>,
    request: CompileRequest,
) -> CompileResponse {
    #[cfg(feature = "test_features")]
    if request.prepared_code == super::protocol::TEST_ABORT_REQUEST {
        std::process::abort();
    }

    let engine = match engines.entry(request.max_memory_pages) {
        hash_map::Entry::Occupied(e) => e.into_mut(),
        hash_map::Entry::Vacant(e) => match create_compiler_engine(request.max_memory_pages) {
            Ok(engine) => e.insert(engine),
            Err(e) => return CompileResponse::Err(format!("failed to create engine: {e}")),
        },
    };
    match engine.precompile_module(&request.prepared_code) {
        Ok(bytes) => CompileResponse::Ok(bytes),
        Err(e) => CompileResponse::Err(e.to_string()),
    }
}

#[cfg(unix)]
fn set_memory_limit() {
    let ret = unsafe {
        let limit = libc::rlimit {
            rlim_cur: MIN_WORKER_MEMORY_LIMIT_BYTES,
            rlim_max: MIN_WORKER_MEMORY_LIMIT_BYTES,
        };
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

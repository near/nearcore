//! Fork-based out-of-process WASM compilation.
//!
//! Isolates Cranelift compilation into a forked child process to protect
//! the main neard process from compiler crashes and to enforce a memory
//! limit via RLIMIT_AS.
//!
//! The child inherits the wasmtime Engine via COW pages -- no serialization
//! needed. Only the Cranelift code path runs in the child; it does not touch
//! tokio, rayon, or any shared mutable state, so fork is safe here despite
//! the parent being multithreaded. jemalloc handles its own locks via
//! `pthread_atfork`.
//!
//! On non-unix platforms, falls back to in-process compilation.

use crate::logic::errors::CompilationError;
use wasmtime::Engine;

/// Memory limit for the forked compilation subprocess.
#[cfg(unix)]
const MEMORY_LIMIT_BYTES: u64 = 4 * 1024 * 1024 * 1024;

#[cfg(unix)]
pub(crate) fn compile_in_fork(
    engine: &Engine,
    prepared_code: &[u8],
) -> Result<Vec<u8>, CompilationError> {
    use std::io::{Read, Write};
    use std::os::unix::io::FromRawFd;

    let mut fds = [0 as libc::c_int; 2];
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return Err(CompilationError::WasmtimeCompileError {
            msg: format!("failed to create pipe: {}", std::io::Error::last_os_error()),
        });
    }
    let mut read_pipe = unsafe { std::fs::File::from_raw_fd(fds[0]) };
    let mut write_pipe = unsafe { std::fs::File::from_raw_fd(fds[1]) };

    match unsafe { libc::fork() } {
        -1 => Err(CompilationError::WasmtimeCompileError {
            msg: format!("fork failed: {}", std::io::Error::last_os_error()),
        }),
        0 => {
            // Child process.
            drop(read_pipe);
            let ret = unsafe {
                let limit =
                    libc::rlimit { rlim_cur: MEMORY_LIMIT_BYTES, rlim_max: MEMORY_LIMIT_BYTES };
                libc::setrlimit(libc::RLIMIT_AS, &limit)
            };
            if ret != 0 {
                eprintln!(
                    "warning: failed to set compilation memory limit: {}",
                    std::io::Error::last_os_error()
                );
            }
            let response = match engine.precompile_module(prepared_code) {
                Ok(bytes) => ChildResponse::Ok(bytes),
                Err(e) => ChildResponse::Err(e.to_string()),
            };
            let payload = borsh::to_vec(&response).unwrap_or_default();
            let exit_code = match write_pipe.write_all(&payload) {
                Ok(()) => 0,
                Err(_) => 1,
            };
            drop(write_pipe);
            unsafe { libc::_exit(exit_code) }
        }
        child_pid => {
            // Parent process.
            drop(write_pipe);
            let mut data = Vec::new();
            if let Err(err) = read_pipe.read_to_end(&mut data) {
                data.clear();
                tracing::warn!(%err, "failed to read from compilation subprocess");
            }
            drop(read_pipe);
            let mut status: libc::c_int = 0;
            unsafe { libc::waitpid(child_pid, &mut status, 0) };
            let response: ChildResponse =
                borsh::from_slice(&data).map_err(|_| CompilationError::WasmtimeCompileError {
                    msg: format!(
                        "compilation subprocess failed (status {:#x}, output {} bytes)",
                        status,
                        data.len(),
                    ),
                })?;
            match response {
                ChildResponse::Ok(bytes) => Ok(bytes),
                ChildResponse::Err(msg) => Err(CompilationError::WasmtimeCompileError { msg }),
            }
        }
    }
}

#[cfg(unix)]
#[derive(borsh::BorshSerialize, borsh::BorshDeserialize)]
enum ChildResponse {
    Ok(Vec<u8>),
    Err(String),
}

#[cfg(not(unix))]
pub(crate) fn compile_in_fork(
    engine: &Engine,
    prepared_code: &[u8],
) -> Result<Vec<u8>, CompilationError> {
    engine
        .precompile_module(prepared_code)
        .map_err(|e| CompilationError::WasmtimeCompileError { msg: e.to_string() })
}

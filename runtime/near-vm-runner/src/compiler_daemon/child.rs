//! Compiler daemon subprocess entry point.
//!
//! Runs inside the child process spawned by the parent. Sets a memory limit,
//! then loops reading compilation requests and writing responses.

use super::protocol::{CompileRequest, CompileResponse, read_frame, write_frame};
use crate::wasmtime_runner::create_compiler_engine;
use std::collections::{HashMap, hash_map};

const MEMORY_LIMIT_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Entry point for the compiler daemon subprocess.
/// Called when the binary is invoked with the `compile-wasm` argument.
pub fn daemon_main() -> ! {
    set_memory_limit();

    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut reader = stdin.lock();
    let mut writer = stdout.lock();
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
        let response = handle_compile(&mut engines, request);
        if write_frame(&mut writer, &borsh::to_vec(&response).unwrap()).is_err() {
            std::process::exit(0);
        }
    }
}

fn handle_compile(
    engines: &mut HashMap<u32, wasmtime::Engine>,
    request: CompileRequest,
) -> CompileResponse {
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
        let limit = libc::rlimit { rlim_cur: MEMORY_LIMIT_BYTES, rlim_max: MEMORY_LIMIT_BYTES };
        libc::setrlimit(libc::RLIMIT_AS, &limit)
    };
    if ret != 0 {
        eprintln!("warning: failed to set memory limit: {}", std::io::Error::last_os_error());
    }
}

#[cfg(not(unix))]
fn set_memory_limit() {}

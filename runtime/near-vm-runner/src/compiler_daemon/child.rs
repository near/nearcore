//! Compiler daemon subprocess entry point.
//!
//! Runs inside the child process spawned by the parent. Sets a memory limit,
//! then loops reading compilation requests and writing responses.

use super::protocol::{
    BenchmarkResponse, CompileRequest, CompileResponse, RequestKind, read_frame, write_frame,
};
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
        // First byte is the request kind tag, rest is the borsh-encoded CompileRequest.
        let (kind, request): (RequestKind, CompileRequest) = match borsh::from_slice(&frame) {
            Ok(r) => r,
            Err(e) => {
                let resp = CompileResponse::Err(format!("failed to deserialize request: {e}"));
                let _ = write_frame(&mut writer, &borsh::to_vec(&resp).unwrap());
                continue;
            }
        };
        match kind {
            RequestKind::Compile => {
                let response = handle_compile(&mut engines, request);
                if write_frame(&mut writer, &borsh::to_vec(&response).unwrap()).is_err() {
                    std::process::exit(0);
                }
            }
            RequestKind::Benchmark => {
                let response = handle_benchmark(&mut engines, request);
                if write_frame(&mut writer, &borsh::to_vec(&response).unwrap()).is_err() {
                    std::process::exit(0);
                }
            }
        }
    }
}

fn handle_compile(
    engines: &mut HashMap<u32, wasmtime::Engine>,
    request: CompileRequest,
) -> CompileResponse {
    let engine = match get_or_create_engine(engines, request.max_memory_pages) {
        Ok(e) => e,
        Err(msg) => return CompileResponse::Err(msg),
    };
    match engine.precompile_module(&request.prepared_code) {
        Ok(bytes) => CompileResponse::Ok(bytes),
        Err(e) => CompileResponse::Err(e.to_string()),
    }
}

fn handle_benchmark(
    engines: &mut HashMap<u32, wasmtime::Engine>,
    request: CompileRequest,
) -> BenchmarkResponse {
    let engine = match get_or_create_engine(engines, request.max_memory_pages) {
        Ok(e) => e,
        Err(msg) => {
            return BenchmarkResponse {
                result: Err(msg),
                compile_time_nanos: 0,
                peak_rss_bytes: 0,
            };
        }
    };
    let start = std::time::Instant::now();
    let result = engine.precompile_module(&request.prepared_code);
    let compile_time_nanos = start.elapsed().as_nanos() as u64;
    let peak_rss_bytes = get_peak_rss();
    BenchmarkResponse {
        result: result.map_err(|e| e.to_string()),
        compile_time_nanos,
        peak_rss_bytes,
    }
}

fn get_or_create_engine(
    engines: &mut HashMap<u32, wasmtime::Engine>,
    max_memory_pages: u32,
) -> Result<&mut wasmtime::Engine, String> {
    match engines.entry(max_memory_pages) {
        hash_map::Entry::Occupied(e) => Ok(e.into_mut()),
        hash_map::Entry::Vacant(e) => match create_compiler_engine(max_memory_pages) {
            Ok(engine) => Ok(e.insert(engine)),
            Err(e) => Err(format!("failed to create engine: {e}")),
        },
    }
}

#[cfg(unix)]
fn get_peak_rss() -> u64 {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    let ret = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    if ret != 0 {
        return 0;
    }
    // On macOS, ru_maxrss is in bytes. On Linux, it's in kilobytes.
    let rss = usage.ru_maxrss as u64;
    if cfg!(target_os = "macos") { rss } else { rss * 1024 }
}

#[cfg(not(unix))]
fn get_peak_rss() -> u64 {
    0
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

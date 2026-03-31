//! Parent-side client for the out-of-process compiler daemon.
//!
//! A global Mutex serializes all access to the daemon subprocess and its
//! pipes, so concurrent callers block until the current compilation finishes.

use super::protocol::{CompileRequest, CompileResponse, read_frame, write_frame};
use crate::logic::errors::CompilationError;
use near_parameters::vm::LimitConfig;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Mutex, OnceLock};

static DAEMON_BINARY: OnceLock<PathBuf> = OnceLock::new();

/// Set the path to the binary that should be spawned as the compiler daemon.
/// In production, `neard` calls this with its own binary path at startup.
/// In integration tests, point it at the test binary.
/// Must be called at most once; subsequent calls are ignored.
pub fn set_daemon_binary(path: PathBuf) {
    if DAEMON_BINARY.set(path).is_err() {
        tracing::warn!("set_daemon_binary called more than once, ignoring");
    }
}

/// Returns true if a daemon binary has been configured via `set_daemon_binary`.
pub fn is_daemon_configured() -> bool {
    DAEMON_BINARY.get().is_some()
}

type CompileResult = Result<Vec<u8>, String>;

struct DaemonProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
}

impl DaemonProcess {
    fn spawn(binary: &Path) -> std::io::Result<Self> {
        let mut child = Command::new(binary)
            .arg("compile-wasm")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        Ok(Self { child, stdin, stdout })
    }

    /// Send a compilation request and read the response. Returns:
    /// - `Ok(Ok(bytes))` -- compilation succeeded
    /// - `Ok(Err(msg))` -- daemon reported a compilation error (not retryable)
    /// - `Err(msg)` -- IPC failure, daemon likely crashed (retryable)
    fn compile_raw(&mut self, request: &CompileRequest) -> Result<CompileResult, String> {
        let request_bytes =
            borsh::to_vec(request).map_err(|e| format!("failed to serialize request: {e}"))?;
        write_frame(&mut self.stdin, &request_bytes)
            .map_err(|e| format!("failed to send to compiler daemon: {e}"))?;
        let response_bytes = read_frame(&mut self.stdout)
            .map_err(|e| format!("failed to read from compiler daemon: {e}"))?;
        let response: CompileResponse = borsh::from_slice(&response_bytes)
            .map_err(|e| format!("failed to deserialize response: {e}"))?;
        match response {
            CompileResponse::Ok(bytes) => Ok(Ok(bytes)),
            CompileResponse::Err(msg) => Ok(Err(msg)),
        }
    }

    fn is_alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }
}

impl Drop for DaemonProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct DaemonState {
    binary: PathBuf,
    daemon: Option<DaemonProcess>,
}

const MAX_SPAWN_ATTEMPTS: u32 = 2;

impl DaemonState {
    fn ensure_daemon(&mut self) -> Result<&mut DaemonProcess, String> {
        if !self.daemon.as_mut().is_some_and(|daemon| daemon.is_alive()) {
            self.daemon = Some(
                DaemonProcess::spawn(&self.binary)
                    .map_err(|e| format!("failed to spawn compiler daemon: {e}"))?,
            );
        }
        Ok(self.daemon.as_mut().unwrap())
    }

    fn compile(&mut self, request: &CompileRequest) -> CompileResult {
        let mut last_err = String::new();
        for attempt in 0..MAX_SPAWN_ATTEMPTS {
            let daemon = self.ensure_daemon()?;
            match daemon.compile_raw(request) {
                Ok(result) => return result,
                Err(ipc_err) => {
                    tracing::warn!(
                        attempt,
                        err = ipc_err,
                        "compiler daemon process failed, respawning"
                    );
                    last_err = ipc_err;
                    self.daemon = None;
                }
            }
        }
        tracing::error!(attempts = MAX_SPAWN_ATTEMPTS, "compiler daemon process failed, giving up");
        Err(last_err)
    }
}

static DAEMON_STATE: OnceLock<Mutex<DaemonState>> = OnceLock::new();

fn get_or_init_state() -> &'static Mutex<DaemonState> {
    DAEMON_STATE.get_or_init(|| {
        let binary = DAEMON_BINARY.get().expect("daemon binary not configured").clone();
        Mutex::new(DaemonState { binary, daemon: None })
    })
}

/// Compile prepared WASM code in the out-of-process daemon.
///
/// Panics if no daemon binary has been configured via `set_daemon_binary`.
pub fn compile_in_subprocess(
    prepared_code: &[u8],
    limit_config: &LimitConfig,
) -> Result<Vec<u8>, CompilationError> {
    let request = CompileRequest {
        prepared_code: prepared_code.to_vec(),
        max_memory_pages: limit_config.max_memory_pages,
        max_tables_per_contract: limit_config.max_tables_per_contract,
        max_elements_per_contract_table: limit_config
            .max_elements_per_contract_table
            .map(|v| v as u64),
    };

    let mut state = get_or_init_state().lock().unwrap();
    state.compile(&request).map_err(|msg| CompilationError::WasmtimeCompileError { msg })
}

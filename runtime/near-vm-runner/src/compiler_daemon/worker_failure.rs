use std::fmt;
use std::process::ExitStatus;
use std::time::Duration;

#[derive(Debug)]
pub(crate) enum WorkerFailure {
    LocalMemoryExhaustion,
    Evicted,
    WatchdogTimeout { phase: &'static str, timeout: Duration },
    Spawn(String),
    Startup(String),
    Crash { status: ExitStatus, protocol_error: String },
    Protocol { error: String, cleanup_status: Option<ExitStatus> },
}

impl fmt::Display for WorkerFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LocalMemoryExhaustion => {
                f.write_str("compiler daemon exhausted its local memory limit")
            }
            Self::Evicted => f.write_str("compiler daemon worker was evicted"),
            Self::WatchdogTimeout { phase, timeout } => write!(
                f,
                "compiler daemon timed out during {phase} after {} seconds",
                timeout.as_secs()
            ),
            Self::Spawn(err) => write!(f, "failed to spawn compiler daemon: {err}"),
            Self::Startup(err) => write!(f, "compiler daemon startup failed: {err}"),
            Self::Crash { status, protocol_error } => {
                write!(f, "compiler daemon exited with {status}: {protocol_error}")
            }
            Self::Protocol { error, cleanup_status: Some(status) } => {
                write!(f, "compiler daemon protocol failed: {error}; cleanup ended with {status}")
            }
            Self::Protocol { error, cleanup_status: None } => {
                write!(f, "compiler daemon protocol failed: {error}")
            }
        }
    }
}

pub(crate) fn worker_failure_kind(failure: &WorkerFailure) -> &'static str {
    match failure {
        WorkerFailure::LocalMemoryExhaustion => "local_memory_exhaustion",
        WorkerFailure::Evicted => "evicted",
        WorkerFailure::WatchdogTimeout { .. } => "watchdog_timeout",
        WorkerFailure::Spawn(_) => "spawn",
        WorkerFailure::Startup(_) => "startup",
        WorkerFailure::Crash { .. } => "crash",
        WorkerFailure::Protocol { .. } => "protocol",
    }
}

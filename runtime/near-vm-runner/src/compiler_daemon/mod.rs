//! Out-of-process WASM compiler daemon.
//!
//! Isolates Cranelift compilation into a subprocess to protect the main neard
//! process from compiler crashes and to enforce memory limits.
//!
//! A global Mutex serializes access to the daemon subprocess and its
//! pipes, so concurrent callers block until the current compilation
//! finishes.

mod child;
mod parent;
pub mod protocol;

pub use child::daemon_main;
pub use parent::{
    benchmark_in_subprocess, compile_in_subprocess, is_daemon_configured, set_daemon_binary,
};

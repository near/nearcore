//! Out-of-process WASM compiler daemon worker.
//!
//! This is only a thin wrapper around
//! [`near_vm_runner::compiler_daemon::daemon_main`], making it easy for neard
//! to include a command that directly calls it without the need for a second
//! binary.

fn main() -> ! {
    near_vm_runner::compiler_daemon::daemon_main()
}

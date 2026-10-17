use crate::compiler_daemon::protocol::WasmtimeVersion;
pub(crate) use implementation::*;
pub(super) use wasmtime_48 as wasmtime;

// Shared code with other wasmtime versions
#[path = "wasmtime_runner/implementation.rs"]
mod implementation;

const WASMTIME_VERSION: WasmtimeVersion = WasmtimeVersion::V48;

fn apply_version_specific_config(config: &mut wasmtime::Config) {
    // Wasmtime 48 validates this relationship even when async support is disabled.
    config.async_stack_size(1024 * 1024 * 1024);
}

fn is_version_specific_unreachable_trap(trap: wasmtime::Trap) -> bool {
    // These require component-model async or exception-handling features, which NEAR disables.
    matches!(trap, wasmtime::Trap::WaitableSyncAndAsync | wasmtime::Trap::UncaughtException)
}

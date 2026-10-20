use crate::compiler_daemon::protocol::WasmtimeVersion;
pub(crate) use implementation::*;
pub(super) use wasmtime_45 as wasmtime;

// Shared code with other wasmtime versions
#[path = "wasmtime_runner/implementation.rs"]
mod implementation;

const WASMTIME_VERSION: WasmtimeVersion = WasmtimeVersion::V45;

fn apply_version_specific_config(_config: &mut wasmtime::Config) {}

fn is_version_specific_unreachable_trap(_trap: wasmtime::Trap) -> bool {
    false
}

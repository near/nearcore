// cspell:ignore waitlist

#![doc = include_str!("../README.md")]

mod cache;
mod errors;
mod features;
mod imports;
pub mod logic;
#[cfg(feature = "metrics")]
mod metrics;
#[cfg(feature = "prepare")]
pub mod prepare;
mod profile;
mod runner;
#[cfg(test)]
mod tests;
mod utils;
#[cfg(feature = "wasmtime_vm")]
mod wasmtime_runner;

pub use crate::logic::with_ext_cost_counter;
#[cfg(not(windows))]
pub use cache::FilesystemContractRuntimeCache;
#[cfg(feature = "wasmtime_vm")]
pub use cache::config_cache_key_signature;
pub use cache::{
    CompiledContract, CompiledContractInfo, ContractRuntimeCache, MockContractRuntimeCache,
    NoContractRuntimeCache, noop_background_spawner, precompile_contract, try_precompile_contract,
};
pub use errors::ContractPrecompilatonResult;
#[cfg(feature = "metrics")]
pub use metrics::{report_metrics, reset_metrics};
pub use near_primitives_core::code::ContractCode;
pub use profile::ProfileDataV3;
pub use runner::{Contract, PreparedContract, VM, contract_cached, prepare, run};

#[cfg(any(feature = "prepare", feature = "wasmtime_vm"))]
pub(crate) const MEMORY_EXPORT: &str = "memory";

#[cfg(any(feature = "prepare", feature = "wasmtime_vm"))]
pub(crate) const REMAINING_GAS_EXPORT: &str = "remaining_gas";

#[cfg(any(feature = "prepare", feature = "wasmtime_vm"))]
pub(crate) const START_EXPORT: &str = "start";

#[cfg(any(feature = "prepare", feature = "wasmtime_vm"))]
pub(crate) const EXPORT_PREFIX: &str = "\0";

/// This is public for internal experimentation use only, and should otherwise be considered an
/// implementation detail of `near-vm-runner`.
#[doc(hidden)]
pub mod internal {
    pub use crate::runner::VMKindExt;
}

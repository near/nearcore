#![doc = include_str!("../README.md")]
#![cfg_attr(enable_const_type_id, feature(const_type_id))]

mod cache;
mod errors;
mod features;
mod imports;
pub mod logic;
#[cfg(feature = "metrics")]
mod metrics;
#[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
mod near_vm_runner;
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
pub use cache::{
    CompiledContract, CompiledContractInfo, ContractRuntimeCache, MockContractRuntimeCache,
    NoContractRuntimeCache, get_contract_cache_key, precompile_contract,
};
#[cfg(feature = "metrics")]
pub use metrics::{report_metrics, reset_metrics};
pub use near_primitives_core::code::ContractCode;
pub use profile::ProfileDataV3;
pub use runner::{Contract, PreparedContract, VM, prepare, run};

/// This is public for internal experimentation use only, and should otherwise be considered an
/// implementation detail of `near-vm-runner`.
#[doc(hidden)]
pub mod internal {
    pub use crate::runner::VMKindExt;
    #[cfg(feature = "prepare")]
    pub use wasmparser;
}

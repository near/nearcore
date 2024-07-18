#![doc = include_str!("../README.md")]

mod cache;
mod code;
mod errors;
mod features;
mod imports;
#[cfg(feature = "prepare")]
mod instrument;
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
#[cfg(all(feature = "wasmer2_vm", target_arch = "x86_64"))]
mod wasmer2_runner;
#[cfg(all(feature = "wasmer0_vm", target_arch = "x86_64"))]
mod wasmer_runner;
#[cfg(feature = "wasmtime_vm")]
mod wasmtime_runner;

pub use crate::logic::with_ext_cost_counter;
pub use cache::{
    get_contract_cache_key, precompile_contract, CompiledContract, CompiledContractInfo,
    ContractRuntimeCache, FilesystemContractRuntimeCache, MockContractRuntimeCache,
    NoContractRuntimeCache,
};
pub use code::ContractCode;
#[cfg(feature = "metrics")]
pub use metrics::{report_metrics, reset_metrics};
pub use profile::ProfileDataV3;
pub use runner::{run, prepare, PreparedContract, Contract, VM};

/// This is public for internal experimentation use only, and should otherwise be considered an
/// implementation detail of `near-vm-runner`.
#[doc(hidden)]
pub mod internal {
    pub use crate::runner::VMKindExt;
    #[cfg(feature = "prepare")]
    pub use wasmparser;
}

#![doc = include_str!("../README.md")]

mod cache;
mod errors;
mod imports;
#[cfg(all(feature = "wasmer0_vm", not(target_arch = "aarch64")))]
mod memory;
#[cfg(not(target_arch = "aarch64"))]
mod preload;
pub mod prepare;
mod runner;
#[cfg(test)]
mod tests;
mod vm_kind;
#[cfg(all(feature = "wasmer2_vm", not(target_arch = "aarch64")))]
mod wasmer2_runner;
#[cfg(all(feature = "wasmer0_vm", not(target_arch = "aarch64")))]
mod wasmer_runner;
#[cfg(feature = "wasmtime_vm")]
mod wasmtime_runner;

pub use near_vm_errors::VMError;
pub use near_vm_logic::with_ext_cost_counter;

pub use cache::{
    get_contract_cache_key, precompile_contract, precompile_contract_vm, MockCompiledContractCache,
};
#[cfg(not(target_arch = "aarch64"))]
pub use preload::{ContractCallPrepareRequest, ContractCallPrepareResult, ContractCaller};
pub use runner::{run, VM};

/// This is public for internal experimentation use only, and should otherwise be considered an
/// implementation detail of `near-vm-runner`.
#[doc(hidden)]
pub mod internal {
    pub use crate::vm_kind::VMKind;
    pub use wasmparser;
}

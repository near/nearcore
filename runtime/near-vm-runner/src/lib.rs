#![doc = include_str!("../README.md")]

mod cache;
mod errors;
mod imports;
#[cfg(feature = "wasmer0_vm")]
mod memory;
mod preload;
pub mod prepare;
mod runner;
#[cfg(test)]
mod tests;
mod vm_kind;
#[cfg(feature = "wasmer2_vm")]
mod wasmer2_runner;
#[cfg(feature = "wasmer0_vm")]
mod wasmer_runner;
#[cfg(feature = "wasmtime_vm")]
mod wasmtime_runner;

pub use near_vm_errors::VMError;
pub use near_vm_logic::with_ext_cost_counter;

pub use cache::{
    get_contract_cache_key, precompile_contract, precompile_contract_vm, MockCompiledContractCache,
};
pub use preload::{ContractCallPrepareRequest, ContractCallPrepareResult, ContractCaller};
pub use runner::{run, VM};

/// This is public for internal experimentation use only, and should otherwise be considered an
/// implementation detail of `near-vm-runner`.
#[doc(hidden)]
pub mod internal {
    pub use crate::vm_kind::VMKind;
    pub use wasmparser;
}

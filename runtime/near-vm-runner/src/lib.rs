#[cfg(test)]
mod tests;

mod cache;
mod errors;
mod imports;
#[cfg(feature = "wasmer0_vm")]
mod memory;
mod vm_kind;

mod preload;
pub mod prepare;
mod runner;

#[cfg(feature = "wasmer0_vm")]
mod wasmer_runner;

#[cfg(feature = "wasmtime_vm")]
mod wasmtime_runner;

#[cfg(feature = "wasmer1_vm")]
mod wasmer1_runner;

pub use near_vm_errors::VMError;
pub use preload::{ContractCallPrepareRequest, ContractCallPrepareResult, ContractCaller};
pub use runner::compile_module;
pub use runner::run;

pub use near_vm_logic::with_ext_cost_counter;

pub use cache::precompile_contract;
pub use cache::MockCompiledContractCache;

// These two are public for the standalone runner, but are an implementation
// detail of `near-vm-runner`. Public API like `run` should not expose VMKind.
pub use runner::run_vm;
pub use vm_kind::VMKind;

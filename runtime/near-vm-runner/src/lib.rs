mod cache;
mod errors;
mod imports;
mod memory;
pub mod prepare;
mod runner;
mod wasmer_runner;
#[cfg(feature = "wasmtime_vm")]
mod wasmtime_runner;
pub use near_vm_errors::VMError;
pub use runner::{compile_module, run, run_vm, run_vm_profiled, with_vm_variants};

#[cfg(feature = "costs_counting")]
pub use near_vm_logic::EXT_COSTS_COUNTER;

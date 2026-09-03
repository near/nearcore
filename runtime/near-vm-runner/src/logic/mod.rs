// Without a VM backend there is nothing calling the host-function support code in this module
// (registers, gas payment helpers, bls12381 / alt_bn128 primitives, ...); it only exists to serve
// `wasmtime_runner`. Keep the crate building as a types-only dependency in that configuration.
#![cfg_attr(not(feature = "wasmtime_vm"), allow(dead_code))]

pub(crate) mod alt_bn128;
pub(crate) mod bls12381;
mod context;
mod dependencies;
pub mod errors;
pub mod gas_counter;
pub(crate) mod logic;
pub mod mocks;
pub mod recorded_storage_counter;
#[cfg(all(test, feature = "wasmtime_vm"))]
mod tests;
pub mod types;
pub(crate) mod utils;
pub(crate) mod vmstate;

pub use context::VMContext;
pub use dependencies::{External, MemSlice, StorageAccessTracker, ValuePtr};
pub use errors::{HostError, VMLogicError};
pub use gas_counter::{GasCounter, with_ext_cost_counter};
pub use logic::{ExecutionResultState, VMOutcome};
pub use near_parameters::vm::{Config, ContractPrepareVersion, LimitConfig};
pub use near_primitives_core::types::ProtocolVersion;
pub use types::ReturnData;

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

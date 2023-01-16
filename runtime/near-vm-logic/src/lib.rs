#![doc = include_str!("../README.md")]

mod alt_bn128;
mod context;
mod dependencies;
pub mod gas_counter;
mod logic;
pub mod mocks;
pub(crate) mod receipt_manager;
pub mod test_utils;
#[cfg(test)]
mod tests;
pub mod types;
mod utils;
mod vmstate;

pub use context::VMContext;
pub use dependencies::{External, MemSlice, MemoryLike, StorageGetMode, ValuePtr};
pub use logic::{VMLogic, VMOutcome};
pub use near_primitives_core::config::*;
pub use near_primitives_core::profile;
pub use near_primitives_core::types::ProtocolVersion;
pub use near_vm_errors::{HostError, VMLogicError};
pub use receipt_manager::ReceiptMetadata;
pub use types::ReturnData;

pub use gas_counter::with_ext_cost_counter;

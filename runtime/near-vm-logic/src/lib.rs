#[cfg(feature = "protocol_feature_alt_bn128")]
pub mod alt_bn128;
mod context;
mod dependencies;
pub mod gas_counter;
mod logic;
pub mod mocks;
pub mod serde_with;
pub mod types;
mod utils;

pub use context::VMContext;
pub use dependencies::{External, MemoryLike, ValuePtr};
pub use logic::{VMLogic, VMOutcome};
pub use near_primitives_core::config::*;
pub use near_primitives_core::profile;
pub use near_primitives_core::types::ProtocolVersion;
pub use near_vm_errors::{HostError, VMLogicError};
pub use types::ReturnData;

pub use gas_counter::with_ext_cost_counter;

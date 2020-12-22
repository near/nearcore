mod config;
mod context;
mod dependencies;
pub mod gas_counter;
mod logic;
pub mod mocks;
pub mod serde_with;
pub mod types;
mod utils;

pub use config::VMKind;
pub use context::VMContext;
pub use dependencies::{External, MemoryLike, ValuePtr};
pub use logic::{VMLogic, VMOutcome};
pub use near_primitives::config::*;
pub use near_primitives::profile;
pub use near_primitives::version;
pub use near_vm_errors::{HostError, VMLogicError};
pub use types::ReturnData;

#[cfg(feature = "costs_counting")]
pub use gas_counter::EXT_COSTS_COUNTER;

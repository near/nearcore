mod config;
mod context;
mod dependencies;
mod gas_counter;
mod logic;
#[cfg(not(target_arch = "wasm32"))]
pub mod mocks;
pub mod serde_with;

pub mod types;
pub use config::VMConfig;
pub use context::VMContext;
pub use dependencies::{External, MemoryLike};
pub use logic::{VMLogic, VMOutcome};
pub use near_vm_errors::{HostError, HostErrorOrStorageError};
pub use types::ReturnData;

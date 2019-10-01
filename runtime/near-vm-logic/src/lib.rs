mod config;
mod context;
mod dependencies;
mod logic;
#[cfg(feature = "mocks")]
pub mod mocks;
pub mod serde_with;

pub mod types;
pub use config::Config;
pub use context::VMContext;
pub use dependencies::{External, MemoryLike};
pub use logic::{VMLogic, VMOutcome};
pub use near_vm_errors::{HostError, HostErrorOrStorageError};
pub use types::ReturnData;

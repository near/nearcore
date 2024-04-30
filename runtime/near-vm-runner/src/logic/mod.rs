mod alt_bn128;
mod context;
mod dependencies;
pub mod errors;
pub mod gas_counter;
mod logic;
pub mod mocks;
pub mod recorded_storage_counter;
pub mod test_utils;
#[cfg(test)]
mod tests;
pub mod types;
mod utils;
mod vmstate;

pub use context::VMContext;
pub use dependencies::{External, MemSlice, MemoryLike, TrieNodesCount, ValuePtr};
pub use errors::{HostError, VMLogicError};
pub use gas_counter::with_ext_cost_counter;
pub use logic::{VMLogic, VMOutcome};
pub use near_parameters::vm::{Config, ContractPrepareVersion, LimitConfig, StorageGetMode};
pub use near_primitives_core::types::ProtocolVersion;
pub use types::ReturnData;

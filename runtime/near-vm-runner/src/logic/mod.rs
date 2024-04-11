use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::hash::CryptoHash;
use std::fmt;

mod alt_bn128;
#[cfg(feature = "protocol_feature_bls12381")]
mod bls12381;
mod context;
mod dependencies;
pub mod errors;
pub mod gas_counter;
mod logic;
pub mod mocks;
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

#[derive(Debug, Clone, PartialEq, BorshDeserialize, BorshSerialize)]
pub enum CompiledContract {
    CompileModuleError(errors::CompilationError),
    Code(Vec<u8>),
}

/// Cache for compiled modules
pub trait CompiledContractCache: Send + Sync {
    fn put(&self, key: &CryptoHash, value: CompiledContract) -> std::io::Result<()>;
    fn get(&self, key: &CryptoHash) -> std::io::Result<Option<CompiledContract>>;
    fn has(&self, key: &CryptoHash) -> std::io::Result<bool> {
        self.get(key).map(|entry| entry.is_some())
    }
}

impl fmt::Debug for dyn CompiledContractCache {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Compiled contracts cache")
    }
}

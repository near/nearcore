use near_primitives_core::hash::CryptoHash;
use std::fmt;

mod alt_bn128;
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

#[derive(Debug, Clone, PartialEq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum CompiledContract {
    CompileModuleError(errors::CompilationError),
    Code(Vec<u8>),
}

/// Cache for compiled modules
pub trait CompiledContractCache: Send + Sync {
    fn put(&self, key: &CryptoHash, value: CompiledContract) -> std::io::Result<()>;
    /// Inspect the cache for the `key`.
    ///
    /// If a lookup fails, `Err` is returned.
    ///
    /// The callback is called with a reference to the value in case the contract is found. If this
    /// happens, this function is guaranteed to return `Ok(true)`.
    ///
    /// Otherwise, the `key` was not found in the cache and `Ok(false)` is returned.
    fn with(
        &self,
        key: &CryptoHash,
        callback: &mut dyn FnMut(&rkyv::Archived<CompiledContract>),
    ) -> std::io::Result<bool>;
    fn has(&self, key: &CryptoHash) -> std::io::Result<bool> {
        self.with(key, &mut |_| {})
    }
}

impl fmt::Debug for dyn CompiledContractCache {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Compiled contracts cache")
    }
}

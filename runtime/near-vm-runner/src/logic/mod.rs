use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::hash::CryptoHash;
use std::fmt;
use types::AccountId;

pub mod action;
mod alt_bn128;
mod context;
pub mod delegate_action;
mod dependencies;
pub mod errors;
pub mod gas_counter;
mod logic;
pub mod mocks;
mod receipt_manager;
pub mod signable_message;
pub mod test_utils;
#[cfg(test)]
mod tests;
pub mod types;
mod utils;
mod vmstate;

pub use context::VMContext;
pub use dependencies::{External, MemSlice, MemoryLike, StorageGetMode, ValuePtr};
pub use errors::{HostError, VMLogicError};
pub use logic::{VMLogic, VMOutcome};
pub use near_primitives_core::config::*;
pub use near_primitives_core::profile;
pub use near_primitives_core::types::ProtocolVersion;
pub use receipt_manager::ReceiptMetadata;
pub use types::ReturnData;

pub use gas_counter::with_ext_cost_counter;

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

/// Counts trie nodes reads during tx/receipt execution for proper storage costs charging.
#[derive(Debug, PartialEq)]
pub struct TrieNodesCount {
    /// Potentially expensive trie node reads which are served from disk in the worst case.
    pub db_reads: u64,
    /// Cheap trie node reads which are guaranteed to be served from RAM.
    pub mem_reads: u64,
}

impl TrieNodesCount {
    /// Used to determine the number of trie nodes charged during some operation.
    pub fn checked_sub(self, other: &Self) -> Option<Self> {
        Some(Self {
            db_reads: self.db_reads.checked_sub(other.db_reads)?,
            mem_reads: self.mem_reads.checked_sub(other.mem_reads)?,
        })
    }
}

/// The outgoing (egress) data which will be transformed
/// to a `DataReceipt` to be sent to a `receipt.receiver`
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Hash,
    Clone,
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct DataReceiver {
    pub data_id: CryptoHash,
    pub receiver_id: AccountId,
}

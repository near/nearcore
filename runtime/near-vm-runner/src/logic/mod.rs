use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::hash::CryptoHash;
use types::AccountId;

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
pub use dependencies::{External, MemSlice, MemoryLike, ValuePtr};
pub use errors::{HostError, VMLogicError};
pub use gas_counter::with_ext_cost_counter;
pub use logic::{VMLogic, VMOutcome};
pub use near_parameters::vm::{Config, ContractPrepareVersion, LimitConfig, StorageGetMode};
pub use near_primitives_core::types::ProtocolVersion;
pub use types::ReturnData;

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

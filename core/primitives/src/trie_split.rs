use crate::types::AccountId;
use borsh::{BorshDeserialize, BorshSerialize};

/// The result of splitting a memtrie into two possibly even parts, according to `memory_usage`
/// stored in the trie nodes.
///
/// **NOTE: This is an artificial value calculated according to `TRIE_COST`. Hence, it does not
/// represent actual memory allocation, but the split ratio should be roughly consistent with that.**
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, BorshSerialize, BorshDeserialize)]
pub struct TrieSplit {
    /// Account ID representing the split path
    pub boundary_account: AccountId,
    /// Total `memory_usage` of the left part (excluding the split path)
    pub left_memory: u64,
    /// Total `memory_usage` of the right part (including the split path)
    pub right_memory: u64,
}

impl TrieSplit {
    pub fn new(boundary_account: AccountId, left_memory: u64, right_memory: u64) -> Self {
        Self { boundary_account, left_memory, right_memory }
    }

    /// Dummy split that will be worse than any actual trie split
    pub fn dummy() -> Self {
        let account_id = "dummy".parse().unwrap();
        Self::new(account_id, 0, u64::MAX)
    }

    pub fn is_dummy(&self) -> bool {
        self.mem_diff() == u64::MAX
    }

    /// Get the split path as bytes
    pub fn split_path_bytes(&self) -> &[u8] {
        self.boundary_account.as_bytes()
    }

    /// Get absolute difference between right and left memory
    pub fn mem_diff(&self) -> u64 {
        self.right_memory.abs_diff(self.left_memory)
    }

    /// Get total memory usage (left + right)
    pub fn total_memory(&self) -> u64 {
        self.left_memory.saturating_add(self.right_memory)
    }
}

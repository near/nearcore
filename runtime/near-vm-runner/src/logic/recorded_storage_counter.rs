use super::errors::InconsistentStateError;
use super::{HostError, VMLogicError};

/// Keeps track of the recorded storage proof size and ensures it does not exceed the limit.
pub struct RecordedStorageCounter {
    initial_storage_size: usize,
    last_observed_storage_size: usize,
    size_limit: usize,
}

impl RecordedStorageCounter {
    pub fn new(initial_storage_size: usize, size_limit: usize) -> Self {
        Self { initial_storage_size, last_observed_storage_size: initial_storage_size, size_limit }
    }

    /// Update the latest observed storage proof size and check if it exceeds the limit.
    /// Should be called after every trie operation.
    pub fn observe_size(&mut self, latest_storage_proof_size: usize) -> Result<(), VMLogicError> {
        self.last_observed_storage_size = latest_storage_proof_size;

        let current_size = self.get_storage_size()?;
        if current_size > self.size_limit {
            return Err(VMLogicError::HostError(HostError::RecordedStorageExceeded {
                size: current_size,
                limit: self.size_limit,
            }));
        }

        Ok(())
    }

    /// Get the size of storage proof that has been recorded so far by this receipt.
    pub fn get_storage_size(&self) -> Result<usize, VMLogicError> {
        self.last_observed_storage_size
            .checked_sub(self.initial_storage_size)
            .ok_or(VMLogicError::InconsistentStateError(InconsistentStateError::IntegerOverflow))
    }
}

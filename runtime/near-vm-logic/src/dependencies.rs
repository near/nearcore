//! External dependencies of the near-vm-logic.

use crate::types::{AccountId, Balance, Gas, IteratorIndex, PromiseIndex, StorageUsage};

/// An abstraction over the memory of the smart contract.
pub trait MemoryLike {
    /// Returns whether the memory interval is completely inside the smart contract memory.
    fn fits_memory(&self, offset: u64, len: u64) -> bool;

    /// Reads the content of the given memory interval.
    ///
    /// # Panics
    ///
    /// If memory interval is outside the smart contract memory.
    fn read_memory(&self, offset: u64, len: u64) -> Vec<u8>;

    /// Reads a single byte from the memory.
    ///
    /// # Panics
    ///
    /// If pointer is outside the smart contract memory.
    fn read_memory_u8(&self, offset: u64) -> u8;

    /// Writes the buffer into the smart contract memory.
    ///
    /// # Panics
    ///
    /// If `offset + buffer.len()` is outside the smart contract memory.
    fn write_memory(&mut self, offset: u64, buffer: &[u8]);
}

pub enum ExternalError {
    InvalidPromiseIndex,
    InvalidIteratorIndex,
    IteratorWasInvalidated,
    InvalidAccountId,
}

pub type Result<T> = ::std::result::Result<T, ExternalError>;

pub trait External {
    fn storage_set(&mut self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>>;

    fn storage_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn storage_remove(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn storage_has_key(&mut self, key: &[u8]) -> Result<bool>;

    fn storage_iter(&mut self, prefix: &[u8]) -> Result<IteratorIndex>;

    fn storage_range(&mut self, start: &[u8], end: &[u8]) -> Result<IteratorIndex>;

    fn storage_iter_next(
        &mut self,
        iterator_idx: IteratorIndex,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>>;

    fn promise_create(
        &mut self,
        account_id: AccountId,
        method_name: Vec<u8>,
        arguments: Vec<u8>,
        amount: Balance,
        gas: Gas,
    ) -> Result<PromiseIndex>;

    fn promise_then(
        &mut self,
        promise_id: PromiseIndex,
        account_id: AccountId,
        method_name: Vec<u8>,
        arguments: Vec<u8>,
        amount: Balance,
        gas: Gas,
    ) -> Result<PromiseIndex>;

    fn promise_and(&mut self, promise_indices: &[PromiseIndex]) -> Result<PromiseIndex>;

    fn storage_usage(&self) -> StorageUsage;
}

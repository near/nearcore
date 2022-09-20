//! External dependencies of the near-vm-logic.

use near_primitives::hash::CryptoHash;
use near_primitives::types::TrieNodesCount;
use near_primitives_core::types::{AccountId, Balance};
use near_vm_errors::VMLogicError;

/// An abstraction over the memory of the smart contract.
pub trait MemoryLike {
    /// Returns whether the memory interval is completely inside the smart contract memory.
    fn fits_memory(&self, offset: u64, len: u64) -> bool;

    /// Reads the content of the given memory interval.
    ///
    /// # Panics
    ///
    /// If memory interval is outside the smart contract memory.
    fn read_memory(&self, offset: u64, buffer: &mut [u8]);

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

pub type Result<T> = ::std::result::Result<T, VMLogicError>;

/// Logical pointer to a value in storage.
/// Allows getting value length before getting the value itself. This is needed so that runtime
/// can charge gas before accessing a potentially large value.
pub trait ValuePtr {
    /// Returns the length of the value
    fn len(&self) -> u32;

    /// Dereferences the pointer.
    /// Takes a box because currently runtime code uses dynamic dispatch.
    /// # Errors
    /// StorageError if reading from storage fails
    fn deref(&self) -> Result<Vec<u8>>;
}

/// An external blockchain interface for the Runtime logic
pub trait External {
    /// Write `value` to the `key` of the storage trie associated with the current account.
    ///
    /// # Example
    ///
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// assert_eq!(external.storage_set(b"key42", b"value1337"), Ok(()));
    /// // Should return an old value if the key exists
    /// assert_eq!(external.storage_set(b"key42", b"new_value"), Ok(()));
    /// ```
    fn storage_set(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Read `key` from the storage trie associated with the current account.
    ///
    /// # Arguments
    ///
    /// * `key` - the key to read
    ///
    /// # Errors
    ///
    /// This function could return [`near_vm_errors::VMError::ExternalError`].
    ///
    /// # Example
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::{External, ValuePtr};
    ///
    /// # let mut external = MockedExternal::new();
    /// external.storage_set(b"key42", b"value1337").unwrap();
    /// assert_eq!(external.storage_get(b"key42").unwrap().map(|ptr| ptr.deref().unwrap()), Some(b"value1337".to_vec()));
    /// // Returns Ok(None) if there is no value for a key
    /// assert_eq!(external.storage_get(b"no_key").unwrap().map(|ptr| ptr.deref().unwrap()), None);
    /// ```
    fn storage_get<'a>(&'a self, key: &[u8]) -> Result<Option<Box<dyn ValuePtr + 'a>>>;

    /// Removes the `key` from the storage trie associated with the current account.
    ///
    /// The operation will succeed even if the `key` does not exist.
    ///
    /// # Arguments
    ///
    /// * `key` - the key to remove
    ///
    /// # Example
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// external.storage_set(b"key42", b"value1337").unwrap();
    /// // Returns Ok if exists
    /// assert_eq!(external.storage_remove(b"key42"), Ok(()));
    /// // Returns Ok if there was no value
    /// assert_eq!(external.storage_remove(b"no_value_key"), Ok(()));
    /// ```
    fn storage_remove(&mut self, key: &[u8]) -> Result<()>;

    /// Note: The method is currently unused and untested.
    ///
    /// Removes all keys with a given `prefix` from the storage trie associated with current
    /// account.
    ///
    /// # Arguments
    ///
    /// * `prefix` - a prefix for all keys to remove
    ///
    /// # Errors
    ///
    /// This function could return [`near_vm_errors::VMError`].
    ///
    /// # Example
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// external.storage_set(b"key1", b"value1337").unwrap();
    /// external.storage_set(b"key2", b"value1337").unwrap();
    /// assert_eq!(external.storage_remove_subtree(b"key"), Ok(()));
    /// assert!(!external.storage_has_key(b"key1").unwrap());
    /// assert!(!external.storage_has_key(b"key2").unwrap());
    /// ```
    fn storage_remove_subtree(&mut self, prefix: &[u8]) -> Result<()>;

    /// Check whether the `key` is present in the storage trie associated with the current account.
    ///
    /// Returns `Ok(true)` if key is present, `Ok(false)` if the key is not present.
    ///
    /// # Arguments
    ///
    /// * `key` - a key to check
    ///
    /// # Errors
    ///
    /// This function could return [`near_vm_errors::VMError`].
    ///
    /// # Example
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// external.storage_set(b"key42", b"value1337").unwrap();
    /// // Returns value if exists
    /// assert_eq!(external.storage_has_key(b"key42"), Ok(true));
    /// // Returns None if there was no value
    /// assert_eq!(external.storage_has_key(b"no_value_key"), Ok(false));
    /// ```
    fn storage_has_key(&mut self, key: &[u8]) -> Result<bool>;

    fn generate_data_id(&mut self) -> CryptoHash;

    /// Returns amount of touched trie nodes by storage operations
    fn get_trie_nodes_count(&self) -> TrieNodesCount;

    /// Returns the validator stake for given account in the current epoch.
    /// If the account is not a validator, returns `None`.
    fn validator_stake(&self, account_id: &AccountId) -> Result<Option<Balance>>;

    /// Returns total stake of validators in the current epoch.
    fn validator_total_stake(&self) -> Result<Balance>;
}

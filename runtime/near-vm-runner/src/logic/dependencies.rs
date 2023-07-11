//! External dependencies of the near-vm-logic.

use super::TrieNodesCount;
use super::VMLogicError;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{AccountId, Balance};

use std::borrow::Cow;

/// Representation of the address slice of guest memory.
#[derive(Clone, Copy)]
pub struct MemSlice {
    /// Offset within guest memory at which the slice starts.
    pub ptr: u64,
    /// Length of the slice.
    pub len: u64,
}

impl MemSlice {
    #[inline]
    pub fn len<T: TryFrom<u64>>(&self) -> Result<T, ()> {
        T::try_from(self.len).map_err(|_| ())
    }

    #[inline]
    pub fn end<T: TryFrom<u64>>(&self) -> Result<T, ()> {
        T::try_from(self.ptr.checked_add(self.len).ok_or(())?).map_err(|_| ())
    }

    #[inline]
    pub fn range<T: TryFrom<u64>>(&self) -> Result<std::ops::Range<T>, ()> {
        let end = self.end()?;
        let start = T::try_from(self.ptr).map_err(|_| ())?;
        Ok(start..end)
    }
}

/// An abstraction over the memory of the smart contract.
pub trait MemoryLike {
    /// Returns success if the memory interval is completely inside smart
    /// contract’s memory.
    ///
    /// You often don’t need to use this method since other methods will perform
    /// the check, however it may be necessary to prevent potential denial of
    /// service attacks.  See [`Self::read_memory`] for description.
    fn fits_memory(&self, slice: MemSlice) -> Result<(), ()>;

    /// Returns view of the content of the given memory interval.
    ///
    /// Not all implementations support borrowing the memory directly.  In those
    /// cases, the data is copied into a vector.
    fn view_memory(&self, slice: MemSlice) -> Result<Cow<[u8]>, ()>;

    /// Reads the content of the given memory interval.
    ///
    /// Returns error if the memory interval isn’t completely inside the smart
    /// contract memory.
    ///
    /// # Potential denial of service
    ///
    /// Note that improper use of this function may lead to denial of service
    /// attacks.  For example, consider the following function:
    ///
    /// ```
    /// # use near_vm_runner::logic::{MemoryLike, MemSlice};
    ///
    /// fn read_vec(mem: &dyn MemoryLike, slice: MemSlice) -> Result<Vec<u8>, ()> {
    ///     let mut vec = vec![0; slice.len()?];
    ///     mem.read_memory(slice.ptr, &mut vec[..])?;
    ///     Ok(vec)
    /// }
    /// ```
    ///
    /// If attacker controls length argument, it may cause attempt at allocation
    /// of arbitrarily-large buffer and crash the program.  In situations like
    /// this, it’s necessary to use [`Self::fits_memory`] method to verify that
    /// the length is valid.  For example:
    ///
    /// ```
    /// # use near_vm_runner::logic::{MemoryLike, MemSlice};
    ///
    /// fn read_vec(mem: &dyn MemoryLike, slice: MemSlice) -> Result<Vec<u8>, ()> {
    ///     mem.fits_memory(slice)?;
    ///     let mut vec = vec![0; slice.len()?];
    ///     mem.read_memory(slice.ptr, &mut vec[..])?;
    ///     Ok(vec)
    /// }
    /// ```
    fn read_memory(&self, offset: u64, buffer: &mut [u8]) -> Result<(), ()>;

    /// Writes the buffer into the smart contract memory.
    ///
    /// Returns error if the memory interval isn’t completely inside the smart
    /// contract memory.
    fn write_memory(&mut self, offset: u64, buffer: &[u8]) -> Result<(), ()>;
}

/// This enum represents if a storage_get call will be performed through flat storage or trie
pub enum StorageGetMode {
    FlatStorage,
    Trie,
}

pub type Result<T, E = VMLogicError> = ::std::result::Result<T, E>;

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
    /// # use near_vm_runner::logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_runner::logic::External;
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
    /// * `mode`- whether the lookup will be performed through flat storage or trie
    /// # Errors
    ///
    /// This function could return [`near_vm_runner::logic::VMRunnerError::ExternalError`].
    ///
    /// # Example
    /// ```
    /// # use near_vm_runner::logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_runner::logic::{External, StorageGetMode, ValuePtr};
    ///
    /// # let mut external = MockedExternal::new();
    /// external.storage_set(b"key42", b"value1337").unwrap();
    /// assert_eq!(external.storage_get(b"key42", StorageGetMode::Trie).unwrap().map(|ptr| ptr.deref().unwrap()), Some(b"value1337".to_vec()));
    /// // Returns Ok(None) if there is no value for a key
    /// assert_eq!(external.storage_get(b"no_key", StorageGetMode::Trie).unwrap().map(|ptr| ptr.deref().unwrap()), None);
    /// ```
    fn storage_get<'a>(
        &'a self,
        key: &[u8],
        mode: StorageGetMode,
    ) -> Result<Option<Box<dyn ValuePtr + 'a>>>;

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
    /// # use near_vm_runner::logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_runner::logic::External;
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
    /// This function could return [`near_vm_runner::logic::VMError`].
    ///
    /// # Example
    /// ```
    /// # use near_vm_runner::logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_runner::logic::{External, StorageGetMode};
    ///
    /// # let mut external = MockedExternal::new();
    /// external.storage_set(b"key1", b"value1337").unwrap();
    /// external.storage_set(b"key2", b"value1337").unwrap();
    /// assert_eq!(external.storage_remove_subtree(b"key"), Ok(()));
    /// assert!(!external.storage_has_key(b"key1", StorageGetMode::Trie).unwrap());
    /// assert!(!external.storage_has_key(b"key2", StorageGetMode::Trie).unwrap());
    /// ```
    fn storage_remove_subtree(&mut self, prefix: &[u8]) -> Result<()>;

    /// Check whether the `key` is present in the storage trie associated with the current account.
    ///
    /// Returns `Ok(true)` if key is present, `Ok(false)` if the key is not present.
    ///
    /// # Arguments
    ///
    /// * `key` - a key to check
    /// * `mode`- whether the lookup will be performed through flat storage or trie
    ///
    /// # Errors
    ///
    /// This function could return [`near_vm_runner::logic::VMError`].
    ///
    /// # Example
    /// ```
    /// # use near_vm_runner::logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_runner::logic::{External, StorageGetMode};
    ///
    /// # let mut external = MockedExternal::new();
    /// external.storage_set(b"key42", b"value1337").unwrap();
    /// // Returns value if exists
    /// assert_eq!(external.storage_has_key(b"key42", StorageGetMode::Trie), Ok(true));
    /// // Returns None if there was no value
    /// assert_eq!(external.storage_has_key(b"no_value_key", StorageGetMode::Trie), Ok(false));
    /// ```
    fn storage_has_key(&mut self, key: &[u8], mode: StorageGetMode) -> Result<bool>;

    fn generate_data_id(&mut self) -> CryptoHash;

    /// Returns amount of touched trie nodes by storage operations
    fn get_trie_nodes_count(&self) -> TrieNodesCount;

    /// Returns the validator stake for given account in the current epoch.
    /// If the account is not a validator, returns `None`.
    fn validator_stake(&self, account_id: &AccountId) -> Result<Option<Balance>>;

    /// Returns total stake of validators in the current epoch.
    fn validator_total_stake(&self) -> Result<Balance>;
}

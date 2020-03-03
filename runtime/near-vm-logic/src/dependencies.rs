//! External dependencies of the near-vm-logic.

use crate::types::{AccountId, Balance, Gas, IteratorIndex, PublicKey, ReceiptIndex};
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
    /// Write to the storage trie of the current account
    ///
    /// # Arguments
    ///
    /// * `key` - a key for a new value
    /// * `value` - a new value to be set
    ///
    /// # Errors
    ///
    /// This function could return HostErrorOrStorageError::StorageError on underlying DB failure
    ///
    /// # Example
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

    /// Reads from the storage trie of the current account
    ///
    /// # Arguments
    ///
    /// * `key` - a key to read
    ///
    /// # Errors
    ///
    /// This function could return HostErrorOrStorageError::StorageError on underlying DB failure
    ///
    /// # Example
    /// ```
    /// # use near_vm_logic::mocks::mock_external::{MockedExternal};
    /// # use near_vm_logic::{External, ValuePtr};
    ///
    /// # let mut external = MockedExternal::new();
    /// external.storage_set(b"key42", b"value1337").unwrap();
    /// assert_eq!(external.storage_get(b"key42").unwrap().map(|ptr| ptr.deref().unwrap()), Some(b"value1337".to_vec()));
    /// // Returns Ok(None) if there is no value for a key
    /// assert_eq!(external.storage_get(b"no_key").unwrap().map(|ptr| ptr.deref().unwrap()), None);
    /// ```
    fn storage_get<'a>(&'a self, key: &[u8]) -> Result<Option<Box<dyn ValuePtr + 'a>>>;

    /// Removes the key from the storage
    ///
    /// # Arguments
    ///
    /// * `key` - a key to remove
    ///
    /// # Errors
    ///
    /// This function could return HostErrorOrStorageError::StorageError on underlying DB failure
    ///
    /// # Example
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// external.storage_set(b"key42", b"value1337").unwrap();
    /// // Returns value if exists
    /// assert_eq!(external.storage_remove(b"key42"), Ok(()));
    /// // Returns None if there was no value
    /// assert_eq!(external.storage_remove(b"no_value_key"), Ok(()));
    /// ```
    fn storage_remove(&mut self, key: &[u8]) -> Result<()>;

    /// Check whether key exists. Returns Ok(true) if key exists or Ok(false) otherwise
    ///
    /// # Arguments
    ///
    /// * `key` - a key to check
    ///
    /// # Errors
    ///
    /// This function could return HostErrorOrStorageError::StorageError on underlying DB failure
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

    /// Creates iterator in memory for a key prefix and returns its ID to use with `storage_iter_next`
    ///
    /// # Arguments
    ///
    /// * `prefix` - a prefix in the storage to iterate on
    ///
    /// # Errors
    ///
    /// This function could return HostErrorOrStorageError::StorageError on underlying DB failure
    ///
    /// # Example
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    ///
    /// external.storage_set(b"key42", b"value1337").unwrap();
    /// // Creates iterator and returns index
    /// let index = external.storage_iter(b"key42").unwrap();
    ///
    /// assert_eq!(external.storage_iter_next(index).unwrap().map(|(key, ptr)| (key, ptr.deref().unwrap())), Some((b"key42".to_vec(), b"value1337".to_vec())));
    /// assert_eq!(external.storage_iter_next(index).unwrap().map(|(key, ptr)| (key, ptr.deref().unwrap())), None);
    ///
    /// external.storage_iter(b"not_existing_key").expect("should be ok");
    /// ```
    fn storage_iter(&mut self, prefix: &[u8]) -> Result<IteratorIndex>;

    /// Creates iterator in memory for a key range and returns its ID to use with `storage_iter_next`
    ///
    /// # Arguments
    ///
    /// * `start` - a start prefix in the storage to iterate on
    /// * `end` - an end prefix in the storage to iterate on (exclusive)
    ///
    /// # Errors
    ///
    /// This function could return:
    /// - HostErrorOrStorageError::StorageError on underlying DB failure
    ///
    ///
    /// # Example
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    ///
    /// external.storage_set(b"key42", b"value1337").unwrap();
    /// external.storage_set(b"key43", b"val").unwrap();
    /// // Creates iterator and returns index
    /// let index = external.storage_iter_range(b"key42", b"key43").unwrap();
    ///
    /// assert_eq!(external.storage_iter_next(index).unwrap().map(|(key, ptr)| (key, ptr.deref().unwrap())), Some((b"key42".to_vec(), b"value1337".to_vec())));
    /// // The second key is `key43`. Returns Ok(None), since the `end` parameter is exclusive
    /// assert_eq!(external.storage_iter_next(index).unwrap().map(|(key, ptr)| (key, ptr.deref().unwrap())), None);
    /// ```
    fn storage_iter_range(&mut self, start: &[u8], end: &[u8]) -> Result<IteratorIndex>;

    /// Returns the current iterator value and advances the iterator
    /// If there is no more values, returns Ok(None)
    ///
    /// See usage examples in `storage_iter` and `storage_iter_range`
    ///
    /// # Arguments
    ///
    /// * `iterator_idx` - an iterator ID, created by `storage_iter` or `storage_iter_range`
    ///
    /// # Errors
    ///
    /// This function could return:
    /// - HostErrorOrStorageError::StorageError on underlying DB failure
    /// ```
    fn storage_iter_next<'a>(
        &'a mut self,
        iterator_idx: IteratorIndex,
    ) -> Result<Option<(Vec<u8>, Box<dyn ValuePtr + 'a>)>>;

    /// Removes iterator index added with `storage_iter` and `storage_iter_range`
    fn storage_iter_drop(&mut self, iterator_idx: IteratorIndex) -> Result<()>;

    /// Creates a receipt which will be executed after `receipt_indices`
    ///
    /// # Arguments
    ///
    /// * `receipt_indices` - a list of receipt indices the new receipt is depend on
    ///
    /// # Example
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// let receipt_index_one = external.create_receipt(vec![], "charli.near".to_owned()).unwrap();
    /// let receipt_index_two = external.create_receipt(vec![receipt_index_one], "bob.near".to_owned());
    ///
    /// ```
    ///
    /// # Panics
    /// Panics if one of `receipt_indices` is missing
    fn create_receipt(
        &mut self,
        receipt_indices: Vec<ReceiptIndex>,
        receiver_id: AccountId,
    ) -> Result<ReceiptIndex>;

    /// Attaches an `Action::CreateAccount` action to an existing receipt
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    ///
    /// # Example
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// let receipt_index = external.create_receipt(vec![], "charli.near".to_owned()).unwrap();
    /// external.append_action_create_account(receipt_index).unwrap();
    ///
    /// ```
    ///
    /// # Panics
    /// Panics if `receipt_index` is missing
    fn append_action_create_account(&mut self, receipt_index: ReceiptIndex) -> Result<()>;

    /// Attaches an `Action::DeployContract` action to an existing receipt
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `code` - a Wasm code to attach
    ///
    /// # Example
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// let receipt_index = external.create_receipt(vec![], "charli.near".to_owned()).unwrap();
    /// external.append_action_deploy_contract(receipt_index, b"some valid Wasm code".to_vec()).unwrap();
    ///
    /// ```
    ///
    /// # Panics
    /// Panics if `receipt_index` is missing
    fn append_action_deploy_contract(
        &mut self,
        receipt_index: ReceiptIndex,
        code: Vec<u8>,
    ) -> Result<()>;

    /// Attaches an `Action::FunctionCall` action to an existing receipt
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `method_name` - a name of the contract method to call
    /// * `arguments` - a Wasm code to attach
    /// * `attached_deposit` - amount of tokens to transfer with the call
    /// * `prepaid_gas` - amount of prepaid gas to attach to the call
    ///
    /// # Example
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// let receipt_index = external.create_receipt(vec![], "charli.near".to_owned()).unwrap();
    /// external.append_action_function_call(
    ///     receipt_index,
    ///     b"method_name".to_vec(),
    ///     b"{serialised: arguments}".to_vec(),
    ///     100000u128,
    ///     100u64
    /// ).unwrap();
    ///
    /// ```
    ///
    /// # Panics
    /// Panics if `receipt_index` is missing
    fn append_action_function_call(
        &mut self,
        receipt_index: ReceiptIndex,
        method_name: Vec<u8>,
        arguments: Vec<u8>,
        attached_deposit: Balance,
        prepaid_gas: Gas,
    ) -> Result<()>;

    /// Attaches an `TransferAction` action to an existing receipt
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `amount` - amount of tokens to transfer
    ///
    /// # Example
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// let receipt_index = external.create_receipt(vec![], "charli.near".to_owned()).unwrap();
    /// external.append_action_transfer(
    ///     receipt_index,
    ///     100000u128,
    /// ).unwrap();
    ///
    /// ```
    ///
    /// # Panics
    /// Panics if `receipt_index` is missing
    fn append_action_transfer(
        &mut self,
        receipt_index: ReceiptIndex,
        amount: Balance,
    ) -> Result<()>;

    /// Attaches an `StakeAction` action to an existing receipt
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `stake` - amount of tokens to stake
    /// * `public_key` - a validator public key
    ///
    /// # Example
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// let receipt_index = external.create_receipt(vec![], "charli.near".to_owned()).unwrap();
    /// external.append_action_stake(
    ///     receipt_index,
    ///     100000u128,
    ///     b"some public key".to_vec()
    /// ).unwrap();
    ///
    /// ```
    ///
    /// # Panics
    /// Panics if `receipt_index` is missing
    fn append_action_stake(
        &mut self,
        receipt_index: ReceiptIndex,
        stake: Balance,
        public_key: PublicKey,
    ) -> Result<()>;

    /// Attaches an `AddKeyAction` action to an existing receipt
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `public_key` - a public key for an access key
    /// * `nonce` - a nonce
    ///
    /// # Example
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// let receipt_index = external.create_receipt(vec![], "charli.near".to_owned()).unwrap();
    /// external.append_action_add_key_with_full_access(
    ///     receipt_index,
    ///     b"some public key".to_vec(),
    ///     0u64
    /// ).unwrap();
    ///
    /// ```
    ///
    /// # Panics
    /// Panics if `receipt_index` is missing
    fn append_action_add_key_with_full_access(
        &mut self,
        receipt_index: ReceiptIndex,
        public_key: PublicKey,
        nonce: u64,
    ) -> Result<()>;

    /// Attaches an `AddKeyAction` action to an existing receipt with `AccessKeyPermission::FunctionCall`
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `public_key` - a public key for an access key
    /// * `nonce` - a nonce
    /// * `allowance` - amount of tokens allowed to spend by this access key
    /// * `receiver_id` - a contract witch will be allowed to call with this access key
    /// * `method_names` - a list of method names is allowed to call with this access key (empty = any method)
    ///
    /// # Example
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// let receipt_index = external.create_receipt(vec![], "charli.near".to_owned()).unwrap();
    /// external.append_action_add_key_with_function_call(
    ///     receipt_index,
    ///     b"some public key".to_vec(),
    ///     0u64,
    ///     None,
    ///     "bob.near".to_owned(),
    ///     vec![b"foo".to_vec(), b"bar".to_vec()]
    /// ).unwrap();
    ///
    /// ```
    ///
    /// # Panics
    /// Panics if `receipt_index` is missing
    fn append_action_add_key_with_function_call(
        &mut self,
        receipt_index: ReceiptIndex,
        public_key: PublicKey,
        nonce: u64,
        allowance: Option<Balance>,
        receiver_id: AccountId,
        method_names: Vec<Vec<u8>>,
    ) -> Result<()>;

    /// Attaches an `DeleteKeyAction` action to an existing receipt
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `public_key` - a public key for an access key to delete
    ///
    /// # Example
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// let receipt_index = external.create_receipt(vec![], "charli.near".to_owned()).unwrap();
    /// external.append_action_delete_key(
    ///     receipt_index,
    ///     b"some public key".to_vec()
    /// ).unwrap();
    ///
    /// ```
    ///
    /// # Panics
    /// Panics if `receipt_index` is missing
    fn append_action_delete_key(
        &mut self,
        receipt_index: ReceiptIndex,
        public_key: PublicKey,
    ) -> Result<()>;

    /// Attaches an `DeleteAccountAction` action to an existing receipt
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `beneficiary_id` - an account id to which the rest of the funds of the removed account will be transferred
    ///
    /// # Example
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// let receipt_index = external.create_receipt(vec![], "charli.near".to_owned()).unwrap();
    /// external.append_action_delete_account(
    ///     receipt_index,
    ///     "sam".to_owned()
    /// ).unwrap();
    ///
    /// ```
    ///
    /// # Panics
    /// Panics if `receipt_index` is missing
    fn append_action_delete_account(
        &mut self,
        receipt_index: ReceiptIndex,
        beneficiary_id: AccountId,
    ) -> Result<()>;

    /// Computes sha256 hash
    ///
    /// # Arguments
    ///
    /// * `data` - data to hash
    ///
    /// # Example
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// let result = external.sha256(b"tesdsst").unwrap();
    /// assert_eq!(&result, &[
    ///        18, 176, 115, 156, 45, 100, 241, 132, 180, 134, 77, 42, 105, 111, 199, 127, 118, 112,
    ///        92, 255, 88, 43, 83, 147, 122, 55, 26, 36, 42, 156, 160, 158,
    /// ]);
    ///
    /// ```
    fn sha256(&self, data: &[u8]) -> Result<Vec<u8>>;

    /// Computes keccak256 hash
    ///
    /// # Arguments
    ///
    /// * `data` - data to hash
    ///
    /// # Example
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// let result = external.keccak256(b"tesdsst").unwrap();
    /// assert_eq!(&result, &[
    ///         174, 42, 184, 134, 113, 104, 230, 180, 244, 77, 240, 72, 199, 42, 110, 178, 6, 168,
    ///         121, 77, 27, 183, 153, 108, 197, 171, 78, 61, 186, 133, 193, 182
    /// ]);
    ///
    /// ```
    fn keccak256(&self, data: &[u8]) -> Result<Vec<u8>>;

    /// Computes keccak512 hash
    ///
    /// # Arguments
    ///
    /// * `data` - data to hash
    ///
    /// # Example
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// let result = external.keccak512(b"tesdsst").unwrap();
    /// assert_eq!(&result, &[
    ///         133, 196, 48, 30, 203, 238, 194, 158, 186, 246, 118, 238, 42, 158, 212, 27, 178, 72,
    ///         90, 229, 98, 108, 195, 221, 222, 161, 96, 219, 252, 99, 2, 48, 224, 15, 95, 220, 35,
    ///         209, 27, 250, 43, 168, 250, 10, 21, 25, 97, 135, 235, 61, 5, 142, 182, 85, 36, 179,
    ///         23, 126, 161, 14, 21, 118, 180, 231
    /// ].to_vec());
    ///
    /// ```
    fn keccak512(&self, data: &[u8]) -> Result<Vec<u8>>;

    /// Returns amount of touched trie nodes by storage operations
    fn get_touched_nodes_count(&self) -> u64;

    /// Resets amount of touched trie nodes by storage operations
    fn reset_touched_nodes_counter(&mut self);
}

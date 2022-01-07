//! External dependencies of the near-vm-logic.

use crate::types::{PublicKey, ReceiptIndex};
use near_primitives_core::types::{AccountId, Balance, Gas};
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
    /// This function could return [`VMError::ExternalError`].
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
    /// This function could return [`VMError`].
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
    /// This function could return [`VMError::RuntimeError`].
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

    /// Create a receipt which will be executed after all the receipts identified by
    /// `receipt_indices` are complete.
    ///
    /// If any of the [`RecepitIndex`]es do not refer to a known receipt, this function will fail
    /// with an error.
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
    /// let receipt_index_one = external.create_receipt(vec![], "charli.near".parse().unwrap()).unwrap();
    /// let receipt_index_two = external.create_receipt(vec![receipt_index_one], "bob.near".parse().unwrap());
    ///
    /// ```
    fn create_receipt(
        &mut self,
        receipt_indices: Vec<ReceiptIndex>,
        receiver_id: AccountId,
    ) -> Result<ReceiptIndex>;

    /// Attach the [`CreateAccountAction`] action to an existing receipt.
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
    /// let receipt_index = external.create_receipt(vec![], "charli.near".parse().unwrap()).unwrap();
    /// external.append_action_create_account(receipt_index).unwrap();
    ///
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    fn append_action_create_account(&mut self, receipt_index: ReceiptIndex) -> Result<()>;

    /// Attach the [`DeployContractAction`] action to an existing receipt.
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `code` - a Wasm code to attach
    ///
    /// # Example
    ///
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// let receipt_index = external.create_receipt(vec![], "charli.near".parse().unwrap()).unwrap();
    /// external.append_action_deploy_contract(receipt_index, b"some valid Wasm code".to_vec()).unwrap();
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    fn append_action_deploy_contract(
        &mut self,
        receipt_index: ReceiptIndex,
        code: Vec<u8>,
    ) -> Result<()>;

    /// Attach the [`FunctionCallAction`] action to an existing receipt.
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
    ///
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// let receipt_index = external.create_receipt(vec![], "charli.near".parse().unwrap()).unwrap();
    /// external.append_action_function_call(
    ///     receipt_index,
    ///     b"method_name".to_vec(),
    ///     b"{serialised: arguments}".to_vec(),
    ///     100000u128,
    ///     100u64
    /// ).unwrap();
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    fn append_action_function_call(
        &mut self,
        receipt_index: ReceiptIndex,
        method_name: Vec<u8>,
        arguments: Vec<u8>,
        attached_deposit: Balance,
        prepaid_gas: Gas,
    ) -> Result<()>;

    /// Attach the [`TransferAction`] action to an existing receipt.
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `amount` - amount of tokens to transfer
    ///
    /// # Example
    ///
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// let receipt_index = external.create_receipt(vec![], "charli.near".parse().unwrap()).unwrap();
    /// external.append_action_transfer(
    ///     receipt_index,
    ///     100000u128,
    /// ).unwrap();
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    fn append_action_transfer(
        &mut self,
        receipt_index: ReceiptIndex,
        amount: Balance,
    ) -> Result<()>;

    /// Attach the [`StakeAction`] action to an existing receipt.
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `stake` - amount of tokens to stake
    /// * `public_key` - a validator public key
    ///
    /// # Example
    ///
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// let receipt_index = external.create_receipt(vec![], "charli.near".parse().unwrap()).unwrap();
    /// external.append_action_stake(
    ///     receipt_index,
    ///     100000u128,
    ///     b"some public key".to_vec()
    /// ).unwrap();
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    fn append_action_stake(
        &mut self,
        receipt_index: ReceiptIndex,
        stake: Balance,
        public_key: PublicKey,
    ) -> Result<()>;

    /// Attach the [`AddKeyAction`] action to an existing receipt.
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `public_key` - a public key for an access key
    /// * `nonce` - a nonce
    ///
    /// # Example
    ///
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// let receipt_index = external.create_receipt(vec![], "charli.near".parse().unwrap()).unwrap();
    /// external.append_action_add_key_with_full_access(
    ///     receipt_index,
    ///     b"some public key".to_vec(),
    ///     0u64
    /// ).unwrap();
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    fn append_action_add_key_with_full_access(
        &mut self,
        receipt_index: ReceiptIndex,
        public_key: PublicKey,
        nonce: u64,
    ) -> Result<()>;

    /// Attach the [`AddKeyAction`] action an existing receipt.
    ///
    /// The access key associated with the action will have the
    /// [`AccessKeyPermission::FunctionCall`] permission scope.
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
    ///
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// let receipt_index = external.create_receipt(vec![], "charli.near".parse().unwrap()).unwrap();
    /// external.append_action_add_key_with_function_call(
    ///     receipt_index,
    ///     b"some public key".to_vec(),
    ///     0u64,
    ///     None,
    ///     "bob.near".parse().unwrap(),
    ///     vec![b"foo".to_vec(), b"bar".to_vec()]
    /// ).unwrap();
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    fn append_action_add_key_with_function_call(
        &mut self,
        receipt_index: ReceiptIndex,
        public_key: PublicKey,
        nonce: u64,
        allowance: Option<Balance>,
        receiver_id: AccountId,
        method_names: Vec<Vec<u8>>,
    ) -> Result<()>;

    /// Attach the [`DeleteKeyAction`] action to an existing receipt.
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `public_key` - a public key for an access key to delete
    ///
    /// # Example
    ///
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// let receipt_index = external.create_receipt(vec![], "charli.near".parse().unwrap()).unwrap();
    /// external.append_action_delete_key(
    ///     receipt_index,
    ///     b"some public key".to_vec()
    /// ).unwrap();
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    fn append_action_delete_key(
        &mut self,
        receipt_index: ReceiptIndex,
        public_key: PublicKey,
    ) -> Result<()>;

    /// Attach the [`DeleteAccountAction`] action to an existing receipt
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `beneficiary_id` - an account id to which the rest of the funds of the removed account will be transferred
    ///
    /// # Example
    ///
    /// ```
    /// # use near_vm_logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_logic::External;
    ///
    /// # let mut external = MockedExternal::new();
    /// let receipt_index = external.create_receipt(vec![], "charli.near".parse().unwrap()).unwrap();
    /// external.append_action_delete_account(
    ///     receipt_index,
    ///     "sam".parse().unwrap()
    /// ).unwrap();
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    fn append_action_delete_account(
        &mut self,
        receipt_index: ReceiptIndex,
        beneficiary_id: AccountId,
    ) -> Result<()>;

    /// Returns amount of touched trie nodes by storage operations
    fn get_touched_nodes_count(&self) -> u64;

    /// Returns the validator stake for given account in the current epoch.
    /// If the account is not a validator, returns `None`.
    fn validator_stake(&self, account_id: &AccountId) -> Result<Option<Balance>>;

    /// Returns total stake of validators in the current epoch.
    fn validator_total_stake(&self) -> Result<Balance>;
}

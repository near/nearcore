//! External dependencies of the near-vm-logic.
use super::VMLogicError;
use super::types::ReceiptIndex;
use near_crypto::PublicKey;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{AccountId, Balance, Gas, GasWeight, Nonce};
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
    fn deref(&self, storage_tracker: &mut dyn StorageAccessTracker) -> Result<Vec<u8>>;
}

pub(super) mod sealed {
    pub trait StorageAccessTrackerSeal {}
}

/// This trait encapsulates the logic of charging gas fees for storage access operations.
///
/// The trait is sealed and is only implementable by types inside the the contract runtime
/// implementation.
pub trait StorageAccessTracker: sealed::StorageAccessTrackerSeal {
    /// Indicate to the contract runtime that a trie node in database has been touched.
    ///
    /// Calling this method will charge the `touching_trie_node` fee.
    fn trie_node_touched(&mut self, count: u64) -> Result<()>;

    /// Indicate to the contract runtime that a cached trie node has been accessed.
    ///
    /// Calling this method will charge the `read_cached_trie_node` fee.
    fn cached_trie_node_access(&mut self, count: u64) -> Result<()>;

    /// Indicate to the contract runtime that the specified number of storage bytes are being
    /// dereferenced as a result of a previous value being evicted during write.
    ///
    /// Calling this method will charge the `cost_per_byte` fee.
    fn deref_write_evicted_value_bytes(&mut self, bytes: u64) -> Result<()>;

    /// Indicate to the contract runtime that the specified number of storage bytes are being
    /// dereferenced as a result of storage key removal.
    ///
    /// Calling this method will charge the `cost_per_byte` fee.
    fn deref_removed_value_bytes(&mut self, bytes: u64) -> Result<()>;
}

/// An external blockchain interface for the Runtime logic
pub trait External {
    /// Write `value` to the `key` of the storage trie associated with the current account.
    ///
    /// Returns the previous value for the key, if present.
    ///
    /// # Example
    ///
    /// ```
    /// # use near_vm_runner::logic::mocks::mock_external::MockedExternal;
    /// # use near_vm_runner::logic::External;
    /// # use near_vm_runner::logic::gas_counter::FreeGasCounter;
    ///
    /// # let mut external = MockedExternal::new();
    /// assert_eq!(external.storage_set(&mut FreeGasCounter, b"key42", b"value1337"), Ok(None));
    /// // Should return an old value if the key exists
    /// assert_eq!(external.storage_set(&mut FreeGasCounter, b"key42", b"new_value"), Ok(Some(b"value1337".to_vec())));
    /// ```
    fn storage_set<'a>(
        &'a mut self,
        access_tracker: &mut dyn StorageAccessTracker,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>>;

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
    /// # use near_vm_runner::logic::{External, ValuePtr};
    /// # use near_vm_runner::logic::gas_counter::FreeGasCounter;
    ///
    /// # let mut external = MockedExternal::new();
    /// let mut gas = FreeGasCounter;
    /// external.storage_set(&mut gas, b"key42", b"value1337").unwrap();
    /// assert_eq!(external.storage_get(&mut gas, b"key42").unwrap().map(|ptr| ptr.deref(&mut gas).unwrap()), Some(b"value1337".to_vec()));
    /// // Returns Ok(None) if there is no value for a key
    /// assert_eq!(external.storage_get(&mut gas, b"no_key").unwrap().map(|ptr| ptr.deref(&mut gas).unwrap()), None);
    /// ```
    fn storage_get<'a>(
        &'a self,
        access_tracker: &mut dyn StorageAccessTracker,
        key: &[u8],
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
    /// # use near_vm_runner::logic::gas_counter::FreeGasCounter;
    ///
    /// # let mut external = MockedExternal::new();
    /// external.storage_set(&mut FreeGasCounter, b"key42", b"value1337").unwrap();
    /// // Returns Ok if exists
    /// assert_eq!(external.storage_remove(&mut FreeGasCounter, b"key42"), Ok(Some(b"value1337".to_vec())));
    /// // Returns Ok if there was no value
    /// assert_eq!(external.storage_remove(&mut FreeGasCounter, b"no_value_key"), Ok(None));
    /// ```
    fn storage_remove(
        &mut self,
        access_tracker: &mut dyn StorageAccessTracker,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>>;

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
    /// # use near_vm_runner::logic::gas_counter::FreeGasCounter;
    /// # use near_vm_runner::logic::{External};
    ///
    /// # let mut external = MockedExternal::new();
    /// external.storage_set(&mut FreeGasCounter, b"key42", b"value1337").unwrap();
    /// // Returns value if exists
    /// assert_eq!(external.storage_has_key(&mut FreeGasCounter, b"key42"), Ok(true));
    /// // Returns None if there was no value
    /// assert_eq!(external.storage_has_key(&mut FreeGasCounter, b"no_value_key"), Ok(false));
    /// ```
    fn storage_has_key(
        &mut self,
        access_tracker: &mut dyn StorageAccessTracker,
        key: &[u8],
    ) -> Result<bool>;

    fn generate_data_id(&mut self) -> CryptoHash;

    /// Size of the recorded trie storage proof.
    fn get_recorded_storage_size(&self) -> usize;

    /// Returns the validator stake for given account in the current epoch.
    /// If the account is not a validator, returns `None`.
    fn validator_stake(&self, account_id: &AccountId) -> Result<Option<Balance>>;

    /// Returns total stake of validators in the current epoch.
    fn validator_total_stake(&self) -> Result<Balance>;

    /// Create an action receipt which will be executed after all the receipts identified by
    /// `receipt_indices` are complete.
    ///
    /// If any of the [`ReceiptIndex`]es do not refer to a known receipt, this function will fail
    /// with an error.
    ///
    /// # Arguments
    ///
    /// * `receipt_indices` - a list of receipt indices the new receipt is depend on
    /// * `receiver_id` - account id of the receiver of the receipt created
    fn create_action_receipt(
        &mut self,
        receipt_indices: Vec<ReceiptIndex>,
        receiver_id: AccountId,
    ) -> Result<ReceiptIndex, VMLogicError>;

    /// Create a PromiseYield action receipt.
    ///
    /// Returns the ReceiptIndex of the newly created receipt and the id of its data dependency.
    ///
    /// # Arguments
    ///
    /// * `receiver_id` - account id of the receiver of the receipt created
    fn create_promise_yield_receipt(
        &mut self,
        receiver_id: AccountId,
    ) -> Result<(ReceiptIndex, CryptoHash), VMLogicError>;

    /// Creates a receipt under the specified `data_id` containing given `data`.
    ///
    /// This function shall return `Ok(true)` if the data dependency of the yield receipt has been
    /// successfully resolved.
    ///
    /// If given `data_id` was not produced by a call to `yield_create_action_receipt`, or the data
    /// dependency could not be resolved for any other reason (e.g. because it timed out,)
    /// `Ok(false)` is returned.
    ///
    /// # Arguments
    ///
    /// * `data_id` - `data_id` with which the DataReceipt should be created
    /// * `data` - contents of the DataReceipt
    fn submit_promise_resume_data(
        &mut self,
        data_id: CryptoHash,
        data: Vec<u8>,
    ) -> Result<bool, VMLogicError>;

    /// Attach the [`CreateAccountAction`] action to an existing receipt.
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    fn append_action_create_account(
        &mut self,
        receipt_index: ReceiptIndex,
    ) -> Result<(), VMLogicError>;

    /// Attach the [`DeployContractAction`] action to an existing receipt.
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `code` - a Wasm code to attach
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    fn append_action_deploy_contract(
        &mut self,
        receipt_index: ReceiptIndex,
        code: Vec<u8>,
    ) -> Result<(), VMLogicError>;

    /// Attach the [`FunctionCallAction`] action to an existing receipt.
    ///
    /// `prepaid_gas` and `gas_weight` can either be specified or both. If a `gas_weight` is
    /// specified, the action should be allocated gas in
    /// [`distribute_unused_gas`](Self::distribute_unused_gas).
    ///
    /// For more information, see [super::VMLogic::promise_batch_action_function_call_weight].
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `method_name` - a name of the contract method to call
    /// * `arguments` - a Wasm code to attach
    /// * `attached_deposit` - amount of tokens to transfer with the call
    /// * `prepaid_gas` - amount of prepaid gas to attach to the call
    /// * `gas_weight` - relative weight of unused gas to distribute to the function call action
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    fn append_action_function_call_weight(
        &mut self,
        receipt_index: ReceiptIndex,
        method_name: Vec<u8>,
        args: Vec<u8>,
        attached_deposit: Balance,
        prepaid_gas: Gas,
        gas_weight: GasWeight,
    ) -> Result<(), VMLogicError>;

    /// Attach the [`TransferAction`] action to an existing receipt.
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `amount` - amount of tokens to transfer
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    fn append_action_transfer(
        &mut self,
        receipt_index: ReceiptIndex,
        deposit: Balance,
    ) -> Result<(), VMLogicError>;

    /// Attach the [`StakeAction`] action to an existing receipt.
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `stake` - amount of tokens to stake
    /// * `public_key` - a validator public key
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    fn append_action_stake(
        &mut self,
        receipt_index: ReceiptIndex,
        stake: Balance,
        public_key: PublicKey,
    );

    /// Attach the [`AddKeyAction`] action to an existing receipt.
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `public_key` - a public key for an access key
    /// * `nonce` - a nonce
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    fn append_action_add_key_with_full_access(
        &mut self,
        receipt_index: ReceiptIndex,
        public_key: PublicKey,
        nonce: Nonce,
    );

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
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    fn append_action_add_key_with_function_call(
        &mut self,
        receipt_index: ReceiptIndex,
        public_key: PublicKey,
        nonce: Nonce,
        allowance: Option<Balance>,
        receiver_id: AccountId,
        method_names: Vec<Vec<u8>>,
    ) -> Result<(), VMLogicError>;

    /// Attach the [`DeleteKeyAction`] action to an existing receipt.
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `public_key` - a public key for an access key to delete
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    fn append_action_delete_key(&mut self, receipt_index: ReceiptIndex, public_key: PublicKey);

    /// Attach the [`DeleteAccountAction`] action to an existing receipt
    ///
    /// # Arguments
    ///
    /// * `receipt_index` - an index of Receipt to append an action
    /// * `beneficiary_id` - an account id to which the rest of the funds of the removed account will be transferred
    ///
    /// # Panics
    ///
    /// Panics if the `receipt_index` does not refer to a known receipt.
    fn append_action_delete_account(
        &mut self,
        receipt_index: ReceiptIndex,
        beneficiary_id: AccountId,
    ) -> Result<(), VMLogicError>;

    /// # Panic
    ///
    /// Panics if `ReceiptIndex` is invalid.
    fn get_receipt_receiver(&self, receipt_index: ReceiptIndex) -> &AccountId;
}

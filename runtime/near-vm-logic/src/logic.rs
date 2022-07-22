use crate::context::VMContext;
use crate::dependencies::{External, MemoryLike};
use crate::gas_counter::{FastGasCounter, GasCounter};
use crate::receipt_manager::ReceiptManager;
use crate::types::{PromiseIndex, PromiseResult, ReceiptIndex, ReturnData};
use crate::utils::split_method_names;
use crate::{ReceiptMetadata, ValuePtr};
use byteorder::ByteOrder;
use near_crypto::Secp256K1Signature;
use near_primitives::checked_feature;
use near_primitives::config::ViewConfig;
use near_primitives::version::is_implicit_account_creation_enabled;
use near_primitives_core::config::ExtCosts::*;
use near_primitives_core::config::{ActionCosts, ExtCosts, VMConfig};
use near_primitives_core::profile::ProfileData;
use near_primitives_core::runtime::fees::{
    transfer_exec_fee, transfer_send_fee, RuntimeFeesConfig,
};
use near_primitives_core::types::{
    AccountId, Balance, EpochHeight, Gas, ProtocolVersion, StorageUsage,
};
use near_primitives_core::types::{GasDistribution, GasWeight};
use near_vm_errors::{HostError, VMLogicError};
use near_vm_errors::{InconsistentStateError, VMError};
use std::collections::HashMap;
use std::mem::size_of;

pub type Result<T> = ::std::result::Result<T, VMLogicError>;

pub struct VMLogic<'a> {
    /// Provides access to the components outside the Wasm runtime for operations on the trie and
    /// receipts creation.
    ext: &'a mut dyn External,
    /// Part of Context API and Economics API that was extracted from the receipt.
    context: VMContext,
    /// Parameters of Wasm and economic parameters.
    config: &'a VMConfig,
    /// Fees for creating (async) actions on runtime.
    fees_config: &'a RuntimeFeesConfig,
    /// If this method execution is invoked directly as a callback by one or more contract calls the
    /// results of the methods that made the callback are stored in this collection.
    promise_results: &'a [PromiseResult],
    /// Pointer to the guest memory.
    memory: &'a mut dyn MemoryLike,

    /// Keeping track of the current account balance, which can decrease when we create promises
    /// and attach balance to them.
    current_account_balance: Balance,
    /// Current amount of locked tokens, does not automatically change when staking transaction is
    /// issued.
    current_account_locked_balance: Balance,
    /// Storage usage of the current account at the moment
    current_storage_usage: StorageUsage,
    gas_counter: GasCounter,
    /// What method returns.
    return_data: ReturnData,
    /// Logs written by the runtime.
    logs: Vec<String>,
    /// Registers can be used by the guest to store blobs of data without moving them across
    /// host-guest boundary.
    registers: HashMap<u64, Vec<u8>>,

    /// The DAG of promises, indexed by promise id.
    promises: Vec<Promise>,
    /// Tracks the total log length. The sum of length of all logs.
    total_log_length: u64,

    /// Current protocol version that is used for the function call.
    current_protocol_version: ProtocolVersion,

    /// Handles the receipts generated through execution.
    receipt_manager: ReceiptManager,
}

/// Promises API allows to create a DAG-structure that defines dependencies between smart contract
/// calls. A single promise can be created with zero or several dependencies on other promises.
/// * If a promise was created from a receipt (using `promise_create` or `promise_then`) it's a
///   `Receipt`;
/// * If a promise was created by merging several promises (using `promise_and`) then
///   it's a `NotReceipt`, but has receipts of all promises it depends on.
#[derive(Debug)]
enum Promise {
    Receipt(ReceiptIndex),
    NotReceipt(Vec<ReceiptIndex>),
}

macro_rules! memory_get {
    ($_type:ty, $name:ident) => {
        fn $name(&mut self, offset: u64) -> Result<$_type> {
            let mut array = [0u8; size_of::<$_type>()];
            self.memory_get_into(offset, &mut array)?;
            Ok(<$_type>::from_le_bytes(array))
        }
    };
}

macro_rules! memory_set {
    ($_type:ty, $name:ident) => {
        fn $name(&mut self, offset: u64, value: $_type) -> Result<()> {
            self.memory_set_slice(offset, &value.to_le_bytes())
        }
    };
}

impl<'a> VMLogic<'a> {
    pub fn new_with_protocol_version(
        ext: &'a mut dyn External,
        context: VMContext,
        config: &'a VMConfig,
        fees_config: &'a RuntimeFeesConfig,
        promise_results: &'a [PromiseResult],
        memory: &'a mut dyn MemoryLike,
        current_protocol_version: ProtocolVersion,
    ) -> Self {
        // Overflow should be checked before calling VMLogic.
        let current_account_balance = context.account_balance + context.attached_deposit;
        let current_storage_usage = context.storage_usage;
        let max_gas_burnt = match context.view_config {
            Some(ViewConfig { max_gas_burnt: max_gas_burnt_view }) => max_gas_burnt_view,
            None => config.limit_config.max_gas_burnt,
        };

        let current_account_locked_balance = context.account_locked_balance;
        let gas_counter = GasCounter::new(
            config.ext_costs.clone(),
            max_gas_burnt,
            config.regular_op_cost,
            context.prepaid_gas,
            context.is_view(),
        );
        Self {
            ext,
            context,
            config,
            fees_config,
            promise_results,
            memory,
            current_account_balance,
            current_account_locked_balance,
            current_storage_usage,
            gas_counter,
            return_data: ReturnData::None,
            logs: vec![],
            registers: HashMap::new(),
            promises: vec![],
            total_log_length: 0,
            current_protocol_version,
            receipt_manager: ReceiptManager::default(),
        }
    }

    /// Returns reference to logs that have been created so far.
    pub fn logs(&self) -> &[String] {
        &self.logs
    }

    /// Returns receipt metadata for created receipts
    pub fn action_receipts(&self) -> &[(AccountId, ReceiptMetadata)] {
        &self.receipt_manager.action_receipts
    }

    #[allow(dead_code)]
    #[cfg(test)]
    pub(crate) fn receipt_manager(&self) -> &ReceiptManager {
        &self.receipt_manager
    }

    #[allow(dead_code)]
    #[cfg(test)]
    pub(crate) fn gas_counter(&self) -> &GasCounter {
        &self.gas_counter
    }

    // ###########################
    // # Memory helper functions #
    // ###########################

    fn try_fit_mem(&mut self, offset: u64, len: u64) -> Result<()> {
        if self.memory.fits_memory(offset, len) {
            Ok(())
        } else {
            Err(HostError::MemoryAccessViolation.into())
        }
    }

    fn memory_get_into(&mut self, offset: u64, buf: &mut [u8]) -> Result<()> {
        self.gas_counter.pay_base(read_memory_base)?;
        self.gas_counter.pay_per(read_memory_byte, buf.len() as _)?;
        self.try_fit_mem(offset, buf.len() as _)?;
        self.memory.read_memory(offset, buf);
        Ok(())
    }

    fn memory_get_vec(&mut self, offset: u64, len: u64) -> Result<Vec<u8>> {
        self.gas_counter.pay_base(read_memory_base)?;
        self.gas_counter.pay_per(read_memory_byte, len)?;
        self.try_fit_mem(offset, len)?;
        let mut buf = vec![0; len as usize];
        self.memory.read_memory(offset, &mut buf);
        Ok(buf)
    }

    memory_get!(u128, memory_get_u128);
    memory_get!(u32, memory_get_u32);
    memory_get!(u16, memory_get_u16);
    memory_get!(u8, memory_get_u8);

    /// Reads an array of `u64` elements.
    fn memory_get_vec_u64(&mut self, offset: u64, num_elements: u64) -> Result<Vec<u64>> {
        let memory_len = num_elements
            .checked_mul(size_of::<u64>() as u64)
            .ok_or(HostError::MemoryAccessViolation)?;
        let data = self.memory_get_vec(offset, memory_len)?;
        let mut res = vec![0u64; num_elements as usize];
        byteorder::LittleEndian::read_u64_into(&data, &mut res);
        Ok(res)
    }

    fn get_vec_from_memory_or_register(&mut self, offset: u64, len: u64) -> Result<Vec<u8>> {
        if len != u64::MAX {
            self.memory_get_vec(offset, len)
        } else {
            self.internal_read_register(offset)
        }
    }

    fn memory_set_slice(&mut self, offset: u64, buf: &[u8]) -> Result<()> {
        self.gas_counter.pay_base(write_memory_base)?;
        self.gas_counter.pay_per(write_memory_byte, buf.len() as _)?;
        self.try_fit_mem(offset, buf.len() as _)?;
        self.memory.write_memory(offset, buf);
        Ok(())
    }

    memory_set!(u128, memory_set_u128);

    // #################
    // # Registers API #
    // #################

    fn internal_read_register(&mut self, register_id: u64) -> Result<Vec<u8>> {
        if let Some(data) = self.registers.get(&register_id) {
            self.gas_counter.pay_base(read_register_base)?;
            self.gas_counter.pay_per(read_register_byte, data.len() as _)?;
            Ok(data.clone())
        } else {
            Err(HostError::InvalidRegisterId { register_id }.into())
        }
    }

    fn internal_write_register(&mut self, register_id: u64, data: Vec<u8>) -> Result<()> {
        self.gas_counter.pay_base(write_register_base)?;
        self.gas_counter.pay_per(write_register_byte, data.len() as u64)?;
        if data.len() as u64 > self.config.limit_config.max_register_size
            || self.registers.len() as u64 >= self.config.limit_config.max_number_registers
        {
            return Err(HostError::MemoryAccessViolation.into());
        }
        self.registers.insert(register_id, data);

        // Calculate the new memory usage.
        let usage: usize =
            self.registers.values().map(|v| size_of::<u64>() + v.len() * size_of::<u8>()).sum();
        if usage as u64 > self.config.limit_config.registers_memory_limit {
            Err(HostError::MemoryAccessViolation.into())
        } else {
            Ok(())
        }
    }

    /// Convenience function for testing.
    pub fn wrapped_internal_write_register(&mut self, register_id: u64, data: &[u8]) -> Result<()> {
        self.internal_write_register(register_id, data.to_vec())
    }

    /// Writes the entire content from the register `register_id` into the memory of the guest starting with `ptr`.
    ///
    /// # Arguments
    ///
    /// * `register_id` -- a register id from where to read the data;
    /// * `ptr` -- location on guest memory where to copy the data.
    ///
    /// # Errors
    ///
    /// * If the content extends outside the memory allocated to the guest. In Wasmer, it returns `MemoryAccessViolation` error message;
    /// * If `register_id` is pointing to unused register returns `InvalidRegisterId` error message.
    ///
    /// # Undefined Behavior
    ///
    /// If the content of register extends outside the preallocated memory on the host side, or the pointer points to a
    /// wrong location this function will overwrite memory that it is not supposed to overwrite causing an undefined behavior.
    ///
    /// # Cost
    ///
    /// `base + read_register_base + read_register_byte * num_bytes + write_memory_base + write_memory_byte * num_bytes`
    pub fn read_register(&mut self, register_id: u64, ptr: u64) -> Result<()> {
        self.gas_counter.pay_base(base)?;
        let data = self.internal_read_register(register_id)?;
        self.memory_set_slice(ptr, &data)
    }

    /// Returns the size of the blob stored in the given register.
    /// * If register is used, then returns the size, which can potentially be zero;
    /// * If register is not used, returns `u64::MAX`
    ///
    /// # Arguments
    ///
    /// * `register_id` -- a register id from where to read the data;
    ///
    /// # Cost
    ///
    /// `base`
    pub fn register_len(&mut self, register_id: u64) -> Result<u64> {
        self.gas_counter.pay_base(base)?;
        Ok(self.registers.get(&register_id).map(|r| r.len() as _).unwrap_or(u64::MAX))
    }

    /// Copies `data` from the guest memory into the register. If register is unused will initialize
    /// it. If register has larger capacity than needed for `data` will not re-allocate it. The
    /// register will lose the pre-existing data if any.
    ///
    /// # Arguments
    ///
    /// * `register_id` -- a register id where to write the data;
    /// * `data_len` -- length of the data in bytes;
    /// * `data_ptr` -- pointer in the guest memory where to read the data from.
    ///
    /// # Cost
    ///
    /// `base + read_memory_base + read_memory_bytes * num_bytes + write_register_base + write_register_bytes * num_bytes`
    pub fn write_register(&mut self, register_id: u64, data_len: u64, data_ptr: u64) -> Result<()> {
        self.gas_counter.pay_base(base)?;
        let data = self.memory_get_vec(data_ptr, data_len)?;
        self.internal_write_register(register_id, data)
    }

    // ###################################
    // # String reading helper functions #
    // ###################################

    /// Helper function to read and return utf8-encoding string.
    /// If `len == u64::MAX` then treats the string as null-terminated with character `'\0'`.
    ///
    /// # Errors
    ///
    /// * If string extends outside the memory of the guest with `MemoryAccessViolation`;
    /// * If string is not UTF-8 returns `BadUtf8`.
    /// * If number of bytes read + `total_log_length` exceeds the `max_total_log_length` returns
    ///   `TotalLogLengthExceeded`.
    ///
    /// # Cost
    ///
    /// For not nul-terminated string:
    /// `read_memory_base + read_memory_byte * num_bytes + utf8_decoding_base + utf8_decoding_byte * num_bytes`
    ///
    /// For nul-terminated string:
    /// `(read_memory_base + read_memory_byte) * num_bytes + utf8_decoding_base + utf8_decoding_byte * num_bytes`
    fn get_utf8_string(&mut self, len: u64, ptr: u64) -> Result<String> {
        self.gas_counter.pay_base(utf8_decoding_base)?;
        let mut buf;
        let max_len =
            self.config.limit_config.max_total_log_length.saturating_sub(self.total_log_length);
        if len != u64::MAX {
            if len > max_len {
                return Err(HostError::TotalLogLengthExceeded {
                    length: self.total_log_length.saturating_add(len),
                    limit: self.config.limit_config.max_total_log_length,
                }
                .into());
            }
            buf = self.memory_get_vec(ptr, len)?;
        } else {
            buf = vec![];
            for i in 0..=max_len {
                // self.try_fit_mem will check for u64 overflow on the first iteration (i == 0)
                let el = self.memory_get_u8(ptr + i)?;
                if el == 0 {
                    break;
                }
                if i == max_len {
                    return Err(HostError::TotalLogLengthExceeded {
                        length: self.total_log_length.saturating_add(max_len).saturating_add(1),
                        limit: self.config.limit_config.max_total_log_length,
                    }
                    .into());
                }
                buf.push(el);
            }
        }
        self.gas_counter.pay_per(utf8_decoding_byte, buf.len() as _)?;
        String::from_utf8(buf).map_err(|_| HostError::BadUTF8.into())
    }

    /// Helper function to get utf8 string, for sandbox debug log. The difference with `get_utf8_string`:
    /// * It's only available on sandbox node
    /// * The cost is 0
    /// * It's up to the caller to set correct len
    #[cfg(feature = "sandbox")]
    fn sandbox_get_utf8_string(&mut self, len: u64, ptr: u64) -> Result<String> {
        self.try_fit_mem(ptr, len)?;
        let mut buf = vec![0; len as usize];
        self.memory.read_memory(ptr, &mut buf);
        String::from_utf8(buf).map_err(|_| HostError::BadUTF8.into())
    }

    /// Helper function to read UTF-16 formatted string from guest memory.
    /// # Errors
    ///
    /// * If string extends outside the memory of the guest with `MemoryAccessViolation`;
    /// * If string is not UTF-16 returns `BadUtf16`.
    /// * If number of bytes read + `total_log_length` exceeds the `max_total_log_length` returns
    ///   `TotalLogLengthExceeded`.
    ///
    /// # Cost
    ///
    /// For not nul-terminated string:
    /// `read_memory_base + read_memory_byte * num_bytes + utf16_decoding_base + utf16_decoding_byte * num_bytes`
    ///
    /// For nul-terminated string:
    /// `read_memory_base * num_bytes / 2 + read_memory_byte * num_bytes + utf16_decoding_base + utf16_decoding_byte * num_bytes`
    fn get_utf16_string(&mut self, len: u64, ptr: u64) -> Result<String> {
        self.gas_counter.pay_base(utf16_decoding_base)?;
        let mut u16_buffer;
        let max_len =
            self.config.limit_config.max_total_log_length.saturating_sub(self.total_log_length);
        if len != u64::MAX {
            let input = self.memory_get_vec(ptr, len)?;
            if len % 2 != 0 {
                return Err(HostError::BadUTF16.into());
            }
            if len > max_len {
                return Err(HostError::TotalLogLengthExceeded {
                    length: self.total_log_length.saturating_add(len),
                    limit: self.config.limit_config.max_total_log_length,
                }
                .into());
            }
            u16_buffer = vec![0u16; len as usize / 2];
            byteorder::LittleEndian::read_u16_into(&input, &mut u16_buffer);
        } else {
            u16_buffer = vec![];
            let limit = max_len / size_of::<u16>() as u64;
            // Takes 2 bytes each iter
            for i in 0..=limit {
                // self.try_fit_mem will check for u64 overflow on the first iteration (i == 0)
                let start = ptr + i * size_of::<u16>() as u64;
                let el = self.memory_get_u16(start)?;
                if el == 0 {
                    break;
                }
                if i == limit {
                    return Err(HostError::TotalLogLengthExceeded {
                        length: self
                            .total_log_length
                            .saturating_add(i * size_of::<u16>() as u64)
                            .saturating_add(size_of::<u16>() as u64),
                        limit: self.config.limit_config.max_total_log_length,
                    }
                    .into());
                }
                u16_buffer.push(el);
            }
        }
        self.gas_counter
            .pay_per(utf16_decoding_byte, u16_buffer.len() as u64 * size_of::<u16>() as u64)?;
        String::from_utf16(&u16_buffer).map_err(|_| HostError::BadUTF16.into())
    }

    // ####################################################
    // # Helper functions to prevent code duplication API #
    // ####################################################

    /// Checks that the current log number didn't reach the limit yet, so we can add a new message.
    fn check_can_add_a_log_message(&self) -> Result<()> {
        if self.logs.len() as u64 >= self.config.limit_config.max_number_logs {
            Err(HostError::NumberOfLogsExceeded { limit: self.config.limit_config.max_number_logs }
                .into())
        } else {
            Ok(())
        }
    }

    /// Adds a given promise to the vector of promises and returns a new promise index.
    /// Throws `NumberPromisesExceeded` if the total number of promises exceeded the limit.
    fn checked_push_promise(&mut self, promise: Promise) -> Result<PromiseIndex> {
        let new_promise_idx = self.promises.len() as PromiseIndex;
        self.promises.push(promise);
        if self.promises.len() as u64
            > self.config.limit_config.max_promises_per_function_call_action
        {
            Err(HostError::NumberPromisesExceeded {
                number_of_promises: self.promises.len() as u64,
                limit: self.config.limit_config.max_promises_per_function_call_action,
            }
            .into())
        } else {
            Ok(new_promise_idx)
        }
    }

    fn checked_push_log(&mut self, message: String) -> Result<()> {
        // The size of logged data can't be too large. No overflow.
        self.total_log_length += message.len() as u64;
        if self.total_log_length > self.config.limit_config.max_total_log_length {
            return Err(HostError::TotalLogLengthExceeded {
                length: self.total_log_length,
                limit: self.config.limit_config.max_total_log_length,
            }
            .into());
        }
        self.logs.push(message);
        Ok(())
    }

    // ###############
    // # Context API #
    // ###############

    /// Saves the account id of the current contract that we execute into the register.
    ///
    /// # Errors
    ///
    /// If the registers exceed the memory limit returns `MemoryAccessViolation`.
    ///
    /// # Cost
    ///
    /// `base + write_register_base + write_register_byte * num_bytes`
    pub fn current_account_id(&mut self, register_id: u64) -> Result<()> {
        self.gas_counter.pay_base(base)?;

        self.internal_write_register(
            register_id,
            self.context.current_account_id.as_ref().as_bytes().to_vec(),
        )
    }

    /// All contract calls are a result of some transaction that was signed by some account using
    /// some access key and submitted into a memory pool (either through the wallet using RPC or by
    /// a node itself). This function returns the id of that account. Saves the bytes of the signer
    /// account id into the register.
    ///
    /// # Errors
    ///
    /// * If the registers exceed the memory limit returns `MemoryAccessViolation`.
    /// * If called as view function returns `ProhibitedInView`.
    ///
    /// # Cost
    ///
    /// `base + write_register_base + write_register_byte * num_bytes`
    pub fn signer_account_id(&mut self, register_id: u64) -> Result<()> {
        self.gas_counter.pay_base(base)?;

        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "signer_account_id".to_string(),
            }
            .into());
        }
        self.internal_write_register(
            register_id,
            self.context.signer_account_id.as_ref().as_bytes().to_vec(),
        )
    }

    /// Saves the public key fo the access key that was used by the signer into the register. In
    /// rare situations smart contract might want to know the exact access key that was used to send
    /// the original transaction, e.g. to increase the allowance or manipulate with the public key.
    ///
    /// # Errors
    ///
    /// * If the registers exceed the memory limit returns `MemoryAccessViolation`.
    /// * If called as view function returns `ProhibitedInView`.
    ///
    /// # Cost
    ///
    /// `base + write_register_base + write_register_byte * num_bytes`
    pub fn signer_account_pk(&mut self, register_id: u64) -> Result<()> {
        self.gas_counter.pay_base(base)?;

        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "signer_account_pk".to_string(),
            }
            .into());
        }
        self.internal_write_register(register_id, self.context.signer_account_pk.clone())
    }

    /// All contract calls are a result of a receipt, this receipt might be created by a transaction
    /// that does function invocation on the contract or another contract as a result of
    /// cross-contract call. Saves the bytes of the predecessor account id into the register.
    ///
    /// # Errors
    ///
    /// * If the registers exceed the memory limit returns `MemoryAccessViolation`.
    /// * If called as view function returns `ProhibitedInView`.
    ///
    /// # Cost
    ///
    /// `base + write_register_base + write_register_byte * num_bytes`
    pub fn predecessor_account_id(&mut self, register_id: u64) -> Result<()> {
        self.gas_counter.pay_base(base)?;

        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "predecessor_account_id".to_string(),
            }
            .into());
        }
        self.internal_write_register(
            register_id,
            self.context.predecessor_account_id.as_ref().as_bytes().to_vec(),
        )
    }

    /// Reads input to the contract call into the register. Input is expected to be in JSON-format.
    /// If input is provided saves the bytes (potentially zero) of input into register. If input is
    /// not provided writes 0 bytes into the register.
    ///
    /// # Cost
    ///
    /// `base + write_register_base + write_register_byte * num_bytes`
    pub fn input(&mut self, register_id: u64) -> Result<()> {
        self.gas_counter.pay_base(base)?;

        self.internal_write_register(register_id, self.context.input.clone())
    }

    /// Returns the current block height.
    ///
    /// # Cost
    ///
    /// `base`
    // TODO #1903 rename to `block_height`
    pub fn block_index(&mut self) -> Result<u64> {
        self.gas_counter.pay_base(base)?;
        Ok(self.context.block_index)
    }

    /// Returns the current block timestamp (number of non-leap-nanoseconds since January 1, 1970 0:00:00 UTC).
    ///
    /// # Cost
    ///
    /// `base`
    pub fn block_timestamp(&mut self) -> Result<u64> {
        self.gas_counter.pay_base(base)?;
        Ok(self.context.block_timestamp)
    }

    /// Returns the current epoch height.
    ///
    /// # Cost
    ///
    /// `base`
    pub fn epoch_height(&mut self) -> Result<EpochHeight> {
        self.gas_counter.pay_base(base)?;
        Ok(self.context.epoch_height)
    }

    /// Get the stake of an account, if the account is currently a validator. Otherwise returns 0.
    /// writes the value into the` u128` variable pointed by `stake_ptr`.
    ///
    /// # Cost
    ///
    /// `base + memory_write_base + memory_write_size * 16 + utf8_decoding_base + utf8_decoding_byte * account_id_len + validator_stake_base`.
    pub fn validator_stake(
        &mut self,
        account_id_len: u64,
        account_id_ptr: u64,
        stake_ptr: u64,
    ) -> Result<()> {
        self.gas_counter.pay_base(base)?;
        let account_id = self.read_and_parse_account_id(account_id_ptr, account_id_len)?;
        self.gas_counter.pay_base(validator_stake_base)?;
        let balance = self.ext.validator_stake(&account_id)?.unwrap_or_default();
        self.memory_set_u128(stake_ptr, balance)
    }

    /// Get the total validator stake of the current epoch.
    /// Write the u128 value into `stake_ptr`.
    /// writes the value into the` u128` variable pointed by `stake_ptr`.
    ///
    /// # Cost
    ///
    /// `base + memory_write_base + memory_write_size * 16 + validator_total_stake_base`
    pub fn validator_total_stake(&mut self, stake_ptr: u64) -> Result<()> {
        self.gas_counter.pay_base(base)?;
        self.gas_counter.pay_base(validator_total_stake_base)?;
        let total_stake = self.ext.validator_total_stake()?;
        self.memory_set_u128(stake_ptr, total_stake)
    }

    /// Returns the number of bytes used by the contract if it was saved to the trie as of the
    /// invocation. This includes:
    /// * The data written with storage_* functions during current and previous execution;
    /// * The bytes needed to store the access keys of the given account.
    /// * The contract code size
    /// * A small fixed overhead for account metadata.
    ///
    /// # Cost
    ///
    /// `base`
    pub fn storage_usage(&mut self) -> Result<StorageUsage> {
        self.gas_counter.pay_base(base)?;
        Ok(self.current_storage_usage)
    }

    // #################
    // # Economics API #
    // #################

    /// The current balance of the given account. This includes the attached_deposit that was
    /// attached to the transaction.
    ///
    /// # Cost
    ///
    /// `base + memory_write_base + memory_write_size * 16`
    pub fn account_balance(&mut self, balance_ptr: u64) -> Result<()> {
        self.gas_counter.pay_base(base)?;
        self.memory_set_u128(balance_ptr, self.current_account_balance)
    }

    /// The current amount of tokens locked due to staking.
    ///
    /// # Cost
    ///
    /// `base + memory_write_base + memory_write_size * 16`
    pub fn account_locked_balance(&mut self, balance_ptr: u64) -> Result<()> {
        self.gas_counter.pay_base(base)?;
        self.memory_set_u128(balance_ptr, self.current_account_locked_balance)
    }

    /// The balance that was attached to the call that will be immediately deposited before the
    /// contract execution starts.
    ///
    /// # Errors
    ///
    /// If called as view function returns `ProhibitedInView``.
    ///
    /// # Cost
    ///
    /// `base + memory_write_base + memory_write_size * 16`
    pub fn attached_deposit(&mut self, balance_ptr: u64) -> Result<()> {
        self.gas_counter.pay_base(base)?;

        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "attached_deposit".to_string(),
            }
            .into());
        }
        self.memory_set_u128(balance_ptr, self.context.attached_deposit)
    }

    /// The amount of gas attached to the call that can be used to pay for the gas fees.
    ///
    /// # Errors
    ///
    /// If called as view function returns `ProhibitedInView`.
    ///
    /// # Cost
    ///
    /// `base`
    pub fn prepaid_gas(&mut self) -> Result<Gas> {
        self.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(
                HostError::ProhibitedInView { method_name: "prepaid_gas".to_string() }.into()
            );
        }
        Ok(self.context.prepaid_gas)
    }

    /// The gas that was already burnt during the contract execution (cannot exceed `prepaid_gas`)
    ///
    /// # Errors
    ///
    /// If called as view function returns `ProhibitedInView`.
    ///
    /// # Cost
    ///
    /// `base`
    pub fn used_gas(&mut self) -> Result<Gas> {
        self.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView { method_name: "used_gas".to_string() }.into());
        }
        Ok(self.gas_counter.used_gas())
    }

    // ############
    // # Math API #
    // ############

    /// Computes multiexp on alt_bn128 curve using Pippenger's algorithm \sum_i
    /// mul_i g_{1 i} should be equal result.
    ///
    /// # Arguments
    ///
    /// * `value` - sequence of (g1:G1, fr:Fr), where
    ///    G1 is point (x:Fq, y:Fq) on alt_bn128,
    ///   alt_bn128 is Y^2 = X^3 + 3 curve over Fq.
    ///
    ///   `value` is encoded as packed, little-endian
    ///   `[((u256, u256), u256)]` slice.
    ///
    /// # Errors
    ///
    /// If `value_len + value_ptr` points outside the memory or the registers
    /// use more memory than the limit, the function returns
    /// `MemoryAccessViolation`.
    ///
    /// If point coordinates are not on curve, point is not in the subgroup,
    /// scalar is not in the field or  `value.len()%96!=0`, the function returns
    /// `AltBn128InvalidInput`.
    ///
    /// # Cost
    ///
    /// `base + write_register_base + write_register_byte * num_bytes +
    ///  alt_bn128_g1_multiexp_base +
    ///  alt_bn128_g1_multiexp_element * num_elements`
    pub fn alt_bn128_g1_multiexp(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<()> {
        self.gas_counter.pay_base(alt_bn128_g1_multiexp_base)?;
        let data = self.get_vec_from_memory_or_register(value_ptr, value_len)?;

        let elements = crate::alt_bn128::split_elements(&data)?;
        self.gas_counter.pay_per(alt_bn128_g1_multiexp_element, elements.len() as u64)?;

        let res = crate::alt_bn128::g1_multiexp(elements)?;

        self.internal_write_register(register_id, res.into())
    }

    /// Computes sum for signed g1 group elements on alt_bn128 curve \sum_i
    /// (-1)^{sign_i} g_{1 i} should be equal result.
    ///
    /// # Arguments
    ///
    /// * `value` - sequence of (sign:bool, g1:G1), where
    ///    G1 is point (x:Fq, y:Fq) on alt_bn128,
    ///    alt_bn128 is Y^2 = X^3 + 3 curve over Fq.
    ///
    ///   `value` is encoded as packed, little-endian
    ///   `[(u8, (u256, u256))]` slice. `0u8` is postive sign,
    ///   `1u8` -- negative.
    ///
    /// # Errors
    ///
    /// If `value_len + value_ptr` points outside the memory or the registers
    /// use more memory than the limit, the function returns `MemoryAccessViolation`.
    ///
    /// If point coordinates are not on curve, point is not in the subgroup,
    /// scalar is not in the field, sign is not 0 or 1, or `value.len()%65!=0`,
    /// the function returns `AltBn128InvalidInput`.
    ///
    /// # Cost
    ///
    /// `base + write_register_base + write_register_byte * num_bytes +
    /// alt_bn128_g1_sum_base + alt_bn128_g1_sum_element * num_elements`
    pub fn alt_bn128_g1_sum(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<()> {
        self.gas_counter.pay_base(alt_bn128_g1_sum_base)?;
        let data = self.get_vec_from_memory_or_register(value_ptr, value_len)?;

        let elements = crate::alt_bn128::split_elements(&data)?;
        self.gas_counter.pay_per(alt_bn128_g1_sum_element, elements.len() as u64)?;

        let res = crate::alt_bn128::g1_sum(elements)?;

        self.internal_write_register(register_id, res.into())
    }

    /// Computes pairing check on alt_bn128 curve.
    /// \sum_i e(g_{1 i}, g_{2 i}) should be equal one (in additive notation), e(g1, g2) is Ate pairing
    ///
    /// # Arguments
    ///
    /// * `value` - sequence of (g1:G1, g2:G2), where
    ///   G2 is Fr-ordered subgroup point (x:Fq2, y:Fq2) on alt_bn128 twist,
    ///   alt_bn128 twist is Y^2 = X^3 + 3/(i+9) curve over Fq2
    ///   Fq2 is complex field element (re: Fq, im: Fq)
    ///   G1 is point (x:Fq, y:Fq) on alt_bn128,
    ///   alt_bn128 is Y^2 = X^3 + 3 curve over Fq
    ///
    ///   `value` is encoded a as packed, little-endian
    ///   `[((u256, u256), ((u256, u256), (u256, u256)))]` slice.
    ///
    /// # Errors
    ///
    /// If `value_len + value_ptr` points outside the memory or the registers use more memory than
    /// the function returns `MemoryAccessViolation`.
    ///
    /// If point coordinates are not on curve, point is not in the subgroup, scalar
    /// is not in the field or data are wrong serialized, for example,
    /// `value.len()%192!=0`, the function returns `AltBn128InvalidInput`.
    ///
    /// # Cost
    ///
    /// `base + write_register_base + write_register_byte * num_bytes + alt_bn128_pairing_base + alt_bn128_pairing_element * num_elements`
    pub fn alt_bn128_pairing_check(&mut self, value_len: u64, value_ptr: u64) -> Result<u64> {
        self.gas_counter.pay_base(alt_bn128_pairing_check_base)?;
        let data = self.get_vec_from_memory_or_register(value_ptr, value_len)?;

        let elements = crate::alt_bn128::split_elements(&data)?;
        self.gas_counter.pay_per(alt_bn128_pairing_check_element, elements.len() as u64)?;

        let res = crate::alt_bn128::pairing_check(elements)?;

        Ok(res as u64)
    }

    /// Writes random seed into the register.
    ///
    /// # Errors
    ///
    /// If the size of the registers exceed the set limit `MemoryAccessViolation`.
    ///
    /// # Cost
    ///
    /// `base + write_register_base + write_register_byte * num_bytes`.
    pub fn random_seed(&mut self, register_id: u64) -> Result<()> {
        self.gas_counter.pay_base(base)?;
        self.internal_write_register(register_id, self.context.random_seed.clone())
    }

    /// Hashes the given value using sha256 and returns it into `register_id`.
    ///
    /// # Errors
    ///
    /// If `value_len + value_ptr` points outside the memory or the registers use more memory than
    /// the limit with `MemoryAccessViolation`.
    ///
    /// # Cost
    ///
    /// `base + write_register_base + write_register_byte * num_bytes + sha256_base + sha256_byte * num_bytes`
    pub fn sha256(&mut self, value_len: u64, value_ptr: u64, register_id: u64) -> Result<()> {
        self.gas_counter.pay_base(sha256_base)?;
        let value = self.get_vec_from_memory_or_register(value_ptr, value_len)?;
        self.gas_counter.pay_per(sha256_byte, value.len() as u64)?;

        use sha2::Digest;

        let value_hash = sha2::Sha256::digest(&value);
        self.internal_write_register(register_id, value_hash.as_slice().to_vec())
    }

    /// Hashes the given value using keccak256 and returns it into `register_id`.
    ///
    /// # Errors
    ///
    /// If `value_len + value_ptr` points outside the memory or the registers use more memory than
    /// the limit with `MemoryAccessViolation`.
    ///
    /// # Cost
    ///
    /// `base + write_register_base + write_register_byte * num_bytes + keccak256_base + keccak256_byte * num_bytes`
    pub fn keccak256(&mut self, value_len: u64, value_ptr: u64, register_id: u64) -> Result<()> {
        self.gas_counter.pay_base(keccak256_base)?;
        let value = self.get_vec_from_memory_or_register(value_ptr, value_len)?;
        self.gas_counter.pay_per(keccak256_byte, value.len() as u64)?;

        use sha3::Digest;

        let value_hash = sha3::Keccak256::digest(&value);
        self.internal_write_register(register_id, value_hash.as_slice().to_vec())
    }

    /// Hashes the given value using keccak512 and returns it into `register_id`.
    ///
    /// # Errors
    ///
    /// If `value_len + value_ptr` points outside the memory or the registers use more memory than
    /// the limit with `MemoryAccessViolation`.
    ///
    /// # Cost
    ///
    /// `base + write_register_base + write_register_byte * num_bytes + keccak512_base + keccak512_byte * num_bytes`
    pub fn keccak512(&mut self, value_len: u64, value_ptr: u64, register_id: u64) -> Result<()> {
        self.gas_counter.pay_base(keccak512_base)?;
        let value = self.get_vec_from_memory_or_register(value_ptr, value_len)?;
        self.gas_counter.pay_per(keccak512_byte, value.len() as u64)?;

        use sha3::Digest;

        let value_hash = sha3::Keccak512::digest(&value);
        self.internal_write_register(register_id, value_hash.as_slice().to_vec())
    }

    /// Hashes the given value using RIPEMD-160 and returns it into `register_id`.
    ///
    /// # Errors
    ///
    /// If `value_len + value_ptr` points outside the memory or the registers use more memory than
    /// the limit with `MemoryAccessViolation`.
    ///
    /// # Cost
    ///
    ///  Where `message_blocks` is `(value_len + 9).div_ceil(64)`.
    ///
    /// `base + write_register_base + write_register_byte * num_bytes + ripemd160_base + ripemd160_block * message_blocks`
    pub fn ripemd160(&mut self, value_len: u64, value_ptr: u64, register_id: u64) -> Result<()> {
        self.gas_counter.pay_base(ripemd160_base)?;
        let value = self.get_vec_from_memory_or_register(value_ptr, value_len)?;

        let message_blocks = value
            .len()
            .checked_add(8)
            .ok_or(VMLogicError::HostError(HostError::IntegerOverflow))?
            / 64
            + 1;

        self.gas_counter.pay_per(ripemd160_block, message_blocks as u64)?;

        use ripemd::Digest;

        let value_hash = ripemd::Ripemd160::digest(&value);
        self.internal_write_register(register_id, value_hash.as_slice().to_vec())
    }

    /// Recovers an ECDSA signer address and returns it into `register_id`.
    ///
    /// Takes in an additional flag to check for malleability of the signature
    /// which is generally only ideal for transactions.
    ///
    /// Returns a bool indicating success or failure as a `u64`.
    ///
    /// # Malleability Flags
    ///
    /// 0 - No extra checks.
    /// 1 - Rejecting upper range.
    ///
    /// # Errors
    ///
    /// * If `hash_ptr`, `r_ptr`, or `s_ptr` point outside the memory or the registers use more
    ///   memory than the limit, then returns `MemoryAccessViolation`.
    ///
    /// # Cost
    ///
    /// `base + write_register_base + write_register_byte * 64 + ecrecover_base`
    pub fn ecrecover(
        &mut self,
        hash_len: u64,
        hash_ptr: u64,
        sig_len: u64,
        sig_ptr: u64,
        v: u64,
        malleability_flag: u64,
        register_id: u64,
    ) -> Result<u64> {
        self.gas_counter.pay_base(ecrecover_base)?;

        let signature = {
            let vec = self.get_vec_from_memory_or_register(sig_ptr, sig_len)?;
            if vec.len() != 64 {
                return Err(VMLogicError::HostError(HostError::ECRecoverError {
                    msg: format!(
                        "The length of the signature: {}, exceeds the limit of 64 bytes",
                        vec.len()
                    ),
                }));
            }

            let mut bytes = [0u8; 65];
            bytes[0..64].copy_from_slice(&vec);

            if v < 4 {
                bytes[64] = v as u8;
                Secp256K1Signature::from(bytes)
            } else {
                return Err(VMLogicError::HostError(HostError::ECRecoverError {
                    msg: format!("V recovery byte 0 through 3 are valid but was provided {}", v),
                }));
            }
        };

        let hash = {
            let vec = self.get_vec_from_memory_or_register(hash_ptr, hash_len)?;
            if vec.len() != 32 {
                return Err(VMLogicError::HostError(HostError::ECRecoverError {
                    msg: format!(
                        "The length of the hash: {}, exceeds the limit of 32 bytes",
                        vec.len()
                    ),
                }));
            }

            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(&vec);
            bytes
        };

        if malleability_flag != 0 && malleability_flag != 1 {
            return Err(VMLogicError::HostError(HostError::ECRecoverError {
                msg: format!(
                    "Malleability flag needs to be 0 or 1, but is instead {}",
                    malleability_flag
                ),
            }));
        }

        if !signature.check_signature_values(malleability_flag != 0) {
            return Ok(false as u64);
        }

        if let Ok(pk) = signature.recover(hash) {
            self.internal_write_register(register_id, pk.as_ref().to_vec())?;
            return Ok(true as u64);
        };

        Ok(false as u64)
    }

    /// Called by gas metering injected into Wasm. Counts both towards `burnt_gas` and `used_gas`.
    ///
    /// # Errors
    ///
    /// * If passed gas amount somehow overflows internal gas counters returns `IntegerOverflow`;
    /// * If we exceed usage limit imposed on burnt gas returns `GasLimitExceeded`;
    /// * If we exceed the `prepaid_gas` then returns `GasExceeded`.
    pub fn gas(&mut self, opcodes: u32) -> Result<()> {
        self.gas_counter.pay_wasm_gas(opcodes)
    }

    // ################
    // # Promises API #
    // ################

    /// A helper function to pay gas fee for creating a new receipt without actions.
    /// # Args:
    /// * `sir`: whether contract call is addressed to itself;
    /// * `data_dependencies`: other contracts that this execution will be waiting on (or rather
    ///   their data receipts), where bool indicates whether this is sender=receiver communication.
    ///
    /// # Cost
    ///
    /// This is a convenience function that encapsulates several costs:
    /// `burnt_gas := dispatch cost of the receipt + base dispatch cost  cost of the data receipt`
    /// `used_gas := burnt_gas + exec cost of the receipt + base exec cost  cost of the data receipt`
    /// Notice that we prepay all base cost upon the creation of the data dependency, we are going to
    /// pay for the content transmitted through the dependency upon the actual creation of the
    /// DataReceipt.
    fn pay_gas_for_new_receipt(&mut self, sir: bool, data_dependencies: &[bool]) -> Result<()> {
        let fees_config_cfg = &self.fees_config;
        let mut burn_gas = fees_config_cfg.action_receipt_creation_config.send_fee(sir);
        let mut use_gas = fees_config_cfg.action_receipt_creation_config.exec_fee();
        for dep in data_dependencies {
            // Both creation and execution for data receipts are considered burnt gas.
            burn_gas = burn_gas
                .checked_add(fees_config_cfg.data_receipt_creation_config.base_cost.send_fee(*dep))
                .ok_or(HostError::IntegerOverflow)?
                .checked_add(fees_config_cfg.data_receipt_creation_config.base_cost.exec_fee())
                .ok_or(HostError::IntegerOverflow)?;
        }
        use_gas = use_gas.checked_add(burn_gas).ok_or(HostError::IntegerOverflow)?;
        self.gas_counter.pay_action_accumulated(burn_gas, use_gas, ActionCosts::new_receipt)
    }

    /// A helper function to subtract balance on transfer or attached deposit for promises.
    /// # Args:
    /// * `amount`: the amount to deduct from the current account balance.
    fn deduct_balance(&mut self, amount: Balance) -> Result<()> {
        self.current_account_balance =
            self.current_account_balance.checked_sub(amount).ok_or(HostError::BalanceExceeded)?;
        Ok(())
    }

    /// Creates a promise that will execute a method on account with given arguments and attaches
    /// the given amount and gas. `amount_ptr` point to slices of bytes representing `u128`.
    ///
    /// # Errors
    ///
    /// * If `account_id_len + account_id_ptr` or `method_name_len + method_name_ptr` or
    /// `arguments_len + arguments_ptr` or `amount_ptr + 16` points outside the memory of the guest
    /// or host returns `MemoryAccessViolation`.
    /// * If called as view function returns `ProhibitedInView`.
    ///
    /// # Returns
    ///
    /// Index of the new promise that uniquely identifies it within the current execution of the
    /// method.
    ///
    /// # Cost
    ///
    /// Since `promise_create` is a convenience wrapper around `promise_batch_create` and
    /// `promise_batch_action_function_call`. This also means it charges `base` cost twice.
    pub fn promise_create(
        &mut self,
        account_id_len: u64,
        account_id_ptr: u64,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        amount_ptr: u64,
        gas: Gas,
    ) -> Result<u64> {
        let new_promise_idx = self.promise_batch_create(account_id_len, account_id_ptr)?;
        self.promise_batch_action_function_call(
            new_promise_idx,
            method_name_len,
            method_name_ptr,
            arguments_len,
            arguments_ptr,
            amount_ptr,
            gas,
        )?;
        Ok(new_promise_idx)
    }

    /// Attaches the callback that is executed after promise pointed by `promise_idx` is complete.
    ///
    /// # Errors
    ///
    /// * If `promise_idx` does not correspond to an existing promise returns `InvalidPromiseIndex`;
    /// * If `account_id_len + account_id_ptr` or `method_name_len + method_name_ptr` or
    ///   `arguments_len + arguments_ptr` or `amount_ptr + 16` points outside the memory of the
    ///   guest or host returns `MemoryAccessViolation`.
    /// * If called as view function returns `ProhibitedInView`.
    ///
    /// # Returns
    ///
    /// Index of the new promise that uniquely identifies it within the current execution of the
    /// method.
    ///
    /// # Cost
    ///
    /// Since `promise_create` is a convenience wrapper around `promise_batch_then` and
    /// `promise_batch_action_function_call`. This also means it charges `base` cost twice.
    pub fn promise_then(
        &mut self,
        promise_idx: u64,
        account_id_len: u64,
        account_id_ptr: u64,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        amount_ptr: u64,
        gas: u64,
    ) -> Result<u64> {
        let new_promise_idx =
            self.promise_batch_then(promise_idx, account_id_len, account_id_ptr)?;
        self.promise_batch_action_function_call(
            new_promise_idx,
            method_name_len,
            method_name_ptr,
            arguments_len,
            arguments_ptr,
            amount_ptr,
            gas,
        )?;
        Ok(new_promise_idx)
    }

    /// Creates a new promise which completes when time all promises passed as arguments complete.
    /// Cannot be used with registers. `promise_idx_ptr` points to an array of `u64` elements, with
    /// `promise_idx_count` denoting the number of elements. The array contains indices of promises
    /// that need to be waited on jointly.
    ///
    /// # Errors
    ///
    /// * If `promise_ids_ptr + 8 * promise_idx_count` extend outside the guest memory returns
    ///   `MemoryAccessViolation`;
    /// * If any of the promises in the array do not correspond to existing promises returns
    ///   `InvalidPromiseIndex`.
    /// * If called as view function returns `ProhibitedInView`.
    /// * If the total number of receipt dependencies exceeds `max_number_input_data_dependencies`
    ///   limit returns `NumInputDataDependenciesExceeded`.
    /// * If the total number of promises exceeds `max_promises_per_function_call_action` limit
    ///   returns `NumPromisesExceeded`.
    ///
    /// # Returns
    ///
    /// Index of the new promise that uniquely identifies it within the current execution of the
    /// method.
    ///
    /// # Cost
    ///
    /// `base + promise_and_base + promise_and_per_promise * num_promises + cost of reading promise ids from memory`.
    pub fn promise_and(
        &mut self,
        promise_idx_ptr: u64,
        promise_idx_count: u64,
    ) -> Result<PromiseIndex> {
        self.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(
                HostError::ProhibitedInView { method_name: "promise_and".to_string() }.into()
            );
        }
        self.gas_counter.pay_base(promise_and_base)?;
        self.gas_counter.pay_per(
            promise_and_per_promise,
            promise_idx_count
                .checked_mul(size_of::<u64>() as u64)
                .ok_or(HostError::IntegerOverflow)?,
        )?;

        let promise_indices = self.memory_get_vec_u64(promise_idx_ptr, promise_idx_count)?;

        let mut receipt_dependencies = vec![];
        for promise_idx in &promise_indices {
            let promise = self
                .promises
                .get(*promise_idx as usize)
                .ok_or(HostError::InvalidPromiseIndex { promise_idx: *promise_idx })?;
            match &promise {
                Promise::Receipt(receipt_idx) => {
                    receipt_dependencies.push(*receipt_idx);
                }
                Promise::NotReceipt(receipt_indices) => {
                    receipt_dependencies.extend(receipt_indices.clone());
                }
            }
            // Checking this in the loop to prevent abuse of too many joined vectors.
            if receipt_dependencies.len() as u64
                > self.config.limit_config.max_number_input_data_dependencies
            {
                return Err(HostError::NumberInputDataDependenciesExceeded {
                    number_of_input_data_dependencies: receipt_dependencies.len() as u64,
                    limit: self.config.limit_config.max_number_input_data_dependencies,
                }
                .into());
            }
        }
        self.checked_push_promise(Promise::NotReceipt(receipt_dependencies))
    }

    /// Creates a new promise towards given `account_id` without any actions attached to it.
    ///
    /// # Errors
    ///
    /// * If `account_id_len + account_id_ptr` points outside the memory of the guest or host
    /// returns `MemoryAccessViolation`.
    /// * If called as view function returns `ProhibitedInView`.
    /// * If the total number of promises exceeds `max_promises_per_function_call_action` limit
    ///   returns `NumPromisesExceeded`.
    ///
    /// # Returns
    ///
    /// Index of the new promise that uniquely identifies it within the current execution of the
    /// method.
    ///
    /// # Cost
    ///
    /// `burnt_gas := base + cost of reading and decoding the account id + dispatch cost of the receipt`.
    /// `used_gas := burnt_gas + exec cost of the receipt`.
    pub fn promise_batch_create(
        &mut self,
        account_id_len: u64,
        account_id_ptr: u64,
    ) -> Result<u64> {
        self.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_batch_create".to_string(),
            }
            .into());
        }
        let account_id = self.read_and_parse_account_id(account_id_ptr, account_id_len)?;
        let sir = account_id == self.context.current_account_id;
        self.pay_gas_for_new_receipt(sir, &[])?;
        let new_receipt_idx = self.receipt_manager.create_receipt(self.ext, vec![], account_id)?;

        self.checked_push_promise(Promise::Receipt(new_receipt_idx))
    }

    /// Creates a new promise towards given `account_id` without any actions attached, that is
    /// executed after promise pointed by `promise_idx` is complete.
    ///
    /// # Errors
    ///
    /// * If `promise_idx` does not correspond to an existing promise returns `InvalidPromiseIndex`;
    /// * If `account_id_len + account_id_ptr` points outside the memory of the guest or host
    /// returns `MemoryAccessViolation`.
    /// * If called as view function returns `ProhibitedInView`.
    /// * If the total number of promises exceeds `max_promises_per_function_call_action` limit
    ///   returns `NumPromisesExceeded`.
    ///
    /// # Returns
    ///
    /// Index of the new promise that uniquely identifies it within the current execution of the
    /// method.
    ///
    /// # Cost
    ///
    /// `base + cost of reading and decoding the account id + dispatch&execution cost of the receipt
    ///  + dispatch&execution base cost for each data dependency`
    pub fn promise_batch_then(
        &mut self,
        promise_idx: u64,
        account_id_len: u64,
        account_id_ptr: u64,
    ) -> Result<u64> {
        self.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_batch_then".to_string(),
            }
            .into());
        }
        let account_id = self.read_and_parse_account_id(account_id_ptr, account_id_len)?;
        // Update the DAG and return new promise idx.
        let promise = self
            .promises
            .get(promise_idx as usize)
            .ok_or(HostError::InvalidPromiseIndex { promise_idx })?;
        let receipt_dependencies = match &promise {
            Promise::Receipt(receipt_idx) => vec![*receipt_idx],
            Promise::NotReceipt(receipt_indices) => receipt_indices.clone(),
        };

        let sir = account_id == self.context.current_account_id;
        let deps: Vec<_> = receipt_dependencies
            .iter()
            .map(|&receipt_idx| self.get_account_by_receipt(receipt_idx) == &account_id)
            .collect();
        self.pay_gas_for_new_receipt(sir, &deps)?;

        let new_receipt_idx =
            self.receipt_manager.create_receipt(self.ext, receipt_dependencies, account_id)?;

        self.checked_push_promise(Promise::Receipt(new_receipt_idx))
    }

    /// Helper function to return the account id towards which the receipt is directed.
    fn get_account_by_receipt(&self, receipt_idx: ReceiptIndex) -> &AccountId {
        self.receipt_manager.get_receipt_receiver(receipt_idx)
    }

    /// Helper function to return the receipt index corresponding to the given promise index.
    /// It also pulls account ID for the given receipt and compares it with the current account ID
    /// to return whether the receipt's account ID is the same.
    fn promise_idx_to_receipt_idx_with_sir(
        &self,
        promise_idx: u64,
    ) -> Result<(ReceiptIndex, bool)> {
        let promise = self
            .promises
            .get(promise_idx as usize)
            .ok_or(HostError::InvalidPromiseIndex { promise_idx })?;
        let receipt_idx = match &promise {
            Promise::Receipt(receipt_idx) => Ok(*receipt_idx),
            Promise::NotReceipt(_) => Err(HostError::CannotAppendActionToJointPromise),
        }?;

        let account_id = self.get_account_by_receipt(receipt_idx);
        let sir = account_id == &self.context.current_account_id;
        Ok((receipt_idx, sir))
    }

    /// Appends `CreateAccount` action to the batch of actions for the given promise pointed by
    /// `promise_idx`.
    ///
    /// # Errors
    ///
    /// * If `promise_idx` does not correspond to an existing promise returns `InvalidPromiseIndex`.
    /// * If the promise pointed by the `promise_idx` is an ephemeral promise created by
    /// `promise_and` returns `CannotAppendActionToJointPromise`.
    /// * If called as view function returns `ProhibitedInView`.
    ///
    /// # Cost
    ///
    /// `burnt_gas := base + dispatch action fee`
    /// `used_gas := burnt_gas + exec action fee`
    pub fn promise_batch_action_create_account(&mut self, promise_idx: u64) -> Result<()> {
        self.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_batch_action_create_account".to_string(),
            }
            .into());
        }
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        self.gas_counter.pay_action_base(
            &self.fees_config.action_creation_config.create_account_cost,
            sir,
            ActionCosts::create_account,
        )?;

        self.receipt_manager.append_action_create_account(receipt_idx)?;
        Ok(())
    }

    /// Appends `DeployContract` action to the batch of actions for the given promise pointed by
    /// `promise_idx`.
    ///
    /// # Errors
    ///
    /// * If `promise_idx` does not correspond to an existing promise returns `InvalidPromiseIndex`.
    /// * If the promise pointed by the `promise_idx` is an ephemeral promise created by
    /// `promise_and` returns `CannotAppendActionToJointPromise`.
    /// * If `code_len + code_ptr` points outside the memory of the guest or host returns
    /// `MemoryAccessViolation`.
    /// * If called as view function returns `ProhibitedInView`.
    /// * If the contract code length exceeds `max_contract_size` returns `ContractSizeExceeded`.
    ///
    /// # Cost
    ///
    /// `burnt_gas := base + dispatch action base fee + dispatch action per byte fee * num bytes + cost of reading vector from memory `
    /// `used_gas := burnt_gas + exec action base fee + exec action per byte fee * num bytes`
    pub fn promise_batch_action_deploy_contract(
        &mut self,
        promise_idx: u64,
        code_len: u64,
        code_ptr: u64,
    ) -> Result<()> {
        self.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_batch_action_deploy_contract".to_string(),
            }
            .into());
        }
        let code = self.get_vec_from_memory_or_register(code_ptr, code_len)?;
        if code.len() as u64 > self.config.limit_config.max_contract_size {
            return Err(HostError::ContractSizeExceeded {
                size: code.len() as u64,
                limit: self.config.limit_config.max_contract_size,
            }
            .into());
        }

        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        let num_bytes = code.len() as u64;
        self.gas_counter.pay_action_base(
            &self.fees_config.action_creation_config.deploy_contract_cost,
            sir,
            ActionCosts::deploy_contract,
        )?;
        self.gas_counter.pay_action_per_byte(
            &self.fees_config.action_creation_config.deploy_contract_cost_per_byte,
            num_bytes,
            sir,
            ActionCosts::deploy_contract,
        )?;

        self.receipt_manager.append_action_deploy_contract(receipt_idx, code)?;
        Ok(())
    }

    /// Appends `FunctionCall` action to the batch of actions for the given promise pointed by
    /// `promise_idx`.
    ///
    /// # Errors
    ///
    /// * If `promise_idx` does not correspond to an existing promise returns `InvalidPromiseIndex`.
    /// * If the promise pointed by the `promise_idx` is an ephemeral promise created by
    /// `promise_and` returns `CannotAppendActionToJointPromise`.
    /// * If `method_name_len + method_name_ptr` or `arguments_len + arguments_ptr` or
    /// `amount_ptr + 16` points outside the memory of the guest or host returns
    /// `MemoryAccessViolation`.
    /// * If called as view function returns `ProhibitedInView`.
    ///
    /// # Cost
    ///
    /// `burnt_gas := base + dispatch action base fee + dispatch action per byte fee * num bytes + cost of reading vector from memory
    ///  + cost of reading u128, method_name and arguments from the memory`
    /// `used_gas := burnt_gas + exec action base fee + exec action per byte fee * num bytes`
    pub fn promise_batch_action_function_call(
        &mut self,
        promise_idx: u64,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        amount_ptr: u64,
        gas: Gas,
    ) -> Result<()> {
        self.promise_batch_action_function_call_weight(
            promise_idx,
            method_name_len,
            method_name_ptr,
            arguments_len,
            arguments_ptr,
            amount_ptr,
            gas,
            0,
        )
    }

    /// Appends `FunctionCall` action to the batch of actions for the given promise pointed by
    /// `promise_idx`. This function allows not specifying a specific gas value and allowing the
    /// runtime to assign remaining gas based on a weight.
    ///
    /// # Gas
    ///
    /// Gas can be specified using a static amount, a weight of remaining prepaid gas, or a mixture
    /// of both. To omit a static gas amount, `0` can be passed for the `gas` parameter.
    /// To omit assigning remaining gas, `0` can be passed as the `gas_weight` parameter.
    ///
    /// The gas weight parameter works as the following:
    ///
    /// All unused prepaid gas from the current function call is split among all function calls
    /// which supply this gas weight. The amount attached to each respective call depends on the
    /// value of the weight.
    ///
    /// For example, if 40 gas is leftover from the current method call and three functions specify
    /// the weights 1, 5, 2 then 5, 25, 10 gas will be added to each function call respectively,
    /// using up all remaining available gas.
    ///
    /// If the `gas_weight` parameter is set as a large value, the amount of distributed gas
    /// to each action can be 0 or a very low value because the amount of gas per weight is
    /// based on the floor division of the amount of gas by the sum of weights.
    ///
    /// Any remaining gas will be distributed to the last scheduled function call with a weight
    /// specified.
    ///
    /// # Errors
    ///
    /// * If `promise_idx` does not correspond to an existing promise returns `InvalidPromiseIndex`.
    /// * If the promise pointed by the `promise_idx` is an ephemeral promise created by
    /// `promise_and` returns `CannotAppendActionToJointPromise`.
    /// * If `method_name_len + method_name_ptr` or `arguments_len + arguments_ptr` or
    /// `amount_ptr + 16` points outside the memory of the guest or host returns
    /// `MemoryAccessViolation`.
    /// * If called as view function returns `ProhibitedInView`.
    pub fn promise_batch_action_function_call_weight(
        &mut self,
        promise_idx: u64,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        amount_ptr: u64,
        gas: Gas,
        gas_weight: u64,
    ) -> Result<()> {
        self.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_batch_action_function_call".to_string(),
            }
            .into());
        }
        let amount = self.memory_get_u128(amount_ptr)?;
        let method_name = self.get_vec_from_memory_or_register(method_name_ptr, method_name_len)?;
        if method_name.is_empty() {
            return Err(HostError::EmptyMethodName.into());
        }
        let arguments = self.get_vec_from_memory_or_register(arguments_ptr, arguments_len)?;

        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        // Input can't be large enough to overflow
        let num_bytes = method_name.len() as u64 + arguments.len() as u64;
        self.gas_counter.pay_action_base(
            &self.fees_config.action_creation_config.function_call_cost,
            sir,
            ActionCosts::function_call,
        )?;
        self.gas_counter.pay_action_per_byte(
            &self.fees_config.action_creation_config.function_call_cost_per_byte,
            num_bytes,
            sir,
            ActionCosts::function_call,
        )?;
        // Prepaid gas
        self.gas_counter.prepay_gas(gas)?;

        self.deduct_balance(amount)?;

        self.receipt_manager.append_action_function_call_weight(
            receipt_idx,
            method_name,
            arguments,
            amount,
            gas,
            GasWeight(gas_weight),
        )
    }

    /// Appends `Transfer` action to the batch of actions for the given promise pointed by
    /// `promise_idx`.
    ///
    /// # Errors
    ///
    /// * If `promise_idx` does not correspond to an existing promise returns `InvalidPromiseIndex`.
    /// * If the promise pointed by the `promise_idx` is an ephemeral promise created by
    /// `promise_and` returns `CannotAppendActionToJointPromise`.
    /// * If `amount_ptr + 16` points outside the memory of the guest or host returns
    /// `MemoryAccessViolation`.
    /// * If called as view function returns `ProhibitedInView`.
    ///
    /// # Cost
    ///
    /// `burnt_gas := base + dispatch action base fee + dispatch action per byte fee * num bytes + cost of reading u128 from memory `
    /// `used_gas := burnt_gas + exec action base fee + exec action per byte fee * num bytes`
    pub fn promise_batch_action_transfer(
        &mut self,
        promise_idx: u64,
        amount_ptr: u64,
    ) -> Result<()> {
        self.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_batch_action_transfer".to_string(),
            }
            .into());
        }
        let amount = self.memory_get_u128(amount_ptr)?;

        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;
        let receiver_id = self.get_account_by_receipt(receipt_idx);
        let is_receiver_implicit =
            is_implicit_account_creation_enabled(self.current_protocol_version)
                && receiver_id.is_implicit();

        let send_fee =
            transfer_send_fee(&self.fees_config.action_creation_config, sir, is_receiver_implicit);
        let exec_fee =
            transfer_exec_fee(&self.fees_config.action_creation_config, is_receiver_implicit);
        let burn_gas = send_fee;
        let use_gas = burn_gas.checked_add(exec_fee).ok_or(HostError::IntegerOverflow)?;
        self.gas_counter.pay_action_accumulated(burn_gas, use_gas, ActionCosts::transfer)?;

        self.deduct_balance(amount)?;

        self.receipt_manager.append_action_transfer(receipt_idx, amount)?;
        Ok(())
    }

    /// Appends `Stake` action to the batch of actions for the given promise pointed by
    /// `promise_idx`.
    ///
    /// # Errors
    ///
    /// * If `promise_idx` does not correspond to an existing promise returns `InvalidPromiseIndex`.
    /// * If the promise pointed by the `promise_idx` is an ephemeral promise created by
    /// `promise_and` returns `CannotAppendActionToJointPromise`.
    /// * If the given public key is not a valid (e.g. wrong length) returns `InvalidPublicKey`.
    /// * If `amount_ptr + 16` or `public_key_len + public_key_ptr` points outside the memory of the
    /// guest or host returns `MemoryAccessViolation`.
    /// * If called as view function returns `ProhibitedInView`.
    ///
    /// # Cost
    ///
    /// `burnt_gas := base + dispatch action base fee + dispatch action per byte fee * num bytes + cost of reading public key from memory `
    /// `used_gas := burnt_gas + exec action base fee + exec action per byte fee * num bytes`
    pub fn promise_batch_action_stake(
        &mut self,
        promise_idx: u64,
        amount_ptr: u64,
        public_key_len: u64,
        public_key_ptr: u64,
    ) -> Result<()> {
        self.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_batch_action_stake".to_string(),
            }
            .into());
        }
        let amount = self.memory_get_u128(amount_ptr)?;
        let public_key = self.get_vec_from_memory_or_register(public_key_ptr, public_key_len)?;

        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        self.gas_counter.pay_action_base(
            &self.fees_config.action_creation_config.stake_cost,
            sir,
            ActionCosts::stake,
        )?;

        self.receipt_manager.append_action_stake(receipt_idx, amount, public_key)?;
        Ok(())
    }

    /// Appends `AddKey` action to the batch of actions for the given promise pointed by
    /// `promise_idx`. The access key will have `FullAccess` permission.
    ///
    /// # Errors
    ///
    /// * If `promise_idx` does not correspond to an existing promise returns `InvalidPromiseIndex`.
    /// * If the promise pointed by the `promise_idx` is an ephemeral promise created by
    /// `promise_and` returns `CannotAppendActionToJointPromise`.
    /// * If the given public key is not a valid (e.g. wrong length) returns `InvalidPublicKey`.
    /// * If `public_key_len + public_key_ptr` points outside the memory of the guest or host
    /// returns `MemoryAccessViolation`.
    /// * If called as view function returns `ProhibitedInView`.
    ///
    /// # Cost
    ///
    /// `burnt_gas := base + dispatch action base fee + dispatch action per byte fee * num bytes + cost of reading public key from memory `
    /// `used_gas := burnt_gas + exec action base fee + exec action per byte fee * num bytes`
    pub fn promise_batch_action_add_key_with_full_access(
        &mut self,
        promise_idx: u64,
        public_key_len: u64,
        public_key_ptr: u64,
        nonce: u64,
    ) -> Result<()> {
        self.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_batch_action_add_key_with_full_access".to_string(),
            }
            .into());
        }
        let public_key = self.get_vec_from_memory_or_register(public_key_ptr, public_key_len)?;

        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        self.gas_counter.pay_action_base(
            &self.fees_config.action_creation_config.add_key_cost.full_access_cost,
            sir,
            ActionCosts::add_key,
        )?;

        self.receipt_manager.append_action_add_key_with_full_access(
            receipt_idx,
            public_key,
            nonce,
        )?;
        Ok(())
    }

    /// Appends `AddKey` action to the batch of actions for the given promise pointed by
    /// `promise_idx`. The access key will have `FunctionCall` permission.
    ///
    /// # Errors
    ///
    /// * If `promise_idx` does not correspond to an existing promise returns `InvalidPromiseIndex`.
    /// * If the promise pointed by the `promise_idx` is an ephemeral promise created by
    /// `promise_and` returns `CannotAppendActionToJointPromise`.
    /// * If the given public key is not a valid (e.g. wrong length) returns `InvalidPublicKey`.
    /// * If `public_key_len + public_key_ptr`, `allowance_ptr + 16`,
    /// `receiver_id_len + receiver_id_ptr` or `method_names_len + method_names_ptr` points outside
    /// the memory of the guest or host returns `MemoryAccessViolation`.
    /// * If called as view function returns `ProhibitedInView`.
    ///
    /// # Cost
    ///
    /// `burnt_gas := base + dispatch action base fee + dispatch action per byte fee * num bytes + cost of reading vector from memory
    ///  + cost of reading u128, method_names and public key from the memory + cost of reading and parsing account name`
    /// `used_gas := burnt_gas + exec action base fee + exec action per byte fee * num bytes`
    pub fn promise_batch_action_add_key_with_function_call(
        &mut self,
        promise_idx: u64,
        public_key_len: u64,
        public_key_ptr: u64,
        nonce: u64,
        allowance_ptr: u64,
        receiver_id_len: u64,
        receiver_id_ptr: u64,
        method_names_len: u64,
        method_names_ptr: u64,
    ) -> Result<()> {
        self.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_batch_action_add_key_with_function_call".to_string(),
            }
            .into());
        }
        let public_key = self.get_vec_from_memory_or_register(public_key_ptr, public_key_len)?;
        let allowance = self.memory_get_u128(allowance_ptr)?;
        let allowance = if allowance > 0 { Some(allowance) } else { None };
        let receiver_id = self.read_and_parse_account_id(receiver_id_ptr, receiver_id_len)?;
        let raw_method_names =
            self.get_vec_from_memory_or_register(method_names_ptr, method_names_len)?;
        let method_names = split_method_names(&raw_method_names)?;

        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        // +1 is to account for null-terminating characters.
        let num_bytes = method_names.iter().map(|v| v.len() as u64 + 1).sum::<u64>();
        self.gas_counter.pay_action_base(
            &self.fees_config.action_creation_config.add_key_cost.function_call_cost,
            sir,
            ActionCosts::add_key,
        )?;
        self.gas_counter.pay_action_per_byte(
            &self.fees_config.action_creation_config.add_key_cost.function_call_cost_per_byte,
            num_bytes,
            sir,
            ActionCosts::add_key,
        )?;

        self.receipt_manager.append_action_add_key_with_function_call(
            receipt_idx,
            public_key,
            nonce,
            allowance,
            receiver_id,
            method_names,
        )?;
        Ok(())
    }

    /// Appends `DeleteKey` action to the batch of actions for the given promise pointed by
    /// `promise_idx`.
    ///
    /// # Errors
    ///
    /// * If `promise_idx` does not correspond to an existing promise returns `InvalidPromiseIndex`.
    /// * If the promise pointed by the `promise_idx` is an ephemeral promise created by
    /// `promise_and` returns `CannotAppendActionToJointPromise`.
    /// * If the given public key is not a valid (e.g. wrong length) returns `InvalidPublicKey`.
    /// * If `public_key_len + public_key_ptr` points outside the memory of the guest or host
    /// returns `MemoryAccessViolation`.
    /// * If called as view function returns `ProhibitedInView`.
    ///
    /// # Cost
    ///
    /// `burnt_gas := base + dispatch action base fee + dispatch action per byte fee * num bytes + cost of reading public key from memory `
    /// `used_gas := burnt_gas + exec action base fee + exec action per byte fee * num bytes`
    pub fn promise_batch_action_delete_key(
        &mut self,
        promise_idx: u64,
        public_key_len: u64,
        public_key_ptr: u64,
    ) -> Result<()> {
        self.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_batch_action_delete_key".to_string(),
            }
            .into());
        }
        let public_key = self.get_vec_from_memory_or_register(public_key_ptr, public_key_len)?;

        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        self.gas_counter.pay_action_base(
            &self.fees_config.action_creation_config.delete_key_cost,
            sir,
            ActionCosts::delete_key,
        )?;

        self.receipt_manager.append_action_delete_key(receipt_idx, public_key)?;
        Ok(())
    }

    /// Appends `DeleteAccount` action to the batch of actions for the given promise pointed by
    /// `promise_idx`.
    ///
    /// # Errors
    ///
    /// * If `promise_idx` does not correspond to an existing promise returns `InvalidPromiseIndex`.
    /// * If the promise pointed by the `promise_idx` is an ephemeral promise created by
    /// `promise_and` returns `CannotAppendActionToJointPromise`.
    /// * If `beneficiary_id_len + beneficiary_id_ptr` points outside the memory of the guest or
    /// host returns `MemoryAccessViolation`.
    /// * If called as view function returns `ProhibitedInView`.
    ///
    /// # Cost
    ///
    /// `burnt_gas := base + dispatch action base fee + dispatch action per byte fee * num bytes + cost of reading and parsing account id from memory `
    /// `used_gas := burnt_gas + exec action base fee + exec action per byte fee * num bytes + fees for transferring funds to the beneficiary`
    pub fn promise_batch_action_delete_account(
        &mut self,
        promise_idx: u64,
        beneficiary_id_len: u64,
        beneficiary_id_ptr: u64,
    ) -> Result<()> {
        self.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_batch_action_delete_account".to_string(),
            }
            .into());
        }
        let beneficiary_id =
            self.read_and_parse_account_id(beneficiary_id_ptr, beneficiary_id_len)?;

        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;
        self.gas_counter.pay_action_base(
            &self.fees_config.action_creation_config.delete_account_cost,
            sir,
            ActionCosts::delete_account,
        )?;

        self.receipt_manager.append_action_delete_account(receipt_idx, beneficiary_id)?;
        Ok(())
    }

    /// If the current function is invoked by a callback we can access the execution results of the
    /// promises that caused the callback. This function returns the number of complete and
    /// incomplete callbacks.
    ///
    /// Note, we are only going to have incomplete callbacks once we have promise_or combinator.
    ///
    ///
    /// * If there is only one callback returns `1`;
    /// * If there are multiple callbacks (e.g. created through `promise_and`) returns their number;
    /// * If the function was called not through the callback returns `0`.
    ///
    /// # Cost
    ///
    /// `base`
    pub fn promise_results_count(&mut self) -> Result<u64> {
        self.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_results_count".to_string(),
            }
            .into());
        }
        Ok(self.promise_results.len() as _)
    }

    /// If the current function is invoked by a callback we can access the execution results of the
    /// promises that caused the callback. This function returns the result in blob format and
    /// places it into the register.
    ///
    /// * If promise result is complete and successful copies its blob into the register;
    /// * If promise result is complete and failed or incomplete keeps register unused;
    ///
    /// # Returns
    ///
    /// * If promise result is not complete returns `0`;
    /// * If promise result is complete and successful returns `1`;
    /// * If promise result is complete and failed returns `2`.
    ///
    /// # Errors
    ///
    /// * If `result_id` does not correspond to an existing result returns `InvalidPromiseResultIndex`;
    /// * If copying the blob exhausts the memory limit it returns `MemoryAccessViolation`.
    /// * If called as view function returns `ProhibitedInView`.
    ///
    /// # Cost
    ///
    /// `base + cost of writing data into a register`
    pub fn promise_result(&mut self, result_idx: u64, register_id: u64) -> Result<u64> {
        self.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(
                HostError::ProhibitedInView { method_name: "promise_result".to_string() }.into()
            );
        }
        match self
            .promise_results
            .get(result_idx as usize)
            .ok_or(HostError::InvalidPromiseResultIndex { result_idx })?
        {
            PromiseResult::NotReady => Ok(0),
            PromiseResult::Successful(data) => {
                self.internal_write_register(register_id, data.clone())?;
                Ok(1)
            }
            PromiseResult::Failed => Ok(2),
        }
    }

    /// When promise `promise_idx` finishes executing its result is considered to be the result of
    /// the current function.
    ///
    /// # Errors
    ///
    /// * If `promise_idx` does not correspond to an existing promise returns `InvalidPromiseIndex`.
    /// * If called as view function returns `ProhibitedInView`.
    ///
    /// # Cost
    ///
    /// `base + promise_return`
    pub fn promise_return(&mut self, promise_idx: u64) -> Result<()> {
        self.gas_counter.pay_base(base)?;
        self.gas_counter.pay_base(promise_return)?;
        if self.context.is_view() {
            return Err(
                HostError::ProhibitedInView { method_name: "promise_return".to_string() }.into()
            );
        }
        match self
            .promises
            .get(promise_idx as usize)
            .ok_or(HostError::InvalidPromiseIndex { promise_idx })?
        {
            Promise::Receipt(receipt_idx) => {
                self.return_data = ReturnData::ReceiptIndex(*receipt_idx);
                Ok(())
            }
            Promise::NotReceipt(_) => Err(HostError::CannotReturnJointPromise.into()),
        }
    }

    // #####################
    // # Miscellaneous API #
    // #####################

    /// Sets the blob of data as the return value of the contract.
    ///
    /// # Errors
    ///
    /// * If `value_len + value_ptr` exceeds the memory container or points to an unused register it
    ///   returns `MemoryAccessViolation`.
    /// * if the length of the returned data exceeds `max_length_returned_data` returns
    ///   `ReturnedValueLengthExceeded`.
    ///
    /// # Cost
    /// `base + cost of reading return value from memory or register + dispatch&exec cost per byte of the data sent * num data receivers`
    pub fn value_return(&mut self, value_len: u64, value_ptr: u64) -> Result<()> {
        self.gas_counter.pay_base(base)?;
        let return_val = self.get_vec_from_memory_or_register(value_ptr, value_len)?;
        let mut burn_gas: Gas = 0;
        let num_bytes = return_val.len() as u64;
        if num_bytes > self.config.limit_config.max_length_returned_data {
            return Err(HostError::ReturnedValueLengthExceeded {
                length: num_bytes,
                limit: self.config.limit_config.max_length_returned_data,
            }
            .into());
        }
        let data_cfg = &self.fees_config.data_receipt_creation_config;
        for data_receiver in &self.context.output_data_receivers {
            let sir = data_receiver == &self.context.current_account_id;
            // We deduct for execution here too, because if we later have an OR combinator
            // for promises then we might have some valid data receipts that arrive too late
            // to be picked up by the execution that waits on them (because it has started
            // after it receives the first data receipt) and then we need to issue a special
            // refund in this situation. Which we avoid by just paying for execution of
            // data receipt that might not be performed.
            // The gas here is considered burnt, cause we'll prepay for it upfront.
            burn_gas = burn_gas
                .checked_add(
                    data_cfg
                        .cost_per_byte
                        .send_fee(sir)
                        .checked_add(data_cfg.cost_per_byte.exec_fee())
                        .ok_or(HostError::IntegerOverflow)?
                        .checked_mul(num_bytes)
                        .ok_or(HostError::IntegerOverflow)?,
                )
                .ok_or(HostError::IntegerOverflow)?;
        }
        self.gas_counter.pay_action_accumulated(burn_gas, burn_gas, ActionCosts::value_return)?;
        self.return_data = ReturnData::Value(return_val);
        Ok(())
    }

    /// Terminates the execution of the program with panic `GuestPanic`.
    ///
    /// # Cost
    ///
    /// `base`
    pub fn panic(&mut self) -> Result<()> {
        self.gas_counter.pay_base(base)?;
        Err(HostError::GuestPanic { panic_msg: "explicit guest panic".to_string() }.into())
    }

    /// Guest panics with the UTF-8 encoded string.
    /// If `len == u64::MAX` then treats the string as null-terminated with character `'\0'`.
    ///
    /// # Errors
    ///
    /// * If string extends outside the memory of the guest with `MemoryAccessViolation`;
    /// * If string is not UTF-8 returns `BadUtf8`.
    /// * If string is longer than `max_log_len` returns `TotalLogLengthExceeded`.
    ///
    /// # Cost
    /// `base + cost of reading and decoding a utf8 string`
    pub fn panic_utf8(&mut self, len: u64, ptr: u64) -> Result<()> {
        self.gas_counter.pay_base(base)?;
        Err(HostError::GuestPanic { panic_msg: self.get_utf8_string(len, ptr)? }.into())
    }

    /// Logs the UTF-8 encoded string.
    /// If `len == u64::MAX` then treats the string as null-terminated with character `'\0'`.
    ///
    /// # Errors
    ///
    /// * If string extends outside the memory of the guest with `MemoryAccessViolation`;
    /// * If string is not UTF-8 returns `BadUtf8`.
    /// * If number of bytes read + `total_log_length` exceeds the `max_total_log_length` returns
    ///   `TotalLogLengthExceeded`.
    /// * If the total number of logs will exceed the `max_number_logs` returns
    ///   `NumberOfLogsExceeded`.
    ///
    /// # Cost
    ///
    /// `base + log_base + log_byte + num_bytes + utf8 decoding cost`
    pub fn log_utf8(&mut self, len: u64, ptr: u64) -> Result<()> {
        self.gas_counter.pay_base(base)?;
        self.check_can_add_a_log_message()?;
        let message = self.get_utf8_string(len, ptr)?;
        self.gas_counter.pay_base(log_base)?;
        self.gas_counter.pay_per(log_byte, message.len() as u64)?;
        self.checked_push_log(message)
    }

    /// Logs the UTF-16 encoded string. If `len == u64::MAX` then treats the string as
    /// null-terminated with two-byte sequence of `0x00 0x00`.
    ///
    /// # Errors
    ///
    /// * If string extends outside the memory of the guest with `MemoryAccessViolation`;
    /// * If string is not UTF-16 returns `BadUtf16`.
    /// * If number of bytes read + `total_log_length` exceeds the `max_total_log_length` returns
    ///   `TotalLogLengthExceeded`.
    /// * If the total number of logs will exceed the `max_number_logs` returns
    ///   `NumberOfLogsExceeded`.
    ///
    /// # Cost
    ///
    /// `base + log_base + log_byte * num_bytes + utf16 decoding cost`
    pub fn log_utf16(&mut self, len: u64, ptr: u64) -> Result<()> {
        self.gas_counter.pay_base(base)?;
        self.check_can_add_a_log_message()?;
        let message = self.get_utf16_string(len, ptr)?;
        self.gas_counter.pay_base(log_base)?;
        // Let's not use `encode_utf16` for gas per byte here, since it's a lot of compute.
        self.gas_counter.pay_per(log_byte, message.len() as u64)?;
        self.checked_push_log(message)
    }

    /// Special import kept for compatibility with AssemblyScript contracts. Not called by smart
    /// contracts directly, but instead called by the code generated by AssemblyScript.
    ///
    /// # Errors
    ///
    /// * If string extends outside the memory of the guest with `MemoryAccessViolation`;
    /// * If string is not UTF-8 returns `BadUtf8`.
    /// * If number of bytes read + `total_log_length` exceeds the `max_total_log_length` returns
    ///   `TotalLogLengthExceeded`.
    /// * If the total number of logs will exceed the `max_number_logs` returns
    ///   `NumberOfLogsExceeded`.
    ///
    /// # Cost
    ///
    /// `base +  log_base + log_byte * num_bytes + utf16 decoding cost`
    pub fn abort(&mut self, msg_ptr: u32, filename_ptr: u32, line: u32, col: u32) -> Result<()> {
        self.gas_counter.pay_base(base)?;
        if msg_ptr < 4 || filename_ptr < 4 {
            return Err(HostError::BadUTF16.into());
        }
        self.check_can_add_a_log_message()?;

        // Underflow checked above.
        let msg_len = self.memory_get_u32((msg_ptr - 4) as u64)?;
        let filename_len = self.memory_get_u32((filename_ptr - 4) as u64)?;

        let msg = self.get_utf16_string(msg_len as u64, msg_ptr as u64)?;
        let filename = self.get_utf16_string(filename_len as u64, filename_ptr as u64)?;

        let message = format!("{}, filename: \"{}\" line: {} col: {}", msg, filename, line, col);
        self.gas_counter.pay_base(log_base)?;
        self.gas_counter.pay_per(log_byte, message.as_bytes().len() as u64)?;
        self.checked_push_log(format!("ABORT: {}", message))?;

        Err(HostError::GuestPanic { panic_msg: message }.into())
    }

    // ###############
    // # Storage API #
    // ###############

    /// Reads account id from the given location in memory.
    ///
    /// # Errors
    ///
    /// * If account is not UTF-8 encoded then returns `BadUtf8`;
    /// * If account is not valid then returns `InvalidAccountId`.
    ///
    /// # Cost
    ///
    /// This is a helper function that encapsulates the following costs:
    /// cost of reading buffer from register or memory,
    /// `utf8_decoding_base + utf8_decoding_byte * num_bytes`.
    fn read_and_parse_account_id(&mut self, ptr: u64, len: u64) -> Result<AccountId> {
        let buf = self.get_vec_from_memory_or_register(ptr, len)?;
        self.gas_counter.pay_base(utf8_decoding_base)?;
        self.gas_counter.pay_per(utf8_decoding_byte, buf.len() as u64)?;

        // We return an illegally constructed AccountId here for the sake of ensuring
        // backwards compatibility. For paths previously involving validation, like receipts
        // we retain validation further down the line in node-runtime/verifier.rs#fn(validate_receipt)
        // mimicing previous behaviour.
        let account_id = String::from_utf8(buf)
            .map(
                #[allow(deprecated)]
                AccountId::new_unvalidated,
            )
            .map_err(|_| HostError::BadUTF8)?;
        Ok(account_id)
    }

    /// Writes key-value into storage.
    /// * If key is not in use it inserts the key-value pair and does not modify the register. Returns `0`;
    /// * If key is in use it inserts the key-value and copies the old value into the `register_id`. Returns `1`.
    ///
    /// # Errors
    ///
    /// * If `key_len + key_ptr` or `value_len + value_ptr` exceeds the memory container or points
    ///   to an unused register it returns `MemoryAccessViolation`;
    /// * If returning the preempted value into the registers exceed the memory container it returns
    ///   `MemoryAccessViolation`.
    /// * If the length of the key exceeds `max_length_storage_key` returns `KeyLengthExceeded`.
    /// * If the length of the value exceeds `max_length_storage_value` returns
    ///   `ValueLengthExceeded`.
    /// * If called as view function returns `ProhibitedInView``.
    ///
    /// # Cost
    ///
    /// `base + storage_write_base + storage_write_key_byte * num_key_bytes + storage_write_value_byte * num_value_bytes
    /// + get_vec_from_memory_or_register_cost x 2`.
    ///
    /// If a value was evicted it costs additional `storage_write_value_evicted_byte * num_evicted_bytes + internal_write_register_cost`.
    pub fn storage_write(
        &mut self,
        key_len: u64,
        key_ptr: u64,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64> {
        self.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(
                HostError::ProhibitedInView { method_name: "storage_write".to_string() }.into()
            );
        }
        self.gas_counter.pay_base(storage_write_base)?;
        let key = self.get_vec_from_memory_or_register(key_ptr, key_len)?;
        if key.len() as u64 > self.config.limit_config.max_length_storage_key {
            return Err(HostError::KeyLengthExceeded {
                length: key.len() as u64,
                limit: self.config.limit_config.max_length_storage_key,
            }
            .into());
        }
        let value = self.get_vec_from_memory_or_register(value_ptr, value_len)?;
        if value.len() as u64 > self.config.limit_config.max_length_storage_value {
            return Err(HostError::ValueLengthExceeded {
                length: value.len() as u64,
                limit: self.config.limit_config.max_length_storage_value,
            }
            .into());
        }
        self.gas_counter.pay_per(storage_write_key_byte, key.len() as u64)?;
        self.gas_counter.pay_per(storage_write_value_byte, value.len() as u64)?;
        let nodes_before = self.ext.get_trie_nodes_count();
        let evicted_ptr = self.ext.storage_get(&key)?;
        let evicted =
            Self::deref_value(&mut self.gas_counter, storage_write_evicted_byte, evicted_ptr)?;
        let nodes_delta = self.ext.get_trie_nodes_count() - nodes_before;

        near_o11y::io_trace!(
            storage_op = "write",
            key =  %near_primitives::serialize::to_base(key.clone()),
            size = value_len,
            evicted_len = evicted.as_ref().map(Vec::len),
            tn_mem_reads = nodes_delta.mem_reads,
            tn_db_reads = nodes_delta.db_reads,
        );

        self.gas_counter.add_trie_fees(&nodes_delta)?;
        self.ext.storage_set(&key, &value)?;
        let storage_config = &self.fees_config.storage_usage_config;
        match evicted {
            Some(old_value) => {
                // Inner value can't overflow, because the value length is limited.
                self.current_storage_usage = self
                    .current_storage_usage
                    .checked_sub(old_value.len() as u64)
                    .ok_or(InconsistentStateError::IntegerOverflow)?;
                // Inner value can't overflow, because the value length is limited.
                self.current_storage_usage = self
                    .current_storage_usage
                    .checked_add(value.len() as u64)
                    .ok_or(InconsistentStateError::IntegerOverflow)?;
                self.internal_write_register(register_id, old_value)?;
                Ok(1)
            }
            None => {
                // Inner value can't overflow, because the key/value length is limited.
                self.current_storage_usage = self
                    .current_storage_usage
                    .checked_add(
                        value.len() as u64
                            + key.len() as u64
                            + storage_config.num_extra_bytes_record,
                    )
                    .ok_or(InconsistentStateError::IntegerOverflow)?;
                Ok(0)
            }
        }
    }

    fn deref_value<'s>(
        gas_counter: &mut GasCounter,
        cost_per_byte: ExtCosts,
        value_ptr: Option<Box<dyn ValuePtr + 's>>,
    ) -> Result<Option<Vec<u8>>> {
        match value_ptr {
            Some(value_ptr) => {
                gas_counter.pay_per(cost_per_byte, value_ptr.len() as u64)?;
                value_ptr.deref().map(Some)
            }
            None => Ok(None),
        }
    }

    /// Reads the value stored under the given key.
    /// * If key is used copies the content of the value into the `register_id`, even if the content
    ///   is zero bytes. Returns `1`;
    /// * If key is not present then does not modify the register. Returns `0`;
    ///
    /// # Errors
    ///
    /// * If `key_len + key_ptr` exceeds the memory container or points to an unused register it
    ///   returns `MemoryAccessViolation`;
    /// * If returning the preempted value into the registers exceed the memory container it returns
    ///   `MemoryAccessViolation`.
    /// * If the length of the key exceeds `max_length_storage_key` returns `KeyLengthExceeded`.
    ///
    /// # Cost
    ///
    /// `base + storage_read_base + storage_read_key_byte * num_key_bytes + storage_read_value_byte + num_value_bytes
    ///  cost to read key from register + cost to write value into register`.
    pub fn storage_read(&mut self, key_len: u64, key_ptr: u64, register_id: u64) -> Result<u64> {
        self.gas_counter.pay_base(base)?;
        self.gas_counter.pay_base(storage_read_base)?;
        let key = self.get_vec_from_memory_or_register(key_ptr, key_len)?;
        if key.len() as u64 > self.config.limit_config.max_length_storage_key {
            return Err(HostError::KeyLengthExceeded {
                length: key.len() as u64,
                limit: self.config.limit_config.max_length_storage_key,
            }
            .into());
        }
        self.gas_counter.pay_per(storage_read_key_byte, key.len() as u64)?;
        let nodes_before = self.ext.get_trie_nodes_count();
        let read = self.ext.storage_get(&key);
        let nodes_delta = self.ext.get_trie_nodes_count() - nodes_before;
        self.gas_counter.add_trie_fees(&nodes_delta)?;
        let read = Self::deref_value(&mut self.gas_counter, storage_read_value_byte, read?)?;

        near_o11y::io_trace!(
            storage_op = "read",
            key =  %near_primitives::serialize::to_base(key.clone()),
            size = read.as_ref().map(Vec::len),
            tn_db_reads = nodes_delta.db_reads,
            tn_mem_reads = nodes_delta.mem_reads,
        );
        match read {
            Some(value) => {
                self.internal_write_register(register_id, value)?;
                Ok(1)
            }
            None => Ok(0),
        }
    }

    /// Removes the value stored under the given key.
    /// * If key is used, removes the key-value from the trie and copies the content of the value
    ///   into the `register_id`, even if the content is zero bytes. Returns `1`;
    /// * If key is not present then does not modify the register. Returns `0`.
    ///
    /// # Errors
    ///
    /// * If `key_len + key_ptr` exceeds the memory container or points to an unused register it
    ///   returns `MemoryAccessViolation`;
    /// * If the registers exceed the memory limit returns `MemoryAccessViolation`;
    /// * If returning the preempted value into the registers exceed the memory container it returns
    ///   `MemoryAccessViolation`.
    /// * If the length of the key exceeds `max_length_storage_key` returns `KeyLengthExceeded`.
    /// * If called as view function returns `ProhibitedInView``.
    ///
    /// # Cost
    ///
    /// `base + storage_remove_base + storage_remove_key_byte * num_key_bytes + storage_remove_ret_value_byte * num_value_bytes
    /// + cost to read the key + cost to write the value`.
    pub fn storage_remove(&mut self, key_len: u64, key_ptr: u64, register_id: u64) -> Result<u64> {
        self.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(
                HostError::ProhibitedInView { method_name: "storage_remove".to_string() }.into()
            );
        }
        self.gas_counter.pay_base(storage_remove_base)?;
        let key = self.get_vec_from_memory_or_register(key_ptr, key_len)?;
        if key.len() as u64 > self.config.limit_config.max_length_storage_key {
            return Err(HostError::KeyLengthExceeded {
                length: key.len() as u64,
                limit: self.config.limit_config.max_length_storage_key,
            }
            .into());
        }
        self.gas_counter.pay_per(storage_remove_key_byte, key.len() as u64)?;
        let nodes_before = self.ext.get_trie_nodes_count();
        let removed_ptr = self.ext.storage_get(&key)?;
        let removed =
            Self::deref_value(&mut self.gas_counter, storage_remove_ret_value_byte, removed_ptr)?;

        self.ext.storage_remove(&key)?;
        let nodes_delta = self.ext.get_trie_nodes_count() - nodes_before;

        near_o11y::io_trace!(
            storage_op = "remove",
            key =  %near_primitives::serialize::to_base(key.clone()),
            evicted_len = removed.as_ref().map(Vec::len),
            tn_mem_reads = nodes_delta.mem_reads,
            tn_db_reads = nodes_delta.db_reads,
        );

        self.gas_counter.add_trie_fees(&nodes_delta)?;
        let storage_config = &self.fees_config.storage_usage_config;
        match removed {
            Some(value) => {
                // Inner value can't overflow, because the key/value length is limited.
                self.current_storage_usage = self
                    .current_storage_usage
                    .checked_sub(
                        value.len() as u64
                            + key.len() as u64
                            + storage_config.num_extra_bytes_record,
                    )
                    .ok_or(InconsistentStateError::IntegerOverflow)?;
                self.internal_write_register(register_id, value)?;
                Ok(1)
            }
            None => Ok(0),
        }
    }

    /// Checks if there is a key-value pair.
    /// * If key is used returns `1`, even if the value is zero bytes;
    /// * Otherwise returns `0`.
    ///
    /// # Errors
    ///
    /// * If `key_len + key_ptr` exceeds the memory container it returns `MemoryAccessViolation`.
    /// * If the length of the key exceeds `max_length_storage_key` returns `KeyLengthExceeded`.
    ///
    /// # Cost
    ///
    /// `base + storage_has_key_base + storage_has_key_byte * num_bytes + cost of reading key`
    pub fn storage_has_key(&mut self, key_len: u64, key_ptr: u64) -> Result<u64> {
        self.gas_counter.pay_base(base)?;
        self.gas_counter.pay_base(storage_has_key_base)?;
        let key = self.get_vec_from_memory_or_register(key_ptr, key_len)?;
        if key.len() as u64 > self.config.limit_config.max_length_storage_key {
            return Err(HostError::KeyLengthExceeded {
                length: key.len() as u64,
                limit: self.config.limit_config.max_length_storage_key,
            }
            .into());
        }
        self.gas_counter.pay_per(storage_has_key_byte, key.len() as u64)?;
        let nodes_before = self.ext.get_trie_nodes_count();
        let res = self.ext.storage_has_key(&key);
        let nodes_delta = self.ext.get_trie_nodes_count() - nodes_before;

        near_o11y::io_trace!(
            storage_op = "exists",
            key =  %near_primitives::serialize::to_base(key.clone()),
            tn_mem_reads = nodes_delta.mem_reads,
            tn_db_reads = nodes_delta.db_reads,
        );

        self.gas_counter.add_trie_fees(&nodes_delta)?;
        Ok(res? as u64)
    }

    /// Debug print given utf-8 string to node log. It's only available in Sandbox node
    ///
    /// # Errors
    ///
    /// * If string is not UTF-8 returns `BadUtf8`
    /// * If the log is over available memory in wasm runner, returns `MemoryAccessViolation`
    ///
    /// # Cost
    ///
    /// 0
    #[cfg(feature = "sandbox")]
    pub fn sandbox_debug_log(&mut self, len: u64, ptr: u64) -> Result<()> {
        let message = self.sandbox_get_utf8_string(len, ptr)?;
        tracing::debug!(target: "sandbox", message = &message[..]);
        Ok(())
    }

    /// DEPRECATED
    /// Creates an iterator object inside the host. Returns the identifier that uniquely
    /// differentiates the given iterator from other iterators that can be simultaneously created.
    /// * It iterates over the keys that have the provided prefix. The order of iteration is defined
    ///   by the lexicographic order of the bytes in the keys;
    /// * If there are no keys, it creates an empty iterator, see below on empty iterators.
    ///
    /// # Errors
    ///
    /// * If `prefix_len + prefix_ptr` exceeds the memory container it returns
    ///   `MemoryAccessViolation`.
    /// * If the length of the prefix exceeds `max_length_storage_key` returns `KeyLengthExceeded`.
    ///
    /// # Cost
    ///
    /// `base + storage_iter_create_prefix_base + storage_iter_create_key_byte * num_prefix_bytes
    ///  cost of reading the prefix`.
    pub fn storage_iter_prefix(&mut self, _prefix_len: u64, _prefix_ptr: u64) -> Result<u64> {
        Err(VMLogicError::HostError(HostError::Deprecated {
            method_name: "storage_iter_prefix".to_string(),
        }))
    }

    /// DEPRECATED
    /// Iterates over all key-values such that keys are between `start` and `end`, where `start` is
    /// inclusive and `end` is exclusive. Unless lexicographically `start < end`, it creates an
    /// empty iterator. Note, this definition allows for `start` or `end` keys to not actually exist
    /// on the given trie.
    ///
    /// # Errors
    ///
    /// * If `start_len + start_ptr` or `end_len + end_ptr` exceeds the memory container or points to
    ///   an unused register it returns `MemoryAccessViolation`.
    /// * If the length of the `start` exceeds `max_length_storage_key` returns `KeyLengthExceeded`.
    /// * If the length of the `end` exceeds `max_length_storage_key` returns `KeyLengthExceeded`.
    ///
    /// # Cost
    ///
    /// `base + storage_iter_create_range_base + storage_iter_create_from_byte * num_from_bytes
    ///  + storage_iter_create_to_byte * num_to_bytes + reading from prefix + reading to prefix`.
    pub fn storage_iter_range(
        &mut self,
        _start_len: u64,
        _start_ptr: u64,
        _end_len: u64,
        _end_ptr: u64,
    ) -> Result<u64> {
        Err(VMLogicError::HostError(HostError::Deprecated {
            method_name: "storage_iter_range".to_string(),
        }))
    }

    /// DEPRECATED
    /// Advances iterator and saves the next key and value in the register.
    /// * If iterator is not empty (after calling next it points to a key-value), copies the key
    ///   into `key_register_id` and value into `value_register_id` and returns `1`;
    /// * If iterator is empty returns `0`;
    /// This allows us to iterate over the keys that have zero bytes stored in values.
    ///
    /// # Errors
    ///
    /// * If `key_register_id == value_register_id` returns `MemoryAccessViolation`;
    /// * If the registers exceed the memory limit returns `MemoryAccessViolation`;
    /// * If `iterator_id` does not correspond to an existing iterator returns `InvalidIteratorId`;
    /// * If between the creation of the iterator and calling `storage_iter_next` the range over
    ///   which it iterates was modified returns `IteratorWasInvalidated`. Specifically, if
    ///   `storage_write` or `storage_remove` was invoked on the key key such that:
    ///   * in case of `storage_iter_prefix`. `key` has the given prefix and:
    ///     * Iterator was not called next yet.
    ///     * `next` was already called on the iterator and it is currently pointing at the `key`
    ///       `curr` such that `curr <= key`.
    ///   * in case of `storage_iter_range`. `start<=key<end` and:
    ///     * Iterator was not called `next` yet.
    ///     * `next` was already called on the iterator and it is currently pointing at the key
    ///       `curr` such that `curr<=key<end`.
    ///
    /// # Cost
    ///
    /// `base + storage_iter_next_base + storage_iter_next_key_byte * num_key_bytes + storage_iter_next_value_byte * num_value_bytes
    ///  + writing key to register + writing value to register`.
    pub fn storage_iter_next(
        &mut self,
        _iterator_id: u64,
        _key_register_id: u64,
        _value_register_id: u64,
    ) -> Result<u64> {
        Err(VMLogicError::HostError(HostError::Deprecated {
            method_name: "storage_iter_next".to_string(),
        }))
    }

    /// Computes the outcome of the execution.
    ///
    /// If `FunctionCallWeight` protocol feature (127) is enabled, unused gas will be
    /// distributed to functions that specify a gas weight. If there are no functions with
    /// a gas weight, the outcome will contain unused gas as usual.
    pub fn compute_outcome_and_distribute_gas(mut self) -> VMOutcome {
        if !self.context.is_view() {
            // Distribute unused gas to scheduled function calls
            let unused_gas = self.gas_counter.unused_gas();

            // Spend all remaining gas by distributing it among function calls that specify
            // a gas weight
            if let GasDistribution::All = self.receipt_manager.distribute_unused_gas(unused_gas) {
                self.gas_counter.prepay_gas(unused_gas).unwrap();
            }
        }

        let burnt_gas = self.gas_counter.burnt_gas();
        let used_gas = self.gas_counter.used_gas();

        let mut profile = self.gas_counter.profile_data();
        profile.compute_wasm_instruction_cost(burnt_gas);

        VMOutcome {
            balance: self.current_account_balance,
            storage_usage: self.current_storage_usage,
            return_data: self.return_data,
            burnt_gas,
            used_gas,
            logs: self.logs,
            profile,
            action_receipts: self.receipt_manager.action_receipts,
        }
    }

    /// Add a cost for loading the contract code in the VM.
    ///
    /// This cost does not consider the structure of the contract code, only the
    /// size. This is currently the only loading fee. A fee that takes the code
    /// structure into consideration could be added. But since that would have
    /// to happen after loading, we cannot pre-charge it. This is the main
    /// motivation to (only) have this simple fee.
    pub fn add_contract_loading_fee(&mut self, code_len: u64) -> Result<()> {
        self.gas_counter.pay_per(contract_loading_bytes, code_len)?;
        self.gas_counter.pay_base(contract_loading_base)
    }

    /// Gets pointer to the fast gas counter.
    pub fn gas_counter_pointer(&mut self) -> *mut FastGasCounter {
        self.gas_counter.gas_counter_raw_ptr()
    }

    /// Properly handles gas limit exceeded error.
    pub fn process_gas_limit(&mut self) -> HostError {
        let new_burn_gas = self.gas_counter.burnt_gas();
        let new_used_gas = self.gas_counter.used_gas();
        self.gas_counter.process_gas_limit(new_burn_gas, new_used_gas)
    }

    /// VM independent setup before loading the executable.
    ///
    /// Does VM independent checks that happen after the instantiation of
    /// VMLogic but before loading the executable. This includes pre-charging gas
    /// costs for loading the executable, which depends on the size of the WASM code.
    pub fn before_loading_executable(
        &mut self,
        method_name: &str,
        current_protocol_version: u32,
        wasm_code_bytes: usize,
    ) -> std::result::Result<(), VMError> {
        if method_name.is_empty() {
            let error =
                VMError::FunctionCallError(near_vm_errors::FunctionCallError::MethodResolveError(
                    near_vm_errors::MethodResolveError::MethodEmptyName,
                ));
            return Err(error);
        }
        if checked_feature!("stable", FixContractLoadingCost, current_protocol_version) {
            if self.add_contract_loading_fee(wasm_code_bytes as u64).is_err() {
                let error =
                    VMError::FunctionCallError(near_vm_errors::FunctionCallError::HostError(
                        near_vm_errors::HostError::GasExceeded,
                    ));
                return Err(error);
            }
        }
        Ok(())
    }

    /// Legacy code to preserve old gas charging behaviour in old protocol versions.
    pub fn after_loading_executable(
        &mut self,
        current_protocol_version: u32,
        wasm_code_bytes: usize,
    ) -> std::result::Result<(), VMError> {
        if !checked_feature!("stable", FixContractLoadingCost, current_protocol_version) {
            if self.add_contract_loading_fee(wasm_code_bytes as u64).is_err() {
                return Err(VMError::FunctionCallError(
                    near_vm_errors::FunctionCallError::HostError(
                        near_vm_errors::HostError::GasExceeded,
                    ),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq)]
pub struct VMOutcome {
    pub balance: Balance,
    pub storage_usage: StorageUsage,
    pub return_data: ReturnData,
    pub burnt_gas: Gas,
    pub used_gas: Gas,
    pub logs: Vec<String>,
    /// Data collected from making a contract call
    pub profile: ProfileData,
    pub action_receipts: Vec<(AccountId, ReceiptMetadata)>,
}

impl std::fmt::Debug for VMOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let return_data_str = match &self.return_data {
            ReturnData::None => "None".to_string(),
            ReturnData::ReceiptIndex(_) => "Receipt".to_string(),
            ReturnData::Value(v) => format!("Value [{} bytes]", v.len()),
        };
        write!(
            f,
            "VMOutcome: balance {} storage_usage {} return data {} burnt gas {} used gas {}",
            self.balance, self.storage_usage, return_data_str, self.burnt_gas, self.used_gas
        )
    }
}

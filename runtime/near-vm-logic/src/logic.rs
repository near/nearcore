use crate::config::ActionCosts;
use crate::config::ExtCosts::*;
use crate::config::VMConfig;
use crate::context::VMContext;
use crate::dependencies::{External, MemoryLike};
use crate::gas_counter::GasCounter;
use crate::types::{
    AccountId, Balance, EpochHeight, Gas, ProfileData, PromiseIndex, PromiseResult, ReceiptIndex,
    ReturnData, StorageUsage,
};
use crate::utils::split_method_names;
use crate::{ExtCosts, HostError, VMLogicError, ValuePtr};
use byteorder::ByteOrder;
use near_runtime_fees::RuntimeFeesConfig;
use near_vm_errors::InconsistentStateError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::mem::size_of;

type Result<T> = ::std::result::Result<T, VMLogicError>;

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
    /// Record the accounts towards which the receipts are directed.
    receipt_to_account: HashMap<ReceiptIndex, AccountId>,

    /// Tracks the total log length. The sum of length of all logs.
    total_log_length: u64,
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
    pub fn new(
        ext: &'a mut dyn External,
        context: VMContext,
        config: &'a VMConfig,
        fees_config: &'a RuntimeFeesConfig,
        promise_results: &'a [PromiseResult],
        memory: &'a mut dyn MemoryLike,
        profile: Option<ProfileData>,
    ) -> Self {
        ext.reset_touched_nodes_counter();
        // Overflow should be checked before calling VMLogic.
        let current_account_balance = context.account_balance + context.attached_deposit;
        let current_storage_usage = context.storage_usage;
        let max_gas_burnt = if context.is_view {
            config.limit_config.max_gas_burnt_view
        } else {
            config.limit_config.max_gas_burnt
        };
        let current_account_locked_balance = context.account_locked_balance;
        let gas_counter = GasCounter::new(
            config.ext_costs.clone(),
            max_gas_burnt,
            context.prepaid_gas,
            context.is_view,
            profile,
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
            receipt_to_account: HashMap::new(),
            total_log_length: 0,
        }
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
        self.gas_counter.pay_per_byte(read_memory_byte, buf.len() as _)?;
        self.try_fit_mem(offset, buf.len() as _)?;
        self.memory.read_memory(offset, buf);
        Ok(())
    }

    fn memory_get_vec(&mut self, offset: u64, len: u64) -> Result<Vec<u8>> {
        self.gas_counter.pay_base(read_memory_base)?;
        self.gas_counter.pay_per_byte(read_memory_byte, len)?;
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
        if len != std::u64::MAX {
            self.memory_get_vec(offset, len)
        } else {
            self.internal_read_register(offset)
        }
    }

    fn memory_set_slice(&mut self, offset: u64, buf: &[u8]) -> Result<()> {
        self.gas_counter.pay_base(write_memory_base)?;
        self.gas_counter.pay_per_byte(write_memory_byte, buf.len() as _)?;
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
            self.gas_counter.pay_per_byte(read_register_byte, data.len() as _)?;
            Ok(data.clone())
        } else {
            Err(HostError::InvalidRegisterId { register_id }.into())
        }
    }

    fn internal_write_register(&mut self, register_id: u64, data: Vec<u8>) -> Result<()> {
        self.gas_counter.pay_base(write_register_base)?;
        self.gas_counter.pay_per_byte(write_register_byte, data.len() as u64)?;
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
        Ok(self.registers.get(&register_id).map(|r| r.len() as _).unwrap_or(std::u64::MAX))
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
        if len != std::u64::MAX {
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
        self.gas_counter.pay_per_byte(utf8_decoding_byte, buf.len() as _)?;
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
        if len != std::u64::MAX {
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
            .pay_per_byte(utf16_decoding_byte, u16_buffer.len() as u64 * size_of::<u16>() as u64)?;
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
            self.context.current_account_id.as_bytes().to_vec(),
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

        if self.context.is_view {
            return Err(HostError::ProhibitedInView {
                method_name: "signer_account_id".to_string(),
            }
            .into());
        }
        self.internal_write_register(
            register_id,
            self.context.signer_account_id.as_bytes().to_vec(),
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

        if self.context.is_view {
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

        if self.context.is_view {
            return Err(HostError::ProhibitedInView {
                method_name: "predecessor_account_id".to_string(),
            }
            .into());
        }
        self.internal_write_register(
            register_id,
            self.context.predecessor_account_id.as_bytes().to_vec(),
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

        if self.context.is_view {
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
        if self.context.is_view {
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
        if self.context.is_view {
            return Err(HostError::ProhibitedInView { method_name: "used_gas".to_string() }.into());
        }
        Ok(self.gas_counter.used_gas())
    }

    // ############
    // # Math API #
    // ############

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
        self.gas_counter.pay_per_byte(sha256_byte, value.len() as u64)?;

        use sha2::Digest;

        let value_hash = sha2::Sha256::digest(&value);
        self.internal_write_register(register_id, value_hash.as_ref().to_vec())
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
        self.gas_counter.pay_per_byte(keccak256_byte, value.len() as u64)?;

        use sha3::Digest;

        let value_hash = sha3::Keccak256::digest(&value);
        self.internal_write_register(register_id, value_hash.as_ref().to_vec())
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
        self.gas_counter.pay_per_byte(keccak512_byte, value.len() as u64)?;

        use sha3::Digest;

        let value_hash = sha3::Keccak512::digest(&value);
        self.internal_write_register(register_id, value_hash.as_ref().to_vec())
    }

    /// Called by gas metering injected into Wasm. Counts both towards `burnt_gas` and `used_gas`.
    ///
    /// # Errors
    ///
    /// * If passed gas amount somehow overflows internal gas counters returns `IntegerOverflow`;
    /// * If we exceed usage limit imposed on burnt gas returns `GasLimitExceeded`;
    /// * If we exceed the `prepaid_gas` then returns `GasExceeded`.
    pub fn gas(&mut self, gas_amount: u32) -> Result<()> {
        let value = Gas::from(gas_amount) * Gas::from(self.config.regular_op_cost);
        self.gas_counter.pay_wasm_gas(value)
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
        if self.context.is_view {
            return Err(
                HostError::ProhibitedInView { method_name: "promise_and".to_string() }.into()
            );
        }
        self.gas_counter.pay_base(promise_and_base)?;
        self.gas_counter.pay_per_byte(
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
        if self.context.is_view {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_batch_create".to_string(),
            }
            .into());
        }
        let account_id = self.read_and_parse_account_id(account_id_ptr, account_id_len)?;
        let sir = account_id == self.context.current_account_id;
        self.pay_gas_for_new_receipt(sir, &[])?;
        let new_receipt_idx = self.ext.create_receipt(vec![], account_id.clone())?;
        self.receipt_to_account.insert(new_receipt_idx, account_id);

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
        if self.context.is_view {
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
            .map(|receipt_idx| {
                self.receipt_to_account
                    .get(receipt_idx)
                    .expect("promises and receipt_to_account should be consistent.")
                    == &account_id
            })
            .collect();
        self.pay_gas_for_new_receipt(sir, &deps)?;

        let new_receipt_idx = self.ext.create_receipt(receipt_dependencies, account_id.clone())?;
        self.receipt_to_account.insert(new_receipt_idx, account_id);

        self.checked_push_promise(Promise::Receipt(new_receipt_idx))
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

        let account_id = self
            .receipt_to_account
            .get(&receipt_idx)
            .expect("promises and receipt_to_account should be consistent.");
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
        if self.context.is_view {
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

        self.ext.append_action_create_account(receipt_idx)?;
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
        if self.context.is_view {
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

        self.ext.append_action_deploy_contract(receipt_idx, code)?;
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
        self.gas_counter.pay_base(base)?;
        if self.context.is_view {
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

        self.ext.append_action_function_call(receipt_idx, method_name, arguments, amount, gas)?;
        Ok(())
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
        if self.context.is_view {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_batch_action_transfer".to_string(),
            }
            .into());
        }
        let amount = self.memory_get_u128(amount_ptr)?;

        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        self.gas_counter.pay_action_base(
            &self.fees_config.action_creation_config.transfer_cost,
            sir,
            ActionCosts::transfer,
        )?;

        self.deduct_balance(amount)?;

        self.ext.append_action_transfer(receipt_idx, amount)?;
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
        if self.context.is_view {
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

        self.ext.append_action_stake(receipt_idx, amount, public_key)?;
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
        if self.context.is_view {
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

        self.ext.append_action_add_key_with_full_access(receipt_idx, public_key, nonce)?;
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
        if self.context.is_view {
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
            ActionCosts::function_call,
        )?;
        self.gas_counter.pay_action_per_byte(
            &self.fees_config.action_creation_config.add_key_cost.function_call_cost_per_byte,
            num_bytes,
            sir,
            ActionCosts::function_call,
        )?;

        self.ext.append_action_add_key_with_function_call(
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
        if self.context.is_view {
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

        self.ext.append_action_delete_key(receipt_idx, public_key)?;
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
    /// `used_gas := burnt_gas + exec action base fee + exec action per byte fee * num bytes`
    pub fn promise_batch_action_delete_account(
        &mut self,
        promise_idx: u64,
        beneficiary_id_len: u64,
        beneficiary_id_ptr: u64,
    ) -> Result<()> {
        self.gas_counter.pay_base(base)?;
        if self.context.is_view {
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

        self.ext.append_action_delete_account(receipt_idx, beneficiary_id)?;
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
        if self.context.is_view {
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
        if self.context.is_view {
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
        if self.context.is_view {
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
        self.gas_counter.pay_per_byte(log_byte, message.len() as u64)?;
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
        self.gas_counter.pay_per_byte(log_byte, message.len() as u64)?;
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
        self.gas_counter.pay_per_byte(log_byte, message.as_bytes().len() as u64)?;
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
    ///
    /// # Cost
    ///
    /// This is a helper function that encapsulates the following costs:
    /// cost of reading buffer from register or memory,
    /// `utf8_decoding_base + utf8_decoding_byte * num_bytes`.
    fn read_and_parse_account_id(&mut self, ptr: u64, len: u64) -> Result<AccountId> {
        let buf = self.get_vec_from_memory_or_register(ptr, len)?;
        self.gas_counter.pay_base(utf8_decoding_base)?;
        self.gas_counter.pay_per_byte(utf8_decoding_byte, buf.len() as u64)?;
        let account_id = AccountId::from_utf8(buf).map_err(|_| HostError::BadUTF8)?;
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
        if self.context.is_view {
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
        self.gas_counter.pay_per_byte(storage_write_key_byte, key.len() as u64)?;
        self.gas_counter.pay_per_byte(storage_write_value_byte, value.len() as u64)?;
        let nodes_before = self.ext.get_touched_nodes_count();
        let evicted_ptr = self.ext.storage_get(&key)?;
        let evicted =
            Self::deref_value(&mut self.gas_counter, storage_write_evicted_byte, evicted_ptr)?;
        self.gas_counter
            .pay_per_byte(touching_trie_node, self.ext.get_touched_nodes_count() - nodes_before)?;
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
                gas_counter.pay_per_byte(cost_per_byte, value_ptr.len() as u64)?;
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
        self.gas_counter.pay_per_byte(storage_read_key_byte, key.len() as u64)?;
        let nodes_before = self.ext.get_touched_nodes_count();
        let read = self.ext.storage_get(&key);
        self.gas_counter
            .pay_per_byte(touching_trie_node, self.ext.get_touched_nodes_count() - nodes_before)?;
        let read = Self::deref_value(&mut self.gas_counter, storage_read_value_byte, read?)?;
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
        if self.context.is_view {
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
        self.gas_counter.pay_per_byte(storage_remove_key_byte, key.len() as u64)?;
        let nodes_before = self.ext.get_touched_nodes_count();
        let removed_ptr = self.ext.storage_get(&key)?;
        let removed =
            Self::deref_value(&mut self.gas_counter, storage_remove_ret_value_byte, removed_ptr)?;

        self.ext.storage_remove(&key)?;
        self.gas_counter
            .pay_per_byte(touching_trie_node, self.ext.get_touched_nodes_count() - nodes_before)?;
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
        self.gas_counter.pay_per_byte(storage_has_key_byte, key.len() as u64)?;
        let nodes_before = self.ext.get_touched_nodes_count();
        let res = self.ext.storage_has_key(&key);
        self.gas_counter
            .pay_per_byte(touching_trie_node, self.ext.get_touched_nodes_count() - nodes_before)?;
        Ok(res? as u64)
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

    /// Computes the outcome of execution.
    pub fn outcome(self) -> VMOutcome {
        VMOutcome {
            balance: self.current_account_balance,
            storage_usage: self.current_storage_usage,
            return_data: self.return_data,
            burnt_gas: self.gas_counter.burnt_gas(),
            used_gas: self.gas_counter.used_gas(),
            logs: self.logs,
        }
    }

    /// clones the outcome of execution.
    pub fn clone_outcome(&self) -> VMOutcome {
        let logs = self.logs.clone();
        let return_data = self.return_data.clone();
        VMOutcome {
            balance: self.current_account_balance,
            storage_usage: self.current_storage_usage,
            return_data,
            burnt_gas: self.gas_counter.burnt_gas(),
            used_gas: self.gas_counter.used_gas(),
            logs,
        }
    }

    pub fn add_contract_compile_fee(&mut self, code_len: u64) -> Result<()> {
        self.gas_counter.pay_per_byte(contract_compile_bytes, code_len)?;
        self.gas_counter.pay_base(contract_compile_base)
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct VMOutcome {
    #[serde(with = "crate::serde_with::u128_dec_format")]
    pub balance: Balance,
    pub storage_usage: StorageUsage,
    pub return_data: ReturnData,
    pub burnt_gas: Gas,
    pub used_gas: Gas,
    pub logs: Vec<String>,
}

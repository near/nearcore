use crate::config::Config;
use crate::context::VMContext;
use crate::dependencies::{External, MemoryLike};
use crate::gas_counter::GasCounter;
use crate::types::{
    AccountId, Balance, Gas, IteratorIndex, PromiseIndex, PromiseResult, ReceiptIndex, ReturnData,
    StorageUsage,
};
use crate::{HostError, HostErrorOrStorageError};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::mem::size_of;

type Result<T> = ::std::result::Result<T, HostErrorOrStorageError>;

pub struct VMLogic<'a> {
    /// Provides access to the components outside the Wasm runtime for operations on the trie and
    /// receipts creation.
    ext: &'a mut dyn External,
    /// Part of Context API and Economics API that was extracted from the receipt.
    context: VMContext,
    /// Parameters of Wasm and economic parameters.
    config: &'a Config,
    /// If this method execution is invoked directly as a callback by one or more contract calls the
    /// results of the methods that made the callback are stored in this collection.
    promise_results: &'a [PromiseResult],
    /// Pointer to the guest memory.
    memory: &'a mut dyn MemoryLike,

    /// Keeping track of the current account balance, which can decrease when we create promises
    /// and attach balance to them.
    current_account_balance: Balance,
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

    /// Iterators that were created and can still be used.
    valid_iterators: HashSet<IteratorIndex>,
    /// Iterators that became invalidated by mutating the trie.
    invalid_iterators: HashSet<IteratorIndex>,

    /// The DAG of promises, indexed by promise id.
    promises: Vec<Promise>,
    /// Record the accounts towards which the receipts are directed.
    receipt_to_account: HashMap<ReceiptIndex, AccountId>,
}

/// Promises API allows to create a DAG-structure that defines dependencies between smart contract
/// calls. A single promise can be created with zero or several dependencies on other promises.
/// * If promise was created from a receipt (using `promise_create` or `promise_then`) then
///   `promise_to_receipt` is `Receipt`;
/// * If promise was created by merging several promises (using `promise_and`) then
///   `promise_to_receipt` is `NotReceipt` but has receipts of all promises it depends on.
#[derive(Debug)]
struct Promise {
    promise_to_receipt: PromiseToReceipts,
}

#[derive(Debug)]
enum PromiseToReceipts {
    Receipt(ReceiptIndex),
    NotReceipt(Vec<ReceiptIndex>),
}

impl<'a> VMLogic<'a> {
    pub fn new(
        ext: &'a mut dyn External,
        context: VMContext,
        config: &'a Config,
        promise_results: &'a [PromiseResult],
        memory: &'a mut dyn MemoryLike,
    ) -> Self {
        let current_account_balance = context.account_balance + context.attached_deposit;
        let current_storage_usage = context.storage_usage;
        let gas_counter =
            GasCounter::new(config.max_gas_burnt, context.prepaid_gas, context.is_view);
        Self {
            ext,
            context,
            config,
            promise_results,
            memory,
            current_account_balance,
            current_storage_usage,
            gas_counter,
            return_data: ReturnData::None,
            logs: vec![],
            registers: HashMap::new(),
            valid_iterators: HashSet::new(),
            invalid_iterators: HashSet::new(),
            promises: vec![],
            receipt_to_account: HashMap::new(),
        }
    }

    // ###########################
    // # Memory helper functions #
    // ###########################

    fn try_fit_mem(memory: &dyn MemoryLike, offset: u64, len: u64) -> Result<()> {
        if memory.fits_memory(offset, len) {
            Ok(())
        } else {
            Err(HostError::MemoryAccessViolation.into())
        }
    }

    fn memory_get_into(memory: &dyn MemoryLike, offset: u64, buf: &mut [u8]) -> Result<()> {
        Self::try_fit_mem(memory, offset, buf.len() as u64)?;
        memory.read_memory(offset, buf);
        Ok(())
    }

    fn memory_get(memory: &dyn MemoryLike, offset: u64, len: u64) -> Result<Vec<u8>> {
        Self::try_fit_mem(memory, offset, len)?;
        let mut buf = vec![0; len as usize];
        memory.read_memory(offset, &mut buf);
        Ok(buf)
    }

    fn memory_set(memory: &mut dyn MemoryLike, offset: u64, buf: &[u8]) -> Result<()> {
        Self::try_fit_mem(memory, offset, buf.len() as _)?;
        memory.write_memory(offset, buf);
        Ok(())
    }

    /// Writes `u128` to Wasm memory.
    #[allow(dead_code)]
    fn memory_set_u128(memory: &mut dyn MemoryLike, offset: u64, value: u128) -> Result<()> {
        let data: [u8; size_of::<u128>()] = value.to_le_bytes();
        Self::memory_set(memory, offset, &data)
    }

    /// Get `u128` from Wasm memory.
    fn memory_get_u128(memory: &dyn MemoryLike, offset: u64) -> Result<u128> {
        let mut array = [0u8; size_of::<u128>()];
        Self::memory_get_into(memory, offset, &mut array)?;
        Ok(u128::from_le_bytes(array))
    }

    /// Reads an array of `u64` elements.
    fn memory_get_array_u64(
        memory: &dyn MemoryLike,
        offset: u64,
        num_elements: u64,
    ) -> Result<Vec<u64>> {
        let memory_len = num_elements
            .checked_mul(size_of::<u64>() as u64)
            .ok_or(HostError::MemoryAccessViolation)?;
        let data = Self::memory_get(memory, offset, memory_len)?;
        Ok(data
            .chunks(size_of::<u64>())
            .map(|buf| {
                assert_eq!(buf.len(), size_of::<u64>());
                let mut array = [0u8; size_of::<u64>()];
                array.copy_from_slice(buf);
                u64::from_le_bytes(array)
            })
            .collect())
    }

    fn read_memory_u32(memory: &dyn MemoryLike, ptr: u64) -> Result<u32> {
        let mut slice = [0u8; size_of::<u32>()];
        let buf = Self::memory_get(memory, ptr, size_of::<u32>() as u64)?;
        slice.copy_from_slice(&buf);
        Ok(u32::from_le_bytes(slice))
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
    /// * If string is longer than `max_log_len` returns `BadUtf8`.
    fn get_utf8_string(&mut self, len: u64, ptr: u64) -> Result<String> {
        self.gas_counter.pay_base(self.config.runtime_fees.ext_costs.log_base)?;
        let mut buf;
        let max_len = self.config.max_log_len;
        if len != std::u64::MAX {
            if len > max_len {
                return Err(HostError::BadUTF8.into());
            }
            self.gas_counter.pay_per_byte(self.config.runtime_fees.ext_costs.log_per_byte, len)?;
            buf = Self::memory_get(self.memory, ptr, len)?;
        } else {
            buf = vec![];
            for i in 0..=max_len {
                self.gas_counter
                    .pay_per_byte(self.config.runtime_fees.ext_costs.log_per_byte, 1)?;
                Self::try_fit_mem(self.memory, ptr + i, 1)?;
                let el = self.memory.read_memory_u8(ptr + i);
                if el == 0 {
                    break;
                }
                if i == max_len {
                    return Err(HostError::BadUTF8.into());
                }
                buf.push(el);
            }
        }
        String::from_utf8(buf).map_err(|_| HostError::BadUTF8.into())
    }

    /// Helper function to read UTF-16 formatted string from guest memory.
    /// # Errors
    ///
    /// * If string extends outside the memory of the guest with `MemoryAccessViolation`;
    /// * If string is not UTF-16 returns `BadUtf16`.
    fn get_utf16_string(&mut self, len: u64, ptr: u64) -> Result<String> {
        self.gas_counter.pay_base(self.config.runtime_fees.ext_costs.log_base)?;
        let mut u16_buffer = Vec::new();
        let max_len = self.config.max_log_len;
        if len != std::u64::MAX {
            let input = Self::memory_get(self.memory, ptr, len as u64)?;
            if len % 2 != 0 || len > max_len {
                return Err(HostError::BadUTF16.into());
            }
            self.gas_counter.pay_per_byte(self.config.runtime_fees.ext_costs.log_per_byte, len)?;
            for i in 0..((len / 2) as usize) {
                u16_buffer
                    .push(u16::from_le_bytes([input[i as usize * 2], input[i as usize * 2 + 1]]));
            }
        } else {
            let limit = max_len / size_of::<u16>() as u64;
            // Takes 2 bytes each iter
            for i in 0..=limit {
                self.gas_counter.pay_per_byte(
                    self.config.runtime_fees.ext_costs.log_per_byte,
                    size_of::<u16>() as u64,
                )?;
                // Self::try_fit_mem will check for u64 overflow on the first iteration (i == 0)
                let start = ptr + i * size_of::<u16>() as u64;
                Self::try_fit_mem(self.memory, start, size_of::<u16>() as u64)?;
                let lo = self.memory.read_memory_u8(start);
                let hi = self.memory.read_memory_u8(start + 1);
                if (lo, hi) == (0, 0) {
                    break;
                }
                if i == limit {
                    return Err(HostError::BadUTF16.into());
                }
                u16_buffer.push(u16::from_le_bytes([lo, hi]));
            }
        }
        String::from_utf16(&u16_buffer).map_err(|_| HostError::BadUTF16.into())
    }

    // #################
    // # Registers API #
    // #################

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
    pub fn read_register(&mut self, register_id: u64, ptr: u64) -> Result<()> {
        let Self { registers, memory, config, .. } = self;
        self.gas_counter.pay_base(config.runtime_fees.ext_costs.read_register_base)?;
        let register = registers.get(&register_id).ok_or(HostError::InvalidRegisterId)?;
        self.gas_counter.pay_per_byte(
            config.runtime_fees.ext_costs.read_register_byte,
            register.len() as u64,
        )?;
        Self::memory_set(*memory, ptr, register)
    }

    /// Returns the size of the blob stored in the given register.
    /// * If register is used, then returns the size, which can potentially be zero;
    /// * If register is not used, returns `u64::MAX`
    ///
    /// # Arguments
    ///
    /// * `register_id` -- a register id from where to read the data;
    pub fn register_len(&mut self, register_id: u64) -> Result<u64> {
        Ok(self.registers.get(&register_id).map(|r| r.len() as _).unwrap_or(std::u64::MAX))
    }

    /// Copies `data` into register. If register is unused will initialize it. If register has
    /// larger capacity than needed for `data` will not re-allocate it. The register will lose
    /// the pre-existing data if any.
    ///
    /// # Arguments
    ///
    /// * `register_id` -- a register into which to write the data;
    /// * `data` -- data to be copied into register.
    pub fn write_register(&mut self, register_id: u64, data: &[u8]) -> Result<()> {
        let Self { registers, config, gas_counter, .. } = self;
        Self::internal_write_register(registers, gas_counter, config, register_id, data)
    }

    fn internal_write_register(
        registers: &mut HashMap<u64, Vec<u8>>,
        gas_counter: &mut GasCounter,
        config: &Config,
        register_id: u64,
        data: &[u8],
    ) -> Result<()> {
        gas_counter.pay_base(config.runtime_fees.ext_costs.write_register_base)?;
        gas_counter
            .pay_per_byte(config.runtime_fees.ext_costs.write_register_byte, data.len() as u64)?;
        if data.len() as u64 > config.max_register_size
            || registers.len() as u64 == config.max_number_registers
        {
            return Err(HostError::MemoryAccessViolation.into());
        }
        let register = registers.entry(register_id).or_insert_with(Vec::new);
        register.clear();
        register.reserve(data.len());
        register.extend_from_slice(data);

        // Calculate the new memory usage.
        let usage: usize =
            registers.values().map(|v| size_of::<u64>() + v.len() * size_of::<u8>()).sum();
        if usage as u64 > config.registers_memory_limit {
            Err(HostError::MemoryAccessViolation.into())
        } else {
            Ok(())
        }
    }

    // ###############
    // # Context API #
    // ###############

    /// Saves the account id of the current contract that we execute into the register.
    ///
    /// # Errors
    ///
    /// If the registers exceed the memory limit returns `MemoryAccessViolation`.
    pub fn current_account_id(&mut self, register_id: u64) -> Result<()> {
        let Self { context, registers, gas_counter, config, .. } = self;
        gas_counter.pay_base(config.runtime_fees.ext_costs.current_account_id)?;
        let data = context.current_account_id.as_bytes();
        gas_counter.pay_per_byte(
            config.runtime_fees.ext_costs.signer_account_id_byte,
            data.len() as u64,
        )?;
        Self::internal_write_register(registers, gas_counter, config, register_id, data)
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
    pub fn signer_account_id(&mut self, register_id: u64) -> Result<()> {
        let Self { context, registers, gas_counter, config, .. } = self;
        gas_counter.pay_base(config.runtime_fees.ext_costs.signer_account_id)?;
        if context.is_view {
            return Err(HostError::ProhibitedInView("signer_account_id".to_string()).into());
        }
        let data = context.signer_account_id.as_bytes();
        gas_counter.pay_per_byte(
            config.runtime_fees.ext_costs.signer_account_id_byte,
            data.len() as u64,
        )?;
        Self::internal_write_register(registers, gas_counter, config, register_id, data)
    }

    /// Saves the public key fo the access key that was used by the signer into the register. In
    /// rare situations smart contract might want to know the exact access key that was used to send
    /// the original transaction, e.g. to increase the allowance or manipulate with the public key.
    ///
    /// # Errors
    ///
    /// * If the registers exceed the memory limit returns `MemoryAccessViolation`.
    /// * If called as view function returns `ProhibitedInView`.
    pub fn signer_account_pk(&mut self, register_id: u64) -> Result<()> {
        let Self { context, registers, gas_counter, config, .. } = self;
        gas_counter.pay_base(config.runtime_fees.ext_costs.signer_account_id)?;
        if context.is_view {
            return Err(HostError::ProhibitedInView("signer_account_pk".to_string()).into());
        }
        let data = context.signer_account_pk.as_slice();
        gas_counter.pay_per_byte(
            config.runtime_fees.ext_costs.signer_account_pk_byte,
            data.len() as u64,
        )?;
        Self::internal_write_register(registers, gas_counter, config, register_id, data)
    }

    /// All contract calls are a result of a receipt, this receipt might be created by a transaction
    /// that does function invocation on the contract or another contract as a result of
    /// cross-contract call. Saves the bytes of the predecessor account id into the register.
    ///
    /// # Errors
    ///
    /// * If the registers exceed the memory limit returns `MemoryAccessViolation`.
    /// * If called as view function returns `ProhibitedInView`.
    pub fn predecessor_account_id(&mut self, register_id: u64) -> Result<()> {
        let Self { context, registers, gas_counter, config, .. } = self;
        gas_counter.pay_base(config.runtime_fees.ext_costs.predecessor_account_id)?;
        if context.is_view {
            return Err(HostError::ProhibitedInView("predecessor_account_id".to_string()).into());
        }
        let data = context.predecessor_account_id.as_bytes();
        gas_counter.pay_per_byte(
            config.runtime_fees.ext_costs.predecessor_account_id_byte,
            data.len() as u64,
        )?;
        Self::internal_write_register(registers, gas_counter, config, register_id, data)
    }

    /// Reads input to the contract call into the register. Input is expected to be in JSON-format.
    /// If input is provided saves the bytes (potentially zero) of input into register. If input is
    /// not provided makes the register "not used", i.e. `register_len` now returns `u64::MAX`.
    pub fn input(&mut self, register_id: u64) -> Result<()> {
        let Self { context, registers, gas_counter, config, .. } = self;
        gas_counter.pay_base(config.runtime_fees.ext_costs.input_base)?;
        gas_counter.pay_per_byte(
            config.runtime_fees.ext_costs.input_per_byte,
            context.input.len() as u64,
        )?;
        Self::internal_write_register(registers, gas_counter, config, register_id, &context.input)
    }

    /// Returns the current block index.
    pub fn block_index(&mut self) -> Result<u64> {
        self.gas_counter.pay_base(self.config.runtime_fees.ext_costs.block_index)?;
        Ok(self.context.block_index)
    }

    /// Returns the current block timestamp.
    pub fn block_timestamp(&mut self) -> Result<u64> {
        self.gas_counter.pay_base(self.config.runtime_fees.ext_costs.block_timestamp)?;
        Ok(self.context.block_timestamp)
    }

    /// Returns the number of bytes used by the contract if it was saved to the trie as of the
    /// invocation. This includes:
    /// * The data written with storage_* functions during current and previous execution;
    /// * The bytes needed to store the access keys of the given account.
    /// * The contract code size
    /// * A small fixed overhead for account metadata.
    ///
    pub fn storage_usage(&mut self) -> Result<StorageUsage> {
        self.gas_counter.pay_base(self.config.runtime_fees.ext_costs.storage_usage)?;
        Ok(self.current_storage_usage)
    }

    // #################
    // # Economics API #
    // #################

    /// The current balance of the given account. This includes the attached_deposit that was
    /// attached to the transaction.
    pub fn account_balance(&mut self, balance_ptr: u64) -> Result<()> {
        self.gas_counter.pay_base(self.config.runtime_fees.ext_costs.account_balance)?;
        Self::memory_set(self.memory, balance_ptr, &self.current_account_balance.to_le_bytes())
    }

    /// The balance that was attached to the call that will be immediately deposited before the
    /// contract execution starts.
    ///
    /// # Errors
    ///
    /// If called as view function returns `ProhibitedInView``.
    pub fn attached_deposit(&mut self, balance_ptr: u64) -> Result<()> {
        self.gas_counter.pay_base(self.config.runtime_fees.ext_costs.attached_deposit)?;
        if self.context.is_view {
            return Err(HostError::ProhibitedInView("attached_deposit".to_string()).into());
        }
        Self::memory_set(self.memory, balance_ptr, &self.context.attached_deposit.to_le_bytes())
    }

    /// The amount of gas attached to the call that can be used to pay for the gas fees.
    ///
    /// # Errors
    ///
    /// If called as view function returns `ProhibitedInView`.
    pub fn prepaid_gas(&mut self) -> Result<Gas> {
        self.gas_counter.pay_base(self.config.runtime_fees.ext_costs.prepaid_gas)?;
        if self.context.is_view {
            return Err(HostError::ProhibitedInView("prepaid_gas".to_string()).into());
        }
        Ok(self.context.prepaid_gas)
    }

    /// The gas that was already burnt during the contract execution (cannot exceed `prepaid_gas`)
    ///
    /// # Errors
    ///
    /// If called as view function returns `ProhibitedInView`.
    pub fn used_gas(&mut self) -> Result<Gas> {
        self.gas_counter.pay_base(self.config.runtime_fees.ext_costs.used_gas)?;
        if self.context.is_view {
            return Err(HostError::ProhibitedInView("used_gas".to_string()).into());
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
    pub fn random_seed(&mut self, register_id: u64) -> Result<()> {
        let Self { context, registers, gas_counter, config, .. } = self;
        gas_counter.pay_base(config.runtime_fees.ext_costs.random_seed_base)?;
        gas_counter.pay_per_byte(
            config.runtime_fees.ext_costs.random_seed_per_byte,
            context.random_seed.len() as u64,
        )?;
        Self::internal_write_register(
            registers,
            gas_counter,
            config,
            register_id,
            &context.random_seed,
        )
    }

    /// Hashes the random sequence of bytes using sha256 and returns it into `register_id`.
    ///
    /// # Errors
    ///
    /// If `value_len + value_ptr` points outside the memory or the registers use more memory than
    /// the limit with `MemoryAccessViolation`.
    pub fn sha256(&mut self, value_len: u64, value_ptr: u64, register_id: u64) -> Result<()> {
        let Self { memory, registers, gas_counter, config, ext, .. } = self;
        gas_counter.pay_base(config.runtime_fees.ext_costs.sha256)?;
        gas_counter.pay_per_byte(config.runtime_fees.ext_costs.sha256_byte, value_len)?;
        let value = Self::memory_get(*memory, value_ptr, value_len)?;
        let value_hash = ext.sha256(&value)?;
        Self::internal_write_register(registers, gas_counter, config, register_id, &value_hash)
    }

    /// Called by gas metering injected into Wasm. Counts both towards `burnt_gas` and `used_gas`.
    ///
    /// # Errors
    ///
    /// * If passed gas amount somehow overflows internal gas counters returns `IntegerOverflow`;
    /// * If we exceed usage limit imposed on burnt gas returns `UsageLimit`;
    /// * If we exceed the `prepaid_gas` then returns `BalanceExceeded`.
    pub fn gas(&mut self, gas_amount: u32) -> Result<()> {
        self.gas_counter.deduct_gas(Gas::from(gas_amount), Gas::from(gas_amount))
    }

    // ################
    // # Promises API #
    // ################

    /// A helper function to pay gas fee for creating a new receipt without actions.
    /// # Args:
    /// * `sir`: whether contract call is addressed to itself;
    /// * `data_dependencies`: other contracts that this execution will be waiting on (or rather
    ///   their data receipts), where bool indicates whether this is sender=receiver communication.
    fn pay_gas_for_new_receipt(&mut self, sir: bool, data_dependencies: &[bool]) -> Result<()> {
        let runtime_fees_cfg = &self.config.runtime_fees;
        let mut burn_gas = runtime_fees_cfg.action_receipt_creation_config.send_fee(sir);
        let mut use_gas = runtime_fees_cfg.action_receipt_creation_config.exec_fee();
        for dep in data_dependencies {
            // Both creation and execution for data receipts are considered burnt gas.
            burn_gas = burn_gas
                .checked_add(runtime_fees_cfg.data_receipt_creation_config.base_cost.send_fee(*dep))
                .ok_or(HostError::IntegerOverflow)?
                .checked_add(runtime_fees_cfg.data_receipt_creation_config.base_cost.exec_fee())
                .ok_or(HostError::IntegerOverflow)?;
        }
        use_gas = use_gas.checked_add(burn_gas).ok_or(HostError::IntegerOverflow)?;
        self.gas_counter.deduct_gas(burn_gas, use_gas)
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
    ///
    /// # Returns
    ///
    /// Index of the new promise that uniquely identifies it within the current execution of the
    /// method.
    pub fn promise_and(
        &mut self,
        promise_idx_ptr: u64,
        promise_idx_count: u64,
    ) -> Result<PromiseIndex> {
        if self.context.is_view {
            return Err(HostError::ProhibitedInView("promise_and".to_string()).into());
        }
        self.gas_counter.pay_base(self.config.runtime_fees.ext_costs.promise_and_base)?;
        self.gas_counter.pay_per_byte(
            self.config.runtime_fees.ext_costs.promise_and_per_promise,
            promise_idx_count * size_of::<u64>() as u64,
        )?;
        let promise_indices =
            Self::memory_get_array_u64(self.memory, promise_idx_ptr, promise_idx_count)?;

        let mut receipt_dependencies = vec![];
        for promise_idx in &promise_indices {
            let promise =
                self.promises.get(*promise_idx as usize).ok_or(HostError::InvalidPromiseIndex)?;
            match &promise.promise_to_receipt {
                PromiseToReceipts::Receipt(receipt_idx) => {
                    receipt_dependencies.push(*receipt_idx);
                }
                PromiseToReceipts::NotReceipt(receipt_indices) => {
                    receipt_dependencies.extend(receipt_indices.clone());
                }
            }
        }
        let new_promise_idx = self.promises.len() as PromiseIndex;
        self.promises.push(Promise {
            promise_to_receipt: PromiseToReceipts::NotReceipt(receipt_dependencies),
        });
        Ok(new_promise_idx)
    }

    /// Creates a new promise towards given `account_id` without any actions attached to it.
    ///
    /// # Errors
    ///
    /// * If `account_id_len + account_id_ptr` points outside the memory of the guest or host
    /// returns `MemoryAccessViolation`.
    /// * If called as view function returns `ProhibitedInView`.
    ///
    /// # Returns
    ///
    /// Index of the new promise that uniquely identifies it within the current execution of the
    /// method.
    pub fn promise_batch_create(
        &mut self,
        account_id_len: u64,
        account_id_ptr: u64,
    ) -> Result<u64> {
        if self.context.is_view {
            return Err(HostError::ProhibitedInView("promise_batch_create".to_string()).into());
        }

        let account_id = self.read_and_parse_account_id(account_id_ptr, account_id_len)?;
        let sir = account_id == self.context.current_account_id;
        self.pay_gas_for_new_receipt(sir, &[])?;
        let new_receipt_idx = self.ext.create_receipt(vec![], account_id.clone())?;
        self.receipt_to_account.insert(new_receipt_idx, account_id);

        let promise_idx = self.promises.len() as PromiseIndex;
        self.promises
            .push(Promise { promise_to_receipt: PromiseToReceipts::Receipt(new_receipt_idx) });
        Ok(promise_idx)
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
    ///
    /// # Returns
    ///
    /// Index of the new promise that uniquely identifies it within the current execution of the
    /// method.
    pub fn promise_batch_then(
        &mut self,
        promise_idx: u64,
        account_id_len: u64,
        account_id_ptr: u64,
    ) -> Result<u64> {
        if self.context.is_view {
            return Err(HostError::ProhibitedInView("promise_batch_then".to_string()).into());
        }
        let account_id = self.read_and_parse_account_id(account_id_ptr, account_id_len)?;
        // Update the DAG and return new promise idx.
        let promise =
            self.promises.get(promise_idx as usize).ok_or(HostError::InvalidPromiseIndex)?;
        let receipt_dependencies = match &promise.promise_to_receipt {
            PromiseToReceipts::Receipt(receipt_idx) => vec![*receipt_idx],
            PromiseToReceipts::NotReceipt(receipt_indices) => receipt_indices.clone(),
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
        let new_promise_idx = self.promises.len() as PromiseIndex;
        self.promises
            .push(Promise { promise_to_receipt: PromiseToReceipts::Receipt(new_receipt_idx) });
        Ok(new_promise_idx)
    }

    /// Helper function to return the receipt index corresponding to the given promise index.
    /// It also pulls account ID for the given receipt and compares it with the current account ID
    /// to return whether the receipt's account ID is the same.
    fn promise_idx_to_receipt_idx_with_sir(
        &self,
        promise_idx: u64,
    ) -> Result<(ReceiptIndex, bool)> {
        let promise =
            self.promises.get(promise_idx as usize).ok_or(HostError::InvalidPromiseIndex)?;
        let receipt_idx = match &promise.promise_to_receipt {
            PromiseToReceipts::Receipt(receipt_idx) => Ok(*receipt_idx),
            PromiseToReceipts::NotReceipt(_) => Err(HostError::CannotAppendActionToJointPromise),
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
    pub fn promise_batch_action_create_account(&mut self, promise_idx: u64) -> Result<()> {
        if self.context.is_view {
            return Err(HostError::ProhibitedInView(
                "promise_batch_action_create_account".to_string(),
            )
            .into());
        }
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        self.gas_counter.pay_action_base(
            &self.config.runtime_fees.action_creation_config.create_account_cost,
            sir,
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
    pub fn promise_batch_action_deploy_contract(
        &mut self,
        promise_idx: u64,
        code_len: u64,
        code_ptr: u64,
    ) -> Result<()> {
        if self.context.is_view {
            return Err(HostError::ProhibitedInView(
                "promise_batch_action_deploy_contract".to_string(),
            )
            .into());
        }
        let code = Self::memory_get(self.memory, code_ptr, code_len)?;

        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        let num_bytes = code.len() as u64;
        self.gas_counter.pay_action_base(
            &self.config.runtime_fees.action_creation_config.deploy_contract_cost,
            sir,
        )?;
        self.gas_counter.pay_action_per_byte(
            &self.config.runtime_fees.action_creation_config.deploy_contract_cost_per_byte,
            num_bytes,
            sir,
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
        if self.context.is_view {
            return Err(HostError::ProhibitedInView(
                "promise_batch_action_function_call".to_string(),
            )
            .into());
        }
        let amount = Self::memory_get_u128(self.memory, amount_ptr)?;
        let method_name = Self::memory_get(self.memory, method_name_ptr, method_name_len)?;
        if method_name.is_empty() {
            return Err(HostError::EmptyMethodName.into());
        }
        let arguments = Self::memory_get(self.memory, arguments_ptr, arguments_len)?;

        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        let num_bytes = (method_name.len() + arguments.len()) as u64;
        self.gas_counter.pay_action_base(
            &self.config.runtime_fees.action_creation_config.function_call_cost,
            sir,
        )?;
        self.gas_counter.pay_action_per_byte(
            &self.config.runtime_fees.action_creation_config.function_call_cost_per_byte,
            num_bytes,
            sir,
        )?;
        // Prepaid gas
        self.gas_counter.deduct_gas(0, gas)?;

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
    pub fn promise_batch_action_transfer(
        &mut self,
        promise_idx: u64,
        amount_ptr: u64,
    ) -> Result<()> {
        if self.context.is_view {
            return Err(
                HostError::ProhibitedInView("promise_batch_action_transfer".to_string()).into()
            );
        }
        let amount = Self::memory_get_u128(self.memory, amount_ptr)?;

        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        self.gas_counter
            .pay_action_base(&self.config.runtime_fees.action_creation_config.transfer_cost, sir)?;

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
    pub fn promise_batch_action_stake(
        &mut self,
        promise_idx: u64,
        amount_ptr: u64,
        public_key_len: u64,
        public_key_ptr: u64,
    ) -> Result<()> {
        if self.context.is_view {
            return Err(
                HostError::ProhibitedInView("promise_batch_action_stake".to_string()).into()
            );
        }
        let amount = Self::memory_get_u128(self.memory, amount_ptr)?;
        let public_key = Self::memory_get(self.memory, public_key_ptr, public_key_len)?;

        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        self.gas_counter
            .pay_action_base(&self.config.runtime_fees.action_creation_config.stake_cost, sir)?;

        self.deduct_balance(amount)?;

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
    pub fn promise_batch_action_add_key_with_full_access(
        &mut self,
        promise_idx: u64,
        public_key_len: u64,
        public_key_ptr: u64,
        nonce: u64,
    ) -> Result<()> {
        if self.context.is_view {
            return Err(HostError::ProhibitedInView(
                "promise_batch_action_add_key_with_full_access".to_string(),
            )
            .into());
        }
        let public_key = Self::memory_get(self.memory, public_key_ptr, public_key_len)?;

        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        self.gas_counter.pay_action_base(
            &self.config.runtime_fees.action_creation_config.add_key_cost.full_access_cost,
            sir,
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
        if self.context.is_view {
            return Err(HostError::ProhibitedInView(
                "promise_batch_action_add_key_with_function_call".to_string(),
            )
            .into());
        }
        let public_key = Self::memory_get(self.memory, public_key_ptr, public_key_len)?;
        let allowance = Self::memory_get_u128(self.memory, allowance_ptr)?;
        let allowance = if allowance > 0 { Some(allowance) } else { None };
        let receiver_id = self.read_and_parse_account_id(receiver_id_ptr, receiver_id_len)?;
        let method_names = Self::memory_get(self.memory, method_names_ptr, method_names_len)?;
        // Use `,` separator to split `method_names` into a vector of method names.
        let method_names =
            method_names
                .split(|c| *c == b',')
                .map(|v| {
                    if v.is_empty() {
                        Err(HostError::EmptyMethodName.into())
                    } else {
                        Ok(v.to_vec())
                    }
                })
                .collect::<Result<Vec<_>>>()?;

        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        // +1 is to account for null-terminating characters.
        let num_bytes = method_names.iter().map(|v| v.len() as u64 + 1).sum::<u64>();
        self.gas_counter.pay_action_base(
            &self.config.runtime_fees.action_creation_config.add_key_cost.function_call_cost,
            sir,
        )?;
        self.gas_counter.pay_action_per_byte(
            &self
                .config
                .runtime_fees
                .action_creation_config
                .add_key_cost
                .function_call_cost_per_byte,
            num_bytes,
            sir,
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
    pub fn promise_batch_action_delete_key(
        &mut self,
        promise_idx: u64,
        public_key_len: u64,
        public_key_ptr: u64,
    ) -> Result<()> {
        if self.context.is_view {
            return Err(
                HostError::ProhibitedInView("promise_batch_action_delete_key".to_string()).into()
            );
        }
        let public_key = Self::memory_get(self.memory, public_key_ptr, public_key_len)?;

        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        self.gas_counter.pay_action_base(
            &self.config.runtime_fees.action_creation_config.delete_key_cost,
            sir,
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
    pub fn promise_batch_action_delete_account(
        &mut self,
        promise_idx: u64,
        beneficiary_id_len: u64,
        beneficiary_id_ptr: u64,
    ) -> Result<()> {
        if self.context.is_view {
            return Err(HostError::ProhibitedInView(
                "promise_batch_action_delete_account".to_string(),
            )
            .into());
        }
        let beneficiary_id =
            self.read_and_parse_account_id(beneficiary_id_ptr, beneficiary_id_len)?;

        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        self.gas_counter.pay_action_base(
            &self.config.runtime_fees.action_creation_config.delete_account_cost,
            sir,
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
    pub fn promise_results_count(&self) -> Result<u64> {
        if self.context.is_view {
            return Err(HostError::ProhibitedInView("promise_results_count".to_string()).into());
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
    /// * If `result_id` does not correspond to an existing result returns `InvalidResultIndex`;
    /// * If copying the blob exhausts the memory limit it returns `MemoryAccessViolation`.
    /// * If called as view function returns `ProhibitedInView`.
    pub fn promise_result(&mut self, result_idx: u64, register_id: u64) -> Result<u64> {
        let Self { promise_results, registers, gas_counter, config, context, .. } = self;
        if context.is_view {
            return Err(HostError::ProhibitedInView("promise_result".to_string()).into());
        }
        gas_counter.pay_base(config.runtime_fees.ext_costs.promise_result_base)?;
        match promise_results
            .get(result_idx as usize)
            .ok_or(HostError::InvalidPromiseResultIndex)?
        {
            PromiseResult::NotReady => Ok(0),
            PromiseResult::Successful(data) => {
                gas_counter.pay_per_byte(
                    config.runtime_fees.ext_costs.promise_result_byte,
                    data.len() as u64,
                )?;
                Self::internal_write_register(registers, gas_counter, config, register_id, data)?;
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
    pub fn promise_return(&mut self, promise_idx: u64) -> Result<()> {
        if self.context.is_view {
            return Err(HostError::ProhibitedInView("promise_return".to_string()).into());
        }
        self.gas_counter.pay_base(self.config.runtime_fees.ext_costs.promise_return)?;
        match self
            .promises
            .get(promise_idx as usize)
            .ok_or(HostError::InvalidPromiseIndex)?
            .promise_to_receipt
        {
            PromiseToReceipts::Receipt(receipt_idx) => {
                self.return_data = ReturnData::ReceiptIndex(receipt_idx);
                Ok(())
            }
            PromiseToReceipts::NotReceipt(_) => Err(HostError::CannotReturnJointPromise.into()),
        }
    }

    // #####################
    // # Miscellaneous API #
    // #####################

    /// Sets the blob of data as the return value of the contract.
    ///
    /// # Errors
    ///
    /// If `value_len + value_ptr` exceeds the memory container or points to an unused register it
    /// returns `MemoryAccessViolation`.
    pub fn value_return(&mut self, value_len: u64, value_ptr: u64) -> Result<()> {
        let return_val = Self::memory_get(self.memory, value_ptr, value_len)?;
        let mut burn_gas: Gas = 0;
        let num_bytes = return_val.len() as u64;
        let data_cfg = &self.config.runtime_fees.data_receipt_creation_config;
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
        self.gas_counter.deduct_gas(burn_gas, burn_gas)?;
        self.return_data = ReturnData::Value(return_val);
        Ok(())
    }

    /// Terminates the execution of the program with panic `GuestPanic`.
    pub fn panic(&self) -> Result<()> {
        Err(HostError::GuestPanic("explicit guest panic".to_string()).into())
    }

    /// Guest panics with the UTF-8 encoded string.
    /// If `len == u64::MAX` then treats the string as null-terminated with character `'\0'`.
    ///
    /// # Errors
    ///
    /// * If string extends outside the memory of the guest with `MemoryAccessViolation`;
    /// * If string is not UTF-8 returns `BadUtf8`.
    /// * If string is longer than `max_log_len` returns `BadUtf8`.
    pub fn panic_utf8(&mut self, len: u64, ptr: u64) -> Result<()> {
        Err(HostError::GuestPanic(self.get_utf8_string(len, ptr)?).into())
    }

    /// Logs the UTF-8 encoded string.
    /// If `len == u64::MAX` then treats the string as null-terminated with character `'\0'`.
    ///
    /// # Errors
    ///
    /// * If string extends outside the memory of the guest with `MemoryAccessViolation`;
    /// * If string is not UTF-8 returns `BadUtf8`.
    /// * If string is longer than `max_log_len` returns `BadUtf8`.
    pub fn log_utf8(&mut self, len: u64, ptr: u64) -> Result<()> {
        let message = format!("LOG: {}", self.get_utf8_string(len, ptr)?);
        self.logs.push(message);
        Ok(())
    }

    /// Logs the UTF-16 encoded string. If `len == u64::MAX` then treats the string as
    /// null-terminated with two-byte sequence of `0x00 0x00`.
    ///
    /// # Errors
    ///
    /// * If string extends outside the memory of the guest with `MemoryAccessViolation`;
    /// * If string is not UTF-16 returns `BadUtf16`.
    pub fn log_utf16(&mut self, len: u64, ptr: u64) -> Result<()> {
        let message = format!("LOG: {}", self.get_utf16_string(len, ptr)?);
        self.logs.push(message);
        Ok(())
    }

    /// Special import kept for compatibility with AssemblyScript contracts. Not called by smart
    /// contracts directly, but instead called by the code generated by AssemblyScript.
    pub fn abort(&mut self, msg_ptr: u32, filename_ptr: u32, line: u32, col: u32) -> Result<()> {
        if msg_ptr < 4 || filename_ptr < 4 {
            return Err(HostError::BadUTF16.into());
        }
        let msg_len = Self::read_memory_u32(self.memory, (msg_ptr - 4) as u64)?;
        let filename_len = Self::read_memory_u32(self.memory, (filename_ptr - 4) as u64)?;

        let msg = self.get_utf16_string(msg_len as u64, msg_ptr as u64)?;
        let filename = self.get_utf16_string(filename_len as u64, filename_ptr as u64)?;

        let message = format!("{}, filename: \"{}\" line: {} col: {}", msg, filename, line, col);
        self.logs.push(format!("ABORT: {}", message));

        Err(HostError::GuestPanic(message).into())
    }

    /// Reads account id from the given location in memory.
    ///
    /// # Errors
    ///
    /// * If account is not UTF-8 encoded then returns `BadUtf8`;
    pub fn read_and_parse_account_id(&self, ptr: u64, len: u64) -> Result<AccountId> {
        let buf = Self::memory_get(self.memory, ptr, len)?;
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
    pub fn storage_write(
        &mut self,
        key_len: u64,
        key_ptr: u64,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<u64> {
        let Self {
            memory,
            registers,
            gas_counter,
            config,
            valid_iterators,
            invalid_iterators,
            ext,
            ..
        } = self;
        gas_counter.pay_base(config.runtime_fees.ext_costs.storage_write_base)?;
        gas_counter.pay_per_byte(config.runtime_fees.ext_costs.storage_write_key_byte, key_len)?;
        gas_counter
            .pay_per_byte(config.runtime_fees.ext_costs.storage_write_value_byte, value_len)?;
        // All iterators that were valid now become invalid
        for invalidated_iter_idx in valid_iterators.drain() {
            ext.storage_iter_drop(invalidated_iter_idx)?;
            invalid_iterators.insert(invalidated_iter_idx);
        }
        let key = Self::memory_get(*memory, key_ptr, key_len)?;
        let value = Self::memory_get(*memory, value_ptr, value_len)?;
        let evicted = self.ext.storage_set(&key, &value)?;
        let storage_config = &config.runtime_fees.storage_usage_config;
        match evicted {
            Some(old_value) => {
                self.current_storage_usage -=
                    (old_value.len() as u64) * storage_config.value_cost_per_byte;
                self.current_storage_usage += value_len * storage_config.value_cost_per_byte;
                Self::internal_write_register(
                    registers,
                    gas_counter,
                    config,
                    register_id,
                    &old_value,
                )?;
                Ok(1)
            }
            None => {
                self.current_storage_usage += value_len * storage_config.value_cost_per_byte;
                self.current_storage_usage += key_len * storage_config.key_cost_per_byte;
                self.current_storage_usage += storage_config.data_record_cost;
                Ok(0)
            }
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
    pub fn storage_read(&mut self, key_len: u64, key_ptr: u64, register_id: u64) -> Result<u64> {
        let Self { ext, memory, registers, gas_counter, config, .. } = self;
        gas_counter.pay_base(config.runtime_fees.ext_costs.storage_write_base)?;
        gas_counter.pay_per_byte(config.runtime_fees.ext_costs.storage_write_key_byte, key_len)?;
        let key = Self::memory_get(*memory, key_ptr, key_len)?;
        let read = ext.storage_get(&key)?;
        match read {
            Some(value) => {
                gas_counter.pay_per_byte(
                    config.runtime_fees.ext_costs.storage_write_value_byte,
                    value.len() as u64,
                )?;
                Self::internal_write_register(registers, gas_counter, config, register_id, &value)?;
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
    pub fn storage_remove(&mut self, key_len: u64, key_ptr: u64, register_id: u64) -> Result<u64> {
        let Self {
            ext,
            memory,
            registers,
            gas_counter,
            config,
            valid_iterators,
            invalid_iterators,
            ..
        } = self;
        gas_counter.pay_base(config.runtime_fees.ext_costs.storage_remove_base)?;
        gas_counter.pay_per_byte(config.runtime_fees.ext_costs.storage_remove_key_byte, key_len)?;
        // All iterators that were valid now become invalid
        for invalidated_iter_idx in valid_iterators.drain() {
            ext.storage_iter_drop(invalidated_iter_idx)?;
            invalid_iterators.insert(invalidated_iter_idx);
        }
        let key = Self::memory_get(*memory, key_ptr, key_len)?;
        let removed = ext.storage_remove(&key)?;
        let storage_config = &config.runtime_fees.storage_usage_config;
        match removed {
            Some(value) => {
                gas_counter.pay_per_byte(
                    config.runtime_fees.ext_costs.storage_remove_ret_value_byte,
                    value.len() as u64,
                )?;
                self.current_storage_usage -=
                    (value.len() as u64) * storage_config.value_cost_per_byte;
                self.current_storage_usage -= key_len * storage_config.key_cost_per_byte;
                self.current_storage_usage -= storage_config.data_record_cost;
                Self::internal_write_register(registers, gas_counter, config, register_id, &value)?;
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
    /// If `key_len + key_ptr` exceeds the memory container it returns `MemoryAccessViolation`.
    pub fn storage_has_key(&mut self, key_len: u64, key_ptr: u64) -> Result<u64> {
        self.gas_counter.pay_base(self.config.runtime_fees.ext_costs.storage_has_key_base)?;
        self.gas_counter
            .pay_per_byte(self.config.runtime_fees.ext_costs.storage_has_key_byte, key_len)?;
        let key = Self::memory_get(self.memory, key_ptr, key_len)?;
        let res = self.ext.storage_has_key(&key)?;
        Ok(res as u64)
    }

    /// Creates an iterator object inside the host. Returns the identifier that uniquely
    /// differentiates the given iterator from other iterators that can be simultaneously created.
    /// * It iterates over the keys that have the provided prefix. The order of iteration is defined
    ///   by the lexicographic order of the bytes in the keys;
    /// * If there are no keys, it creates an empty iterator, see below on empty iterators.
    ///
    /// # Errors
    ///
    /// If `prefix_len + prefix_ptr` exceeds the memory container it returns `MemoryAccessViolation`.
    pub fn storage_iter_prefix(&mut self, prefix_len: u64, prefix_ptr: u64) -> Result<u64> {
        self.gas_counter
            .pay_base(self.config.runtime_fees.ext_costs.storage_iter_create_prefix_base)?;
        self.gas_counter.pay_per_byte(
            self.config.runtime_fees.ext_costs.storage_iter_create_key_byte,
            prefix_len,
        )?;
        let prefix = Self::memory_get(self.memory, prefix_ptr, prefix_len)?;
        let iterator_index = self.ext.storage_iter(&prefix)?;
        self.valid_iterators.insert(iterator_index);
        Ok(iterator_index)
    }

    /// Iterates over all key-values such that keys are between `start` and `end`, where `start` is
    /// inclusive and `end` is exclusive. Unless lexicographically `start < end`, it creates an
    /// empty iterator. Note, this definition allows for `start` or `end` keys to not actually exist
    /// on the given trie.
    ///
    /// # Errors
    ///
    /// If `start_len + start_ptr` or `end_len + end_ptr` exceeds the memory container or points to
    /// an unused register it returns `MemoryAccessViolation`.
    pub fn storage_iter_range(
        &mut self,
        start_len: u64,
        start_ptr: u64,
        end_len: u64,
        end_ptr: u64,
    ) -> Result<u64> {
        self.gas_counter
            .pay_base(self.config.runtime_fees.ext_costs.storage_iter_create_range_base)?;
        let start_key = Self::memory_get(self.memory, start_ptr, start_len)?;
        let end_key = Self::memory_get(self.memory, end_ptr, end_len)?;
        self.gas_counter.pay_per_byte(
            self.config.runtime_fees.ext_costs.storage_iter_create_key_byte,
            start_len,
        )?;
        self.gas_counter.pay_per_byte(
            self.config.runtime_fees.ext_costs.storage_iter_create_key_byte,
            end_len,
        )?;
        let iterator_index = self.ext.storage_iter_range(&start_key, &end_key)?;
        self.valid_iterators.insert(iterator_index);
        Ok(iterator_index)
    }

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
    pub fn storage_iter_next(
        &mut self,
        iterator_id: u64,
        key_register_id: u64,
        value_register_id: u64,
    ) -> Result<u64> {
        let Self {
            ext, registers, gas_counter, config, valid_iterators, invalid_iterators, ..
        } = self;
        gas_counter.pay_base(config.runtime_fees.ext_costs.storage_iter_next_base)?;
        if invalid_iterators.contains(&iterator_id) {
            return Err(HostError::IteratorWasInvalidated.into());
        } else if !valid_iterators.contains(&iterator_id) {
            return Err(HostError::InvalidIteratorIndex.into());
        }

        let value = ext.storage_iter_next(iterator_id)?;
        match value {
            Some((key, value)) => {
                gas_counter.pay_per_byte(
                    config.runtime_fees.ext_costs.storage_iter_next_key_byte,
                    key.len() as u64,
                )?;
                gas_counter.pay_per_byte(
                    config.runtime_fees.ext_costs.storage_iter_next_value_byte,
                    value.len() as u64,
                )?;
                Self::internal_write_register(
                    registers,
                    gas_counter,
                    config,
                    key_register_id,
                    &key,
                )?;
                Self::internal_write_register(
                    registers,
                    gas_counter,
                    config,
                    value_register_id,
                    &value,
                )?;
                Ok(1)
            }
            None => Ok(0),
        }
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
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct VMOutcome {
    pub balance: Balance,
    pub storage_usage: StorageUsage,
    pub return_data: ReturnData,
    pub burnt_gas: Gas,
    pub used_gas: Gas,
    pub logs: Vec<String>,
}

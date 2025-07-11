use super::context::VMContext;
use super::dependencies::{External, MemSlice, MemoryLike};
use super::errors::{FunctionCallError, InconsistentStateError};
use super::gas_counter::GasCounter;
use super::recorded_storage_counter::RecordedStorageCounter;
use super::types::{
    GlobalContractDeployMode, GlobalContractIdentifier, PromiseIndex, PromiseResult, ReceiptIndex,
    ReturnData,
};
use super::utils::split_method_names;
use super::{HostError, VMLogicError};
use crate::ProfileDataV3;
use crate::bls12381_impl;
use crate::logic::gas_counter::FreeGasCounter;
use ExtCosts::*;
use near_crypto::Secp256K1Signature;
use near_parameters::vm::Config;
use near_parameters::{
    ActionCosts, ExtCosts, RuntimeFeesConfig, transfer_exec_fee, transfer_send_fee,
};
use near_primitives_core::config::INLINE_DISK_VALUE_THRESHOLD;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{
    AccountId, Balance, Compute, EpochHeight, Gas, GasWeight, StorageUsage,
};
use std::mem::size_of;
use std::sync::Arc;

pub type Result<T, E = VMLogicError> = ::std::result::Result<T, E>;

/// Structure representing the results and outcomes of a contract execution.
///
/// This is a subset of [`VMLogic`] that's strictly necessary to produce `VMOutcome`s.
pub struct ExecutionResultState {
    /// All gas and economic parameters required during contract execution.
    pub(crate) config: Arc<Config>,
    /// Gas tracking for the current contract execution.
    gas_counter: GasCounter,
    /// Logs written by the runtime.
    logs: Vec<String>,
    /// Tracks the total log length. The sum of length of all logs.
    total_log_length: u64,
    /// What method returns.
    return_data: ReturnData,
    /// Keeping track of the current account balance, which can decrease when we create promises
    /// and attach balance to them.
    current_account_balance: Balance,
    /// Storage usage of the current account at the moment
    current_storage_usage: StorageUsage,
}

impl ExecutionResultState {
    /// Create a new state.
    ///
    /// # Panics
    ///
    /// Note that `context.account_balance + context.attached_deposit` must not overflow `u128`,
    /// otherwise this function will panic.
    pub fn new(context: &VMContext, gas_counter: GasCounter, config: Arc<Config>) -> Self {
        let current_account_balance = context
            .account_balance
            .checked_add(context.attached_deposit)
            .expect("current_account_balance overflowed");
        let current_storage_usage = context.storage_usage;
        Self {
            config,
            gas_counter,
            logs: vec![],
            total_log_length: 0,
            return_data: ReturnData::None,
            current_account_balance,
            current_storage_usage,
        }
    }

    /// A helper function to subtract balance on transfer or attached deposit for promises.
    ///
    /// ### Args
    ///
    /// * `amount`: the amount to deduct from the current account balance.
    fn deduct_balance(&mut self, amount: Balance) -> Result<()> {
        self.current_account_balance =
            self.current_account_balance.checked_sub(amount).ok_or(HostError::BalanceExceeded)?;
        Ok(())
    }

    /// Checks that the current log number didn't reach the limit yet, so we can add a new message.
    fn check_can_add_a_log_message(&self) -> Result<()> {
        if self.logs.len() as u64 >= self.config.limit_config.max_number_logs {
            Err(HostError::NumberOfLogsExceeded { limit: self.config.limit_config.max_number_logs }
                .into())
        } else {
            Ok(())
        }
    }

    fn checked_push_log(&mut self, message: String) -> Result<()> {
        let len = u64::try_from(message.len()).unwrap_or(u64::MAX);
        let Some(total_log_length) = self.total_log_length.checked_add(len) else {
            return self.total_log_length_exceeded(len);
        };
        self.total_log_length = total_log_length;
        if self.total_log_length > self.config.limit_config.max_total_log_length {
            return self.total_log_length_exceeded(len);
        }
        self.logs.push(message);
        Ok(())
    }

    fn total_log_length_exceeded<T>(&self, add_len: u64) -> Result<T> {
        Err(HostError::TotalLogLengthExceeded {
            length: self.total_log_length.saturating_add(add_len),
            limit: self.config.limit_config.max_total_log_length,
        }
        .into())
    }

    /// Computes the outcome of the execution.
    ///
    /// If `FunctionCallWeight` protocol feature is enabled, unused gas will be
    /// distributed to functions that specify a gas weight. If there are no functions with
    /// a gas weight, the outcome will contain unused gas as usual.
    pub fn compute_outcome(self) -> VMOutcome {
        let burnt_gas = self.gas_counter.burnt_gas();
        let used_gas = self.gas_counter.used_gas();

        let mut profile = self.gas_counter.profile_data();
        profile.compute_wasm_instruction_cost(burnt_gas);
        let compute_usage = profile.total_compute_usage(&self.config.ext_costs);

        VMOutcome {
            balance: self.current_account_balance,
            storage_usage: self.current_storage_usage,
            return_data: self.return_data,
            burnt_gas,
            used_gas,
            compute_usage,
            logs: self.logs,
            profile,
            aborted: None,
        }
    }
}

/// Structure
pub struct VMLogic<'a> {
    /// Provides access to the components outside the Wasm runtime for operations on the trie and
    /// receipts creation.
    ext: &'a mut dyn External,
    /// Part of Context API and Economics API that was extracted from the receipt.
    context: &'a VMContext,
    /// Pointer to the guest memory.
    memory: super::vmstate::Memory<'a>,

    /// All gas and economic parameters required during contract execution.
    config: Arc<Config>,
    /// Fees charged for various operations that contract may execute.
    fees_config: Arc<RuntimeFeesConfig>,

    /// Current amount of locked tokens, does not automatically change when staking transaction is
    /// issued.
    current_account_locked_balance: Balance,
    /// Registers can be used by the guest to store blobs of data without moving them across
    /// host-guest boundary.
    registers: super::vmstate::Registers,
    /// The DAG of promises, indexed by promise id.
    promises: Vec<Promise>,

    /// Stores the amount of stack space remaining
    remaining_stack: u64,

    /// Tracks size of the recorded trie storage proof.
    recorded_storage_counter: RecordedStorageCounter,

    pub(crate) result_state: ExecutionResultState,
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

/// Helper for calling `super::vmstate::get_memory_or_register`.
///
/// super::vmstate::get_memory_or_register has a whole lot of wordy arguments
/// which are always the same when invoked inside of one of VMLogic method.
/// This macro helps with that invocation.
macro_rules! get_memory_or_register {
    ($logic:expr, $offset:expr, $len:expr) => {
        super::vmstate::get_memory_or_register(
            &mut $logic.result_state.gas_counter,
            &$logic.memory,
            &$logic.registers,
            $offset,
            $len,
        )
    };
}

/// A wrapper for reading public key.
///
/// This exists for historical reasons because we must maintain when errors are
/// returned.  In the old days, between reading the public key and decoding it
/// we could return unrelated error.  Because of that we cannot change the code
/// to return deserialization errors immediately after reading the public key.
///
/// This struct abstracts away the fact that we’re deserializing the key
/// immediately.  Decoding errors are detected as soon as this object is created
/// but they are communicated to the user only once they call [`Self::decode`].
///
/// Why not just keep the old ways without this noise? By doing deserialization
/// immediately we’re copying the data onto the stack without having to allocate
/// a temporary vector.
struct PublicKeyBuffer(Result<near_crypto::PublicKey, ()>);

impl PublicKeyBuffer {
    fn new(data: &[u8]) -> Self {
        Self(borsh::BorshDeserialize::try_from_slice(data).map_err(|_| ()))
    }

    fn decode(self) -> Result<near_crypto::PublicKey> {
        self.0.map_err(|_| HostError::InvalidPublicKey.into())
    }
}

impl<'a> VMLogic<'a> {
    pub fn new(
        ext: &'a mut dyn External,
        context: &'a VMContext,
        fees_config: Arc<RuntimeFeesConfig>,
        result_state: ExecutionResultState,
        memory: &'a mut dyn MemoryLike,
    ) -> Self {
        let current_account_locked_balance = context.account_locked_balance;
        let config = Arc::clone(&result_state.config);
        let recorded_storage_counter = RecordedStorageCounter::new(
            ext.get_recorded_storage_size(),
            config.limit_config.per_receipt_storage_proof_size_limit,
        );
        let remaining_stack = u64::from(config.limit_config.max_stack_height);
        Self {
            ext,
            context,
            config,
            fees_config,
            memory: super::vmstate::Memory::new(memory),
            current_account_locked_balance,
            recorded_storage_counter,
            registers: Default::default(),
            promises: vec![],
            remaining_stack,
            result_state,
        }
    }

    /// Returns reference to logs that have been created so far.
    pub fn logs(&self) -> &[String] {
        &self.result_state.logs
    }

    #[cfg(test)]
    pub(super) fn config(&self) -> &Config {
        &self.config
    }

    #[cfg(test)]
    pub(super) fn memory(&mut self) -> &mut super::vmstate::Memory<'a> {
        &mut self.memory
    }

    #[cfg(test)]
    pub(super) fn registers(&mut self) -> &mut super::vmstate::Registers {
        &mut self.registers
    }

    // #########################
    // # Finite-wasm internals #
    // #########################
    pub fn finite_wasm_gas(&mut self, gas: u64) -> Result<()> {
        self.gas(gas)
    }

    fn linear_gas(&mut self, count: u32, linear: u64, constant: u64) -> Result<u32> {
        let linear = u64::from(count).checked_mul(linear).ok_or(HostError::IntegerOverflow)?;
        let gas = constant.checked_add(linear).ok_or(HostError::IntegerOverflow)?;
        self.gas(gas)?;
        Ok(count)
    }

    pub fn finite_wasm_memory_copy(
        &mut self,
        count: u32,
        linear: u64,
        constant: u64,
    ) -> Result<u32> {
        self.linear_gas(count, linear, constant)
    }

    pub fn finite_wasm_memory_fill(
        &mut self,
        count: u32,
        linear: u64,
        constant: u64,
    ) -> Result<u32> {
        self.linear_gas(count, linear, constant)
    }

    pub fn finite_wasm_memory_init(
        &mut self,
        count: u32,
        linear: u64,
        constant: u64,
    ) -> Result<u32> {
        self.linear_gas(count, linear, constant)
    }

    pub fn finite_wasm_table_copy(
        &mut self,
        count: u32,
        linear: u64,
        constant: u64,
    ) -> Result<u32> {
        self.linear_gas(count, linear, constant)
    }

    pub fn finite_wasm_table_fill(
        &mut self,
        count: u32,
        linear: u64,
        constant: u64,
    ) -> Result<u32> {
        self.linear_gas(count, linear, constant)
    }

    pub fn finite_wasm_table_init(
        &mut self,
        count: u32,
        linear: u64,
        constant: u64,
    ) -> Result<u32> {
        self.linear_gas(count, linear, constant)
    }

    pub fn finite_wasm_stack(&mut self, operand_size: u64, frame_size: u64) -> Result<()> {
        self.remaining_stack =
            match self.remaining_stack.checked_sub(operand_size.saturating_add(frame_size)) {
                Some(s) => s,
                None => return Err(VMLogicError::HostError(HostError::MemoryAccessViolation)),
            };
        self.gas(((frame_size + 7) / 8) * u64::from(self.config.regular_op_cost))?;
        Ok(())
    }

    pub fn finite_wasm_unstack(&mut self, operand_size: u64, frame_size: u64) -> Result<()> {
        self.remaining_stack = self
            .remaining_stack
            .checked_add(operand_size.saturating_add(frame_size))
            .expect("remaining stack integer overflow");
        Ok(())
    }

    // #################
    // # Registers API #
    // #################

    /// Convenience function for testing.
    #[cfg(test)]
    pub fn wrapped_internal_write_register(&mut self, register_id: u64, data: &[u8]) -> Result<()> {
        self.registers.set(
            &mut self.result_state.gas_counter,
            &self.config.limit_config,
            register_id,
            data,
        )
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
        self.result_state.gas_counter.pay_base(base)?;
        let data = self.registers.get(&mut self.result_state.gas_counter, register_id)?;
        self.memory.set(&mut self.result_state.gas_counter, ptr, data)
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
        self.result_state.gas_counter.pay_base(base)?;
        Ok(self.registers.get_len(register_id).unwrap_or(u64::MAX))
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
        self.result_state.gas_counter.pay_base(base)?;
        let data = self
            .memory
            .view(&mut self.result_state.gas_counter, MemSlice { ptr: data_ptr, len: data_len })?;
        self.registers.set(
            &mut self.result_state.gas_counter,
            &self.config.limit_config,
            register_id,
            data,
        )
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
        self.result_state.gas_counter.pay_base(utf8_decoding_base)?;
        let mut buf;
        let max_len = self
            .config
            .limit_config
            .max_total_log_length
            .saturating_sub(self.result_state.total_log_length);
        if len != u64::MAX {
            if len > max_len {
                return self.result_state.total_log_length_exceeded(len);
            }
            buf = self
                .memory
                .view(&mut self.result_state.gas_counter, MemSlice { ptr, len })?
                .into_owned();
        } else {
            buf = vec![];
            for i in 0..=max_len {
                // self.memory_get_u8 will check for u64 overflow on the first iteration (i == 0)
                let el = self.memory.get_u8(&mut self.result_state.gas_counter, ptr + i)?;
                if el == 0 {
                    break;
                }
                if i == max_len {
                    return self.result_state.total_log_length_exceeded(max_len.saturating_add(1));
                }
                buf.push(el);
            }
        }
        self.result_state.gas_counter.pay_per(utf8_decoding_byte, buf.len() as _)?;
        String::from_utf8(buf).map_err(|_| HostError::BadUTF8.into())
    }

    /// Helper function to get utf8 string, for sandbox debug log. The difference with `get_utf8_string`:
    /// * It's only available on sandbox node
    /// * The cost is 0
    /// * It's up to the caller to set correct len
    #[cfg(feature = "sandbox")]
    fn sandbox_get_utf8_string(&self, len: u64, ptr: u64) -> Result<String> {
        let buf = self.memory.view_for_free(MemSlice { ptr, len })?.into_owned();
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
    fn get_utf16_string(&mut self, mut len: u64, ptr: u64) -> Result<String> {
        self.result_state.gas_counter.pay_base(utf16_decoding_base)?;
        let max_len = self
            .config
            .limit_config
            .max_total_log_length
            .saturating_sub(self.result_state.total_log_length);

        let mem_view = if len == u64::MAX {
            len = self.get_nul_terminated_utf16_len(ptr, max_len)?;
            self.memory.view_for_free(MemSlice { ptr, len })
        } else {
            self.memory.view(&mut self.result_state.gas_counter, MemSlice { ptr, len })
        }?;

        let input = stdx::as_chunks_exact(&mem_view).map_err(|_| HostError::BadUTF16)?;
        if len > max_len {
            return self.result_state.total_log_length_exceeded(len);
        }

        self.result_state.gas_counter.pay_per(utf16_decoding_byte, len)?;
        char::decode_utf16(input.into_iter().copied().map(u16::from_le_bytes))
            .collect::<Result<String, _>>()
            .map_err(|_| HostError::BadUTF16.into())
    }

    /// Helper function to get length of NUL-terminated UTF-16 formatted string
    /// in guest memory.
    ///
    /// In other words, counts how many bytes are there to first pair of NUL
    /// bytes.
    fn get_nul_terminated_utf16_len(&mut self, ptr: u64, max_len: u64) -> Result<u64> {
        let mut len = 0;
        loop {
            if self.memory.get_u16(&mut self.result_state.gas_counter, ptr.saturating_add(len))?
                == 0
            {
                return Ok(len);
            }
            len = match len.checked_add(2) {
                Some(len) if len <= max_len => len,
                Some(len) => return self.result_state.total_log_length_exceeded(len),
                None => return self.result_state.total_log_length_exceeded(u64::MAX),
            };
        }
    }

    // ####################################################
    // # Helper functions to prevent code duplication API #
    // ####################################################

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

    fn get_public_key(&mut self, ptr: u64, len: u64) -> Result<PublicKeyBuffer> {
        Ok(PublicKeyBuffer::new(&get_memory_or_register!(self, ptr, len)?))
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
        self.result_state.gas_counter.pay_base(base)?;
        self.registers.set(
            &mut self.result_state.gas_counter,
            &self.config.limit_config,
            register_id,
            self.context.current_account_id.as_bytes(),
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
        self.result_state.gas_counter.pay_base(base)?;

        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "signer_account_id".to_string(),
            }
            .into());
        }
        self.registers.set(
            &mut self.result_state.gas_counter,
            &self.config.limit_config,
            register_id,
            self.context.signer_account_id.as_bytes(),
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
        self.result_state.gas_counter.pay_base(base)?;

        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "signer_account_pk".to_string(),
            }
            .into());
        }
        self.registers.set(
            &mut self.result_state.gas_counter,
            &self.config.limit_config,
            register_id,
            self.context.signer_account_pk.as_slice(),
        )
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
        self.result_state.gas_counter.pay_base(base)?;

        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "predecessor_account_id".to_string(),
            }
            .into());
        }
        self.registers.set(
            &mut self.result_state.gas_counter,
            &self.config.limit_config,
            register_id,
            self.context.predecessor_account_id.as_bytes(),
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
        self.result_state.gas_counter.pay_base(base)?;

        self.registers.set(
            &mut self.result_state.gas_counter,
            &self.config.limit_config,
            register_id,
            self.context.input.as_slice(),
        )
    }

    /// Returns the current block height.
    ///
    /// It’s only due to historical reasons, this host function is called
    /// `block_index` rather than `block_height`.
    ///
    /// # Cost
    ///
    /// `base`
    pub fn block_index(&mut self) -> Result<u64> {
        self.result_state.gas_counter.pay_base(base)?;
        Ok(self.context.block_height)
    }

    /// Returns the current block timestamp (number of non-leap-nanoseconds since January 1, 1970 0:00:00 UTC).
    ///
    /// # Cost
    ///
    /// `base`
    pub fn block_timestamp(&mut self) -> Result<u64> {
        self.result_state.gas_counter.pay_base(base)?;
        Ok(self.context.block_timestamp)
    }

    /// Returns the current epoch height.
    ///
    /// # Cost
    ///
    /// `base`
    pub fn epoch_height(&mut self) -> Result<EpochHeight> {
        self.result_state.gas_counter.pay_base(base)?;
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
        self.result_state.gas_counter.pay_base(base)?;
        let account_id = self.read_and_parse_account_id(account_id_ptr, account_id_len)?;
        self.result_state.gas_counter.pay_base(validator_stake_base)?;
        let balance = self.ext.validator_stake(&account_id)?.unwrap_or_default();
        self.memory.set_u128(&mut self.result_state.gas_counter, stake_ptr, balance)
    }

    /// Get the total validator stake of the current epoch.
    /// Write the u128 value into `stake_ptr`.
    /// writes the value into the` u128` variable pointed by `stake_ptr`.
    ///
    /// # Cost
    ///
    /// `base + memory_write_base + memory_write_size * 16 + validator_total_stake_base`
    pub fn validator_total_stake(&mut self, stake_ptr: u64) -> Result<()> {
        self.result_state.gas_counter.pay_base(base)?;
        self.result_state.gas_counter.pay_base(validator_total_stake_base)?;
        let total_stake = self.ext.validator_total_stake()?;
        self.memory.set_u128(&mut self.result_state.gas_counter, stake_ptr, total_stake)
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
        self.result_state.gas_counter.pay_base(base)?;
        Ok(self.result_state.current_storage_usage)
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
        self.result_state.gas_counter.pay_base(base)?;
        self.memory.set_u128(
            &mut self.result_state.gas_counter,
            balance_ptr,
            self.result_state.current_account_balance,
        )
    }

    /// The current amount of tokens locked due to staking.
    ///
    /// # Cost
    ///
    /// `base + memory_write_base + memory_write_size * 16`
    pub fn account_locked_balance(&mut self, balance_ptr: u64) -> Result<()> {
        self.result_state.gas_counter.pay_base(base)?;
        self.memory.set_u128(
            &mut self.result_state.gas_counter,
            balance_ptr,
            self.current_account_locked_balance,
        )
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
        self.result_state.gas_counter.pay_base(base)?;

        self.memory.set_u128(
            &mut self.result_state.gas_counter,
            balance_ptr,
            self.context.attached_deposit,
        )
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
        self.result_state.gas_counter.pay_base(base)?;
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
        self.result_state.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView { method_name: "used_gas".to_string() }.into());
        }
        Ok(self.result_state.gas_counter.used_gas())
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
    ///
    /// cspell:words Pippenger
    pub fn alt_bn128_g1_multiexp(
        &mut self,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> Result<()> {
        self.result_state.gas_counter.pay_base(alt_bn128_g1_multiexp_base)?;
        let data = get_memory_or_register!(self, value_ptr, value_len)?;

        let elements = super::alt_bn128::split_elements(&data)?;
        self.result_state
            .gas_counter
            .pay_per(alt_bn128_g1_multiexp_element, elements.len() as u64)?;

        let res = super::alt_bn128::g1_multiexp(elements)?;

        self.registers.set(
            &mut self.result_state.gas_counter,
            &self.config.limit_config,
            register_id,
            res,
        )
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
    ///   `[(u8, (u256, u256))]` slice. `0u8` is positive sign,
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
        self.result_state.gas_counter.pay_base(alt_bn128_g1_sum_base)?;
        let data = get_memory_or_register!(self, value_ptr, value_len)?;

        let elements = super::alt_bn128::split_elements(&data)?;
        self.result_state.gas_counter.pay_per(alt_bn128_g1_sum_element, elements.len() as u64)?;

        let res = super::alt_bn128::g1_sum(elements)?;

        self.registers.set(
            &mut self.result_state.gas_counter,
            &self.config.limit_config,
            register_id,
            res,
        )
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
    /// If `value_len + value_ptr` points outside the memory or the registers
    /// use more memory than the limit the function returns `MemoryAccessViolation`.
    ///
    /// If point coordinates are not on curve, point is not in the subgroup, scalar
    /// is not in the field or data are wrong serialized, for example,
    /// `value.len()%192!=0`, the function returns `AltBn128InvalidInput`.
    ///
    /// # Cost
    ///
    /// `base + write_register_base + write_register_byte * num_bytes + alt_bn128_pairing_base + alt_bn128_pairing_element * num_elements`
    pub fn alt_bn128_pairing_check(&mut self, value_len: u64, value_ptr: u64) -> Result<u64> {
        self.result_state.gas_counter.pay_base(alt_bn128_pairing_check_base)?;
        let data = get_memory_or_register!(self, value_ptr, value_len)?;

        let elements = super::alt_bn128::split_elements(&data)?;
        self.result_state
            .gas_counter
            .pay_per(alt_bn128_pairing_check_element, elements.len() as u64)?;

        let res = super::alt_bn128::pairing_check(elements)?;

        Ok(res as u64)
    }

    bls12381_impl!(
        r"Calculates the sum of signed elements on the BLS12-381 curve.
It accepts an arbitrary number of pairs (sign_i, p_i),
where p_i from E(Fp) and sign_i is 0 or 1.
It calculates sum_i (-1)^{sign_i} * p_i

# Arguments

* `value` -  sequence of (sign:bool, p:E(Fp)), where
  p is point (x:Fp, y:Fp) on BLS12-381,
  BLS12-381 is Y^2 = X^3 + 4 curve over Fp.

  `value` is encoded as packed `[(u8, ([u8;48], [u8;48]))]` slice.
  `0u8` is positive sign, `1u8` -- negative.
  Elements from Fp encoded as big-endian [u8;48].

# Output

If the input data is correct returns 0 and the 96 bytes represent
the resulting points from E(Fp) which will be written to the register with
the register_id identifier

If one of the points not on the curve,
the sign or points are incorrectly encoded then 1 will be returned
and nothing will be written to the register.

# Errors

If `value_len + value_ptr` points outside the memory or the registers
use more memory than the limit the function returns `MemoryAccessViolation`.

If `value_len % 97 != 0`, the function returns `BLS12381InvalidInput`.

# Cost

`base + write_register_base + write_register_byte * num_bytes +
bls12381_p1_sum_base + bls12381_p1_sum_element * num_elements`
",
        bls12381_p1_sum,
        97,
        bls12381_p1_sum_base,
        bls12381_p1_sum_element,
        p1_sum
    );

    bls12381_impl!(
        r"Calculates the sum of signed elements on the twisted BLS12-381 curve.
It accepts an arbitrary number of pairs (sign_i, p_i),
where p_i from E'(Fp^2) and sign_i is 0 or 1.
It calculates sum_i (-1)^{sign_i} * p_i

# Arguments

* `value` -  sequence of (sign:bool, p:E'(Fp^2)), where
  p is point (x:Fp^2, y:Fp^2) on twisted BLS12-381,
  twisted BLS12-381 is Y^2 = X^3 + 4(u + 1) curve over Fp^2.

`value` is encoded as packed `[(u8, ([u8;96], [u8;96]))]` slice.
`0u8` is positive, `1u8` is negative.
Elements q = c0 + c1 * u from Fp^2 encoded as concatenation of c1 and c0,
where c1 and c0 from Fp and encoded as big-endian [u8;48].

# Output

If the input data is correct returns 0 and the 192 bytes represent
the resulting points from E'(Fp^2) which will be written to the register with
the register_id identifier

If one of the points not on the curve,
the sign or points are incorrectly encoded then 1 will be returned
and nothing will be written to the register.

# Errors

If `value_len + value_ptr` points outside the memory or the registers
use more memory than the limit the function returns `MemoryAccessViolation`.

If `value_len % 193 != 0`, the function returns `BLS12381InvalidInput`.

# Cost

`base + write_register_base + write_register_byte * num_bytes +
bls12381_p2_sum_base + bls12381_p2_sum_element * num_elements`
",
        bls12381_p2_sum,
        193,
        bls12381_p2_sum_base,
        bls12381_p2_sum_element,
        p2_sum
    );

    bls12381_impl!(
        r"Calculates multiexp on BLS12-381 curve:
accepts an arbitrary number of pairs (p_i, s_i),
where p_i from G1 and s_i is a scalar and
calculates sum_i s_i*p_i

# Arguments

* `value` -  sequence of (p:E(Fp), s:u256), where
   p is point (x:Fp, y:Fp) on BLS12-381,
   BLS12-381 is Y^2 = X^3 + 4 curve over Fp.

   `value` is encoded as packed `[(([u8;48], [u8;48]), [u8;32])]` slice.
   Elements from Fp encoded as big-endian [u8;48].
   Scalars encoded as little-endian [u8;32].

# Output

If the input data is correct returns 0 and the 96 bytes represent
the resulting points from G1 which will be written to the register with
the register_id identifier

If one of the points not from G1 subgroup
or points are incorrectly encoded then 1 will be returned
and nothing will be written to the register.

# Errors

If `value_len + value_ptr` points outside the memory or the registers
use more memory than the limit the function returns `MemoryAccessViolation`.

If `value_len % 128 != 0`, the function returns `BLS12381InvalidInput`.

# Cost

`base + write_register_base + write_register_byte * num_bytes +
bls12381_g1_multiexp_base + bls12381_g1_multiexp_element * num_elements`
",
        bls12381_g1_multiexp,
        128,
        bls12381_g1_multiexp_base,
        bls12381_g1_multiexp_element,
        g1_multiexp
    );

    bls12381_impl!(
        r"Calculates multiexp on twisted BLS12-381 curve:
accepts an arbitrary number of pairs (p_i, s_i),
where p_i from G2 and s_i is a scalar and
calculates sum_i s_i*p_i

# Arguments

* `value` -  sequence of (p:E'(Fp^2), s:u256), where
  p is point (x:Fp^2, y:Fp^2) on twisted BLS12-381,
  BLS12-381 is Y^2 = X^3 + 4(u + 1) curve over Fp^2.

`value` is encoded as packed `[(([u8;96], [u8;96]), [u8;32])]` slice.
Elements q = c0 + c1 * u from Fp^2 encoded as concatenation of c1 and c0,
where c1 and c0 from Fp and encoded as big-endian [u8;48].
Scalars encoded as little-endian [u8;32].

# Output

If the input data is correct returns 0 and the 192 bytes represent
the resulting points from G2 which will be written to the register with
the register_id identifier

If one of the points not from G2 subgroup
or points are incorrectly encoded then 1 will be returned
and nothing will be written to the register.

# Errors

If `value_len + value_ptr` points outside the memory or the registers
use more memory than the limit the function returns `MemoryAccessViolation`.

If `value_len % 224 != 0`, the function returns `BLS12381InvalidInput`.

# Cost

`base + write_register_base + write_register_byte * num_bytes +
bls12381_g2_multiexp_base + bls12381_g2_multiexp_element * num_elements`
",
        bls12381_g2_multiexp,
        224,
        bls12381_g2_multiexp_base,
        bls12381_g2_multiexp_element,
        g2_multiexp
    );

    bls12381_impl!(
        r"Maps elements from Fp to the G1 subgroup of BLS12-381 curve.

# Arguments

* `value` -  sequence of p from Fp.

   `value` is encoded as packed `[[u8;48]]` slice.
   Elements from Fp encoded as big-endian [u8;48].

# Output

If the input data is correct returns 0 and the 96*num_elements bytes represent
the resulting points from G1 which will be written to the register with
the register_id identifier

If one of the element >= p, then 1 will be returned
and nothing will be written to the register.

# Errors

If `value_len + value_ptr` points outside the memory or the registers
use more memory than the limit the function returns `MemoryAccessViolation`.

If `value_len % 48 != 0`, the function returns `BLS12381InvalidInput`.

# Cost

`base + write_register_base + write_register_byte * num_bytes +
bls12381_map_fp_to_g1_base + bls12381_map_fp_to_g1_element * num_elements`
",
        bls12381_map_fp_to_g1,
        48,
        bls12381_map_fp_to_g1_base,
        bls12381_map_fp_to_g1_element,
        map_fp_to_g1
    );

    bls12381_impl!(
        r"Maps elements from Fp^2 to the G2 subgroup of twisted BLS12-381 curve.

# Arguments

* `value` -  sequence of p from Fp^2.

`value` is encoded as packed `[[u8;96]]` slice.
Elements q = c0 + c1 * u from Fp^2 encoded as concatenation of c1 and c0,
where c1 and c0 from Fp and encoded as big-endian [u8;48].

# Output

If the input data is correct returns 0 and the 192*num_elements bytes represent
the resulting points from G2 which will be written to the register with
the register_id identifier

If one of the element not valid Fp^2, then 1 will be returned
and nothing will be written to the register.

# Errors

If `value_len + value_ptr` points outside the memory or the registers
use more memory than the limit the function returns `MemoryAccessViolation`.

If `value_len % 96 != 0`, the function returns `BLS12381InvalidInput`.

# Cost
`base + write_register_base + write_register_byte * num_bytes +
bls12381_map_fp2_to_g2_base + bls12381_map_fp2_to_g2_element * num_elements`
",
        bls12381_map_fp2_to_g2,
        96,
        bls12381_map_fp2_to_g2_base,
        bls12381_map_fp2_to_g2_element,
        map_fp2_to_g2
    );

    /// Computes pairing check on BLS12-381 curve.
    /// In other words, computes whether \sum_i e(g_{1 i}, g_{2 i})
    /// is equal to one (in additive notation), where e(g1, g2) is the pairing function
    ///
    /// # Arguments
    ///
    /// * `value` -  sequence of (g1:G1, g2:G2), where
    ///    g1 is point (x:Fp, y:Fp) on BLS12-381,
    ///    BLS12-381 is Y^2 = X^3 + 4 curve over Fp.
    ///    g2 is point (x:Fp^2, y:Fp^2) on twisted BLS12-381,
    ///    twisted BLS12-381 is Y^2 = X^3 + 4(u + 1) curve over Fp^2.
    ///
    ///   `value` is encoded as packed `[(([u8;48], [u8;48]), ([u8;96], [u8;96]))]` slice.
    ///    Elements from Fp encoded as big-endian [u8;48].
    ///    Elements q = c0 + c1 * u from Fp^2 encoded as concatenation of c1 and c0,
    ///    where c1 and c0 from Fp.
    ///
    /// # Output
    ///
    /// If the input data is correct and
    /// the pairing result equals the multiplicative identity returns 0.
    ///
    /// If one of the points not on the curve, not from G1/G2 or
    /// incorrectly encoded then 1 will be returned
    ///
    /// If the input data is correct and
    /// the pairing result does NOT equal the multiplicative identity returns 2.
    ///
    /// # Errors
    ///
    /// If `value_len + value_ptr` points outside the memory or the registers
    /// use more memory than the limit the function returns `MemoryAccessViolation`.
    ///
    /// If `value_len % 288 != 0`, the function returns `BLS12381InvalidInput`.
    ///
    /// # Cost
    /// `base + write_register_base + write_register_byte * num_bytes +
    ///   bls12381_pairing_base + bls12381_pairing_element * num_elements`
    pub fn bls12381_pairing_check(&mut self, value_len: u64, value_ptr: u64) -> Result<u64> {
        self.result_state.gas_counter.pay_base(bls12381_pairing_base)?;

        const BLS_P1_SIZE: usize = 96;
        const BLS_P2_SIZE: usize = 192;
        const ITEM_SIZE: usize = BLS_P1_SIZE + BLS_P2_SIZE;

        let data = get_memory_or_register!(self, value_ptr, value_len)?;
        let elements_count = data.len() / ITEM_SIZE;

        self.result_state.gas_counter.pay_per(bls12381_pairing_element, elements_count as u64)?;

        super::bls12381::pairing_check(&data)
    }

    bls12381_impl!(
        r"Decompress points from BLS12-381 curve.

# Arguments

* `value` -  sequence of p:E(Fp), where
   p is point in compressed format on BLS12-381,
   BLS12-381 is Y^2 = X^3 + 4 curve over Fp.

   `value` is encoded as packed `[[u8;48]]` slice.
   Where points (x: Fp, y: Fp) from E(Fp) encoded as
   [u8; 48] -- big-endian x: Fp. y determined by the formula y=+-sqrt(x^3 + 4)

   The highest bit should be set as 1, the second-highest bit marks the point at infinity,
   The third-highest bit represent the sign of y (0 for positive).

# Output

If the input data is correct returns 0 and the 96*num_elements bytes represent
the resulting uncompressed points from E(Fp) which will be written to the register with
the register_id identifier

If one of the points not on the curve
or points are incorrectly encoded then 1 will be returned
and nothing will be written to the register.

# Errors

If `value_len + value_ptr` points outside the memory or the registers
use more memory than the limit the function returns `MemoryAccessViolation`.

If `value_len % 48 != 0`, the function returns `BLS12381InvalidInput`.

# Cost
`base + write_register_base + write_register_byte * num_bytes +
bls12381_p1_decompress_base + bls12381_p1_decompress_element * num_elements`
",
        bls12381_p1_decompress,
        48,
        bls12381_p1_decompress_base,
        bls12381_p1_decompress_element,
        p1_decompress
    );

    bls12381_impl!(
        r"Decompress points from twisted BLS12-381 curve.

# Arguments

* `value` -  sequence of p:E'(Fp^2), where
   p is point in compressed format on twisted BLS12-381,
   twisted BLS12-381 is Y^2 = X^3 + 4(u + 1) curve over Fp^2.

   `value` is encoded as packed `[[u8;96]]` slice.
   Where points (x: Fp^2, y: Fp^2) from E'(Fp^2) encoded as
   [u8; 96] -- x: Fp^2. y determined by the formula y=+-sqrt(x^3 + 4(u + 1))

   Elements q = c0 + c1 * u from Fp^2 encoded as concatenation of c1 and c0,
   where c1 and c0 from Fp and encoded as big-endian [u8;48].

   The highest bit should be set as 1, the second-highest bit marks the point at infinity,
   The third-highest bit represent the sign of y (0 for positive).

# Output

If the input data is correct returns 0 and the 192*num_elements bytes represent
the resulting uncompressed points from E'(Fp^2) which will be written to the register with
the register_id identifier

If one of the points not on the curve
or points are incorrectly encoded then 1 will be returned
and nothing will be written to the register.

# Errors

If `value_len + value_ptr` points outside the memory or the registers
use more memory than the limit the function returns `MemoryAccessViolation`.

If `value_len % 96 != 0`, the function returns `BLS12381InvalidInput`.

# Cost

`base + write_register_base + write_register_byte * num_bytes +
bls12381_p2_decompress_base + bls12381_p2_decompress_element * num_elements`
        ",
        bls12381_p2_decompress,
        96,
        bls12381_p2_decompress_base,
        bls12381_p2_decompress_element,
        p2_decompress
    );

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
        self.result_state.gas_counter.pay_base(base)?;
        self.registers.set(
            &mut self.result_state.gas_counter,
            &self.config.limit_config,
            register_id,
            self.context.random_seed.as_slice(),
        )
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
        self.result_state.gas_counter.pay_base(sha256_base)?;
        let value = get_memory_or_register!(self, value_ptr, value_len)?;
        self.result_state.gas_counter.pay_per(sha256_byte, value.len() as u64)?;

        use sha2::Digest;

        let value_hash = sha2::Sha256::digest(&value);
        self.registers.set(
            &mut self.result_state.gas_counter,
            &self.config.limit_config,
            register_id,
            value_hash.as_slice(),
        )
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
        self.result_state.gas_counter.pay_base(keccak256_base)?;
        let value = get_memory_or_register!(self, value_ptr, value_len)?;
        self.result_state.gas_counter.pay_per(keccak256_byte, value.len() as u64)?;

        use sha3::Digest;

        let value_hash = sha3::Keccak256::digest(&value);
        self.registers.set(
            &mut self.result_state.gas_counter,
            &self.config.limit_config,
            register_id,
            value_hash.as_slice(),
        )
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
        self.result_state.gas_counter.pay_base(keccak512_base)?;
        let value = get_memory_or_register!(self, value_ptr, value_len)?;
        self.result_state.gas_counter.pay_per(keccak512_byte, value.len() as u64)?;

        use sha3::Digest;

        let value_hash = sha3::Keccak512::digest(&value);
        self.registers.set(
            &mut self.result_state.gas_counter,
            &self.config.limit_config,
            register_id,
            value_hash.as_slice(),
        )
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
        self.result_state.gas_counter.pay_base(ripemd160_base)?;
        let value = get_memory_or_register!(self, value_ptr, value_len)?;

        let message_blocks = value
            .len()
            .checked_add(8)
            .ok_or(VMLogicError::HostError(HostError::IntegerOverflow))?
            / 64
            + 1;

        self.result_state.gas_counter.pay_per(ripemd160_block, message_blocks as u64)?;

        use ripemd::Digest;

        let value_hash = ripemd::Ripemd160::digest(&value);
        self.registers.set(
            &mut self.result_state.gas_counter,
            &self.config.limit_config,
            register_id,
            value_hash.as_slice(),
        )
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
        self.result_state.gas_counter.pay_base(ecrecover_base)?;

        let signature = {
            let vec = get_memory_or_register!(self, sig_ptr, sig_len)?;
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
            let vec = get_memory_or_register!(self, hash_ptr, hash_len)?;
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
            self.registers.set(
                &mut self.result_state.gas_counter,
                &self.config.limit_config,
                register_id,
                pk.as_ref(),
            )?;
            return Ok(true as u64);
        };

        Ok(false as u64)
    }

    /// Verify an ED25519 signature given a message and a public key.
    ///
    /// Returns a bool indicating success (1) or failure (0) as a `u64`.
    ///
    /// # Errors
    ///
    /// * If the public key's size is not equal to 32, or signature size is not
    ///   equal to 64, returns [HostError::Ed25519VerifyInvalidInput].
    /// * If any of the signature, message or public key arguments are out of
    ///   memory bounds, returns [`HostError::MemoryAccessViolation`]
    ///
    /// # Cost
    ///
    /// Each input can either be in memory or in a register. Set the length of
    /// the input to `u64::MAX` to declare that the input is a register number
    /// and not a pointer. Each input has a gas cost input_cost(num_bytes) that
    /// depends on whether it is from memory or from a register. It is either
    /// read_memory_base + num_bytes * read_memory_byte in the former case or
    /// read_register_base + num_bytes * read_register_byte in the latter. This
    /// function is labeled as `input_cost` below.
    ///
    /// `input_cost(num_bytes_signature) + input_cost(num_bytes_message) +
    ///  input_cost(num_bytes_public_key) + ed25519_verify_base +
    ///  ed25519_verify_byte * num_bytes_message`
    pub fn ed25519_verify(
        &mut self,
        signature_len: u64,
        signature_ptr: u64,
        message_len: u64,
        message_ptr: u64,
        public_key_len: u64,
        public_key_ptr: u64,
    ) -> Result<u64> {
        use ed25519_dalek::Verifier;

        self.result_state.gas_counter.pay_base(ed25519_verify_base)?;

        let signature: ed25519_dalek::Signature = {
            let vec = get_memory_or_register!(self, signature_ptr, signature_len)?;
            let b = <&[u8; ed25519_dalek::SIGNATURE_LENGTH]>::try_from(&vec[..]).map_err(|_| {
                VMLogicError::HostError(HostError::Ed25519VerifyInvalidInput {
                    msg: "invalid signature length".to_string(),
                })
            })?;
            // Sanity-check that was performed by ed25519-dalek in from_bytes before version 2,
            // but was removed with version 2. It is not actually any good a check, but we need
            // it to avoid costs changing.
            if b[ed25519_dalek::SIGNATURE_LENGTH - 1] & 0b1110_0000 != 0 {
                return Ok(false as u64);
            }
            ed25519_dalek::Signature::from_bytes(b)
        };

        let message = get_memory_or_register!(self, message_ptr, message_len)?;
        self.result_state.gas_counter.pay_per(ed25519_verify_byte, message.len() as u64)?;

        let public_key: ed25519_dalek::VerifyingKey = {
            let vec = get_memory_or_register!(self, public_key_ptr, public_key_len)?;
            let b =
                <&[u8; ed25519_dalek::PUBLIC_KEY_LENGTH]>::try_from(&vec[..]).map_err(|_| {
                    VMLogicError::HostError(HostError::Ed25519VerifyInvalidInput {
                        msg: "invalid public key length".to_string(),
                    })
                })?;
            match ed25519_dalek::VerifyingKey::from_bytes(b) {
                Ok(public_key) => public_key,
                Err(_) => return Ok(false as u64),
            }
        };

        match public_key.verify(&message, &signature) {
            Err(_) => Ok(false as u64),
            Ok(()) => Ok(true as u64),
        }
    }

    /// Consume gas. Counts both towards `burnt_gas` and `used_gas`.
    ///
    /// # Errors
    ///
    /// * If passed gas amount somehow overflows internal gas counters returns `IntegerOverflow`;
    /// * If we exceed usage limit imposed on burnt gas returns `GasLimitExceeded`;
    /// * If we exceed the `prepaid_gas` then returns `GasExceeded`.
    pub fn gas(&mut self, gas: Gas) -> Result<()> {
        self.result_state.gas_counter.burn_gas(Gas::from(gas))
    }

    pub fn gas_opcodes(&mut self, opcodes: u32) -> Result<()> {
        self.gas(opcodes as u64 * self.config.regular_op_cost as u64)
    }

    /// An alias for [`VMLogic::gas`].
    pub fn burn_gas(&mut self, gas: Gas) -> Result<()> {
        self.gas(gas)
    }

    /// This is the function that is exposed to WASM contracts under the name `gas`.
    ///
    /// For now it is consuming the gas for `gas` opcodes. When we switch to finite-wasm it’ll
    /// be made to be a no-op.
    ///
    /// This function might be intrinsified.
    pub fn gas_seen_from_wasm(&mut self, opcodes: u32) -> Result<()> {
        self.gas_opcodes(opcodes)
    }

    #[cfg(feature = "test_features")]
    pub fn sleep_nanos(&mut self, nanos: u64) -> Result<()> {
        let duration = std::time::Duration::from_nanos(nanos);
        std::thread::sleep(duration);
        Ok(())
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
    /// `burnt_gas := dispatch cost of the receipt + base dispatch cost of the data receipt`
    /// `used_gas := burnt_gas + exec cost of the receipt + base exec cost of the data receipt`
    /// Notice that we prepay all base cost upon the creation of the data dependency, we are going to
    /// pay for the content transmitted through the dependency upon the actual creation of the
    /// DataReceipt.
    fn pay_gas_for_new_receipt(&mut self, sir: bool, data_dependencies: &[bool]) -> Result<()> {
        let fees_config_cfg = &self.fees_config;
        let mut burn_gas = fees_config_cfg.fee(ActionCosts::new_action_receipt).send_fee(sir);
        let mut use_gas = fees_config_cfg.fee(ActionCosts::new_action_receipt).exec_fee();
        for dep in data_dependencies {
            // Both creation and execution for data receipts are considered burnt gas.
            burn_gas = burn_gas
                .checked_add(fees_config_cfg.fee(ActionCosts::new_data_receipt_base).send_fee(*dep))
                .ok_or(HostError::IntegerOverflow)?
                .checked_add(fees_config_cfg.fee(ActionCosts::new_data_receipt_base).exec_fee())
                .ok_or(HostError::IntegerOverflow)?;
        }
        use_gas = use_gas.checked_add(burn_gas).ok_or(HostError::IntegerOverflow)?;
        // This should go to `new_data_receipt_base` and `new_action_receipt` in parts.
        // But we have to keep charing these two together unless we make a protocol change.
        self.result_state.gas_counter.pay_action_accumulated(
            burn_gas,
            use_gas,
            ActionCosts::new_action_receipt,
        )
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
    /// `promise_create` is a convenience wrapper around `promise_batch_create` and
    /// `promise_batch_action_function_call`. This means it charges the `base` cost twice.
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
    /// `promise_then` is a convenience wrapper around `promise_batch_then` and
    /// `promise_batch_action_function_call`. This means it charges the `base` cost twice.
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
        self.result_state.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(
                HostError::ProhibitedInView { method_name: "promise_and".to_string() }.into()
            );
        }
        self.result_state.gas_counter.pay_base(promise_and_base)?;
        let memory_len = promise_idx_count
            .checked_mul(size_of::<u64>() as u64)
            .ok_or(HostError::IntegerOverflow)?;
        self.result_state.gas_counter.pay_per(promise_and_per_promise, memory_len)?;

        // Read indices as little endian u64.
        let promise_indices = self.memory.view(
            &mut self.result_state.gas_counter,
            MemSlice { ptr: promise_idx_ptr, len: memory_len },
        )?;
        let promise_indices = stdx::as_chunks_exact::<{ size_of::<u64>() }, u8>(&promise_indices)
            .unwrap()
            .into_iter()
            .map(|bytes| u64::from_le_bytes(*bytes));

        let mut receipt_dependencies = vec![];
        for promise_idx in promise_indices {
            let promise = self
                .promises
                .get(promise_idx as usize)
                .ok_or(HostError::InvalidPromiseIndex { promise_idx })?;
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
        self.result_state.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_batch_create".to_string(),
            }
            .into());
        }
        let account_id = self.read_and_parse_account_id(account_id_ptr, account_id_len)?;
        let sir = account_id == self.context.current_account_id;
        self.pay_gas_for_new_receipt(sir, &[])?;
        let new_receipt_idx = self.ext.create_action_receipt(vec![], account_id)?;

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
        self.result_state.gas_counter.pay_base(base)?;
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
            .map(|&receipt_idx| self.ext.get_receipt_receiver(receipt_idx) == &account_id)
            .collect();
        self.pay_gas_for_new_receipt(sir, &deps)?;

        let new_receipt_idx = self.ext.create_action_receipt(receipt_dependencies, account_id)?;

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

        let account_id = self.ext.get_receipt_receiver(receipt_idx);
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
        self.result_state.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_batch_action_create_account".to_string(),
            }
            .into());
        }
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        self.pay_action_base(ActionCosts::create_account, sir)?;

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
        self.result_state.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_batch_action_deploy_contract".to_string(),
            }
            .into());
        }
        let code = get_memory_or_register!(self, code_ptr, code_len)?;
        let code_len = code.len() as u64;
        let limit = self.config.limit_config.max_contract_size;
        if code_len > limit {
            return Err(HostError::ContractSizeExceeded { size: code_len, limit }.into());
        }
        let code = code.into_owned();

        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        self.pay_action_base(ActionCosts::deploy_contract_base, sir)?;
        self.pay_action_per_byte(ActionCosts::deploy_contract_byte, code_len, sir)?;

        self.ext.append_action_deploy_contract(receipt_idx, code)?;
        Ok(())
    }

    /// Appends `DeployGlobalContract` action to the batch of actions for the given promise
    /// pointed by `promise_idx`.
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
    pub fn promise_batch_action_deploy_global_contract(
        &mut self,
        promise_idx: u64,
        code_len: u64,
        code_ptr: u64,
    ) -> Result<()> {
        self.promise_batch_action_deploy_global_contract_impl(
            promise_idx,
            code_len,
            code_ptr,
            GlobalContractDeployMode::CodeHash,
            "promise_batch_action_deploy_global_contract",
        )
    }

    /// Appends `DeployGlobalContractByAccountId` action to the batch of actions for the given
    /// promise pointed by `promise_idx`.
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
    pub fn promise_batch_action_deploy_global_contract_by_account_id(
        &mut self,
        promise_idx: u64,
        code_len: u64,
        code_ptr: u64,
    ) -> Result<()> {
        self.promise_batch_action_deploy_global_contract_impl(
            promise_idx,
            code_len,
            code_ptr,
            GlobalContractDeployMode::AccountId,
            "promise_batch_action_deploy_global_contract_by_account_id",
        )
    }

    fn promise_batch_action_deploy_global_contract_impl(
        &mut self,
        promise_idx: u64,
        code_len: u64,
        code_ptr: u64,
        mode: GlobalContractDeployMode,
        method_name: &str,
    ) -> Result<()> {
        self.result_state.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView { method_name: method_name.to_owned() }.into());
        }
        let code = get_memory_or_register!(self, code_ptr, code_len)?;
        let code_len = code.len() as u64;
        let limit = self.config.limit_config.max_contract_size;
        if code_len > limit {
            return Err(HostError::ContractSizeExceeded { size: code_len, limit }.into());
        }
        let code = code.into_owned();

        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        self.pay_action_base(ActionCosts::deploy_global_contract_base, sir)?;
        self.pay_action_per_byte(ActionCosts::deploy_global_contract_byte, code_len, sir)?;

        self.ext.append_action_deploy_global_contract(receipt_idx, code, mode)?;
        Ok(())
    }

    /// Appends `UseGlobalContract` action to the batch of actions for the given promise
    /// pointed by `promise_idx`.
    ///
    /// # Errors
    ///
    /// * If `promise_idx` does not correspond to an existing promise returns `InvalidPromiseIndex`.
    /// * If the promise pointed by the `promise_idx` is an ephemeral promise created by
    /// `promise_and` returns `CannotAppendActionToJointPromise`.
    /// * If called as view function returns `ProhibitedInView`.
    /// * If `code_hash_len + code_hash_ptr` points outside the memory of the guest or host returns
    /// `MemoryAccessViolation`.
    /// * If a malformed code hash is passed, returns `ContractCodeHashMalformed`.
    ///
    /// # Cost
    ///
    /// `burnt_gas := base + dispatch action base fee + dispatch action per byte fee * num bytes + cost of reading vector from memory `
    /// `used_gas := burnt_gas + exec action base fee + exec action per byte fee * num bytes`
    pub fn promise_batch_action_use_global_contract(
        &mut self,
        promise_idx: u64,
        code_hash_len: u64,
        code_hash_ptr: u64,
    ) -> Result<()> {
        self.promise_batch_action_use_global_contract_impl(
            promise_idx,
            GlobalContractIdentifierPtrData::CodeHash { code_hash_len, code_hash_ptr },
            "promise_batch_action_use_global_contract",
        )
    }

    /// Appends `UseGlobalContract` action to the batch of actions for the given promise
    /// pointed by `promise_idx`.
    ///
    /// # Errors
    ///
    /// * If `promise_idx` does not correspond to an existing promise returns `InvalidPromiseIndex`.
    /// * If the promise pointed by the `promise_idx` is an ephemeral promise created by
    /// `promise_and` returns `CannotAppendActionToJointPromise`.
    /// * If called as view function returns `ProhibitedInView`.
    /// * If `account_id_len + account_id_ptr` points outside the memory of the guest or host returns
    /// `MemoryAccessViolation`.
    /// * If account_id string is not UTF-8 returns `BadUtf8`.
    ///
    /// # Cost
    ///
    /// `burnt_gas := base + dispatch action base fee + dispatch action per byte fee * num bytes
    /// + cost of reading vector from memory + cost of reading and parsing account name`
    /// `used_gas := burnt_gas + exec action base fee + exec action per byte fee * num bytes`
    pub fn promise_batch_action_use_global_contract_by_account_id(
        &mut self,
        promise_idx: u64,
        account_id_len: u64,
        account_id_ptr: u64,
    ) -> Result<()> {
        self.promise_batch_action_use_global_contract_impl(
            promise_idx,
            GlobalContractIdentifierPtrData::AccountId { account_id_len, account_id_ptr },
            "promise_batch_action_use_global_contract_by_account_id",
        )
    }

    fn promise_batch_action_use_global_contract_impl(
        &mut self,
        promise_idx: u64,
        contract_id_ptr: GlobalContractIdentifierPtrData,
        method_name: &str,
    ) -> Result<()> {
        self.result_state.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView { method_name: method_name.to_owned() }.into());
        }
        let contract_id = match contract_id_ptr {
            GlobalContractIdentifierPtrData::CodeHash { code_hash_len, code_hash_ptr } => {
                let code_hash_bytes = get_memory_or_register!(self, code_hash_ptr, code_hash_len)?;
                let code_hash: [_; CryptoHash::LENGTH] = (&*code_hash_bytes)
                    .try_into()
                    .map_err(|_| HostError::ContractCodeHashMalformed)?;
                GlobalContractIdentifier::CodeHash(CryptoHash(code_hash))
            }
            GlobalContractIdentifierPtrData::AccountId { account_id_len, account_id_ptr } => {
                let account_id = self.read_and_parse_account_id(account_id_ptr, account_id_len)?;
                GlobalContractIdentifier::AccountId(account_id)
            }
        };

        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        self.pay_action_base(ActionCosts::use_global_contract_base, sir)?;
        let len = contract_id.len() as u64;
        self.pay_action_per_byte(ActionCosts::use_global_contract_byte, len, sir)?;

        self.ext.append_action_use_global_contract(receipt_idx, contract_id)?;
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
        self.result_state.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_batch_action_function_call".to_string(),
            }
            .into());
        }
        let amount = self.memory.get_u128(&mut self.result_state.gas_counter, amount_ptr)?;
        let method_name = get_memory_or_register!(self, method_name_ptr, method_name_len)?;
        if method_name.is_empty() {
            return Err(HostError::EmptyMethodName.into());
        }
        let arguments = get_memory_or_register!(self, arguments_ptr, arguments_len)?;

        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        let method_name = method_name.into_owned();
        let arguments = arguments.into_owned();
        // Input can't be large enough to overflow
        let num_bytes = method_name.len() as u64 + arguments.len() as u64;
        self.pay_action_base(ActionCosts::function_call_base, sir)?;
        self.pay_action_per_byte(ActionCosts::function_call_byte, num_bytes, sir)?;
        // Prepaid gas
        self.result_state.gas_counter.prepay_gas(gas)?;
        self.result_state.deduct_balance(amount)?;
        self.ext.append_action_function_call_weight(
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
        self.result_state.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_batch_action_transfer".to_string(),
            }
            .into());
        }
        let amount = self.memory.get_u128(&mut self.result_state.gas_counter, amount_ptr)?;

        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;
        let receiver_id = self.ext.get_receipt_receiver(receipt_idx);
        let send_fee = transfer_send_fee(
            &self.fees_config,
            sir,
            self.config.implicit_account_creation,
            self.config.eth_implicit_accounts,
            receiver_id.get_account_type(),
        );
        let exec_fee = transfer_exec_fee(
            &self.fees_config,
            self.config.implicit_account_creation,
            self.config.eth_implicit_accounts,
            receiver_id.get_account_type(),
        );
        let burn_gas = send_fee;
        let use_gas = burn_gas.checked_add(exec_fee).ok_or(HostError::IntegerOverflow)?;
        self.result_state.gas_counter.pay_action_accumulated(
            burn_gas,
            use_gas,
            ActionCosts::transfer,
        )?;
        self.result_state.deduct_balance(amount)?;
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
        self.result_state.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_batch_action_stake".to_string(),
            }
            .into());
        }
        let amount = self.memory.get_u128(&mut self.result_state.gas_counter, amount_ptr)?;
        let public_key = self.get_public_key(public_key_ptr, public_key_len)?;
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;
        self.pay_action_base(ActionCosts::stake, sir)?;
        self.ext.append_action_stake(receipt_idx, amount, public_key.decode()?);
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
        self.result_state.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_batch_action_add_key_with_full_access".to_string(),
            }
            .into());
        }
        let public_key = self.get_public_key(public_key_ptr, public_key_len)?;
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;
        self.pay_action_base(ActionCosts::add_full_access_key, sir)?;
        self.ext.append_action_add_key_with_full_access(receipt_idx, public_key.decode()?, nonce);
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
        self.result_state.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_batch_action_add_key_with_function_call".to_string(),
            }
            .into());
        }
        let public_key = self.get_public_key(public_key_ptr, public_key_len)?;
        let allowance = self.memory.get_u128(&mut self.result_state.gas_counter, allowance_ptr)?;
        let allowance = if allowance > 0 { Some(allowance) } else { None };
        let receiver_id = self.read_and_parse_account_id(receiver_id_ptr, receiver_id_len)?;
        let raw_method_names = get_memory_or_register!(self, method_names_ptr, method_names_len)?;
        let method_names = split_method_names(&raw_method_names)?;

        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;

        // +1 is to account for null-terminating characters.
        let num_bytes = method_names.iter().map(|v| v.len() as u64 + 1).sum::<u64>();
        self.pay_action_base(ActionCosts::add_function_call_key_base, sir)?;
        self.pay_action_per_byte(ActionCosts::add_function_call_key_byte, num_bytes, sir)?;

        self.ext.append_action_add_key_with_function_call(
            receipt_idx,
            public_key.decode()?,
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
        self.result_state.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_batch_action_delete_key".to_string(),
            }
            .into());
        }
        let public_key = self.get_public_key(public_key_ptr, public_key_len)?;
        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;
        self.pay_action_base(ActionCosts::delete_key, sir)?;
        self.ext.append_action_delete_key(receipt_idx, public_key.decode()?);
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
        self.result_state.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_batch_action_delete_account".to_string(),
            }
            .into());
        }
        let beneficiary_id =
            self.read_and_parse_account_id(beneficiary_id_ptr, beneficiary_id_len)?;

        let (receipt_idx, sir) = self.promise_idx_to_receipt_idx_with_sir(promise_idx)?;
        self.pay_action_base(ActionCosts::delete_account, sir)?;

        self.ext.append_action_delete_account(receipt_idx, beneficiary_id)?;
        Ok(())
    }

    /// Creates a promise that will execute a method on the current account with given arguments
    /// and gas. The created promise will have a special input data dependency.
    ///
    /// A resumption token is written by this function into the register denoted by `register_id`.
    /// To satisfy the data dependency, call `promise_yield_resume` with the resumption token
    /// and a payload. The provided method will then be executed with input
    /// `PromiseResult::Successful(payload)`.
    ///
    /// The resumption token is portable across transactions, but only the current account
    /// is allowed to resolve this data dependency.
    ///
    /// If `promise_yield_resume` has not been called after a certain protocol-defined number of
    /// of blocks (as defined by the `yield_timeout_length_in_blocks` parameter) the created
    /// promise will instead be executed with input `PromiseResult::Failed`.
    ///
    /// # Errors
    ///
    /// * If `method_name_len + method_name_ptr` or `arguments_len + arguments_ptr` point outside
    /// the memory of the guest or host returns `MemoryAccessViolation`;
    /// * If called as view function returns `ProhibitedInView`;
    /// * Gas is insufficient;
    /// * Too many promises have been created already;
    /// * Resumption token cannot be written to the register `register_id`.
    ///
    /// # Returns
    ///
    /// Index of the new promise that uniquely identifies it within the current execution of the
    /// method.
    ///
    /// # Cost
    ///
    /// The following fees are charged:
    ///
    /// * `base` fee;
    /// * `yield_create_base` fee;
    /// * `yield_create_byte` for each byte of `method_name` and `arguments`;
    /// * Fees for reading the `method_name` and `arguments`;
    /// * Fees for writing the Data ID to the output register;
    /// * Fees for setting up the receipt and the eventual function call of the method.
    pub fn promise_yield_create(
        &mut self,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        gas: Gas,
        gas_weight: u64,
        register_id: u64,
    ) -> Result<u64> {
        self.result_state.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_yield_create".to_string(),
            }
            .into());
        }
        self.result_state.gas_counter.pay_base(yield_create_base)?;

        let method_name = get_memory_or_register!(self, method_name_ptr, method_name_len)?;
        if method_name.is_empty() {
            return Err(HostError::EmptyMethodName.into());
        }
        let arguments = get_memory_or_register!(self, arguments_ptr, arguments_len)?;
        let method_name = method_name.into_owned();
        let arguments = arguments.into_owned();

        // Input can't be large enough to overflow, WebAssembly address space is 32-bits.
        let num_bytes = method_name.len() as u64 + arguments.len() as u64;
        self.result_state.gas_counter.pay_per(yield_create_byte, num_bytes)?;
        // Prepay gas for the callback so that it cannot be used for this execution any longer.
        self.result_state.gas_counter.prepay_gas(gas)?;

        // Here we are creating a receipt with a single data dependency which will then be
        // resolved by the resume call.
        self.pay_gas_for_new_receipt(true, &[true])?;
        let (new_receipt_idx, data_id) =
            self.ext.create_promise_yield_receipt(self.context.current_account_id.clone())?;

        let new_promise_idx = self.checked_push_promise(Promise::Receipt(new_receipt_idx))?;
        self.pay_action_base(ActionCosts::function_call_base, true)?;
        self.pay_action_per_byte(ActionCosts::function_call_byte, num_bytes, true)?;
        self.ext.append_action_function_call_weight(
            new_receipt_idx,
            method_name,
            arguments,
            0,
            gas,
            GasWeight(gas_weight),
        )?;

        self.registers.set(
            &mut self.result_state.gas_counter,
            &self.config.limit_config,
            register_id,
            *data_id.as_bytes(),
        )?;
        Ok(new_promise_idx)
    }

    /// Submits the data for a yield promise which is awaiting its value.
    ///
    /// The `data_id` pair of parameters must refer to a resumption token generated by a call to
    /// the [`promise_yield_create`] made by the same account.
    ///
    /// Returns `1` if submitting the payload for the data dependency was successful. This
    /// guarantees that the yield callback function will be executed with a payload. Otherwise a
    /// `0` is returned.
    ///
    /// # Errors
    ///
    /// * If `data_id_ptr + data_id_ptr` points outside the memory of the guest or host
    /// returns `MemoryAccessViolation`;
    /// * If a malformed data id is passed, returns `DataIdMalformed`;
    /// * If `payload_len` exceeds the maximum permitted returns `YieldPayloadLength`;
    /// * If called as view function returns `ProhibitedInView`;
    /// * Runs out of gas.
    ///
    /// # Cost
    ///
    /// The following fees are charged:
    ///
    /// * `base` fee;
    /// * `yield_resume_base` fee;
    /// * `yield_resume_byte` for each byte of `payload`;
    /// * Fees for reading the `data_id` and `payload`.
    pub fn promise_yield_resume(
        &mut self,
        data_id_len: u64,
        data_id_ptr: u64,
        payload_len: u64,
        payload_ptr: u64,
    ) -> Result<u32, VMLogicError> {
        self.result_state.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_submit_data".to_string(),
            }
            .into());
        }
        self.result_state.gas_counter.pay_base(yield_resume_base)?;
        self.result_state.gas_counter.pay_per(yield_resume_byte, payload_len)?;
        let data_id = get_memory_or_register!(self, data_id_ptr, data_id_len)?;
        let payload = get_memory_or_register!(self, payload_ptr, payload_len)?;
        let payload_len = payload.len() as u64;
        if payload_len > self.config.limit_config.max_yield_payload_size {
            return Err(HostError::YieldPayloadLength {
                length: payload_len,
                limit: self.config.limit_config.max_yield_payload_size,
            }
            .into());
        }

        let data_id: [_; CryptoHash::LENGTH] =
            (&*data_id).try_into().map_err(|_| HostError::DataIdMalformed)?;
        let data_id = CryptoHash(data_id);
        let payload = payload.into_owned();
        self.ext.submit_promise_resume_data(data_id, payload).map(u32::from)
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
        self.result_state.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(HostError::ProhibitedInView {
                method_name: "promise_results_count".to_string(),
            }
            .into());
        }
        Ok(self.context.promise_results.len() as _)
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
        self.result_state.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(
                HostError::ProhibitedInView { method_name: "promise_result".to_string() }.into()
            );
        }
        match self
            .context
            .promise_results
            .get(result_idx as usize)
            .ok_or(HostError::InvalidPromiseResultIndex { result_idx })?
        {
            PromiseResult::NotReady => Ok(0),
            PromiseResult::Successful(data) => {
                self.registers.set(
                    &mut self.result_state.gas_counter,
                    &self.config.limit_config,
                    register_id,
                    data.as_slice(),
                )?;
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
        self.result_state.gas_counter.pay_base(base)?;
        self.result_state.gas_counter.pay_base(promise_return)?;
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
                self.result_state.return_data = ReturnData::ReceiptIndex(*receipt_idx);
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
        self.result_state.gas_counter.pay_base(base)?;
        let return_val = get_memory_or_register!(self, value_ptr, value_len)?;
        let mut burn_gas: Gas = 0;
        let num_bytes = return_val.len() as u64;
        if num_bytes > self.config.limit_config.max_length_returned_data {
            return Err(HostError::ReturnedValueLengthExceeded {
                length: num_bytes,
                limit: self.config.limit_config.max_length_returned_data,
            }
            .into());
        }
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
                    self.fees_config
                        .fee(ActionCosts::new_data_receipt_byte)
                        .send_fee(sir)
                        .checked_add(
                            self.fees_config.fee(ActionCosts::new_data_receipt_byte).exec_fee(),
                        )
                        .ok_or(HostError::IntegerOverflow)?
                        .checked_mul(num_bytes)
                        .ok_or(HostError::IntegerOverflow)?,
                )
                .ok_or(HostError::IntegerOverflow)?;
        }
        self.result_state.gas_counter.pay_action_accumulated(
            burn_gas,
            burn_gas,
            ActionCosts::new_data_receipt_byte,
        )?;
        self.result_state.return_data = ReturnData::Value(return_val.into_owned());
        Ok(())
    }

    /// Terminates the execution of the program with panic `GuestPanic`.
    ///
    /// # Cost
    ///
    /// `base`
    pub fn panic(&mut self) -> Result<()> {
        self.result_state.gas_counter.pay_base(base)?;
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
        self.result_state.gas_counter.pay_base(base)?;
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
        self.result_state.gas_counter.pay_base(base)?;
        self.result_state.check_can_add_a_log_message()?;
        let message = self.get_utf8_string(len, ptr)?;
        self.result_state.gas_counter.pay_base(log_base)?;
        self.result_state.gas_counter.pay_per(log_byte, message.len() as u64)?;
        self.result_state.checked_push_log(message)
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
        self.result_state.gas_counter.pay_base(base)?;
        self.result_state.check_can_add_a_log_message()?;
        let message = self.get_utf16_string(len, ptr)?;
        self.result_state.gas_counter.pay_base(log_base)?;
        // Let's not use `encode_utf16` for gas per byte here, since it's a lot of compute.
        self.result_state.gas_counter.pay_per(log_byte, message.len() as u64)?;
        self.result_state.checked_push_log(message)
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
        self.result_state.gas_counter.pay_base(base)?;
        if msg_ptr < 4 || filename_ptr < 4 {
            return Err(HostError::BadUTF16.into());
        }
        self.result_state.check_can_add_a_log_message()?;

        // Underflow checked above.
        let msg_len =
            self.memory.get_u32(&mut self.result_state.gas_counter, (msg_ptr - 4) as u64)?;
        let filename_len =
            self.memory.get_u32(&mut self.result_state.gas_counter, (filename_ptr - 4) as u64)?;

        let msg = self.get_utf16_string(msg_len as u64, msg_ptr as u64)?;
        let filename = self.get_utf16_string(filename_len as u64, filename_ptr as u64)?;

        let message = format!("{}, filename: \"{}\" line: {} col: {}", msg, filename, line, col);
        self.result_state.gas_counter.pay_base(log_base)?;
        self.result_state.gas_counter.pay_per(log_byte, message.as_bytes().len() as u64)?;
        self.result_state.checked_push_log(format!("ABORT: {}", message))?;

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
        let buf = get_memory_or_register!(self, ptr, len)?;
        self.result_state.gas_counter.pay_base(utf8_decoding_base)?;
        self.result_state.gas_counter.pay_per(utf8_decoding_byte, buf.len() as u64)?;

        // We return an illegally constructed AccountId here for the sake of ensuring
        // backwards compatibility. For paths previously involving validation, like receipts
        // we retain validation further down the line in node-runtime/verifier.rs#fn(validate_receipt)
        // mimicking previous behaviour.
        let account_id = String::from_utf8(buf.into_owned())
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
        self.result_state.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(
                HostError::ProhibitedInView { method_name: "storage_write".to_string() }.into()
            );
        }
        self.result_state.gas_counter.pay_base(storage_write_base)?;
        let key = get_memory_or_register!(self, key_ptr, key_len)?;
        if key.len() as u64 > self.config.limit_config.max_length_storage_key {
            return Err(HostError::KeyLengthExceeded {
                length: key.len() as u64,
                limit: self.config.limit_config.max_length_storage_key,
            }
            .into());
        }
        let value = get_memory_or_register!(self, value_ptr, value_len)?;
        if value.len() as u64 > self.config.limit_config.max_length_storage_value {
            return Err(HostError::ValueLengthExceeded {
                length: value.len() as u64,
                limit: self.config.limit_config.max_length_storage_value,
            }
            .into());
        }
        self.result_state.gas_counter.pay_per(storage_write_key_byte, key.len() as u64)?;
        self.result_state.gas_counter.pay_per(storage_write_value_byte, value.len() as u64)?;
        let evicted = self.ext.storage_set(&mut self.result_state.gas_counter, &key, &value)?;
        let storage_config = &self.fees_config.storage_usage_config;
        self.recorded_storage_counter.observe_size(self.ext.get_recorded_storage_size())?;
        match evicted {
            Some(old_value) => {
                // Inner value can't overflow, because the value length is limited.
                self.result_state.current_storage_usage = self
                    .result_state
                    .current_storage_usage
                    .checked_sub(old_value.len() as u64)
                    .ok_or(InconsistentStateError::IntegerOverflow)?;
                // Inner value can't overflow, because the value length is limited.
                self.result_state.current_storage_usage = self
                    .result_state
                    .current_storage_usage
                    .checked_add(value.len() as u64)
                    .ok_or(InconsistentStateError::IntegerOverflow)?;
                self.registers.set(
                    &mut self.result_state.gas_counter,
                    &self.config.limit_config,
                    register_id,
                    old_value,
                )?;
                Ok(1)
            }
            None => {
                // Inner value can't overflow, because the key/value length is limited.
                self.result_state.current_storage_usage = self
                    .result_state
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
        self.result_state.gas_counter.pay_base(base)?;
        self.result_state.gas_counter.pay_base(storage_read_base)?;
        let key = get_memory_or_register!(self, key_ptr, key_len)?;
        if key.len() as u64 > self.config.limit_config.max_length_storage_key {
            return Err(HostError::KeyLengthExceeded {
                length: key.len() as u64,
                limit: self.config.limit_config.max_length_storage_key,
            }
            .into());
        }
        self.result_state.gas_counter.pay_per(storage_read_key_byte, key.len() as u64)?;
        let read = self.ext.storage_get(&mut self.result_state.gas_counter, &key);
        let read = match read? {
            Some(read) => {
                // Here we'll do u32 -> usize -> u64, which is always infallible
                let read_len = read.len() as usize;
                self.result_state.gas_counter.pay_per(storage_read_value_byte, read_len as u64)?;
                if read_len > INLINE_DISK_VALUE_THRESHOLD {
                    self.result_state.gas_counter.pay_base(storage_large_read_overhead_base)?;
                    self.result_state
                        .gas_counter
                        .pay_per(storage_large_read_overhead_byte, read_len as u64)?;
                }
                Some(read.deref(&mut FreeGasCounter)?)
            }
            None => None,
        };

        self.recorded_storage_counter.observe_size(self.ext.get_recorded_storage_size())?;
        match read {
            Some(value) => {
                self.registers.set(
                    &mut self.result_state.gas_counter,
                    &self.config.limit_config,
                    register_id,
                    value,
                )?;
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
        self.result_state.gas_counter.pay_base(base)?;
        if self.context.is_view() {
            return Err(
                HostError::ProhibitedInView { method_name: "storage_remove".to_string() }.into()
            );
        }
        self.result_state.gas_counter.pay_base(storage_remove_base)?;
        let key = get_memory_or_register!(self, key_ptr, key_len)?;
        if key.len() as u64 > self.config.limit_config.max_length_storage_key {
            return Err(HostError::KeyLengthExceeded {
                length: key.len() as u64,
                limit: self.config.limit_config.max_length_storage_key,
            }
            .into());
        }
        self.result_state.gas_counter.pay_per(storage_remove_key_byte, key.len() as u64)?;
        let removed = self.ext.storage_remove(&mut self.result_state.gas_counter, &key)?;
        let storage_config = &self.fees_config.storage_usage_config;
        self.recorded_storage_counter.observe_size(self.ext.get_recorded_storage_size())?;
        match removed {
            Some(value) => {
                // Inner value can't overflow, because the key/value length is limited.
                self.result_state.current_storage_usage = self
                    .result_state
                    .current_storage_usage
                    .checked_sub(
                        value.len() as u64
                            + key.len() as u64
                            + storage_config.num_extra_bytes_record,
                    )
                    .ok_or(InconsistentStateError::IntegerOverflow)?;
                self.registers.set(
                    &mut self.result_state.gas_counter,
                    &self.config.limit_config,
                    register_id,
                    value,
                )?;
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
        self.result_state.gas_counter.pay_base(base)?;
        self.result_state.gas_counter.pay_base(storage_has_key_base)?;
        let key = get_memory_or_register!(self, key_ptr, key_len)?;
        if key.len() as u64 > self.config.limit_config.max_length_storage_key {
            return Err(HostError::KeyLengthExceeded {
                length: key.len() as u64,
                limit: self.config.limit_config.max_length_storage_key,
            }
            .into());
        }
        self.result_state.gas_counter.pay_per(storage_has_key_byte, key.len() as u64)?;
        let res = self.ext.storage_has_key(&mut self.result_state.gas_counter, &key);

        self.recorded_storage_counter.observe_size(self.ext.get_recorded_storage_size())?;
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
    ///   `storage_write` or `storage_remove` was invoked on the key such that:
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

    /// Obtain a reference to the gas counter.
    ///
    /// This is meant for use in tests and implementation of VMs only. Implementations of host
    /// functions should be using `pay_*` functions instead.
    #[cfg(any(test, all(feature = "near_vm", target_arch = "x86_64")))]
    pub(crate) fn gas_counter(&mut self) -> &mut GasCounter {
        &mut self.result_state.gas_counter
    }

    /// Properly handles gas limit exceeded error.
    pub fn process_gas_limit(&mut self) -> HostError {
        let new_burn_gas = self.result_state.gas_counter.burnt_gas();
        let new_used_gas = self.result_state.gas_counter.used_gas();
        self.result_state.gas_counter.process_gas_limit(new_burn_gas, new_used_gas)
    }

    /// A helper function to pay base cost gas fee for batching an action.
    pub fn pay_action_base(&mut self, action: ActionCosts, sir: bool) -> Result<()> {
        let base_fee = self.fees_config.fee(action);
        let burn_gas = base_fee.send_fee(sir);
        let use_gas =
            burn_gas.checked_add(base_fee.exec_fee()).ok_or(HostError::IntegerOverflow)?;
        self.result_state.gas_counter.pay_action_accumulated(burn_gas, use_gas, action)
    }

    /// A helper function to pay per byte gas fee for batching an action.
    pub fn pay_action_per_byte(
        &mut self,
        action: ActionCosts,
        num_bytes: u64,
        sir: bool,
    ) -> Result<()> {
        let per_byte_fee = self.fees_config.fee(action);
        let burn_gas =
            num_bytes.checked_mul(per_byte_fee.send_fee(sir)).ok_or(HostError::IntegerOverflow)?;
        let use_gas = burn_gas
            .checked_add(
                num_bytes.checked_mul(per_byte_fee.exec_fee()).ok_or(HostError::IntegerOverflow)?,
            )
            .ok_or(HostError::IntegerOverflow)?;
        self.result_state.gas_counter.pay_action_accumulated(burn_gas, use_gas, action)
    }
}

#[derive(PartialEq)]
pub struct VMOutcome {
    pub balance: Balance,
    pub storage_usage: StorageUsage,
    pub return_data: ReturnData,
    pub burnt_gas: Gas,
    pub used_gas: Gas,
    pub compute_usage: Compute,
    pub logs: Vec<String>,
    /// Data collected from making a contract call
    pub profile: ProfileDataV3,
    pub aborted: Option<FunctionCallError>,
}

impl VMOutcome {
    /// Consumes the `VMLogic` object and computes the final outcome with the
    /// given error that stopped execution from finishing successfully.
    pub fn abort(state: ExecutionResultState, error: FunctionCallError) -> VMOutcome {
        let mut outcome = state.compute_outcome();
        outcome.aborted = Some(error);
        outcome
    }

    /// Consumes the `VMLogic` object and computes the final outcome for a
    /// successful execution.
    pub fn ok(state: ExecutionResultState) -> VMOutcome {
        state.compute_outcome()
    }

    /// Creates an outcome with a no-op outcome.
    pub fn nop_outcome(error: FunctionCallError) -> VMOutcome {
        VMOutcome {
            // Note: Balance and storage fields are ignored on a failed outcome.
            balance: 0,
            storage_usage: 0,
            // Note: Fields below are added or merged when processing the
            // outcome. With 0 or the empty set, those are no-ops.
            return_data: ReturnData::None,
            burnt_gas: 0,
            used_gas: 0,
            compute_usage: 0,
            logs: Vec::new(),
            profile: ProfileDataV3::default(),
            aborted: Some(error),
        }
    }

    /// Like `Self::abort()` but without feature `FixContractLoadingCost` it
    /// will return a NOP outcome. This is used for backwards-compatibility only.
    pub fn abort_but_nop_outcome_in_old_protocol(
        state: ExecutionResultState,
        error: FunctionCallError,
    ) -> VMOutcome {
        if state.config.fix_contract_loading_cost {
            Self::abort(state, error)
        } else {
            Self::nop_outcome(error)
        }
    }
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
        )?;
        if let Some(err) = &self.aborted {
            write!(f, " failed with {err}")?;
        }
        Ok(())
    }
}

enum GlobalContractIdentifierPtrData {
    CodeHash { code_hash_len: u64, code_hash_ptr: u64 },
    AccountId { account_id_len: u64, account_id_ptr: u64 },
}

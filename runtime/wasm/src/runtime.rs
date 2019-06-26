use std::collections::HashSet;

use byteorder::{ByteOrder, LittleEndian};
use wasmer_runtime::{memory::Memory, units::Bytes};

use near_primitives::hash::hash;
use near_primitives::logging::pretty_utf8;
use near_primitives::types::{
    AccountId, Balance, PromiseId, ReceiptId, StorageUsage, StorageUsageChange,
};
use near_primitives::utils::is_valid_account_id;

use crate::ext::External;
use crate::types::{Config, ReturnData, RuntimeContext, RuntimeError as Error};

type Result<T> = ::std::result::Result<T, Error>;

type DataTypeIndex = u32;

pub const DATA_TYPE_ORIGINATOR_ACCOUNT_ID: DataTypeIndex = 1;
pub const DATA_TYPE_CURRENT_ACCOUNT_ID: DataTypeIndex = 2;
pub const DATA_TYPE_STORAGE: DataTypeIndex = 3;
pub const DATA_TYPE_INPUT: DataTypeIndex = 4;
pub const DATA_TYPE_RESULT: DataTypeIndex = 5;
pub const DATA_TYPE_STORAGE_ITER: DataTypeIndex = 6;

/// Converts u128 into array of bytes.
#[inline]
fn to_uint128<'a>(value: u128) -> &'a [u8] {
    unsafe { std::slice::from_raw_parts(&value as *const u128 as *const u8, 16) }
}

pub struct Runtime<'a> {
    ext: &'a mut dyn External,
    input_data: &'a [u8],
    result_data: &'a [Option<Vec<u8>>],
    pub frozen_balance: Balance,
    pub liquid_balance: Balance,
    /// Keep track of how much of the liquid balance is used by the contract so far,
    /// without deposits/withdrawals and resending of the balance to other contracts.
    usage_counter: Balance,
    context: &'a RuntimeContext<'a>,
    config: Config,
    pub storage_counter: StorageUsageChange,
    promise_ids: Vec<PromiseId>,
    pub return_data: ReturnData,
    pub random_seed: Vec<u8>,
    random_buffer_offset: usize,
    pub logs: Vec<String>,
    memory: Memory,
}

impl<'a> Runtime<'a> {
    pub fn new(
        ext: &'a mut dyn External,
        input_data: &'a [u8],
        result_data: &'a [Option<Vec<u8>>],
        context: &'a RuntimeContext<'a>,
        config: Config,
        memory: Memory,
    ) -> Runtime<'a> {
        Runtime {
            ext,
            input_data,
            result_data,
            frozen_balance: context.initial_balance,
            liquid_balance: context.received_amount,
            usage_counter: 0,
            context,
            config,
            storage_counter: 0,
            promise_ids: Vec::new(),
            return_data: ReturnData::None,
            random_seed: hash(&context.random_seed).into(),
            random_buffer_offset: 0,
            logs: Vec::new(),
            memory,
        }
    }

    fn memory_can_fit(&self, offset: usize, len: usize) -> bool {
        match offset.checked_add(len) {
            None => false,
            Some(end) => self.memory.size().bytes() >= Bytes(end),
        }
    }

    fn memory_get(&self, offset: usize, len: usize) -> Result<Vec<u8>> {
        if !self.memory_can_fit(offset, len) {
            Err(Error::MemoryAccessViolation)
        } else if len == 0 {
            Ok(Vec::new())
        } else {
            Ok(self.memory.view()[offset..(offset + len)]
                .iter()
                .map(std::cell::Cell::get)
                .collect())
        }
    }

    fn memory_set(&mut self, offset: usize, buf: &[u8]) -> Result<()> {
        if !self.memory_can_fit(offset, buf.len()) {
            Err(Error::MemoryAccessViolation)
        } else if buf.is_empty() {
            Ok(())
        } else {
            self.memory.view()[offset..(offset + buf.len())]
                .iter()
                .zip(buf.iter())
                .for_each(|(cell, v)| cell.set(*v));
            Ok(())
        }
    }

    fn memory_get_u32(&self, offset: usize) -> Result<u32> {
        let buf = self.memory_get(offset, 4)?;
        Ok(LittleEndian::read_u32(&buf))
    }

    fn memory_get_u128(&self, offset: usize) -> Result<u128> {
        let buf = self.memory_get(offset, 16)?;
        Ok(LittleEndian::read_u128(&buf))
    }

    fn random_u8(&mut self) -> u8 {
        if self.random_buffer_offset >= self.random_seed.len() {
            self.random_seed = hash(&self.random_seed).into();
            self.random_buffer_offset = 0;
        }
        self.random_buffer_offset += 1;
        self.random_seed[self.random_buffer_offset - 1]
    }

    /// Reads AssemblyScript string from utf-16
    fn read_string(&self, offset: usize) -> Result<String> {
        let len: u32 = self.memory_get_u32(offset - 4)?;
        if len % 2 != 0 {
            return Err(Error::BadUtf16);
        }
        let buffer = self.memory_get(offset, len as usize)?;
        let mut u16_buffer = Vec::new();
        for i in 0..((len / 2) as usize) {
            let c = u16::from(buffer[i * 2]) + u16::from(buffer[i * 2 + 1]) * 0x100;
            u16_buffer.push(c);
        }
        String::from_utf16(&u16_buffer).map_err(|_| Error::BadUtf16)
    }

    fn promise_index_to_id(&self, promise_index: u32) -> Result<PromiseId> {
        Ok(self.promise_ids.get(promise_index as usize).ok_or(Error::InvalidPromiseIndex)?.clone())
    }

    fn read_and_parse_account_id(&self, ptr: u32, len: u32) -> Result<AccountId> {
        let buf = self.memory_get(ptr as usize, len as usize)?;
        let account_id = AccountId::from_utf8(buf).map_err(|_| Error::BadUtf8)?;
        if !is_valid_account_id(&account_id) {
            return Err(Error::InvalidAccountId);
        }
        Ok(account_id)
    }

    /// Attempt to charge liquid balance.
    fn charge_balance(&mut self, amount: Balance) -> Result<()> {
        if self.liquid_balance < amount {
            if self.context.free_of_charge {
                Ok(())
            } else {
                Err(Error::BalanceExceeded)
            }
        } else {
            self.liquid_balance -= amount;
            Ok(())
        }
    }

    /// Attempt to charge liquid balance, respecting usage limit.
    fn charge_balance_with_limit(&mut self, amount: Balance) -> Result<()> {
        let new_usage = self.usage_counter + amount;
        if new_usage > self.config.usage_limit as u128 {
            if self.context.free_of_charge {
                Ok(())
            } else {
                Err(Error::UsageLimit)
            }
        } else {
            self.charge_balance(amount).map(|res| {
                self.usage_counter = new_usage;
                res
            })
        }
    }

    /// Called by WASM.
    fn gas(&mut self, gas_amount: u32) -> Result<()> {
        let res = self.charge_balance_with_limit(Balance::from(gas_amount));
        res
    }

    /// Writes to storage from wasm memory
    fn storage_write(
        &mut self,
        key_len: u32,
        key_ptr: u32,
        value_len: u32,
        value_ptr: u32,
    ) -> Result<()> {
        let key = self.memory_get(key_ptr as usize, key_len as usize)?;
        let value = self.memory_get(value_ptr as usize, value_len as usize)?;

        let evicted = self.ext.storage_set(&key, &value).map_err(|_| Error::StorageUpdateError)?;
        if let Some(evicted) = evicted {
            self.storage_counter +=
                value_len as StorageUsageChange - evicted.len() as StorageUsageChange;
        } else {
            self.storage_counter += key_len as StorageUsageChange + value_len as StorageUsageChange;
        }
        debug!(target: "wasm", "storage_write('{}', '{}')", pretty_utf8(&key), pretty_utf8(&value));
        Ok(())
    }

    /// Remove key from storage
    fn storage_remove(&mut self, key_len: u32, key_ptr: u32) -> Result<()> {
        let key = self.memory_get(key_ptr as usize, key_len as usize)?;
        let removed = self.ext.storage_remove(&key).map_err(|_| Error::StorageRemoveError)?;
        if let Some(removed) = removed {
            self.storage_counter -=
                key_len as StorageUsageChange + removed.len() as StorageUsageChange;
        }
        debug!(target: "wasm", "storage_remove('{}')", pretty_utf8(&key));
        Ok(())
    }

    /// Returns whether the key is present in the storage
    fn storage_has_key(&mut self, key_len: u32, key_ptr: u32) -> Result<u32> {
        let key = self.memory_get(key_ptr as usize, key_len as usize)?;
        // TODO(#743): Improve performance of has_key. Don't need to retrive the value.
        let val = self.ext.storage_get(&key).map_err(|_| Error::StorageReadError)?;
        let res = val.is_some();
        debug!(target: "wasm", "storage_has_key('{}') -> {}", pretty_utf8(&key), res);
        Ok(res as u32)
    }

    /// Gets iterator for keys with given prefix
    fn storage_iter(&mut self, prefix_len: u32, prefix_ptr: u32) -> Result<u32> {
        let prefix = self.memory_get(prefix_ptr as usize, prefix_len as usize)?;
        let storage_id = self.ext.storage_iter(&prefix).map_err(|_| Error::StorageReadError)?;
        debug!(target: "wasm", "storage_iter('{}') -> {}", pretty_utf8(&prefix), storage_id);
        Ok(storage_id)
    }

    /// Gets iterator for the range of keys between given start and end keys
    fn storage_range(
        &mut self,
        start_len: u32,
        start_ptr: u32,
        end_len: u32,
        end_ptr: u32,
    ) -> Result<u32> {
        let start_key = self.memory_get(start_ptr as usize, start_len as usize)?;
        let end_key = self.memory_get(end_ptr as usize, end_len as usize)?;
        let storage_id =
            self.ext.storage_range(&start_key, &end_key).map_err(|_| Error::StorageReadError)?;
        debug!(target: "wasm", "storage_range('{}', '{}') -> {}",
            pretty_utf8(&start_key),
            pretty_utf8(&end_key),
            storage_id);
        Ok(storage_id)
    }

    /// Advances iterator. Returns true if iteration isn't finished yet.
    fn storage_iter_next(&mut self, storage_id: u32) -> Result<u32> {
        let key = self.ext.storage_iter_next(storage_id).map_err(|_| Error::StorageUpdateError)?;
        debug!(target: "wasm", "storage_iter_next({}) -> '{}'", storage_id, pretty_utf8(&key.clone().unwrap_or_default()));
        Ok(key.is_some() as u32)
    }

    fn promise_create(
        &mut self,
        account_id_len: u32,
        account_id_ptr: u32,
        method_name_len: u32,
        method_name_ptr: u32,
        arguments_len: u32,
        arguments_ptr: u32,
        amount_ptr: u32,
    ) -> Result<u32> {
        let amount = self.memory_get_u128(amount_ptr as usize)?;
        let account_id = self.read_and_parse_account_id(account_id_ptr, account_id_len)?;
        let method_name = self.memory_get(method_name_ptr as usize, method_name_len as usize)?;

        if let Some(b'_') = method_name.get(0) {
            return Err(Error::PrivateMethod);
        }

        let arguments = self.memory_get(arguments_ptr as usize, arguments_len as usize)?;
        self.charge_balance(self.config.contract_call_cost + amount)?;

        let promise_id = self
            .ext
            .promise_create(account_id, method_name, arguments, amount)
            .map_err(|_| Error::PromiseError)?;

        let promise_index = self.promise_ids.len();
        self.promise_ids.push(promise_id);

        Ok(promise_index as u32)
    }

    fn promise_then(
        &mut self,
        promise_index: u32,
        method_name_len: u32,
        method_name_ptr: u32,
        arguments_len: u32,
        arguments_ptr: u32,
        amount_ptr: u32,
    ) -> Result<u32> {
        let amount = self.memory_get_u128(amount_ptr as usize)?;
        let promise_id = self.promise_index_to_id(promise_index)?;
        let method_name = self.memory_get(method_name_ptr as usize, method_name_len as usize)?;
        if method_name.is_empty() {
            return Err(Error::EmptyMethodName);
        }
        let arguments = self.memory_get(arguments_ptr as usize, arguments_len as usize)?;

        let num_promises = match &promise_id {
            PromiseId::Receipt(_) => 1,
            PromiseId::Callback(_) => return Err(Error::PromiseError),
            PromiseId::Joiner(v) => v.len() as u64,
        } as u128;
        self.charge_balance((num_promises * self.config.contract_call_cost + amount).into())?;

        let promise_id = self
            .ext
            .promise_then(promise_id, method_name, arguments, amount.into())
            .map_err(|_| Error::PromiseError)?;

        let promise_index = self.promise_ids.len();
        self.promise_ids.push(promise_id);

        Ok(promise_index as u32)
    }

    fn promise_and(&mut self, promise_index1: u32, promise_index2: u32) -> Result<u32> {
        let promise_ids =
            [self.promise_index_to_id(promise_index1)?, self.promise_index_to_id(promise_index2)?];

        let mut receipt_ids = vec![];
        let mut unique_receipt_ids = HashSet::new();
        {
            let mut add_receipt_id = |receipt_id: ReceiptId| -> Result<()> {
                if !unique_receipt_ids.insert(receipt_id.clone()) {
                    return Err(Error::PromiseError);
                }
                receipt_ids.push(receipt_id);
                Ok(())
            };

            for promise_id in promise_ids.iter() {
                match promise_id {
                    PromiseId::Receipt(receipt_id) => add_receipt_id(receipt_id.clone())?,
                    PromiseId::Callback(_) => return Err(Error::PromiseError),
                    PromiseId::Joiner(v) => {
                        for receipt_id in v {
                            add_receipt_id(receipt_id.clone())?
                        }
                    }
                };
            }
        }

        let promise_id = PromiseId::Joiner(receipt_ids);
        let promise_index = self.promise_ids.len();
        self.promise_ids.push(promise_id);

        Ok(promise_index as u32)
    }

    fn check_ethash(
        &mut self,
        block_number: u64,
        header_hash_ptr: u32,
        header_hash_len: u32,
        nonce: u64,
        mix_hash_ptr: u32,
        mix_hash_len: u32,
        difficulty: u64,
    ) -> Result<u32> {
        let header_hash = self.memory_get(header_hash_ptr as usize, header_hash_len as usize)?;
        let mix_hash = self.memory_get(mix_hash_ptr as usize, mix_hash_len as usize)?;
        Ok(self.ext.check_ethash(block_number, &header_hash, nonce, &mix_hash, difficulty) as u32)
    }

    /// Returns the number of results.
    /// Results are available as part of the callback from a promise.
    fn result_count(&self) -> Result<u32> {
        Ok(self.result_data.len() as u32)
    }

    fn result_is_ok(&self, result_index: u32) -> Result<u32> {
        let result =
            self.result_data.get(result_index as usize).ok_or(Error::InvalidResultIndex)?;

        Ok(result.is_some() as u32)
    }

    fn return_value(&mut self, value_len: u32, value_ptr: u32) -> Result<()> {
        let return_val = self.memory_get(value_ptr as usize, value_len as usize)?;

        self.return_data = ReturnData::Value(return_val);

        Ok(())
    }

    fn return_promise(&mut self, promise_index: u32) -> Result<()> {
        let promise_id = self.promise_index_to_id(promise_index)?;

        self.return_data = ReturnData::Promise(promise_id);

        Ok(())
    }

    fn get_frozen_balance(&mut self, balance_ptr: u32) -> Result<()> {
        self.memory_set(balance_ptr as usize, to_uint128(self.frozen_balance))
    }

    fn get_liquid_balance(&mut self, balance_ptr: u32) -> Result<()> {
        self.memory_set(balance_ptr as usize, to_uint128(self.liquid_balance))
    }

    /// Helper function to transfer between two accounts.
    fn transfer_helper(
        from: &mut Balance,
        to: &mut Balance,
        min_amount: Balance,
        max_amount: Balance,
    ) -> Result<Balance> {
        let result = if *from >= max_amount {
            *from -= max_amount;
            *to += max_amount;
            max_amount
        } else {
            if *from >= min_amount {
                let amount = *from;
                *from = 0;
                *to += amount;
                amount
            } else {
                0
            }
        };
        Ok(result)
    }

    /// Deposit the given amount to the account balance and return deposited amount.
    /// If there is enough of liquid balance will deposit `max_amount`, otherwise will deposit
    /// as much as possible and will fail if there is less than `min_amount`.
    fn deposit(
        &mut self,
        min_amount_ptr: u32,
        max_amount_ptr: u32,
        balance_ptr: u32,
    ) -> Result<()> {
        let min_amount = self.memory_get_u128(min_amount_ptr as usize)?;
        let max_amount = self.memory_get_u128(max_amount_ptr as usize)?;
        Self::transfer_helper(
            &mut self.liquid_balance,
            &mut self.frozen_balance,
            min_amount,
            max_amount,
        )
        .map(to_uint128)
        .and_then(|val| self.memory_set(balance_ptr as usize, val))
    }

    /// Withdraw the given amount from the account balance and return withdrawn amount.
    /// If there is enough of frozen balance will withdraw `max_amount`, otherwise will withdraw
    /// as much as possible and will fail if there is less than `min_amount`.
    fn withdraw(
        &mut self,
        min_amount_ptr: u32,
        max_amount_ptr: u32,
        balance_ptr: u32,
    ) -> Result<()> {
        let min_amount = self.memory_get_u128(min_amount_ptr as usize)?;
        let max_amount = self.memory_get_u128(max_amount_ptr as usize)?;
        Self::transfer_helper(
            &mut self.frozen_balance,
            &mut self.liquid_balance,
            min_amount,
            max_amount,
        )
        .map(to_uint128)
        .and_then(|val| self.memory_set(balance_ptr as usize, val))
    }

    fn storage_usage(&self) -> Result<StorageUsage> {
        let storage_usage = self.context.storage_usage as StorageUsageChange + self.storage_counter;
        Ok(storage_usage as StorageUsage)
    }

    fn received_amount(&mut self, balance_ptr: u32) -> Result<()> {
        self.memory_set(balance_ptr as usize, to_uint128(self.context.received_amount))
    }

    fn assert(&self, expression: u32) -> Result<()> {
        if expression != 0 {
            Ok(())
        } else {
            Err(Error::AssertFailed)
        }
    }

    fn abort(&mut self, msg_ptr: u32, filename_ptr: u32, line: u32, col: u32) -> Result<()> {
        let msg = self.read_string(msg_ptr as usize)?;
        let filename = self.read_string(filename_ptr as usize)?;

        let message =
            format!("ABORT: {:?} filename: {:?} line: {:?} col: {:?}", msg, filename, line, col);
        debug!(target: "wasm", "{}", &message);
        self.logs.push(message);

        Err(Error::AssertFailed)
    }

    fn debug(&mut self, msg_len: u32, msg_ptr: u32) -> Result<()> {
        let val = self.memory_get(msg_ptr as usize, msg_len as usize)?;
        let message = format!(
            "LOG: {}",
            std::str::from_utf8(&val).unwrap_or_else(|_| "debug(): from_utf8 failed")
        );
        debug!(target: "wasm", "{}", &message);
        self.logs.push(message);

        Ok(())
    }

    fn log(&mut self, msg_ptr: u32) -> Result<()> {
        let message = format!(
            "LOG: {}",
            self.read_string(msg_ptr as usize)
                .unwrap_or_else(|_| "log(): read_string failed".to_string())
        );
        debug!(target: "wasm", "{}", &message);
        self.logs.push(message);

        Ok(())
    }

    /// Generic data read. Tries to write data into the given buffer, only if the buffer has available capacity.
    /// Returns length of the data in bytes for the given buffer type and the given key.
    /// NOTE: Majority of reads would be small enough in size to fit into the given preallocated buffer.
    /// Params:
    /// buffer_type_index -> The index of the data column type to read, e.g. storage, sender's account_id or results
    /// key_len and key_ptr -> Depends on buffer type. Represents a key to read.
    ///     key is either a pointer to a key buffer or a key index
    /// max_buf_len -> Capacity of the preallocated buffer to write data into. Can be 0, if we first want to read the length
    /// buf_ptr -> Pointer to the buffer to write data into.
    fn data_read(
        &mut self,
        data_type_index: DataTypeIndex,
        key_len: u32,
        key: u32,
        max_buf_len: u32,
        buf_ptr: u32,
    ) -> Result<u32> {
        let tmp_vec;
        let buf = match data_type_index {
            DATA_TYPE_ORIGINATOR_ACCOUNT_ID => self.context.sender_id.as_bytes(),
            DATA_TYPE_CURRENT_ACCOUNT_ID => self.context.account_id.as_bytes(),
            DATA_TYPE_STORAGE => {
                let key = self.memory_get(key as usize, key_len as usize)?;
                let val = self.ext.storage_get(&key).map_err(|_| Error::StorageUpdateError)?;
                match val {
                    Some(v) => {
                        tmp_vec = v;
                        &tmp_vec[..]
                    }
                    None => &[],
                }
            }
            DATA_TYPE_INPUT => self.input_data,
            DATA_TYPE_RESULT => {
                let result = self.result_data.get(key as usize).ok_or(Error::InvalidResultIndex)?;

                match result {
                    Some(v) => &v[..],
                    None => return Err(Error::ResultIsNotOk),
                }
            }
            DATA_TYPE_STORAGE_ITER => {
                let storage_id = key;
                let key_buf = self
                    .ext
                    .storage_iter_peek(storage_id)
                    .map_err(|_| Error::StorageUpdateError)?;
                match key_buf {
                    Some(v) => {
                        tmp_vec = v;
                        &tmp_vec[..]
                    }
                    None => &[],
                }
            }
            _ => return Err(Error::UnknownDataTypeIndex),
        };
        if buf.len() <= max_buf_len as usize {
            self.memory_set(buf_ptr as usize, &buf)?;
        }
        Ok(buf.len() as u32)
    }

    fn hash(&mut self, value_len: u32, value_ptr: u32, buf_ptr: u32) -> Result<()> {
        let buf = self.memory_get(value_ptr as usize, value_len as usize)?;
        let buf_hash = hash(&buf);

        self.memory_set(buf_ptr as usize, buf_hash.as_ref())
    }

    fn hash32(&self, value_len: u32, value_ptr: u32) -> Result<u32> {
        let buf = self.memory_get(value_ptr as usize, value_len as usize)?;
        let buf_hash = hash(&buf);
        let buf_hash_ref = buf_hash.as_ref();

        let mut buf_hash_32: u32 = 0;
        for b in buf_hash_ref.iter().take(4) {
            buf_hash_32 <<= 8;
            buf_hash_32 += u32::from(*b);
        }

        Ok(buf_hash_32)
    }

    fn random_buf(&mut self, len: u32, out_ptr: u32) -> Result<()> {
        if !self.memory_can_fit(out_ptr as usize, len as usize) {
            return Err(Error::MemoryAccessViolation);
        }

        let mut buf = Vec::with_capacity(len as usize);
        for _ in 0..len {
            buf.push(self.random_u8());
        }

        self.memory_set(out_ptr as usize, &buf)
    }

    fn random32(&mut self) -> Result<u32> {
        let mut random_val: u32 = 0;
        for _ in 0..4 {
            random_val <<= 8;
            random_val += u32::from(self.random_u8());
        }

        Ok(random_val)
    }

    fn block_index(&self) -> Result<u64> {
        Ok(self.context.block_index as u64)
    }
}

pub mod imports {
    use std::ffi::c_void;

    use wasmer_runtime::{func, imports, Ctx, ImportObject};

    use super::{Memory, Result, Runtime};

    macro_rules! wrapped_imports {
        ( $( $import_name:expr => $func:ident < [ $( $arg_name:ident : $arg_type:ident ),* ] -> [ $( $returns:ident ),* ] >, )* ) => {
            $(
                fn $func( ctx: &mut Ctx, $( $arg_name: $arg_type ),* ) -> Result<($( $returns ),*)> {
                    let runtime: &mut Runtime = unsafe { &mut *(ctx.data as *mut Runtime) };
                    runtime.$func( $( $arg_name, )* )
                }
            )*

            pub(crate) fn build(memory: Memory, raw_ptr: *mut c_void) -> ImportObject {
                let dtor = (|_: *mut c_void| {}) as fn(*mut c_void);
                imports! {
                    move || { (raw_ptr, dtor) },
                    "env" => {
                        "memory" => memory,
                        $(
                            $import_name => func!($func),
                        )*
                    },
                }
            }
        }
    }

    wrapped_imports! {
        // Storage related
        // name               // func          // signature
        // Storage write. Writes given value for the given key.
        "storage_write" => storage_write<[key_len: u32, key_ptr: u32, value_len: u32, value_ptr: u32] -> []>,
        "storage_iter" => storage_iter<[prefix_len: u32, prefix_ptr: u32] -> [u32]>,
        "storage_range" => storage_range<[start_len: u32, start_ptr: u32, end_len: u32, end_ptr: u32] -> [u32]>,
        "storage_iter_next" => storage_iter_next<[storage_id: u32] -> [u32]>,
        "storage_remove" => storage_remove<[key_len: u32, key_ptr: u32] -> []>,
        "storage_has_key" => storage_has_key<[key_len: u32, key_ptr: u32] -> [u32]>,
        // Generic data read. Tries to write data into the given buffer, only if the buffer has available capacity.
        "data_read" => data_read<[data_type_index: u32, key_len: u32, key: u32, max_buf_len: u32, buf_ptr: u32] -> [u32]>,

        // Promises, callbacks and async calls
        // Creates a new promise that makes an async call to some other contract.
        "promise_create" => promise_create<[
            account_id_len: u32, account_id_ptr: u32,
            method_name_len: u32, method_name_ptr: u32,
            arguments_len: u32, arguments_ptr: u32,
            amount_ptr: u32
        ] -> [u32]>,
        // Attaches a callback to a given promise. This promise can be either an
        // async call or multiple joined promises.
        // NOTE: The given promise can't be a callback.
        "promise_then" => promise_then<[
            promise_index: u32,
            method_name_len: u32, method_name_ptr: u32,
            arguments_len: u32, arguments_ptr: u32,
            amount_ptr: u32
        ] -> [u32]>,
        // Joins 2 given promises together and returns a new promise.
        "promise_and" => promise_and<[promise_index1: u32, promise_index2: u32] -> [u32]>,
        "check_ethash" => check_ethash<[
            block_number: u64,
            header_hash_ptr: u32, header_hash_len: u32,
            nonce: u64,
            mix_hash_ptr: u32, mix_hash_len: u32,
            difficulty: u64
        ] -> [u32]>,
        // Returns the number of returned results for this callback.
        "result_count" => result_count<[] -> [u32]>,
        "result_is_ok" => result_is_ok<[result_index: u32] -> [u32]>,

        // Called to return value from the function.
        "return_value" => return_value<[value_len: u32, value_ptr: u32] -> []>,
        // Called to return promise from the function.
        "return_promise" => return_promise<[promise_index: u32] -> []>,

        // Context
        // Returns the frozen balance.
        "frozen_balance" => get_frozen_balance<[balance_ptr: u32] -> []>,
        // Returns the liquid balance.
        "liquid_balance" => get_liquid_balance<[balance_ptr: u32] -> []>,
        // Deposits balance from liquid to frozen.
        "deposit" => deposit<[min_amount_ptr: u32, max_amount_ptr: u32, balance_ptr: u32] -> []>,
        // Withdraws balance from frozen to liquid.
         "withdraw" => withdraw<[min_amount_ptr: u32, max_amount_ptr: u32, balance_ptr: u32] -> []>,

        // Returns the storage usage.
        "storage_usage" => storage_usage<[] -> [u64]>,
        // Returns the amount of tokens received with this call.
        "received_amount" => received_amount<[amount_ptr: u32] -> []>,
        // Returns currently produced block index.
        "block_index" => block_index<[] -> [u64]>,

        // Contracts can assert properties. E.g. check the amount available mana.
        "assert" => assert<[expression: u32] -> []>,
        // Assembly Script specific abort
        "abort" => abort<[msg_ptr: u32, filename_ptr: u32, line: u32, col: u32] -> []>,
        // Hashes given value and writes 32 bytes of result in the given pointer.
        "hash" => hash<[value_len: u32, value_ptr: u32, buf_ptr: u32] -> []>,
        // Hashes given value and returns first 32 bits as u32.
        "hash32" => hash32<[value_len: u32, value_ptr: u32] -> [u32]>,
        // Fills given buffer of given length with random values.
        "random_buf" => random_buf<[buf_len: u32, buf_ptr: u32] -> []>,
        // Returns random u32.
        "random32" => random32<[] -> [u32]>,
        // Prints to logs utf-8 string using given msg and msg_ptr
        "debug" => debug<[msg_len: u32, msg_ptr: u32] -> []>,
        // Prints to logs given AssemblyScript string in utf-16 format
        "log" => log<[msg_ptr: u32] -> []>,

        // Function for the injected gas counter. Automatically called by the gas meter.
        "gas" => gas<[gas_amount: u32] -> []>,
    }
}

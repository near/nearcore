use crate::ext::External;

use crate::types::{RuntimeError as Error, ReturnData, RuntimeContext};

use primitives::types::{AccountId, PromiseId, ReceiptId, Balance, Mana, Gas};
use primitives::hash::hash;
use primitives::utils::is_valid_account_id;
use std::collections::HashSet;
use wasmer_runtime::{
    memory::Memory,
    units::Bytes,
};
use byteorder::{ByteOrder, LittleEndian};

type Result<T> = ::std::result::Result<T, Error>;

type BufferTypeIndex = u32;

pub const BUFFER_TYPE_ORIGINATOR_ACCOUNT_ID: BufferTypeIndex = 1;
pub const BUFFER_TYPE_CURRENT_ACCOUNT_ID: BufferTypeIndex = 2;

pub struct Runtime<'a> {
    ext: &'a mut External,
    input_data: &'a [u8],
    result_data: &'a [Option<Vec<u8>>],
    pub mana_counter: Mana,
    context: &'a RuntimeContext,
    pub balance: Balance,
    pub gas_counter: Gas,
    gas_limit: Gas,
    promise_ids: Vec<PromiseId>,
    pub return_data: ReturnData,
    pub random_seed: Vec<u8>,
    random_buffer_offset: usize,
    pub logs: Vec<String>,
    memory: Memory,
}

impl<'a> Runtime<'a> {
    pub fn new(
        ext: &'a mut External,
        input_data: &'a [u8],
        result_data: &'a [Option<Vec<u8>>],
        context: &'a RuntimeContext,
        gas_limit: Gas,
        memory: Memory,
    ) -> Runtime<'a> {
        Runtime {
            ext,
            input_data,
            result_data,
            mana_counter: 0,
            context,
            balance: context.initial_balance + context.received_amount,
            gas_counter: 0,
            gas_limit,
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
            Ok(self.memory
                .view()[offset..(offset + len)]
                .iter()
                .map(|cell| cell.get())
                .collect())
        }
    }

    fn memory_set(&mut self, offset: usize, buf: &[u8]) -> Result<()> {
        if !self.memory_can_fit(offset, buf.len()) {
            Err(Error::MemoryAccessViolation)
        } else if buf.is_empty() {
            Ok(())
        } else {
            self.memory
                .view()[offset..(offset + buf.len())]
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

    fn read_buffer(&self, offset: usize) -> Result<Vec<u8>> {
        let len: u32 = self.memory_get_u32(offset)?;
        self.memory_get(offset + 4, len as usize)
    }

    fn random_u8(&mut self) -> u8 {
        if self.random_buffer_offset >= self.random_seed.len() {
            self.random_seed = hash(&self.random_seed).into();
            self.random_buffer_offset = 0;
        }
        self.random_buffer_offset += 1;
        self.random_seed[self.random_buffer_offset - 1]
    }

    fn read_string(&self, offset: usize) -> Result<String> {
        let len: u32 = self.memory_get_u32(offset)?;
        let buffer = self.memory_get(offset + 4, (len * 2) as usize)?;
        let mut u16_buffer = Vec::new();
        for i in 0..(len as usize) {
            let c = u16::from(buffer[i * 2]) + u16::from(buffer[i * 2 + 1]) * 0x100;
            u16_buffer.push(c);
        }
        String::from_utf16(&u16_buffer).map_err(|_| Error::BadUtf16)
    }

    fn promise_index_to_id(&self, promise_index: u32) -> Result<PromiseId> {
        Ok(self.promise_ids.get(promise_index as usize).ok_or(Error::InvalidPromiseIndex)?.clone())
    }

    fn read_and_parse_account_id(&self, offset: u32) -> Result<AccountId> {
        let buf = self.read_buffer(offset as usize)?;
        let account_id = AccountId::from_utf8(buf).map_err(|_| Error::BadUtf8)?;
        if !is_valid_account_id(&account_id) {
            return Err(Error::InvalidAccountId);
        }
        Ok(account_id)
    }

    fn charge_gas(&mut self, gas_amount: Gas) -> bool {
        let prev = self.gas_counter;
        match prev.checked_add(gas_amount) {
            // gas charge overflow protection
            None => false,
            Some(val) if val > self.gas_limit => false,
            Some(_) => {
                self.gas_counter = prev + gas_amount;
                true
            }
        }
    }

    fn charge_mana(&mut self, mana: Mana) -> bool {
        let prev = self.mana_counter;
        match prev.checked_add(mana) {
            // mana charge overflow protection
            None => false,
            Some(val) if val > self.context.mana => false,
            Some(_) => {
                self.mana_counter = prev + mana;
                true
            }
        }
    }

    fn charge_mana_or_fail(&mut self, mana: Mana) -> Result<()> {
        if self.charge_mana(mana) {
            Ok(())
        } else {
            Err(Error::ManaLimit)
        }
    }

    /// Returns length of the value from the storage
    fn storage_read_len(&mut self, key_ptr: u32) -> Result<u32> {
        let key = self.read_buffer(key_ptr as usize)?;
        let val = self
            .ext
            .storage_get(&key)
            .map_err(|_| Error::StorageUpdateError)?;
        let len = match val {
            Some(v) => v.len(),
            None => 0,
        };
        debug!(target: "wasm", "storage_read_len('{}') => {}", format_buf(&key), len);
        Ok(len as u32)
    }

    /// Reads from the storage to wasm memory
    fn storage_read_into(&mut self, key_ptr: u32, val_ptr: u32) -> Result<()> {
        let key = self.read_buffer(key_ptr as usize)?;
        let val = self
            .ext
            .storage_get(&key)
            .map_err(|_| Error::StorageUpdateError)?;
        if let Some(buf) = val {
            self.memory_set(val_ptr as usize, &buf)?;
            debug!(target: "wasm", "storage_read_into('{}') => '{}'", format_buf(&key), format_buf(&buf));
        }
        Ok(())
    }

    /// Writes to storage from wasm memory
    fn storage_write(&mut self, key_ptr: u32, val_ptr: u32) -> Result<()> {
        let key = self.read_buffer(key_ptr as usize)?;
        let val = self.read_buffer(val_ptr as usize)?;
        // TODO: Charge gas for storage

        self.ext
            .storage_set(&key, &val)
            .map_err(|_| Error::StorageUpdateError)?;
        debug!(target: "wasm", "storage_write('{}', '{}')", format_buf(&key), format_buf(&val));
        Ok(())
    }

    /// Gets iterator for keys with given prefix
    fn storage_iter(&mut self, prefix_ptr: u32) -> Result<u32> {
        let prefix = self.read_buffer(prefix_ptr as usize)?;
        let storage_id = self
            .ext
            .storage_iter(&prefix)
            .map_err(|_| Error::StorageUpdateError)?;
        debug!(target: "wasm", "storage_iter('{}') -> {}", format_buf(&prefix), storage_id);
        Ok(storage_id)
    }

    /// Advances iterator. Returns true if iteration isn't finished yet.
    fn storage_iter_next(&mut self, storage_id: u32) -> Result<u32> {
        let key = self
            .ext
            .storage_iter_next(storage_id)
            .map_err(|_| Error::StorageUpdateError)?;
        debug!(target: "wasm", "storage_iter_next({}) -> '{}'", storage_id, format_buf(&key.clone().unwrap_or_default()));
        Ok(key.is_some() as u32)
    }

    /// Returns length of next key in iterator or 0 if there is no next value.
    fn storage_iter_peek_len(&mut self, storage_id: u32) -> Result<u32> {
        let key = self
            .ext
            .storage_iter_peek(storage_id)
            .map_err(|_| Error::StorageUpdateError)?;
        match key {
            Some(key) => Ok(key.len() as u32),
            None => Ok(0)
        }
    }

    /// Writes next key in iterator to given buffer.
    fn storage_iter_peek_into(&mut self, storage_id: u32, val_ptr: u32) -> Result<()> {
        let key = self
            .ext
            .storage_iter_peek(storage_id)
            .map_err(|_| Error::StorageUpdateError)?;
        if let Some(buf) = key {
            self.memory_set(val_ptr as usize, &buf)?;
        }
        Ok(())
    }

    fn gas(&mut self, gas_amount: u32) -> Result<()> {
        if self.charge_gas(Gas::from(gas_amount)) {
            Ok(())
        } else {
            Err(Error::GasLimit)
        }
    }

    fn promise_create(&mut self, account_id_ptr: u32, method_name_ptr: u32, arguments_ptr: u32, mana: u32, amount: u64) -> Result<u32> {
        let account_id = self.read_and_parse_account_id(account_id_ptr)?;
        let method_name = self.read_buffer(method_name_ptr as usize)?;

        match method_name.get(0) {
            Some(b'_') => return Err(Error::PrivateMethod),
            None if amount == 0 => return Err(Error::EmptyMethodNameWithZeroAmount),
            _ => (),
        };

        let arguments = self.read_buffer(arguments_ptr as usize)?;

        // Charging separately reserved mana + 1 to call this promise.
        self.charge_mana_or_fail(mana)?;
        self.charge_mana_or_fail(1)?;

        if amount > self.balance {
            return Err(Error::BalanceExceeded);
        }
        self.balance -= amount;

        let promise_id = self.ext
            .promise_create(account_id, method_name, arguments, mana, amount)
            .map_err(|_| Error::PromiseError)?;

        let promise_index = self.promise_ids.len();
        self.promise_ids.push(promise_id);

        Ok(promise_index as u32)
    }

    fn promise_then(&mut self, promise_index: u32, method_name_ptr: u32, arguments_ptr: u32, mana: u32) -> Result<u32> {
        let promise_id = self.promise_index_to_id(promise_index)?;
        let method_name = self.read_buffer(method_name_ptr as usize)?;
        if method_name.is_empty() {
            return Err(Error::EmptyMethodName);
        }
        let arguments = self.read_buffer(arguments_ptr as usize)?;

        // Charging separately reserved mana + N to add callback for the promise.
        // N is the number of callbacks in case of promise joiner.
        self.charge_mana_or_fail(mana)?;
        let num_promises = match &promise_id {
            PromiseId::Receipt(_) => 1,
            PromiseId::Callback(_) => return Err(Error::PromiseError),
            PromiseId::Joiner(v) => v.len() as u32,
        };
        self.charge_mana_or_fail(num_promises)?;

        let promise_id = self.ext
            .promise_then(promise_id, method_name, arguments, mana)
            .map_err(|_| Error::PromiseError)?;

        let promise_index = self.promise_ids.len();
        self.promise_ids.push(promise_id);

        Ok(promise_index as u32)
    }

    fn promise_and(&mut self, promise_index1: u32, promise_index2: u32) -> Result<u32> {
        let promise_ids = [
            self.promise_index_to_id(promise_index1)?,
            self.promise_index_to_id(promise_index2)?,
        ];

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
                    },
                };
            }
        }

        let promise_id = PromiseId::Joiner(receipt_ids);
        let promise_index = self.promise_ids.len();
        self.promise_ids.push(promise_id);

        Ok(promise_index as u32)
    }

    /// Returns length of the input (arguments)
    fn input_read_len(&self) -> Result<u32> {
        Ok(self.input_data.len() as u32)
    }

    /// Reads from the input (arguments) to wasm memory
    fn input_read_into(&mut self, val_ptr: u32) -> Result<()> {
        self.memory_set(val_ptr as usize, &self.input_data)
    }

    /// Returns the number of results.
    /// Results are available as part of the callback from a promise.
    fn result_count(&self) -> Result<u32> {
        Ok(self.result_data.len() as u32)
    }

    fn result_is_ok(&self, result_index: u32) -> Result<u32> {
        let result = self.result_data.get(result_index as usize).ok_or(Error::InvalidResultIndex)?;

        Ok(result.is_some() as u32)
    }

    /// Returns length of the result (arguments)
    fn result_read_len(&self, result_index: u32) -> Result<u32> {
        let result = self.result_data.get(result_index as usize).ok_or(Error::InvalidResultIndex)?;

        match result {
            Some(buf) => Ok(buf.len() as u32),
            None => Err(Error::ResultIsNotOk),
        }
    }

    /// Read from the input to wasm memory
    fn result_read_into(&mut self, result_index: u32, val_ptr: u32) -> Result<()> {
        let result = self.result_data.get(result_index as usize).ok_or(Error::InvalidResultIndex)?;

        match result {
            Some(buf) => self.memory_set(val_ptr as usize, &buf),
            None => Err(Error::ResultIsNotOk),
        }
    }

    fn return_value(&mut self, return_val_ptr: u32) -> Result<()> {
        let return_val = self.read_buffer(return_val_ptr as usize)?;

        self.return_data = ReturnData::Value(return_val);

        Ok(())
    }

    fn return_promise(&mut self, promise_index: u32) -> Result<()> {
        let promise_id = self.promise_index_to_id(promise_index)?;

        self.return_data = ReturnData::Promise(promise_id);

        Ok(())
    }

    fn get_balance(&self) -> Result<u64> {
        Ok(self.balance)
    }

    fn gas_left(&self) -> Result<u64> {
        let gas_left = self.gas_limit - self.gas_counter;

        Ok(gas_left)
    }

    fn mana_left(&self) -> Result<u32> {
        let mana_left = self.context.mana - self.mana_counter;

        Ok(mana_left as u32)
    }

    fn received_amount(&self) -> Result<u64> {
        Ok(self.context.received_amount)
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

        let message = format!("ABORT: {:?} filename: {:?} line: {:?} col: {:?}", msg, filename, line, col);
        debug!(target: "wasm", "{}", &message);
        self.logs.push(message);

        Err(Error::AssertFailed)
    }

    fn debug(&mut self, msg_ptr: u32) -> Result<()> {
        let message = format!("LOG: {}", self.read_string(msg_ptr as usize).unwrap_or_else(|_| "log(): read_string failed".to_string()));
        debug!(target: "wasm", "{}", &message);
        self.logs.push(message);

        Ok(())
    }

    fn log(&mut self, msg_ptr: u32) -> Result<()> {
        self.debug(msg_ptr)
    }

    /// Returns length of the buffer for the type/key pair
    fn read_len(&mut self, buffer_type_index: BufferTypeIndex, _key_ptr: u32) -> Result<u32> {
        let len = match buffer_type_index {
            BUFFER_TYPE_ORIGINATOR_ACCOUNT_ID => self.context.originator_id.as_bytes().len(),
            BUFFER_TYPE_CURRENT_ACCOUNT_ID => self.context.account_id.as_bytes().len(),
            _ => return Err(Error::UnknownBufferTypeIndex)
        };
        Ok(len as u32)
    }

    /// Writes content of the buffer for the type/key pair into given pointer
    fn read_into(&mut self, buffer_type_index: BufferTypeIndex, _key_ptr: u32, val_ptr: u32) -> Result<()> {
        let buf = match buffer_type_index {
            BUFFER_TYPE_ORIGINATOR_ACCOUNT_ID => self.context.originator_id.as_bytes(),
            BUFFER_TYPE_CURRENT_ACCOUNT_ID => self.context.account_id.as_bytes(),
            _ => return Err(Error::UnknownBufferTypeIndex)
        };
        self.memory_set(val_ptr as usize, buf)
    }

    fn hash(&mut self, buf_ptr: u32, out_ptr: u32) -> Result<()> {
        let buf = self.read_buffer(buf_ptr as usize)?;
        let buf_hash = hash(&buf);

        self.memory_set(out_ptr as usize, buf_hash.as_ref())
    }

    fn hash32(&self, buf_ptr: u32) -> Result<u32> {
        let buf = self.read_buffer(buf_ptr as usize)?;
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

fn format_buf(buf: &[u8]) -> String {
    std::str::from_utf8(&buf).unwrap_or(&format!("{:?}", buf)).to_string()
}

pub mod imports {
    use super::{Runtime, Memory, Result};

    use wasmer_runtime::{
        imports,
        func,
        ImportObject,
        Ctx,
    };

    macro_rules! wrapped_imports {
        ( $( $import_name:expr => $func:ident < [ $( $arg_name:ident : $arg_type:ident ),* ] -> [ $( $returns:ident ),* ] >, )* ) => {
            $(
                fn $func( $( $arg_name: $arg_type, )* ctx: &mut Ctx) -> Result<($( $returns )*)> {
                    let runtime: &mut Runtime = unsafe { &mut *(ctx.data as *mut Runtime) };
                    runtime.$func( $( $arg_name, )* )
                }
            )*

            pub(crate) fn build(memory: Memory) -> ImportObject {
                imports! {
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
        "storage_read_len" => storage_read_len<[key_ptr: u32] -> [u32]>,
        "storage_read_into" => storage_read_into<[key_ptr: u32, val_ptr: u32] -> []>,
        "storage_iter" => storage_iter<[prefix_ptr: u32] -> [u32]>,
        "storage_iter_next" => storage_iter_next<[storage_id: u32] -> [u32]>,
        "storage_iter_peek_len" => storage_iter_peek_len<[storage_id: u32] -> [u32]>,
        "storage_iter_peek_into" => storage_iter_peek_into<[storage_id: u32, val_ptr: u32] -> []>,
        "storage_write" => storage_write<[key_ptr: u32, val_ptr: u32] -> []>,
        // TODO(#350): Refactor all reads and writes into generic reads. 
        // Generic data read. Returns the length of the buffer for the type/key.
        "read_len" => read_len<[buffer_type_index: u32, _key_ptr: u32] -> [u32]>,
        // Generic data read. Writes content of the buffer for the type/key into the given pointer.
        "read_into" => read_into<[buffer_type_index: u32, _key_ptr: u32, val_ptr: u32] -> []>,

        // Promises, callbacks and async calls
        // Creates a new promise that makes an async call to some other contract.
        "promise_create" => promise_create<[account_id_ptr: u32, method_name_ptr: u32, arguments_ptr: u32, mana: u32, amount: u64] -> [u32]>,
        // Attaches a callback to a given promise. This promise can be either an
        // async call or multiple joined promises.
        // NOTE: The given promise can't be a callback.
        "promise_then" => promise_then<[promise_index: u32, method_name_ptr: u32, arguments_ptr: u32, mana: u32] -> [u32]>,
        // Joins 2 given promises together and returns a new promise.
        "promise_and" => promise_and<[promise_index1: u32, promise_index2: u32] -> [u32]>,
        // Returns total byte length of the arguments.
        "input_read_len" => input_read_len<[] -> [u32]>,
        "input_read_into" => input_read_into<[val_ptr: u32] -> []>,
        // Returns the number of returned results for this callback.
        "result_count" => result_count<[] -> [u32]>,
        "result_is_ok" => result_is_ok<[result_index: u32] -> [u32]>,
        "result_read_len" => result_read_len<[result_index: u32] -> [u32]>,
        "result_read_into" => result_read_into<[result_index: u32, val_ptr: u32] -> []>,

        // Called to return value from the function.
        "return_value" => return_value<[return_val_ptr: u32] -> []>,
        // Called to return promise from the function.
        "return_promise" => return_promise<[promise_index: u32] -> []>,

        // Context
        // Returns the current balance.
        "balance" => get_balance<[] -> [u64]>,
        // Returns the amount of MANA left.
        "mana_left" => mana_left<[] -> [u32]>,
        // Returns the amount of GAS left.
        "gas_left" => gas_left<[] -> [u64]>,
        // Returns the amount of tokens received with this call.
        "received_amount" => received_amount<[] -> [u64]>,
        // Returns currently produced block index.
        "block_index" => block_index<[] -> [u64]>,

        // Contracts can assert properties. E.g. check the amount available mana.
        "assert" => assert<[expression: u32] -> []>,
        "abort" => abort<[msg_ptr: u32, filename_ptr: u32, line: u32, col: u32] -> []>,
        // Hashes given buffer and writes 32 bytes of result in the given pointer.
        "hash" => hash<[buf_ptr: u32, out_ptr: u32] -> []>,
        // Hashes given buffer and returns first 32 bits as u32.
        "hash32" => hash32<[buf_ptr: u32] -> [u32]>,
        // Fills given buffer of given length with random values.
        "random_buf" => random_buf<[len: u32, out_ptr: u32] -> []>,
        // Returns random u32.
        "random32" => random32<[] -> [u32]>,
        "debug" => debug<[msg_ptr: u32] -> []>,
        "log" => log<[msg_ptr: u32] -> []>,

        // Function for the injected gas counter. Automatically called by the gas meter.
        "gas" => gas<[gas_amount: u32] -> []>,
    }
}

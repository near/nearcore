use ext::External;

use memory::Memory;
use wasmi::{RuntimeArgs, RuntimeValue};
use types::RuntimeError as Error;

use primitives::types::{AccountAlias, PromiseId};

type Result<T> = ::std::result::Result<T, Error>;

pub struct Runtime<'a> {
    ext: &'a mut External,
    input_data: &'a [u8],
    result_data: &'a [Option<Vec<u8>>],
    memory: Memory,
    pub gas_counter: u64,
    gas_limit: u64,
    promise_ids: Vec<PromiseId>,
}

impl<'a> Runtime<'a> {
    pub fn new(
        ext: &'a mut External,
        input_data: &'a [u8],
        result_data: &'a [Option<Vec<u8>>],
        memory: Memory,
        gas_limit: u64,
    ) -> Runtime<'a> {
        Runtime {
            ext,
            input_data,
            result_data,
            memory,
            gas_counter: 0,
            gas_limit,
            promise_ids: Vec::new(),
        }
    }

    fn read_buffer(&self, offset: u32) -> Result<Vec<u8>> {
        let len: u32 = self
            .memory
            .get_u32(offset)
            .map_err(|_| Error::MemoryAccessViolation)?;
        let buf = self
            .memory
            .get(offset + 4, len as usize)
            .map_err(|_| Error::MemoryAccessViolation)?;
        Ok(buf)
    }

    fn promise_index_to_id(&self, promise_index: u32) -> Result<PromiseId> {
        Ok(self.promise_ids.get(promise_index as usize).ok_or(Error::InvalidPromiseIndex)?.clone())
    }

    fn read_and_parse_account_alias(&self, offset: u32) -> Result<AccountAlias> {
        let buf = self.read_buffer(offset)?;
        AccountAlias::from_utf8(buf).map_err(|_| Error::BadUtf8)
    }

    fn charge_gas(&mut self, amount: u64) -> bool {
        let prev = self.gas_counter;
        match prev.checked_add(amount) {
            // gas charge overflow protection
            None => false,
            Some(val) if val > self.gas_limit => false,
            Some(_) => {
                self.gas_counter = prev + amount;
                true
            }
        }
    }

    /// Returns length of the value from the storage
    fn storage_read_len(&mut self, args: &RuntimeArgs) -> Result<RuntimeValue> {
        let key_ptr: u32 = args.nth_checked(0)?;

        let key = self.read_buffer(key_ptr)?;
        let val = self
            .ext
            .storage_get(&key)
            .map_err(|_| Error::StorageUpdateError)?;
        let len = match val {
            Some(v) => v.len(),
            None => 0,
        };

        Ok(RuntimeValue::I32(len as i32))
    }

    /// Reads from the storage to wasm memory
    fn storage_read_into(&mut self, args: &RuntimeArgs) -> Result<()> {
        let key_ptr: u32 = args.nth_checked(0)?;
        let val_ptr: u32 = args.nth_checked(1)?;

        let key = self.read_buffer(key_ptr)?;
        let val = self
            .ext
            .storage_get(&key)
            .map_err(|_| Error::StorageUpdateError)?;
        if let Some(buf) = val {
            self.memory
                .set(val_ptr, &buf)
                .map_err(|_| Error::MemoryAccessViolation)?;
        }
        Ok(())
    }

    /// Writes to storage from wasm memory
    fn storage_write(&mut self, args: &RuntimeArgs) -> Result<()> {
        let key_ptr: u32 = args.nth_checked(0)?;
        let val_ptr: u32 = args.nth_checked(1)?;

        let key = self.read_buffer(key_ptr)?;
        let val = self.read_buffer(val_ptr)?;
        // TODO: Charge gas for storage

        self.ext
            .storage_set(&key, &val)
            .map_err(|_| Error::StorageUpdateError)?;
        Ok(())
    }

    fn gas(&mut self, args: &RuntimeArgs) -> Result<()> {
        let amount: u32 = args.nth_checked(0)?;
        if self.charge_gas(u64::from(amount)) {
            Ok(())
        } else {
            Err(Error::GasLimit)
        }
    }

    fn promise_create(&mut self, args: &RuntimeArgs) -> Result<RuntimeValue> {
        let accound_id_ptr: u32 = args.nth_checked(0)?;
        let method_name_ptr: u32 = args.nth_checked(1)?;
        let arguments_ptr: u32 = args.nth_checked(2)?;
        let mana: u32 = args.nth_checked(3)?;
        let amount: u64 = args.nth_checked(4)?;

        let accound_alias = self.read_and_parse_account_alias(accound_id_ptr)?;
        let method_name = self.read_buffer(method_name_ptr)?;
        let arguments = self.read_buffer(arguments_ptr)?;

        let promise_id = self.ext
            .promise_create(accound_alias, method_name, arguments, mana, amount)
            .map_err(|_| Error::PromiseError)?;

        let promise_index = self.promise_ids.len() as u32;
        self.promise_ids.push(promise_id);

        Ok(RuntimeValue::I32(promise_index as i32))
    }

    fn promise_then(&mut self, args: &RuntimeArgs) -> Result<RuntimeValue> {
        let promise_index: u32 = args.nth_checked(0)?;
        let method_name_ptr: u32 = args.nth_checked(1)?;
        let arguments_ptr: u32 = args.nth_checked(2)?;
        let mana: u32 = args.nth_checked(3)?;

        let promise_id = self.promise_index_to_id(promise_index)?;
        let method_name = self.read_buffer(method_name_ptr)?;
        let arguments = self.read_buffer(arguments_ptr)?;

        let promise_id = self.ext
            .promise_then(promise_id, method_name, arguments, mana)
            .map_err(|_| Error::PromiseError)?;

        let promise_index = self.promise_ids.len();
        self.promise_ids.push(promise_id);

        Ok(RuntimeValue::I32(promise_index as i32))
    }

    fn promise_and(&mut self, args: &RuntimeArgs) -> Result<RuntimeValue> {
        let promise_index1: u32 = args.nth_checked(0)?;
        let promise_index2: u32 = args.nth_checked(1)?;

        let promise_id1 = self.promise_index_to_id(promise_index1)?;
        let promise_id2 = self.promise_index_to_id(promise_index2)?;

        let promise_id = self.ext
            .promise_and(promise_id1, promise_id2)
            .map_err(|_| Error::PromiseError)?;

        let promise_index = self.promise_ids.len();
        self.promise_ids.push(promise_id);

        Ok(RuntimeValue::I32(promise_index as i32))
    }

    /// Returns length of the input (arguments)
    fn input_read_len(&self) -> Result<RuntimeValue> {
        Ok(RuntimeValue::I32(self.input_data.len() as u32 as i32))
    }

    /// Reads from the input (arguments) to wasm memory
    fn input_read_into(&mut self, args: &RuntimeArgs) -> Result<()> {
        let val_ptr: u32 = args.nth_checked(0)?;
        self.memory
            .set(val_ptr, &self.input_data)
            .map_err(|_| Error::MemoryAccessViolation)?;
        Ok(())
    }

    /// Returns the number of results.
    /// Results are available as part of the callback from a promise.
    fn result_count(&self) -> Result<RuntimeValue> {
        Ok(RuntimeValue::I32(self.result_data.len() as u32 as i32))
    }

    fn result_is_ok(&self, args: &RuntimeArgs) -> Result<RuntimeValue> {
        let result_index: u32 = args.nth_checked(0)?;

        let result = self.result_data.get(result_index as usize).ok_or(Error::InvalidResultIndex)?;

        Ok(RuntimeValue::I32(result.is_some() as i32))
    }

    /// Returns length of the result (arguments)
    fn result_read_len(&self, args: &RuntimeArgs) -> Result<RuntimeValue> {
        let result_index: u32 = args.nth_checked(0)?;

        let result = self.result_data.get(result_index as usize).ok_or(Error::InvalidResultIndex)?;

        match result {
            Some(buf) => Ok(RuntimeValue::I32(buf.len() as u32 as i32)),
            None => Err(Error::ResultIsNotOk),
        }
    }

    /// Read from the input to wasm memory
    fn result_read_into(&self, args: &RuntimeArgs) -> Result<()> {
        let result_index: u32 = args.nth_checked(0)?;
        let val_ptr: u32 = args.nth_checked(1)?;

        let result = self.result_data.get(result_index as usize).ok_or(Error::InvalidResultIndex)?;

        match result {
            Some(buf) => {
                self.memory
                    .set(val_ptr, &buf)
                    .map_err(|_| Error::MemoryAccessViolation)?;
                Ok(())
            }
            None => Err(Error::ResultIsNotOk),
        }
    }

    /// Releases this Runtime instance and parses given result and writes it to the
    /// output data buffer.
    pub fn parse_result(self, result: Option<RuntimeValue>, output_data: &mut Vec<u8>) -> Result<()> {
        match result {
            Some(RuntimeValue::I32(result_ptr)) => {
                let buf = self.read_buffer(result_ptr as u32)?;
                output_data.resize(buf.len(), 0);
                output_data.copy_from_slice(&buf);
                Ok(())
            },
            None => {
                output_data.resize(0, 0);
                Ok(())
            },
            _ => Err(Error::InvalidReturn),
        }
    }
}

mod ext_impl {

    use ext::ids::*;
    use wasmi::{Externals, RuntimeArgs, RuntimeValue, Trap};

    macro_rules! void {
		{ $e: expr } => { { $e?; Ok(None) } }
	}

    macro_rules! some {
		{ $e: expr } => { { Ok(Some($e?)) } }
	}

    impl<'a> Externals for super::Runtime<'a> {
        fn invoke_index(
            &mut self,
            index: usize,
            args: RuntimeArgs,
        ) -> Result<Option<RuntimeValue>, Trap> {
            match index {
                STORAGE_WRITE_FUNC => void!(self.storage_write(&args)),
                STORAGE_READ_LEN_FUNC => some!(self.storage_read_len(&args)),
                STORAGE_READ_INTO_FUNC => void!(self.storage_read_into(&args)),
                GAS_FUNC => void!(self.gas(&args)),
                PROMISE_CREATE_FUNC => some!(self.promise_create(&args)),
                PROMISE_THEN_FUNC => some!(self.promise_then(&args)),
                PROMISE_AND_FUNC => some!(self.promise_and(&args)),
                INPUT_READ_LEN_FUNC => some!(self.input_read_len()),
                INPUT_READ_INTO_FUNC => void!(self.input_read_into(&args)),
                RESULT_COUNT_FUNC => some!(self.result_count()),
                RESULT_IS_OK_FUNC => some!(self.result_is_ok(&args)),
                RESULT_READ_LEN_FUNC => some!(self.result_read_len(&args)),
                RESULT_READ_INTO_FUNC => void!(self.result_read_into(&args)),
                _ => panic!("env module doesn't provide function at index {}", index),
            }
        }
    }
}

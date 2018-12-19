use ext::External;

use memory::Memory;
use wasmi::{RuntimeArgs, RuntimeValue};
use types::{RuntimeError as Error, ReturnData, RuntimeContext};

use primitives::types::{AccountAlias, AccountId, PromiseId, ReceiptId, Balance, Mana, Gas};
use std::collections::HashSet;

type Result<T> = ::std::result::Result<T, Error>;

pub struct Runtime<'a> {
    ext: &'a mut External,
    input_data: &'a [u8],
    result_data: &'a [Option<Vec<u8>>],
    memory: Memory,
    pub mana_counter: Mana,
    context: &'a RuntimeContext,
    pub balance: Balance,
    pub gas_counter: Gas,
    gas_limit: Gas,
    promise_ids: Vec<PromiseId>,
    pub return_data: ReturnData,
}

impl<'a> Runtime<'a> {
    pub fn new(
        ext: &'a mut External,
        input_data: &'a [u8],
        result_data: &'a [Option<Vec<u8>>],
        memory: Memory,
        context: &'a RuntimeContext,
        gas_limit: Gas,
    ) -> Runtime<'a> {
        Runtime {
            ext,
            input_data,
            result_data,
            memory,
            mana_counter: 0,
            context,
            balance: context.initial_balance + context.received_amount,
            gas_counter: 0,
            gas_limit,
            promise_ids: Vec::new(),
            return_data: ReturnData::None,
        }
    }

    fn read_buffer_with_size(&self, offset: u32, len: usize) -> Result<Vec<u8>> {
        let buf = self
            .memory
            .get(offset, len)
            .map_err(|_| Error::MemoryAccessViolation)?;
        Ok(buf)
    }


    fn read_buffer(&self, offset: u32) -> Result<Vec<u8>> {
        let len: u32 = self
            .memory
            .get_u32(offset)
            .map_err(|_| Error::MemoryAccessViolation)?;
        self.read_buffer_with_size(offset + 4, len as usize)
    }

    fn read_string(&self, offset: u32) -> Result<String> {
        let len: u32 = self
            .memory
            .get_u32(offset)
            .map_err(|_| Error::MemoryAccessViolation)?;
        let buffer = self
            .read_buffer_with_size(offset + 4, (len * 2) as usize)
            .map_err(|_| Error::MemoryAccessViolation)?;
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

    fn read_and_parse_account_alias(&self, offset: u32) -> Result<AccountAlias> {
        let buf = self.read_buffer(offset)?;
        AccountAlias::from_utf8(buf).map_err(|_| Error::BadUtf8)
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
        debug!(target: "wasm", "storage_read_len('{}') => {}", format_buf(key), len);
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
            debug!(target: "wasm", "storage_read_into('{}') => '{}'", format_buf(key), format_buf(buf));
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
        debug!(target: "wasm", "storage_write('{}', '{}')", format_buf(key), format_buf(val));
        Ok(())
    }

    fn gas(&mut self, args: &RuntimeArgs) -> Result<()> {
        let gas_amount: u32 = args.nth_checked(0)?;
        if self.charge_gas(Gas::from(gas_amount)) {
            Ok(())
        } else {
            Err(Error::GasLimit)
        }
    }

    fn promise_create(&mut self, args: &RuntimeArgs) -> Result<RuntimeValue> {
        let account_id_ptr: u32 = args.nth_checked(0)?;
        let method_name_ptr: u32 = args.nth_checked(1)?;
        let arguments_ptr: u32 = args.nth_checked(2)?;
        let mana: u32 = args.nth_checked(3)?;
        let amount: u64 = args.nth_checked(4)?;

        let account_id_buf = self.read_buffer_with_size(account_id_ptr, 32)?;
        let account_id: AccountId = AccountId::from(account_id_buf);
        let method_name = self.read_buffer(method_name_ptr)?;
        let arguments = self.read_buffer(arguments_ptr)?;

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

        Ok(RuntimeValue::I32(promise_index as i32))
    }

    fn promise_and(&mut self, args: &RuntimeArgs) -> Result<RuntimeValue> {
        let promise_index1: u32 = args.nth_checked(0)?;
        let promise_index2: u32 = args.nth_checked(1)?;

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

    fn return_value(&mut self, args: &RuntimeArgs) -> Result<()> {
        let return_val_ptr: u32 = args.nth_checked(0)?;
        let return_val = self.read_buffer(return_val_ptr)?;

        self.return_data = ReturnData::Value(return_val);

        Ok(())
    }

    fn return_promise(&mut self, args: &RuntimeArgs) -> Result<()> {
        let promise_index: u32 = args.nth_checked(0)?;
        let promise_id = self.promise_index_to_id(promise_index)?;

        self.return_data = ReturnData::Promise(promise_id);

        Ok(())
    }

    fn get_balance(&self) -> Result<RuntimeValue> {
        Ok(RuntimeValue::I64(self.balance as i64))
    }

    fn gas_left(&self) -> Result<RuntimeValue> {
        let gas_left = self.gas_limit - self.gas_counter;

        Ok(RuntimeValue::I64(gas_left as i64))
    }

    fn mana_left(&self) -> Result<RuntimeValue> {
        let mana_left = self.context.mana - self.mana_counter;

        Ok(RuntimeValue::I32(mana_left as i32))
    }

    fn received_amount(&self) -> Result<RuntimeValue> {
        Ok(RuntimeValue::I64(self.context.received_amount as i64))
    }

    fn assert(&self, args: &RuntimeArgs) -> Result<()> {
        let expression: bool = args.nth_checked(0)?;

        if expression {
            Ok(())
        } else {
            Err(Error::AssertFailed)
        }
    }

    fn abort(&self, args: &RuntimeArgs) -> Result<()> {
        let msg_ptr: u32 = args.nth_checked(0)?;
        let filename_ptr: u32 = args.nth_checked(1)?;
        let line: u32 = args.nth_checked(2)?;
        let col: u32 = args.nth_checked(3)?;

        let msg = self.read_string(msg_ptr)?;
        let filename = self.read_string(filename_ptr)?;

        debug!(target: "wasm", "abort with msg: {:?} filename: {:?} line: {:?} col: {:?}", msg, filename, line, col);

        Err(Error::AssertFailed)
    }

    fn log(&self, args: &RuntimeArgs) -> Result<()> {
        let msg_ptr: u32 = args.nth_checked(0)?;
        debug!(target: "wasm", "{}", self.read_string(msg_ptr).unwrap_or_else(|_| "log(): read_string failed".to_string()));
        Ok(())
    }

    fn sender_id(&self, args: &RuntimeArgs) -> Result<()> {
        let val_ptr: u32 = args.nth_checked(0)?;

        self.memory
            .set(val_ptr, self.context.sender_id.as_ref())
            .map_err(|_| Error::MemoryAccessViolation)?;
        Ok(())
    }

    fn account_id(&self, args: &RuntimeArgs) -> Result<()> {
        let val_ptr: u32 = args.nth_checked(0)?;

        self.memory
            .set(val_ptr, self.context.account_id.as_ref())
            .map_err(|_| Error::MemoryAccessViolation)?;
        Ok(())
    }

    fn account_alias_to_id(&self, args: &RuntimeArgs) -> Result<()> {
        let account_alias_ptr: u32 = args.nth_checked(0)?;
        let val_ptr: u32 = args.nth_checked(1)?;

        let account_alias = self.read_and_parse_account_alias(account_alias_ptr)?;
        let account_id = AccountId::from(&account_alias);

        self.memory
            .set(val_ptr, account_id.as_ref())
            .map_err(|_| Error::MemoryAccessViolation)?;
        Ok(())
    }
}

fn format_buf(buf: Vec<u8>) -> String {
    std::str::from_utf8(&buf).unwrap_or(&format!("{:?}", buf)).to_string()
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
                RETURN_VALUE_FUNC => void!(self.return_value(&args)),
                RETURN_PROMISE_FUNC => void!(self.return_promise(&args)),
                BALANCE_FUNC => some!(self.get_balance()),
                MANA_LEFT_FUNC => some!(self.mana_left()),
                GAS_LEFT_FUNC => some!(self.gas_left()),
                RECEIVED_AMOUNT_FUNC => some!(self.received_amount()),
                ASSERT_FUNC => void!(self.assert(&args)),
                ABORT_FUNC => void!(self.abort(&args)),
                SENDER_ID_FUNC => void!(self.sender_id(&args)),
                ACCOUNT_ID_FUNC => void!(self.account_id(&args)),
                ACCOUNT_ALIAS_TO_ID_FUNC => void!(self.account_alias_to_id(&args)),
                LOG_FUNC => void!(self.log(&args)),
                _ => panic!("env module doesn't provide function at index {}", index),
            }
        }
    }
}

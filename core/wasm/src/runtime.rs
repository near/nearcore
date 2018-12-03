use ext::External;

use memory::Memory;
use wasmi::{RuntimeArgs, RuntimeValue};
use types::RuntimeError as Error;

pub struct RuntimeContext {
    /*
	pub address: Address,
	pub sender: Address,
	pub origin: Address,
	pub code_address: Address,
	pub value: U256,
    */}

type Result<T> = ::std::result::Result<T, Error>;

pub struct Runtime<'a> {
    ext: &'a mut External,
    _context: RuntimeContext,
    memory: Memory,
    gas_counter: u64,
    gas_limit: u64,
}

impl<'a> Runtime<'a> {
    pub fn new(
        ext: &mut External,
        context: RuntimeContext,
        memory: Memory,
        gas_limit: u64,
    ) -> Runtime {
        Runtime {
            ext,
            _context: context,
            memory,
            gas_counter: 0,
            gas_limit,
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

    /// Read from the storage to wasm memory
    pub fn storage_read_len(&mut self, args: &RuntimeArgs) -> Result<RuntimeValue> {
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

    /// Read from the storage to wasm memory
    pub fn storage_read_into(&mut self, args: &RuntimeArgs) -> Result<()> {
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

    /// Write to storage from wasm memory
    pub fn storage_write(&mut self, args: &RuntimeArgs) -> Result<()> {
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

    pub fn gas(&mut self, args: &RuntimeArgs) -> Result<()> {
        let amount: u32 = args.nth_checked(0)?;
        if self.charge_gas(u64::from(amount)) {
            Ok(())
        } else {
            Err(Error::GasLimit)
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
    /*
	macro_rules! cast {
		{ $e: expr } => { { Ok(Some($e)) } }
	}
	*/

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
                /*
				RET_FUNC => void!(self.ret(args)),
				INPUT_LENGTH_FUNC => cast!(self.input_legnth()),
				FETCH_INPUT_FUNC => void!(self.fetch_input(args)),
				PANIC_FUNC => void!(self.panic(args)),
				DEBUG_FUNC => void!(self.debug(args)),
				CCALL_FUNC => some!(self.ccall(args)),
				DCALL_FUNC => some!(self.dcall(args)),
				SCALL_FUNC => some!(self.scall(args)),
				VALUE_FUNC => void!(self.value(args)),
				CREATE_FUNC => some!(self.create(args)),
				SUICIDE_FUNC => void!(self.suicide(args)),
				BLOCKHASH_FUNC => void!(self.blockhash(args)),
				BLOCKNUMBER_FUNC => some!(self.blocknumber()),
				COINBASE_FUNC => void!(self.coinbase(args)),
				DIFFICULTY_FUNC => void!(self.difficulty(args)),
				GASLIMIT_FUNC => void!(self.gaslimit(args)),
				TIMESTAMP_FUNC => some!(self.timestamp()),
				ADDRESS_FUNC => void!(self.address(args)),
				SENDER_FUNC => void!(self.sender(args)),
				ORIGIN_FUNC => void!(self.origin(args)),
				ELOG_FUNC => void!(self.elog(args)),
				CREATE2_FUNC => some!(self.create2(args)),
				GASLEFT_FUNC => some!(self.gasleft()),
                */
                _ => panic!("env module doesn't provide function at index {}", index),
            }
        }
    }
}

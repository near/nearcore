use ext::Externalities;

use wasmi::{self, RuntimeArgs, RuntimeValue, Error as WasmiError, Trap, TrapKind};
use memory::Memory;

pub struct RuntimeContext {
    /*
	pub address: Address,
	pub sender: Address,
	pub origin: Address,
	pub code_address: Address,
	pub value: U256,
    */
}

/// User trap in native code
#[derive(Debug, Clone, PartialEq)]
pub enum Error {
	/// Storage read error
	StorageReadError,
	/// Storage update error
	StorageUpdateError,
	/// Memory access violation
	MemoryAccessViolation,
	/// Native code requested execution to finish
	Return,
	/// Invalid gas state inside interpreter
	InvalidGasState,
	/// Query of the balance resulted in an error
	BalanceQueryError,
	/// Failed allocation
	AllocationFailed,
	/// Gas limit reached
	GasLimit,
	/// Unknown runtime function
	Unknown,
	/// Passed string had invalid utf-8 encoding
	BadUtf8,
	/// Log event error
	Log,
	/// Other error in native code
	Other,
	/// Syscall signature mismatch
	InvalidSyscall,
	/// Unreachable instruction encountered
	Unreachable,
	/// Invalid virtual call
	InvalidVirtualCall,
	/// Division by zero
	DivisionByZero,
	/// Invalid conversion to integer
	InvalidConversionToInt,
	/// Stack overflow
	StackOverflow,
	/// Panic with message
	Panic(String),
}

impl wasmi::HostError for Error { }

impl From<Trap> for Error {
	fn from(trap: Trap) -> Self {
		match *trap.kind() {
			TrapKind::Unreachable => Error::Unreachable,
			TrapKind::MemoryAccessOutOfBounds => Error::MemoryAccessViolation,
			TrapKind::TableAccessOutOfBounds | TrapKind::ElemUninitialized => Error::InvalidVirtualCall,
			TrapKind::DivisionByZero => Error::DivisionByZero,
			TrapKind::InvalidConversionToInt => Error::InvalidConversionToInt,
			TrapKind::UnexpectedSignature => Error::InvalidVirtualCall,
			TrapKind::StackOverflow => Error::StackOverflow,
			TrapKind::Host(_) => Error::Other,
		}
	}
}

impl From<WasmiError> for Error {
	fn from(err: WasmiError) -> Self {
		match err {
			WasmiError::Value(_) => Error::InvalidSyscall,
			WasmiError::Memory(_) => Error::MemoryAccessViolation,
			_ => Error::Other,
		}
	}
}

impl ::std::fmt::Display for Error {
	fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::result::Result<(), ::std::fmt::Error> {
		match *self {
			Error::StorageReadError => write!(f, "Storage read error"),
			Error::StorageUpdateError => write!(f, "Storage update error"),
			Error::MemoryAccessViolation => write!(f, "Memory access violation"),
			Error::InvalidGasState => write!(f, "Invalid gas state"),
			Error::BalanceQueryError => write!(f, "Balance query resulted in an error"),
			Error::Return => write!(f, "Return result"),
			Error::Unknown => write!(f, "Unknown runtime function invoked"),
			Error::AllocationFailed => write!(f, "Memory allocation failed (OOM)"),
			Error::BadUtf8 => write!(f, "String encoding is bad utf-8 sequence"),
			Error::GasLimit => write!(f, "Invocation resulted in gas limit violated"),
			Error::Log => write!(f, "Error occured while logging an event"),
			Error::InvalidSyscall => write!(f, "Invalid syscall signature encountered at runtime"),
			Error::Other => write!(f, "Other unspecified error"),
			Error::Unreachable => write!(f, "Unreachable instruction encountered"),
			Error::InvalidVirtualCall => write!(f, "Invalid virtual call"),
			Error::DivisionByZero => write!(f, "Division by zero"),
			Error::StackOverflow => write!(f, "Stack overflow"),
			Error::InvalidConversionToInt => write!(f, "Invalid conversion to integer"),
			Error::Panic(ref msg) => write!(f, "Panic: {}", msg),
		}
	}
}


type Result<T> = ::std::result::Result<T, Error>;

pub struct Runtime<'a> {
	ext: &'a mut Externalities,
    context: RuntimeContext,
    memory: Memory,
}

impl<'a> Runtime<'a> {
    pub fn new(
		ext: &mut Externalities,
        context: RuntimeContext,
		memory: Memory,
    ) -> Runtime {
        Runtime {
            ext,
            context,
            memory,
        }
    }

    fn read_buffer(&self, offset: u32) -> Result<Vec<u8>> {
        let len: u32 = self.memory.get_u32(offset).map_err(|_| Error::MemoryAccessViolation)?;
        let buf = self.memory.get(offset + 4, len as usize).map_err(|_| Error::MemoryAccessViolation)?;
		Ok(buf)
    }

	/// Read from the storage to wasm memory
	pub fn storage_read_len(&mut self, args: RuntimeArgs) -> Result<RuntimeValue> {
        /*
		let key = self.h256_at(args.nth_checked(0)?)?;
		let val_ptr: u32 = args.nth_checked(1)?;

		let val = self.ext.storage_at(&key).map_err(|_| Error::StorageReadError)?;

		self.adjusted_charge(|schedule| schedule.sload_gas as u64)?;

		self.memory.set(val_ptr as u32, &*val)?;
        */
		Ok(RuntimeValue::I32(0))
	}

	/// Read from the storage to wasm memory
	pub fn storage_read_into(&mut self, args: RuntimeArgs) -> Result<()> {
        /*
		let key = self.h256_at(args.nth_checked(0)?)?;
		let val_ptr: u32 = args.nth_checked(1)?;

		let val = self.ext.storage_at(&key).map_err(|_| Error::StorageReadError)?;

		self.adjusted_charge(|schedule| schedule.sload_gas as u64)?;

		self.memory.set(val_ptr as u32, &*val)?;
        */
		Ok(())
	}

	/// Write to storage from wasm memory
	pub fn storage_write(&mut self, args: RuntimeArgs) -> Result<()> {
		let key_ptr: u32 = args.nth_checked(0)?;
		let val_ptr: u32 = args.nth_checked(1)?;

		let key = self.read_buffer(key_ptr)?;
		let val = self.read_buffer(val_ptr)?;

		self.ext.storage_put(&key, &val).map_err(|_| Error::StorageUpdateError)?;
		Ok(())
	}

	fn charge_gas(&mut self, amount: u64) -> bool {
		println!("Charge gas {}", amount);
		true
		/*
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
		*/
	}

	pub fn gas(&mut self, args: RuntimeArgs) -> Result<()> {
		let amount: u32 = args.nth_checked(0)?;
		if self.charge_gas(amount as u64) {
			Ok(())
		} else {
			Err(Error::GasLimit)
		}
	}

}

mod ext_impl {

	use wasmi::{Externals, RuntimeArgs, RuntimeValue, Trap};
	use ext::ids::*;

	macro_rules! void {
		{ $e: expr } => { { $e?; Ok(None) } }
	}

	macro_rules! some {
		{ $e: expr } => { { Ok(Some($e?)) } }
	}

	macro_rules! cast {
		{ $e: expr } => { { Ok(Some($e)) } }
	}

	impl<'a> Externals for super::Runtime<'a> {
		fn invoke_index(
			&mut self,
			index: usize,
			args: RuntimeArgs,
		) -> Result<Option<RuntimeValue>, Trap> {
			match index {
				STORAGE_WRITE_FUNC => void!(self.storage_write(args)),
				STORAGE_READ_LEN_FUNC => some!(self.storage_read_len(args)),
				STORAGE_READ_INTO_FUNC => void!(self.storage_read_into(args)),
				GAS_FUNC => void!(self.gas(args)),
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
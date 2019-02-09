use primitives::types::{PromiseId, AccountId, Balance, Mana, BlockIndex};
use wasmer_runtime::error as WasmerError;

#[derive(Debug, PartialEq, Eq)]
/// Error that can occur while preparing or executing wasm smart-contract.
pub enum PrepareError {
    /// Error happened while serializing the module.
    Serialization,

    /// Error happened while deserializing the module.
    Deserialization,

    /// Internal memory declaration has been found in the module.
    InternalMemoryDeclared,

    /// Gas instrumentation failed.
    ///
    /// This most likely indicates the module isn't valid.
    GasInstrumentation,

    /// Stack instrumentation failed.
    ///
    /// This  most likely indicates the module isn't valid.
    StackHeightInstrumentation,

    /// Error happened during invocation of the contract's entrypoint.
    ///
    /// Most likely because of trap.
    Invoke,

    /// Error happened during instantiation.
    ///
    /// This might indicate that `start` function trapped, or module isn't
    /// instantiable and/or unlinkable.
    Instantiate,

    /// Memory creation error.
    ///
    /// This might happen when the memory import has invalid descriptor or
    /// requested too much resources.
    Memory,
}

/// User trap in native code
#[allow(unused)]
#[derive(Debug, Clone, PartialEq)]
pub enum RuntimeError {
    /// Storage read error
    StorageReadError,
    /// Storage update error
    StorageUpdateError,
    /// Memory access violation
    MemoryAccessViolation,
    /// Native code returned incorrect value
    InvalidReturn,
    /// Error in external promise method
    PromiseError,
    /// Invalid promise index given by the WASM
    InvalidPromiseIndex,
    /// Invalid result index given by the WASM to read results
    InvalidResultIndex,
    // WASM is trying to read data from a result that is an error
    ResultIsNotOk,
    /// Invalid gas state inside interpreter
    InvalidGasState,
    /// Query of the balance resulted in an error
    BalanceQueryError,
    /// Transfer exceeded the available balance of the account
    BalanceExceeded,
    /// WASM-side assert failed
    AssertFailed,
    /// Mana limit exceeded
    ManaLimit,
    /// Gas limit reached
    GasLimit,
    /// Unknown runtime function
    Unknown,
    /// Passed string had invalid utf-8 encoding
    BadUtf8,
    /// Passed string had invalid utf-16 encoding
    BadUtf16,
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
    /// Unknown buffer type index for reading or writing
    UnknownBufferTypeIndex,
    /// Invalid account id
    InvalidAccountId,
    /// Creating a promise with a private method. The method name starts with '_'.
    PrivateMethod,
    /// Creating a callback with an empty method name.
    EmptyMethodName,
    /// Creating a promise with an empty method name and 0 amount.
    /// It's considered useless waste of mana
    EmptyMethodNameWithZeroAmount,
    /// Panic with message
    Panic(String),
}

impl ::std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::result::Result<(), ::std::fmt::Error> {
        match *self {
            RuntimeError::StorageReadError => write!(f, "Storage read error"),
            RuntimeError::StorageUpdateError => write!(f, "Storage update error"),
            RuntimeError::MemoryAccessViolation => write!(f, "Memory access violation"),
            RuntimeError::InvalidGasState => write!(f, "Invalid gas state"),
            RuntimeError::BalanceQueryError => write!(f, "Balance query resulted in an error"),
            RuntimeError::BalanceExceeded => write!(f, "Transfer exceeded the available balance of the account"),
            RuntimeError::InvalidReturn => write!(f, "Invalid return value"),
            RuntimeError::PromiseError => write!(f, "Error in the external promise method"),
            RuntimeError::InvalidPromiseIndex => write!(f, "Invalid promise index given by WASM"),
            RuntimeError::InvalidResultIndex => write!(f, "Invalid result index given by the WASM to read results"),
            RuntimeError::ResultIsNotOk => write!(f, "WASM is trying to read data from a result that is an error"),
            RuntimeError::Unknown => write!(f, "Unknown runtime function invoked"),
            RuntimeError::AssertFailed => write!(f, "WASM-side assert failed"),
            RuntimeError::BadUtf8 => write!(f, "String encoding is bad utf-8 sequence"),
            RuntimeError::BadUtf16 => write!(f, "String encoding is bad utf-16 sequence"),
            RuntimeError::ManaLimit => write!(f, "Mana limit exceeded"),
            RuntimeError::GasLimit => write!(f, "Invocation resulted in gas limit violated"),
            RuntimeError::Log => write!(f, "Error occured while logging an event"),
            RuntimeError::InvalidSyscall => write!(f, "Invalid syscall signature encountered at runtime"),
            RuntimeError::Other => write!(f, "Other unspecified error"),
            RuntimeError::Unreachable => write!(f, "Unreachable instruction encountered"),
            RuntimeError::InvalidVirtualCall => write!(f, "Invalid virtual call"),
            RuntimeError::DivisionByZero => write!(f, "Division by zero"),
            RuntimeError::StackOverflow => write!(f, "Stack overflow"),
            RuntimeError::InvalidConversionToInt => write!(f, "Invalid conversion to integer"),
            RuntimeError::UnknownBufferTypeIndex => write!(f, "Unknown buffer type index"),
            RuntimeError::InvalidAccountId => write!(f, "Invalid AccountID"),
            RuntimeError::PrivateMethod => write!(f, "Creating a promise with a private method"),
            RuntimeError::EmptyMethodName => write!(f, "Creating a callback with an empty method name"),
            RuntimeError::EmptyMethodNameWithZeroAmount => write!(f, "Creating a promise with an empty method name and 0 amount"),
            RuntimeError::Panic(ref msg) => write!(f, "Panic: {}", msg),
        }
    }
}

/// Wrapped error
#[derive(Debug)]
pub enum Error {
    /// Method name can't be decoded to UTF8.
    BadUtf8,

    /// Method name is empty.
    EmptyMethodName,

    /// Method is private, because it starts with '_'.
    PrivateMethod,

    Wasmer(WasmerError::Error),

    Runtime(RuntimeError),

    Prepare(PrepareError),
}

impl From<WasmerError::Error> for Error {
    fn from(e: WasmerError::Error) -> Self {
        Error::Wasmer(e)
    }
}

impl From<RuntimeError> for Error {
    fn from(e: RuntimeError) -> Self {
        Error::Runtime(e)
    }
}

/// Returned data from the method. 
#[derive(Clone, Debug)]
pub enum ReturnData {
    /// Method returned some value or data.
    Value(Vec<u8>),

    /// Method returned a promise.
    Promise(PromiseId),

    /// Method hasn't returned any data or promise.
    None,
}

// TODO: Extract it to the root of the crate
#[derive(Clone, Debug)]
pub struct Config {
    /// Gas cost of a growing memory by single page.
    pub grow_mem_cost: u32,

    /// Gas cost of a regular operation.
    pub regular_op_cost: u32,

    /// Gas cost per one byte returned.
    pub return_data_per_byte_cost: u32,

    /// How tall the stack is allowed to grow?
    ///
    /// See https://wiki.parity.io/WebAssembly-StackHeight to find out
    /// how the stack frame cost is calculated.
    pub max_stack_height: u32,

    /// What is the maximal memory pages amount is allowed to have for
    /// a contract.
    pub max_memory_pages: u32,

    /// Gas limit of the one contract call
    pub gas_limit: u64,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            grow_mem_cost: 1,
            regular_op_cost: 1,
            return_data_per_byte_cost: 1,
            max_stack_height: 64 * 1024,
            max_memory_pages: 32,
            gas_limit: 10 * 1024 * 1024,
        }
    }
}

/// Context for the WASM contract execution.
#[derive(Default, Clone, Debug)]
pub struct RuntimeContext {
    /// Initial balance is the balance of the account before the
    /// received_amount is added.
    pub initial_balance: Balance,
    /// The amount sent by the Sender.
    pub received_amount: Balance,
    /// Originator's Account ID.
    pub originator_id: AccountId,
    /// Current Account ID.
    pub account_id: AccountId,
    /// Available mana for the execution by this contract.
    pub mana: Mana,
    /// Currently produced block index
    pub block_index: BlockIndex,
    /// Initial seed for randomness
    pub random_seed: Vec<u8>,
}

impl RuntimeContext {
    pub fn new(
        initial_balance: Balance,
        received_amount: Balance,
        sender_id: &AccountId,
        account_id: &AccountId,
        mana: Mana,
        block_index: BlockIndex,
        random_seed: Vec<u8>,
    ) -> RuntimeContext {
        RuntimeContext {
            initial_balance,
            received_amount,
            originator_id: sender_id.clone(),
            account_id: account_id.clone(),
            mana,
            block_index,
            random_seed,
        }
    }
}
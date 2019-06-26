use std::fmt;

use wasmer_runtime::error as WasmerError;

use near_primitives::hash::{hash, CryptoHash};
use near_primitives::logging;
use near_primitives::types::{AccountId, Balance, BlockIndex, PromiseId, StorageUsage};

use crate::types::Error::Runtime;
use near_primitives::crypto::signature::PublicKey;

#[derive(Debug, Clone)]
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

    /// Memory error.
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
    /// Storage remove error
    StorageRemoveError,
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
    /// Gas limit reached
    UsageLimit,
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
    /// Unknown data type index for reading or writing
    UnknownDataTypeIndex,
    /// Invalid account id
    InvalidAccountId,
    /// Creating a promise with a private method. The method name starts with '_'.
    PrivateMethod,
    /// Creating a callback with an empty method name.
    EmptyMethodName,
    /// Panic with message
    Panic(String),
}

impl ::std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::result::Result<(), ::std::fmt::Error> {
        match *self {
            RuntimeError::StorageReadError => write!(f, "Storage read error"),
            RuntimeError::StorageUpdateError => write!(f, "Storage update error"),
            RuntimeError::StorageRemoveError => write!(f, "Storage remove error"),
            RuntimeError::MemoryAccessViolation => write!(f, "Memory access violation"),
            RuntimeError::InvalidGasState => write!(f, "Invalid gas state"),
            RuntimeError::BalanceQueryError => write!(f, "Balance query resulted in an error"),
            RuntimeError::BalanceExceeded => {
                write!(f, "Transfer exceeded the available balance of the account")
            }
            RuntimeError::InvalidReturn => write!(f, "Invalid return value"),
            RuntimeError::PromiseError => write!(f, "Error in the external promise method"),
            RuntimeError::InvalidPromiseIndex => write!(f, "Invalid promise index given by WASM"),
            RuntimeError::InvalidResultIndex => {
                write!(f, "Invalid result index given by the WASM to read results")
            }
            RuntimeError::ResultIsNotOk => {
                write!(f, "WASM is trying to read data from a result that is an error")
            }
            RuntimeError::Unknown => write!(f, "Unknown runtime function invoked"),
            RuntimeError::AssertFailed => write!(f, "WASM-side assert failed"),
            RuntimeError::BadUtf8 => write!(f, "String encoding is bad utf-8 sequence"),
            RuntimeError::BadUtf16 => write!(f, "String encoding is bad utf-16 sequence"),
            RuntimeError::UsageLimit => write!(f, "Invocation resulted in usage limit violated"),
            RuntimeError::Log => write!(f, "Error occured while logging an event"),
            RuntimeError::InvalidSyscall => {
                write!(f, "Invalid syscall signature encountered at runtime")
            }
            RuntimeError::Other => write!(f, "Other unspecified error"),
            RuntimeError::Unreachable => write!(f, "Unreachable instruction encountered"),
            RuntimeError::InvalidVirtualCall => write!(f, "Invalid virtual call"),
            RuntimeError::DivisionByZero => write!(f, "Division by zero"),
            RuntimeError::StackOverflow => write!(f, "Stack overflow"),
            RuntimeError::InvalidConversionToInt => write!(f, "Invalid conversion to integer"),
            RuntimeError::UnknownDataTypeIndex => {
                write!(f, "Unknown data type index for reading or writing")
            }
            RuntimeError::InvalidAccountId => write!(f, "Invalid AccountID"),
            RuntimeError::PrivateMethod => write!(f, "Creating a promise with a private method"),
            RuntimeError::EmptyMethodName => {
                write!(f, "Creating a callback with an empty method name")
            }
            RuntimeError::Panic(ref msg) => write!(f, "Panic: {}", msg),
        }
    }
}

/// Wrapped error
#[derive(Debug, Clone)]
pub enum Error {
    /// Method name can't be decoded to UTF8.
    BadUtf8,

    /// Method name is empty.
    EmptyMethodName,

    /// Method is private, because it starts with '_'.
    PrivateMethod,

    Wasmer(String), // TODO: WasmerError::Error is not shareable between threads

    Runtime(RuntimeError),

    Prepare(PrepareError),

    Cache(String),
}

impl From<WasmerError::RuntimeError> for Error {
    fn from(e: WasmerError::RuntimeError) -> Self {
        let default_msg = Error::Wasmer(format!("{}", e));
        match e {
            WasmerError::RuntimeError::Trap { msg: _ } => default_msg,
            WasmerError::RuntimeError::Error { data } => {
                if let Some(err) = data.downcast_ref::<RuntimeError>() {
                    Runtime(err.clone())
                } else {
                    default_msg
                }
            }
        }
    }
}

impl From<WasmerError::Error> for Error {
    fn from(e: WasmerError::Error) -> Self {
        let default_msg = Error::Wasmer(format!("{}", e));
        match e {
            WasmerError::Error::CallError(ce) => match ce {
                WasmerError::CallError::Resolve(_) => default_msg,
                WasmerError::CallError::Runtime(re) => re.into(),
            },
            WasmerError::Error::RuntimeError(re) => re.into(),
            WasmerError::Error::CompileError(_)
            | WasmerError::Error::LinkError(_)
            | WasmerError::Error::ResolveError(_)
            | WasmerError::Error::CreationError(_) => default_msg,
        }
    }
}

impl From<RuntimeError> for Error {
    fn from(e: RuntimeError) -> Self {
        Error::Runtime(e)
    }
}

/// Returned data from the method.
#[derive(Clone)]
pub enum ReturnData {
    /// Method returned some value or data.
    Value(Vec<u8>),

    /// Method returned a promise.
    Promise(PromiseId),

    /// Method hasn't returned any data or promise.
    None,
}

impl ReturnData {
    pub fn to_result(&self) -> Option<Vec<u8>> {
        match self {
            ReturnData::Value(v) => Some(v.clone()),
            _ => Some(vec![]),
        }
    }
}

impl fmt::Debug for ReturnData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ReturnData::Value(v) => {
                f.debug_tuple("Value").field(&format_args!("{}", logging::pretty_utf8(&v))).finish()
            }
            ReturnData::Promise(promise_id) => f.debug_tuple("Promise").field(&promise_id).finish(),
            ReturnData::None => write!(f, "None"),
        }
    }
}

// TODO: Extract it to the root of the crate
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    /// Gas cost of a growing memory by single page.
    pub grow_mem_cost: u32,

    /// Gas cost of a regular operation.
    pub regular_op_cost: u32,

    /// Gas cost per one byte returned.
    pub return_data_per_byte_cost: u32,

    /// Gas cost of the contract call.
    pub contract_call_cost: Balance,

    /// How tall the stack is allowed to grow?
    ///
    /// See https://wiki.parity.io/WebAssembly-StackHeight to find out
    /// how the stack frame cost is calculated.
    pub max_stack_height: u32,

    // The initial number of memory pages.
    pub initial_memory_pages: u32,

    /// What is the maximal memory pages amount is allowed to have for
    /// a contract.
    pub max_memory_pages: u32,

    /// Gas limit of the one contract call
    pub usage_limit: u64,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            grow_mem_cost: 1,
            regular_op_cost: 1,
            return_data_per_byte_cost: 1,
            contract_call_cost: 0,
            max_stack_height: 64 * 1024,
            initial_memory_pages: 17,
            max_memory_pages: 32,
            usage_limit: 1024 * 1024 * 1024,
        }
    }
}

/// Context for the WASM contract execution.
#[derive(Clone, Debug)]
pub struct RuntimeContext<'a> {
    /// Initial balance is the balance of the account before the
    /// received_amount is added.
    pub initial_balance: Balance,
    /// The amount sent by the Sender.
    pub received_amount: Balance,
    /// Senders's Account ID. (Immediate predecessor)
    pub sender_id: &'a AccountId,
    /// Current Account ID.
    pub account_id: &'a AccountId,
    /// Storage that the account is already using.
    pub storage_usage: StorageUsage,
    /// Currently produced block index
    pub block_index: BlockIndex,
    /// Initial seed for randomness
    pub random_seed: Vec<u8>,
    /// Whether the execution should not charge any costs.
    pub free_of_charge: bool,
    /// Account ID of the account who signed the initial transaction.
    pub originator_id: &'a AccountId,
    /// The public key used to sign the initial transaction.
    pub public_key: &'a PublicKey,
}

impl<'a> RuntimeContext<'a> {
    pub fn new(
        initial_balance: Balance,
        received_amount: Balance,
        sender_id: &'a AccountId,
        account_id: &'a AccountId,
        storage_usage: StorageUsage,
        block_index: BlockIndex,
        random_seed: Vec<u8>,
        free_of_charge: bool,
        originator_id: &'a AccountId,
        public_key: &'a PublicKey,
    ) -> RuntimeContext<'a> {
        RuntimeContext {
            initial_balance,
            received_amount,
            sender_id,
            account_id,
            storage_usage,
            block_index,
            random_seed,
            free_of_charge,
            originator_id,
            public_key,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct ContractCode {
    code: Vec<u8>,
    hash: CryptoHash,
}

impl ContractCode {
    pub fn new(code: Vec<u8>) -> ContractCode {
        let hash = hash(&code);
        ContractCode { code, hash }
    }

    pub fn get_hash(&self) -> CryptoHash {
        self.hash
    }

    pub fn get_code(&self) -> &Vec<u8> {
        &self.code
    }
}

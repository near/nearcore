#![doc = include_str!("../README.md")]

use borsh::{BorshDeserialize, BorshSerialize};
use near_account_id::AccountId;
use near_rpc_error_macro::RpcError;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt::{self, Error, Formatter};

#[derive(Debug, PartialEq, Eq)]
pub enum VMError {
    FunctionCallError(FunctionCallError),
    /// Type erased error from `External` trait implementation.
    ExternalError(AnyError),
    /// An error that is caused by an operation on an inconsistent state.
    /// E.g. an integer overflow by using a value from the given context.
    InconsistentStateError(InconsistentStateError),
    /// Error caused by caching.
    CacheError(CacheError),
}

#[derive(Debug, PartialEq, Eq)]
pub enum FunctionCallError {
    /// Wasm compilation error
    CompilationError(CompilationError),
    /// Wasm binary env link error
    LinkError {
        msg: String,
    },
    /// Import/export resolve error
    MethodResolveError(MethodResolveError),
    /// A trap happened during execution of a binary
    WasmTrap(WasmTrap),
    WasmUnknownError {
        debug_message: String,
    },
    HostError(HostError),
    // Unused, can be reused by a future error but must be exactly one error to keep Nondeterministic
    // error borsh serialized at correct index
    _EVMError,
    /// Non-deterministic error.
    Nondeterministic(String),
}

/// Serializable version of `FunctionCallError`. Must never reorder/remove elements, can only
/// add new variants at the end (but do that very carefully). This type must be never used
/// directly, and must be converted to `ContractCallError` instead using `into()` converter.
/// It describes stable serialization format, and only used by serialization logic.
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Debug, Clone, PartialEq, Eq, BorshDeserialize, BorshSerialize, Serialize, Deserialize)]
pub enum FunctionCallErrorSer {
    /// Wasm compilation error
    CompilationError(CompilationError),
    /// Wasm binary env link error
    LinkError {
        msg: String,
    },
    /// Import/export resolve error
    MethodResolveError(MethodResolveError),
    /// A trap happened during execution of a binary
    WasmTrap(WasmTrap),
    WasmUnknownError,
    HostError(HostError),
    // Unused, can be reused by a future error but must be exactly one error to keep ExecutionError
    // error borsh serialized at correct index
    _EVMError,
    ExecutionError(String),
}

#[derive(Debug, PartialEq, Eq)]
pub enum CacheError {
    ReadError,
    WriteError,
    DeserializationError,
    SerializationError { hash: [u8; 32] },
}
/// A kind of a trap happened during execution of a binary
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(
    Debug, Clone, PartialEq, Eq, BorshDeserialize, BorshSerialize, Deserialize, Serialize, RpcError,
)]
pub enum WasmTrap {
    /// An `unreachable` opcode was executed.
    Unreachable,
    /// Call indirect incorrect signature trap.
    IncorrectCallIndirectSignature,
    /// Memory out of bounds trap.
    MemoryOutOfBounds,
    /// Call indirect out of bounds trap.
    CallIndirectOOB,
    /// An arithmetic exception, e.g. divided by zero.
    IllegalArithmetic,
    /// Misaligned atomic access trap.
    MisalignedAtomicAccess,
    /// Indirect call to null.
    IndirectCallToNull,
    /// Stack overflow.
    StackOverflow,
    /// Generic trap.
    GenericTrap,
}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(
    Debug, Clone, PartialEq, Eq, BorshDeserialize, BorshSerialize, Deserialize, Serialize, RpcError,
)]
pub enum MethodResolveError {
    MethodEmptyName,
    MethodNotFound,
    MethodInvalidSignature,
}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(
    Debug, Clone, PartialEq, Eq, BorshDeserialize, BorshSerialize, Deserialize, Serialize, RpcError,
)]
pub enum CompilationError {
    CodeDoesNotExist { account_id: AccountId },
    PrepareError(PrepareError),
    WasmerCompileError { msg: String },
    UnsupportedCompiler { msg: String },
}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(
    Debug, Clone, PartialEq, Eq, BorshDeserialize, BorshSerialize, Deserialize, Serialize, RpcError,
)]
/// Error that can occur while preparing or executing Wasm smart-contract.
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
    /// Error happened during instantiation.
    ///
    /// This might indicate that `start` function trapped, or module isn't
    /// instantiable and/or unlinkable.
    Instantiate,
    /// Error creating memory.
    Memory,
    /// Contract contains too many functions.
    TooManyFunctions,
    /// Contract contains too many locals.
    TooManyLocals,
}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(
    Debug, Clone, PartialEq, Eq, BorshDeserialize, BorshSerialize, Deserialize, Serialize, RpcError,
)]
pub enum HostError {
    /// String encoding is bad UTF-16 sequence
    BadUTF16,
    /// String encoding is bad UTF-8 sequence
    BadUTF8,
    /// Exceeded the prepaid gas
    GasExceeded,
    /// Exceeded the maximum amount of gas allowed to burn per contract
    GasLimitExceeded,
    /// Exceeded the account balance
    BalanceExceeded,
    /// Tried to call an empty method name
    EmptyMethodName,
    /// Smart contract panicked
    GuestPanic { panic_msg: String },
    /// IntegerOverflow happened during a contract execution
    IntegerOverflow,
    /// `promise_idx` does not correspond to existing promises
    InvalidPromiseIndex { promise_idx: u64 },
    /// Actions can only be appended to non-joint promise.
    CannotAppendActionToJointPromise,
    /// Returning joint promise is currently prohibited
    CannotReturnJointPromise,
    /// Accessed invalid promise result index
    InvalidPromiseResultIndex { result_idx: u64 },
    /// Accessed invalid register id
    InvalidRegisterId { register_id: u64 },
    /// Iterator `iterator_index` was invalidated after its creation by performing a mutable operation on trie
    IteratorWasInvalidated { iterator_index: u64 },
    /// Accessed memory outside the bounds
    MemoryAccessViolation,
    /// VM Logic returned an invalid receipt index
    InvalidReceiptIndex { receipt_index: u64 },
    /// Iterator index `iterator_index` does not exist
    InvalidIteratorIndex { iterator_index: u64 },
    /// VM Logic returned an invalid account id
    InvalidAccountId,
    /// VM Logic returned an invalid method name
    InvalidMethodName,
    /// VM Logic provided an invalid public key
    InvalidPublicKey,
    /// `method_name` is not allowed in view calls
    ProhibitedInView { method_name: String },
    /// The total number of logs will exceed the limit.
    NumberOfLogsExceeded { limit: u64 },
    /// The storage key length exceeded the limit.
    KeyLengthExceeded { length: u64, limit: u64 },
    /// The storage value length exceeded the limit.
    ValueLengthExceeded { length: u64, limit: u64 },
    /// The total log length exceeded the limit.
    TotalLogLengthExceeded { length: u64, limit: u64 },
    /// The maximum number of promises within a FunctionCall exceeded the limit.
    NumberPromisesExceeded { number_of_promises: u64, limit: u64 },
    /// The maximum number of input data dependencies exceeded the limit.
    NumberInputDataDependenciesExceeded { number_of_input_data_dependencies: u64, limit: u64 },
    /// The returned value length exceeded the limit.
    ReturnedValueLengthExceeded { length: u64, limit: u64 },
    /// The contract size for DeployContract action exceeded the limit.
    ContractSizeExceeded { size: u64, limit: u64 },
    /// The host function was deprecated.
    Deprecated { method_name: String },
    /// General errors for ECDSA recover.
    ECRecoverError { msg: String },
    /// Invalid input to alt_bn128 familiy of functions (e.g., point which isn't
    /// on the curve).
    AltBn128InvalidInput { msg: String },
    /// Invalid input to ed25519 signature verification function (e.g. signature cannot be
    /// derived from bytes).
    Ed25519VerifyInvalidInput { msg: String },
}

#[derive(Debug, PartialEq)]
pub enum VMLogicError {
    /// Errors coming from native Wasm VM.
    HostError(HostError),
    /// Type erased error from `External` trait implementation.
    ExternalError(AnyError),
    /// An error that is caused by an operation on an inconsistent state.
    InconsistentStateError(InconsistentStateError),
}

impl std::error::Error for VMLogicError {}

/// An error that is caused by an operation on an inconsistent state, such as
/// integer overflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InconsistentStateError {
    /// Math operation with a value from the state resulted in a integer overflow.
    IntegerOverflow,
}

impl From<HostError> for VMLogicError {
    fn from(err: HostError) -> Self {
        VMLogicError::HostError(err)
    }
}

impl From<InconsistentStateError> for VMLogicError {
    fn from(err: InconsistentStateError) -> Self {
        VMLogicError::InconsistentStateError(err)
    }
}

impl From<PrepareError> for VMError {
    fn from(err: PrepareError) -> Self {
        VMError::FunctionCallError(FunctionCallError::CompilationError(
            CompilationError::PrepareError(err),
        ))
    }
}

impl From<VMLogicError> for VMError {
    fn from(err: VMLogicError) -> Self {
        match err {
            VMLogicError::HostError(h) => {
                VMError::FunctionCallError(FunctionCallError::HostError(h))
            }
            VMLogicError::ExternalError(s) => VMError::ExternalError(s),
            VMLogicError::InconsistentStateError(e) => VMError::InconsistentStateError(e),
        }
    }
}

impl fmt::Display for VMLogicError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{:?}", self)
    }
}

impl fmt::Display for PrepareError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        use PrepareError::*;
        f.write_str(match self {
            Serialization => "Error happened while serializing the module.",
            Deserialization => "Error happened while deserializing the module.",
            InternalMemoryDeclared => "Internal memory declaration has been found in the module.",
            GasInstrumentation => "Gas instrumentation failed.",
            StackHeightInstrumentation => "Stack instrumentation failed.",
            Instantiate => "Error happened during instantiation.",
            Memory => "Error creating memory.",
            TooManyFunctions => "Too many functions in contract.",
            TooManyLocals => "Too many locals declared in the contract.",
        })
    }
}

impl fmt::Display for FunctionCallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            FunctionCallError::CompilationError(e) => e.fmt(f),
            FunctionCallError::MethodResolveError(e) => e.fmt(f),
            FunctionCallError::HostError(e) => e.fmt(f),
            FunctionCallError::LinkError { msg } => write!(f, "{}", msg),
            FunctionCallError::WasmTrap(trap) => write!(f, "WebAssembly trap: {}", trap),
            FunctionCallError::WasmUnknownError { debug_message } => {
                write!(f, "Unknown error during Wasm contract execution: {}", debug_message)
            }
            FunctionCallError::Nondeterministic(msg) => {
                write!(f, "Nondeterministic error during contract execution: {}", msg)
            }
            FunctionCallError::_EVMError => unreachable!(),
        }
    }
}

impl fmt::Display for WasmTrap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            WasmTrap::Unreachable => write!(f, "An `unreachable` opcode was executed."),
            WasmTrap::IncorrectCallIndirectSignature => {
                write!(f, "Call indirect incorrect signature trap.")
            }
            WasmTrap::MemoryOutOfBounds => write!(f, "Memory out of bounds trap."),
            WasmTrap::CallIndirectOOB => write!(f, "Call indirect out of bounds trap."),
            WasmTrap::IllegalArithmetic => {
                write!(f, "An arithmetic exception, e.g. divided by zero.")
            }
            WasmTrap::MisalignedAtomicAccess => write!(f, "Misaligned atomic access trap."),
            WasmTrap::GenericTrap => write!(f, "Generic trap."),
            WasmTrap::StackOverflow => write!(f, "Stack overflow."),
            WasmTrap::IndirectCallToNull => write!(f, "Indirect call to null."),
        }
    }
}

impl fmt::Display for CompilationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            CompilationError::CodeDoesNotExist { account_id } => {
                write!(f, "cannot find contract code for account {}", account_id)
            }
            CompilationError::PrepareError(p) => write!(f, "PrepareError: {}", p),
            CompilationError::WasmerCompileError { msg } => {
                write!(f, "Wasmer compilation error: {}", msg)
            }
            CompilationError::UnsupportedCompiler { msg } => {
                write!(f, "Unsupported compiler: {}", msg)
            }
        }
    }
}

impl fmt::Display for MethodResolveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        fmt::Debug::fmt(self, f)
    }
}

impl fmt::Display for VMError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            VMError::FunctionCallError(err) => fmt::Display::fmt(err, f),
            VMError::ExternalError(_err) => write!(f, "Serialized ExternalError"),
            VMError::InconsistentStateError(err) => fmt::Display::fmt(err, f),
            VMError::CacheError(err) => write!(f, "Cache error: {:?}", err),
        }
    }
}

impl std::fmt::Display for InconsistentStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            InconsistentStateError::IntegerOverflow => write!(
                f,
                "Math operation with a value from the state resulted in a integer overflow.",
            ),
        }
    }
}

impl std::fmt::Display for HostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        use HostError::*;
        match self {
            BadUTF8 => write!(f, "String encoding is bad UTF-8 sequence."),
            BadUTF16 => write!(f, "String encoding is bad UTF-16 sequence."),
            GasExceeded => write!(f, "Exceeded the prepaid gas."),
            GasLimitExceeded => write!(f, "Exceeded the maximum amount of gas allowed to burn per contract."),
            BalanceExceeded => write!(f, "Exceeded the account balance."),
            EmptyMethodName => write!(f, "Tried to call an empty method name."),
            GuestPanic { panic_msg } => write!(f, "Smart contract panicked: {}", panic_msg),
            IntegerOverflow => write!(f, "Integer overflow."),
            InvalidIteratorIndex { iterator_index } => write!(f, "Iterator index {:?} does not exist", iterator_index),
            InvalidPromiseIndex { promise_idx } => write!(f, "{:?} does not correspond to existing promises", promise_idx),
            CannotAppendActionToJointPromise => write!(f, "Actions can only be appended to non-joint promise."),
            CannotReturnJointPromise => write!(f, "Returning joint promise is currently prohibited."),
            InvalidPromiseResultIndex { result_idx } => write!(f, "Accessed invalid promise result index: {:?}", result_idx),
            InvalidRegisterId { register_id } => write!(f, "Accessed invalid register id: {:?}", register_id),
            IteratorWasInvalidated { iterator_index } => write!(f, "Iterator {:?} was invalidated after its creation by performing a mutable operation on trie", iterator_index),
            MemoryAccessViolation => write!(f, "Accessed memory outside the bounds."),
            InvalidReceiptIndex { receipt_index } => write!(f, "VM Logic returned an invalid receipt index: {:?}", receipt_index),
            InvalidAccountId => write!(f, "VM Logic returned an invalid account id"),
            InvalidMethodName => write!(f, "VM Logic returned an invalid method name"),
            InvalidPublicKey => write!(f, "VM Logic provided an invalid public key"),
            ProhibitedInView { method_name } => write!(f, "{} is not allowed in view calls", method_name),
            NumberOfLogsExceeded { limit } => write!(f, "The number of logs will exceed the limit {}", limit),
            KeyLengthExceeded { length, limit } => write!(f, "The length of a storage key {} exceeds the limit {}", length, limit),
            ValueLengthExceeded { length, limit } => write!(f, "The length of a storage value {} exceeds the limit {}", length, limit),
            TotalLogLengthExceeded{ length, limit } => write!(f, "The length of a log message {} exceeds the limit {}", length, limit),
            NumberPromisesExceeded { number_of_promises, limit } => write!(f, "The number of promises within a FunctionCall {} exceeds the limit {}", number_of_promises, limit),
            NumberInputDataDependenciesExceeded { number_of_input_data_dependencies, limit } => write!(f, "The number of input data dependencies {} exceeds the limit {}", number_of_input_data_dependencies, limit),
            ReturnedValueLengthExceeded { length, limit } => write!(f, "The length of a returned value {} exceeds the limit {}", length, limit),
            ContractSizeExceeded { size, limit } => write!(f, "The size of a contract code in DeployContract action {} exceeds the limit {}", size, limit),
            Deprecated {method_name}=> write!(f, "Attempted to call deprecated host function {}", method_name),
            AltBn128InvalidInput { msg } => write!(f, "AltBn128 invalid input: {}", msg),
            ECRecoverError { msg } => write!(f, "ECDSA recover error: {}", msg),
            Ed25519VerifyInvalidInput { msg } => write!(f, "ED25519 signature verification error: {}", msg),
        }
    }
}

/// Type-erased error used to shuttle some concrete error coming from `External`
/// through vm-logic.
///
/// The caller is supposed to downcast this to a concrete error type they should
/// know. This would be just `Box<dyn Any + Eq>` if the latter actually worked.
pub struct AnyError {
    any: Box<dyn AnyEq>,
}

impl AnyError {
    pub fn new<E: Any + Eq + Send + Sync + 'static>(err: E) -> AnyError {
        AnyError { any: Box::new(err) }
    }
    pub fn downcast<E: Any + Eq + Send + Sync + 'static>(self) -> Result<E, ()> {
        match self.any.into_any().downcast::<E>() {
            Ok(it) => Ok(*it),
            Err(_) => Err(()),
        }
    }
}

impl PartialEq for AnyError {
    fn eq(&self, other: &Self) -> bool {
        self.any.any_eq(&*other.any)
    }
}

impl Eq for AnyError {}

impl fmt::Debug for AnyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.any.as_any(), f)
    }
}

trait AnyEq: Any + Send + Sync {
    fn any_eq(&self, rhs: &dyn AnyEq) -> bool;
    fn as_any(&self) -> &dyn Any;
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

impl<T: Any + Eq + Sized + Send + Sync> AnyEq for T {
    fn any_eq(&self, rhs: &dyn AnyEq) -> bool {
        match rhs.as_any().downcast_ref::<Self>() {
            Some(rhs) => self == rhs,
            None => false,
        }
    }
    fn as_any(&self) -> &dyn Any {
        &*self
    }
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::{CompilationError, FunctionCallError, MethodResolveError, PrepareError, VMError};

    #[test]
    fn test_display() {
        // TODO: proper printing
        assert_eq!(
            VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                MethodResolveError::MethodInvalidSignature
            ))
            .to_string(),
            "MethodInvalidSignature"
        );
        assert_eq!(
            VMError::FunctionCallError(FunctionCallError::CompilationError(
                CompilationError::PrepareError(PrepareError::StackHeightInstrumentation)
            ))
            .to_string(),
            "PrepareError: Stack instrumentation failed."
        );
    }
}

use borsh::{BorshDeserialize, BorshSerialize};
use near_rpc_error_macro::RpcError;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, BorshDeserialize, BorshSerialize, Deserialize, Serialize)]
pub enum VMError {
    FunctionCallError(FunctionCallError),
    /// Serialized external error from External trait implementation.
    ExternalError(Vec<u8>),
    /// An error that is caused by an operation on an inconsistent state.
    /// E.g. an integer overflow by using a value from the given context.
    InconsistentStateError(InconsistentStateError),
}

#[derive(
    Debug, Clone, PartialEq, Eq, BorshDeserialize, BorshSerialize, Deserialize, Serialize, RpcError,
)]
pub enum FunctionCallError {
    CompilationError(CompilationError),
    LinkError { msg: String },
    MethodResolveError(MethodResolveError),
    WasmTrap { msg: String },
    HostError(HostError),
}

#[derive(
    Debug, Clone, PartialEq, Eq, BorshDeserialize, BorshSerialize, Deserialize, Serialize, RpcError,
)]
pub enum MethodResolveError {
    MethodEmptyName,
    MethodUTF8Error,
    MethodNotFound,
    MethodInvalidSignature,
}

#[derive(
    Debug, Clone, PartialEq, Eq, BorshDeserialize, BorshSerialize, Deserialize, Serialize, RpcError,
)]
pub enum CompilationError {
    CodeDoesNotExist { account_id: String },
    PrepareError(PrepareError),
    WasmerCompileError { msg: String },
}

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
}

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
}

#[derive(Debug, Clone, PartialEq, BorshDeserialize, BorshSerialize, Deserialize, Serialize)]
pub enum VMLogicError {
    HostError(HostError),
    /// Serialized external error from External trait implementation.
    ExternalError(Vec<u8>),
    /// An error that is caused by an operation on an inconsistent state.
    InconsistentStateError(InconsistentStateError),
}

/// An error that is caused by an operation on an inconsistent state.
/// E.g. a deserialization error or an integer overflow.
#[derive(Debug, Clone, PartialEq, Eq, BorshDeserialize, BorshSerialize, Deserialize, Serialize)]
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

impl fmt::Display for PrepareError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        use PrepareError::*;
        match self {
            Serialization => write!(f, "Error happened while serializing the module."),
            Deserialization => write!(f, "Error happened while deserializing the module."),
            InternalMemoryDeclared => {
                write!(f, "Internal memory declaration has been found in the module.")
            }
            GasInstrumentation => write!(f, "Gas instrumentation failed."),
            StackHeightInstrumentation => write!(f, "Stack instrumentation failed."),
            Instantiate => write!(f, "Error happened during instantiation."),
            Memory => write!(f, "Error creating memory"),
        }
    }
}

impl fmt::Display for FunctionCallError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            FunctionCallError::CompilationError(e) => e.fmt(f),
            FunctionCallError::MethodResolveError(e) => e.fmt(f),
            FunctionCallError::HostError(e) => e.fmt(f),
            FunctionCallError::LinkError { msg } => write!(f, "{}", msg),
            FunctionCallError::WasmTrap { msg } => write!(f, "WebAssembly trap: {}", msg),
        }
    }
}

impl fmt::Display for CompilationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            CompilationError::CodeDoesNotExist { account_id } => {
                write!(f, "cannot find contract code for account {}", account_id)
            }
            CompilationError::PrepareError(p) => write!(f, "PrepareError: {}", p),
            CompilationError::WasmerCompileError { msg } => {
                write!(f, "Wasmer compilation error: {}", msg)
            }
        }
    }
}

impl fmt::Display for MethodResolveError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        fmt::Debug::fmt(self, f)
    }
}

impl fmt::Display for VMError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            VMError::FunctionCallError(err) => fmt::Display::fmt(err, f),
            VMError::ExternalError(_err) => write!(f, "Serialized ExternalError"),
            VMError::InconsistentStateError(err) => fmt::Display::fmt(err, f),
        }
    }
}

impl std::fmt::Display for InconsistentStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            InconsistentStateError::IntegerOverflow => write!(
                f,
                "Math operation with a value from the state resulted in a integer overflow.",
            ),
        }
    }
}

impl std::fmt::Display for HostError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
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
        }
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

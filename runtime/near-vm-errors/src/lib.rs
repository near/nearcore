use borsh::{BorshDeserialize, BorshSerialize};
use near_rpc_error_macro::RpcError;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(
    Debug, Clone, PartialEq, Eq, BorshDeserialize, BorshSerialize, Deserialize, Serialize, RpcError,
)]
pub enum VMError {
    FunctionExecError(FunctionExecError),
    // TODO: serialize/deserialize?
    StorageError(Vec<u8>),
}

#[derive(
    Debug, Clone, PartialEq, Eq, BorshDeserialize, BorshSerialize, Deserialize, Serialize, RpcError,
)]
pub enum FunctionExecError {
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
}

#[derive(Debug, Clone, PartialEq, BorshDeserialize, BorshSerialize, Deserialize, Serialize)]
pub enum HostErrorOrStorageError {
    HostError(HostError),
    /// Error from underlying storage, serialized
    StorageError(Vec<u8>),
}

impl From<HostError> for HostErrorOrStorageError {
    fn from(err: HostError) -> Self {
        HostErrorOrStorageError::HostError(err)
    }
}

impl From<PrepareError> for VMError {
    fn from(err: PrepareError) -> Self {
        VMError::FunctionExecError(FunctionExecError::CompilationError(
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

impl fmt::Display for FunctionExecError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            FunctionExecError::CompilationError(e) => e.fmt(f),
            FunctionExecError::MethodResolveError(e) => e.fmt(f),
            FunctionExecError::HostError(e) => e.fmt(f),
            FunctionExecError::LinkError { msg } => write!(f, "{}", msg),
            FunctionExecError::WasmTrap { msg } => write!(f, "WebAssembly trap: {}", msg),
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
            VMError::FunctionExecError(err) => fmt::Display::fmt(err, f),
            VMError::StorageError(_err) => write!(f, "StorageError"),
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
            GuestPanic{ panic_msg } => write!(f, "Smart contract panicked: {}", panic_msg),
            IntegerOverflow => write!(f, "Integer overflow."),
            InvalidIteratorIndex{iterator_index} => write!(f, "Iterator index {:?} does not exist", iterator_index),
            InvalidPromiseIndex{promise_idx} => write!(f, "{:?} does not correspond to existing promises", promise_idx),
            CannotAppendActionToJointPromise => write!(f, "Actions can only be appended to non-joint promise."),
            CannotReturnJointPromise => write!(f, "Returning joint promise is currently prohibited."),
            InvalidPromiseResultIndex{result_idx} => write!(f, "Accessed invalid promise result index: {:?}", result_idx),
            InvalidRegisterId{register_id} => write!(f, "Accessed invalid register id: {:?}", register_id),
            IteratorWasInvalidated{iterator_index} => write!(f, "Iterator {:?} was invalidated after its creation by performing a mutable operation on trie", iterator_index),
            MemoryAccessViolation => write!(f, "Accessed memory outside the bounds."),
            InvalidReceiptIndex{receipt_index} => write!(f, "VM Logic returned an invalid receipt index: {:?}", receipt_index),
            InvalidAccountId => write!(f, "VM Logic returned an invalid account id"),
            InvalidMethodName => write!(f, "VM Logic returned an invalid method name"),
            InvalidPublicKey => write!(f, "VM Logic provided an invalid public key"),
            ProhibitedInView{method_name} => write!(f, "{} is not allowed in view calls", method_name),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{CompilationError, FunctionExecError, MethodResolveError, PrepareError, VMError};

    #[test]
    fn test_display() {
        // TODO: proper printing
        assert_eq!(
            VMError::FunctionExecError(FunctionExecError::MethodResolveError(
                MethodResolveError::MethodInvalidSignature
            ))
            .to_string(),
            "MethodInvalidSignature"
        );
        assert_eq!(
            VMError::FunctionExecError(FunctionExecError::CompilationError(
                CompilationError::PrepareError(PrepareError::StackHeightInstrumentation)
            ))
            .to_string(),
            "PrepareError: Stack instrumentation failed."
        );
    }
}

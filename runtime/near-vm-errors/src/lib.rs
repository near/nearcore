use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VMError {
    FunctionCallError(FunctionCallError),
    StorageError(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FunctionCallError {
    CompilationError(CompilationError),
    LinkError(String),
    ResolveError(MethodResolveError),
    WasmTrap(String),
    HostError(HostError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MethodResolveError {
    MethodEmptyName,
    MethodUTF8Error,
    MethodNotFound,
    MethodInvalidSignature,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompilationError {
    CodeDoesNotExist(String),
    PrepareError(PrepareError),
    WasmerCompileError(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostError {
    BadUTF16,
    BadUTF8,
    GasExceeded,
    GasLimitExceeded,
    BalanceExceeded,
    EmptyMethodName,
    GuestPanic(String),
    IntegerOverflow,
    InvalidPromiseIndex(u64),
    CannotAppendActionToJointPromise,
    CannotReturnJointPromise,
    InvalidPromiseResultIndex(u64),
    InvalidRegisterId(u64),
    IteratorWasInvalidated(u64),
    MemoryAccessViolation,
    InvalidReceiptIndex(u64),
    InvalidIteratorIndex(u64),
    InvalidAccountId,
    InvalidMethodName,
    InvalidPublicKey,
    ProhibitedInView(String),
    TooManyLogs,
    KeyLengthExceeded,
    ValueLengthExceeded,
    LogLengthExceeded,
    NumPromisesExceeded,
    NumInputDataDependenciesExceeded,
    ReturnedValueLengthExceeded,
    ContractSizeExceeded,
}

#[derive(Debug, Clone, PartialEq)]
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
            FunctionCallError::ResolveError(e) => e.fmt(f),
            FunctionCallError::HostError(e) => e.fmt(f),
            FunctionCallError::LinkError(s) => write!(f, "{}", s),
            FunctionCallError::WasmTrap(s) => write!(f, "WebAssembly trap: {}", s),
        }
    }
}

impl fmt::Display for CompilationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            CompilationError::CodeDoesNotExist(account_id) => {
                write!(f, "cannot find contract code for account {}", account_id)
            }
            CompilationError::PrepareError(p) => write!(f, "PrepareError: {}", p),
            CompilationError::WasmerCompileError(s) => write!(f, "Wasmer compilation error: {}", s),
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
            GuestPanic(s) => write!(f, "Smart contract panicked: {}", s),
            IntegerOverflow => write!(f, "Integer overflow."),
            InvalidIteratorIndex(index) => write!(f, "Iterator index {:?} does not exist", index),
            InvalidPromiseIndex(index) => write!(f, "{:?} does not correspond to existing promises", index),
            CannotAppendActionToJointPromise => write!(f, "Actions can only be appended to non-joint promise."),
            CannotReturnJointPromise => write!(f, "Returning joint promise is currently prohibited."),
            InvalidPromiseResultIndex(index) => write!(f, "Accessed invalid promise result index: {:?}", index),
            InvalidRegisterId(id) => write!(f, "Accessed invalid register id: {:?}", id),
            IteratorWasInvalidated(index) => write!(f, "Iterator {:?} was invalidated after its creation by performing a mutable operation on trie", index),
            MemoryAccessViolation => write!(f, "Accessed memory outside the bounds."),
            InvalidReceiptIndex(index) => write!(f, "VM Logic returned an invalid receipt index: {:?}", index),
            InvalidAccountId => write!(f, "VM Logic returned an invalid account id"),
            InvalidMethodName => write!(f, "VM Logic returned an invalid method name"),
            InvalidPublicKey => write!(f, "VM Logic provided an invalid public key"),
            ProhibitedInView(method_name) => write!(f, "{} is not allowed in view calls", method_name),
            TooManyLogs => write!(f, "Exceeded the maximum number of log messages"),
            KeyLengthExceeded => write!(f, "Exceeded the length of a storage key"),
            ValueLengthExceeded => write!(f, "Exceeded the length of a storage value"),
            LogLengthExceeded => write!(f, "Exceeded the length of a log message"),
            NumPromisesExceeded => write!(f, "Exceeded the maximum number of promises within a function call"),
            NumInputDataDependenciesExceeded => write!(f, "Exceeded the maximum number of input data dependencies"),
            ReturnedValueLengthExceeded => write!(f, "Exceeded the returned value length"),
            ContractSizeExceeded => write!(f, "Exceeded the length of a deploy contract code"),
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
            VMError::FunctionCallError(FunctionCallError::ResolveError(
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

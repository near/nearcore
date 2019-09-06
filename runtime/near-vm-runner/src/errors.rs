use near_vm_logic::HostError;
use wasmer_runtime::error::{CallError, CompileError, CreationError, RuntimeError};

#[derive(Debug, Clone)]
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

impl std::fmt::Display for PrepareError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
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

#[derive(Debug, Clone, PartialEq)]
/// Error that occurs when trying to run a method from a smart contract.
/// TODO handling for StorageError
pub enum VMError {
    /// Error occurs during the preparation of smart contract.
    PrepareError(String),
    /// Error that occurs when creating memory for Wasmer to run.
    WasmerMemoryCreation(String),
    /// Error that occurs when compiling prepared Wasm with Wasmer.
    WasmerCompileError(String),
    /// Instantiates a Wasm module can raise an error, if `start` function is specified.
    WasmerInstantiateError(String),
    /// Error when calling a method using Wasmer, includes errors raised by the host functions.
    WasmerCallError(String),
    /// Tried to invoke method using empty name.
    MethodEmptyName,
    /// Tried to invoke a method name that was not UTF-8 encoded.
    MethodUTF8Error,
}

impl From<PrepareError> for VMError {
    fn from(err: PrepareError) -> Self {
        VMError::PrepareError(format!("{}", err))
    }
}

impl From<CompileError> for VMError {
    fn from(err: CompileError) -> Self {
        VMError::WasmerCompileError(format!("{}", err))
    }
}

impl From<CreationError> for VMError {
    fn from(err: CreationError) -> Self {
        VMError::WasmerMemoryCreation(format!("{}", err))
    }
}

impl From<CallError> for VMError {
    fn from(err: CallError) -> Self {
        if let CallError::Runtime(RuntimeError::Error { data }) = &err {
            if let Some(err) = data.downcast_ref::<HostError>() {
                return VMError::WasmerCallError(format!("{}", err));
            }
        }
        VMError::WasmerCallError(format!("{}", err))
    }
}

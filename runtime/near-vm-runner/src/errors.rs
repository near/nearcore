use near_vm_errors::{CompilationError, FunctionExecError, MethodResolveError, VMError};
use near_vm_logic::HostErrorOrStorageError;

pub trait IntoVMError {
    fn into_vm_error(self) -> VMError;
}

impl IntoVMError for wasmer_runtime::error::Error {
    fn into_vm_error(self) -> VMError {
        use wasmer_runtime::error::Error;
        match self {
            Error::CompileError(err) => err.into_vm_error(),
            Error::LinkError(err) => VMError::FunctionExecError(FunctionExecError::LinkError(
                format!("{:.500}", Error::LinkError(err).to_string()),
            )),
            Error::RuntimeError(err) => err.into_vm_error(),
            Error::ResolveError(err) => err.into_vm_error(),
            Error::CallError(err) => err.into_vm_error(),
            Error::CreationError(err) => panic!(err),
        }
    }
}

impl IntoVMError for wasmer_runtime::error::CallError {
    fn into_vm_error(self) -> VMError {
        use wasmer_runtime::error::CallError;
        match self {
            CallError::Resolve(err) => err.into_vm_error(),
            CallError::Runtime(err) => err.into_vm_error(),
        }
    }
}

impl IntoVMError for wasmer_runtime::error::CompileError {
    fn into_vm_error(self) -> VMError {
        VMError::FunctionExecError(FunctionExecError::CompilationError(
            CompilationError::WasmerCompileError(self.to_string()),
        ))
    }
}

impl IntoVMError for wasmer_runtime::error::ResolveError {
    fn into_vm_error(self) -> VMError {
        use wasmer_runtime::error::ResolveError as WasmerResolveError;
        match self {
            WasmerResolveError::Signature { .. } => VMError::FunctionExecError(
                FunctionExecError::ResolveError(MethodResolveError::MethodInvalidSignature),
            ),
            WasmerResolveError::ExportNotFound { .. } => VMError::FunctionExecError(
                FunctionExecError::ResolveError(MethodResolveError::MethodNotFound),
            ),
            WasmerResolveError::ExportWrongType { .. } => VMError::FunctionExecError(
                FunctionExecError::ResolveError(MethodResolveError::MethodNotFound),
            ),
        }
    }
}

impl IntoVMError for wasmer_runtime::error::RuntimeError {
    fn into_vm_error(self) -> VMError {
        use wasmer_runtime::error::RuntimeError;
        match &self {
            RuntimeError::Trap { msg } => {
                VMError::FunctionExecError(FunctionExecError::WasmTrap(msg.to_string()))
            }
            RuntimeError::Error { data } => {
                if let Some(err) = data.downcast_ref::<HostErrorOrStorageError>() {
                    match err {
                        HostErrorOrStorageError::StorageError(s) => {
                            VMError::StorageError(s.clone())
                        }
                        HostErrorOrStorageError::HostError(h) => {
                            VMError::FunctionExecError(FunctionExecError::HostError(h.clone()))
                        }
                    }
                } else {
                    eprintln!(
                        "Bad error case! Output is non-deterministic {:?} {:?}",
                        data.type_id(),
                        self.to_string()
                    );
                    VMError::FunctionExecError(FunctionExecError::WasmTrap("unknown".to_string()))
                }
            }
        }
    }
}

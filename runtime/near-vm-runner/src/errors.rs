use near_vm_errors::{CompilationError, FunctionCallError, MethodResolveError, VMError};
use near_vm_logic::VMLogicError;

pub trait IntoVMError {
    fn into_vm_error(self) -> VMError;
}

impl IntoVMError for wasmer_runtime::error::Error {
    fn into_vm_error(self) -> VMError {
        use wasmer_runtime::error::Error;
        match self {
            Error::CompileError(err) => err.into_vm_error(),
            Error::LinkError(err) => VMError::FunctionCallError(FunctionCallError::LinkError {
                msg: format!("{:.500}", Error::LinkError(err).to_string()),
            }),
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
        match self {
            wasmer_runtime::error::CompileError::InternalError { .. } => {
                // An internal Wasmer error the most probably is a result of a node malfunction
                panic!("Internal Wasmer error on Wasm compilation: {}", self);
            }
            _ => VMError::FunctionCallError(FunctionCallError::CompilationError(
                CompilationError::WasmerCompileError { msg: self.to_string() },
            )),
        }
    }
}

impl IntoVMError for wasmer_runtime::error::ResolveError {
    fn into_vm_error(self) -> VMError {
        use wasmer_runtime::error::ResolveError as WasmerResolveError;
        match self {
            WasmerResolveError::Signature { .. } => VMError::FunctionCallError(
                FunctionCallError::MethodResolveError(MethodResolveError::MethodInvalidSignature),
            ),
            WasmerResolveError::ExportNotFound { .. } => VMError::FunctionCallError(
                FunctionCallError::MethodResolveError(MethodResolveError::MethodNotFound),
            ),
            WasmerResolveError::ExportWrongType { .. } => VMError::FunctionCallError(
                FunctionCallError::MethodResolveError(MethodResolveError::MethodNotFound),
            ),
        }
    }
}

impl IntoVMError for wasmer_runtime::error::RuntimeError {
    fn into_vm_error(self) -> VMError {
        use wasmer_runtime::ExceptionCode;
        let data = &*self.0;

        // TODO: Wasmer provides no way to distingush runtime Internal Wasmer errors or host panics
        // (at least for a single-pass backend)
        // https://github.com/wasmerio/wasmer/issues/1338
        if let Some(err) = data.downcast_ref::<VMLogicError>() {
            match err {
                VMLogicError::HostError(h) => {
                    VMError::FunctionCallError(FunctionCallError::HostError(h.clone()))
                }
                VMLogicError::ExternalError(s) => VMError::ExternalError(s.clone()),
                VMLogicError::InconsistentStateError(e) => {
                    VMError::InconsistentStateError(e.clone())
                }
            }
        } else if let Some(_) = data.downcast_ref::<ExceptionCode>() {
            VMError::FunctionCallError(FunctionCallError::WasmTrap { msg: format!("{}", self) })
        } else if let Some(_) = data.downcast_ref::<String>() {
            VMError::FunctionCallError(FunctionCallError::WasmTrap { msg: format!("{}", self) })
        } else if let Some(_) = data.downcast_ref::<&str>() {
            VMError::FunctionCallError(FunctionCallError::WasmTrap { msg: format!("{}", self) })
        } else {
            eprintln!(
                "Bad error case! Output is non-deterministic {:?} {:?}",
                data.type_id(),
                self.to_string()
            );
            VMError::FunctionCallError(FunctionCallError::WasmTrap { msg: "unknown".into() })
        }
    }
}

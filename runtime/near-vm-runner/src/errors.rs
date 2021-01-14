use near_vm_errors::{FunctionCallError, VMError, VMLogicError};
pub trait IntoVMError {
    fn into_vm_error(self) -> VMError;
}

impl IntoVMError for &VMLogicError {
    fn into_vm_error(self) -> VMError {
        match self {
            VMLogicError::HostError(h) => {
                VMError::FunctionCallError(FunctionCallError::HostError(h.clone()))
            }
            VMLogicError::ExternalError(s) => VMError::ExternalError(s.clone()),
            VMLogicError::InconsistentStateError(e) => VMError::InconsistentStateError(e.clone()),
            VMLogicError::EvmError(_) => unreachable!("Wasm can't return EVM error"),
        }
    }
}

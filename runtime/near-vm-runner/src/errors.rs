use crate::logic::errors::{FunctionCallError, VMRunnerError};

pub trait IntoVMError {
    fn into_vm_error(self) -> Result<FunctionCallError, VMRunnerError>;
}

#[derive(Debug, PartialEq)]
pub enum ContractPrecompilatonResult {
    ContractCompiled,
    ContractAlreadyInCache,
    CacheNotAvailable,
}

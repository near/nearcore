use core::fmt;
use near_vm_errors::VMError;

pub trait IntoVMError {
    fn into_vm_error(self) -> VMError;
}

#[derive(Debug, PartialEq)]
pub struct ContractPrecompilatonError {
    // TODO: actually make this a distinct error
    inner: VMError,
}

#[derive(Debug, PartialEq)]
pub enum ContractPrecompilatonResult {
    ContractCompiled,
    ContractAlreadyInCache,
    CacheNotAvailable,
}

impl ContractPrecompilatonError {
    pub fn new(err: VMError) -> ContractPrecompilatonError {
        ContractPrecompilatonError { inner: err }
    }
}

impl fmt::Display for ContractPrecompilatonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

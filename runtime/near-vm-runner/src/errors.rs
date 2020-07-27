use near_vm_errors::VMError;

pub trait IntoVMError {
    fn into_vm_error(self) -> VMError;
}

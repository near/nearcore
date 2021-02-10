pub enum ViewAccountError {
    InvalidAccountId(near_primitives::types::AccountId),
    AccountDoesNotExist(near_primitives::types::AccountId),
    StorageError(near_primitives::errors::StorageError),
}

pub enum ViewContractCodeError {
    InvalidAccountId(near_primitives::types::AccountId),
    AccountDoesNotExist(near_primitives::types::AccountId),
    StorageError(near_primitives::errors::StorageError),
    ContractCodeDoesNotExist(near_primitives::types::AccountId),
}

pub enum ViewAccessKeyError {
    InvalidAccountId(near_primitives::types::AccountId),
    StorageError(near_primitives::errors::StorageError),
    AccessKeyDoesNotExist(near_crypto::PublicKey),
    InternalError(String),
}

pub enum ViewStateError {
    InvalidAccountId(near_primitives::types::AccountId),
    StorageError(near_primitives::errors::StorageError),
}

pub enum CallFunctionError {
    InvalidAccountId(near_primitives::types::AccountId),
    AccountDoesNotExist(near_primitives::types::AccountId),
    StorageError(near_primitives::errors::StorageError),
    VMError(String),
}

impl From<near_primitives::errors::StorageError> for ViewAccountError {
    fn from(storage_error: near_primitives::errors::StorageError) -> Self {
        Self::StorageError(storage_error)
    }
}

impl From<ViewAccountError> for ViewContractCodeError {
    fn from(view_account_error: ViewAccountError) -> Self {
        match view_account_error {
            ViewAccountError::InvalidAccountId(account_id) => Self::AccountDoesNotExist(account_id),
            ViewAccountError::AccountDoesNotExist(account_id) => {
                Self::AccountDoesNotExist(account_id)
            }
            ViewAccountError::StorageError(storage_error) => Self::StorageError(storage_error),
        }
    }
}

impl From<near_primitives::errors::StorageError> for ViewContractCodeError {
    fn from(storage_error: near_primitives::errors::StorageError) -> Self {
        Self::StorageError(storage_error)
    }
}

impl From<near_primitives::errors::StorageError> for ViewAccessKeyError {
    fn from(storage_error: near_primitives::errors::StorageError) -> Self {
        Self::StorageError(storage_error)
    }
}

impl From<near_primitives::errors::StorageError> for ViewStateError {
    fn from(storage_error: near_primitives::errors::StorageError) -> Self {
        Self::StorageError(storage_error)
    }
}

impl From<near_primitives::errors::StorageError> for CallFunctionError {
    fn from(storage_error: near_primitives::errors::StorageError) -> Self {
        Self::StorageError(storage_error)
    }
}

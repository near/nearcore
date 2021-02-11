#[derive(thiserror::Error, Debug)]
pub enum ViewAccountError {
    #[error("Account ID {0} is invalid")]
    InvalidAccountId(near_primitives::types::AccountId),
    #[error("Account ID {0} does not exist")]
    AccountDoesNotExist(near_primitives::types::AccountId),
    #[error("Storage error: {0}")]
    StorageError(near_primitives::errors::StorageError),
}

#[derive(thiserror::Error, Debug)]
pub enum ViewContractCodeError {
    #[error("Account ID {0} is invalid")]
    InvalidAccountId(near_primitives::types::AccountId),
    #[error("Account ID {0} does not exist")]
    AccountDoesNotExist(near_primitives::types::AccountId),
    #[error("Storage error: {0}")]
    StorageError(near_primitives::errors::StorageError),
    #[error("Contract code for contract ID {0} does not exist")]
    ContractCodeDoesNotExist(near_primitives::types::AccountId),
}

#[derive(thiserror::Error, Debug)]
pub enum ViewAccessKeyError {
    #[error("Account ID {0} is invalid")]
    InvalidAccountId(near_primitives::types::AccountId),
    #[error("Storage error: {0}")]
    StorageError(near_primitives::errors::StorageError),
    #[error("Access key for public key {0} does not exist")]
    AccessKeyDoesNotExist(near_crypto::PublicKey),
    #[error("Internal error occurred: {0}")]
    InternalError(String),
}

#[derive(thiserror::Error, Debug)]
pub enum ViewStateError {
    #[error("Account ID {0} is invalid")]
    InvalidAccountId(near_primitives::types::AccountId),
    #[error("Storage error: {0}")]
    StorageError(near_primitives::errors::StorageError),
}

#[derive(thiserror::Error, Debug)]
pub enum CallFunctionError {
    #[error("Account ID {0} is invalid")]
    InvalidAccountId(near_primitives::types::AccountId),
    #[error("Account ID {0} does not exist")]
    AccountDoesNotExist(near_primitives::types::AccountId),
    #[error("Storage error: {0}")]
    StorageError(near_primitives::errors::StorageError),
    #[error("VM error occurred: {0}")]
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

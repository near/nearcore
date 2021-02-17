#[derive(thiserror::Error, Debug)]
pub enum ViewAccountError {
    #[error("Account ID #{requested_account_id} is invalid")]
    InvalidAccountId { requested_account_id: near_primitives::types::AccountId },
    #[error("Account ID #{requested_account_id} does not exist")]
    AccountDoesNotExist { requested_account_id: near_primitives::types::AccountId },
    #[error("Storage error: #{storage_error}")]
    StorageError {
        #[from]
        storage_error: near_primitives::errors::StorageError,
    },
}

#[derive(thiserror::Error, Debug)]
pub enum ViewContractCodeError {
    #[error("Account ID #{requested_account_id} is invalid")]
    InvalidAccountId { requested_account_id: near_primitives::types::AccountId },
    #[error("Account ID #{requested_account_id} does not exist")]
    AccountDoesNotExist { requested_account_id: near_primitives::types::AccountId },
    #[error("Storage error: #{storage_error}")]
    StorageError {
        #[from]
        storage_error: near_primitives::errors::StorageError,
    },
    #[error("Contract code for contract ID #{contract_account_id} does not exist")]
    NoContractCode { contract_account_id: near_primitives::types::AccountId },
}

#[derive(thiserror::Error, Debug)]
pub enum ViewAccessKeyError {
    #[error("Account ID #{requested_account_id} is invalid")]
    InvalidAccountId { requested_account_id: near_primitives::types::AccountId },
    #[error("Storage error: #{storage_error:?}")]
    StorageError {
        #[from]
        storage_error: near_primitives::errors::StorageError,
    },
    #[error("Access key for public key #{public_key} does not exist")]
    AccessKeyDoesNotExist { public_key: near_crypto::PublicKey },
    #[error("Internal error occurred: #{error_message}")]
    InternalError { error_message: String },
}

#[derive(thiserror::Error, Debug)]
pub enum ViewStateError {
    #[error("Account ID #{requested_account_id} is invalid")]
    InvalidAccountId { requested_account_id: near_primitives::types::AccountId },
    #[error("Storage error: #{storage_error}")]
    StorageError {
        #[from]
        storage_error: near_primitives::errors::StorageError,
    },
}

#[derive(thiserror::Error, Debug)]
pub enum CallFunctionError {
    #[error("Account ID #{requested_account_id} is invalid")]
    InvalidAccountId { requested_account_id: near_primitives::types::AccountId },
    #[error("Account ID #{requested_account_id} does not exist")]
    AccountDoesNotExist { requested_account_id: near_primitives::types::AccountId },
    #[error("Storage error: {0}")]
    StorageError(#[from] near_primitives::errors::StorageError),
    #[error("VM error occurred: #{error_message}")]
    VMError { error_message: String },
}

impl From<ViewAccountError> for ViewContractCodeError {
    fn from(view_account_error: ViewAccountError) -> Self {
        match view_account_error {
            ViewAccountError::InvalidAccountId { requested_account_id } => {
                Self::AccountDoesNotExist { requested_account_id }
            }
            ViewAccountError::AccountDoesNotExist { requested_account_id } => {
                Self::AccountDoesNotExist { requested_account_id }
            }
            ViewAccountError::StorageError { storage_error } => {
                Self::StorageError { storage_error }
            }
        }
    }
}

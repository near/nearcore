#[derive(thiserror::Error, Debug)]
pub enum ViewAccountError {
    #[error("Account ID \"{requested_account_id}\" is invalid")]
    InvalidAccountId { requested_account_id: near_primitives::types::AccountId },
    #[error("Account ID #{requested_account_id} does not exist")]
    AccountDoesNotExist { requested_account_id: near_primitives::types::AccountId },
    #[error("Internal error: #{error_message}")]
    InternalError { error_message: String },
}

#[derive(thiserror::Error, Debug)]
pub enum ViewContractCodeError {
    #[error("Account ID \"{requested_account_id}\" is invalid")]
    InvalidAccountId { requested_account_id: near_primitives::types::AccountId },
    #[error("Account ID #{requested_account_id} does not exist")]
    AccountDoesNotExist { requested_account_id: near_primitives::types::AccountId },
    #[error("Contract code for contract ID #{contract_account_id} does not exist")]
    NoContractCode { contract_account_id: near_primitives::types::AccountId },
    #[error("Internal error: #{error_message}")]
    InternalError { error_message: String },
}

#[derive(thiserror::Error, Debug)]
pub enum ViewAccessKeyError {
    #[error("Account ID \"{requested_account_id}\" is invalid")]
    InvalidAccountId { requested_account_id: near_primitives::types::AccountId },
    #[error("Access key for public key #{public_key} does not exist")]
    AccessKeyDoesNotExist { public_key: near_crypto::PublicKey },
    #[error("Internal error: #{error_message}")]
    InternalError { error_message: String },
}

#[derive(thiserror::Error, Debug)]
pub enum ViewStateError {
    #[error("Account ID \"{requested_account_id}\" is invalid")]
    InvalidAccountId { requested_account_id: near_primitives::types::AccountId },
    #[error("Account {requested_account_id} does not exist")]
    AccountDoesNotExist { requested_account_id: near_primitives::types::AccountId },
    #[error("The state of {requested_account_id} is too large")]
    AccountStateTooLarge { requested_account_id: near_primitives::types::AccountId },
    #[error("Internal error: #{error_message}")]
    InternalError { error_message: String },
}

#[derive(thiserror::Error, Debug)]
pub enum CallFunctionError {
    #[error("Account ID \"{requested_account_id}\" is invalid")]
    InvalidAccountId { requested_account_id: near_primitives::types::AccountId },
    #[error("Account ID #{requested_account_id} does not exist")]
    AccountDoesNotExist { requested_account_id: near_primitives::types::AccountId },
    #[error("Internal error: #{error_message}")]
    InternalError { error_message: String },
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
            ViewAccountError::InternalError { error_message } => {
                Self::InternalError { error_message }
            }
        }
    }
}

impl From<near_primitives::errors::StorageError> for ViewAccountError {
    fn from(storage_error: near_primitives::errors::StorageError) -> Self {
        Self::InternalError { error_message: storage_error.to_string() }
    }
}

impl From<near_primitives::errors::StorageError> for ViewContractCodeError {
    fn from(storage_error: near_primitives::errors::StorageError) -> Self {
        Self::InternalError { error_message: storage_error.to_string() }
    }
}

impl From<near_primitives::errors::StorageError> for ViewAccessKeyError {
    fn from(storage_error: near_primitives::errors::StorageError) -> Self {
        Self::InternalError { error_message: storage_error.to_string() }
    }
}

impl From<near_primitives::errors::StorageError> for ViewStateError {
    fn from(storage_error: near_primitives::errors::StorageError) -> Self {
        Self::InternalError { error_message: storage_error.to_string() }
    }
}

impl From<near_primitives::errors::StorageError> for CallFunctionError {
    fn from(storage_error: near_primitives::errors::StorageError) -> Self {
        Self::InternalError { error_message: storage_error.to_string() }
    }
}

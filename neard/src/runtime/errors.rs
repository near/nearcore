use near_chain::near_chain_primitives::error::QueryError;

// This wrapper struct is necessary because we cannot implement
// From<> trait for the original QueryError struct since it is a foreign type
#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct WrappedQueryError(pub QueryError);

impl From<WrappedQueryError> for QueryError {
    fn from(error: WrappedQueryError) -> Self {
        error.0
    }
}

impl From<node_runtime::state_viewer::errors::ViewAccountError> for WrappedQueryError {
    fn from(error: node_runtime::state_viewer::errors::ViewAccountError) -> Self {
        match error {
            node_runtime::state_viewer::errors::ViewAccountError::InvalidAccountId {
                requested_account_id,
            } => Self(QueryError::InvalidAccount { requested_account_id }),
            node_runtime::state_viewer::errors::ViewAccountError::AccountDoesNotExist {
                requested_account_id,
            } => Self(QueryError::AccountDoesNotExist { requested_account_id }),
            node_runtime::state_viewer::errors::ViewAccountError::StorageError(storage_error) => {
                Self(QueryError::StorageError(storage_error))
            }
        }
    }
}

impl From<node_runtime::state_viewer::errors::ViewContractCodeError> for WrappedQueryError {
    fn from(error: node_runtime::state_viewer::errors::ViewContractCodeError) -> Self {
        match error {
            node_runtime::state_viewer::errors::ViewContractCodeError::InvalidAccountId {
                requested_account_id,
            } => Self(QueryError::InvalidAccount { requested_account_id }),
            node_runtime::state_viewer::errors::ViewContractCodeError::AccountDoesNotExist {
                requested_account_id,
            } => Self(QueryError::AccountDoesNotExist { requested_account_id }),
            node_runtime::state_viewer::errors::ViewContractCodeError::StorageError(
                storage_error,
            ) => Self(QueryError::StorageError(storage_error)),
            node_runtime::state_viewer::errors::ViewContractCodeError::NoContractCode {
                contract_account_id,
            } => Self(QueryError::ContractCodeDoesNotExist { contract_account_id }),
        }
    }
}

impl From<node_runtime::state_viewer::errors::CallFunctionError> for WrappedQueryError {
    fn from(error: node_runtime::state_viewer::errors::CallFunctionError) -> Self {
        match error {
            node_runtime::state_viewer::errors::CallFunctionError::InvalidAccountId {
                requested_account_id,
            } => Self(QueryError::InvalidAccount { requested_account_id }),
            node_runtime::state_viewer::errors::CallFunctionError::AccountDoesNotExist {
                requested_account_id,
            } => Self(QueryError::AccountDoesNotExist { requested_account_id }),
            node_runtime::state_viewer::errors::CallFunctionError::StorageError(storage_error) => {
                Self(QueryError::StorageError(storage_error))
            }
            node_runtime::state_viewer::errors::CallFunctionError::VMError(s) => {
                Self(QueryError::VMError(s))
            }
        }
    }
}

impl From<node_runtime::state_viewer::errors::ViewStateError> for WrappedQueryError {
    fn from(error: node_runtime::state_viewer::errors::ViewStateError) -> Self {
        match error {
            node_runtime::state_viewer::errors::ViewStateError::InvalidAccountId {
                requested_account_id,
            } => Self(QueryError::InvalidAccount { requested_account_id }),
            node_runtime::state_viewer::errors::ViewStateError::StorageError(storage_error) => {
                Self(QueryError::StorageError(storage_error))
            }
        }
    }
}

impl From<node_runtime::state_viewer::errors::ViewAccessKeyError> for WrappedQueryError {
    fn from(error: node_runtime::state_viewer::errors::ViewAccessKeyError) -> Self {
        match error {
            node_runtime::state_viewer::errors::ViewAccessKeyError::InvalidAccountId {
                requested_account_id,
            } => Self(QueryError::InvalidAccount { requested_account_id }),
            node_runtime::state_viewer::errors::ViewAccessKeyError::StorageError(storage_error) => {
                Self(QueryError::StorageError(storage_error))
            }
            node_runtime::state_viewer::errors::ViewAccessKeyError::AccessKeyDoesNotExist {
                public_key,
            } => Self(QueryError::AccessKeyDoesNotExist { public_key: public_key.to_string() }),
            node_runtime::state_viewer::errors::ViewAccessKeyError::InternalError {
                error_message,
            } => Self(QueryError::InternalError { error_message }),
        }
    }
}

impl From<near_primitives::errors::EpochError> for WrappedQueryError {
    fn from(error: near_primitives::errors::EpochError) -> Self {
        Self(QueryError::InternalError { error_message: error.to_string() })
    }
}

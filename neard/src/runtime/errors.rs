use near_chain::near_chain_primitives::error::QueryError;

#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct RuntimeQueryError(#[from] pub QueryError);

impl From<RuntimeQueryError> for QueryError {
    fn from(error: RuntimeQueryError) -> Self {
        error.0
    }
}

impl From<node_runtime::state_viewer::errors::ViewAccountError> for RuntimeQueryError {
    fn from(error: node_runtime::state_viewer::errors::ViewAccountError) -> Self {
        match error {
            node_runtime::state_viewer::errors::ViewAccountError::InvalidAccountId(account_id) => {
                Self(QueryError::InvalidAccount(account_id))
            }
            node_runtime::state_viewer::errors::ViewAccountError::AccountDoesNotExist(
                account_id,
            ) => Self(QueryError::AccountDoesNotExist(account_id)),
            node_runtime::state_viewer::errors::ViewAccountError::StorageError(storage_error) => {
                Self(QueryError::StorageError(storage_error))
            }
        }
    }
}

impl From<node_runtime::state_viewer::errors::ViewContractCodeError> for RuntimeQueryError {
    fn from(error: node_runtime::state_viewer::errors::ViewContractCodeError) -> Self {
        match error {
            node_runtime::state_viewer::errors::ViewContractCodeError::InvalidAccountId(
                account_id,
            ) => Self(QueryError::InvalidAccount(account_id)),
            node_runtime::state_viewer::errors::ViewContractCodeError::AccountDoesNotExist(
                account_id,
            ) => Self(QueryError::AccountDoesNotExist(account_id)),
            node_runtime::state_viewer::errors::ViewContractCodeError::StorageError(
                storage_error,
            ) => Self(QueryError::StorageError(storage_error)),
            node_runtime::state_viewer::errors::ViewContractCodeError::ContractCodeDoesNotExist(
                contract_id,
            ) => Self(QueryError::ContractCodeDoesNotExist(contract_id)),
        }
    }
}

impl From<node_runtime::state_viewer::errors::CallFunctionError> for RuntimeQueryError {
    fn from(error: node_runtime::state_viewer::errors::CallFunctionError) -> Self {
        match error {
            node_runtime::state_viewer::errors::CallFunctionError::InvalidAccountId(account_id) => {
                Self(QueryError::InvalidAccount(account_id))
            }
            node_runtime::state_viewer::errors::CallFunctionError::AccountDoesNotExist(
                account_id,
            ) => Self(QueryError::AccountDoesNotExist(account_id)),
            node_runtime::state_viewer::errors::CallFunctionError::StorageError(storage_error) => {
                Self(QueryError::StorageError(storage_error))
            }
            node_runtime::state_viewer::errors::CallFunctionError::VMError(s) => {
                Self(QueryError::VMError(s))
            }
        }
    }
}

impl From<node_runtime::state_viewer::errors::ViewStateError> for RuntimeQueryError {
    fn from(error: node_runtime::state_viewer::errors::ViewStateError) -> Self {
        match error {
            node_runtime::state_viewer::errors::ViewStateError::InvalidAccountId(account_id) => {
                Self(QueryError::InvalidAccount(account_id))
            }
            node_runtime::state_viewer::errors::ViewStateError::StorageError(storage_error) => {
                Self(QueryError::StorageError(storage_error))
            }
        }
    }
}

impl From<near_primitives::errors::EpochError> for RuntimeQueryError {
    fn from(error: near_primitives::errors::EpochError) -> Self {
        Self(QueryError::InternalError(error.to_string()))
    }
}

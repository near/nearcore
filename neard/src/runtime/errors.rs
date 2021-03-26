use near_chain::near_chain_primitives::error::QueryError;

#[easy_ext::ext(FromStateViewerErrors)]
impl QueryError {
    pub fn from_call_function_error(
        error: node_runtime::state_viewer::errors::CallFunctionError,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    ) -> Self {
        match error {
            node_runtime::state_viewer::errors::CallFunctionError::InvalidAccountId {
                requested_account_id,
            } => Self::InvalidAccount { requested_account_id, block_height, block_hash },
            node_runtime::state_viewer::errors::CallFunctionError::AccountDoesNotExist {
                requested_account_id,
            } => Self::UnknownAccount { requested_account_id, block_height, block_hash },
            node_runtime::state_viewer::errors::CallFunctionError::InternalError {
                error_message,
            } => Self::InternalError { error_message, block_height, block_hash },
            node_runtime::state_viewer::errors::CallFunctionError::VMError { error_message } => {
                Self::ContractExecutionError { error_message, block_height, block_hash }
            }
        }
    }

    pub fn from_view_account_error(
        error: node_runtime::state_viewer::errors::ViewAccountError,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    ) -> Self {
        match error {
            node_runtime::state_viewer::errors::ViewAccountError::InvalidAccountId {
                requested_account_id,
            } => Self::InvalidAccount { requested_account_id, block_height, block_hash },
            node_runtime::state_viewer::errors::ViewAccountError::AccountDoesNotExist {
                requested_account_id,
            } => Self::UnknownAccount { requested_account_id, block_height, block_hash },
            node_runtime::state_viewer::errors::ViewAccountError::InternalError {
                error_message,
            } => Self::InternalError { error_message, block_height, block_hash },
        }
    }

    pub fn from_view_contract_code_error(
        error: node_runtime::state_viewer::errors::ViewContractCodeError,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    ) -> Self {
        match error {
            node_runtime::state_viewer::errors::ViewContractCodeError::InvalidAccountId {
                requested_account_id,
            } => Self::InvalidAccount { requested_account_id, block_height, block_hash },
            node_runtime::state_viewer::errors::ViewContractCodeError::AccountDoesNotExist {
                requested_account_id,
            } => Self::UnknownAccount { requested_account_id, block_height, block_hash },
            node_runtime::state_viewer::errors::ViewContractCodeError::InternalError {
                error_message,
            } => Self::InternalError { error_message, block_height, block_hash },
            node_runtime::state_viewer::errors::ViewContractCodeError::NoContractCode {
                contract_account_id,
            } => Self::NoContractCode { contract_account_id, block_height, block_hash },
        }
    }

    pub fn from_view_state_error(
        error: node_runtime::state_viewer::errors::ViewStateError,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    ) -> Self {
        match error {
            node_runtime::state_viewer::errors::ViewStateError::InvalidAccountId {
                requested_account_id,
            } => Self::InvalidAccount { requested_account_id, block_height, block_hash },
            node_runtime::state_viewer::errors::ViewStateError::InternalError { error_message } => {
                Self::InternalError { error_message, block_height, block_hash }
            }
            node_runtime::state_viewer::errors::ViewStateError::AccountDoesNotExist {
                requested_account_id,
            } => Self::UnknownAccount { requested_account_id, block_height, block_hash },
            node_runtime::state_viewer::errors::ViewStateError::AccountStateTooLarge {
                requested_account_id,
            } => Self::TooLargeContractState { requested_account_id, block_height, block_hash },
        }
    }

    pub fn from_view_access_key_error(
        error: node_runtime::state_viewer::errors::ViewAccessKeyError,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    ) -> Self {
        match error {
            node_runtime::state_viewer::errors::ViewAccessKeyError::InvalidAccountId {
                requested_account_id,
            } => Self::InvalidAccount { requested_account_id, block_height, block_hash },
            node_runtime::state_viewer::errors::ViewAccessKeyError::AccessKeyDoesNotExist {
                public_key,
            } => Self::UnknownAccessKey { public_key, block_height, block_hash },
            node_runtime::state_viewer::errors::ViewAccessKeyError::InternalError {
                error_message,
            } => Self::InternalError { error_message, block_height, block_hash },
        }
    }

    pub fn from_epoch_error(
        error: near_primitives::errors::EpochError,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    ) -> Self {
        Self::InternalError { error_message: error.to_string(), block_height, block_hash }
    }
}

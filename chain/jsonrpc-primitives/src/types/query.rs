use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Max size of the query path (soft-deprecated)
const QUERY_DATA_MAX_SIZE: usize = 10 * 1024;

#[derive(Serialize, Deserialize)]
pub struct RpcQueryRequest {
    #[serde(flatten)]
    pub block_reference: near_primitives::types::BlockReference,
    #[serde(flatten)]
    pub request: near_primitives::views::QueryRequest,
}

#[derive(thiserror::Error, Debug)]
pub enum RpcQueryError {
    #[error("There are no fully synchronized blocks on the node yet")]
    NoSyncedBlocks,
    #[error("The node does not track the shard ID {requested_shard_id}")]
    UnavailableShard { requested_shard_id: near_primitives::types::ShardId },
    #[error("Block either has never been observed on the node or has been garbage collected: {block_reference:?}")]
    UnknownBlock { block_reference: near_primitives::types::BlockReference },
    #[error("Account ID {requested_account_id} is invalid")]
    InvalidAccount {
        requested_account_id: near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error("account {requested_account_id} does not exist while viewing")]
    UnknownAccount {
        requested_account_id: near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error(
        "Contract code for contract ID #{contract_account_id} has never been observed on the node"
    )]
    NoContractCode {
        contract_account_id: near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error("State of contract {contract_account_id} is too large to be viewed")]
    TooLargeContractState {
        contract_account_id: near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error("Access key for public key {public_key} has never been observed on the node")]
    UnknownAccessKey {
        public_key: near_crypto::PublicKey,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error("Function call returned an error: {vm_error}")]
    ContractExecutionError {
        vm_error: String,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}")]
    Unreachable { error_message: String },
}

#[derive(Serialize, Deserialize)]
pub struct RpcQueryResponse {
    #[serde(flatten)]
    pub kind: QueryResponseKind,
    pub block_height: near_primitives::types::BlockHeight,
    pub block_hash: near_primitives::hash::CryptoHash,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum QueryResponseKind {
    ViewAccount(near_primitives::views::AccountView),
    ViewCode(near_primitives::views::ContractCodeView),
    ViewState(near_primitives::views::ViewStateResult),
    CallResult(near_primitives::views::CallResult),
    AccessKey(near_primitives::views::AccessKeyView),
    AccessKeyList(near_primitives::views::AccessKeyList),
}

impl RpcQueryRequest {
    pub fn parse(value: Option<Value>) -> Result<RpcQueryRequest, crate::errors::RpcParseError> {
        let query_request = if let Ok((path, data)) =
            crate::utils::parse_params::<(String, String)>(value.clone())
        {
            // Handle a soft-deprecated version of the query API, which is based on
            // positional arguments with a "path"-style first argument.
            //
            // This whole block can be removed one day, when the new API is 100% adopted.
            let data = near_primitives_core::serialize::from_base(&data)
                .map_err(|err| crate::errors::RpcParseError(err.to_string()))?;
            let query_data_size = path.len() + data.len();
            if query_data_size > QUERY_DATA_MAX_SIZE {
                return Err(crate::errors::RpcParseError(format!(
                    "Query data size {} is too large",
                    query_data_size
                )));
            }

            let mut path_parts = path.splitn(3, '/');
            let make_err =
                || crate::errors::RpcParseError("Not enough query parameters provided".to_string());
            let query_command = path_parts.next().ok_or_else(make_err)?;
            let account_id =
                near_primitives::types::AccountId::from(path_parts.next().ok_or_else(make_err)?);
            let maybe_extra_arg = path_parts.next();

            let request = match query_command {
                "account" => near_primitives::views::QueryRequest::ViewAccount { account_id },
                "access_key" => match maybe_extra_arg {
                    None => near_primitives::views::QueryRequest::ViewAccessKeyList { account_id },
                    Some(pk) => near_primitives::views::QueryRequest::ViewAccessKey {
                        account_id,
                        public_key: pk.parse().map_err(|_| {
                            crate::errors::RpcParseError("Invalid public key".to_string())
                        })?,
                    },
                },
                "code" => near_primitives::views::QueryRequest::ViewCode { account_id },
                "contract" => near_primitives::views::QueryRequest::ViewState {
                    account_id,
                    prefix: data.into(),
                },
                "call" => match maybe_extra_arg {
                    Some(method_name) => near_primitives::views::QueryRequest::CallFunction {
                        account_id,
                        method_name: method_name.to_string(),
                        args: data.into(),
                    },
                    None => {
                        return Err(crate::errors::RpcParseError(
                            "Method name is missing".to_string(),
                        ))
                    }
                },
                _ => {
                    return Err(crate::errors::RpcParseError(format!(
                        "Unknown path {}",
                        query_command
                    )))
                }
            };
            // Use Finality::None here to make backward compatibility tests work
            RpcQueryRequest {
                request,
                block_reference: near_primitives::types::BlockReference::latest(),
            }
        } else {
            crate::utils::parse_params::<RpcQueryRequest>(value)?
        };
        Ok(query_request)
    }
}

impl From<near_client_primitives::types::QueryError> for RpcQueryError {
    fn from(error: near_client_primitives::types::QueryError) -> Self {
        match error {
            near_client_primitives::types::QueryError::InternalError { error_message } => {
                Self::InternalError { error_message }
            }
            near_client_primitives::types::QueryError::NoSyncedBlocks => Self::NoSyncedBlocks,
            near_client_primitives::types::QueryError::UnavailableShard { requested_shard_id } => {
                Self::UnavailableShard { requested_shard_id }
            }
            near_client_primitives::types::QueryError::UnknownBlock { block_reference } => {
                Self::UnknownBlock { block_reference }
            }
            near_client_primitives::types::QueryError::InvalidAccount {
                requested_account_id,
                block_height,
                block_hash,
            } => Self::InvalidAccount { requested_account_id, block_height, block_hash },
            near_client_primitives::types::QueryError::UnknownAccount {
                requested_account_id,
                block_height,
                block_hash,
            } => Self::UnknownAccount { requested_account_id, block_height, block_hash },
            near_client_primitives::types::QueryError::NoContractCode {
                contract_account_id,
                block_height,
                block_hash,
            } => Self::NoContractCode { contract_account_id, block_height, block_hash },
            near_client_primitives::types::QueryError::UnknownAccessKey {
                public_key,
                block_height,
                block_hash,
            } => Self::UnknownAccessKey { public_key, block_height, block_hash },
            near_client_primitives::types::QueryError::ContractExecutionError {
                vm_error,
                block_height,
                block_hash,
            } => Self::ContractExecutionError { vm_error, block_height, block_hash },
            near_client_primitives::types::QueryError::Unreachable { error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                near_metrics::inc_counter_vec(
                    &crate::metrics::RPC_UNREACHABLE_ERROR_COUNT,
                    &["RpcQueryError"],
                );
                Self::Unreachable { error_message }
            }
            near_client_primitives::types::QueryError::TooLargeContractState {
                contract_account_id,
                block_height,
                block_hash,
            } => Self::TooLargeContractState { contract_account_id, block_height, block_hash },
        }
    }
}

impl From<near_primitives::views::QueryResponse> for RpcQueryResponse {
    fn from(query_response: near_primitives::views::QueryResponse) -> Self {
        Self {
            kind: query_response.kind.into(),
            block_hash: query_response.block_hash,
            block_height: query_response.block_height,
        }
    }
}

impl From<near_primitives::views::QueryResponseKind> for QueryResponseKind {
    fn from(query_response_kind: near_primitives::views::QueryResponseKind) -> Self {
        match query_response_kind {
            near_primitives::views::QueryResponseKind::ViewAccount(account_view) => {
                Self::ViewAccount(account_view)
            }
            near_primitives::views::QueryResponseKind::ViewCode(contract_code_view) => {
                Self::ViewCode(contract_code_view)
            }
            near_primitives::views::QueryResponseKind::ViewState(view_state_result) => {
                Self::ViewState(view_state_result)
            }
            near_primitives::views::QueryResponseKind::CallResult(call_result) => {
                Self::CallResult(call_result)
            }
            near_primitives::views::QueryResponseKind::AccessKey(access_key_view) => {
                Self::AccessKey(access_key_view)
            }
            near_primitives::views::QueryResponseKind::AccessKeyList(access_key_list) => {
                Self::AccessKeyList(access_key_list)
            }
        }
    }
}

impl From<RpcQueryError> for crate::errors::RpcError {
    fn from(error: RpcQueryError) -> Self {
        let error_data = Some(Value::String(error.to_string()));

        Self::new(-32_000, "Server error".to_string(), error_data)
    }
}

impl From<actix::MailboxError> for RpcQueryError {
    fn from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

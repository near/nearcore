use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize)]
pub struct RpcQueryRequest {
    #[serde(flatten)]
    pub block_reference: near_primitives::types::BlockReference,
    #[serde(flatten)]
    pub request: near_primitives::views::QueryRequest,
}

#[derive(thiserror::Error, Debug)]
pub enum RpcQueryError {
    #[error("IO Error: {0}")]
    IOError(String),
    #[error("There are no fully synchronized blocks yet")]
    NotSyncedYet,
    #[error("The node does not track the shard")]
    DoesNotTrackShard,
    #[error("Invalid account ID {0}")]
    InvalidAccount(near_primitives::types::AccountId),
    #[error("Account ID {0} has never been observed on the node")]
    AccountDoesNotExist(near_primitives::types::AccountId),
    #[error("Contract code for contract ID {0} has never been observed on the node")]
    ContractCodeDoesNotExist(near_primitives::types::AccountId),
    #[error("Access key for public key {0} has never been observed on the node")]
    AccessKeyDoesNotExist(String),
    #[error("VM error occurred: {0}")]
    VMError(String),
    #[error("The node reached its limits. Try again later. More details: {0}")]
    InternalError(String),
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {0}")]
    Unreachable(String),
}

#[derive(Serialize, Deserialize)]
pub struct RpcQueryResponse {
    #[serde(flatten)]
    pub query_response: near_primitives::views::QueryResponse,
}

impl RpcQueryRequest {
    pub fn parse(value: Option<Value>) -> Result<RpcQueryRequest, crate::errors::RpcParseError> {
        let query_request = if let Ok((path, data)) =
            crate::utils::parse_params::<(String, String)>(value.clone())
        {
            let data = near_primitives_core::serialize::from_base(&data)
                .map_err(|err| crate::errors::RpcParseError(err.to_string()))?;
            let query_data_size = path.len() + data.len();

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
            near_client_primitives::types::QueryError::IOError(s) => Self::InternalError(s),
            near_client_primitives::types::QueryError::NotSyncedYet => Self::NotSyncedYet,
            near_client_primitives::types::QueryError::DoesNotTrackShard => Self::DoesNotTrackShard,
            near_client_primitives::types::QueryError::InvalidAccount(account_id) => {
                Self::InvalidAccount(account_id)
            }
            near_client_primitives::types::QueryError::AccountDoesNotExist(account_id) => {
                Self::AccountDoesNotExist(account_id)
            }
            near_client_primitives::types::QueryError::ContractCodeDoesNotExist(account_id) => {
                Self::ContractCodeDoesNotExist(account_id)
            }
            near_client_primitives::types::QueryError::AccessKeyDoesNotExist(public_key) => {
                Self::AccessKeyDoesNotExist(public_key)
            }
            near_client_primitives::types::QueryError::VMError(s) => Self::VMError(s),
            near_client_primitives::types::QueryError::Unreachable(s) => {
                near_metrics::inc_counter_vec(
                    &crate::metrics::RPC_UNREACHABLE_ERROR_COUNT,
                    &["RpcQueryError", &s],
                );
                Self::Unreachable(s)
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
        Self::InternalError(error.to_string())
    }
}

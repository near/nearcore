use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcStateChangesRequest {
    #[serde(flatten)]
    pub block_reference: crate::types::blocks::BlockReference,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcStateChangesResponse {
    pub block_hash: near_primitives::hash::CryptoHash,
    pub changes: near_primitives::views::StateChangesView,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcStateChangesInBlockRequest {
    #[serde(flatten)]
    pub block_reference: crate::types::blocks::BlockReference,
    #[serde(flatten)]
    pub state_changes_request: near_primitives::views::StateChangesRequestView,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcStateChangesInBlockResponse {
    pub block_hash: near_primitives::hash::CryptoHash,
    pub changes: near_primitives::views::StateChangesKindsView,
}

#[derive(thiserror::Error, Debug, Serialize, Deserialize)]
pub enum RpcStateChangesError {
    #[error("Block not found: {error_message}")]
    UnknownBlock { error_message: String },
    #[error("There are no fully synchronized blocks yet")]
    NotSyncedYet,
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}")]
    Unreachable { error_message: String },
}

impl RpcStateChangesRequest {
    pub fn parse(value: Option<Value>) -> Result<Self, crate::errors::RpcParseError> {
        Ok(crate::utils::parse_params::<Self>(value)?)
    }
}

impl RpcStateChangesInBlockRequest {
    pub fn parse(value: Option<Value>) -> Result<Self, crate::errors::RpcParseError> {
        Ok(crate::utils::parse_params::<Self>(value)?)
    }
}

impl From<near_client_primitives::types::GetBlockError> for RpcStateChangesError {
    fn from(error: near_client_primitives::types::GetBlockError) -> Self {
        match error {
            near_client_primitives::types::GetBlockError::UnknownBlock { error_message } => {
                Self::UnknownBlock { error_message }
            }
            near_client_primitives::types::GetBlockError::NotSyncedYet => Self::NotSyncedYet,
            near_client_primitives::types::GetBlockError::IOError { error_message } => {
                Self::InternalError { error_message }
            }
            near_client_primitives::types::GetBlockError::Unreachable { error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                near_metrics::inc_counter_vec(
                    &crate::metrics::RPC_UNREACHABLE_ERROR_COUNT,
                    &["RpcStateChangesError"],
                );
                Self::Unreachable { error_message }
            }
        }
    }
}

impl From<near_client_primitives::types::GetStateChangesError> for RpcStateChangesError {
    fn from(error: near_client_primitives::types::GetStateChangesError) -> Self {
        match error {
            near_client_primitives::types::GetStateChangesError::IOError { error_message } => {
                Self::InternalError { error_message }
            }
            near_client_primitives::types::GetStateChangesError::UnknownBlock { error_message } => {
                Self::UnknownBlock { error_message }
            }
            near_client_primitives::types::GetStateChangesError::NotSyncedYet => Self::NotSyncedYet,
            near_client_primitives::types::GetStateChangesError::Unreachable { error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                near_metrics::inc_counter_vec(
                    &crate::metrics::RPC_UNREACHABLE_ERROR_COUNT,
                    &["RpcStateChangesError"],
                );
                Self::Unreachable { error_message }
            }
        }
    }
}

impl From<RpcStateChangesError> for crate::errors::RpcError {
    fn from(error: RpcStateChangesError) -> Self {
        let error_data = Some(Value::String(error.to_string()));

        Self::new(-32_000, "Server error".to_string(), error_data)
    }
}

impl From<actix::MailboxError> for RpcStateChangesError {
    fn from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

use crate::types::blocks::BlockReference;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize)]
pub struct RpcProtocolConfigRequest {
    #[serde(flatten)]
    pub block_reference: BlockReference,
}

impl RpcProtocolConfigRequest {
    pub fn parse(
        value: Option<Value>,
    ) -> Result<RpcProtocolConfigRequest, crate::errors::RpcParseError> {
        crate::utils::parse_params::<BlockReference>(value)
            .map(|block_reference| RpcProtocolConfigRequest { block_reference })
    }
}

#[derive(Serialize, Deserialize)]
pub struct RpcProtocolConfigResponse {
    #[serde(flatten)]
    pub config_view: near_chain_configs::ProtocolConfigView,
}

#[derive(thiserror::Error, Debug)]
pub enum RpcProtocolConfigError {
    #[error("Block has never been observed: {0}")]
    UnknownBlock(String),
    #[error("The node reached its limits. Try again later. More details: {0}")]
    InternalError(String),
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {0}")]
    Unreachable(String),
}

impl From<near_client_primitives::types::GetProtocolConfigError> for RpcProtocolConfigError {
    fn from(error: near_client_primitives::types::GetProtocolConfigError) -> Self {
        match error {
            near_client_primitives::types::GetProtocolConfigError::UnknownBlock(s) => {
                Self::UnknownBlock(s)
            }
            near_client_primitives::types::GetProtocolConfigError::IOError(s) => {
                Self::InternalError(s)
            }
            near_client_primitives::types::GetProtocolConfigError::Unreachable(error_message) => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                near_metrics::inc_counter_vec(
                    &crate::metrics::RPC_UNREACHABLE_ERROR_COUNT,
                    &["RpcProtocolConfigError", &error_message],
                );
                Self::Unreachable(error_message)
            }
        }
    }
}

impl From<actix::MailboxError> for RpcProtocolConfigError {
    fn from(error: actix::MailboxError) -> Self {
        Self::InternalError(error.to_string())
    }
}

impl From<RpcProtocolConfigError> for crate::errors::RpcError {
    fn from(error: RpcProtocolConfigError) -> Self {
        let error_data = match error {
            RpcProtocolConfigError::UnknownBlock(hash) => {
                Some(Value::String(format!("Block Not Found: {}", hash)))
            }
            RpcProtocolConfigError::Unreachable(s) => Some(Value::String(s)),
            RpcProtocolConfigError::InternalError(_) => Some(Value::String(error.to_string())),
        };

        Self::new(-32_000, "Server error".to_string(), error_data)
    }
}

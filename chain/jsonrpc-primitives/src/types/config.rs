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

#[derive(thiserror::Error, Debug, Serialize)]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcProtocolConfigError {
    #[error("Block has never been observed: {error_message}")]
    UnknownBlock {
        #[serde(skip_serializing)]
        error_message: String,
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

impl From<near_client_primitives::types::GetProtocolConfigError> for RpcProtocolConfigError {
    fn from(error: near_client_primitives::types::GetProtocolConfigError) -> Self {
        match error {
            near_client_primitives::types::GetProtocolConfigError::UnknownBlock(error_message) => {
                Self::UnknownBlock { error_message }
            }
            near_client_primitives::types::GetProtocolConfigError::IOError(error_message) => {
                Self::InternalError { error_message }
            }
            near_client_primitives::types::GetProtocolConfigError::Unreachable(error_message) => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                near_metrics::inc_counter_vec(
                    &crate::metrics::RPC_UNREACHABLE_ERROR_COUNT,
                    &["RpcProtocolConfigError"],
                );
                Self::Unreachable { error_message }
            }
        }
    }
}

impl From<actix::MailboxError> for RpcProtocolConfigError {
    fn from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl From<RpcProtocolConfigError> for crate::errors::RpcError {
    fn from(error: RpcProtocolConfigError) -> Self {
        let error_data = match &error {
            RpcProtocolConfigError::UnknownBlock { error_message } => {
                Some(Value::String(format!("Block Not Found: {}", error_message)))
            }
            RpcProtocolConfigError::Unreachable { error_message } => {
                Some(Value::String(error_message.clone()))
            }
            RpcProtocolConfigError::InternalError { .. } => Some(Value::String(error.to_string())),
        };

        Self::new_handler_error(
            error_data,
            serde_json::to_value(error)
                .expect("Not expected serialization error while serializing struct"),
        )
    }
}

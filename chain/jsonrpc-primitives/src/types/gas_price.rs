use near_client_primitives::types::GetGasPriceError;
use near_primitives::types::MaybeBlockId;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize)]
pub struct RpcGasPriceRequest {
    #[serde(flatten)]
    pub block_id: MaybeBlockId,
}

#[derive(Serialize, Deserialize)]
pub struct RpcGasPriceResponse {
    #[serde(flatten)]
    pub gas_price_view: near_primitives::views::GasPriceView,
}

#[derive(thiserror::Error, Debug)]
pub enum RpcGasPriceError {
    #[error("Internal error: {error_message}")]
    InternalError { error_message: String },
    #[error("Block either has never been observed on the node or has been garbage collected: {error_message}")]
    UnknownBlock { error_message: String },
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}")]
    Unreachable { error_message: String },
}

impl From<near_client_primitives::types::GetGasPriceError> for RpcGasPriceError {
    fn from(error: near_client_primitives::types::GetGasPriceError) -> Self {
        match error {
            GetGasPriceError::UnknownBlock { error_message } => {
                Self::UnknownBlock { error_message }
            }
            GetGasPriceError::InternalError { error_message } => {
                Self::InternalError { error_message }
            }
            GetGasPriceError::Unreachable { error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                near_metrics::inc_counter_vec(
                    &crate::metrics::RPC_UNREACHABLE_ERROR_COUNT,
                    &["RpcGasPriceError"],
                );
                Self::Unreachable { error_message }
            }
        }
    }
}

impl From<actix::MailboxError> for RpcGasPriceError {
    fn from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl From<RpcGasPriceError> for crate::errors::RpcError {
    fn from(error: RpcGasPriceError) -> Self {
        let error_data = match error {
            RpcGasPriceError::UnknownBlock { error_message } => Some(Value::String(format!(
                "DB Not Found Error: {} \n Cause: Unknown",
                error_message
            ))),
            RpcGasPriceError::InternalError { .. } => Some(Value::String(error.to_string())),
            RpcGasPriceError::Unreachable { error_message } => Some(Value::String(error_message)),
        };

        Self::new(-32_000, "Server error".to_string(), error_data)
    }
}

impl RpcGasPriceRequest {
    pub fn parse(value: Option<Value>) -> Result<RpcGasPriceRequest, crate::errors::RpcParseError> {
        crate::utils::parse_params::<(MaybeBlockId,)>(value)
            .map(|(block_id,)| RpcGasPriceRequest { block_id })
    }
}

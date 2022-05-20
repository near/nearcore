use near_client_primitives::types::GetGasPriceError;
use near_primitives::types::MaybeBlockId;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcGasPriceRequest {
    pub block_id: MaybeBlockId,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcGasPriceResponse {
    #[serde(flatten)]
    pub gas_price_view: near_primitives::views::GasPriceView,
}

#[derive(thiserror::Error, Debug, Serialize, Deserialize)]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcGasPriceError {
    #[error("Internal error: {error_message}")]
    InternalError { error_message: String },
    #[error("Block either has never been observed on the node or has been garbage collected: {error_message}")]
    UnknownBlock {
        #[serde(skip_serializing)]
        error_message: String,
    },
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
            GetGasPriceError::Unreachable { ref error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcGasPriceError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
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
        let error_data = match &error {
            RpcGasPriceError::UnknownBlock { error_message } => Some(Value::String(format!(
                "DB Not Found Error: {} \n Cause: Unknown",
                error_message
            ))),
            RpcGasPriceError::InternalError { .. } => Some(Value::String(error.to_string())),
        };

        let error_data_value = match serde_json::to_value(error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcGasPriceError: {:?}", err),
                )
            }
        };

        Self::new_internal_or_handler_error(error_data, error_data_value)
    }
}

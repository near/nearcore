use near_primitives::types::MaybeBlockId;
use serde_json::Value;

#[derive(serde::Serialize, serde::Deserialize, Debug, arbitrary::Arbitrary)]
pub struct RpcGasPriceRequest {
    pub block_id: MaybeBlockId,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct RpcGasPriceResponse {
    #[serde(flatten)]
    pub gas_price_view: near_primitives::views::GasPriceView,
}

#[derive(thiserror::Error, Debug, serde::Serialize, serde::Deserialize)]
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

use serde_json::Value;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct RpcProtocolConfigRequest {
    #[serde(flatten)]
    pub block_reference: near_primitives::types::BlockReference,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct RpcProtocolConfigResponse {
    #[serde(flatten)]
    pub config_view: near_chain_configs::ProtocolConfigView,
}

#[derive(thiserror::Error, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcProtocolConfigError {
    #[error("Block has never been observed: {error_message}")]
    UnknownBlock {
        #[serde(skip_serializing)]
        error_message: String,
    },
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
}

impl From<RpcProtocolConfigError> for crate::errors::RpcError {
    fn from(error: RpcProtocolConfigError) -> Self {
        let error_data = match &error {
            RpcProtocolConfigError::UnknownBlock { error_message } => {
                Some(Value::String(format!("Block Not Found: {}", error_message)))
            }
            RpcProtocolConfigError::InternalError { .. } => Some(Value::String(error.to_string())),
        };

        let error_data_value = match serde_json::to_value(error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcProtocolConfigError: {:?}", err),
                )
            }
        };

        Self::new_internal_or_handler_error(error_data, error_data_value)
    }
}

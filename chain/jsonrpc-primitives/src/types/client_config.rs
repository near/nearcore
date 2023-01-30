use serde::Serialize;
use serde_json::Value;

#[derive(Serialize)]
pub struct RpcClientConfigResponse {
    #[serde(flatten)]
    pub client_config: near_chain_configs::ClientConfig,
}

#[derive(thiserror::Error, Debug, Serialize)]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcClientConfigError {
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
}

impl From<RpcClientConfigError> for crate::errors::RpcError {
    fn from(error: RpcClientConfigError) -> Self {
        let error_data = match &error {
            RpcClientConfigError::InternalError { .. } => Some(Value::String(error.to_string())),
        };

        let error_data_value = match serde_json::to_value(error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcClientConfigError: {:?}", err),
                )
            }
        };

        Self::new_internal_or_handler_error(error_data, error_data_value)
    }
}

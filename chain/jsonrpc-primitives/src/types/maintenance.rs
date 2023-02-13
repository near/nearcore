use serde_json::Value;

pub type RpcMaintenanceWindowsResponse =
    Vec<(near_primitives::types::BlockHeight, near_primitives::types::BlockHeight)>;

#[derive(thiserror::Error, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcMaintenanceWindowsError {
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct RpcMaintenanceWindowsRequest {
    pub account_id: near_primitives::types::AccountId,
}

impl From<RpcMaintenanceWindowsError> for crate::errors::RpcError {
    fn from(error: RpcMaintenanceWindowsError) -> Self {
        let error_data = match &error {
            RpcMaintenanceWindowsError::InternalError { .. } => {
                Some(Value::String(error.to_string()))
            }
        };

        let error_data_value = match serde_json::to_value(error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcMaintenanceError: {:?}", err),
                )
            }
        };

        Self::new_internal_or_handler_error(error_data, error_data_value)
    }
}

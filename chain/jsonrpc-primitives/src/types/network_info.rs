use serde::Serialize;
use serde_json::Value;

#[derive(Serialize, Debug)]
pub struct RpcNetworkInfoResponse {
    #[serde(flatten)]
    pub network_info_response: near_client_primitives::types::NetworkInfoResponse,
}

#[derive(thiserror::Error, Debug, Serialize)]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcNetworkInfoError {
    #[error("Internal error: {error_message}")]
    InternalError { error_message: String },
}

impl From<near_client_primitives::types::NetworkInfoResponse> for RpcNetworkInfoResponse {
    fn from(network_info_response: near_client_primitives::types::NetworkInfoResponse) -> Self {
        Self { network_info_response }
    }
}

impl From<actix::MailboxError> for RpcNetworkInfoError {
    fn from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl From<String> for RpcNetworkInfoError {
    fn from(error_message: String) -> Self {
        Self::InternalError { error_message }
    }
}

impl From<RpcNetworkInfoError> for crate::errors::RpcError {
    fn from(error: RpcNetworkInfoError) -> Self {
        Self::new_handler_error(
            Some(Value::String(error.to_string())),
            serde_json::to_value(error)
                .expect("Not expected serialization error while serializing struct"),
        )
    }
}

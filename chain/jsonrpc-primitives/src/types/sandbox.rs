use near_primitives::state_record::StateRecord;
use near_primitives::types::BlockHeightDelta;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize, Serialize, Debug)]
pub struct RpcSandboxPatchStateRequest {
    pub records: Vec<StateRecord>,
}

#[cfg(feature = "server")]
impl crate::RpcRequest for RpcSandboxPatchStateRequest {
    fn parse(value: Option<Value>) -> Result<Self, crate::errors::RpcParseError> {
        crate::utils::parse_params::<Self>(value)
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct RpcSandboxPatchStateResponse {}

#[derive(thiserror::Error, Debug, Serialize, Deserialize)]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcSandboxPatchStateError {
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
}

impl From<actix::MailboxError> for RpcSandboxPatchStateError {
    fn from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl From<RpcSandboxPatchStateError> for crate::errors::RpcError {
    fn from(error: RpcSandboxPatchStateError) -> Self {
        let error_data = match serde_json::to_value(error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcSandboxPatchStateError: {:?}", err),
                )
            }
        };
        Self::new_internal_or_handler_error(Some(error_data.clone()), error_data)
    }
}

#[derive(Deserialize, Serialize)]
pub struct RpcSandboxFastForwardRequest {
    pub delta_height: BlockHeightDelta,
}

#[cfg(feature = "server")]
impl crate::RpcRequest for RpcSandboxFastForwardRequest {
    fn parse(value: Option<Value>) -> Result<Self, crate::errors::RpcParseError> {
        crate::utils::parse_params::<Self>(value)
    }
}

#[derive(Deserialize, Serialize)]
pub struct RpcSandboxFastForwardResponse {}

#[derive(thiserror::Error, Debug, Serialize, Deserialize)]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcSandboxFastForwardError {
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
}

impl From<actix::MailboxError> for RpcSandboxFastForwardError {
    fn from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl From<RpcSandboxFastForwardError> for crate::errors::RpcError {
    fn from(error: RpcSandboxFastForwardError) -> Self {
        let error_data = match serde_json::to_value(error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcSandboxFastForwardError: {:?}", err),
                )
            }
        };
        Self::new_internal_or_handler_error(Some(error_data.clone()), error_data)
    }
}

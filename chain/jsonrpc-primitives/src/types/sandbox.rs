use near_primitives::state_record::StateRecord;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize, Serialize)]
pub struct RpcSandboxPatchStateRequest {
    pub records: Vec<StateRecord>,
}

impl RpcSandboxPatchStateRequest {
    pub fn parse(value: Option<Value>) -> Result<Self, crate::errors::RpcParseError> {
        Ok(crate::utils::parse_params::<RpcSandboxPatchStateRequest>(value)?)
    }
}

#[derive(Deserialize, Serialize)]
pub struct RpcSandboxPatchStateResponse;

#[derive(thiserror::Error, Debug, Serialize)]
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
        Self::new_handler_error(
            Some(Value::String(error.to_string())),
            serde_json::to_value(error)
                .expect("Not expected serialization error while serializing struct"),
        )
    }
}

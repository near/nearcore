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
pub struct RpcSandboxPatchStateResponse {}

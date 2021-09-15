use serde_json::Value;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct AcceptConnectionFromRequest {
    pub address: String,
}

impl AcceptConnectionFromRequest {
    pub fn parse(value: Option<Value>) -> Result<Self, crate::errors::RpcParseError> {
        crate::utils::parse_params::<AcceptConnectionFromRequest>(value)
    }
}
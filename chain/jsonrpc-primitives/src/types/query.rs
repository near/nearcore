use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryReference {}

#[derive(Serialize, Deserialize)]
pub struct RpcQueryRequest {
    #[serde(flatten)]
    pub query_reference: QueryReference,
}

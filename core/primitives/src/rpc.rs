use serde::{Deserialize, Serialize};

use super::types::MaybeBlockId;
use super::views::QueryRequest;

#[derive(Serialize, Deserialize)]
pub struct RpcQueryRequest {
    pub block_id: MaybeBlockId,
    #[serde(flatten)]
    pub request: QueryRequest,
}

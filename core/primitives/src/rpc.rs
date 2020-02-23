use serde::{Deserialize, Serialize};

use super::types::MaybeBlockId;
use super::views::{Finality, QueryRequest};
use crate::types::BlockId;

#[derive(Serialize, Deserialize)]
pub struct RpcQueryRequest {
    pub block_id: MaybeBlockId,
    #[serde(flatten)]
    pub request: QueryRequest,
    pub finality: Finality,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockQueryInfo {
    BlockId(BlockId),
    Finality(Finality),
}

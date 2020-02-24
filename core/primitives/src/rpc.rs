use serde::{Deserialize, Serialize};

use crate::types::BlockId;
use crate::types::MaybeBlockId;
use crate::views::{Finality, QueryRequest};

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

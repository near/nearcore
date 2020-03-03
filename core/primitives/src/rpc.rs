use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;
use validator::Validate;
use validator_derive::Validate;

use crate::types::{BlockId, MaybeBlockId};
use crate::views::{Finality, QueryRequest};

#[derive(Debug, SmartDefault, Serialize, Deserialize, Validate)]
#[serde(default)]
pub struct RpcPagination {
    pub offset: usize,
    #[default(100)]
    #[validate(range(min = 1, max = 100))]
    pub limit: usize,
}

#[derive(Serialize, Deserialize, Validate)]
pub struct RpcGenesisRecordsRequest {
    #[serde(default)]
    #[validate]
    pub pagination: RpcPagination,
}

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

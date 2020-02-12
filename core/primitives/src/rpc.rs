use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use super::types::MaybeBlockId;
use super::views::QueryRequest;

#[derive(Serialize, Deserialize)]
pub struct RpcQueryRequest {
    pub block_id: MaybeBlockId,
    #[serde(flatten)]
    pub request: QueryRequest,
    pub finality: Finality,
}

/// Different types of finality.
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq)]
pub enum Finality {
    None,
    DoomSlug,
    NFG,
}

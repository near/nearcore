//! This module defines RPC types to nearcore public APIs.
//!
//! NOTE: This module should be only used in RPC server and RPC client implementations, and
//! should not leak these types anywhere else.
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;
use validator::Validate;
use validator_derive::Validate;

use crate::hash::CryptoHash;
use crate::types::BlockIdOrFinality;
use crate::views::{
    QueryRequest, StateChangeWithCauseView, StateChangesKindsView, StateChangesRequestView,
};

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
    #[serde(flatten)]
    pub block_id_or_finality: BlockIdOrFinality,
    #[serde(flatten)]
    pub request: QueryRequest,
}

#[derive(Serialize, Deserialize)]
pub struct RpcStateChangesRequest {
    #[serde(flatten)]
    pub block_id_or_finality: BlockIdOrFinality,
    #[serde(flatten)]
    pub state_changes_request: StateChangesRequestView,
}

#[derive(Serialize, Deserialize)]
pub struct RpcStateChangesResponse {
    pub block_hash: CryptoHash,
    pub changes: Vec<StateChangeWithCauseView>,
}

#[derive(Serialize, Deserialize)]
pub struct RpcStateChangesInBlockRequest {
    #[serde(flatten)]
    pub block_id_or_finality: BlockIdOrFinality,
}

#[derive(Serialize, Deserialize)]
pub struct RpcStateChangesInBlockResponse {
    pub block_hash: CryptoHash,
    pub changes: StateChangesKindsView,
}

#[derive(Serialize, Deserialize)]
pub struct RpcBroadcastTxSyncResponse {
    pub transaction_hash: String,
    pub is_routed: bool,
}

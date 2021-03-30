//! This module defines RPC types to nearcore public APIs.
//!
//! NOTE: This module should be only used in RPC server and RPC client implementations, and
//! should not leak these types anywhere else.
use serde::{Deserialize, Serialize};

use near_primitives::hash::CryptoHash;
use near_primitives::merkle::MerklePath;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockReference, MaybeBlockId, TransactionOrReceiptId};
use near_primitives::views::{
    ExecutionOutcomeWithIdView, LightClientBlockLiteView, QueryRequest, StateChangeWithCauseView,
    StateChangesKindsView, StateChangesRequestView,
};

#[derive(Serialize, Deserialize)]
pub struct RpcQueryRequest {
    #[serde(flatten)]
    pub block_reference: BlockReference,
    #[serde(flatten)]
    pub request: QueryRequest,
}

#[derive(Serialize, Deserialize)]
pub struct RpcStateChangesRequest {
    #[serde(flatten)]
    pub block_reference: BlockReference,
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
    pub block_reference: BlockReference,
}

#[derive(Serialize, Deserialize)]
pub struct RpcStateChangesInBlockResponse {
    pub block_hash: CryptoHash,
    pub changes: StateChangesKindsView,
}

#[derive(Serialize, Deserialize)]
pub struct RpcLightClientExecutionProofRequest {
    #[serde(flatten)]
    pub id: TransactionOrReceiptId,
    pub light_client_head: CryptoHash,
}

#[derive(Serialize, Deserialize)]
pub struct RpcLightClientExecutionProofResponse {
    pub outcome_proof: ExecutionOutcomeWithIdView,
    pub outcome_root_proof: MerklePath,
    pub block_header_lite: LightClientBlockLiteView,
    pub block_proof: MerklePath,
}

#[derive(Clone, Debug)]
pub enum TransactionInfo {
    Transaction(SignedTransaction),
    TransactionId { hash: CryptoHash, account_id: AccountId },
}

#[derive(Serialize, Deserialize)]
pub struct RpcValidatorsOrderedRequest {
    pub block_id: MaybeBlockId,
}

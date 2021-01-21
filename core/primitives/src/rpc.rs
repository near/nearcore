//! This module defines RPC types to nearcore public APIs.
//!
//! NOTE: This module should be only used in RPC server and RPC client implementations, and
//! should not leak these types anywhere else.
use serde::{Deserialize, Serialize};

use crate::hash::CryptoHash;
use crate::merkle::MerklePath;
use crate::transaction::SignedTransaction;
use crate::types::{AccountId, BlockReference, MaybeBlockId, TransactionOrReceiptId};
use crate::views::{
    ExecutionOutcomeWithIdView, LightClientBlockLiteView, QueryRequest, StateChangeWithCauseView,
    StateChangesKindsView, StateChangesRequestView,
};
use serde::de::DeserializeOwned;
use serde_json::Value;

#[derive(Serialize)]
pub struct RpcParseError(pub String);

fn parse_params<T: DeserializeOwned>(value: Option<Value>) -> Result<T, RpcParseError> {
    if let Some(value) = value {
        serde_json::from_value(value)
            .map_err(|err| RpcParseError(format!("Failed parsing args: {}", err)))
    } else {
        Err(RpcParseError("Require at least one parameter".to_owned()))
    }
}

#[derive(Serialize, Deserialize)]
pub struct RpcBlockRequest {
    #[serde(flatten)]
    pub block_reference: BlockReference,
}

impl RpcBlockRequest {
    pub fn parse(value: Option<Value>) -> Result<RpcBlockRequest, RpcParseError> {
        let block_reference =
            if let Ok((block_id,)) = parse_params::<(crate::types::BlockId,)>(value.clone()) {
                BlockReference::BlockId(block_id)
            } else {
                parse_params::<BlockReference>(value)?
            };
        Ok(RpcBlockRequest { block_reference })
    }
}

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
pub struct RpcBroadcastTxSyncResponse {
    pub transaction_hash: String,
    pub is_routed: bool,
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

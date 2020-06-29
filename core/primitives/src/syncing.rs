use borsh::{BorshDeserialize, BorshSerialize};
use serde::Serialize;

use crate::hash::CryptoHash;
use crate::merkle::MerklePath;
use crate::receipt::Receipt;
use crate::sharding::{ReceiptProof, ShardChunk, ShardChunkHeader};
use crate::types::{ShardId, StateRootNode};

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, Serialize)]
pub struct ReceiptResponse(pub CryptoHash, pub Vec<Receipt>);

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, Serialize)]
pub struct ReceiptProofResponse(pub CryptoHash, pub Vec<ReceiptProof>);

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, Serialize)]
pub struct RootProof(pub CryptoHash, pub MerklePath);

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, Serialize)]
pub struct StatePartKey(pub CryptoHash, pub ShardId, pub u64 /* PartId */);

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Serialize)]
pub struct ShardStateSyncResponseHeader {
    pub chunk: ShardChunk,
    pub chunk_proof: MerklePath,
    pub prev_chunk_header: Option<ShardChunkHeader>,
    pub prev_chunk_proof: Option<MerklePath>,
    pub incoming_receipts_proofs: Vec<ReceiptProofResponse>,
    pub root_proofs: Vec<Vec<RootProof>>,
    pub state_root_node: StateRootNode,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Serialize)]
pub struct ShardStateSyncResponse {
    pub header: Option<ShardStateSyncResponseHeader>,
    pub part: Option<(u64, Vec<u8>)>,
}

impl ShardStateSyncResponse {
    pub fn part_id(&self) -> Option<u64> {
        self.part.as_ref().map(|(part_id, _)| *part_id)
    }
}

pub fn get_num_state_parts(memory_usage: u64) -> u64 {
    // We assume that 1 Mb is a good limit for state part size.
    // On the other side, it's important to divide any state into
    // several parts to make sure that partitioning always works.
    // TODO #1708
    memory_usage / (1024 * 1024) + 3
}

use borsh::{BorshDeserialize, BorshSerialize};
use serde::Serialize;

use crate::hash::CryptoHash;
use crate::merkle::MerklePath;
use crate::receipt::Receipt;
use crate::sharding::{
    ReceiptProof, ShardChunk, ShardChunkHeader, VersionedShardChunk, VersionedShardChunkHeader,
};
use crate::types::{BlockHeight, ShardId, StateRoot, StateRootNode};

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, Serialize)]
pub struct ReceiptResponse(pub CryptoHash, pub Vec<Receipt>);

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, Serialize)]
pub struct ReceiptProofResponse(pub CryptoHash, pub Vec<ReceiptProof>);

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, Serialize)]
pub struct RootProof(pub CryptoHash, pub MerklePath);

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, Serialize)]
pub struct StateHeaderKey(pub ShardId, pub CryptoHash);

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
pub struct ShardStateSyncResponseHeaderV2 {
    pub chunk: VersionedShardChunk,
    pub chunk_proof: MerklePath,
    pub prev_chunk_header: Option<VersionedShardChunkHeader>,
    pub prev_chunk_proof: Option<MerklePath>,
    pub incoming_receipts_proofs: Vec<ReceiptProofResponse>,
    pub root_proofs: Vec<Vec<RootProof>>,
    pub state_root_node: StateRootNode,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Serialize)]
pub enum VersionedShardStateSyncResponseHeader {
    V1(ShardStateSyncResponseHeader),
    V2(ShardStateSyncResponseHeaderV2),
}

impl VersionedShardStateSyncResponseHeader {
    pub fn versioned_chunk(self) -> VersionedShardChunk {
        match self {
            Self::V1(header) => VersionedShardChunk::V1(header.chunk),
            Self::V2(header) => header.chunk,
        }
    }

    pub fn cloned_versioned_chunk(&self) -> VersionedShardChunk {
        match self {
            Self::V1(header) => VersionedShardChunk::V1(header.chunk.clone()),
            Self::V2(header) => header.chunk.clone(),
        }
    }

    pub fn cloned_versioned_prev_chunk_header(&self) -> Option<VersionedShardChunkHeader> {
        match self {
            Self::V1(header) => header.prev_chunk_header.clone().map(VersionedShardChunkHeader::V1),
            Self::V2(header) => header.prev_chunk_header.clone(),
        }
    }

    pub fn chunk_height_included(&self) -> BlockHeight {
        match self {
            Self::V1(header) => header.chunk.header.height_included,
            Self::V2(header) => header.chunk.height_included(),
        }
    }

    pub fn chunk_prev_state_root(&self) -> StateRoot {
        match self {
            Self::V1(header) => header.chunk.header.inner.prev_state_root,
            Self::V2(header) => header.chunk.prev_state_root(),
        }
    }

    pub fn chunk_proof(&self) -> &MerklePath {
        match self {
            Self::V1(header) => &header.chunk_proof,
            Self::V2(header) => &header.chunk_proof,
        }
    }

    pub fn prev_chunk_proof(&self) -> &Option<MerklePath> {
        match self {
            Self::V1(header) => &header.prev_chunk_proof,
            Self::V2(header) => &header.prev_chunk_proof,
        }
    }

    pub fn incoming_receipts_proofs(&self) -> &Vec<ReceiptProofResponse> {
        match self {
            Self::V1(header) => &header.incoming_receipts_proofs,
            Self::V2(header) => &header.incoming_receipts_proofs,
        }
    }

    pub fn root_proofs(&self) -> &Vec<Vec<RootProof>> {
        match self {
            Self::V1(header) => &header.root_proofs,
            Self::V2(header) => &header.root_proofs,
        }
    }

    pub fn state_root_node(&self) -> &StateRootNode {
        match self {
            Self::V1(header) => &header.state_root_node,
            Self::V2(header) => &header.state_root_node,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Serialize)]
pub struct ShardStateSyncResponse {
    pub header: Option<ShardStateSyncResponseHeader>,
    pub part: Option<(u64, Vec<u8>)>,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Serialize)]
pub struct ShardStateSyncResponseV2 {
    pub header: Option<ShardStateSyncResponseHeaderV2>,
    pub part: Option<(u64, Vec<u8>)>,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Serialize)]
pub enum VersionedShardStateSyncResponse {
    V1(ShardStateSyncResponse),
    V2(ShardStateSyncResponseV2),
}

impl VersionedShardStateSyncResponse {
    pub fn part_id(&self) -> Option<u64> {
        match self {
            Self::V1(response) => response.part_id(),
            Self::V2(response) => response.part.as_ref().map(|(part_id, _)| *part_id),
        }
    }

    pub fn take_header(self) -> Option<VersionedShardStateSyncResponseHeader> {
        match self {
            Self::V1(response) => response.header.map(VersionedShardStateSyncResponseHeader::V1),
            Self::V2(response) => response.header.map(VersionedShardStateSyncResponseHeader::V2),
        }
    }

    pub fn part(&self) -> &Option<(u64, Vec<u8>)> {
        match self {
            Self::V1(response) => &response.part,
            Self::V2(response) => &response.part,
        }
    }

    pub fn take_part(self) -> Option<(u64, Vec<u8>)> {
        match self {
            Self::V1(response) => response.part,
            Self::V2(response) => response.part,
        }
    }
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

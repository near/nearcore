use crate::hash::CryptoHash;
use crate::merkle::MerklePath;
use crate::sharding::{ReceiptProof, ShardChunk, ShardChunkHeader};
use crate::types::{EpochId, ShardId, StateRootNode};
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::EpochHeight;
use std::sync::Arc;

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct ReceiptProofResponse(pub CryptoHash, pub Arc<Vec<ReceiptProof>>);

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct RootProof(pub CryptoHash, pub MerklePath);

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct StateHeaderKey(pub ShardId, pub CryptoHash);

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct StatePartKey(pub CryptoHash, pub ShardId, pub u64 /* PartId */);

pub const STATE_PART_MEMORY_LIMIT: bytesize::ByteSize = bytesize::ByteSize(30 * bytesize::MIB);

pub fn get_num_state_parts(memory_usage: u64) -> u64 {
    (memory_usage + STATE_PART_MEMORY_LIMIT.as_u64() - 1) / STATE_PART_MEMORY_LIMIT.as_u64()
}

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct StateSyncHeaderV1 {
    pub chunk: ShardChunk,
    pub chunk_proof: MerklePath,
    pub prev_chunk_header: Option<ShardChunkHeader>,
    pub prev_chunk_proof: Option<MerklePath>,
    pub incoming_receipts_proofs: Vec<ReceiptProofResponse>,
    pub root_proofs: Vec<Vec<RootProof>>,
    pub state_root_node: StateRootNode,
}

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub enum StateRequestHeader {
    V1(StateRequestHeaderV1),
}

impl StateRequestHeader {
    pub fn new(sync_hash: CryptoHash, shard_id: ShardId) -> Self {
        Self::V1(StateRequestHeaderV1 { sync_hash, shard_id })
    }

    pub fn sync_hash(&self) -> CryptoHash {
        match self {
            Self::V1(header) => header.sync_hash,
        }
    }

    pub fn shard_id(&self) -> ShardId {
        match self {
            Self::V1(header) => header.shard_id,
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct StateRequestHeaderV1 {
    pub sync_hash: CryptoHash,
    pub shard_id: ShardId,
}

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub enum StateRequestPart {
    V1(StateRequestPartV1),
}

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct StateRequestPartV1 {
    pub sync_hash: CryptoHash,
    pub shard_id: ShardId,
    pub part_id: u64,
}

impl StateRequestPart {
    pub fn new(sync_hash: CryptoHash, shard_id: ShardId, part_id: u64) -> Self {
        Self::V1(StateRequestPartV1 { sync_hash, shard_id, part_id })
    }

    pub fn sync_hash(&self) -> CryptoHash {
        match self {
            Self::V1(request) => request.sync_hash,
        }
    }

    pub fn shard_id(&self) -> ShardId {
        match self {
            Self::V1(request) => request.shard_id,
        }
    }

    pub fn part_id(&self) -> u64 {
        match self {
            Self::V1(request) => request.part_id,
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub enum StateResponseHeader {
    V1(StateResponseHeaderV1),
}

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct StateResponseHeaderV1 {
    pub sync_hash: CryptoHash,
    pub shard_id: ShardId,
    pub header: StateSyncHeader,
}

impl StateResponseHeader {
    pub fn new(sync_hash: CryptoHash, shard_id: ShardId, header: StateSyncHeader) -> Self {
        Self::V1(StateResponseHeaderV1 { sync_hash, shard_id, header })
    }

    pub fn sync_hash(&self) -> CryptoHash {
        match self {
            Self::V1(header) => header.sync_hash,
        }
    }

    pub fn shard_id(&self) -> ShardId {
        match self {
            Self::V1(header) => header.shard_id,
        }
    }

    pub fn header(&self) -> &StateSyncHeader {
        match self {
            Self::V1(header) => &header.header,
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub enum StateSyncHeader {
    V1(StateSyncHeaderV1),
}

impl StateSyncHeader {
    pub fn chunk(&self) -> &ShardChunk {
        match self {
            Self::V1(header) => &header.chunk,
        }
    }

    pub fn state_root_node(&self) -> &StateRootNode {
        match self {
            Self::V1(header) => &header.state_root_node,
        }
    }

    pub fn incoming_receipts_proofs(&self) -> &[ReceiptProofResponse] {
        match self {
            Self::V1(header) => &header.incoming_receipts_proofs,
        }
    }

    pub fn prev_chunk_header(&self) -> &Option<ShardChunkHeader> {
        match self {
            Self::V1(header) => &header.prev_chunk_header,
        }
    }

    pub fn chunk_proof(&self) -> &MerklePath {
        match self {
            Self::V1(header) => &header.chunk_proof,
        }
    }

    pub fn prev_chunk_proof(&self) -> &Option<MerklePath> {
        match self {
            Self::V1(header) => &header.prev_chunk_proof,
        }
    }

    pub fn root_proofs(&self) -> &[Vec<RootProof>] {
        match self {
            Self::V1(header) => &header.root_proofs,
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub enum StateResponsePart {
    V1(StateResponsePartV1),
}

impl StateResponsePart {
    pub fn new(sync_hash: CryptoHash, shard_id: ShardId, part_id: u64, part: Vec<u8>) -> Self {
        Self::V1(StateResponsePartV1 { sync_hash, shard_id, part_id, part })
    }
}

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct StateResponsePartV1 {
    pub sync_hash: CryptoHash,
    pub shard_id: ShardId,
    pub part_id: u64,
    pub part: Vec<u8>,
}

impl StateResponsePart {
    pub fn sync_hash(&self) -> CryptoHash {
        match self {
            Self::V1(part) => part.sync_hash,
        }
    }

    pub fn shard_id(&self) -> ShardId {
        match self {
            Self::V1(part) => part.shard_id,
        }
    }

    pub fn part_id(&self) -> u64 {
        match self {
            Self::V1(part) => part.part_id,
        }
    }

    pub fn part(&self) -> &Vec<u8> {
        match self {
            Self::V1(part) => &part.part,
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
/// Represents the progress of dumps state of a shard.
pub enum StateSyncDumpProgress {
    /// Represents two cases:
    /// * An epoch dump is complete
    /// * The node is running its first epoch and there is nothing to dump.
    AllDumped {
        /// The dumped state corresponds to the state at the beginning of the specified epoch.
        epoch_id: EpochId,
        epoch_height: EpochHeight,
    },
    /// Represents the case of an epoch being partially dumped.
    InProgress {
        /// The dumped state corresponds to the state at the beginning of the specified epoch.
        epoch_id: EpochId,
        epoch_height: EpochHeight,
        /// Block hash of the first block of the epoch.
        /// The dumped state corresponds to the state before applying this block.
        sync_hash: CryptoHash,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_num_state_parts() {
        assert_eq!(get_num_state_parts(0), 0);
        assert_eq!(get_num_state_parts(1), 1);
        assert_eq!(get_num_state_parts(STATE_PART_MEMORY_LIMIT.as_u64()), 1);
        assert_eq!(get_num_state_parts(STATE_PART_MEMORY_LIMIT.as_u64() + 1), 2);
        assert_eq!(get_num_state_parts(STATE_PART_MEMORY_LIMIT.as_u64() * 100), 100);
        assert_eq!(get_num_state_parts(STATE_PART_MEMORY_LIMIT.as_u64() * 100 + 1), 101);
    }
}

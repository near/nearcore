use crate::hash::CryptoHash;
use crate::merkle::MerklePath;
use crate::sharding::{
    ReceiptProof, ShardChunk, ShardChunkHeader, ShardChunkHeaderV1, ShardChunkV1,
};
use crate::types::{BlockHeight, EpochId, ShardId, StateRoot, StateRootNode};
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::EpochHeight;
use near_schema_checker_lib::ProtocolSchema;
use std::sync::Arc;

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ReceiptProofResponse(pub CryptoHash, pub Arc<Vec<ReceiptProof>>);

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct RootProof(pub CryptoHash, pub MerklePath);

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct StateHeaderKey(pub ShardId, pub CryptoHash);

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct StatePartKey(pub CryptoHash, pub ShardId, pub u64 /* PartId */);

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ShardStateSyncResponseHeaderV1 {
    pub chunk: ShardChunkV1,
    pub chunk_proof: MerklePath,
    pub prev_chunk_header: Option<ShardChunkHeaderV1>,
    pub prev_chunk_proof: Option<MerklePath>,
    pub incoming_receipts_proofs: Vec<ReceiptProofResponse>,
    pub root_proofs: Vec<Vec<RootProof>>,
    pub state_root_node: StateRootNode,
}

/// Let B[h] be the block with hash h.
/// Let shard_id be the shard ID of the shard this header is meant for
/// As a shorthand,let B_sync = B[sync_hash], B_prev = B[B_sync.prev_hash]
///
/// Also let B_chunk be the block with height B_prev.chunks[shard_id].height_included
/// that is an ancestor of B_sync. So, the last block with a new chunk before B_sync.
/// And let B_prev_chunk = B[B_chunk.prev_hash]. So, the block before the last block with a new chunk before B_sync.
///
/// Given these definitiions, the meaning of fields are explained below.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ShardStateSyncResponseHeaderV2 {
    /// The chunk whose header in included as B_prev.chunks[shard_id]
    /// This chunk will be applied after downloading state
    pub chunk: ShardChunk,
    /// A merkle path for (Self::chunk.hash, Self::chunk.height_included), verifiable
    /// against B_prev.chunk_headers_root
    pub chunk_proof: MerklePath,
    /// This is None if sync_hash is the genesis hash. Otherwise, it's B_prev_chunk.chunks[shard_id]
    pub prev_chunk_header: Option<ShardChunkHeader>,
    /// A merkle path for (Self::prev_chunk_header.hash, Self::prev_chunk_header.height_included), verifiable
    /// against B_prev_chunk.chunk_headers_root
    pub prev_chunk_proof: Option<MerklePath>,
    /// This field contains the incoming receipts for shard_id for B_sync and B_prev_chunk.
    /// So, this field has at most two elements.
    /// These receipts are used to apply `chunk` after downloading state
    pub incoming_receipts_proofs: Vec<ReceiptProofResponse>,
    /// This field contains the info necessary to verify that the receipt proofs in Self::incoming_receipts_proofs
    /// are actually the ones referenced on chain
    ///
    /// The length of this field is the same as the length of Self::incoming_receipts_proofs, and elements
    /// of the two at a given index are taken together for verification. For a given index i,
    /// root_proofs[i] is a vector of the same length as incoming_receipts_proofs[i].1 , which itself is a
    /// vector of receipt proofs for all "from_shard_ids" that sent receipts to shard_id. root_proofs[i][j]
    /// contains a merkle root equal to the prev_outgoing_receipts_root field of the corresponding chunk
    /// included in the block with hash incoming_receipts_proofs[i].0, and a merkle path to verify it against
    /// that block's prev_chunk_outgoing_receipts_root field.
    pub root_proofs: Vec<Vec<RootProof>>,
    /// The state root with hash equal to B_prev.chunks[shard_id].prev_state_root.
    /// That is, the state root node of the trie before applying the chunks in B_prev
    pub state_root_node: StateRootNode,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum CachedParts {
    AllParts,
    NoParts,
    /// Represents a subset of parts cached.
    /// Can represent both NoParts and AllParts, but in those cases use the
    /// corresponding enum values for efficiency.
    BitArray(BitArray),
}

/// Represents an array of boolean values in a compact form.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct BitArray {
    data: Vec<u8>,
    capacity: u64,
}

impl BitArray {
    pub fn new(capacity: u64) -> Self {
        let num_bytes = (capacity + 7) / 8;
        Self { data: vec![0; num_bytes as usize], capacity }
    }

    pub fn set_bit(&mut self, bit: u64) {
        assert!(bit < self.capacity);
        self.data[(bit / 8) as usize] |= 1 << (bit % 8);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum ShardStateSyncResponseHeader {
    V1(ShardStateSyncResponseHeaderV1),
    V2(ShardStateSyncResponseHeaderV2),
}

impl ShardStateSyncResponseHeader {
    #[inline]
    pub fn take_chunk(self) -> ShardChunk {
        match self {
            Self::V1(header) => ShardChunk::V1(header.chunk),
            Self::V2(header) => header.chunk,
        }
    }

    #[inline]
    pub fn cloned_chunk(&self) -> ShardChunk {
        match self {
            Self::V1(header) => ShardChunk::V1(header.chunk.clone()),
            Self::V2(header) => header.chunk.clone(),
        }
    }

    #[inline]
    pub fn cloned_prev_chunk_header(&self) -> Option<ShardChunkHeader> {
        match self {
            Self::V1(header) => header.prev_chunk_header.clone().map(ShardChunkHeader::V1),
            Self::V2(header) => header.prev_chunk_header.clone(),
        }
    }

    #[inline]
    pub fn chunk_height_included(&self) -> BlockHeight {
        match self {
            Self::V1(header) => header.chunk.header.height_included,
            Self::V2(header) => header.chunk.height_included(),
        }
    }

    #[inline]
    pub fn chunk_prev_state_root(&self) -> StateRoot {
        match self {
            Self::V1(header) => header.chunk.header.inner.prev_state_root,
            Self::V2(header) => header.chunk.prev_state_root(),
        }
    }

    #[inline]
    pub fn chunk_proof(&self) -> &MerklePath {
        match self {
            Self::V1(header) => &header.chunk_proof,
            Self::V2(header) => &header.chunk_proof,
        }
    }

    #[inline]
    pub fn prev_chunk_proof(&self) -> &Option<MerklePath> {
        match self {
            Self::V1(header) => &header.prev_chunk_proof,
            Self::V2(header) => &header.prev_chunk_proof,
        }
    }

    #[inline]
    pub fn incoming_receipts_proofs(&self) -> &[ReceiptProofResponse] {
        match self {
            Self::V1(header) => &header.incoming_receipts_proofs,
            Self::V2(header) => &header.incoming_receipts_proofs,
        }
    }

    #[inline]
    pub fn root_proofs(&self) -> &[Vec<RootProof>] {
        match self {
            Self::V1(header) => &header.root_proofs,
            Self::V2(header) => &header.root_proofs,
        }
    }

    #[inline]
    pub fn state_root_node(&self) -> &StateRootNode {
        match self {
            Self::V1(header) => &header.state_root_node,
            Self::V2(header) => &header.state_root_node,
        }
    }

    pub fn num_state_parts(&self) -> u64 {
        get_num_state_parts(self.state_root_node().memory_usage)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ShardStateSyncResponseV1 {
    pub header: Option<ShardStateSyncResponseHeaderV1>,
    pub part: Option<(u64, Vec<u8>)>,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ShardStateSyncResponseV2 {
    pub header: Option<ShardStateSyncResponseHeaderV2>,
    pub part: Option<(u64, Vec<u8>)>,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ShardStateSyncResponseV3 {
    pub header: Option<ShardStateSyncResponseHeaderV2>,
    pub part: Option<(u64, Vec<u8>)>,
    /// Parts that can be provided **cheaply**.
    // Can be `None` only if both `header` and `part` are `None`.
    pub cached_parts: Option<CachedParts>,
    /// Whether the node can provide parts for this epoch of this shard.
    /// Assumes that a node can either provide all state parts or no state parts.
    pub can_generate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum ShardStateSyncResponse {
    V1(ShardStateSyncResponseV1),
    V2(ShardStateSyncResponseV2),
    V3(ShardStateSyncResponseV3),
}

impl ShardStateSyncResponse {
    pub fn part_id(&self) -> Option<u64> {
        match self {
            Self::V1(response) => response.part_id(),
            Self::V2(response) => response.part.as_ref().map(|(part_id, _)| *part_id),
            Self::V3(response) => response.part.as_ref().map(|(part_id, _)| *part_id),
        }
    }

    pub fn take_header(self) -> Option<ShardStateSyncResponseHeader> {
        match self {
            Self::V1(response) => response.header.map(ShardStateSyncResponseHeader::V1),
            Self::V2(response) => response.header.map(ShardStateSyncResponseHeader::V2),
            Self::V3(response) => response.header.map(ShardStateSyncResponseHeader::V2),
        }
    }

    pub fn part(&self) -> &Option<(u64, Vec<u8>)> {
        match self {
            Self::V1(response) => &response.part,
            Self::V2(response) => &response.part,
            Self::V3(response) => &response.part,
        }
    }

    pub fn take_part(self) -> Option<(u64, Vec<u8>)> {
        match self {
            Self::V1(response) => response.part,
            Self::V2(response) => response.part,
            Self::V3(response) => response.part,
        }
    }

    pub fn can_generate(&self) -> bool {
        match self {
            Self::V1(_response) => false,
            Self::V2(_response) => false,
            Self::V3(response) => response.can_generate,
        }
    }

    pub fn cached_parts(&self) -> &Option<CachedParts> {
        match self {
            Self::V1(_response) => &None,
            Self::V2(_response) => &None,
            Self::V3(response) => &response.cached_parts,
        }
    }
}

impl ShardStateSyncResponseV1 {
    pub fn part_id(&self) -> Option<u64> {
        self.part.as_ref().map(|(part_id, _)| *part_id)
    }
}

pub const STATE_PART_MEMORY_LIMIT: bytesize::ByteSize = bytesize::ByteSize(30 * bytesize::MIB);

pub fn get_num_state_parts(memory_usage: u64) -> u64 {
    (memory_usage + STATE_PART_MEMORY_LIMIT.as_u64() - 1) / STATE_PART_MEMORY_LIMIT.as_u64()
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, serde::Serialize, ProtocolSchema)]
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
    /// * An epoch dump is skipped in the epoch where shard layout changes
    Skipped { epoch_id: EpochId, epoch_height: EpochHeight },
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
    use crate::state_sync::{get_num_state_parts, STATE_PART_MEMORY_LIMIT};

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

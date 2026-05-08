use crate::hash::CryptoHash;
use crate::merkle::MerklePath;
use crate::sharding::{
    ReceiptProof, ShardChunk, ShardChunkHeader, ShardChunkHeaderV1, ShardChunkV1,
};
use crate::state_part::{StatePart, StatePartV0};
use crate::stateless_validation::spice_chunk_endorsement::SpiceEndorsementCoreStatement;
use crate::types::{BlockHeight, ChunkExecutionResult, EpochId, ShardId, StateRoot, StateRootNode};
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::{EpochHeight, ProtocolVersion};
use near_primitives_core::version::ProtocolFeature;
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

#[derive(
    Copy, PartialEq, Eq, Clone, Debug, Hash, BorshSerialize, BorshDeserialize, ProtocolSchema,
)]
pub enum PartIdOrHeader {
    Part { part_id: u64 },
    Header,
}

impl Into<&'static str> for PartIdOrHeader {
    fn into(self) -> &'static str {
        match self {
            PartIdOrHeader::Part { .. } => "part",
            PartIdOrHeader::Header => "header",
        }
    }
}

#[derive(Copy, PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum StateRequestAckBody {
    WillRespond,
    Busy,
    Error,
}

impl Into<&'static str> for StateRequestAckBody {
    fn into(self) -> &'static str {
        match self {
            StateRequestAckBody::WillRespond => "will_respond",
            StateRequestAckBody::Busy => "busy",
            StateRequestAckBody::Error => "error",
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct StateRequestAck {
    /// Requested shard id
    pub shard_id: ShardId,
    /// Sync block hash
    pub sync_hash: CryptoHash,
    /// Requested header or part id
    pub part_id_or_header: PartIdOrHeader,
    /// Ack contents
    pub body: StateRequestAckBody,
}

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
/// Given these definitions, the meaning of fields are explained below.
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

/// State sync response header for SPICE chunks (V6 chunk headers are tx-only;
/// the authoritative state root and outgoing-receipts root live in the
/// `ChunkExecutionResult` certified by endorsement quorum, not in chunk-header
/// fields).
///
/// Trust path on the requester: validator set (anchored by epoch sync) →
/// signatures on `endorsements` → quorum stake on the chunk's validator
/// assignment → `execution_result.chunk_extra` and
/// `execution_result.outgoing_receipts_root` are trusted.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ShardStateSyncResponseHeaderV3 {
    /// The anchor block `B_X^certified` whose chunk for this shard is the one
    /// whose post-execution state we are syncing to. Under the strict sync-hash
    /// invariant this is `sync_prev_hash` for every shard.
    pub block_hash: CryptoHash,
    /// The certified execution result for `(block_hash, shard_id)`. Carries
    /// `chunk_extra` (post-execution state info, including the state root the
    /// downloaded parts represent) and `outgoing_receipts_root` (used to verify
    /// receipt proofs that arrive separately via the T2 pull path).
    pub execution_result: ChunkExecutionResult,
    /// Endorsements that together satisfy quorum stake on the chunk validator
    /// assignment for `(epoch_of(block_hash), shard_id, height_of(block_hash))`.
    /// PR 1 sends all available endorsements; trimming to the minimum stake-
    /// satisfying subset is a follow-up if header size becomes a concern.
    pub endorsements: Vec<SpiceEndorsementCoreStatement>,
    /// Used to size the parts download (`get_num_state_parts(memory_usage)`)
    /// and to structurally validate the trie root, now against
    /// `execution_result.chunk_extra.state_root()`.
    pub state_root_node: StateRootNode,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum CachedParts {
    AllParts = 0,
    NoParts = 1,
    /// Represents a subset of parts cached.
    /// Can represent both NoParts and AllParts, but in those cases use the
    /// corresponding enum values for efficiency.
    BitArray(BitArray) = 2,
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
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
#[allow(clippy::large_enum_variant)]
pub enum ShardStateSyncResponseHeader {
    V1(ShardStateSyncResponseHeaderV1) = 0,
    V2(ShardStateSyncResponseHeaderV2) = 1,
    V3(ShardStateSyncResponseHeaderV3) = 2,
}

impl ShardStateSyncResponseHeader {
    #[inline]
    pub fn take_chunk(self) -> ShardChunk {
        match self {
            Self::V1(header) => ShardChunk::V1(header.chunk),
            Self::V2(header) => header.chunk,
            Self::V3(_) => panic!("take_chunk: V3 is for SPICE; callers must branch on variant"),
        }
    }

    #[inline]
    pub fn cloned_chunk(&self) -> ShardChunk {
        match self {
            Self::V1(header) => ShardChunk::V1(header.chunk.clone()),
            Self::V2(header) => header.chunk.clone(),
            Self::V3(_) => panic!("cloned_chunk: V3 is for SPICE; callers must branch on variant"),
        }
    }

    #[inline]
    pub fn cloned_prev_chunk_header(&self) -> Option<ShardChunkHeader> {
        match self {
            Self::V1(header) => header.prev_chunk_header.clone().map(ShardChunkHeader::V1),
            Self::V2(header) => header.prev_chunk_header.clone(),
            Self::V3(_) => {
                panic!("cloned_prev_chunk_header: V3 is for SPICE; callers must branch on variant")
            }
        }
    }

    #[inline]
    pub fn chunk_height_included(&self) -> BlockHeight {
        match self {
            Self::V1(header) => header.chunk.header.height_included,
            Self::V2(header) => header.chunk.height_included(),
            Self::V3(_) => {
                panic!("chunk_height_included: V3 is for SPICE; callers must branch on variant")
            }
        }
    }

    /// Block hash to set as the flat-storage head for the synced shard.
    /// V1/V2 anchor at the chunk's `prev_block` (the block whose state the
    /// chunk's `prev_state_root` matches). V3 carries the anchor block hash
    /// (sync_prev_prev) directly.
    #[inline]
    pub fn flat_head_hash(&self) -> CryptoHash {
        match self {
            Self::V1(_) | Self::V2(_) => *self.cloned_chunk().prev_block(),
            Self::V3(header) => header.block_hash,
        }
    }

    /// Returns the state root that the downloaded state parts represent.
    /// For V1/V2 this is the chunk's `prev_state_root` (pre-execution state,
    /// then re-applied locally). For V3 this is the certified post-execution
    /// state root from the `ChunkExecutionResult`.
    #[inline]
    pub fn state_root(&self) -> StateRoot {
        match self {
            Self::V1(header) => header.chunk.header.inner.prev_state_root,
            Self::V2(header) => header.chunk.prev_state_root(),
            Self::V3(header) => *header.execution_result.chunk_extra.state_root(),
        }
    }

    #[inline]
    pub fn chunk_prev_state_root(&self) -> StateRoot {
        match self {
            Self::V1(header) => header.chunk.header.inner.prev_state_root,
            Self::V2(header) => header.chunk.prev_state_root(),
            Self::V3(_) => panic!(
                "chunk_prev_state_root: V3 is for SPICE; use state_root() or branch on variant"
            ),
        }
    }

    #[inline]
    pub fn chunk_proof(&self) -> &MerklePath {
        match self {
            Self::V1(header) => &header.chunk_proof,
            Self::V2(header) => &header.chunk_proof,
            Self::V3(_) => panic!("chunk_proof: V3 is for SPICE; callers must branch on variant"),
        }
    }

    #[inline]
    pub fn prev_chunk_proof(&self) -> &Option<MerklePath> {
        match self {
            Self::V1(header) => &header.prev_chunk_proof,
            Self::V2(header) => &header.prev_chunk_proof,
            Self::V3(_) => {
                panic!("prev_chunk_proof: V3 is for SPICE; callers must branch on variant")
            }
        }
    }

    #[inline]
    pub fn incoming_receipts_proofs(&self) -> &[ReceiptProofResponse] {
        match self {
            Self::V1(header) => &header.incoming_receipts_proofs,
            Self::V2(header) => &header.incoming_receipts_proofs,
            Self::V3(_) => {
                panic!("incoming_receipts_proofs: V3 is for SPICE; receipts arrive via T2 pull")
            }
        }
    }

    #[inline]
    pub fn root_proofs(&self) -> &[Vec<RootProof>] {
        match self {
            Self::V1(header) => &header.root_proofs,
            Self::V2(header) => &header.root_proofs,
            Self::V3(_) => panic!("root_proofs: V3 is for SPICE; callers must branch on variant"),
        }
    }

    #[inline]
    pub fn state_root_node(&self) -> &StateRootNode {
        match self {
            Self::V1(header) => &header.state_root_node,
            Self::V2(header) => &header.state_root_node,
            Self::V3(header) => &header.state_root_node,
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

impl ShardStateSyncResponseV1 {
    pub fn part_id(&self) -> Option<u64> {
        self.part.as_ref().map(|(part_id, _)| *part_id)
    }

    pub fn payload_length(&self) -> Option<usize> {
        self.part.as_ref().map(|(_, part)| part.len())
    }
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
    pub cached_parts: Option<CachedParts>,
    pub can_generate: bool,
}

/// Between V3 to V4 we removed unused fields `cached_parts` and `can_generate` and introduced versioned `StatePart`.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ShardStateSyncResponseV4 {
    pub header: Option<ShardStateSyncResponseHeaderV2>,
    pub part: Option<(u64, StatePart)>,
}

/// V5 carries a SPICE-anchored header (`ShardStateSyncResponseHeaderV3`).
/// Same `part` shape as V4 — every SPICE-enabled epoch is past
/// `ProtocolFeature::StatePartsCompression`, so the versioned `StatePart` is
/// always available.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ShardStateSyncResponseV5 {
    pub header: Option<ShardStateSyncResponseHeaderV3>,
    pub part: Option<(u64, StatePart)>,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum ShardStateSyncResponse {
    V1(ShardStateSyncResponseV1) = 0,
    V2(ShardStateSyncResponseV2) = 1,
    V3(ShardStateSyncResponseV3) = 2,
    V4(ShardStateSyncResponseV4) = 3,
    V5(ShardStateSyncResponseV5) = 4,
}

impl ShardStateSyncResponse {
    pub fn new_from_header(
        header: Option<ShardStateSyncResponseHeaderV2>,
        protocol_version: ProtocolVersion,
    ) -> Self {
        Self::new_from_header_or_part(header, None, protocol_version)
    }

    pub fn new_from_part(
        part: Option<(u64, StatePart)>,
        protocol_version: ProtocolVersion,
    ) -> Self {
        Self::new_from_header_or_part(None, part, protocol_version)
    }

    fn new_from_header_or_part(
        header: Option<ShardStateSyncResponseHeaderV2>,
        part: Option<(u64, StatePart)>,
        protocol_version: ProtocolVersion,
    ) -> Self {
        if ProtocolFeature::StatePartsCompression.enabled(protocol_version) {
            return Self::V4(ShardStateSyncResponseV4 { header, part });
        }
        let part = match part {
            None => None,
            Some((part_id, StatePart::V0(part))) => Some((part_id, part.0)),
            // This should not happen, as it would mean we serve `StatePartV1`` or higher
            // before `StatePartsCompression` is enabled.
            _ => panic!("StatePartsCompression not supported and part={part:?}"),
        };
        Self::V3(ShardStateSyncResponseV3 { header, part, cached_parts: None, can_generate: false })
    }

    /// Constructs a SPICE-anchored response (V5) from a V3 header.
    pub fn new_spice_from_header(header: Option<ShardStateSyncResponseHeaderV3>) -> Self {
        Self::V5(ShardStateSyncResponseV5 { header, part: None })
    }

    /// Constructs a SPICE-anchored response (V5) from a state part.
    pub fn new_spice_from_part(part: Option<(u64, StatePart)>) -> Self {
        Self::V5(ShardStateSyncResponseV5 { header: None, part })
    }

    pub fn take_header(self) -> Option<ShardStateSyncResponseHeader> {
        match self {
            Self::V1(response) => response.header.map(ShardStateSyncResponseHeader::V1),
            Self::V2(response) => response.header.map(ShardStateSyncResponseHeader::V2),
            Self::V3(response) => response.header.map(ShardStateSyncResponseHeader::V2),
            Self::V4(response) => response.header.map(ShardStateSyncResponseHeader::V2),
            Self::V5(response) => response.header.map(ShardStateSyncResponseHeader::V3),
        }
    }

    pub fn part_id(&self) -> Option<u64> {
        match self {
            Self::V1(response) => response.part.as_ref().map(|(part_id, _)| *part_id),
            Self::V2(response) => response.part.as_ref().map(|(part_id, _)| *part_id),
            Self::V3(response) => response.part.as_ref().map(|(part_id, _)| *part_id),
            Self::V4(response) => response.part.as_ref().map(|(part_id, _)| *part_id),
            Self::V5(response) => response.part.as_ref().map(|(part_id, _)| *part_id),
        }
    }

    pub fn take_part(self) -> Option<(u64, StatePart)> {
        match self {
            Self::V1(response) => {
                response.part.map(|(idx, part)| (idx, StatePart::V0(StatePartV0(part))))
            }
            Self::V2(response) => {
                response.part.map(|(idx, part)| (idx, StatePart::V0(StatePartV0(part))))
            }
            Self::V3(response) => {
                response.part.map(|(idx, part)| (idx, StatePart::V0(StatePartV0(part))))
            }
            Self::V4(response) => response.part,
            Self::V5(response) => response.part,
        }
    }

    pub fn payload_length(&self) -> Option<usize> {
        match self {
            Self::V1(response) => response.part.as_ref().map(|(_, part)| part.len()),
            Self::V2(response) => response.part.as_ref().map(|(_, part)| part.len()),
            Self::V3(response) => response.part.as_ref().map(|(_, part)| part.len()),
            Self::V4(response) => response.part.as_ref().map(|(_, part)| part.payload_length()),
            Self::V5(response) => response.part.as_ref().map(|(_, part)| part.payload_length()),
        }
    }
}

pub const STATE_PART_MEMORY_LIMIT: bytesize::ByteSize = bytesize::ByteSize(30 * bytesize::MIB);

pub fn get_num_state_parts(memory_usage: u64) -> u64 {
    (memory_usage + STATE_PART_MEMORY_LIMIT.as_u64() - 1) / STATE_PART_MEMORY_LIMIT.as_u64()
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, serde::Serialize, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
/// Represents the progress of dumps state of a shard.
pub enum StateSyncDumpProgress {
    /// Represents two cases:
    /// * An epoch dump is complete
    /// * The node is running its first epoch and there is nothing to dump.
    AllDumped {
        /// The dumped state corresponds to the state at the beginning of the specified epoch.
        epoch_id: EpochId,
        epoch_height: EpochHeight,
    } = 0,
    /// * An epoch dump is skipped in the epoch where shard layout changes
    Skipped { epoch_id: EpochId, epoch_height: EpochHeight } = 1,
    /// Represents the case of an epoch being partially dumped.
    InProgress {
        /// The dumped state corresponds to the state at the beginning of the specified epoch.
        epoch_id: EpochId,
        epoch_height: EpochHeight,
        /// Block hash of the first block of the epoch.
        /// The dumped state corresponds to the state before applying this block.
        sync_hash: CryptoHash,
    } = 2,
}

#[cfg(test)]
mod tests {
    use crate::state_sync::{STATE_PART_MEMORY_LIMIT, get_num_state_parts};

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

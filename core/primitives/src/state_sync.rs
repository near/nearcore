use crate::hash::CryptoHash;
use crate::merkle::MerklePath;
use crate::sharding::{
    ReceiptProof, ShardChunk, ShardChunkHeader, ShardChunkHeaderV1, ShardChunkV1,
};
use crate::state_part::{StatePart, StatePartIndex, StatePartV0};
use crate::types::{
    BlockHeight, ChunkExecutionResult, ChunkExecutionRoots, EpochId, ShardId, StateRoot,
    StateRootNode,
};
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
pub struct StatePartKey(pub CryptoHash, pub ShardId, pub StatePartIndex);

#[derive(
    Copy, PartialEq, Eq, Clone, Debug, Hash, BorshSerialize, BorshDeserialize, ProtocolSchema,
)]
pub enum PartOrHeader {
    Part { part_idx: StatePartIndex },
    Header,
}

impl Into<&'static str> for PartOrHeader {
    fn into(self) -> &'static str {
        match self {
            PartOrHeader::Part { .. } => "part",
            PartOrHeader::Header => "header",
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
    /// Requested part or header
    pub part_or_header: PartOrHeader,
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

/// Proof that a `ChunkExecutionRoots` leaf belongs to a block's `chunk_execution_root`.
///
/// A spice chunk header commits no state root of its own - the chunk executes after the
/// block that carries it - so the root state sync validates parts against comes from the
/// chunk's execution result instead. That result is committed by a *later* block, via the
/// `chunk_execution_root` field of its header. Header sync precedes state sync, so the
/// syncing node already holds that header and needs only the leaf and the path to it.
#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct SpiceRootProof {
    /// Block whose header's `chunk_execution_root` this proof is verified against.
    pub committing_block_hash: CryptoHash,
    /// The merkle leaf, carrying the chunk's post-execution `state_root`.
    pub roots: ChunkExecutionRoots,
    /// Path from `roots` to the committing block's `chunk_execution_root`.
    pub proof: MerklePath,
}

/// The spice state sync header. `sync_hash` is the epoch's first block and the state being
/// synced is the one left behind by that block's chunk, so unlike V1/V2 there is no chunk to
/// apply afterwards and hence no chunk, receipts or receipt proofs to carry.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ShardStateSyncResponseHeaderV3 {
    /// The state root node of the trie the parts reconstruct: the state after the chunk in
    /// the sync block ran.
    pub state_root_node: StateRootNode,
    /// Proves `execution_result`'s leaf against the committing block's `chunk_execution_root`.
    pub state_root_proof: SpiceRootProof,
    /// The chunk's certified execution result, which the receiving node records as the
    /// `ChunkExtra` of the sync block so the executor can carry on from there.
    ///
    /// Fully covered by `state_root_proof`: the leaf carries `execution_result_hash`, so
    /// re-deriving `ChunkExecutionRoots` from this result and comparing it against the proven
    /// leaf ties every field of the result - the three roots and the rest of the `ChunkExtra`
    /// alike - to what the chain committed. `set_state_header` does exactly that.
    pub execution_result: ChunkExecutionResult,
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
    /// The chunk to apply after downloading the state. `None` under spice, where the
    /// downloaded state already includes the sync block's chunk.
    #[inline]
    pub fn take_chunk(self) -> Option<ShardChunk> {
        match self {
            Self::V1(header) => Some(ShardChunk::V1(header.chunk)),
            Self::V2(header) => Some(header.chunk),
            Self::V3(_) => None,
        }
    }

    #[inline]
    pub fn cloned_chunk(&self) -> Option<ShardChunk> {
        match self {
            Self::V1(header) => Some(ShardChunk::V1(header.chunk.clone())),
            Self::V2(header) => Some(header.chunk.clone()),
            Self::V3(_) => None,
        }
    }

    #[inline]
    pub fn cloned_prev_chunk_header(&self) -> Option<ShardChunkHeader> {
        match self {
            Self::V1(header) => header.prev_chunk_header.clone().map(ShardChunkHeader::V1),
            Self::V2(header) => header.prev_chunk_header.clone(),
            Self::V3(_) => None,
        }
    }

    #[inline]
    pub fn chunk_height_included(&self) -> Option<BlockHeight> {
        match self {
            Self::V1(header) => Some(header.chunk.header.height_included),
            Self::V2(header) => Some(header.chunk.height_included()),
            Self::V3(_) => None,
        }
    }

    /// The root of the trie the state parts reconstruct, and the root the node holds once the
    /// sync finishes. V1/V2 give the state from before the sync-prev chunk ran, which the node
    /// then advances by applying that chunk; V3 gives the state from after the sync block's
    /// chunk ran, which needs no further application.
    #[inline]
    pub fn synced_state_root(&self) -> StateRoot {
        match self {
            Self::V1(header) => header.chunk.header.inner.prev_state_root,
            Self::V2(header) => header.chunk.prev_state_root(),
            Self::V3(header) => *header.state_root_proof.roots.state_root(),
        }
    }

    #[inline]
    pub fn chunk_proof(&self) -> Option<&MerklePath> {
        match self {
            Self::V1(header) => Some(&header.chunk_proof),
            Self::V2(header) => Some(&header.chunk_proof),
            Self::V3(_) => None,
        }
    }

    #[inline]
    pub fn prev_chunk_proof(&self) -> &Option<MerklePath> {
        const NONE: &Option<MerklePath> = &None;
        match self {
            Self::V1(header) => &header.prev_chunk_proof,
            Self::V2(header) => &header.prev_chunk_proof,
            Self::V3(_) => NONE,
        }
    }

    #[inline]
    pub fn incoming_receipts_proofs(&self) -> &[ReceiptProofResponse] {
        match self {
            Self::V1(header) => &header.incoming_receipts_proofs,
            Self::V2(header) => &header.incoming_receipts_proofs,
            Self::V3(_) => &[],
        }
    }

    #[inline]
    pub fn root_proofs(&self) -> &[Vec<RootProof>] {
        match self {
            Self::V1(header) => &header.root_proofs,
            Self::V2(header) => &header.root_proofs,
            Self::V3(_) => &[],
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

    /// The spice root proof, present exactly on V3.
    #[inline]
    pub fn spice_root_proof(&self) -> Option<&SpiceRootProof> {
        match self {
            Self::V1(_) | Self::V2(_) => None,
            Self::V3(header) => Some(&header.state_root_proof),
        }
    }

    /// The certified execution result of the sync block's chunk, present exactly on V3.
    #[inline]
    pub fn spice_execution_result(&self) -> Option<&ChunkExecutionResult> {
        match self {
            Self::V1(_) | Self::V2(_) => None,
            Self::V3(header) => Some(&header.execution_result),
        }
    }

    pub fn num_state_parts(&self) -> u64 {
        get_num_state_parts(self.state_root_node().memory_usage)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ShardStateSyncResponseV1 {
    pub header: Option<ShardStateSyncResponseHeaderV1>,
    pub part: Option<(StatePartIndex, Vec<u8>)>,
}

impl ShardStateSyncResponseV1 {
    pub fn part_idx(&self) -> Option<StatePartIndex> {
        self.part.as_ref().map(|(part_idx, _)| *part_idx)
    }

    pub fn payload_length(&self) -> Option<usize> {
        self.part.as_ref().map(|(_, part)| part.len())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ShardStateSyncResponseV2 {
    pub header: Option<ShardStateSyncResponseHeaderV2>,
    pub part: Option<(StatePartIndex, Vec<u8>)>,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ShardStateSyncResponseV3 {
    pub header: Option<ShardStateSyncResponseHeaderV2>,
    pub part: Option<(StatePartIndex, Vec<u8>)>,
    pub cached_parts: Option<CachedParts>,
    pub can_generate: bool,
}

/// Between V3 to V4 we removed unused fields `cached_parts` and `can_generate` and introduced versioned `StatePart`.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ShardStateSyncResponseV4 {
    pub header: Option<ShardStateSyncResponseHeaderV2>,
    pub part: Option<(StatePartIndex, StatePart)>,
}

/// V4 to V5: the header is a `ShardStateSyncResponseHeaderV3`, which is what spice serves.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ShardStateSyncResponseV5 {
    pub header: Option<ShardStateSyncResponseHeaderV3>,
    pub part: Option<(StatePartIndex, StatePart)>,
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
    /// Each response version pins one header version, so the response version follows from
    /// the header being served.
    pub fn new_from_header(header: Option<ShardStateSyncResponseHeader>) -> Self {
        match header {
            Some(ShardStateSyncResponseHeader::V1(header)) => {
                Self::V1(ShardStateSyncResponseV1 { header: Some(header), part: None })
            }
            Some(ShardStateSyncResponseHeader::V2(header)) => {
                Self::V4(ShardStateSyncResponseV4 { header: Some(header), part: None })
            }
            Some(ShardStateSyncResponseHeader::V3(header)) => {
                Self::V5(ShardStateSyncResponseV5 { header: Some(header), part: None })
            }
            None => Self::V4(ShardStateSyncResponseV4 { header: None, part: None }),
        }
    }

    pub fn new_from_part(part: Option<(StatePartIndex, StatePart)>) -> Self {
        Self::V4(ShardStateSyncResponseV4 { header: None, part })
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

    pub fn part_idx(&self) -> Option<StatePartIndex> {
        match self {
            Self::V1(response) => response.part.as_ref().map(|(part_idx, _)| *part_idx),
            Self::V2(response) => response.part.as_ref().map(|(part_idx, _)| *part_idx),
            Self::V3(response) => response.part.as_ref().map(|(part_idx, _)| *part_idx),
            Self::V4(response) => response.part.as_ref().map(|(part_idx, _)| *part_idx),
            Self::V5(response) => response.part.as_ref().map(|(part_idx, _)| *part_idx),
        }
    }

    pub fn take_part(self) -> Option<(StatePartIndex, StatePart)> {
        match self {
            Self::V1(response) => {
                response.part.map(|(part_idx, part)| (part_idx, StatePart::V0(StatePartV0(part))))
            }
            Self::V2(response) => {
                response.part.map(|(part_idx, part)| (part_idx, StatePart::V0(StatePartV0(part))))
            }
            Self::V3(response) => {
                response.part.map(|(part_idx, part)| (part_idx, StatePart::V0(StatePartV0(part))))
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
    use crate::bandwidth_scheduler::BandwidthRequests;
    use crate::congestion_info::CongestionInfo;
    use crate::hash::{CryptoHash, hash};
    use crate::merkle::{MerklePath, merklize, verify_path};
    use crate::state_sync::{STATE_PART_MEMORY_LIMIT, SpiceRootProof, get_num_state_parts};
    use crate::types::chunk_extra::ChunkExtra;
    use crate::types::{
        Balance, ChunkExecutionResult, ChunkExecutionRoots, Gas, ShardId, SpiceChunkId,
        sorted_chunk_execution_roots,
    };

    /// A block's worth of execution results, one per shard, with distinguishable roots.
    fn execution_results(
        block_hash: CryptoHash,
        num_shards: u64,
    ) -> Vec<(SpiceChunkId, ChunkExecutionResult)> {
        (0..num_shards)
            .map(|i| {
                let chunk_id = SpiceChunkId { block_hash, shard_id: ShardId::new(i) };
                let result = ChunkExecutionResult {
                    chunk_extra: ChunkExtra::new_with_only_state_root(&hash(
                        format!("state-root-{i}").as_bytes(),
                    )),
                    outgoing_receipts_root: hash(format!("receipts-root-{i}").as_bytes()),
                };
                (chunk_id, result)
            })
            .collect()
    }

    fn leaves_and_root(
        results: &[(SpiceChunkId, ChunkExecutionResult)],
    ) -> (Vec<ChunkExecutionRoots>, CryptoHash, Vec<MerklePath>) {
        let leaves = sorted_chunk_execution_roots(results.iter().map(|(id, r)| (id, r)));
        let (root, paths) = merklize(&leaves);
        (leaves, root, paths)
    }

    /// A proof built from a block's execution results verifies against the
    /// `chunk_execution_root` that block's header commits to, and the leaf carries the
    /// shard's post-execution state root.
    #[test]
    fn test_spice_root_proof_round_trip() {
        let block_hash = hash(b"committing-block");
        let results = execution_results(block_hash, 4);
        let (leaves, chunk_execution_root, paths) = leaves_and_root(&results);

        for (index, leaf) in leaves.iter().enumerate() {
            let proof = SpiceRootProof {
                committing_block_hash: block_hash,
                roots: leaf.clone(),
                proof: paths[index].clone(),
            };
            assert!(
                verify_path(chunk_execution_root, &proof.proof, &proof.roots),
                "leaf {index} should verify against the root it was merklized into"
            );

            // The leaf is what supplies the state root state sync validates parts against.
            let (_, expected) = results
                .iter()
                .find(|(id, _)| id == proof.roots.chunk_id())
                .expect("leaf must name one of the chunks");
            assert_eq!(proof.roots.state_root(), expected.chunk_extra.state_root());
        }
    }

    /// A leaf from a different chunk still verifies against its own root, which is why
    /// `set_state_header` binds the leaf to the expected `SpiceChunkId` before checking
    /// the path - the path alone does not say *which* chunk was proven.
    #[test]
    fn test_spice_root_proof_leaf_identifies_its_chunk() {
        let block_hash = hash(b"committing-block");
        let results = execution_results(block_hash, 4);
        let (leaves, chunk_execution_root, paths) = leaves_and_root(&results);

        let wanted = SpiceChunkId { block_hash, shard_id: ShardId::new(0) };
        let other_index =
            leaves.iter().position(|leaf| leaf.chunk_id() != &wanted).expect("another chunk");

        // The other chunk's proof is perfectly valid...
        assert!(verify_path(chunk_execution_root, &paths[other_index], &leaves[other_index]));
        // ...but it does not prove anything about the chunk we asked for.
        assert_ne!(leaves[other_index].chunk_id(), &wanted);
    }

    /// A tampered leaf or a mismatched path must not verify.
    #[test]
    fn test_spice_root_proof_rejects_tampering() {
        let block_hash = hash(b"committing-block");
        let results = execution_results(block_hash, 4);
        let (leaves, chunk_execution_root, paths) = leaves_and_root(&results);

        // Same leaf, another leaf's path.
        assert!(!verify_path(chunk_execution_root, &paths[1], &leaves[0]));

        // Right path, but the leaf's state root has been swapped for another shard's.
        let ChunkExecutionRoots::V1(mut tampered) = leaves[0].clone();
        tampered.state_root = hash(b"state-root-1");
        assert!(!verify_path(chunk_execution_root, &paths[0], &ChunkExecutionRoots::V1(tampered)));

        // Right leaf and path, wrong root.
        assert!(!verify_path(hash(b"some-other-block"), &paths[0], &leaves[0]));
    }

    /// The leaf covers the whole `ChunkExecutionResult`, not only the three roots the chain
    /// reads directly. Two results that agree on every root but differ in a `ChunkExtra` field
    /// still produce different leaves, so a proof of one does not carry over to the other and
    /// `set_state_header` cannot be handed a result with a tampered gas limit.
    #[test]
    fn test_spice_leaf_covers_the_whole_execution_result() {
        let block_hash = hash(b"committing-block");
        let chunk_id = SpiceChunkId { block_hash, shard_id: ShardId::new(0) };
        let state_root = hash(b"state-root");
        let outcome_root = hash(b"outcome-root");
        let outgoing_receipts_root = hash(b"receipts-root");

        let with_gas_limit = |gas_limit| ChunkExecutionResult {
            chunk_extra: ChunkExtra::new(
                &state_root,
                outcome_root,
                vec![],
                Gas::ZERO,
                gas_limit,
                Balance::ZERO,
                Some(CongestionInfo::default()),
                BandwidthRequests::empty(),
                None,
            ),
            outgoing_receipts_root,
        };
        let cheap = with_gas_limit(Gas::from_gas(1_000));
        let pricey = with_gas_limit(Gas::from_gas(2_000));

        // Every root the chain reads directly is identical...
        assert_eq!(cheap.chunk_extra.state_root(), pricey.chunk_extra.state_root());
        assert_eq!(cheap.chunk_extra.outcome_root(), pricey.chunk_extra.outcome_root());
        assert_eq!(cheap.outgoing_receipts_root, pricey.outgoing_receipts_root);

        // ...but the leaves differ, because each carries the result's hash.
        assert_ne!(
            ChunkExecutionRoots::from_execution_result(&chunk_id, &cheap),
            ChunkExecutionRoots::from_execution_result(&chunk_id, &pricey)
        );
    }

    /// The V3 header carries the execution result whose `ChunkExtra` the syncing node
    /// records for the sync block. `set_state_header` ties it to the proven leaf by
    /// re-deriving the roots, which catches a result swapped for another shard's.
    #[test]
    fn test_spice_execution_result_is_tied_to_the_proven_leaf() {
        let block_hash = hash(b"committing-block");
        let results = execution_results(block_hash, 4);
        let (leaves, _, _) = leaves_and_root(&results);

        for leaf in &leaves {
            let chunk_id = leaf.chunk_id();
            let (_, result) = results.iter().find(|(id, _)| id == chunk_id).unwrap();
            assert_eq!(&ChunkExecutionRoots::from_execution_result(chunk_id, result), leaf);

            let (other_id, other_result) =
                results.iter().find(|(id, _)| id != chunk_id).expect("another chunk");
            assert_ne!(&ChunkExecutionRoots::from_execution_result(chunk_id, other_result), leaf);
            assert_ne!(&ChunkExecutionRoots::from_execution_result(other_id, result), leaf);
        }
    }

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

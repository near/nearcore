use borsh::{BorshDeserialize, BorshSerialize};
use serde::Serialize;

use crate::block_header::BlockHeader;
use crate::epoch_manager::block_info::BlockInfo;
use crate::epoch_manager::epoch_info::EpochInfo;
use crate::hash::CryptoHash;
use crate::merkle::{MerklePath, PartialMerkleTree};
use crate::receipt::Receipt;
use crate::sharding::{
    ReceiptProof, ShardChunk, ShardChunkHeader, ShardChunkHeaderV1, ShardChunkV1,
};
use crate::types::{BlockHeight, ShardId, StateRoot, StateRootNode};
use crate::views::LightClientBlockView;

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
pub struct ShardStateSyncResponseHeaderV1 {
    pub chunk: ShardChunkV1,
    pub chunk_proof: MerklePath,
    pub prev_chunk_header: Option<ShardChunkHeaderV1>,
    pub prev_chunk_proof: Option<MerklePath>,
    pub incoming_receipts_proofs: Vec<ReceiptProofResponse>,
    pub root_proofs: Vec<Vec<RootProof>>,
    pub state_root_node: StateRootNode,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Serialize)]
pub struct ShardStateSyncResponseHeaderV2 {
    pub chunk: ShardChunk,
    pub chunk_proof: MerklePath,
    pub prev_chunk_header: Option<ShardChunkHeader>,
    pub prev_chunk_proof: Option<MerklePath>,
    pub incoming_receipts_proofs: Vec<ReceiptProofResponse>,
    pub root_proofs: Vec<Vec<RootProof>>,
    pub state_root_node: StateRootNode,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Serialize)]
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
    pub fn incoming_receipts_proofs(&self) -> &Vec<ReceiptProofResponse> {
        match self {
            Self::V1(header) => &header.incoming_receipts_proofs,
            Self::V2(header) => &header.incoming_receipts_proofs,
        }
    }

    #[inline]
    pub fn root_proofs(&self) -> &Vec<Vec<RootProof>> {
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
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Serialize)]
pub struct ShardStateSyncResponseV1 {
    pub header: Option<ShardStateSyncResponseHeaderV1>,
    pub part: Option<(u64, Vec<u8>)>,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Serialize)]
pub struct ShardStateSyncResponseV2 {
    pub header: Option<ShardStateSyncResponseHeaderV2>,
    pub part: Option<(u64, Vec<u8>)>,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Serialize)]
pub enum ShardStateSyncResponse {
    V1(ShardStateSyncResponseV1),
    V2(ShardStateSyncResponseV2),
}

impl ShardStateSyncResponse {
    pub fn part_id(&self) -> Option<u64> {
        match self {
            Self::V1(response) => response.part_id(),
            Self::V2(response) => response.part.as_ref().map(|(part_id, _)| *part_id),
        }
    }

    pub fn take_header(self) -> Option<ShardStateSyncResponseHeader> {
        match self {
            Self::V1(response) => response.header.map(ShardStateSyncResponseHeader::V1),
            Self::V2(response) => response.header.map(ShardStateSyncResponseHeader::V2),
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

impl ShardStateSyncResponseV1 {
    pub fn part_id(&self) -> Option<u64> {
        self.part.as_ref().map(|(part_id, _)| *part_id)
    }
}

#[derive(BorshSerialize, BorshDeserialize, Eq, PartialEq, Debug, Clone)]
pub struct EpochSyncFinalizationResponse {
    pub cur_epoch_header: BlockHeader,
    pub prev_epoch_headers: Vec<BlockHeader>,
    pub header_sync_init_header: BlockHeader,
    pub header_sync_init_header_tree: PartialMerkleTree,
    // This Block Info is required by Epoch Manager when it checks if it's a good time to start a new Epoch.
    // Epoch Manager asks for height difference by obtaining first Block Info of the Epoch.
    pub prev_epoch_first_block_info: BlockInfo,
    // This Block Info is required in State Sync that is started right after Epoch Sync is finished.
    // It is used by `verify_chunk_signature_with_header_parts` in `save_block` as it calls `get_epoch_id_from_prev_block`.
    pub prev_epoch_prev_last_block_info: BlockInfo,
    // This Block Info is connected with the first actual Block received in State Sync.
    // It is also used in Epoch Manager.
    pub prev_epoch_last_block_info: BlockInfo,
    pub prev_epoch_info: EpochInfo,
    pub cur_epoch_info: EpochInfo,
    // Next Epoch Info is required by Block Sync when Blocks of current Epoch will come.
    // It asks in `process_block_single`, returns `Epoch Out Of Bounds` error otherwise.
    pub next_epoch_info: EpochInfo,
}

#[derive(BorshSerialize, BorshDeserialize, Eq, PartialEq, Debug, Clone)]
pub enum EpochSyncResponse {
    UpToDate,
    Advance { light_client_block_view: LightClientBlockView },
}

pub fn get_num_state_parts(memory_usage: u64) -> u64 {
    // We assume that 1 Mb is a good limit for state part size.
    // On the other side, it's important to divide any state into
    // several parts to make sure that partitioning always works.
    // TODO #1708
    memory_usage / (1024 * 1024) + 3
}

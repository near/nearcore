//! Structs in this module are used for debug purposes, and might change at any time
//! without backwards compatibility of JSON encoding.
use crate::types::StatusError;
use chrono::DateTime;
use near_primitives::types::EpochId;
use near_primitives::views::{
    CatchupStatusView, ChainProcessingInfo, EpochValidatorInfo, RequestedStatePartsView,
    SyncStatusView,
};
use near_primitives::{
    block_header::ApprovalInner,
    hash::CryptoHash,
    sharding::ChunkHash,
    types::{AccountId, BlockHeight},
    views::ValidatorInfo,
};
use std::collections::HashMap;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct TrackedShardsView {
    pub shards_tracked_this_epoch: Vec<bool>,
    pub shards_tracked_next_epoch: Vec<bool>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct EpochInfoView {
    pub epoch_id: CryptoHash,
    pub height: BlockHeight,
    pub first_block: Option<(CryptoHash, DateTime<chrono::Utc>)>,
    pub block_producers: Vec<ValidatorInfo>,
    pub chunk_only_producers: Vec<String>,
    pub validator_info: Option<EpochValidatorInfo>,
    pub protocol_version: u32,
    pub shards_size_and_parts: Vec<(u64, u64, bool)>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct DebugChunkStatus {
    pub shard_id: u64,
    pub chunk_hash: ChunkHash,
    pub chunk_producer: Option<AccountId>,
    pub gas_used: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processing_time_ms: Option<u64>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct DebugBlockStatus {
    pub block_hash: CryptoHash,
    pub prev_block_hash: CryptoHash,
    pub block_height: u64,
    pub block_timestamp: u64,
    pub block_producer: Option<AccountId>,
    pub full_block_missing: bool, // only header available
    pub is_on_canonical_chain: bool,
    pub chunks: Vec<DebugChunkStatus>,
    // Time that was spent processing a given block.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processing_time_ms: Option<u64>,
    pub gas_price_ratio: f64,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct MissedHeightInfo {
    pub block_height: u64,
    pub block_producer: Option<AccountId>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct DebugBlockStatusData {
    pub blocks: Vec<DebugBlockStatus>,
    pub missed_heights: Vec<MissedHeightInfo>,
    pub head: CryptoHash,
    pub header_head: CryptoHash,
}

// Information about the approval created by this node.
// Used for debug purposes only.
#[derive(serde::Serialize, Debug, Clone)]
pub struct ApprovalHistoryEntry {
    // If target_height == base_height + 1  - this is endorsement.
    // Otherwise this is a skip.
    pub parent_height: BlockHeight,
    pub target_height: BlockHeight,
    // Time when we actually created the approval and sent it out.
    pub approval_creation_time: DateTime<chrono::Utc>,
    // The moment when we were ready to send this approval (or skip)
    pub timer_started_ago_millis: u64,
    // But we had to wait at least this long before doing it.
    pub expected_delay_millis: u64,
}

// Information about chunk produced by this node.
// For debug purposes only.
#[derive(serde::Serialize, Debug, Default, Clone)]
pub struct ChunkProduction {
    // Time when we produced the chunk.
    pub chunk_production_time: Option<DateTime<chrono::Utc>>,
    // How long did the chunk production take (reed solomon encoding, preparing fragments etc.)
    // Doesn't include network latency.
    pub chunk_production_duration_millis: Option<u64>,
}
// Information about the block produced by this node.
// For debug purposes only.
#[derive(serde::Serialize, Debug, Clone, Default)]
pub struct BlockProduction {
    // Approvals that we received.
    pub approvals: ApprovalAtHeightStatus,
    // Chunk producer and time at which we received chunk for given shard. This field will not be
    // set if we didn't produce the block.
    pub chunks_collection_time: Vec<ChunkCollection>,
    // Time when we produced the block, None if we didn't produce the block.
    pub block_production_time: Option<DateTime<chrono::Utc>>,
    // Whether this block is included on the canonical chain.
    pub block_included: bool,
}

#[derive(serde::Serialize, Debug, Clone)]
pub struct ChunkCollection {
    // Chunk producer of the chunk
    pub chunk_producer: AccountId,
    // Time when the chunk was received. Note that this field can be filled even if the block doesn't
    // include a chunk for the shard, if a chunk at this height was received after the block was produced.
    pub received_time: Option<DateTime<chrono::Utc>>,
    // Whether the block included a chunk for this shard
    pub chunk_included: bool,
}

// Information about things related to block/chunk production
// at given height.
// For debug purposes only.
#[derive(serde::Serialize, Debug, Default)]
pub struct ProductionAtHeight {
    // Stores information about block production is we are responsible for producing this block,
    // None if we are not responsible for producing this block.
    pub block_production: Option<BlockProduction>,
    // Map from shard_id to chunk that we are responsible to produce at this height
    pub chunk_production: HashMap<u64, ChunkProduction>,
}

// Infromation about the approvals that we received.
#[derive(serde::Serialize, Debug, Default, Clone)]
pub struct ApprovalAtHeightStatus {
    // Map from validator id to the type of approval that they sent and timestamp.
    pub approvals: HashMap<AccountId, (ApprovalInner, DateTime<chrono::Utc>)>,
    // Time at which we received 2/3 approvals (doomslug threshold).
    pub ready_at: Option<DateTime<chrono::Utc>>,
}

#[derive(serde::Serialize, Debug)]
pub struct ValidatorStatus {
    pub validator_name: Option<AccountId>,
    // Current number of shards
    pub shards: u64,
    // Current height.
    pub head_height: u64,
    // Current validators with their stake (stake is in NEAR - not yoctonear).
    pub validators: Option<Vec<(AccountId, u64)>>,
    // All approvals that we've sent.
    pub approval_history: Vec<ApprovalHistoryEntry>,
    // Blocks & chunks that we've produced or about to produce.
    // Sorted by block height inversely (high to low)
    // The range of heights are controlled by constants in client_actor.rs
    pub production: Vec<(BlockHeight, ProductionAtHeight)>,
    // Chunk producers that this node has banned.
    pub banned_chunk_producers: Vec<(EpochId, Vec<AccountId>)>,
}

// Different debug requests that can be sent by HTML pages, via GET.
pub enum DebugStatus {
    // Request for the current sync status
    SyncStatus,
    // Request currently tracked shards
    TrackedShards,
    // Detailed information about last couple epochs.
    EpochInfo,
    // Detailed information about last couple blocks.
    BlockStatus(Option<BlockHeight>),
    // Consensus related information.
    ValidatorStatus,
    // Request for the current catchup status
    CatchupStatus,
    // Request for the current state of chain processing (blocks in progress etc).
    ChainProcessingStatus,
    // The state parts already requested.
    RequestedStateParts,
}

impl actix::Message for DebugStatus {
    type Result = Result<DebugStatusResponse, StatusError>;
}

#[derive(serde::Serialize, Debug)]
pub enum DebugStatusResponse {
    SyncStatus(SyncStatusView),
    CatchupStatus(Vec<CatchupStatusView>),
    TrackedShards(TrackedShardsView),
    // List of epochs - in descending order (next epoch is first).
    EpochInfo(Vec<EpochInfoView>),
    // Detailed information about blocks.
    BlockStatus(DebugBlockStatusData),
    // Detailed information about the validator (approvals, block & chunk production etc.)
    ValidatorStatus(ValidatorStatus),
    // Detailed information about chain processing (blocks in progress etc).
    ChainProcessingStatus(ChainProcessingInfo),
    // The state parts already requested.
    RequestedStateParts(Vec<RequestedStatePartsView>),
}

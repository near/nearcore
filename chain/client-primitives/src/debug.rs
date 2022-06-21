// Structs in this file are used for debug purposes, and might change at any time
// without backwards compatibility.

use actix::Message;
use chrono::DateTime;
use near_primitives::{
    hash::CryptoHash, sharding::ChunkHash, types::BlockHeight, views::ValidatorInfo,
};
use serde::{Deserialize, Serialize};

use crate::types::{StatusError, SyncStatus};

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Serialize, Deserialize, Debug)]
pub struct TrackedShardsView {
    pub shards_tracked_this_epoch: Vec<bool>,
    pub shards_tracked_next_epoch: Vec<bool>,
}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Serialize, Deserialize, Debug)]
pub struct EpochInfoView {
    pub epoch_id: CryptoHash,
    pub height: BlockHeight,
    pub first_block: Option<(CryptoHash, DateTime<chrono::Utc>)>,
    pub validators: Vec<ValidatorInfo>,
    pub chunk_only_producers: Vec<String>,
    pub protocol_version: u32,
    pub shards_size_and_parts: Vec<(u64, u64, bool)>,
}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Serialize, Deserialize, Debug)]
pub struct DebugChunkStatus {
    pub shard_id: u64,
    pub chunk_hash: ChunkHash,
    pub chunk_producer: String,
    pub gas_used: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processing_time_ms: Option<u64>,
}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Serialize, Deserialize, Debug)]
pub struct DebugBlockStatus {
    pub block_hash: CryptoHash,
    pub block_height: u64,
    pub block_producer: String,
    pub chunks: Vec<DebugChunkStatus>,
    // Time that was spent processing a given block.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processing_time_ms: Option<u64>,
    // Time between this block and the next one in chain.
    pub timestamp_delta: u64,
    pub gas_price_ratio: f64,
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
    BlockStatus,
}

impl Message for DebugStatus {
    type Result = Result<DebugStatusResponse, StatusError>;
}

#[derive(Serialize, Debug)]
pub enum DebugStatusResponse {
    SyncStatus(SyncStatus),
    TrackedShards(TrackedShardsView),
    // List of epochs - in descending order (next epoch is first).
    EpochInfo(Vec<EpochInfoView>),
    // Detailed information about blocks.
    BlockStatus(Vec<DebugBlockStatus>),
}

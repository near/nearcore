#[cfg(feature = "metric_recorder")]
use near_network::recorder::MetricRecorder;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use actix::Message;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use near_network::types::{AccountOrPeerIdOrHash, KnownProducer};
use near_network::PeerInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ChunkHash;
use near_primitives::types::{
    AccountId, BlockHeight, BlockIdOrFinality, MaybeBlockId, ShardId, StateChangesRequest,
};
use near_primitives::utils::generate_random_string;
use near_primitives::views::{
    BlockView, ChunkView, EpochValidatorInfo, FinalExecutionOutcomeView, GasPriceView,
    LightClientBlockView, QueryRequest, QueryResponse, StateChangesKindsView, StateChangesView,
};
pub use near_primitives::views::{StatusResponse, StatusSyncInfo};

/// Combines errors coming from chain, tx pool and block producer.
#[derive(Debug)]
pub enum Error {
    Chain(near_chain::Error),
    Chunk(near_chunks::Error),
    BlockProducer(String),
    ChunkProducer(String),
    Other(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::Chain(err) => write!(f, "Chain: {}", err),
            Error::Chunk(err) => write!(f, "Chunk: {}", err),
            Error::BlockProducer(err) => write!(f, "Block Producer: {}", err),
            Error::ChunkProducer(err) => write!(f, "Chunk Producer: {}", err),
            Error::Other(err) => write!(f, "Other: {}", err),
        }
    }
}

impl std::error::Error for Error {}

impl From<near_chain::Error> for Error {
    fn from(e: near_chain::Error) -> Self {
        Error::Chain(e)
    }
}

impl From<near_chain::ErrorKind> for Error {
    fn from(e: near_chain::ErrorKind) -> Self {
        let error: near_chain::Error = e.into();
        Error::Chain(error)
    }
}

impl From<near_chunks::Error> for Error {
    fn from(err: near_chunks::Error) -> Self {
        Error::Chunk(err)
    }
}

impl From<String> for Error {
    fn from(e: String) -> Self {
        Error::Other(e)
    }
}

#[derive(Debug)]
pub struct DownloadStatus {
    pub start_time: DateTime<Utc>,
    pub prev_update_time: DateTime<Utc>,
    pub run_me: Arc<AtomicBool>,
    pub error: bool,
    pub done: bool,
    pub state_requests_count: u64,
    pub last_target: Option<AccountOrPeerIdOrHash>,
}

impl Clone for DownloadStatus {
    fn clone(&self) -> Self {
        DownloadStatus {
            start_time: self.start_time,
            prev_update_time: self.prev_update_time,
            run_me: Arc::new(AtomicBool::new(self.run_me.load(Ordering::SeqCst))),
            error: self.error,
            done: self.done,
            state_requests_count: self.state_requests_count,
            last_target: self.last_target.clone(),
        }
    }
}

/// Various status of syncing a specific shard.
#[derive(Clone, Debug)]
pub enum ShardSyncStatus {
    StateDownloadHeader,
    StateDownloadParts,
    StateDownloadFinalize,
    StateDownloadComplete,
}

#[derive(Clone, Debug)]
pub struct ShardSyncDownload {
    pub downloads: Vec<DownloadStatus>,
    pub status: ShardSyncStatus,
}

/// Various status sync can be in, whether it's fast sync or archival.
#[derive(Clone, Debug, strum::AsStaticStr)]
pub enum SyncStatus {
    /// Initial state. Not enough peers to do anything yet. If boolean is false, skip this step.
    AwaitingPeers,
    /// Not syncing / Done syncing.
    NoSync,
    /// Downloading block headers for fast sync.
    HeaderSync { current_height: BlockHeight, highest_height: BlockHeight },
    /// State sync, with different states of state sync for different shards.
    StateSync(CryptoHash, HashMap<ShardId, ShardSyncDownload>),
    /// Sync state across all shards is done.
    StateSyncDone,
    /// Catch up on blocks.
    BodySync { current_height: BlockHeight, highest_height: BlockHeight },
}

impl SyncStatus {
    /// Get a string representation of the status variant
    pub fn as_variant_name(&self) -> &'static str {
        strum::AsStaticRef::as_static(self)
    }

    /// True if currently engaged in syncing the chain.
    pub fn is_syncing(&self) -> bool {
        match self {
            SyncStatus::NoSync => false,
            _ => true,
        }
    }
}

/// Actor message requesting block by id or hash.
pub struct GetBlock(pub BlockIdOrFinality);

impl GetBlock {
    pub fn latest() -> Self {
        Self(BlockIdOrFinality::latest())
    }
}

impl Message for GetBlock {
    type Result = Result<BlockView, String>;
}

/// Actor message requesting a chunk by chunk hash and block hash + shard id.
pub enum GetChunk {
    Height(BlockHeight, ShardId),
    BlockHash(CryptoHash, ShardId),
    ChunkHash(ChunkHash),
}

impl Message for GetChunk {
    type Result = Result<ChunkView, String>;
}

/// Queries client for given path / data.
#[derive(Deserialize, Clone)]
pub struct Query {
    pub query_id: String,
    pub block_id_or_finality: BlockIdOrFinality,
    pub request: QueryRequest,
}

impl Query {
    pub fn new(block_id_or_finality: BlockIdOrFinality, request: QueryRequest) -> Self {
        Query { query_id: generate_random_string(10), block_id_or_finality, request }
    }
}

impl Message for Query {
    type Result = Result<Option<QueryResponse>, String>;
}

pub struct Status {
    pub is_health_check: bool,
}

impl Message for Status {
    type Result = Result<StatusResponse, String>;
}

pub struct GetNextLightClientBlock {
    pub last_block_hash: CryptoHash,
}

impl Message for GetNextLightClientBlock {
    type Result = Result<Option<LightClientBlockView>, String>;
}

pub struct GetNetworkInfo {}

impl Message for GetNetworkInfo {
    type Result = Result<NetworkInfoResponse, String>;
}

pub struct GetGasPrice {
    pub block_id: MaybeBlockId,
}

impl Message for GetGasPrice {
    type Result = Result<GasPriceView, String>;
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NetworkInfoResponse {
    pub active_peers: Vec<PeerInfo>,
    pub num_active_peers: usize,
    pub peer_max_count: u32,
    pub sent_bytes_per_sec: u64,
    pub received_bytes_per_sec: u64,
    /// Accounts of known block and chunk producers from routing table.
    pub known_producers: Vec<KnownProducer>,
    #[cfg(feature = "metric_recorder")]
    pub metric_recorder: MetricRecorder,
}

/// Status of given transaction including all the subsequent receipts.
pub struct TxStatus {
    pub tx_hash: CryptoHash,
    pub signer_account_id: AccountId,
}

impl Message for TxStatus {
    type Result = Result<Option<FinalExecutionOutcomeView>, String>;
}

pub struct GetValidatorInfo {
    pub block_id: MaybeBlockId,
}

impl Message for GetValidatorInfo {
    type Result = Result<EpochValidatorInfo, String>;
}

pub struct GetStateChanges {
    pub block_hash: CryptoHash,
    pub state_changes_request: StateChangesRequest,
}

impl Message for GetStateChanges {
    type Result = Result<StateChangesView, String>;
}

pub struct GetStateChangesInBlock {
    pub block_hash: CryptoHash,
}

impl Message for GetStateChangesInBlock {
    type Result = Result<StateChangesKindsView, String>;
}

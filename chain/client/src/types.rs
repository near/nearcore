use std::cmp::min;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use actix::Message;
use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};

use near_crypto::{InMemorySigner, Signer};
use near_network::PeerInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ChunkHash;
use near_primitives::types::{AccountId, BlockIndex, ShardId, ValidatorId, Version};
use near_primitives::utils::generate_random_string;
use near_primitives::views::{
    BlockView, ChunkView, EpochValidatorInfo, FinalExecutionOutcomeView, LightClientBlockView,
    QueryResponse,
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

#[derive(Clone)]
pub struct ClientConfig {
    /// Version of the binary.
    pub version: Version,
    /// Chain id for status.
    pub chain_id: String,
    /// Listening rpc port for status.
    pub rpc_addr: String,
    /// Duration to check for producing / skipping block.
    pub block_production_tracking_delay: Duration,
    /// Minimum duration before producing block.
    pub min_block_production_delay: Duration,
    /// Maximum wait for approvals before producing block.
    pub max_block_production_delay: Duration,
    /// Maximum duration before skipping given height.
    pub max_block_wait_delay: Duration,
    /// Duration to reduce the wait for each missed block by validator.
    pub reduce_wait_for_missing_block: Duration,
    /// Expected block weight (num of tx, gas, etc).
    pub block_expected_weight: u32,
    /// Skip waiting for sync (for testing or single node testnet).
    pub skip_sync_wait: bool,
    /// How often to check that we are not out of sync.
    pub sync_check_period: Duration,
    /// While syncing, how long to check for each step.
    pub sync_step_period: Duration,
    /// Sync weight threshold: below this difference in weight don't start syncing.
    pub sync_weight_threshold: u128,
    /// Sync height threshold: below this difference in height don't start syncing.
    pub sync_height_threshold: BlockIndex,
    /// Minimum number of peers to start syncing.
    pub min_num_peers: usize,
    /// Period between logging summary information.
    pub log_summary_period: Duration,
    /// Produce empty blocks, use `false` for testing.
    pub produce_empty_blocks: bool,
    /// Epoch length.
    pub epoch_length: BlockIndex,
    /// Total number of block producers
    pub num_block_producers: ValidatorId,
    /// Maximum blocks ahead of us before becoming validators to announce account.
    pub announce_account_horizon: BlockIndex,
    /// Time to persist Accounts Id in the router without removing them.
    pub ttl_account_id_router: Duration,
    /// Horizon at which instead of fetching block, fetch full state.
    pub block_fetch_horizon: BlockIndex,
    /// Horizon to step from the latest block when fetching state.
    pub state_fetch_horizon: BlockIndex,
    /// Time between check to perform catchup.
    pub catchup_step_period: Duration,
    /// Time between checking to re-request chunks.
    pub chunk_request_retry_period: Duration,
    /// Behind this horizon header fetch kicks in.
    pub block_header_fetch_horizon: BlockIndex,
    /// Accounts that this client tracks
    pub tracked_accounts: Vec<AccountId>,
    /// Shards that this client tracks
    pub tracked_shards: Vec<ShardId>,
}

impl ClientConfig {
    pub fn test(
        skip_sync_wait: bool,
        min_block_prod_time: u64,
        max_block_prod_time: u64,
        num_block_producers: ValidatorId,
    ) -> Self {
        ClientConfig {
            version: Default::default(),
            chain_id: "unittest".to_string(),
            rpc_addr: "0.0.0.0:3030".to_string(),
            block_production_tracking_delay: Duration::from_millis(std::cmp::max(
                10,
                min_block_prod_time / 5,
            )),
            min_block_production_delay: Duration::from_millis(min_block_prod_time),
            max_block_production_delay: Duration::from_millis(max_block_prod_time),
            max_block_wait_delay: Duration::from_millis(3 * min_block_prod_time),
            reduce_wait_for_missing_block: Duration::from_millis(0),
            block_expected_weight: 1000,
            skip_sync_wait,
            sync_check_period: Duration::from_millis(100),
            sync_step_period: Duration::from_millis(10),
            sync_weight_threshold: 0,
            sync_height_threshold: 1,
            min_num_peers: 1,
            log_summary_period: Duration::from_secs(10),
            produce_empty_blocks: true,
            epoch_length: 10,
            num_block_producers,
            announce_account_horizon: 5,
            ttl_account_id_router: Duration::from_secs(60 * 60),
            block_fetch_horizon: 50,
            state_fetch_horizon: 5,
            catchup_step_period: Duration::from_millis(min_block_prod_time / 2),
            chunk_request_retry_period: min(
                Duration::from_millis(100),
                Duration::from_millis(min_block_prod_time / 5),
            ),
            block_header_fetch_horizon: 50,
            tracked_accounts: vec![],
            tracked_shards: vec![],
        }
    }
}

/// Required information to produce blocks.
#[derive(Clone)]
pub struct BlockProducer {
    pub account_id: AccountId,
    pub signer: Arc<dyn Signer>,
}

impl From<InMemorySigner> for BlockProducer {
    fn from(signer: InMemorySigner) -> Self {
        BlockProducer { account_id: signer.account_id.clone(), signer: Arc::new(signer) }
    }
}

impl From<Arc<InMemorySigner>> for BlockProducer {
    fn from(signer: Arc<InMemorySigner>) -> Self {
        BlockProducer { account_id: signer.account_id.clone(), signer }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DownloadStatus {
    pub start_time: DateTime<Utc>,
    pub prev_update_time: DateTime<Utc>,
    pub run_me: bool,
    pub error: bool,
    pub done: bool,
}

/// Various status of syncing a specific shard.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShardSyncStatus {
    StateDownloadHeader,
    StateDownloadParts,
    StateDownloadFinalize,
    StateDownloadComplete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardSyncDownload {
    pub downloads: Vec<DownloadStatus>,
    pub status: ShardSyncStatus,
}

/// Various status sync can be in, whether it's fast sync or archival.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SyncStatus {
    /// Initial state. Not enough peers to do anything yet. If boolean is false, skip this step.
    AwaitingPeers,
    /// Not syncing / Done syncing.
    NoSync,
    /// Downloading block headers for fast sync.
    HeaderSync { current_height: BlockIndex, highest_height: BlockIndex },
    /// State sync, with different states of state sync for different shards.
    StateSync(CryptoHash, HashMap<ShardId, ShardSyncDownload>),
    /// Sync state across all shards is done.
    StateSyncDone,
    /// Catch up on blocks.
    BodySync { current_height: BlockIndex, highest_height: BlockIndex },
}

impl SyncStatus {
    /// True if currently engaged in syncing the chain.
    pub fn is_syncing(&self) -> bool {
        self != &SyncStatus::NoSync
    }
}

/// Actor message requesting block by id or hash.
pub enum GetBlock {
    Best,
    Height(BlockIndex),
    Hash(CryptoHash),
}

impl Message for GetBlock {
    type Result = Result<BlockView, String>;
}

/// Actor message requesting a chunk by chunk hash and block hash + shard id.
pub enum GetChunk {
    BlockHeight(BlockIndex, ShardId),
    BlockHash(CryptoHash, ShardId),
    ChunkHash(ChunkHash),
}

impl Message for GetChunk {
    type Result = Result<ChunkView, String>;
}

/// Queries client for given path / data.
#[derive(Clone)]
pub struct Query {
    pub path: String,
    pub data: Vec<u8>,
    pub id: String,
}

impl Query {
    pub fn new(path: String, data: Vec<u8>) -> Self {
        Query { path, data, id: generate_random_string(10) }
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

#[derive(Serialize, Deserialize, Debug)]
pub struct NetworkInfoResponse {
    pub active_peers: Vec<PeerInfo>,
    pub num_active_peers: usize,
    pub peer_max_count: u32,
    pub sent_bytes_per_sec: u64,
    pub received_bytes_per_sec: u64,
    /// Accounts of known block and chunk producers from routing table.
    pub known_producers: Vec<AccountId>,
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
    pub last_block_hash: CryptoHash,
}

impl Message for GetValidatorInfo {
    type Result = Result<EpochValidatorInfo, String>;
}

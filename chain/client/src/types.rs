use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use actix::Message;
use chrono::{DateTime, Utc};

use near_crypto::{InMemorySigner, Signer};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockIndex, ShardId, ValidatorId, Version};
use near_primitives::views::{
    BlockView, FinalTransactionResult, QueryResponse, TransactionResultView,
};
pub use near_primitives::views::{StatusResponse, StatusSyncInfo};

/// Combines errors coming from chain, tx pool and block producer.
#[derive(Debug)]
pub enum Error {
    Chain(near_chain::Error),
    Pool(near_pool::Error),
    BlockProducer(String),
    ChunkProducer(String),
    Other(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::Chain(err) => write!(f, "Chain: {}", err),
            Error::Pool(err) => write!(f, "Pool: {}", err),
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

impl From<near_pool::Error> for Error {
    fn from(e: near_pool::Error) -> Self {
        Error::Pool(e)
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
    /// Expected block weight (num of tx, gas, etc).
    pub block_expected_weight: u32,
    /// Skip waiting for sync (for testing or single node testnet).
    pub skip_sync_wait: bool,
    /// How often to check that we are not out of sync.
    pub sync_check_period: Duration,
    /// While syncing, how long to check for each step.
    pub sync_step_period: Duration,
    /// Sync weight threshold: below this difference in weight don't start syncing.
    pub sync_weight_threshold: u64,
    /// Sync height threshold: below this difference in height don't start syncing.
    pub sync_height_threshold: u64,
    /// Minimum number of peers to start syncing.
    pub min_num_peers: usize,
    /// Period between fetching data from other parts of the system.
    pub fetch_info_period: Duration,
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
    /// Behind this horizon header fetch kicks in.
    pub block_header_fetch_horizon: BlockIndex,
    /// Number of blocks for which a transaction is valid
    pub transaction_validity_period: BlockIndex,
    /// Accounts that this client tracks
    pub tracked_accounts: Vec<AccountId>,
    /// Shards that this client tracks
    pub tracked_shards: Vec<ShardId>,
}

impl ClientConfig {
    pub fn test(
        skip_sync_wait: bool,
        block_prod_time: u64,
        num_block_producers: ValidatorId,
    ) -> Self {
        ClientConfig {
            version: Default::default(),
            chain_id: "unittest".to_string(),
            rpc_addr: "0.0.0.0:3030".to_string(),
            block_production_tracking_delay: Duration::from_millis(std::cmp::max(
                10,
                block_prod_time / 5,
            )),
            min_block_production_delay: Duration::from_millis(block_prod_time),
            max_block_production_delay: Duration::from_millis(2 * block_prod_time),
            max_block_wait_delay: Duration::from_millis(3 * block_prod_time),
            block_expected_weight: 1000,
            skip_sync_wait,
            sync_check_period: Duration::from_millis(100),
            sync_step_period: Duration::from_millis(10),
            sync_weight_threshold: 0,
            sync_height_threshold: 1,
            min_num_peers: 1,
            fetch_info_period: Duration::from_millis(100),
            log_summary_period: Duration::from_secs(10),
            produce_empty_blocks: true,
            epoch_length: 10,
            num_block_producers,
            announce_account_horizon: 5,
            ttl_account_id_router: Duration::from_secs(60 * 60),
            block_fetch_horizon: 50,
            state_fetch_horizon: 5,
            catchup_step_period: Duration::from_millis(block_prod_time / 2),
            block_header_fetch_horizon: 50,
            transaction_validity_period: 100,
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

/// Various status of syncing a specific shard.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShardSyncStatus {
    /// Downloading state for fast sync.
    StateDownload {
        start_time: DateTime<Utc>,
        prev_update_time: DateTime<Utc>,
        prev_downloaded_size: u64,
        downloaded_size: u64,
        total_size: u64,
    },
    /// Validating the full state.
    StateValidation,
    /// Finalizing state sync.
    StateDone,
    /// Syncing errored out, restart.
    Error(String),
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
    StateSync(CryptoHash, HashMap<ShardId, ShardSyncStatus>),
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

/// Queries client for given path / data.
pub struct Query {
    pub path: String,
    pub data: Vec<u8>,
}

impl Message for Query {
    type Result = Result<QueryResponse, String>;
}

pub struct Status {}

impl Message for Status {
    type Result = Result<StatusResponse, String>;
}

/// Status of given transaction including all the subsequent receipts.
pub struct TxStatus {
    pub tx_hash: CryptoHash,
}

impl Message for TxStatus {
    type Result = Result<FinalTransactionResult, String>;
}

/// Details about given transaction.
pub struct TxDetails {
    pub tx_hash: CryptoHash,
}

impl Message for TxDetails {
    type Result = Result<TransactionResultView, String>;
}

use std::sync::Arc;
use std::time::Duration;

use actix::Message;
use chrono::{DateTime, Utc};

use near_chain::Block;
use near_network::types::FullPeerInfo;
use primitives::crypto::signer::{AccountSigner, EDSigner, InMemorySigner};
use primitives::hash::CryptoHash;
use primitives::types::AccountId;

#[derive(Debug)]
pub enum Error {
    Chain(near_chain::Error),
    Pool(near_pool::Error),
    BlockProducer(String),
    Other(String),
}

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

pub struct ClientConfig {
    /// Genesis timestamp. Client will wait until this date to start.
    pub genesis_timestamp: DateTime<Utc>,
    /// Minimum duration before producing block.
    pub min_block_production_delay: Duration,
    /// Maximum duration before producing block or skipping height.
    pub max_block_production_delay: Duration,
    /// Expected block weight (num of tx, gas, etc).
    pub block_expected_weight: u32,
    /// Skip waiting for sync (for testing or single node testnet).
    pub skip_sync_wait: bool,
    /// Sync period.
    pub sync_period: Duration,
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
}

impl ClientConfig {
    pub fn test(skip_sync_wait: bool) -> Self {
        ClientConfig {
            genesis_timestamp: Utc::now(),
            min_block_production_delay: Duration::from_millis(100),
            max_block_production_delay: Duration::from_millis(300),
            block_expected_weight: 1000,
            skip_sync_wait,
            sync_period: Duration::from_millis(100),
            sync_weight_threshold: 0,
            sync_height_threshold: 1,
            min_num_peers: 0,
            fetch_info_period: Duration::from_millis(100),
            log_summary_period: Duration::from_secs(10),
        }
    }
}

impl ClientConfig {
    pub fn new(genesis_timestamp: DateTime<Utc>) -> Self {
        ClientConfig {
            genesis_timestamp,
            min_block_production_delay: Duration::from_millis(100),
            max_block_production_delay: Duration::from_millis(2000),
            block_expected_weight: 1000,
            skip_sync_wait: false,
            sync_period: Duration::from_millis(100),
            sync_weight_threshold: 0,
            sync_height_threshold: 1,
            min_num_peers: 1,
            fetch_info_period: Duration::from_millis(100),
            log_summary_period: Duration::from_secs(10),
        }
    }
}

/// Required information to produce blocks.
pub struct BlockProducer {
    pub account_id: AccountId,
    pub signer: Arc<EDSigner>,
}

impl From<Arc<InMemorySigner>> for BlockProducer {
    fn from(signer: Arc<InMemorySigner>) -> Self {
        BlockProducer { account_id: signer.account_id(), signer }
    }
}

/// Various status sync can be in, whether it's fast sync or archival.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SyncStatus {
    /// Not syncing / Done syncing.
    NoSync,
    /// Not enough peers to do anything yet.
    AwaitingPeers,
    /// Downloading block headers for fast sync.
    HeaderSync,
    /// Downloading state for fasy sync.
    StateDownload,
    /// Validating the full state.
    StateValidation,
    /// Finalizing state sync.
    StateDone,
    /// Catch up on blocks.
    BodySync,
}

impl SyncStatus {
    /// True if currently engaged in syncing the chain.
    pub fn is_syncing(&self) -> bool {
        match self {
            SyncStatus::NoSync | SyncStatus::AwaitingPeers => false,
            _ => true,
        }
    }
}

pub struct NetworkInfo {
    pub num_active_peers: usize,
    pub peer_max_count: u32,
    pub max_weight_peer: Option<FullPeerInfo>,
}

/// Actor message requesting block by id or hash.
pub enum GetBlock {
    Best,
    Hash(CryptoHash),
}

impl Message for GetBlock {
    type Result = Option<Block>;
}

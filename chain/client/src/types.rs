use std::sync::Arc;
use std::time::Duration;

use actix::Message;
use chrono::{DateTime, Utc};

use near_chain::Block;
use near_network::types::FullPeerInfo;
use near_primitives::crypto::signer::{AccountSigner, EDSigner, InMemorySigner};
use near_primitives::hash::CryptoHash;
use near_primitives::rpc::ABCIQueryResponse;
use near_primitives::transaction::FinalTransactionResult;
use near_primitives::types::{AccountId, BlockIndex, MerkleHash};
use serde_derive::{Deserialize, Serialize};

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
    /// Chain id for status.
    pub chain_id: String,
    /// Listening rpc port for status.
    pub rpc_addr: String,
    /// Minimum duration before producing block.
    pub min_block_production_delay: Duration,
    /// Maximum duration before producing block or skipping height.
    pub max_block_production_delay: Duration,
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
}

impl ClientConfig {
    pub fn test(skip_sync_wait: bool) -> Self {
        ClientConfig {
            chain_id: "unittest".to_string(),
            rpc_addr: "0.0.0.0:3030".to_string(),
            min_block_production_delay: Duration::from_millis(100),
            max_block_production_delay: Duration::from_millis(300),
            block_expected_weight: 1000,
            skip_sync_wait,
            sync_check_period: Duration::from_millis(100),
            sync_step_period: Duration::from_millis(10),
            sync_weight_threshold: 0,
            sync_height_threshold: 1,
            min_num_peers: 1,
            fetch_info_period: Duration::from_millis(100),
            log_summary_period: Duration::from_secs(10),
        }
    }
}

impl ClientConfig {
    pub fn new() -> Self {
        ClientConfig {
            chain_id: "test".to_string(),
            rpc_addr: "0.0.0.0:3030".to_string(),
            min_block_production_delay: Duration::from_millis(100),
            max_block_production_delay: Duration::from_millis(2000),
            block_expected_weight: 1000,
            skip_sync_wait: false,
            sync_check_period: Duration::from_secs(10),
            sync_step_period: Duration::from_millis(10),
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
    /// Initial state. Not enough peers to do anything yet. If boolean is false, skip this step.
    AwaitingPeers,
    /// Not syncing / Done syncing.
    NoSync,
    /// Downloading block headers for fast sync.
    HeaderSync { current_height: BlockIndex, highest_height: BlockIndex },
    /// Downloading state for fast sync.
    StateDownload,
    /// Validating the full state.
    StateValidation,
    /// Finalizing state sync.
    StateDone,
    /// Catch up on blocks.
    BodySync { current_height: BlockIndex, highest_height: BlockIndex },
}

impl SyncStatus {
    /// True if currently engaged in syncing the chain.
    pub fn is_syncing(&self) -> bool {
        self != &SyncStatus::NoSync
    }
}

pub struct NetworkInfo {
    pub num_active_peers: usize,
    pub peer_max_count: u32,
    pub most_weight_peers: Vec<FullPeerInfo>,
}

/// Actor message requesting block by id or hash.
pub enum GetBlock {
    Best,
    Height(BlockIndex),
    Hash(CryptoHash),
}

impl Message for GetBlock {
    type Result = Result<Block, String>;
}

/// Queries client for given path / data.
pub struct Query {
    pub path: String,
    pub data: Vec<u8>,
}

impl Message for Query {
    type Result = Result<ABCIQueryResponse, String>;
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StatusSyncInfo {
    pub latest_block_hash: CryptoHash,
    pub latest_block_height: BlockIndex,
    pub latest_state_root: MerkleHash,
    pub latest_block_time: DateTime<Utc>,
    pub syncing: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StatusResponse {
    /// Unique chain id.
    pub chain_id: String,
    /// Address for RPC server.
    pub rpc_addr: String,
    pub sync_info: StatusSyncInfo,
}

pub struct Status {}

impl Message for Status {
    type Result = Result<StatusResponse, String>;
}

pub struct TxStatus {
    pub tx_hash: CryptoHash,
}

impl Message for TxStatus {
    type Result = Result<FinalTransactionResult, String>;
}

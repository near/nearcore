#[cfg(feature = "metric_recorder")]
use near_network::recorder::MetricRecorder;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use actix::Message;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use near_chain_configs::ProtocolConfigView;
use near_network::types::{AccountOrPeerIdOrHash, KnownProducer};
use near_network::PeerInfo;
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{MerklePath, PartialMerkleTree};
use near_primitives::sharding::ChunkHash;
use near_primitives::types::{
    AccountId, BlockHeight, BlockReference, MaybeBlockId, ShardId, TransactionOrReceiptId,
};
use near_primitives::utils::generate_random_string;
use near_primitives::views::{
    BlockView, ChunkView, EpochValidatorInfo, ExecutionOutcomeWithIdView,
    FinalExecutionOutcomeViewEnum, GasPriceView, LightClientBlockLiteView, LightClientBlockView,
    QueryRequest, QueryResponse, ReceiptView, StateChangesKindsView, StateChangesRequestView,
    StateChangesView, ValidatorStakeView,
};
pub use near_primitives::views::{StatusResponse, StatusSyncInfo};

/// Combines errors coming from chain, tx pool and block producer.
#[derive(Debug)]
pub enum Error {
    Chain(near_chain_primitives::Error),
    Chunk(near_chunks::Error),
    BlockProducer(String),
    ChunkProducer(String),
    Other(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

impl From<near_chain_primitives::Error> for Error {
    fn from(e: near_chain_primitives::Error) -> Self {
        Error::Chain(e)
    }
}

impl From<near_chain_primitives::ErrorKind> for Error {
    fn from(e: near_chain_primitives::ErrorKind) -> Self {
        let error: near_chain_primitives::Error = e.into();
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
    /// Initial state. Not enough peers to do anything yet.
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
pub struct GetBlock(pub BlockReference);

#[derive(thiserror::Error, Debug)]
pub enum GetBlockError {
    #[error("IO Error: {0}")]
    IOError(String),
    #[error("Block `{0}` is missing")]
    BlockMissing(CryptoHash),
    #[error("Block not found")]
    BlockNotFound(String),
    #[error("There are no fully synchronized blocks yet")]
    NotSyncedYet,
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {0}")]
    Unreachable(String),
}

impl From<near_chain_primitives::Error> for GetBlockError {
    fn from(error: near_chain_primitives::Error) -> Self {
        match error.kind() {
            near_chain_primitives::ErrorKind::IOErr(s) => Self::IOError(s),
            near_chain_primitives::ErrorKind::DBNotFoundErr(s) => Self::BlockNotFound(s),
            near_chain_primitives::ErrorKind::BlockMissing(hash) => Self::BlockMissing(hash),
            _ => Self::Unreachable(error.to_string()),
        }
    }
}

impl GetBlock {
    pub fn latest() -> Self {
        Self(BlockReference::latest())
    }
}

impl Message for GetBlock {
    type Result = Result<BlockView, GetBlockError>;
}

/// Get block with the block merkle tree. Used for testing
pub struct GetBlockWithMerkleTree(pub BlockReference);

impl GetBlockWithMerkleTree {
    pub fn latest() -> Self {
        Self(BlockReference::latest())
    }
}

impl Message for GetBlockWithMerkleTree {
    type Result = Result<(BlockView, PartialMerkleTree), GetBlockError>;
}

/// Actor message requesting a chunk by chunk hash and block hash + shard id.
pub enum GetChunk {
    Height(BlockHeight, ShardId),
    BlockHash(CryptoHash, ShardId),
    ChunkHash(ChunkHash),
}

impl Message for GetChunk {
    type Result = Result<ChunkView, GetChunkError>;
}

#[derive(thiserror::Error, Debug)]
pub enum GetChunkError {
    #[error("IO Error: {0}")]
    IOError(String),
    #[error("Block has never been observed: {0}")]
    UnknownBlock(String),
    #[error("Block with hash `{0}` is unavailable on the node")]
    UnavailableBlock(CryptoHash),
    #[error("Shard ID {0} is invalid")]
    InvalidShardId(u64),
    #[error("Chunk with hash {0:?} has never been observed on this node")]
    UnknownChunk(ChunkHash),
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {0}")]
    Unreachable(String),
}

impl From<near_chain_primitives::Error> for GetChunkError {
    fn from(error: near_chain_primitives::Error) -> Self {
        match error.kind() {
            near_chain_primitives::ErrorKind::IOErr(s) => Self::IOError(s),
            near_chain_primitives::ErrorKind::DBNotFoundErr(s) => Self::UnknownBlock(s),
            near_chain_primitives::ErrorKind::BlockMissing(hash) => Self::UnavailableBlock(hash),
            near_chain_primitives::ErrorKind::InvalidShardId(shard_id) => {
                Self::InvalidShardId(shard_id)
            }
            near_chain_primitives::ErrorKind::ChunkMissing(hash) => Self::UnknownChunk(hash),
            _ => Self::Unreachable(error.to_string()),
        }
    }
}

/// Queries client for given path / data.
#[derive(Clone, Debug)]
pub struct Query {
    pub query_id: String,
    pub block_reference: BlockReference,
    pub request: QueryRequest,
}

impl Query {
    pub fn new(block_reference: BlockReference, request: QueryRequest) -> Self {
        Query { query_id: generate_random_string(10), block_reference, request }
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
    pub fetch_receipt: bool,
}

#[derive(Debug)]
pub enum TxStatusError {
    ChainError(near_chain_primitives::Error),
    MissingTransaction(CryptoHash),
    InvalidTx(InvalidTxError),
    InternalError,
    TimeoutError,
}

impl From<TxStatusError> for String {
    fn from(error: TxStatusError) -> Self {
        match error {
            TxStatusError::ChainError(err) => format!("Chain error: {}", err),
            TxStatusError::MissingTransaction(tx_hash) => {
                format!("Transaction {} doesn't exist", tx_hash)
            }
            TxStatusError::InternalError => format!("Internal error"),
            TxStatusError::TimeoutError => format!("Timeout error"),
            TxStatusError::InvalidTx(e) => format!("Invalid transaction: {}", e),
        }
    }
}

impl Message for TxStatus {
    type Result = Result<Option<FinalExecutionOutcomeViewEnum>, TxStatusError>;
}

pub struct GetValidatorInfo {
    pub block_id: MaybeBlockId,
}

impl Message for GetValidatorInfo {
    type Result = Result<EpochValidatorInfo, String>;
}

pub struct GetValidatorOrdered {
    pub block_id: MaybeBlockId,
}

impl Message for GetValidatorOrdered {
    type Result = Result<Vec<ValidatorStakeView>, String>;
}

pub struct GetStateChanges {
    pub block_hash: CryptoHash,
    pub state_changes_request: StateChangesRequestView,
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

pub struct GetStateChangesWithCauseInBlock {
    pub block_hash: CryptoHash,
}

impl Message for GetStateChangesWithCauseInBlock {
    type Result = Result<StateChangesView, String>;
}

pub struct GetExecutionOutcome {
    pub id: TransactionOrReceiptId,
}

pub struct GetExecutionOutcomeResponse {
    pub outcome_proof: ExecutionOutcomeWithIdView,
    pub outcome_root_proof: MerklePath,
}

impl Message for GetExecutionOutcome {
    type Result = Result<GetExecutionOutcomeResponse, String>;
}

pub struct GetExecutionOutcomesForBlock {
    pub block_hash: CryptoHash,
}

impl Message for GetExecutionOutcomesForBlock {
    type Result = Result<HashMap<ShardId, Vec<ExecutionOutcomeWithIdView>>, String>;
}

pub struct GetBlockProof {
    pub block_hash: CryptoHash,
    pub head_block_hash: CryptoHash,
}

pub struct GetBlockProofResponse {
    pub block_header_lite: LightClientBlockLiteView,
    pub proof: MerklePath,
}

impl Message for GetBlockProof {
    type Result = Result<GetBlockProofResponse, String>;
}

pub struct GetReceipt {
    pub receipt_id: CryptoHash,
}

#[derive(thiserror::Error, Debug)]
pub enum GetReceiptError {
    #[error("IO Error: {0}")]
    IOError(String),
    #[error("Receipt with id {0} has never been observed on this node")]
    UnknownReceipt(near_primitives::hash::CryptoHash),
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {0}")]
    Unreachable(String),
}

impl From<near_chain_primitives::Error> for GetReceiptError {
    fn from(error: near_chain_primitives::Error) -> Self {
        match error.kind() {
            near_chain_primitives::ErrorKind::IOErr(s) => Self::IOError(s),
            _ => Self::Unreachable(error.to_string()),
        }
    }
}

impl Message for GetReceipt {
    type Result = Result<Option<ReceiptView>, GetReceiptError>;
}

pub struct GetProtocolConfig(pub BlockReference);

impl Message for GetProtocolConfig {
    type Result = Result<ProtocolConfigView, GetProtocolConfigError>;
}

#[derive(thiserror::Error, Debug)]
pub enum GetProtocolConfigError {
    #[error("IO Error: {0}")]
    IOError(String),
    #[error("Block has never been observed: {0}")]
    UnknownBlock(String),
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {0}")]
    Unreachable(String),
}

impl From<near_chain_primitives::Error> for GetProtocolConfigError {
    fn from(error: near_chain_primitives::Error) -> Self {
        match error.kind() {
            near_chain_primitives::ErrorKind::IOErr(s) => Self::IOError(s),
            near_chain_primitives::ErrorKind::DBNotFoundErr(s) => Self::UnknownBlock(s),
            _ => Self::Unreachable(error.to_string()),
        }
    }
}

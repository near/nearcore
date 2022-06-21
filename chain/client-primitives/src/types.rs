use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use actix::Message;
use chrono::DateTime;
use near_primitives::time::Utc;

use near_chain_configs::ProtocolConfigView;
use near_network_primitives::types::{AccountOrPeerIdOrHash, KnownProducer, PeerInfo};
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{MerklePath, PartialMerkleTree};
use near_primitives::sharding::ChunkHash;
use near_primitives::types::{
    AccountId, BlockHeight, BlockReference, EpochId, EpochReference, MaybeBlockId, ShardId,
    TransactionOrReceiptId,
};
use near_primitives::utils::generate_random_string;
use near_primitives::views::validator_stake_view::ValidatorStakeView;
use near_primitives::views::{
    BlockView, ChunkView, DebugBlockStatus, EpochInfoView, EpochValidatorInfo,
    ExecutionOutcomeWithIdView, FinalExecutionOutcomeViewEnum, GasPriceView,
    LightClientBlockLiteView, LightClientBlockView, QueryRequest, QueryResponse, ReceiptView,
    StateChangesKindsView, StateChangesRequestView, StateChangesView, TrackedShardsView,
};
pub use near_primitives::views::{StatusResponse, StatusSyncInfo};
use serde::Serialize;

/// Combines errors coming from chain, tx pool and block producer.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Chain: {0}")]
    Chain(#[from] near_chain_primitives::Error),
    #[error("Chunk: {0}")]
    Chunk(#[from] near_chunks_primitives::Error),
    #[error("Block Producer: {0}")]
    BlockProducer(String),
    #[error("Chunk Producer: {0}")]
    ChunkProducer(String),
    #[error("Other: {0}")]
    Other(String),
}

#[derive(Debug, Serialize)]
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
#[derive(Clone, Debug, Serialize)]
pub enum ShardSyncStatus {
    StateDownloadHeader,
    StateDownloadParts,
    StateDownloadScheduling,
    StateDownloadApplying,
    StateDownloadComplete,
    StateSplitScheduling,
    StateSplitApplying,
    StateSyncDone,
}

#[derive(Clone, Debug, Serialize)]
pub struct ShardSyncDownload {
    pub downloads: Vec<DownloadStatus>,
    pub status: ShardSyncStatus,
}

/// Various status sync can be in, whether it's fast sync or archival.
#[derive(Clone, Debug, strum::AsRefStr, Serialize)]
pub enum SyncStatus {
    /// Initial state. Not enough peers to do anything yet.
    AwaitingPeers,
    /// Not syncing / Done syncing.
    NoSync,
    /// Syncing using light-client headers to a recent epoch
    // TODO #3488
    // Bowen: why do we use epoch ordinal instead of epoch id?
    EpochSync { epoch_ord: u64 },
    /// Downloading block headers for fast sync.
    HeaderSync {
        start_height: BlockHeight,
        current_height: BlockHeight,
        highest_height: BlockHeight,
    },
    /// State sync, with different states of state sync for different shards.
    StateSync(CryptoHash, HashMap<ShardId, ShardSyncDownload>),
    /// Sync state across all shards is done.
    StateSyncDone,
    /// Catch up on blocks.
    BodySync { start_height: BlockHeight, current_height: BlockHeight, highest_height: BlockHeight },
}

impl SyncStatus {
    /// Get a string representation of the status variant
    pub fn as_variant_name(&self) -> &str {
        self.as_ref()
    }

    /// True if currently engaged in syncing the chain.
    pub fn is_syncing(&self) -> bool {
        match self {
            SyncStatus::NoSync => false,
            _ => true,
        }
    }

    pub fn repr(&self) -> u8 {
        match self {
            // Represent NoSync as 0 because it is the state of a normal well-behaving node.
            SyncStatus::NoSync => 0,
            SyncStatus::AwaitingPeers => 1,
            SyncStatus::EpochSync { epoch_ord: _ } => 2,
            SyncStatus::HeaderSync { start_height: _, current_height: _, highest_height: _ } => 3,
            SyncStatus::StateSync(_, _) => 4,
            SyncStatus::StateSyncDone => 5,
            SyncStatus::BodySync { start_height: _, current_height: _, highest_height: _ } => 6,
        }
    }

    pub fn start_height(&self) -> Option<BlockHeight> {
        match self {
            SyncStatus::HeaderSync { start_height, .. } => Some(*start_height),
            SyncStatus::BodySync { start_height, .. } => Some(*start_height),
            _ => None,
        }
    }
}

/// Actor message requesting block by id, hash or sync state.
pub struct GetBlock(pub BlockReference);

#[derive(thiserror::Error, Debug)]
pub enum GetBlockError {
    #[error("IO Error: {error_message}")]
    IOError { error_message: String },
    #[error("Block either has never been observed on the node or has been garbage collected: {error_message}")]
    UnknownBlock { error_message: String },
    #[error("There are no fully synchronized blocks yet")]
    NotSyncedYet,
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}")]
    Unreachable { error_message: String },
}

impl From<near_chain_primitives::Error> for GetBlockError {
    fn from(error: near_chain_primitives::Error) -> Self {
        match error {
            near_chain_primitives::Error::IOErr(error) => {
                Self::IOError { error_message: error.to_string() }
            }
            near_chain_primitives::Error::DBNotFoundErr(error_message) => {
                Self::UnknownBlock { error_message }
            }
            _ => Self::Unreachable { error_message: error.to_string() },
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

/// Actor message requesting block hash by id, hash or sync state.
pub struct GetBlockHash(pub BlockReference);

impl Message for GetBlockHash {
    type Result = Result<CryptoHash, GetBlockError>;
}

/// Get block with the block merkle tree. Used for testing
pub struct GetBlockWithMerkleTree(pub BlockReference);

impl GetBlockWithMerkleTree {
    pub fn latest() -> Self {
        Self(BlockReference::latest())
    }
}

impl Message for GetBlockWithMerkleTree {
    type Result = Result<(BlockView, Arc<PartialMerkleTree>), GetBlockError>;
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
    #[error("IO Error: {error_message}")]
    IOError { error_message: String },
    #[error("Block either has never been observed on the node or has been garbage collected: {error_message}")]
    UnknownBlock { error_message: String },
    #[error("Shard ID {shard_id} is invalid")]
    InvalidShardId { shard_id: u64 },
    #[error("Chunk with hash {chunk_hash:?} has never been observed on this node")]
    UnknownChunk { chunk_hash: ChunkHash },
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}")]
    Unreachable { error_message: String },
}

impl From<near_chain_primitives::Error> for GetChunkError {
    fn from(error: near_chain_primitives::Error) -> Self {
        match error {
            near_chain_primitives::Error::IOErr(error) => {
                Self::IOError { error_message: error.to_string() }
            }
            near_chain_primitives::Error::DBNotFoundErr(error_message) => {
                Self::UnknownBlock { error_message }
            }
            near_chain_primitives::Error::InvalidShardId(shard_id) => {
                Self::InvalidShardId { shard_id }
            }
            near_chain_primitives::Error::ChunkMissing(chunk_hash) => {
                Self::UnknownChunk { chunk_hash }
            }
            _ => Self::Unreachable { error_message: error.to_string() },
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
    type Result = Result<QueryResponse, QueryError>;
}

#[derive(thiserror::Error, Debug)]
pub enum QueryError {
    #[error("There are no fully synchronized blocks on the node yet")]
    NoSyncedBlocks,
    #[error("The node does not track the shard ID {requested_shard_id}")]
    UnavailableShard { requested_shard_id: near_primitives::types::ShardId },
    #[error("Account ID {requested_account_id} is invalid")]
    InvalidAccount {
        requested_account_id: near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error(
        "Account {requested_account_id} does not exist while viewing at block #{block_height}"
    )]
    UnknownAccount {
        requested_account_id: near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error(
        "Contract code for contract ID {contract_account_id} has never been observed on the node at block #{block_height}"
    )]
    NoContractCode {
        contract_account_id: near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error("State of contract {contract_account_id} is too large to be viewed")]
    TooLargeContractState {
        contract_account_id: near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error("Access key for public key {public_key} has never been observed on the node at block #{block_height}")]
    UnknownAccessKey {
        public_key: near_crypto::PublicKey,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error("Function call returned an error: {vm_error}")]
    ContractExecutionError {
        vm_error: String,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
    #[error(
        "The data for block #{block_height} is garbage collected on this node, use an archival node to fetch historical data"
    )]
    GarbageCollectedBlock {
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error("Block either has never been observed on the node or has been garbage collected: {block_reference:?}")]
    UnknownBlock { block_reference: near_primitives::types::BlockReference },
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}")]
    Unreachable { error_message: String },
}

pub struct Status {
    pub is_health_check: bool,
    // If true - return more detailed information about the current status (recent blocks etc).
    pub detailed: bool,
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

#[derive(thiserror::Error, Debug)]
pub enum StatusError {
    #[error("Node is syncing")]
    NodeIsSyncing,
    #[error("No blocks for {elapsed:?}")]
    NoNewBlocks { elapsed: std::time::Duration },
    #[error("Epoch Out Of Bounds {epoch_id:?}")]
    EpochOutOfBounds { epoch_id: near_primitives::types::EpochId },
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}")]
    Unreachable { error_message: String },
}

impl From<near_chain_primitives::error::Error> for StatusError {
    fn from(error: near_chain_primitives::error::Error) -> Self {
        match error {
            near_chain_primitives::error::Error::IOErr(error) => {
                Self::InternalError { error_message: error.to_string() }
            }
            near_chain_primitives::error::Error::DBNotFoundErr(error_message)
            | near_chain_primitives::error::Error::ValidatorError(error_message) => {
                Self::InternalError { error_message }
            }
            near_chain_primitives::error::Error::EpochOutOfBounds(epoch_id) => {
                Self::EpochOutOfBounds { epoch_id }
            }
            _ => Self::Unreachable { error_message: error.to_string() },
        }
    }
}

impl Message for Status {
    type Result = Result<StatusResponse, StatusError>;
}

pub struct GetNextLightClientBlock {
    pub last_block_hash: CryptoHash,
}

#[derive(thiserror::Error, Debug)]
pub enum GetNextLightClientBlockError {
    #[error("Internal error: {error_message}")]
    InternalError { error_message: String },
    #[error("Block either has never been observed on the node or has been garbage collected: {error_message}")]
    UnknownBlock { error_message: String },
    #[error("Epoch Out Of Bounds {epoch_id:?}")]
    EpochOutOfBounds { epoch_id: near_primitives::types::EpochId },
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}")]
    Unreachable { error_message: String },
}

impl From<near_chain_primitives::error::Error> for GetNextLightClientBlockError {
    fn from(error: near_chain_primitives::error::Error) -> Self {
        match error {
            near_chain_primitives::error::Error::DBNotFoundErr(error_message) => {
                Self::UnknownBlock { error_message }
            }
            near_chain_primitives::error::Error::IOErr(error) => {
                Self::InternalError { error_message: error.to_string() }
            }
            near_chain_primitives::error::Error::EpochOutOfBounds(epoch_id) => {
                Self::EpochOutOfBounds { epoch_id }
            }
            _ => Self::Unreachable { error_message: error.to_string() },
        }
    }
}

impl Message for GetNextLightClientBlock {
    type Result = Result<Option<Arc<LightClientBlockView>>, GetNextLightClientBlockError>;
}

pub struct GetNetworkInfo {}

impl Message for GetNetworkInfo {
    type Result = Result<NetworkInfoResponse, String>;
}

pub struct GetGasPrice {
    pub block_id: MaybeBlockId,
}

impl Message for GetGasPrice {
    type Result = Result<GasPriceView, GetGasPriceError>;
}

#[derive(thiserror::Error, Debug)]
pub enum GetGasPriceError {
    #[error("Internal error: {error_message}")]
    InternalError { error_message: String },
    #[error("Block either has never been observed on the node or has been garbage collected: {error_message}")]
    UnknownBlock { error_message: String },
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}")]
    Unreachable { error_message: String },
}

impl From<near_chain_primitives::Error> for GetGasPriceError {
    fn from(error: near_chain_primitives::Error) -> Self {
        match error {
            near_chain_primitives::Error::IOErr(error) => {
                Self::InternalError { error_message: error.to_string() }
            }
            near_chain_primitives::Error::DBNotFoundErr(error_message) => {
                Self::UnknownBlock { error_message }
            }
            _ => Self::Unreachable { error_message: error.to_string() },
        }
    }
}

#[derive(Debug)]
pub struct NetworkInfoResponse {
    pub connected_peers: Vec<PeerInfo>,
    pub num_connected_peers: usize,
    pub peer_max_count: u32,
    pub sent_bytes_per_sec: u64,
    pub received_bytes_per_sec: u64,
    /// Accounts of known block and chunk producers from routing table.
    pub known_producers: Vec<KnownProducer>,
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
    InternalError(String),
    TimeoutError,
}

impl From<TxStatusError> for String {
    fn from(error: TxStatusError) -> Self {
        match error {
            TxStatusError::ChainError(err) => format!("Chain error: {}", err),
            TxStatusError::MissingTransaction(tx_hash) => {
                format!("Transaction {} doesn't exist", tx_hash)
            }
            TxStatusError::InternalError(debug_message) => {
                format!("Internal error: {}", debug_message)
            }
            TxStatusError::TimeoutError => format!("Timeout error"),
            TxStatusError::InvalidTx(e) => format!("Invalid transaction: {}", e),
        }
    }
}

impl Message for TxStatus {
    type Result = Result<Option<FinalExecutionOutcomeViewEnum>, TxStatusError>;
}

pub struct GetValidatorInfo {
    pub epoch_reference: EpochReference,
}

impl Message for GetValidatorInfo {
    type Result = Result<EpochValidatorInfo, GetValidatorInfoError>;
}

#[derive(thiserror::Error, Debug)]
pub enum GetValidatorInfoError {
    #[error("IO Error: {0}")]
    IOError(String),
    #[error("Unknown epoch")]
    UnknownEpoch,
    #[error("Validator info unavailable")]
    ValidatorInfoUnavailable,
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {0}")]
    Unreachable(String),
}

impl From<near_chain_primitives::Error> for GetValidatorInfoError {
    fn from(error: near_chain_primitives::Error) -> Self {
        match error {
            near_chain_primitives::Error::DBNotFoundErr(_)
            | near_chain_primitives::Error::EpochOutOfBounds(_) => Self::UnknownEpoch,
            near_chain_primitives::Error::IOErr(s) => Self::IOError(s.to_string()),
            _ => Self::Unreachable(error.to_string()),
        }
    }
}

pub struct GetValidatorOrdered {
    pub block_id: MaybeBlockId,
}

impl Message for GetValidatorOrdered {
    type Result = Result<Vec<ValidatorStakeView>, GetValidatorInfoError>;
}

pub struct GetStateChanges {
    pub block_hash: CryptoHash,
    pub state_changes_request: StateChangesRequestView,
}

#[derive(thiserror::Error, Debug)]
pub enum GetStateChangesError {
    #[error("IO Error: {error_message}")]
    IOError { error_message: String },
    #[error("Block either has never been observed on the node or has been garbage collected: {error_message}")]
    UnknownBlock { error_message: String },
    #[error("There are no fully synchronized blocks yet")]
    NotSyncedYet,
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}")]
    Unreachable { error_message: String },
}

impl From<near_chain_primitives::Error> for GetStateChangesError {
    fn from(error: near_chain_primitives::Error) -> Self {
        match error {
            near_chain_primitives::Error::IOErr(error) => {
                Self::IOError { error_message: error.to_string() }
            }
            near_chain_primitives::Error::DBNotFoundErr(error_message) => {
                Self::UnknownBlock { error_message }
            }
            _ => Self::Unreachable { error_message: error.to_string() },
        }
    }
}

impl Message for GetStateChanges {
    type Result = Result<StateChangesView, GetStateChangesError>;
}

pub struct GetStateChangesInBlock {
    pub block_hash: CryptoHash,
}

impl Message for GetStateChangesInBlock {
    type Result = Result<StateChangesKindsView, GetStateChangesError>;
}

pub struct GetStateChangesWithCauseInBlock {
    pub block_hash: CryptoHash,
}

impl Message for GetStateChangesWithCauseInBlock {
    type Result = Result<StateChangesView, GetStateChangesError>;
}

pub struct GetStateChangesWithCauseInBlockForTrackedShards {
    pub block_hash: CryptoHash,
    pub epoch_id: EpochId,
}

impl Message for GetStateChangesWithCauseInBlockForTrackedShards {
    type Result = Result<HashMap<ShardId, StateChangesView>, GetStateChangesError>;
}

pub struct GetExecutionOutcome {
    pub id: TransactionOrReceiptId,
}

#[derive(thiserror::Error, Debug)]
pub enum GetExecutionOutcomeError {
    #[error("Block either has never been observed on the node or has been garbage collected: {error_message}")]
    UnknownBlock { error_message: String },
    #[error("Inconsistent state. Total number of shards is {number_or_shards} but the execution outcome is in shard {execution_outcome_shard_id}")]
    InconsistentState {
        number_or_shards: usize,
        execution_outcome_shard_id: near_primitives::types::ShardId,
    },
    #[error("{transaction_or_receipt_id} has not been confirmed")]
    NotConfirmed { transaction_or_receipt_id: near_primitives::hash::CryptoHash },
    #[error("{transaction_or_receipt_id} does not exist")]
    UnknownTransactionOrReceipt { transaction_or_receipt_id: near_primitives::hash::CryptoHash },
    #[error("Node doesn't track the shard where {transaction_or_receipt_id} is executed")]
    UnavailableShard {
        transaction_or_receipt_id: near_primitives::hash::CryptoHash,
        shard_id: near_primitives::types::ShardId,
    },
    #[error("Internal error: {error_message}")]
    InternalError { error_message: String },
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}")]
    Unreachable { error_message: String },
}

impl From<TxStatusError> for GetExecutionOutcomeError {
    fn from(error: TxStatusError) -> Self {
        match error {
            TxStatusError::ChainError(err) => {
                Self::InternalError { error_message: err.to_string() }
            }
            _ => Self::Unreachable { error_message: format!("{:?}", error) },
        }
    }
}

impl From<near_chain_primitives::error::Error> for GetExecutionOutcomeError {
    fn from(error: near_chain_primitives::error::Error) -> Self {
        match error {
            near_chain_primitives::Error::IOErr(error) => {
                Self::InternalError { error_message: error.to_string() }
            }
            near_chain_primitives::Error::DBNotFoundErr(error_message) => {
                Self::UnknownBlock { error_message }
            }
            _ => Self::Unreachable { error_message: error.to_string() },
        }
    }
}

pub struct GetExecutionOutcomeResponse {
    pub outcome_proof: ExecutionOutcomeWithIdView,
    pub outcome_root_proof: MerklePath,
}

impl Message for GetExecutionOutcome {
    type Result = Result<GetExecutionOutcomeResponse, GetExecutionOutcomeError>;
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

#[derive(thiserror::Error, Debug)]
pub enum GetBlockProofError {
    #[error("Block either has never been observed on the node or has been garbage collected: {error_message}")]
    UnknownBlock { error_message: String },
    #[error("Internal error: {error_message}")]
    InternalError { error_message: String },
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}")]
    Unreachable { error_message: String },
}

impl From<near_chain_primitives::error::Error> for GetBlockProofError {
    fn from(error: near_chain_primitives::error::Error) -> Self {
        match error {
            near_chain_primitives::error::Error::DBNotFoundErr(error_message) => {
                Self::UnknownBlock { error_message }
            }
            near_chain_primitives::error::Error::Other(error_message) => {
                Self::InternalError { error_message }
            }
            err => Self::Unreachable { error_message: err.to_string() },
        }
    }
}

impl Message for GetBlockProof {
    type Result = Result<GetBlockProofResponse, GetBlockProofError>;
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
        match error {
            near_chain_primitives::Error::IOErr(error) => Self::IOError(error.to_string()),
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
        match error {
            near_chain_primitives::Error::IOErr(error) => Self::IOError(error.to_string()),
            near_chain_primitives::Error::DBNotFoundErr(s) => Self::UnknownBlock(s),
            _ => Self::Unreachable(error.to_string()),
        }
    }
}

#[cfg(feature = "sandbox")]
#[derive(Debug)]
pub enum SandboxMessage {
    SandboxPatchState(Vec<near_primitives::state_record::StateRecord>),
    SandboxPatchStateStatus,
    SandboxFastForward(near_primitives::types::BlockHeightDelta),
    SandboxFastForwardStatus,
}

#[cfg(feature = "sandbox")]
#[derive(Eq, PartialEq, Debug, actix::MessageResponse)]
pub enum SandboxResponse {
    SandboxPatchStateFinished(bool),
    SandboxFastForwardFinished(bool),
    SandboxFastForwardFailed(String),
    SandboxNoResponse,
}
#[cfg(feature = "sandbox")]
impl Message for SandboxMessage {
    type Result = SandboxResponse;
}

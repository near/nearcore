use actix::Message;
use near_chain_configs::{ClientConfig, ProtocolConfigView};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{MerklePath, PartialMerkleTree};
use near_primitives::network::PeerId;
use near_primitives::sharding::{ChunkHash, ShardChunk};
use near_primitives::types::{
    AccountId, BlockHeight, BlockReference, EpochId, EpochReference, MaybeBlockId, ShardId,
    TransactionOrReceiptId,
};
use near_primitives::views::validator_stake_view::ValidatorStakeView;
use near_primitives::views::{
    BlockView, ChunkView, EpochValidatorInfo, ExecutionOutcomeWithIdView, GasPriceView,
    LightClientBlockLiteView, LightClientBlockView, MaintenanceWindowsView, QueryRequest,
    QueryResponse, ReceiptView, SplitStorageInfoView, StateChangesKindsView,
    StateChangesRequestView, StateChangesView, StateSyncStatusView, SyncStatusView, TxStatusView,
};
pub use near_primitives::views::{StatusResponse, StatusSyncInfo};
use near_time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug_span;

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

impl From<near_primitives::errors::EpochError> for Error {
    fn from(err: near_primitives::errors::EpochError) -> Self {
        Error::Chain(err.into())
    }
}

/// Various status of syncing a specific shard.
#[derive(Clone, Debug, Copy)]
pub enum ShardSyncStatus {
    StateDownloadHeader,
    StateDownloadParts,
    StateApplyScheduling,
    StateApplyInProgress,
    StateApplyFinalizing,
    StateSyncDone,
}

impl ShardSyncStatus {
    pub fn repr(&self) -> u8 {
        match self {
            ShardSyncStatus::StateDownloadHeader => 0,
            ShardSyncStatus::StateDownloadParts => 1,
            ShardSyncStatus::StateApplyScheduling => 2,
            ShardSyncStatus::StateApplyInProgress => 3,
            ShardSyncStatus::StateApplyFinalizing => 4,
            ShardSyncStatus::StateSyncDone => 5,
        }
    }
}

/// Manually implement compare for ShardSyncStatus to compare only based on variant name
impl PartialEq<Self> for ShardSyncStatus {
    fn eq(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

impl Eq for ShardSyncStatus {}

impl ToString for ShardSyncStatus {
    fn to_string(&self) -> String {
        match self {
            ShardSyncStatus::StateDownloadHeader => "header".to_string(),
            ShardSyncStatus::StateDownloadParts => "parts".to_string(),
            ShardSyncStatus::StateApplyScheduling => "apply scheduling".to_string(),
            ShardSyncStatus::StateApplyInProgress => "apply in progress".to_string(),
            ShardSyncStatus::StateApplyFinalizing => "apply finalizing".to_string(),
            ShardSyncStatus::StateSyncDone => "done".to_string(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct StateSyncStatus {
    pub sync_hash: CryptoHash,
    pub sync_status: HashMap<ShardId, ShardSyncStatus>,
    pub download_tasks: Vec<String>,
    pub computation_tasks: Vec<String>,
}

impl StateSyncStatus {
    pub fn new(sync_hash: CryptoHash) -> Self {
        Self {
            sync_hash,
            sync_status: HashMap::new(),
            download_tasks: Vec::new(),
            computation_tasks: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct EpochSyncStatus {
    pub source_peer_height: BlockHeight,
    pub source_peer_id: PeerId,
    pub attempt_time: near_time::Utc,
}

/// Various status sync can be in, whether it's fast sync or archival.
#[derive(Clone, Debug, strum::AsRefStr)]
pub enum SyncStatus {
    /// Initial state. Not enough peers to do anything yet.
    AwaitingPeers,
    /// Not syncing / Done syncing.
    NoSync,
    /// Syncing using light-client headers to a recent epoch
    EpochSync(EpochSyncStatus),
    EpochSyncDone,
    /// Downloading block headers for fast sync.
    HeaderSync {
        /// Head height at the beginning. Not the header head height!
        /// Used only for reporting the progress of the sync.
        start_height: BlockHeight,
        /// Current header head height.
        current_height: BlockHeight,
        /// Highest height of our peers.
        highest_height: BlockHeight,
    },
    /// State sync, with different states of state sync for different shards.
    StateSync(StateSyncStatus),
    /// Sync state across all shards is done.
    StateSyncDone,
    /// Download and process blocks until the head reaches the head of the network.
    BlockSync {
        /// Header head height at the beginning.
        /// Used only for reporting the progress of the sync.
        start_height: BlockHeight,
        /// Current head height.
        current_height: BlockHeight,
        /// Highest height of our peers.
        highest_height: BlockHeight,
    },
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
            SyncStatus::EpochSync { .. } => 2,
            SyncStatus::EpochSyncDone { .. } => 3,
            SyncStatus::HeaderSync { .. } => 4,
            SyncStatus::StateSync(_) => 5,
            SyncStatus::StateSyncDone => 6,
            SyncStatus::BlockSync { .. } => 7,
        }
    }

    pub fn start_height(&self) -> Option<BlockHeight> {
        match self {
            SyncStatus::HeaderSync { start_height, .. } => Some(*start_height),
            SyncStatus::BlockSync { start_height, .. } => Some(*start_height),
            _ => None,
        }
    }

    pub fn update(&mut self, new_value: Self) {
        let _span =
            debug_span!(target: "sync", "update_sync_status", old_value = ?self, ?new_value)
                .entered();
        *self = new_value;
    }
}

impl From<SyncStatus> for SyncStatusView {
    fn from(status: SyncStatus) -> Self {
        match status {
            SyncStatus::AwaitingPeers => SyncStatusView::AwaitingPeers,
            SyncStatus::NoSync => SyncStatusView::NoSync,
            SyncStatus::EpochSync(status) => SyncStatusView::EpochSync {
                source_peer_height: status.source_peer_height,
                source_peer_id: status.source_peer_id.to_string(),
                attempt_time: status.attempt_time.to_string(),
            },
            SyncStatus::EpochSyncDone => SyncStatusView::EpochSyncDone,
            SyncStatus::HeaderSync { start_height, current_height, highest_height } => {
                SyncStatusView::HeaderSync { start_height, current_height, highest_height }
            }
            SyncStatus::StateSync(state_sync_status) => {
                SyncStatusView::StateSync(StateSyncStatusView {
                    sync_hash: state_sync_status.sync_hash,
                    shard_sync_status: state_sync_status
                        .sync_status
                        .iter()
                        .map(|(shard_id, shard_sync_status)| {
                            (*shard_id, shard_sync_status.to_string())
                        })
                        .collect(),
                    download_tasks: state_sync_status.download_tasks,
                    computation_tasks: state_sync_status.computation_tasks,
                })
            }
            SyncStatus::StateSyncDone => SyncStatusView::StateSyncDone,
            SyncStatus::BlockSync { start_height, current_height, highest_height } => {
                SyncStatusView::BlockSync { start_height, current_height, highest_height }
            }
        }
    }
}

/// Actor message requesting block by id, hash or sync state.
#[derive(Clone, Debug)]
pub struct GetBlock(pub BlockReference);

#[derive(thiserror::Error, Debug)]
pub enum GetBlockError {
    #[error("IO Error: {error_message}")]
    IOError { error_message: String },
    #[error(
        "Block either has never been observed on the node or has been garbage collected: {error_message}"
    )]
    UnknownBlock { error_message: String },
    #[error("There are no fully synchronized blocks yet")]
    NotSyncedYet,
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error(
        "It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}"
    )]
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

/// Get block with the block merkle tree. Used for testing
#[derive(Debug)]
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
#[derive(Clone, Debug)]
pub enum GetChunk {
    Height(BlockHeight, ShardId),
    BlockHash(CryptoHash, ShardId),
    ChunkHash(ChunkHash),
}

impl Message for GetChunk {
    type Result = Result<ChunkView, GetChunkError>;
}

/// Actor message requesting a chunk by chunk hash and block hash + shard id.
/// The difference between this and `GetChunk` is that it returns the actual `ShardChunk`
/// instead of a `ChunkView`
#[derive(Debug)]
pub enum GetShardChunk {
    Height(BlockHeight, ShardId),
    BlockHash(CryptoHash, ShardId),
    ChunkHash(ChunkHash),
}

impl Message for GetShardChunk {
    type Result = Result<ShardChunk, GetChunkError>;
}

#[derive(thiserror::Error, Debug)]
pub enum GetChunkError {
    #[error("IO Error: {error_message}")]
    IOError { error_message: String },
    #[error(
        "Block either has never been observed on the node or has been garbage collected: {error_message}"
    )]
    UnknownBlock { error_message: String },
    #[error("Shard ID {shard_id} is invalid")]
    InvalidShardId { shard_id: ShardId },
    #[error("Chunk with hash {chunk_hash:?} has never been observed on this node")]
    UnknownChunk { chunk_hash: ChunkHash },
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error(
        "It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}"
    )]
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
    pub block_reference: BlockReference,
    pub request: QueryRequest,
}

impl Query {
    pub fn new(block_reference: BlockReference, request: QueryRequest) -> Self {
        Query { block_reference, request }
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
    #[error("Account {requested_account_id} does not exist while viewing at block #{block_height}")]
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
    #[error(
        "Access key for public key {public_key} has never been observed on the node at block #{block_height}"
    )]
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
    #[error(
        "Block either has never been observed on the node or has been garbage collected: {block_reference:?}"
    )]
    UnknownBlock { block_reference: near_primitives::types::BlockReference },
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error(
        "It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}"
    )]
    Unreachable { error_message: String },
}

#[derive(Debug)]
pub struct Status {
    pub is_health_check: bool,
    // If true - return more detailed information about the current status (recent blocks etc).
    pub detailed: bool,
}

#[derive(thiserror::Error, Debug)]
pub enum StatusError {
    #[error("Node is syncing")]
    NodeIsSyncing,
    #[error("No blocks for {elapsed:?}")]
    NoNewBlocks { elapsed: Duration },
    #[error("Epoch Out Of Bounds {epoch_id:?}")]
    EpochOutOfBounds { epoch_id: near_primitives::types::EpochId },
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error(
        "It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}"
    )]
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

#[derive(Debug)]
pub struct GetNextLightClientBlock {
    pub last_block_hash: CryptoHash,
}

#[derive(thiserror::Error, Debug)]
pub enum GetNextLightClientBlockError {
    #[error("Internal error: {error_message}")]
    InternalError { error_message: String },
    #[error(
        "Block either has never been observed on the node or has been garbage collected: {error_message}"
    )]
    UnknownBlock { error_message: String },
    #[error("Epoch Out Of Bounds {epoch_id:?}")]
    EpochOutOfBounds { epoch_id: near_primitives::types::EpochId },
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error(
        "It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}"
    )]
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

#[derive(Debug)]
pub struct GetNetworkInfo {}

impl Message for GetNetworkInfo {
    type Result = Result<NetworkInfoResponse, String>;
}

#[derive(Debug)]
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
    #[error(
        "Block either has never been observed on the node or has been garbage collected: {error_message}"
    )]
    UnknownBlock { error_message: String },
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error(
        "It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}"
    )]
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

#[derive(Clone, Debug)]
pub struct PeerInfo {
    pub id: PeerId,
    pub addr: Option<std::net::SocketAddr>,
    pub account_id: Option<AccountId>,
}

#[derive(Clone, Debug)]
pub struct KnownProducer {
    pub account_id: AccountId,
    pub addr: Option<std::net::SocketAddr>,
    pub peer_id: PeerId,
    pub next_hops: Option<Vec<PeerId>>,
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
#[derive(Debug)]
pub struct TxStatus {
    pub tx_hash: CryptoHash,
    pub signer_account_id: AccountId,
    pub fetch_receipt: bool,
}

#[derive(Debug)]
pub enum TxStatusError {
    ChainError(near_chain_primitives::Error),
    MissingTransaction(CryptoHash),
    InternalError(String),
    TimeoutError,
}

impl From<near_chain_primitives::Error> for TxStatusError {
    fn from(error: near_chain_primitives::Error) -> Self {
        Self::ChainError(error)
    }
}

impl Message for TxStatus {
    type Result = Result<TxStatusView, TxStatusError>;
}

#[derive(Debug)]
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
    #[error(
        "It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {0}"
    )]
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

#[derive(Debug)]
pub struct GetValidatorOrdered {
    pub block_id: MaybeBlockId,
}

impl Message for GetValidatorOrdered {
    type Result = Result<Vec<ValidatorStakeView>, GetValidatorInfoError>;
}

#[derive(Debug)]
pub struct GetStateChanges {
    pub block_hash: CryptoHash,
    pub state_changes_request: StateChangesRequestView,
}

#[derive(thiserror::Error, Debug)]
pub enum GetStateChangesError {
    #[error("IO Error: {error_message}")]
    IOError { error_message: String },
    #[error(
        "Block either has never been observed on the node or has been garbage collected: {error_message}"
    )]
    UnknownBlock { error_message: String },
    #[error("There are no fully synchronized blocks yet")]
    NotSyncedYet,
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error(
        "It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}"
    )]
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

#[derive(Debug)]
pub struct GetStateChangesInBlock {
    pub block_hash: CryptoHash,
}

impl Message for GetStateChangesInBlock {
    type Result = Result<StateChangesKindsView, GetStateChangesError>;
}

#[derive(Debug)]
pub struct GetStateChangesWithCauseInBlock {
    pub block_hash: CryptoHash,
}

impl Message for GetStateChangesWithCauseInBlock {
    type Result = Result<StateChangesView, GetStateChangesError>;
}

#[derive(Debug)]
pub struct GetStateChangesWithCauseInBlockForTrackedShards {
    pub block_hash: CryptoHash,
    pub epoch_id: EpochId,
}

impl Message for GetStateChangesWithCauseInBlockForTrackedShards {
    type Result = Result<HashMap<ShardId, StateChangesView>, GetStateChangesError>;
}

#[derive(Debug)]
pub struct GetExecutionOutcome {
    pub id: TransactionOrReceiptId,
}

#[derive(thiserror::Error, Debug)]
pub enum GetExecutionOutcomeError {
    #[error(
        "Block either has never been observed on the node or has been garbage collected: {error_message}"
    )]
    UnknownBlock { error_message: String },
    #[error(
        "Inconsistent state. Total number of shards is {number_or_shards} but the execution outcome is in shard {execution_outcome_shard_id}"
    )]
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
    #[error(
        "It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}"
    )]
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

#[derive(Debug)]
pub struct GetExecutionOutcomesForBlock {
    pub block_hash: CryptoHash,
}

impl Message for GetExecutionOutcomesForBlock {
    type Result = Result<HashMap<ShardId, Vec<ExecutionOutcomeWithIdView>>, String>;
}

#[derive(Debug)]
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
    #[error(
        "Block either has never been observed on the node or has been garbage collected: {error_message}"
    )]
    UnknownBlock { error_message: String },
    #[error("Internal error: {error_message}")]
    InternalError { error_message: String },
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error(
        "It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}"
    )]
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

#[derive(Debug)]
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
    #[error(
        "It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {0}"
    )]
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

#[derive(Debug)]
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
    #[error(
        "It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {0}"
    )]
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

#[derive(Debug)]
pub struct GetMaintenanceWindows {
    pub account_id: AccountId,
}

impl Message for GetMaintenanceWindows {
    type Result = Result<MaintenanceWindowsView, GetMaintenanceWindowsError>;
}

#[derive(thiserror::Error, Debug)]
pub enum GetMaintenanceWindowsError {
    #[error("IO Error: {0}")]
    IOError(String),
    #[error(
        "It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {0}"
    )]
    Unreachable(String),
}

impl From<near_chain_primitives::Error> for GetMaintenanceWindowsError {
    fn from(error: near_chain_primitives::Error) -> Self {
        match error {
            near_chain_primitives::Error::IOErr(error) => Self::IOError(error.to_string()),
            _ => Self::Unreachable(error.to_string()),
        }
    }
}

#[derive(Debug)]
pub struct GetClientConfig {}

impl Message for GetClientConfig {
    type Result = Result<ClientConfig, GetClientConfigError>;
}

#[derive(thiserror::Error, Debug)]
pub enum GetClientConfigError {
    #[error("IO Error: {0}")]
    IOError(String),
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error(
        "It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {0}"
    )]
    Unreachable(String),
}

impl From<near_chain_primitives::Error> for GetClientConfigError {
    fn from(error: near_chain_primitives::Error) -> Self {
        match error {
            near_chain_primitives::Error::IOErr(error) => Self::IOError(error.to_string()),
            _ => Self::Unreachable(error.to_string()),
        }
    }
}

#[derive(Debug)]
pub struct GetSplitStorageInfo {}

impl Message for GetSplitStorageInfo {
    type Result = Result<SplitStorageInfoView, GetSplitStorageInfoError>;
}

#[derive(thiserror::Error, Debug)]
pub enum GetSplitStorageInfoError {
    #[error("IO Error: {0}")]
    IOError(String),
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error(
        "It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {0}"
    )]
    Unreachable(String),
}

impl From<near_chain_primitives::Error> for GetSplitStorageInfoError {
    fn from(error: near_chain_primitives::Error) -> Self {
        match error {
            near_chain_primitives::Error::IOErr(error) => Self::IOError(error.to_string()),
            _ => Self::Unreachable(error.to_string()),
        }
    }
}

impl From<std::io::Error> for GetSplitStorageInfoError {
    fn from(error: std::io::Error) -> Self {
        Self::IOError(error.to_string())
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

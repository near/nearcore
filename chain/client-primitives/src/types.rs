use actix::Message;
use ansi_term::Color::{Purple, Yellow};
use ansi_term::Style;
use chrono::DateTime;
use chrono::Utc;
use near_chain_configs::{ClientConfig, ProtocolConfigView};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{MerklePath, PartialMerkleTree};
use near_primitives::network::PeerId;
use near_primitives::sharding::ChunkHash;
use near_primitives::types::{
    AccountId, BlockHeight, BlockReference, EpochId, EpochReference, MaybeBlockId, ShardId,
    TransactionOrReceiptId,
};
use near_primitives::views::validator_stake_view::ValidatorStakeView;
use near_primitives::views::{
    BlockView, ChunkView, DownloadStatusView, EpochValidatorInfo, ExecutionOutcomeWithIdView,
    FinalExecutionOutcomeViewEnum, GasPriceView, LightClientBlockLiteView, LightClientBlockView,
    MaintenanceWindowsView, QueryRequest, QueryResponse, ReceiptView, ShardSyncDownloadView,
    SplitStorageInfoView, StateChangesKindsView, StateChangesRequestView, StateChangesView,
    SyncStatusView,
};
pub use near_primitives::views::{StatusResponse, StatusSyncInfo};
use once_cell::sync::OnceCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

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

#[derive(Clone, Debug, serde::Serialize, PartialEq)]
pub enum AccountOrPeerIdOrHash {
    AccountId(AccountId),
    PeerId(PeerId),
    Hash(CryptoHash),
}

#[derive(Debug, serde::Serialize)]
pub struct DownloadStatus {
    pub start_time: DateTime<Utc>,
    pub prev_update_time: DateTime<Utc>,
    pub run_me: Arc<AtomicBool>,
    pub error: bool,
    pub done: bool,
    pub state_requests_count: u64,
    pub last_target: Option<AccountOrPeerIdOrHash>,
}

impl DownloadStatus {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            start_time: now,
            prev_update_time: now,
            run_me: Arc::new(AtomicBool::new(true)),
            error: false,
            done: false,
            state_requests_count: 0,
            last_target: None,
        }
    }
}

impl Clone for DownloadStatus {
    /// Clones an object, but it clones the value of `run_me` instead of the
    /// `Arc` that wraps that value.
    fn clone(&self) -> Self {
        DownloadStatus {
            start_time: self.start_time,
            prev_update_time: self.prev_update_time,
            // Creates a new `Arc` holding the same value.
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
    StateDownloadScheduling,
    StateDownloadApplying,
    StateDownloadComplete,
    StateSplitScheduling,
    StateSplitApplying(Arc<StateSplitApplyingStatus>),
    StateSyncDone,
}

impl ShardSyncStatus {
    pub fn repr(&self) -> u8 {
        match self {
            ShardSyncStatus::StateDownloadHeader => 0,
            ShardSyncStatus::StateDownloadParts => 1,
            ShardSyncStatus::StateDownloadScheduling => 2,
            ShardSyncStatus::StateDownloadApplying => 3,
            ShardSyncStatus::StateDownloadComplete => 4,
            ShardSyncStatus::StateSplitScheduling => 5,
            ShardSyncStatus::StateSplitApplying(_) => 6,
            ShardSyncStatus::StateSyncDone => 7,
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
            ShardSyncStatus::StateDownloadScheduling => "scheduling".to_string(),
            ShardSyncStatus::StateDownloadApplying => "applying".to_string(),
            ShardSyncStatus::StateDownloadComplete => "download complete".to_string(),
            ShardSyncStatus::StateSplitScheduling => "split scheduling".to_string(),
            ShardSyncStatus::StateSplitApplying(state_split_status) => {
                let str = if let Some(total_parts) = state_split_status.total_parts.get() {
                    format!(
                        "total parts {} done {}",
                        total_parts,
                        state_split_status.done_parts.load(Ordering::Relaxed)
                    )
                } else {
                    "not started".to_string()
                };
                format!("split applying {}", str)
            }
            ShardSyncStatus::StateSyncDone => "done".to_string(),
        }
    }
}

impl From<&DownloadStatus> for DownloadStatusView {
    fn from(status: &DownloadStatus) -> Self {
        DownloadStatusView { done: status.done, error: status.error }
    }
}

impl From<ShardSyncDownload> for ShardSyncDownloadView {
    fn from(download: ShardSyncDownload) -> Self {
        ShardSyncDownloadView {
            downloads: download.downloads.iter().map(|x| x.into()).collect(),
            status: download.status.to_string(),
        }
    }
}

#[derive(Debug, Default)]
pub struct StateSplitApplyingStatus {
    /// total number of parts to be applied
    pub total_parts: OnceCell<u64>,
    /// number of parts that are done
    pub done_parts: AtomicU64,
}

/// Stores status of shard sync and statuses of downloading shards.
#[derive(Clone, Debug)]
pub struct ShardSyncDownload {
    /// Stores all download statuses. If we are downloading state parts, its
    /// length equals the number of state parts. Otherwise it is 1, since we
    /// have only one piece of data to download, like shard state header. It
    /// could be 0 when we are not downloading anything but rather splitting a
    /// shard as part of resharding.
    pub downloads: Vec<DownloadStatus>,
    pub status: ShardSyncStatus,
}

impl ShardSyncDownload {
    /// Creates a instance of self which includes initial statuses for shard state header download at the given time.
    pub fn new_download_state_header(now: DateTime<Utc>) -> Self {
        Self {
            downloads: vec![DownloadStatus::new(now)],
            status: ShardSyncStatus::StateDownloadHeader,
        }
    }

    /// Creates a instance of self which includes initial statuses for shard state parts download at the given time.
    pub fn new_download_state_parts(now: DateTime<Utc>, num_parts: u64) -> Self {
        // Avoid using `vec![x; num_parts]`, because each element needs to have
        // its own independent value of `response`.
        let mut downloads = Vec::with_capacity(num_parts as usize);
        for _ in 0..num_parts {
            downloads.push(DownloadStatus::new(now));
        }
        Self { downloads, status: ShardSyncStatus::StateDownloadParts }
    }
}

pub fn format_shard_sync_phase_per_shard(
    new_shard_sync: &HashMap<ShardId, ShardSyncDownload>,
    use_colour: bool,
) -> Vec<(ShardId, String)> {
    new_shard_sync
        .iter()
        .map(|(&shard_id, shard_progress)| {
            (shard_id, format_shard_sync_phase(shard_progress, use_colour))
        })
        .collect::<Vec<(_, _)>>()
}

/// Applies style if `use_colour` is enabled.
fn paint(s: &str, style: Style, use_style: bool) -> String {
    if use_style {
        style.paint(s).to_string()
    } else {
        s.to_string()
    }
}

/// Formats the given ShardSyncDownload for logging.
pub fn format_shard_sync_phase(
    shard_sync_download: &ShardSyncDownload,
    use_colour: bool,
) -> String {
    match &shard_sync_download.status {
        ShardSyncStatus::StateDownloadHeader => format!(
            "{} requests sent {}, last target {:?}",
            paint("HEADER", Purple.bold(), use_colour),
            shard_sync_download.downloads.get(0).map_or(0, |x| x.state_requests_count),
            shard_sync_download.downloads.get(0).map_or(None, |x| x.last_target.as_ref()),
        ),
        ShardSyncStatus::StateDownloadParts => {
            let mut num_parts_done = 0;
            let mut num_parts_not_done = 0;
            let mut text = "".to_string();
            for (i, download) in shard_sync_download.downloads.iter().enumerate() {
                if download.done {
                    num_parts_done += 1;
                    continue;
                }
                num_parts_not_done += 1;
                text.push_str(&format!(
                    "[{}: {}, {}, {:?}] ",
                    paint(&i.to_string(), Yellow.bold(), use_colour),
                    download.done,
                    download.state_requests_count,
                    download.last_target
                ));
            }
            format!(
                "{} [{}: is_done, requests sent, last target] {} num_parts_done={} num_parts_not_done={}",
                paint("PARTS", Purple.bold(), use_colour),
                paint("part_id", Yellow.bold(), use_colour),
                text,
                num_parts_done,
                num_parts_not_done
            )
        }
        status => format!("{:?}", status),
    }
}

#[derive(Clone)]
pub struct StateSyncStatus {
    pub sync_hash: CryptoHash,
    pub sync_status: HashMap<ShardId, ShardSyncDownload>,
}

/// If alternate flag was specified, write formatted sync_status per shard.
impl std::fmt::Debug for StateSyncStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if f.alternate() {
            write!(
                f,
                "StateSyncStatus {{ sync_hash: {:?}, shard_sync: {:?} }}",
                self.sync_hash,
                format_shard_sync_phase_per_shard(&self.sync_status, false)
            )
        } else {
            write!(
                f,
                "StateSyncStatus {{ sync_hash: {:?}, sync_status: {:?} }}",
                self.sync_hash, self.sync_status
            )
        }
    }
}

/// Various status sync can be in, whether it's fast sync or archival.
#[derive(Clone, Debug, strum::AsRefStr)]
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
    StateSync(StateSyncStatus),
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
            SyncStatus::StateSync(_) => 4,
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

impl From<SyncStatus> for SyncStatusView {
    fn from(status: SyncStatus) -> Self {
        match status {
            SyncStatus::AwaitingPeers => SyncStatusView::AwaitingPeers,
            SyncStatus::NoSync => SyncStatusView::NoSync,
            SyncStatus::EpochSync { epoch_ord } => SyncStatusView::EpochSync { epoch_ord },
            SyncStatus::HeaderSync { start_height, current_height, highest_height } => {
                SyncStatusView::HeaderSync { start_height, current_height, highest_height }
            }
            SyncStatus::StateSync(state_sync_status) => SyncStatusView::StateSync(
                state_sync_status.sync_hash,
                state_sync_status
                    .sync_status
                    .into_iter()
                    .map(|(shard_id, shard_sync)| (shard_id, shard_sync.into()))
                    .collect(),
            ),
            SyncStatus::StateSyncDone => SyncStatusView::StateSyncDone,
            SyncStatus::BodySync { start_height, current_height, highest_height } => {
                SyncStatusView::BodySync { start_height, current_height, highest_height }
            }
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
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {0}")]
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
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {0}")]
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
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {0}")]
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

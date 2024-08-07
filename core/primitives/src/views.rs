//! This module defines "stable" internal API to view internal data using view_client.
//!
//! These types should only change when we cannot avoid this. Thus, when the counterpart internal
//! type gets changed, the view should preserve the old shape and only re-map the necessary bits
//! from the source structure in the relevant `From<SourceStruct>` impl.
use crate::account::{AccessKey, AccessKeyPermission, Account, FunctionCallPermission};
use crate::action::delegate::{DelegateAction, SignedDelegateAction};
use crate::block::{Block, BlockHeader, Tip};
use crate::block_header::{
    BlockHeaderInnerLite, BlockHeaderInnerRest, BlockHeaderInnerRestV2, BlockHeaderInnerRestV3,
    BlockHeaderV1, BlockHeaderV2, BlockHeaderV3,
};
use crate::block_header::{BlockHeaderInnerRestV4, BlockHeaderV4};
use crate::challenge::{Challenge, ChallengesResult};
use crate::checked_feature;
use crate::congestion_info::{CongestionInfo, CongestionInfoV1};
use crate::errors::TxExecutionError;
use crate::hash::{hash, CryptoHash};
use crate::merkle::{combine_hash, MerklePath};
use crate::network::PeerId;
use crate::receipt::{ActionReceipt, DataReceipt, DataReceiver, Receipt, ReceiptEnum, ReceiptV1};
use crate::serialize::dec_format;
use crate::sharding::{
    ChunkHash, ShardChunk, ShardChunkHeader, ShardChunkHeaderInner, ShardChunkHeaderInnerV2,
    ShardChunkHeaderInnerV3, ShardChunkHeaderV3,
};
#[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
use crate::transaction::NonrefundableStorageTransferAction;
use crate::transaction::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeleteKeyAction,
    DeployContractAction, ExecutionMetadata, ExecutionOutcome, ExecutionOutcomeWithIdAndProof,
    ExecutionStatus, FunctionCallAction, PartialExecutionOutcome, PartialExecutionStatus,
    SignedTransaction, StakeAction, TransferAction,
};
use crate::types::{
    AccountId, AccountWithPublicKey, Balance, BlockHeight, EpochHeight, EpochId, FunctionArgs, Gas,
    Nonce, NumBlocks, ShardId, StateChangeCause, StateChangeKind, StateChangeValue,
    StateChangeWithCause, StateChangesRequest, StateRoot, StorageUsage, StoreKey, StoreValue,
    ValidatorKickoutReason,
};
use crate::version::{ProtocolVersion, Version};
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::{PublicKey, Signature};
use near_fmt::{AbbrBytes, Slice};
use near_parameters::config::CongestionControlConfig;
use near_parameters::view::CongestionControlConfigView;
use near_parameters::{ActionCosts, ExtCosts};
use near_primitives_core::version::PROTOCOL_VERSION;
use near_schema_checker_lib::ProtocolSchema;
use near_time::Utc;
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use strum::IntoEnumIterator;
use validator_stake_view::ValidatorStakeView;

/// A view of the account
#[derive(serde::Serialize, serde::Deserialize, Debug, Eq, PartialEq, Clone)]
pub struct AccountView {
    #[serde(with = "dec_format")]
    pub amount: Balance,
    #[serde(with = "dec_format")]
    pub locked: Balance,
    #[serde(with = "dec_format")]
    #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
    pub permanent_storage_bytes: StorageUsage,
    pub code_hash: CryptoHash,
    pub storage_usage: StorageUsage,
    /// TODO(2271): deprecated.
    #[serde(default)]
    pub storage_paid_at: BlockHeight,
}

/// A view of the contract code.
#[serde_as]
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct ContractCodeView {
    #[serde(rename = "code_base64")]
    #[serde_as(as = "Base64")]
    pub code: Vec<u8>,
    pub hash: CryptoHash,
}

impl From<&Account> for AccountView {
    fn from(account: &Account) -> Self {
        AccountView {
            amount: account.amount(),
            locked: account.locked(),
            #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
            permanent_storage_bytes: account.permanent_storage_bytes(),
            code_hash: account.code_hash(),
            storage_usage: account.storage_usage(),
            storage_paid_at: 0,
        }
    }
}

impl From<Account> for AccountView {
    fn from(account: Account) -> Self {
        (&account).into()
    }
}

impl From<&AccountView> for Account {
    fn from(view: &AccountView) -> Self {
        #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
        let permanent_storage_bytes = view.permanent_storage_bytes;
        #[cfg(not(feature = "protocol_feature_nonrefundable_transfer_nep491"))]
        let permanent_storage_bytes = 0;
        Account::new(
            view.amount,
            view.locked,
            permanent_storage_bytes,
            view.code_hash,
            view.storage_usage,
            PROTOCOL_VERSION,
        )
    }
}

impl From<AccountView> for Account {
    fn from(view: AccountView) -> Self {
        (&view).into()
    }
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    Eq,
    PartialEq,
    Clone,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum AccessKeyPermissionView {
    FunctionCall {
        #[serde(with = "dec_format")]
        allowance: Option<Balance>,
        receiver_id: String,
        method_names: Vec<String>,
    },
    FullAccess,
}

impl From<AccessKeyPermission> for AccessKeyPermissionView {
    fn from(permission: AccessKeyPermission) -> Self {
        match permission {
            AccessKeyPermission::FunctionCall(func_call) => AccessKeyPermissionView::FunctionCall {
                allowance: func_call.allowance,
                receiver_id: func_call.receiver_id,
                method_names: func_call.method_names,
            },
            AccessKeyPermission::FullAccess => AccessKeyPermissionView::FullAccess,
        }
    }
}

impl From<AccessKeyPermissionView> for AccessKeyPermission {
    fn from(view: AccessKeyPermissionView) -> Self {
        match view {
            AccessKeyPermissionView::FunctionCall { allowance, receiver_id, method_names } => {
                AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance,
                    receiver_id,
                    method_names,
                })
            }
            AccessKeyPermissionView::FullAccess => AccessKeyPermission::FullAccess,
        }
    }
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    Eq,
    PartialEq,
    Clone,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct AccessKeyView {
    pub nonce: Nonce,
    pub permission: AccessKeyPermissionView,
}

impl From<AccessKey> for AccessKeyView {
    fn from(access_key: AccessKey) -> Self {
        Self { nonce: access_key.nonce, permission: access_key.permission.into() }
    }
}

impl From<AccessKeyView> for AccessKey {
    fn from(view: AccessKeyView) -> Self {
        Self { nonce: view.nonce, permission: view.permission.into() }
    }
}

/// Item of the state, key and value are serialized in base64 and proof for inclusion of given state item.
#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct StateItem {
    pub key: StoreKey,
    pub value: StoreValue,
}

#[serde_as]
#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct ViewStateResult {
    pub values: Vec<StateItem>,
    #[serde_as(as = "Vec<Base64>")]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub proof: Vec<Arc<[u8]>>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Clone, Default)]
pub struct CallResult {
    pub result: Vec<u8>,
    pub logs: Vec<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct QueryError {
    pub error: String,
    pub logs: Vec<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct AccessKeyInfoView {
    pub public_key: PublicKey,
    pub access_key: AccessKeyView,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct AccessKeyList {
    pub keys: Vec<AccessKeyInfoView>,
}

impl FromIterator<AccessKeyInfoView> for AccessKeyList {
    fn from_iter<I: IntoIterator<Item = AccessKeyInfoView>>(iter: I) -> Self {
        Self { keys: iter.into_iter().collect() }
    }
}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct KnownPeerStateView {
    pub peer_id: PeerId,
    pub status: String,
    pub addr: String,
    pub first_seen: i64,
    pub last_seen: i64,
    pub last_attempt: Option<(i64, String)>,
}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct ConnectionInfoView {
    pub peer_id: PeerId,
    pub addr: String,
    pub time_established: i64,
    pub time_connected_until: i64,
}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct SnapshotHostInfoView {
    pub peer_id: PeerId,
    pub sync_hash: CryptoHash,
    pub epoch_height: u64,
    pub shards: Vec<u64>,
}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum QueryResponseKind {
    ViewAccount(AccountView),
    ViewCode(ContractCodeView),
    ViewState(ViewStateResult),
    CallResult(CallResult),
    AccessKey(AccessKeyView),
    AccessKeyList(AccessKeyList),
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(tag = "request_type", rename_all = "snake_case")]
pub enum QueryRequest {
    ViewAccount {
        account_id: AccountId,
    },
    ViewCode {
        account_id: AccountId,
    },
    ViewState {
        account_id: AccountId,
        #[serde(rename = "prefix_base64")]
        prefix: StoreKey,
        #[serde(default, skip_serializing_if = "is_false")]
        include_proof: bool,
    },
    ViewAccessKey {
        account_id: AccountId,
        public_key: PublicKey,
    },
    ViewAccessKeyList {
        account_id: AccountId,
    },
    CallFunction {
        account_id: AccountId,
        method_name: String,
        #[serde(rename = "args_base64")]
        args: FunctionArgs,
    },
}

fn is_false(v: &bool) -> bool {
    !*v
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct QueryResponse {
    pub kind: QueryResponseKind,
    pub block_height: BlockHeight,
    pub block_hash: CryptoHash,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct StatusSyncInfo {
    pub latest_block_hash: CryptoHash,
    pub latest_block_height: BlockHeight,
    pub latest_state_root: CryptoHash,
    #[serde(with = "near_time::serde_utc_as_iso")]
    pub latest_block_time: Utc,
    pub syncing: bool,
    pub earliest_block_hash: Option<CryptoHash>,
    pub earliest_block_height: Option<BlockHeight>,
    #[serde(with = "near_time::serde_opt_utc_as_iso")]
    pub earliest_block_time: Option<Utc>,
    pub epoch_id: Option<EpochId>,
    pub epoch_start_height: Option<BlockHeight>,
}

// TODO: add more information to ValidatorInfo
#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct ValidatorInfo {
    pub account_id: AccountId,
    pub is_slashed: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct PeerInfoView {
    pub addr: String,
    pub account_id: Option<AccountId>,
    pub height: Option<BlockHeight>,
    pub block_hash: Option<CryptoHash>,
    pub is_highest_block_invalid: bool,
    pub tracked_shards: Vec<ShardId>,
    pub archival: bool,
    pub peer_id: PublicKey,
    pub received_bytes_per_sec: u64,
    pub sent_bytes_per_sec: u64,
    pub last_time_peer_requested_millis: u64,
    pub last_time_received_message_millis: u64,
    pub connection_established_time_millis: u64,
    pub is_outbound_peer: bool,
    /// Connection nonce.
    pub nonce: u64,
}

/// Information about a Producer: its account name, peer_id and a list of connected peers that
/// the node can use to send message for this producer.
#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct KnownProducerView {
    pub account_id: AccountId,
    pub peer_id: PublicKey,
    pub next_hops: Option<Vec<PublicKey>>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct Tier1ProxyView {
    pub addr: std::net::SocketAddr,
    pub peer_id: PublicKey,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct AccountDataView {
    pub peer_id: PublicKey,
    pub proxies: Vec<Tier1ProxyView>,
    pub account_key: PublicKey,
    #[serde(with = "near_time::serde_utc_as_iso")]
    pub timestamp: Utc,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct NetworkInfoView {
    pub peer_max_count: u32,
    pub num_connected_peers: usize,
    pub connected_peers: Vec<PeerInfoView>,
    pub known_producers: Vec<KnownProducerView>,
    pub tier1_accounts_keys: Vec<PublicKey>,
    pub tier1_accounts_data: Vec<AccountDataView>,
    pub tier1_connections: Vec<PeerInfoView>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub enum SyncStatusView {
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
    StateSync(CryptoHash, HashMap<ShardId, ShardSyncDownloadView>),
    /// Sync state across all shards is done.
    StateSyncDone,
    /// Download and process blocks until the head reaches the head of the network.
    BlockSync {
        start_height: BlockHeight,
        current_height: BlockHeight,
        highest_height: BlockHeight,
    },
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct PeerStoreView {
    pub peer_states: Vec<KnownPeerStateView>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct RecentOutboundConnectionsView {
    pub recent_outbound_connections: Vec<ConnectionInfoView>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct SnapshotHostsView {
    pub hosts: Vec<SnapshotHostInfoView>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct EdgeView {
    pub peer0: PeerId,
    pub peer1: PeerId,
    pub nonce: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct NetworkGraphView {
    pub edges: Vec<EdgeView>,
    pub next_hops: HashMap<PeerId, Vec<PeerId>>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct LabeledEdgeView {
    pub peer0: u32,
    pub peer1: u32,
    pub nonce: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct EdgeCacheView {
    pub peer_labels: HashMap<PeerId, u32>,
    pub spanning_trees: HashMap<u32, Vec<LabeledEdgeView>>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct PeerDistancesView {
    pub distance: Vec<Option<u32>>,
    pub min_nonce: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct NetworkRoutesView {
    pub edge_cache: EdgeCacheView,
    pub local_edges: HashMap<PeerId, EdgeView>,
    pub peer_distances: HashMap<PeerId, PeerDistancesView>,
    pub my_distances: HashMap<PeerId, u32>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct ShardSyncDownloadView {
    pub downloads: Vec<DownloadStatusView>,
    pub status: String,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct DownloadStatusView {
    pub error: bool,
    pub done: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct CatchupStatusView {
    // This is the first block of the epoch that we are catching up
    pub sync_block_hash: CryptoHash,
    pub sync_block_height: BlockHeight,
    // Status of all shards that need to sync
    pub shard_sync_status: HashMap<ShardId, String>,
    // Blocks that we need to catchup, if it is empty, it means catching up is done
    pub blocks_to_catchup: Vec<BlockStatusView>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct RequestedStatePartsView {
    // This is the first block of the epoch that was requested
    pub block_hash: CryptoHash,
    // All the part ids of the shards that were requested
    pub shard_requested_parts: HashMap<ShardId, Vec<PartElapsedTimeView>>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct BlockStatusView {
    pub height: BlockHeight,
    pub hash: CryptoHash,
}

impl BlockStatusView {
    pub fn new(height: &BlockHeight, hash: &CryptoHash) -> BlockStatusView {
        Self { height: *height, hash: *hash }
    }
}

impl From<Tip> for BlockStatusView {
    fn from(tip: Tip) -> Self {
        Self { height: tip.height, hash: tip.last_block_hash }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct PartElapsedTimeView {
    pub part_id: u64,
    pub elapsed_ms: u128,
}

impl PartElapsedTimeView {
    pub fn new(part_id: &u64, elapsed_ms: u128) -> PartElapsedTimeView {
        Self { part_id: *part_id, elapsed_ms }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct BlockByChunksView {
    pub height: BlockHeight,
    pub hash: CryptoHash,
    pub block_status: String,
    pub chunk_status: String,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ChainProcessingInfo {
    pub num_blocks_in_processing: usize,
    pub num_orphans: usize,
    pub num_blocks_missing_chunks: usize,
    /// contains processing info of recent blocks, ordered by height high to low
    pub blocks_info: Vec<BlockProcessingInfo>,
    /// contains processing info of chunks that we don't know which block it belongs to yet
    pub floating_chunks_info: Vec<ChunkProcessingInfo>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct BlockProcessingInfo {
    pub height: BlockHeight,
    pub hash: CryptoHash,
    #[serde(with = "near_time::serde_utc_as_iso")]
    pub received_timestamp: Utc,
    /// Time (in ms) between when the block was first received and when it was processed
    pub in_progress_ms: u128,
    /// Time (in ms) that the block spent in the orphan pool. If the block was never put in the
    /// orphan pool, it is None. If the block is still in the orphan pool, it is since the time
    /// it was put into the pool until the current time.
    pub orphaned_ms: Option<u128>,
    /// Time (in ms) that the block spent in the missing chunks pool. If the block was never put in the
    /// missing chunks pool, it is None. If the block is still in the missing chunks pool, it is
    /// since the time it was put into the pool until the current time.
    pub missing_chunks_ms: Option<u128>,
    pub block_status: BlockProcessingStatus,
    /// Only contains new chunks that belong to this block, if the block doesn't produce a new chunk
    /// for a shard, the corresponding item will be None.
    pub chunks_info: Vec<Option<ChunkProcessingInfo>>,
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum BlockProcessingStatus {
    Orphan,
    WaitingForChunks,
    InProcessing,
    Accepted,
    Error(String),
    Dropped(DroppedReason),
    Unknown,
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum DroppedReason {
    // If the node has already processed a block at this height
    HeightProcessed,
    // If the block processing pool is full
    TooManyProcessingBlocks,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ChunkProcessingInfo {
    pub height_created: BlockHeight,
    pub shard_id: ShardId,
    pub chunk_hash: ChunkHash,
    pub prev_block_hash: CryptoHash,
    /// Account id of the validator who created this chunk
    /// Theoretically this field should never be None unless there is some database corruption.
    pub created_by: Option<AccountId>,
    pub status: ChunkProcessingStatus,
    /// Timestamp of first time when we request for this chunk.
    #[serde(with = "near_time::serde_opt_utc_as_iso")]
    pub requested_timestamp: Option<Utc>,
    /// Timestamp of when the chunk is complete
    #[serde(with = "near_time::serde_opt_utc_as_iso")]
    pub completed_timestamp: Option<Utc>,
    /// Time (in millis) that it takes between when the chunk is requested and when it is completed.
    pub request_duration: Option<u64>,
    pub chunk_parts_collection: Vec<PartCollectionInfo>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct PartCollectionInfo {
    pub part_owner: AccountId,
    // Time when the part is received through any message
    #[serde(with = "near_time::serde_opt_utc_as_iso")]
    pub received_time: Option<Utc>,
    // Time when we receive a PartialEncodedChunkForward containing this part
    #[serde(with = "near_time::serde_opt_utc_as_iso")]
    pub forwarded_received_time: Option<Utc>,
    // Time when we receive the PartialEncodedChunk message containing this part
    #[serde(with = "near_time::serde_opt_utc_as_iso")]
    pub chunk_received_time: Option<Utc>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum ChunkProcessingStatus {
    NeedToRequest,
    Requested,
    Completed,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct DetailedDebugStatus {
    pub network_info: NetworkInfoView,
    pub sync_status: String,
    pub catchup_status: Vec<CatchupStatusView>,
    pub current_head_status: BlockStatusView,
    pub current_header_head_status: BlockStatusView,
    pub block_production_delay_millis: u64,
}

// TODO: add more information to status.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct StatusResponse {
    /// Binary version.
    pub version: Version,
    /// Unique chain id.
    pub chain_id: String,
    /// Currently active protocol version.
    pub protocol_version: u32,
    /// Latest protocol version that this client supports.
    pub latest_protocol_version: u32,
    /// Address for RPC server.  None if node doesn’t have RPC endpoint enabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rpc_addr: Option<String>,
    /// Current epoch validators.
    pub validators: Vec<ValidatorInfo>,
    /// Sync status of the node.
    pub sync_info: StatusSyncInfo,
    /// Validator id of the node
    pub validator_account_id: Option<AccountId>,
    /// Public key of the validator.
    pub validator_public_key: Option<PublicKey>,
    /// Public key of the node.
    pub node_public_key: PublicKey,
    /// Deprecated; same as `validator_public_key` which you should use instead.
    pub node_key: Option<PublicKey>,
    /// Uptime of the node.
    pub uptime_sec: i64,
    /// Genesis hash of the chain.
    pub genesis_hash: CryptoHash,
    /// Information about last blocks, network, epoch and chain & chunk info.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detailed_debug_status: Option<DetailedDebugStatus>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct ChallengeView {
    // TODO: decide how to represent challenges in json.
}

impl From<Challenge> for ChallengeView {
    fn from(_challenge: Challenge) -> Self {
        Self {}
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct BlockHeaderView {
    pub height: BlockHeight,
    pub prev_height: Option<BlockHeight>,
    pub epoch_id: CryptoHash,
    pub next_epoch_id: CryptoHash,
    pub hash: CryptoHash,
    pub prev_hash: CryptoHash,
    pub prev_state_root: CryptoHash,
    pub block_body_hash: Option<CryptoHash>,
    pub chunk_receipts_root: CryptoHash,
    pub chunk_headers_root: CryptoHash,
    pub chunk_tx_root: CryptoHash,
    pub outcome_root: CryptoHash,
    pub chunks_included: u64,
    pub challenges_root: CryptoHash,
    /// Legacy json number. Should not be used.
    pub timestamp: u64,
    #[serde(with = "dec_format")]
    pub timestamp_nanosec: u64,
    pub random_value: CryptoHash,
    pub validator_proposals: Vec<ValidatorStakeView>,
    pub chunk_mask: Vec<bool>,
    #[serde(with = "dec_format")]
    pub gas_price: Balance,
    pub block_ordinal: Option<NumBlocks>,
    /// TODO(2271): deprecated.
    #[serde(with = "dec_format")]
    pub rent_paid: Balance,
    /// TODO(2271): deprecated.
    #[serde(with = "dec_format")]
    pub validator_reward: Balance,
    #[serde(with = "dec_format")]
    pub total_supply: Balance,
    pub challenges_result: ChallengesResult,
    pub last_final_block: CryptoHash,
    pub last_ds_final_block: CryptoHash,
    pub next_bp_hash: CryptoHash,
    pub block_merkle_root: CryptoHash,
    pub epoch_sync_data_hash: Option<CryptoHash>,
    pub approvals: Vec<Option<Box<Signature>>>,
    pub signature: Signature,
    pub latest_protocol_version: ProtocolVersion,
}

impl From<BlockHeader> for BlockHeaderView {
    fn from(header: BlockHeader) -> Self {
        Self {
            height: header.height(),
            prev_height: header.prev_height(),
            epoch_id: header.epoch_id().0,
            next_epoch_id: header.next_epoch_id().0,
            hash: *header.hash(),
            prev_hash: *header.prev_hash(),
            prev_state_root: *header.prev_state_root(),
            block_body_hash: header.block_body_hash(),
            chunk_receipts_root: *header.prev_chunk_outgoing_receipts_root(),
            chunk_headers_root: *header.chunk_headers_root(),
            chunk_tx_root: *header.chunk_tx_root(),
            chunks_included: header.chunks_included(),
            challenges_root: *header.challenges_root(),
            outcome_root: *header.outcome_root(),
            timestamp: header.raw_timestamp(),
            timestamp_nanosec: header.raw_timestamp(),
            random_value: *header.random_value(),
            validator_proposals: header.prev_validator_proposals().map(Into::into).collect(),
            chunk_mask: header.chunk_mask().to_vec(),
            block_ordinal: if header.block_ordinal() != 0 {
                Some(header.block_ordinal())
            } else {
                None
            },
            gas_price: header.next_gas_price(),
            rent_paid: 0,
            validator_reward: 0,
            total_supply: header.total_supply(),
            challenges_result: header.challenges_result().clone(),
            last_final_block: *header.last_final_block(),
            last_ds_final_block: *header.last_ds_final_block(),
            next_bp_hash: *header.next_bp_hash(),
            block_merkle_root: *header.block_merkle_root(),
            epoch_sync_data_hash: header.epoch_sync_data_hash(),
            approvals: header.approvals().to_vec(),
            signature: header.signature().clone(),
            latest_protocol_version: header.latest_protocol_version(),
        }
    }
}

impl From<BlockHeaderView> for BlockHeader {
    fn from(view: BlockHeaderView) -> Self {
        let inner_lite = BlockHeaderInnerLite {
            height: view.height,
            epoch_id: EpochId(view.epoch_id),
            next_epoch_id: EpochId(view.next_epoch_id),
            prev_state_root: view.prev_state_root,
            prev_outcome_root: view.outcome_root,
            timestamp: view.timestamp,
            next_bp_hash: view.next_bp_hash,
            block_merkle_root: view.block_merkle_root,
        };
        const LAST_HEADER_V2_VERSION: ProtocolVersion =
            crate::version::ProtocolFeature::BlockHeaderV3.protocol_version() - 1;
        if view.latest_protocol_version <= 29 {
            let validator_proposals = view
                .validator_proposals
                .into_iter()
                .map(|v| v.into_validator_stake().into_v1())
                .collect();
            let mut header = BlockHeaderV1 {
                prev_hash: view.prev_hash,
                inner_lite,
                inner_rest: BlockHeaderInnerRest {
                    prev_chunk_outgoing_receipts_root: view.chunk_receipts_root,
                    chunk_headers_root: view.chunk_headers_root,
                    chunk_tx_root: view.chunk_tx_root,
                    chunks_included: view.chunks_included,
                    challenges_root: view.challenges_root,
                    random_value: view.random_value,
                    prev_validator_proposals: validator_proposals,
                    chunk_mask: view.chunk_mask,
                    next_gas_price: view.gas_price,
                    total_supply: view.total_supply,
                    challenges_result: view.challenges_result,
                    last_final_block: view.last_final_block,
                    last_ds_final_block: view.last_ds_final_block,
                    approvals: view.approvals.clone(),
                    latest_protocol_version: view.latest_protocol_version,
                },
                signature: view.signature,
                hash: CryptoHash::default(),
            };
            header.init();
            BlockHeader::BlockHeaderV1(Arc::new(header))
        } else if view.latest_protocol_version <= LAST_HEADER_V2_VERSION {
            let validator_proposals = view
                .validator_proposals
                .into_iter()
                .map(|v| v.into_validator_stake().into_v1())
                .collect();
            let mut header = BlockHeaderV2 {
                prev_hash: view.prev_hash,
                inner_lite,
                inner_rest: BlockHeaderInnerRestV2 {
                    prev_chunk_outgoing_receipts_root: view.chunk_receipts_root,
                    chunk_headers_root: view.chunk_headers_root,
                    chunk_tx_root: view.chunk_tx_root,
                    challenges_root: view.challenges_root,
                    random_value: view.random_value,
                    prev_validator_proposals: validator_proposals,
                    chunk_mask: view.chunk_mask,
                    next_gas_price: view.gas_price,
                    total_supply: view.total_supply,
                    challenges_result: view.challenges_result,
                    last_final_block: view.last_final_block,
                    last_ds_final_block: view.last_ds_final_block,
                    approvals: view.approvals.clone(),
                    latest_protocol_version: view.latest_protocol_version,
                },
                signature: view.signature,
                hash: CryptoHash::default(),
            };
            header.init();
            BlockHeader::BlockHeaderV2(Arc::new(header))
        } else if !checked_feature!("stable", BlockHeaderV4, view.latest_protocol_version) {
            let mut header = BlockHeaderV3 {
                prev_hash: view.prev_hash,
                inner_lite,
                inner_rest: BlockHeaderInnerRestV3 {
                    prev_chunk_outgoing_receipts_root: view.chunk_receipts_root,
                    chunk_headers_root: view.chunk_headers_root,
                    chunk_tx_root: view.chunk_tx_root,
                    challenges_root: view.challenges_root,
                    random_value: view.random_value,
                    prev_validator_proposals: view
                        .validator_proposals
                        .into_iter()
                        .map(Into::into)
                        .collect(),
                    chunk_mask: view.chunk_mask,
                    next_gas_price: view.gas_price,
                    block_ordinal: view.block_ordinal.unwrap_or(0),
                    total_supply: view.total_supply,
                    challenges_result: view.challenges_result,
                    last_final_block: view.last_final_block,
                    last_ds_final_block: view.last_ds_final_block,
                    prev_height: view.prev_height.unwrap_or_default(),
                    epoch_sync_data_hash: view.epoch_sync_data_hash,
                    approvals: view.approvals.clone(),
                    latest_protocol_version: view.latest_protocol_version,
                },
                signature: view.signature,
                hash: CryptoHash::default(),
            };
            header.init();
            BlockHeader::BlockHeaderV3(Arc::new(header))
        } else {
            let mut header = BlockHeaderV4 {
                prev_hash: view.prev_hash,
                inner_lite,
                inner_rest: BlockHeaderInnerRestV4 {
                    block_body_hash: view.block_body_hash.unwrap_or_default(),
                    prev_chunk_outgoing_receipts_root: view.chunk_receipts_root,
                    chunk_headers_root: view.chunk_headers_root,
                    chunk_tx_root: view.chunk_tx_root,
                    challenges_root: view.challenges_root,
                    random_value: view.random_value,
                    prev_validator_proposals: view
                        .validator_proposals
                        .into_iter()
                        .map(Into::into)
                        .collect(),
                    chunk_mask: view.chunk_mask,
                    next_gas_price: view.gas_price,
                    block_ordinal: view.block_ordinal.unwrap_or(0),
                    total_supply: view.total_supply,
                    challenges_result: view.challenges_result,
                    last_final_block: view.last_final_block,
                    last_ds_final_block: view.last_ds_final_block,
                    prev_height: view.prev_height.unwrap_or_default(),
                    epoch_sync_data_hash: view.epoch_sync_data_hash,
                    approvals: view.approvals.clone(),
                    latest_protocol_version: view.latest_protocol_version,
                },
                signature: view.signature,
                hash: CryptoHash::default(),
            };
            header.init();
            BlockHeader::BlockHeaderV4(Arc::new(header))
        }
    }
}

#[derive(
    PartialEq,
    Eq,
    Debug,
    Clone,
    BorshDeserialize,
    BorshSerialize,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct BlockHeaderInnerLiteView {
    pub height: BlockHeight,
    pub epoch_id: CryptoHash,
    pub next_epoch_id: CryptoHash,
    pub prev_state_root: CryptoHash,
    pub outcome_root: CryptoHash,
    /// Legacy json number. Should not be used.
    pub timestamp: u64,
    #[serde(with = "dec_format")]
    pub timestamp_nanosec: u64,
    pub next_bp_hash: CryptoHash,
    pub block_merkle_root: CryptoHash,
}

impl From<BlockHeader> for BlockHeaderInnerLiteView {
    fn from(header: BlockHeader) -> Self {
        let inner_lite = match &header {
            BlockHeader::BlockHeaderV1(header) => &header.inner_lite,
            BlockHeader::BlockHeaderV2(header) => &header.inner_lite,
            BlockHeader::BlockHeaderV3(header) => &header.inner_lite,
            BlockHeader::BlockHeaderV4(header) => &header.inner_lite,
        };
        BlockHeaderInnerLiteView {
            height: inner_lite.height,
            epoch_id: inner_lite.epoch_id.0,
            next_epoch_id: inner_lite.next_epoch_id.0,
            prev_state_root: inner_lite.prev_state_root,
            outcome_root: inner_lite.prev_outcome_root,
            timestamp: inner_lite.timestamp,
            timestamp_nanosec: inner_lite.timestamp,
            next_bp_hash: inner_lite.next_bp_hash,
            block_merkle_root: inner_lite.block_merkle_root,
        }
    }
}

impl From<BlockHeaderInnerLiteView> for BlockHeaderInnerLite {
    fn from(view: BlockHeaderInnerLiteView) -> Self {
        BlockHeaderInnerLite {
            height: view.height,
            epoch_id: EpochId(view.epoch_id),
            next_epoch_id: EpochId(view.next_epoch_id),
            prev_state_root: view.prev_state_root,
            prev_outcome_root: view.outcome_root,
            timestamp: view.timestamp_nanosec,
            next_bp_hash: view.next_bp_hash,
            block_merkle_root: view.block_merkle_root,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct ChunkHeaderView {
    pub chunk_hash: CryptoHash,
    pub prev_block_hash: CryptoHash,
    pub outcome_root: CryptoHash,
    pub prev_state_root: StateRoot,
    pub encoded_merkle_root: CryptoHash,
    pub encoded_length: u64,
    pub height_created: BlockHeight,
    pub height_included: BlockHeight,
    pub shard_id: ShardId,
    pub gas_used: Gas,
    pub gas_limit: Gas,
    /// TODO(2271): deprecated.
    #[serde(with = "dec_format")]
    pub rent_paid: Balance,
    /// TODO(2271): deprecated.
    #[serde(with = "dec_format")]
    pub validator_reward: Balance,
    #[serde(with = "dec_format")]
    pub balance_burnt: Balance,
    pub outgoing_receipts_root: CryptoHash,
    pub tx_root: CryptoHash,
    pub validator_proposals: Vec<ValidatorStakeView>,
    pub congestion_info: Option<CongestionInfoView>,
    pub signature: Signature,
}

impl From<ShardChunkHeader> for ChunkHeaderView {
    fn from(chunk: ShardChunkHeader) -> Self {
        let hash = chunk.chunk_hash();
        let signature = chunk.signature().clone();
        let height_included = chunk.height_included();
        let inner = chunk.take_inner();
        ChunkHeaderView {
            chunk_hash: hash.0,
            prev_block_hash: *inner.prev_block_hash(),
            outcome_root: *inner.prev_outcome_root(),
            prev_state_root: *inner.prev_state_root(),
            encoded_merkle_root: *inner.encoded_merkle_root(),
            encoded_length: inner.encoded_length(),
            height_created: inner.height_created(),
            height_included,
            shard_id: inner.shard_id(),
            gas_used: inner.prev_gas_used(),
            gas_limit: inner.gas_limit(),
            rent_paid: 0,
            validator_reward: 0,
            balance_burnt: inner.prev_balance_burnt(),
            outgoing_receipts_root: *inner.prev_outgoing_receipts_root(),
            tx_root: *inner.tx_root(),
            validator_proposals: inner.prev_validator_proposals().map(Into::into).collect(),
            congestion_info: inner.congestion_info().map(Into::into),
            signature,
        }
    }
}

impl From<ChunkHeaderView> for ShardChunkHeader {
    fn from(view: ChunkHeaderView) -> Self {
        if let Some(congestion_info) = view.congestion_info {
            let mut header = ShardChunkHeaderV3 {
                inner: ShardChunkHeaderInner::V3(ShardChunkHeaderInnerV3 {
                    prev_block_hash: view.prev_block_hash,
                    prev_state_root: view.prev_state_root,
                    prev_outcome_root: view.outcome_root,
                    encoded_merkle_root: view.encoded_merkle_root,
                    encoded_length: view.encoded_length,
                    height_created: view.height_created,
                    shard_id: view.shard_id,
                    prev_gas_used: view.gas_used,
                    gas_limit: view.gas_limit,
                    prev_balance_burnt: view.balance_burnt,
                    prev_outgoing_receipts_root: view.outgoing_receipts_root,
                    tx_root: view.tx_root,
                    prev_validator_proposals: view
                        .validator_proposals
                        .into_iter()
                        .map(Into::into)
                        .collect(),
                    congestion_info: congestion_info.into(),
                }),
                height_included: view.height_included,
                signature: view.signature,
                hash: ChunkHash::default(),
            };
            header.init();
            ShardChunkHeader::V3(header)
        } else {
            let mut header = ShardChunkHeaderV3 {
                inner: ShardChunkHeaderInner::V2(ShardChunkHeaderInnerV2 {
                    prev_block_hash: view.prev_block_hash,
                    prev_state_root: view.prev_state_root,
                    prev_outcome_root: view.outcome_root,
                    encoded_merkle_root: view.encoded_merkle_root,
                    encoded_length: view.encoded_length,
                    height_created: view.height_created,
                    shard_id: view.shard_id,
                    prev_gas_used: view.gas_used,
                    gas_limit: view.gas_limit,
                    prev_balance_burnt: view.balance_burnt,
                    prev_outgoing_receipts_root: view.outgoing_receipts_root,
                    tx_root: view.tx_root,
                    prev_validator_proposals: view
                        .validator_proposals
                        .into_iter()
                        .map(Into::into)
                        .collect(),
                }),
                height_included: view.height_included,
                signature: view.signature,
                hash: ChunkHash::default(),
            };
            header.init();
            ShardChunkHeader::V3(header)
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct BlockView {
    pub author: AccountId,
    pub header: BlockHeaderView,
    pub chunks: Vec<ChunkHeaderView>,
}

impl BlockView {
    pub fn from_author_block(author: AccountId, block: Block) -> Self {
        BlockView {
            author,
            header: block.header().clone().into(),
            chunks: block.chunks().iter().cloned().map(Into::into).collect(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ChunkView {
    pub author: AccountId,
    pub header: ChunkHeaderView,
    pub transactions: Vec<SignedTransactionView>,
    pub receipts: Vec<ReceiptView>,
}

impl ChunkView {
    pub fn from_author_chunk(author: AccountId, chunk: ShardChunk) -> Self {
        match chunk {
            ShardChunk::V1(chunk) => Self {
                author,
                header: ShardChunkHeader::V1(chunk.header).into(),
                transactions: chunk.transactions.into_iter().map(Into::into).collect(),
                receipts: chunk.prev_outgoing_receipts.into_iter().map(Into::into).collect(),
            },
            ShardChunk::V2(chunk) => Self {
                author,
                header: chunk.header.into(),
                transactions: chunk.transactions.into_iter().map(Into::into).collect(),
                receipts: chunk.prev_outgoing_receipts.into_iter().map(Into::into).collect(),
            },
        }
    }
}

#[serde_as]
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum ActionView {
    CreateAccount,
    DeployContract {
        #[serde_as(as = "Base64")]
        code: Vec<u8>,
    },
    FunctionCall {
        method_name: String,
        args: FunctionArgs,
        gas: Gas,
        #[serde(with = "dec_format")]
        deposit: Balance,
    },
    Transfer {
        #[serde(with = "dec_format")]
        deposit: Balance,
    },
    #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
    NonrefundableStorageTransfer {
        #[serde(with = "dec_format")]
        deposit: Balance,
    },
    Stake {
        #[serde(with = "dec_format")]
        stake: Balance,
        public_key: PublicKey,
    },
    AddKey {
        public_key: PublicKey,
        access_key: AccessKeyView,
    },
    DeleteKey {
        public_key: PublicKey,
    },
    DeleteAccount {
        beneficiary_id: AccountId,
    },
    Delegate {
        delegate_action: DelegateAction,
        signature: Signature,
    },
}

impl From<Action> for ActionView {
    fn from(action: Action) -> Self {
        match action {
            Action::CreateAccount(_) => ActionView::CreateAccount,
            Action::DeployContract(action) => {
                let code = hash(&action.code).as_ref().to_vec();
                ActionView::DeployContract { code }
            }
            Action::FunctionCall(action) => ActionView::FunctionCall {
                method_name: action.method_name,
                args: action.args.into(),
                gas: action.gas,
                deposit: action.deposit,
            },
            Action::Transfer(action) => ActionView::Transfer { deposit: action.deposit },
            #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
            Action::NonrefundableStorageTransfer(action) => {
                ActionView::NonrefundableStorageTransfer { deposit: action.deposit }
            }
            Action::Stake(action) => {
                ActionView::Stake { stake: action.stake, public_key: action.public_key }
            }
            Action::AddKey(action) => ActionView::AddKey {
                public_key: action.public_key,
                access_key: action.access_key.into(),
            },
            Action::DeleteKey(action) => ActionView::DeleteKey { public_key: action.public_key },
            Action::DeleteAccount(action) => {
                ActionView::DeleteAccount { beneficiary_id: action.beneficiary_id }
            }
            Action::Delegate(action) => ActionView::Delegate {
                delegate_action: action.delegate_action,
                signature: action.signature,
            },
        }
    }
}

impl TryFrom<ActionView> for Action {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(action_view: ActionView) -> Result<Self, Self::Error> {
        Ok(match action_view {
            ActionView::CreateAccount => Action::CreateAccount(CreateAccountAction {}),
            ActionView::DeployContract { code } => {
                Action::DeployContract(DeployContractAction { code })
            }
            ActionView::FunctionCall { method_name, args, gas, deposit } => {
                Action::FunctionCall(Box::new(FunctionCallAction {
                    method_name,
                    args: args.into(),
                    gas,
                    deposit,
                }))
            }
            ActionView::Transfer { deposit } => Action::Transfer(TransferAction { deposit }),
            #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
            ActionView::NonrefundableStorageTransfer { deposit } => {
                Action::NonrefundableStorageTransfer(NonrefundableStorageTransferAction { deposit })
            }
            ActionView::Stake { stake, public_key } => {
                Action::Stake(Box::new(StakeAction { stake, public_key }))
            }
            ActionView::AddKey { public_key, access_key } => {
                Action::AddKey(Box::new(AddKeyAction { public_key, access_key: access_key.into() }))
            }
            ActionView::DeleteKey { public_key } => {
                Action::DeleteKey(Box::new(DeleteKeyAction { public_key }))
            }
            ActionView::DeleteAccount { beneficiary_id } => {
                Action::DeleteAccount(DeleteAccountAction { beneficiary_id })
            }
            ActionView::Delegate { delegate_action, signature } => {
                Action::Delegate(Box::new(SignedDelegateAction { delegate_action, signature }))
            }
        })
    }
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    PartialEq,
    Eq,
    Clone,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct SignedTransactionView {
    pub signer_id: AccountId,
    pub public_key: PublicKey,
    pub nonce: Nonce,
    pub receiver_id: AccountId,
    pub actions: Vec<ActionView>,
    pub priority_fee: u64,
    pub signature: Signature,
    pub hash: CryptoHash,
}

impl From<SignedTransaction> for SignedTransactionView {
    fn from(signed_tx: SignedTransaction) -> Self {
        let hash = signed_tx.get_hash();
        let transaction = signed_tx.transaction;
        let priority_fee = transaction.priority_fee().unwrap_or_default();
        SignedTransactionView {
            signer_id: transaction.signer_id().clone(),
            public_key: transaction.public_key().clone(),
            nonce: transaction.nonce(),
            receiver_id: transaction.receiver_id().clone(),
            actions: transaction.take_actions().into_iter().map(|action| action.into()).collect(),
            signature: signed_tx.signature,
            hash,
            priority_fee,
        }
    }
}

#[serde_as]
#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    PartialEq,
    Eq,
    Clone,
    Default,
)]
pub enum FinalExecutionStatus {
    /// The execution has not yet started.
    #[default]
    NotStarted,
    /// The execution has started and still going.
    Started,
    /// The execution has failed with the given error.
    Failure(TxExecutionError),
    /// The execution has succeeded and returned some value or an empty vec encoded in base64.
    SuccessValue(#[serde_as(as = "Base64")] Vec<u8>),
}

impl fmt::Debug for FinalExecutionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FinalExecutionStatus::NotStarted => f.write_str("NotStarted"),
            FinalExecutionStatus::Started => f.write_str("Started"),
            FinalExecutionStatus::Failure(e) => f.write_fmt(format_args!("Failure({:?})", e)),
            FinalExecutionStatus::SuccessValue(v) => {
                f.write_fmt(format_args!("SuccessValue({})", AbbrBytes(v)))
            }
        }
    }
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    PartialEq,
    Eq,
    Clone,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum ServerError {
    TxExecutionError(TxExecutionError),
    Timeout,
    Closed,
}

#[serde_as]
#[derive(
    BorshSerialize, BorshDeserialize, serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone,
)]
pub enum ExecutionStatusView {
    /// The execution is pending or unknown.
    Unknown,
    /// The execution has failed.
    Failure(TxExecutionError),
    /// The final action succeeded and returned some value or an empty vec encoded in base64.
    SuccessValue(#[serde_as(as = "Base64")] Vec<u8>),
    /// The final action of the receipt returned a promise or the signed transaction was converted
    /// to a receipt. Contains the receipt_id of the generated receipt.
    SuccessReceiptId(CryptoHash),
}

impl fmt::Debug for ExecutionStatusView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutionStatusView::Unknown => f.write_str("Unknown"),
            ExecutionStatusView::Failure(e) => f.write_fmt(format_args!("Failure({:?})", e)),
            ExecutionStatusView::SuccessValue(v) => {
                f.write_fmt(format_args!("SuccessValue({})", AbbrBytes(v)))
            }
            ExecutionStatusView::SuccessReceiptId(receipt_id) => {
                f.write_fmt(format_args!("SuccessReceiptId({})", receipt_id))
            }
        }
    }
}

impl From<ExecutionStatus> for ExecutionStatusView {
    fn from(outcome: ExecutionStatus) -> Self {
        match outcome {
            ExecutionStatus::Unknown => ExecutionStatusView::Unknown,
            ExecutionStatus::Failure(e) => ExecutionStatusView::Failure(e),
            ExecutionStatus::SuccessValue(v) => ExecutionStatusView::SuccessValue(v),
            ExecutionStatus::SuccessReceiptId(receipt_id) => {
                ExecutionStatusView::SuccessReceiptId(receipt_id)
            }
        }
    }
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Clone,
    Eq,
    Debug,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct CostGasUsed {
    pub cost_category: String,
    pub cost: String,
    #[serde(with = "dec_format")]
    pub gas_used: Gas,
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Clone,
    Eq,
    Debug,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct ExecutionMetadataView {
    pub version: u32,
    pub gas_profile: Option<Vec<CostGasUsed>>,
}

impl Default for ExecutionMetadataView {
    fn default() -> Self {
        ExecutionMetadata::V1.into()
    }
}

impl From<ExecutionMetadata> for ExecutionMetadataView {
    fn from(metadata: ExecutionMetadata) -> Self {
        let version = match metadata {
            ExecutionMetadata::V1 => 1,
            ExecutionMetadata::V2(_) => 2,
            ExecutionMetadata::V3(_) => 3,
        };
        let mut gas_profile = match metadata {
            ExecutionMetadata::V1 => None,
            ExecutionMetadata::V2(profile_data) => {
                // Add actions, wasm op, and ext costs in groups.

                // actions should use the old format, since `ActionCosts`
                // includes more detailed entries than were present in the old
                // profile
                let mut costs: Vec<CostGasUsed> = profile_data
                    .legacy_action_costs()
                    .into_iter()
                    .filter(|&(_, gas)| gas > 0)
                    .map(|(name, gas)| CostGasUsed::action(name.to_string(), gas))
                    .collect();

                // wasm op is a single cost, for historical reasons it is inaccurately displayed as "wasm host"
                costs.push(CostGasUsed::wasm_host(
                    "WASM_INSTRUCTION".to_string(),
                    profile_data.get_wasm_cost(),
                ));

                // ext costs are 1-to-1, except for those added later which we will display as 0
                for ext_cost in ExtCosts::iter() {
                    costs.push(CostGasUsed::wasm_host(
                        format!("{:?}", ext_cost).to_ascii_uppercase(),
                        profile_data.get_ext_cost(ext_cost),
                    ));
                }

                Some(costs)
            }
            ExecutionMetadata::V3(profile) => {
                // Add actions, wasm op, and ext costs in groups.
                // actions costs are 1-to-1
                let mut costs: Vec<CostGasUsed> = ActionCosts::iter()
                    .flat_map(|cost| {
                        let gas_used = profile.get_action_cost(cost);
                        (gas_used > 0).then(|| {
                            CostGasUsed::action(
                                format!("{:?}", cost).to_ascii_uppercase(),
                                gas_used,
                            )
                        })
                    })
                    .collect();

                // wasm op is a single cost, for historical reasons it is inaccurately displayed as "wasm host"
                let wasm_gas_used = profile.get_wasm_cost();
                if wasm_gas_used > 0 {
                    costs.push(CostGasUsed::wasm_host(
                        "WASM_INSTRUCTION".to_string(),
                        wasm_gas_used,
                    ));
                }

                // ext costs are 1-to-1
                for ext_cost in ExtCosts::iter() {
                    let gas_used = profile.get_ext_cost(ext_cost);
                    if gas_used > 0 {
                        costs.push(CostGasUsed::wasm_host(
                            format!("{:?}", ext_cost).to_ascii_uppercase(),
                            gas_used,
                        ));
                    }
                }

                Some(costs)
            }
        };
        if let Some(ref mut costs) = gas_profile {
            // The order doesn't really matter, but the default one is just
            // historical, which is especially unintuitive, so let's sort
            // lexicographically.
            //
            // Can't `sort_by_key` here because lifetime inference in
            // closures is limited.
            costs.sort_by(|lhs, rhs| {
                lhs.cost_category.cmp(&rhs.cost_category).then_with(|| lhs.cost.cmp(&rhs.cost))
            });
        }
        ExecutionMetadataView { version, gas_profile }
    }
}

impl CostGasUsed {
    pub fn action(cost: String, gas_used: Gas) -> Self {
        Self { cost_category: "ACTION_COST".to_string(), cost, gas_used }
    }

    pub fn wasm_host(cost: String, gas_used: Gas) -> Self {
        Self { cost_category: "WASM_HOST_COST".to_string(), cost, gas_used }
    }
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct ExecutionOutcomeView {
    /// Logs from this transaction or receipt.
    pub logs: Vec<String>,
    /// Receipt IDs generated by this transaction or receipt.
    pub receipt_ids: Vec<CryptoHash>,
    /// The amount of the gas burnt by the given transaction or receipt.
    pub gas_burnt: Gas,
    /// The amount of tokens burnt corresponding to the burnt gas amount.
    /// This value doesn't always equal to the `gas_burnt` multiplied by the gas price, because
    /// the prepaid gas price might be lower than the actual gas price and it creates a deficit.
    #[serde(with = "dec_format")]
    pub tokens_burnt: Balance,
    /// The id of the account on which the execution happens. For transaction this is signer_id,
    /// for receipt this is receiver_id.
    pub executor_id: AccountId,
    /// Execution status. Contains the result in case of successful execution.
    pub status: ExecutionStatusView,
    /// Execution metadata, versioned
    #[serde(default)]
    pub metadata: ExecutionMetadataView,
}

impl From<ExecutionOutcome> for ExecutionOutcomeView {
    fn from(outcome: ExecutionOutcome) -> Self {
        Self {
            logs: outcome.logs,
            receipt_ids: outcome.receipt_ids,
            gas_burnt: outcome.gas_burnt,
            tokens_burnt: outcome.tokens_burnt,
            executor_id: outcome.executor_id,
            status: outcome.status.into(),
            metadata: outcome.metadata.into(),
        }
    }
}

impl From<&ExecutionOutcomeView> for PartialExecutionOutcome {
    fn from(outcome: &ExecutionOutcomeView) -> Self {
        Self {
            receipt_ids: outcome.receipt_ids.clone(),
            gas_burnt: outcome.gas_burnt,
            tokens_burnt: outcome.tokens_burnt,
            executor_id: outcome.executor_id.clone(),
            status: outcome.status.clone().into(),
        }
    }
}
impl From<ExecutionStatusView> for PartialExecutionStatus {
    fn from(status: ExecutionStatusView) -> PartialExecutionStatus {
        match status {
            ExecutionStatusView::Unknown => PartialExecutionStatus::Unknown,
            ExecutionStatusView::Failure(_) => PartialExecutionStatus::Failure,
            ExecutionStatusView::SuccessValue(value) => PartialExecutionStatus::SuccessValue(value),
            ExecutionStatusView::SuccessReceiptId(id) => {
                PartialExecutionStatus::SuccessReceiptId(id)
            }
        }
    }
}

impl ExecutionOutcomeView {
    // Same behavior as ExecutionOutcomeWithId's to_hashes.
    pub fn to_hashes(&self, id: CryptoHash) -> Vec<CryptoHash> {
        let mut result = Vec::with_capacity(self.logs.len().saturating_add(2));
        result.push(id);
        result.push(CryptoHash::hash_borsh(&PartialExecutionOutcome::from(self)));
        result.extend(self.logs.iter().map(|log| hash(log.as_bytes())));
        result
    }
}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    PartialEq,
    Eq,
    Clone,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct ExecutionOutcomeWithIdView {
    pub proof: MerklePath,
    pub block_hash: CryptoHash,
    pub id: CryptoHash,
    pub outcome: ExecutionOutcomeView,
}

impl From<ExecutionOutcomeWithIdAndProof> for ExecutionOutcomeWithIdView {
    fn from(outcome_with_id_and_proof: ExecutionOutcomeWithIdAndProof) -> Self {
        Self {
            proof: outcome_with_id_and_proof.proof,
            block_hash: outcome_with_id_and_proof.block_hash,
            id: outcome_with_id_and_proof.outcome_with_id.id,
            outcome: outcome_with_id_and_proof.outcome_with_id.outcome.into(),
        }
    }
}

impl ExecutionOutcomeWithIdView {
    pub fn to_hashes(&self) -> Vec<CryptoHash> {
        self.outcome.to_hashes(self.id)
    }
}
#[derive(Clone, Debug)]
pub struct TxStatusView {
    pub execution_outcome: Option<FinalExecutionOutcomeViewEnum>,
    pub status: TxExecutionStatus,
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Debug,
    Default,
    Eq,
    PartialEq,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TxExecutionStatus {
    /// Transaction is waiting to be included into the block
    None,
    /// Transaction is included into the block. The block may be not finalised yet
    Included,
    /// Transaction is included into the block +
    /// All non-refund transaction receipts finished their execution.
    /// The corresponding blocks for tx and each receipt may be not finalised yet
    #[default]
    ExecutedOptimistic,
    /// Transaction is included into finalised block
    IncludedFinal,
    /// Transaction is included into finalised block +
    /// All non-refund transaction receipts finished their execution.
    /// The corresponding blocks for each receipt may be not finalised yet
    Executed,
    /// Transaction is included into finalised block +
    /// Execution of all transaction receipts is finalised, including refund receipts
    Final,
}

#[derive(BorshSerialize, BorshDeserialize, serde::Serialize, serde::Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum FinalExecutionOutcomeViewEnum {
    FinalExecutionOutcome(FinalExecutionOutcomeView),
    FinalExecutionOutcomeWithReceipt(FinalExecutionOutcomeWithReceiptView),
}

impl FinalExecutionOutcomeViewEnum {
    pub fn into_outcome(self) -> FinalExecutionOutcomeView {
        match self {
            Self::FinalExecutionOutcome(outcome) => outcome,
            Self::FinalExecutionOutcomeWithReceipt(outcome) => outcome.final_outcome,
        }
    }
}

impl TxStatusView {
    pub fn into_outcome(self) -> Option<FinalExecutionOutcomeView> {
        self.execution_outcome.map(|outcome| match outcome {
            FinalExecutionOutcomeViewEnum::FinalExecutionOutcome(outcome) => outcome,
            FinalExecutionOutcomeViewEnum::FinalExecutionOutcomeWithReceipt(outcome) => {
                outcome.final_outcome
            }
        })
    }
}

/// Execution outcome of the transaction and all the subsequent receipts.
/// Could be not finalised yet
#[derive(
    BorshSerialize, BorshDeserialize, serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone,
)]
pub struct FinalExecutionOutcomeView {
    /// Execution status defined by chain.rs:get_final_transaction_result
    /// FinalExecutionStatus::NotStarted - the tx is not converted to the receipt yet
    /// FinalExecutionStatus::Started - we have at least 1 receipt, but the first leaf receipt_id (using dfs) hasn't finished the execution
    /// FinalExecutionStatus::Failure - the result of the first leaf receipt_id
    /// FinalExecutionStatus::SuccessValue - the result of the first leaf receipt_id
    pub status: FinalExecutionStatus,
    /// Signed Transaction
    pub transaction: SignedTransactionView,
    /// The execution outcome of the signed transaction.
    pub transaction_outcome: ExecutionOutcomeWithIdView,
    /// The execution outcome of receipts.
    pub receipts_outcome: Vec<ExecutionOutcomeWithIdView>,
}

impl fmt::Debug for FinalExecutionOutcomeView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FinalExecutionOutcome")
            .field("status", &self.status)
            .field("transaction", &self.transaction)
            .field("transaction_outcome", &self.transaction_outcome)
            .field("receipts_outcome", &Slice(&self.receipts_outcome))
            .finish()
    }
}

/// Final execution outcome of the transaction and all of subsequent the receipts. Also includes
/// the generated receipt.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct FinalExecutionOutcomeWithReceiptView {
    /// Final outcome view without receipts
    #[serde(flatten)]
    pub final_outcome: FinalExecutionOutcomeView,
    /// Receipts generated from the transaction
    pub receipts: Vec<ReceiptView>,
}

pub mod validator_stake_view {
    pub use super::ValidatorStakeViewV1;
    use crate::types::validator_stake::ValidatorStake;
    use borsh::{BorshDeserialize, BorshSerialize};
    use near_primitives_core::types::AccountId;
    use serde::Deserialize;

    #[derive(
        BorshSerialize, BorshDeserialize, serde::Serialize, Deserialize, Debug, Clone, Eq, PartialEq,
    )]
    #[serde(tag = "validator_stake_struct_version")]
    pub enum ValidatorStakeView {
        V1(ValidatorStakeViewV1),
    }

    impl ValidatorStakeView {
        pub fn into_validator_stake(self) -> ValidatorStake {
            self.into()
        }

        #[inline]
        pub fn take_account_id(self) -> AccountId {
            match self {
                Self::V1(v1) => v1.account_id,
            }
        }

        #[inline]
        pub fn account_id(&self) -> &AccountId {
            match self {
                Self::V1(v1) => &v1.account_id,
            }
        }
    }

    impl From<ValidatorStake> for ValidatorStakeView {
        fn from(stake: ValidatorStake) -> Self {
            match stake {
                ValidatorStake::V1(v1) => Self::V1(ValidatorStakeViewV1 {
                    account_id: v1.account_id,
                    public_key: v1.public_key,
                    stake: v1.stake,
                }),
            }
        }
    }

    impl From<ValidatorStakeView> for ValidatorStake {
        fn from(view: ValidatorStakeView) -> Self {
            match view {
                ValidatorStakeView::V1(v1) => Self::new_v1(v1.account_id, v1.public_key, v1.stake),
            }
        }
    }
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    Clone,
    Eq,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct ValidatorStakeViewV1 {
    pub account_id: AccountId,
    pub public_key: PublicKey,
    #[serde(with = "dec_format")]
    pub stake: Balance,
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct ReceiptView {
    pub predecessor_id: AccountId,
    pub receiver_id: AccountId,
    pub receipt_id: CryptoHash,

    pub receipt: ReceiptEnumView,
    pub priority: u64,
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct DataReceiverView {
    pub data_id: CryptoHash,
    pub receiver_id: AccountId,
}

#[serde_as]
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum ReceiptEnumView {
    Action {
        signer_id: AccountId,
        signer_public_key: PublicKey,
        #[serde(with = "dec_format")]
        gas_price: Balance,
        output_data_receivers: Vec<DataReceiverView>,
        input_data_ids: Vec<CryptoHash>,
        actions: Vec<ActionView>,
        #[serde(default = "default_is_promise")]
        is_promise_yield: bool,
    },
    Data {
        data_id: CryptoHash,
        #[serde_as(as = "Option<Base64>")]
        data: Option<Vec<u8>>,
        #[serde(default = "default_is_promise")]
        is_promise_resume: bool,
    },
}

// Default value used when deserializing ReceiptEnumViews which are missing either the
// `is_promise_yield` or `is_promise_resume` fields. Data which is missing this field was
// serialized before the introduction of yield execution.
fn default_is_promise() -> bool {
    false
}

impl From<Receipt> for ReceiptView {
    fn from(receipt: Receipt) -> Self {
        let is_promise_yield = matches!(receipt.receipt(), ReceiptEnum::PromiseYield(_));
        let is_promise_resume = matches!(receipt.receipt(), ReceiptEnum::PromiseResume(_));
        let priority = receipt.priority().value();

        ReceiptView {
            predecessor_id: receipt.predecessor_id().clone(),
            receiver_id: receipt.receiver_id().clone(),
            receipt_id: *receipt.receipt_id(),
            receipt: match receipt.take_receipt() {
                ReceiptEnum::Action(action_receipt) | ReceiptEnum::PromiseYield(action_receipt) => {
                    ReceiptEnumView::Action {
                        signer_id: action_receipt.signer_id,
                        signer_public_key: action_receipt.signer_public_key,
                        gas_price: action_receipt.gas_price,
                        output_data_receivers: action_receipt
                            .output_data_receivers
                            .into_iter()
                            .map(|data_receiver| DataReceiverView {
                                data_id: data_receiver.data_id,
                                receiver_id: data_receiver.receiver_id,
                            })
                            .collect(),
                        input_data_ids: action_receipt
                            .input_data_ids
                            .into_iter()
                            .map(Into::into)
                            .collect(),
                        actions: action_receipt.actions.into_iter().map(Into::into).collect(),
                        is_promise_yield,
                    }
                }
                ReceiptEnum::Data(data_receipt) | ReceiptEnum::PromiseResume(data_receipt) => {
                    ReceiptEnumView::Data {
                        data_id: data_receipt.data_id,
                        data: data_receipt.data,
                        is_promise_resume,
                    }
                }
            },
            priority,
        }
    }
}

impl TryFrom<ReceiptView> for Receipt {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(receipt_view: ReceiptView) -> Result<Self, Self::Error> {
        Ok(Receipt::V1(ReceiptV1 {
            predecessor_id: receipt_view.predecessor_id,
            receiver_id: receipt_view.receiver_id,
            receipt_id: receipt_view.receipt_id,
            receipt: match receipt_view.receipt {
                ReceiptEnumView::Action {
                    signer_id,
                    signer_public_key,
                    gas_price,
                    output_data_receivers,
                    input_data_ids,
                    actions,
                    is_promise_yield,
                } => {
                    let action_receipt = ActionReceipt {
                        signer_id,
                        signer_public_key,
                        gas_price,
                        output_data_receivers: output_data_receivers
                            .into_iter()
                            .map(|data_receiver_view| DataReceiver {
                                data_id: data_receiver_view.data_id,
                                receiver_id: data_receiver_view.receiver_id,
                            })
                            .collect(),
                        input_data_ids: input_data_ids.into_iter().map(Into::into).collect(),
                        actions: actions
                            .into_iter()
                            .map(TryInto::try_into)
                            .collect::<Result<Vec<_>, _>>()?,
                    };

                    if is_promise_yield {
                        ReceiptEnum::PromiseYield(action_receipt)
                    } else {
                        ReceiptEnum::Action(action_receipt)
                    }
                }
                ReceiptEnumView::Data { data_id, data, is_promise_resume } => {
                    let data_receipt = DataReceipt { data_id, data };

                    if is_promise_resume {
                        ReceiptEnum::PromiseResume(data_receipt)
                    } else {
                        ReceiptEnum::Data(data_receipt)
                    }
                }
            },
            priority: receipt_view.priority,
        }))
    }
}

/// Information about this epoch validators and next epoch validators
#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Clone, ProtocolSchema)]
pub struct EpochValidatorInfo {
    /// Validators for the current epoch
    pub current_validators: Vec<CurrentEpochValidatorInfo>,
    /// Validators for the next epoch
    pub next_validators: Vec<NextEpochValidatorInfo>,
    /// Fishermen for the current epoch
    pub current_fishermen: Vec<ValidatorStakeView>,
    /// Fishermen for the next epoch
    pub next_fishermen: Vec<ValidatorStakeView>,
    /// Proposals in the current epoch
    pub current_proposals: Vec<ValidatorStakeView>,
    /// Kickout in the previous epoch
    pub prev_epoch_kickout: Vec<ValidatorKickoutView>,
    /// Epoch start block height
    pub epoch_start_height: BlockHeight,
    /// Epoch height
    pub epoch_height: EpochHeight,
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    PartialEq,
    Eq,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
pub struct ValidatorKickoutView {
    pub account_id: AccountId,
    pub reason: ValidatorKickoutReason,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Clone, ProtocolSchema)]
pub struct CurrentEpochValidatorInfo {
    pub account_id: AccountId,
    pub public_key: PublicKey,
    pub is_slashed: bool,
    #[serde(with = "dec_format")]
    pub stake: Balance,
    pub shards: Vec<ShardId>,
    pub num_produced_blocks: NumBlocks,
    pub num_expected_blocks: NumBlocks,
    #[serde(default)]
    pub num_produced_chunks: NumBlocks,
    #[serde(default)]
    pub num_expected_chunks: NumBlocks,
    // The following two fields correspond to the shards in the shard array.
    #[serde(default)]
    pub num_produced_chunks_per_shard: Vec<NumBlocks>,
    #[serde(default)]
    pub num_expected_chunks_per_shard: Vec<NumBlocks>,
    #[serde(default, skip_serializing_if = "num_blocks_is_zero")]
    pub num_produced_endorsements: NumBlocks,
    #[serde(default, skip_serializing_if = "num_blocks_is_zero")]
    pub num_expected_endorsements: NumBlocks,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub num_produced_endorsements_per_shard: Vec<NumBlocks>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub num_expected_endorsements_per_shard: Vec<NumBlocks>,
}

fn num_blocks_is_zero(n: &NumBlocks) -> bool {
    n == &0
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    PartialEq,
    Eq,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
pub struct NextEpochValidatorInfo {
    pub account_id: AccountId,
    pub public_key: PublicKey,
    #[serde(with = "dec_format")]
    pub stake: Balance,
    pub shards: Vec<ShardId>,
}

#[derive(
    PartialEq,
    Eq,
    Debug,
    Clone,
    BorshDeserialize,
    BorshSerialize,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct LightClientBlockView {
    pub prev_block_hash: CryptoHash,
    pub next_block_inner_hash: CryptoHash,
    pub inner_lite: BlockHeaderInnerLiteView,
    pub inner_rest_hash: CryptoHash,
    pub next_bps: Option<Vec<ValidatorStakeView>>,
    pub approvals_after_next: Vec<Option<Box<Signature>>>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, BorshDeserialize, BorshSerialize)]
pub struct LightClientBlockLiteView {
    pub prev_block_hash: CryptoHash,
    pub inner_rest_hash: CryptoHash,
    pub inner_lite: BlockHeaderInnerLiteView,
}

impl From<BlockHeader> for LightClientBlockLiteView {
    fn from(header: BlockHeader) -> Self {
        Self {
            prev_block_hash: *header.prev_hash(),
            inner_rest_hash: hash(&header.inner_rest_bytes()),
            inner_lite: header.into(),
        }
    }
}
impl LightClientBlockLiteView {
    pub fn hash(&self) -> CryptoHash {
        let block_header_inner_lite: BlockHeaderInnerLite = self.inner_lite.clone().into();
        combine_hash(
            &combine_hash(
                &hash(&borsh::to_vec(&block_header_inner_lite).unwrap()),
                &self.inner_rest_hash,
            ),
            &self.prev_block_hash,
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct GasPriceView {
    #[serde(with = "dec_format")]
    pub gas_price: Balance,
}

/// It is a [serializable view] of [`StateChangesRequest`].
///
/// [serializable view]: ./index.html
/// [`StateChangesRequest`]: ../types/struct.StateChangesRequest.html
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "changes_type", rename_all = "snake_case")]
pub enum StateChangesRequestView {
    AccountChanges {
        account_ids: Vec<AccountId>,
    },
    SingleAccessKeyChanges {
        keys: Vec<AccountWithPublicKey>,
    },
    AllAccessKeyChanges {
        account_ids: Vec<AccountId>,
    },
    ContractCodeChanges {
        account_ids: Vec<AccountId>,
    },
    DataChanges {
        account_ids: Vec<AccountId>,
        #[serde(rename = "key_prefix_base64")]
        key_prefix: StoreKey,
    },
}

impl From<StateChangesRequestView> for StateChangesRequest {
    fn from(request: StateChangesRequestView) -> Self {
        match request {
            StateChangesRequestView::AccountChanges { account_ids } => {
                Self::AccountChanges { account_ids }
            }
            StateChangesRequestView::SingleAccessKeyChanges { keys } => {
                Self::SingleAccessKeyChanges { keys }
            }
            StateChangesRequestView::AllAccessKeyChanges { account_ids } => {
                Self::AllAccessKeyChanges { account_ids }
            }
            StateChangesRequestView::ContractCodeChanges { account_ids } => {
                Self::ContractCodeChanges { account_ids }
            }
            StateChangesRequestView::DataChanges { account_ids, key_prefix } => {
                Self::DataChanges { account_ids, key_prefix }
            }
        }
    }
}

/// It is a [serializable view] of [`StateChangeKind`].
///
/// [serializable view]: ./index.html
/// [`StateChangeKind`]: ../types/struct.StateChangeKind.html
#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum StateChangeKindView {
    AccountTouched { account_id: AccountId },
    AccessKeyTouched { account_id: AccountId },
    DataTouched { account_id: AccountId },
    ContractCodeTouched { account_id: AccountId },
}

impl From<StateChangeKind> for StateChangeKindView {
    fn from(state_change_kind: StateChangeKind) -> Self {
        match state_change_kind {
            StateChangeKind::AccountTouched { account_id } => Self::AccountTouched { account_id },
            StateChangeKind::AccessKeyTouched { account_id } => {
                Self::AccessKeyTouched { account_id }
            }
            StateChangeKind::DataTouched { account_id } => Self::DataTouched { account_id },
            StateChangeKind::ContractCodeTouched { account_id } => {
                Self::ContractCodeTouched { account_id }
            }
        }
    }
}

pub type StateChangesKindsView = Vec<StateChangeKindView>;

/// See crate::types::StateChangeCause for details.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum StateChangeCauseView {
    NotWritableToDisk,
    InitialState,
    TransactionProcessing { tx_hash: CryptoHash },
    ActionReceiptProcessingStarted { receipt_hash: CryptoHash },
    ActionReceiptGasReward { receipt_hash: CryptoHash },
    ReceiptProcessing { receipt_hash: CryptoHash },
    PostponedReceipt { receipt_hash: CryptoHash },
    UpdatedDelayedReceipts,
    ValidatorAccountsUpdate,
    Migration,
    Resharding,
}

impl From<StateChangeCause> for StateChangeCauseView {
    fn from(state_change_cause: StateChangeCause) -> Self {
        match state_change_cause {
            StateChangeCause::NotWritableToDisk => Self::NotWritableToDisk,
            StateChangeCause::InitialState => Self::InitialState,
            StateChangeCause::TransactionProcessing { tx_hash } => {
                Self::TransactionProcessing { tx_hash }
            }
            StateChangeCause::ActionReceiptProcessingStarted { receipt_hash } => {
                Self::ActionReceiptProcessingStarted { receipt_hash }
            }
            StateChangeCause::ActionReceiptGasReward { receipt_hash } => {
                Self::ActionReceiptGasReward { receipt_hash }
            }
            StateChangeCause::ReceiptProcessing { receipt_hash } => {
                Self::ReceiptProcessing { receipt_hash }
            }
            StateChangeCause::PostponedReceipt { receipt_hash } => {
                Self::PostponedReceipt { receipt_hash }
            }
            StateChangeCause::UpdatedDelayedReceipts => Self::UpdatedDelayedReceipts,
            StateChangeCause::ValidatorAccountsUpdate => Self::ValidatorAccountsUpdate,
            StateChangeCause::Migration => Self::Migration,
            StateChangeCause::Resharding => Self::Resharding,
        }
    }
}

#[serde_as]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "change")]
pub enum StateChangeValueView {
    AccountUpdate {
        account_id: AccountId,
        #[serde(flatten)]
        account: AccountView,
    },
    AccountDeletion {
        account_id: AccountId,
    },
    AccessKeyUpdate {
        account_id: AccountId,
        public_key: PublicKey,
        access_key: AccessKeyView,
    },
    AccessKeyDeletion {
        account_id: AccountId,
        public_key: PublicKey,
    },
    DataUpdate {
        account_id: AccountId,
        #[serde(rename = "key_base64")]
        key: StoreKey,
        #[serde(rename = "value_base64")]
        value: StoreValue,
    },
    DataDeletion {
        account_id: AccountId,
        #[serde(rename = "key_base64")]
        key: StoreKey,
    },
    ContractCodeUpdate {
        account_id: AccountId,
        #[serde(rename = "code_base64")]
        #[serde_as(as = "Base64")]
        code: Vec<u8>,
    },
    ContractCodeDeletion {
        account_id: AccountId,
    },
}

impl From<StateChangeValue> for StateChangeValueView {
    fn from(state_change: StateChangeValue) -> Self {
        match state_change {
            StateChangeValue::AccountUpdate { account_id, account } => {
                Self::AccountUpdate { account_id, account: account.into() }
            }
            StateChangeValue::AccountDeletion { account_id } => {
                Self::AccountDeletion { account_id }
            }
            StateChangeValue::AccessKeyUpdate { account_id, public_key, access_key } => {
                Self::AccessKeyUpdate { account_id, public_key, access_key: access_key.into() }
            }
            StateChangeValue::AccessKeyDeletion { account_id, public_key } => {
                Self::AccessKeyDeletion { account_id, public_key }
            }
            StateChangeValue::DataUpdate { account_id, key, value } => {
                Self::DataUpdate { account_id, key, value }
            }
            StateChangeValue::DataDeletion { account_id, key } => {
                Self::DataDeletion { account_id, key }
            }
            StateChangeValue::ContractCodeUpdate { account_id, code } => {
                Self::ContractCodeUpdate { account_id, code }
            }
            StateChangeValue::ContractCodeDeletion { account_id } => {
                Self::ContractCodeDeletion { account_id }
            }
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct StateChangeWithCauseView {
    pub cause: StateChangeCauseView,
    #[serde(flatten)]
    pub value: StateChangeValueView,
}

impl From<StateChangeWithCause> for StateChangeWithCauseView {
    fn from(state_change_with_cause: StateChangeWithCause) -> Self {
        let StateChangeWithCause { cause, value } = state_change_with_cause;
        Self { cause: cause.into(), value: value.into() }
    }
}

pub type StateChangesView = Vec<StateChangeWithCauseView>;

/// Maintenance windows view are a vector of maintenance window.
pub type MaintenanceWindowsView = Vec<Range<BlockHeight>>;

/// Contains the split storage information.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct SplitStorageInfoView {
    pub head_height: Option<BlockHeight>,
    pub final_head_height: Option<BlockHeight>,
    pub cold_head_height: Option<BlockHeight>,

    pub hot_db_kind: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CongestionInfoView {
    #[serde(with = "dec_format")]
    pub delayed_receipts_gas: u128,

    #[serde(with = "dec_format")]
    pub buffered_receipts_gas: u128,

    pub receipt_bytes: u64,

    pub allowed_shard: u16,
}

impl From<CongestionInfo> for CongestionInfoView {
    fn from(congestion_info: CongestionInfo) -> Self {
        match congestion_info {
            CongestionInfo::V1(congestion_info) => congestion_info.into(),
        }
    }
}

impl From<CongestionInfoV1> for CongestionInfoView {
    fn from(congestion_info: CongestionInfoV1) -> Self {
        Self {
            delayed_receipts_gas: congestion_info.delayed_receipts_gas,
            buffered_receipts_gas: congestion_info.buffered_receipts_gas,
            receipt_bytes: congestion_info.receipt_bytes,
            allowed_shard: congestion_info.allowed_shard,
        }
    }
}

impl From<CongestionInfoView> for CongestionInfo {
    fn from(_: CongestionInfoView) -> Self {
        CongestionInfo::default()
    }
}

impl CongestionInfoView {
    pub fn congestion_level(&self, config_view: CongestionControlConfigView) -> f64 {
        let congestion_config = CongestionControlConfig::from(config_view);
        // Localized means without considering missed chunks congestion. As far
        // as clients are concerned, this is the only congestion level that
        // matters.
        // Missed chunks congestion exists to reduce incoming load after a
        // number of chunks were missed. It is not a property of a specific
        // chunk but rather a property that changes over time. It is even a bit
        // misleading to call it congestion, as it is not a problem with too
        // much traffic.
        CongestionInfo::from(self.clone()).localized_congestion_level(&congestion_config)
    }
}

#[cfg(test)]
#[cfg(not(feature = "nightly"))]
#[cfg(not(feature = "statelessnet_protocol"))]
mod tests {
    use super::ExecutionMetadataView;
    use crate::profile_data_v2::ProfileDataV2;
    use crate::profile_data_v3::ProfileDataV3;
    use crate::transaction::ExecutionMetadata;

    /// The JSON representation used in RPC responses must not remove or rename
    /// fields, only adding fields is allowed or we risk breaking clients.
    #[test]
    fn test_runtime_config_view() {
        use near_parameters::{RuntimeConfig, RuntimeConfigStore, RuntimeConfigView};
        use near_primitives_core::version::PROTOCOL_VERSION;

        let config_store = RuntimeConfigStore::new(None);
        let config = config_store.get_config(PROTOCOL_VERSION);
        let view = RuntimeConfigView::from(RuntimeConfig::clone(config));
        insta::assert_json_snapshot!(&view, { ".wasm_config.vm_kind" => "<REDACTED>"});
    }

    /// `ExecutionMetadataView` with profile V1 displayed on the RPC should not change.
    #[test]
    fn test_exec_metadata_v1_view() {
        let metadata = ExecutionMetadata::V1;
        let view = ExecutionMetadataView::from(metadata);
        insta::assert_json_snapshot!(view);
    }

    /// `ExecutionMetadataView` with profile V2 displayed on the RPC should not change.
    #[test]
    fn test_exec_metadata_v2_view() {
        let metadata = ExecutionMetadata::V2(ProfileDataV2::test());
        let view = ExecutionMetadataView::from(metadata);
        insta::assert_json_snapshot!(view);
    }

    /// `ExecutionMetadataView` with profile V3 displayed on the RPC should not change.
    #[test]
    fn test_exec_metadata_v3_view() {
        let metadata = ExecutionMetadata::V3(ProfileDataV3::test().into());
        let view = ExecutionMetadataView::from(metadata);
        insta::assert_json_snapshot!(view);
    }
}

/// Contains files used for a few different purposes:
/// - Changes related to network config - TODO - move to another file
/// - actix messages - used for communicating with `PeerManagerActor` - TODO move to another file
/// - internal types used by `peer-store` only - TODO move to `peer_store.rs`
/// - some types used for different purposes that don't meet any of the criteria above
/// - unused code - TODO remove?
/// - Some types types that are neither of the above
///
/// NOTE:
/// - We also export publicly types from `crate::network_protocol`
use actix::dev::{MessageResponse, ResponseChannel};
use actix::{Actor, Message};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::DateTime;
use near_crypto::{SecretKey, Signature};
use near_primitives::block::{Block, BlockHeader, GenesisId};
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::syncing::{EpochSyncFinalizationResponse, EpochSyncResponse};
use near_primitives::time::{Clock, Utc};
use near_primitives::transaction::ExecutionOutcomeWithIdAndProof;
use near_primitives::types::{AccountId, BlockHeight, EpochId, ShardId};
use near_primitives::utils::{from_timestamp, to_timestamp};
use near_primitives::views::{FinalExecutionOutcomeView, QueryResponse};
use std::fmt::Debug;
use std::hash::Hash;
use std::net::SocketAddr;
use std::time::Duration;
use strum::AsStaticStr;
use tokio::net::TcpStream;

/// Exported types, which are part of network protocol.
pub use crate::network_protocol::{
    PartialEncodedChunkForwardMsg, PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg,
    PeerChainInfo, PeerChainInfoV2, PeerIdOrHash, PeerInfo, Ping, Pong, RoutedMessage,
    RoutedMessageBody, StateResponseInfo, StateResponseInfoV1, StateResponseInfoV2,
};

pub use crate::config::{blacklist_from_iter, BlockedPorts, NetworkConfig};

pub use crate::network_protocol::edge::{Edge, EdgeState, PartialEdgeInfo, SimpleEdge};

/// Number of hops a message is allowed to travel before being dropped.
/// This is used to avoid infinite loop because of inconsistent view of the network
/// by different nodes.
pub const ROUTED_MESSAGE_TTL: u8 = 100;
/// On every message from peer don't update `last_time_received_message`
/// but wait some "small" timeout between updates to avoid a lot of messages between
/// Peer and PeerManager.
pub const UPDATE_INTERVAL_LAST_TIME_RECEIVED_MESSAGE: Duration = Duration::from_secs(60);
/// Due to implementation limits of `Graph` in `near-network`, we support up to 128 client.
pub const MAX_NUM_PEERS: usize = 128;

/// Peer type.
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PeerType {
    /// Inbound session
    Inbound,
    /// Outbound session
    Outbound,
}

/// Account route description
/// Unused?
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct AnnounceAccountRoute {
    pub peer_id: PeerId,
    pub hash: CryptoHash,
    pub signature: Signature,
}

// Don't need Borsh ?
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Hash)]
/// Defines the destination for a network request.
/// The request should be sent either to the `account_id` as a routed message, or directly to
/// any peer that tracks the shard.
/// If `prefer_peer` is `true`, should be sent to the peer, unless no peer tracks the shard, in which
/// case fall back to sending to the account.
/// Otherwise, send to the account, unless we do not know the route, in which case send to the peer.
pub struct AccountIdOrPeerTrackingShard {
    /// Target account to send the the request to
    pub account_id: Option<AccountId>,
    /// Whether to check peers first or target account first
    pub prefer_peer: bool,
    /// Select peers that track shard `shard_id`
    pub shard_id: ShardId,
    /// Select peers that are archival nodes if it is true
    pub only_archival: bool,
    /// Only send messages to peers whose latest chain height is no less `min_height`
    pub min_height: BlockHeight,
}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Hash)]
pub enum AccountOrPeerIdOrHash {
    AccountId(AccountId),
    PeerId(PeerId),
    Hash(CryptoHash),
}

impl AccountOrPeerIdOrHash {
    fn peer_id_or_hash(&self) -> Option<PeerIdOrHash> {
        match self {
            AccountOrPeerIdOrHash::AccountId(_) => None,
            AccountOrPeerIdOrHash::PeerId(peer_id) => Some(PeerIdOrHash::PeerId(peer_id.clone())),
            AccountOrPeerIdOrHash::Hash(hash) => Some(PeerIdOrHash::Hash(*hash)),
        }
    }
}

#[derive(Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct RawRoutedMessage {
    pub target: AccountOrPeerIdOrHash,
    pub body: RoutedMessageBody,
}

impl RawRoutedMessage {
    /// Add signature to the message.
    /// Panics if the target is an AccountId instead of a PeerId.
    pub fn sign(
        self,
        author: PeerId,
        secret_key: &SecretKey,
        routed_message_ttl: u8,
    ) -> RoutedMessage {
        let target = self.target.peer_id_or_hash().unwrap();
        let hash = RoutedMessage::build_hash(&target, &author, &self.body);
        let signature = secret_key.sign(hash.as_ref());
        RoutedMessage { target, author, signature, ttl: routed_message_ttl, body: self.body }
    }
}

/// Routed Message wrapped with previous sender of the message.
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Clone, Debug)]
pub struct RoutedMessageFrom {
    /// Routed messages.
    pub msg: RoutedMessage,
    /// Previous hop in the route. Used for messages that needs routing back.
    pub from: PeerId,
}

impl Message for RoutedMessageFrom {
    type Result = bool;
}

/// not part of protocol, probably doesn't need `borsh`
/// Status of the known peers.
#[derive(BorshSerialize, BorshDeserialize, Eq, PartialEq, Debug, Clone)]
pub enum KnownPeerStatus {
    Unknown,
    NotConnected,
    Connected,
    Banned(ReasonForBan, u64),
}

impl KnownPeerStatus {
    pub fn is_banned(&self) -> bool {
        matches!(self, KnownPeerStatus::Banned(_, _))
    }
}

/// not part of protocol, probably doesn't need `borsh`
/// Information node stores about known peers.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct KnownPeerState {
    pub peer_info: PeerInfo,
    pub status: KnownPeerStatus,
    pub first_seen: u64,
    pub last_seen: u64,
}

impl KnownPeerState {
    pub fn new(peer_info: PeerInfo) -> Self {
        KnownPeerState {
            peer_info,
            status: KnownPeerStatus::Unknown,
            first_seen: to_timestamp(Clock::utc()),
            last_seen: to_timestamp(Clock::utc()),
        }
    }

    pub fn first_seen(&self) -> DateTime<Utc> {
        from_timestamp(self.first_seen)
    }

    pub fn last_seen(&self) -> DateTime<Utc> {
        from_timestamp(self.last_seen)
    }
}

/// Actor message that holds the TCP stream from an inbound TCP connection
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct InboundTcpConnect {
    /// Tcp stream of the inbound connections
    pub stream: TcpStream,
}

#[cfg(feature = "deepsize_feature")]
impl deepsize::DeepSizeOf for InboundTcpConnect {
    fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
        0
    }
}

impl InboundTcpConnect {
    /// Method to create a new InboundTcpConnect message from a TCP stream
    pub fn new(stream: TcpStream) -> InboundTcpConnect {
        InboundTcpConnect { stream }
    }
}

/// Actor message to request the creation of an outbound TCP connection to a peer.
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct OutboundTcpConnect {
    /// Peer information of the outbound connection
    pub peer_info: PeerInfo,
}

/// Unregister message from Peer to PeerManager.
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Message)]
#[rtype(result = "()")]
pub struct Unregister {
    pub peer_id: PeerId,
    pub peer_type: PeerType,
    pub remove_from_peer_store: bool,
}

/// Ban reason.
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq, Copy)]
pub enum ReasonForBan {
    None = 0,
    BadBlock = 1,
    BadBlockHeader = 2,
    HeightFraud = 3,
    BadHandshake = 4,
    BadBlockApproval = 5,
    Abusive = 6,
    InvalidSignature = 7,
    InvalidPeerId = 8,
    InvalidHash = 9,
    InvalidEdge = 10,
    EpochSyncNoResponse = 11,
    EpochSyncInvalidResponse = 12,
    EpochSyncInvalidFinalizationResponse = 13,
}

/// Banning signal sent from Peer instance to PeerManager
/// just before Peer instance is stopped.
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct Ban {
    pub peer_id: PeerId,
    pub ban_reason: ReasonForBan,
}

/// Messages from PeerManager to Peer
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub enum PeerManagerRequest {
    BanPeer(ReasonForBan),
    UnregisterPeer,
}

/// Messages from Peer to PeerManager
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub enum PeerRequest {}

#[derive(Debug, Clone)]
pub struct KnownProducer {
    pub account_id: AccountId,
    pub addr: Option<SocketAddr>,
    pub peer_id: PeerId,
}

#[cfg(feature = "deepsize_feature")]
impl deepsize::DeepSizeOf for KnownProducer {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        self.account_id.deep_size_of_children(context) + self.peer_id.deep_size_of_children(context)
    }
}

#[cfg(feature = "test_features")]
#[derive(Debug)]
pub enum NetworkAdversarialMessage {
    AdvProduceBlocks(u64, bool),
    AdvSwitchToHeight(u64),
    AdvDisableHeaderSync,
    AdvDisableDoomslug,
    AdvGetSavedBlocks,
    AdvCheckStorageConsistency,
    AdvSetSyncInfo(u64),
}

#[cfg(feature = "sandbox")]
#[derive(Debug)]
pub enum NetworkSandboxMessage {
    SandboxPatchState(Vec<near_primitives::state_record::StateRecord>),
    SandboxPatchStateStatus,
}

#[cfg(feature = "sandbox")]
#[derive(Eq, PartialEq, Debug)]
pub enum SandboxResponse {
    SandboxPatchStateFinished(bool),
}

#[derive(AsStaticStr)]
pub enum NetworkViewClientMessages {
    #[cfg(feature = "test_features")]
    Adversarial(NetworkAdversarialMessage),

    /// Transaction status query
    TxStatus { tx_hash: CryptoHash, signer_account_id: AccountId },
    /// Transaction status response
    TxStatusResponse(Box<FinalExecutionOutcomeView>),
    /// Request for receipt outcome
    ReceiptOutcomeRequest(CryptoHash),
    /// Receipt outcome response
    ReceiptOutcomeResponse(Box<ExecutionOutcomeWithIdAndProof>),
    /// Request a block.
    BlockRequest(CryptoHash),
    /// Request headers.
    BlockHeadersRequest(Vec<CryptoHash>),
    /// State request header.
    StateRequestHeader { shard_id: ShardId, sync_hash: CryptoHash },
    /// State request part.
    StateRequestPart { shard_id: ShardId, sync_hash: CryptoHash, part_id: u64 },
    /// A request for a light client info during Epoch Sync
    EpochSyncRequest { epoch_id: EpochId },
    /// A request for headers and proofs during Epoch Sync
    EpochSyncFinalizationRequest { epoch_id: EpochId },
    /// Get Chain information from Client.
    GetChainInfo,
    /// Account announcements that needs to be validated before being processed.
    /// They are paired with last epoch id known to this announcement, in order to accept only
    /// newer announcements.
    AnnounceAccount(Vec<(AnnounceAccount, Option<EpochId>)>),
}

#[derive(Debug)]
pub enum NetworkViewClientResponses {
    /// Transaction execution outcome
    TxStatus(Box<FinalExecutionOutcomeView>),
    /// Response to general queries
    QueryResponse { query_id: String, response: Result<QueryResponse, String> },
    /// Receipt outcome response
    ReceiptOutcomeResponse(Box<ExecutionOutcomeWithIdAndProof>),
    /// Block response.
    Block(Box<Block>),
    /// Headers response.
    BlockHeaders(Vec<BlockHeader>),
    /// Chain information.
    ChainInfo {
        genesis_id: GenesisId,
        height: BlockHeight,
        tracked_shards: Vec<ShardId>,
        archival: bool,
    },
    /// Response to state request.
    StateResponse(Box<StateResponseInfo>),
    /// Valid announce accounts.
    AnnounceAccount(Vec<AnnounceAccount>),
    /// A response to a request for a light client block during Epoch Sync
    EpochSyncResponse(Box<EpochSyncResponse>),
    /// A response to a request for headers and proofs during Epoch Sync
    EpochSyncFinalizationResponse(Box<EpochSyncFinalizationResponse>),
    /// Ban peer for malicious behavior.
    Ban { ban_reason: ReasonForBan },
    /// Response not needed
    NoResponse,
}

impl<A, M> MessageResponse<A, M> for NetworkViewClientResponses
where
    A: Actor,
    M: Message<Result = NetworkViewClientResponses>,
{
    fn handle<R: ResponseChannel<M>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self)
        }
    }
}

impl Message for NetworkViewClientMessages {
    type Result = NetworkViewClientResponses;
}

/// Peer stats query.
pub struct QueryPeerStats {}

/// Peer stats result
#[derive(Debug)]
pub struct PeerStatsResult {
    /// Chain info.
    pub chain_info: PeerChainInfoV2,
    /// Number of bytes we've received from the peer.
    pub received_bytes_per_sec: u64,
    /// Number of bytes we've sent to the peer.
    pub sent_bytes_per_sec: u64,
    /// Returns if this peer is abusive and should be banned.
    pub is_abusive: bool,
    /// Counts of incoming/outgoing messages from given peer.
    pub message_counts: (u64, u64),
}

impl<A, M> MessageResponse<A, M> for PeerStatsResult
where
    A: Actor,
    M: Message<Result = PeerStatsResult>,
{
    fn handle<R: ResponseChannel<M>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self)
        }
    }
}

impl Message for QueryPeerStats {
    type Result = PeerStatsResult;
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_primitives::syncing::ShardStateSyncResponseV1;

    // NOTE: this has it's counterpart in `near_network::types::tests`
    const ALLOWED_SIZE: usize = 1 << 20;
    const NOTIFY_SIZE: usize = 1024;

    macro_rules! assert_size {
        ($type:ident) => {
            let struct_size = std::mem::size_of::<$type>();
            if struct_size >= NOTIFY_SIZE {
                println!("The size of {} is {}", stringify!($type), struct_size);
            }
            assert!(struct_size <= ALLOWED_SIZE);
        };
    }

    #[test]
    fn test_enum_size() {
        assert_size!(PeerType);
        assert_size!(RoutedMessageBody);
        assert_size!(PeerIdOrHash);
        assert_size!(KnownPeerStatus);
        assert_size!(ReasonForBan);
        assert_size!(PeerManagerRequest);
    }

    #[test]
    fn test_struct_size() {
        assert_size!(PeerInfo);
        assert_size!(PeerChainInfoV2);
        assert_size!(AnnounceAccountRoute);
        assert_size!(AnnounceAccount);
        assert_size!(Ping);
        assert_size!(Pong);
        assert_size!(RawRoutedMessage);
        assert_size!(RoutedMessage);
        assert_size!(RoutedMessageFrom);
        assert_size!(KnownPeerState);
        assert_size!(InboundTcpConnect);
        assert_size!(OutboundTcpConnect);
        assert_size!(Unregister);
        assert_size!(Ban);
        assert_size!(StateResponseInfoV1);
        assert_size!(QueryPeerStats);
        assert_size!(PartialEncodedChunkRequestMsg);
    }

    #[test]
    fn routed_message_body_compatibility_smoke_test() {
        #[track_caller]
        fn check(msg: RoutedMessageBody, expected: &[u8]) {
            let actual = msg.try_to_vec().unwrap();
            assert_eq!(actual.as_slice(), expected);
        }

        check(
            RoutedMessageBody::TxStatusRequest("test_x".parse().unwrap(), CryptoHash([42; 32])),
            &[
                2, 6, 0, 0, 0, 116, 101, 115, 116, 95, 120, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42,
                42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42,
                42,
            ],
        );

        check(
            RoutedMessageBody::VersionedStateResponse(StateResponseInfo::V1(StateResponseInfoV1 {
                shard_id: 62,
                sync_hash: CryptoHash([92; 32]),
                state_response: ShardStateSyncResponseV1 { header: None, part: None },
            })),
            &[
                17, 0, 62, 0, 0, 0, 0, 0, 0, 0, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92,
                92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 92, 0, 0,
            ],
        );
    }
}

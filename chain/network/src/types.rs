/// Type that belong to the network protocol.
pub use crate::network_protocol::{
    AccountOrPeerIdOrHash, Disconnect, Encoding, Handshake, HandshakeFailureReason, PeerMessage,
    RoutingTableUpdate, SignedAccountData,
};
use crate::routing::routing_table_view::RoutingTableInfo;
use near_async::messaging::{
    AsyncSender, CanSend, CanSendAsync, IntoAsyncSender, IntoSender, Sender,
};
use near_async::time;
use near_crypto::PublicKey;
use near_primitives::block::{ApprovalMessage, Block, GenesisId};
use near_primitives::challenge::Challenge;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::sharding::PartialEncodedChunkWithArcReceipts;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::BlockHeight;
use near_primitives::types::{AccountId, ShardId};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;

/// Exported types, which are part of network protocol.
pub use crate::network_protocol::{
    Edge, PartialEdgeInfo, PartialEncodedChunkForwardMsg, PartialEncodedChunkRequestMsg,
    PartialEncodedChunkResponseMsg, PeerChainInfoV2, PeerInfo, StateResponseInfo,
    StateResponseInfoV1, StateResponseInfoV2,
};

/// Number of hops a message is allowed to travel before being dropped.
/// This is used to avoid infinite loop because of inconsistent view of the network
/// by different nodes.
pub const ROUTED_MESSAGE_TTL: u8 = 100;

/// Peer type.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, strum::IntoStaticStr)]
pub enum PeerType {
    /// Inbound session
    Inbound,
    /// Outbound session
    Outbound,
}

#[derive(Debug, Clone)]
pub struct KnownProducer {
    pub account_id: AccountId,
    pub addr: Option<SocketAddr>,
    pub peer_id: PeerId,
    pub next_hops: Option<Vec<PeerId>>,
}

/// Ban reason.
#[derive(borsh::BorshSerialize, borsh::BorshDeserialize, Debug, Clone, PartialEq, Eq, Copy)]
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
    InvalidDistanceVector = 11,
    Blacklisted = 14,
}

/// Banning signal sent from Peer instance to PeerManager
/// just before Peer instance is stopped.
#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct Ban {
    pub peer_id: PeerId,
    pub ban_reason: ReasonForBan,
}

/// Status of the known peers.
#[derive(Eq, PartialEq, Debug, Clone)]
pub enum KnownPeerStatus {
    /// We got information about this peer from someone, but we didn't
    /// verify them yet. This peer might not exist, invalid IP etc.
    /// Also the peers that we failed to connect to, will be marked as 'Unknown'.
    Unknown,
    /// We know that this peer exists - we were connected to it, or it was provided as boot node.
    NotConnected,
    /// We're currently connected to this peer.
    Connected,
    /// We banned this peer for some reason. Once the ban time is over, it will move to 'NotConnected' state.
    Banned(ReasonForBan, time::Utc),
}

/// Information node stores about known peers.
#[derive(Debug, Clone)]
pub struct KnownPeerState {
    pub peer_info: PeerInfo,
    pub status: KnownPeerStatus,
    pub first_seen: time::Utc,
    pub last_seen: time::Utc,
    // Last time we tried to connect to this peer.
    // This data is not persisted in storage.
    pub last_outbound_attempt: Option<(time::Utc, Result<(), String>)>,
}

impl KnownPeerState {
    pub fn new(peer_info: PeerInfo, now: time::Utc) -> Self {
        KnownPeerState {
            peer_info,
            status: KnownPeerStatus::Unknown,
            first_seen: now,
            last_seen: now,
            last_outbound_attempt: None,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ConnectionInfo {
    pub peer_info: PeerInfo,
    pub time_established: time::Utc,
    pub time_connected_until: time::Utc,
}

impl KnownPeerStatus {
    pub fn is_banned(&self) -> bool {
        matches!(self, KnownPeerStatus::Banned(_, _))
    }
}

/// Set of account keys.
/// This is information which chain pushes to network to implement tier1.
/// See ChainInfo.
pub type AccountKeys = HashMap<AccountId, HashSet<PublicKey>>;

/// Network-relevant data about the chain.
// TODO(gprusak): it is more like node info, or sth.
#[derive(Debug, Clone)]
pub struct ChainInfo {
    pub tracked_shards: Vec<ShardId>,
    // The lastest block on chain.
    pub block: Block,
    // Public keys of accounts participating in the BFT consensus
    // It currently includes "block producers", "chunk producers" and "approvers".
    // They are collectively known as "validators".
    // Peers acting on behalf of these accounts have a higher
    // priority on the NEAR network than other peers.
    pub tier1_accounts: Arc<AccountKeys>,
}

#[derive(Debug, actix::Message)]
#[rtype(result = "()")]
pub struct SetChainInfo(pub ChainInfo);

/// Public actix interface of `PeerManagerActor`.
#[derive(actix::Message, Debug, strum::IntoStaticStr)]
#[rtype(result = "PeerManagerMessageResponse")]
pub enum PeerManagerMessageRequest {
    NetworkRequests(NetworkRequests),
    /// Request PeerManager to connect to the given peer.
    /// Used in tests and internally by PeerManager.
    /// TODO: replace it with AsyncContext::spawn/run_later for internal use.
    OutboundTcpConnect(crate::tcp::Stream),
    /// The following types of requests are used to trigger actions in the Peer Manager for testing.
    /// TEST-ONLY: Fetch current routing table.
    FetchRoutingTable,
}

impl PeerManagerMessageRequest {
    pub fn as_network_requests(self) -> NetworkRequests {
        if let PeerManagerMessageRequest::NetworkRequests(item) = self {
            item
        } else {
            panic!("expected PeerMessageRequest::NetworkRequests(");
        }
    }

    pub fn as_network_requests_ref(&self) -> &NetworkRequests {
        if let PeerManagerMessageRequest::NetworkRequests(item) = self {
            item
        } else {
            panic!("expected PeerMessageRequest::NetworkRequests");
        }
    }
}

/// List of all replies to messages to `PeerManager`. See `PeerManagerMessageRequest` for more details.
#[derive(actix::MessageResponse, Debug)]
pub enum PeerManagerMessageResponse {
    NetworkResponses(NetworkResponses),
    /// TEST-ONLY
    OutboundTcpConnect,
    FetchRoutingTable(RoutingTableInfo),
}

impl PeerManagerMessageResponse {
    pub fn as_network_response(self) -> NetworkResponses {
        if let PeerManagerMessageResponse::NetworkResponses(item) = self {
            item
        } else {
            panic!("expected PeerMessageRequest::NetworkResponses");
        }
    }
}

impl From<NetworkResponses> for PeerManagerMessageResponse {
    fn from(msg: NetworkResponses) -> Self {
        PeerManagerMessageResponse::NetworkResponses(msg)
    }
}

// TODO(#1313): Use Box
#[derive(Clone, strum::AsRefStr, Debug, Eq, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum NetworkRequests {
    /// Sends block, either when block was just produced or when requested.
    Block { block: Block },
    /// Sends approval.
    Approval { approval_message: ApprovalMessage },
    /// Request block with given hash from given peer.
    BlockRequest { hash: CryptoHash, peer_id: PeerId },
    /// Request given block headers.
    BlockHeadersRequest { hashes: Vec<CryptoHash>, peer_id: PeerId },
    /// Request state header for given shard at given state root.
    StateRequestHeader { shard_id: ShardId, sync_hash: CryptoHash, target: AccountOrPeerIdOrHash },
    /// Request state part for given shard at given state root.
    StateRequestPart {
        shard_id: ShardId,
        sync_hash: CryptoHash,
        part_id: u64,
        target: AccountOrPeerIdOrHash,
    },
    /// Response to state request.
    StateResponse { route_back: CryptoHash, response: StateResponseInfo },
    /// Ban given peer.
    BanPeer { peer_id: PeerId, ban_reason: ReasonForBan },
    /// Announce account
    AnnounceAccount(AnnounceAccount),

    /// Request chunk parts and/or receipts
    PartialEncodedChunkRequest {
        target: AccountIdOrPeerTrackingShard,
        request: PartialEncodedChunkRequestMsg,
        create_time: time::Instant,
    },
    /// Information about chunk such as its header, some subset of parts and/or incoming receipts
    PartialEncodedChunkResponse { route_back: CryptoHash, response: PartialEncodedChunkResponseMsg },
    /// Information about chunk such as its header, some subset of parts and/or incoming receipts
    PartialEncodedChunkMessage {
        account_id: AccountId,
        partial_encoded_chunk: PartialEncodedChunkWithArcReceipts,
    },
    /// Forwarding a chunk part to a validator tracking the shard
    PartialEncodedChunkForward { account_id: AccountId, forward: PartialEncodedChunkForwardMsg },

    /// Valid transaction but since we are not validators we send this transaction to current validators.
    ForwardTx(AccountId, SignedTransaction),
    /// Query transaction status
    TxStatus(AccountId, AccountId, CryptoHash),
    /// A challenge to invalidate a block.
    Challenge(Challenge),
}

/// Combines peer address info, chain.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FullPeerInfo {
    pub peer_info: PeerInfo,
    pub chain_info: PeerChainInfo,
}

/// These are the information needed for highest height peers. For these peers, we guarantee that
/// the height and hash of the latest block are set.
#[derive(Debug, Clone)]
pub struct HighestHeightPeerInfo {
    pub peer_info: PeerInfo,
    /// Chain Id and hash of genesis block.
    pub genesis_id: GenesisId,
    /// Height and hash of the highest block we've ever received from the peer
    pub highest_block_height: BlockHeight,
    /// Hash of the latest block
    pub highest_block_hash: CryptoHash,
    /// Shards that the peer is tracking.
    pub tracked_shards: Vec<ShardId>,
    /// Denote if a node is running in archival mode or not.
    pub archival: bool,
}

impl From<FullPeerInfo> for Option<HighestHeightPeerInfo> {
    fn from(p: FullPeerInfo) -> Self {
        if p.chain_info.last_block.is_some() {
            Some(HighestHeightPeerInfo {
                peer_info: p.peer_info,
                genesis_id: p.chain_info.genesis_id,
                highest_block_height: p.chain_info.last_block.unwrap().height,
                highest_block_hash: p.chain_info.last_block.unwrap().hash,
                tracked_shards: p.chain_info.tracked_shards,
                archival: p.chain_info.archival,
            })
        } else {
            None
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct BlockInfo {
    pub height: BlockHeight,
    pub hash: CryptoHash,
}

/// This is the internal representation of PeerChainInfoV2.
/// We separate these two structs because PeerChainInfoV2 is part of network protocol, and can't be
/// modified easily.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PeerChainInfo {
    /// Chain Id and hash of genesis block.
    pub genesis_id: GenesisId,
    /// Height and hash of the highest block we've ever received from the peer
    pub last_block: Option<BlockInfo>,
    /// Shards that the peer is tracking.
    pub tracked_shards: Vec<ShardId>,
    /// Denote if a node is running in archival mode or not.
    pub archival: bool,
}

// Information about the connected peer that is shared with the rest of the system.
#[derive(Debug, Clone)]
pub struct ConnectedPeerInfo {
    pub full_peer_info: FullPeerInfo,
    /// Number of bytes we've received from the peer.
    pub received_bytes_per_sec: u64,
    /// Number of bytes we've sent to the peer.
    pub sent_bytes_per_sec: u64,
    /// Last time requested peers.
    pub last_time_peer_requested: time::Instant,
    /// Last time we received a message from this peer.
    pub last_time_received_message: time::Instant,
    /// Time where the connection was established.
    pub connection_established_time: time::Instant,
    /// Who started connection. Inbound (other) or Outbound (us).
    pub peer_type: PeerType,
    /// Nonce used for the connection with the peer.
    pub nonce: u64,
}

#[derive(Debug, Clone, actix::MessageResponse)]
pub struct NetworkInfo {
    /// TIER2 connections.
    pub connected_peers: Vec<ConnectedPeerInfo>,
    pub num_connected_peers: usize,
    pub peer_max_count: u32,
    pub highest_height_peers: Vec<HighestHeightPeerInfo>,
    pub sent_bytes_per_sec: u64,
    pub received_bytes_per_sec: u64,
    /// Accounts of known block and chunk producers from routing table.
    pub known_producers: Vec<KnownProducer>,
    /// Collected data about the current TIER1 accounts.
    pub tier1_accounts_keys: Vec<PublicKey>,
    pub tier1_accounts_data: Vec<Arc<SignedAccountData>>,
    /// TIER1 connections.
    pub tier1_connections: Vec<ConnectedPeerInfo>,
}

#[derive(Debug, actix::MessageResponse, PartialEq, Eq)]
pub enum NetworkResponses {
    NoResponse,
    RouteNotFound,
}

#[derive(Clone, derive_more::AsRef)]
pub struct PeerManagerAdapter {
    pub async_request_sender:
        AsyncSender<PeerManagerMessageRequest, Result<PeerManagerMessageResponse, ()>>,
    pub request_sender: Sender<PeerManagerMessageRequest>,
    pub set_chain_info_sender: Sender<SetChainInfo>,
}

impl<
        A: CanSendAsync<PeerManagerMessageRequest, Result<PeerManagerMessageResponse, ()>>
            + CanSend<PeerManagerMessageRequest>
            + CanSend<SetChainInfo>,
    > From<Arc<A>> for PeerManagerAdapter
{
    fn from(arc: Arc<A>) -> Self {
        Self {
            async_request_sender: arc.as_async_sender(),
            request_sender: arc.as_sender(),
            set_chain_info_sender: arc.as_sender(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network_protocol::{RawRoutedMessage, RoutedMessage, RoutedMessageBody};
    use borsh::BorshSerialize as _;
    use near_primitives::syncing::ShardStateSyncResponseV1;

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
    fn test_size() {
        assert_size!(HandshakeFailureReason);
        assert_size!(NetworkRequests);
        assert_size!(NetworkResponses);
        assert_size!(Handshake);
        assert_size!(RoutingTableUpdate);
        assert_size!(FullPeerInfo);
        assert_size!(NetworkInfo);
    }

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
        assert_size!(KnownPeerStatus);
        assert_size!(ReasonForBan);
    }

    #[test]
    fn test_struct_size() {
        assert_size!(PeerInfo);
        assert_size!(AnnounceAccount);
        assert_size!(RawRoutedMessage);
        assert_size!(RoutedMessage);
        assert_size!(KnownPeerState);
        assert_size!(Ban);
        assert_size!(StateResponseInfoV1);
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

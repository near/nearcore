/// Type that belong to the network protocol.
pub use crate::network_protocol::{
    Encoding, Handshake, HandshakeFailureReason, ParsePeerMessageError, PeerMessage,
    RoutingTableUpdate, SignedAccountData,
};
use crate::routing::routing_table_view::RoutingTableInfo;
use futures::future::BoxFuture;
use futures::FutureExt;
use near_crypto::PublicKey;
use near_network_primitives::time;
use near_network_primitives::types::{
    AccountIdOrPeerTrackingShard, AccountOrPeerIdOrHash, KnownProducer, OutboundTcpConnect,
    PartialEdgeInfo, PartialEncodedChunkForwardMsg, PartialEncodedChunkRequestMsg,
    PartialEncodedChunkResponseMsg, PeerChainInfoV2, PeerInfo, PeerType, Ping, Pong, ReasonForBan,
    StateResponseInfo,
};
use near_primitives::block::{Approval, ApprovalMessage, Block, BlockHeader};
use near_primitives::challenge::Challenge;
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::sharding::{PartialEncodedChunk, PartialEncodedChunkWithArcReceipts};
use near_primitives::syncing::{EpochSyncFinalizationResponse, EpochSyncResponse};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::BlockHeight;
use near_primitives::types::{AccountId, EpochId, ShardId};
use near_primitives::views::{KnownProducerView, NetworkInfoView, PeerInfoView};
use once_cell::sync::OnceCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

/// Set of account keys.
/// This is information which chain pushes to network to implement tier1.
/// See ChainInfo.
pub type AccountKeys = HashMap<(EpochId, AccountId), PublicKey>;

/// Network-relevant data about the chain.
// TODO(gprusak): it is more like node info, or sth.
#[derive(Debug, Clone, Default)]
pub struct ChainInfo {
    pub tracked_shards: Vec<ShardId>,
    pub height: BlockHeight,
    // Public keys of accounts participating in the BFT consensus
    // (both accounts from current and next epoch are important, that's why
    // the map is indexed by (EpochId,AccountId) pair).
    // It currently includes "block producers", "chunk producers" and "approvers".
    // They are collectively known as "validators".
    // Peers acting on behalf of these accounts have a higher
    // priority on the NEAR network than other peers.
    pub tier1_accounts: Arc<AccountKeys>,
}

#[derive(Debug, actix::Message)]
#[rtype(result = "()")]
pub struct SetChainInfo(pub ChainInfo);

#[derive(Debug, actix::Message)]
#[rtype(result = "NetworkInfo")]
pub struct GetNetworkInfo;

/// Public actix interface of `PeerManagerActor`.
#[derive(actix::Message, Debug, strum::IntoStaticStr)]
#[rtype(result = "PeerManagerMessageResponse")]
pub enum PeerManagerMessageRequest {
    NetworkRequests(NetworkRequests),
    /// Request PeerManager to connect to the given peer.
    /// Used in tests and internally by PeerManager.
    /// TODO: replace it with AsyncContext::spawn/run_later for internal use.
    OutboundTcpConnect(OutboundTcpConnect),
    /// TEST-ONLY
    SetAdvOptions(crate::test_utils::SetAdvOptions),
    /// The following types of requests are used to trigger actions in the Peer Manager for testing.
    /// TEST-ONLY: Fetch current routing table.
    FetchRoutingTable,
    /// TEST-ONLY Start ping to `PeerId` with `nonce`.
    PingTo {
        nonce: u64,
        target: PeerId,
    },
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
    OutboundTcpConnect,
    /// TEST-ONLY
    SetAdvOptions,
    FetchRoutingTable(RoutingTableInfo),
    PingTo,
}

impl PeerManagerMessageResponse {
    pub fn as_network_response(self) -> NetworkResponses {
        if let PeerManagerMessageResponse::NetworkResponses(item) = self {
            item
        } else {
            panic!("expected PeerMessageRequest::NetworkResponses(");
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
    Block {
        block: Block,
    },
    /// Sends approval.
    Approval {
        approval_message: ApprovalMessage,
    },
    /// Request block with given hash from given peer.
    BlockRequest {
        hash: CryptoHash,
        peer_id: PeerId,
    },
    /// Request given block headers.
    BlockHeadersRequest {
        hashes: Vec<CryptoHash>,
        peer_id: PeerId,
    },
    /// Request state header for given shard at given state root.
    StateRequestHeader {
        shard_id: ShardId,
        sync_hash: CryptoHash,
        target: AccountOrPeerIdOrHash,
    },
    /// Request state part for given shard at given state root.
    StateRequestPart {
        shard_id: ShardId,
        sync_hash: CryptoHash,
        part_id: u64,
        target: AccountOrPeerIdOrHash,
    },
    /// Response to state request.
    StateResponse {
        route_back: CryptoHash,
        response: StateResponseInfo,
    },
    EpochSyncRequest {
        peer_id: PeerId,
        epoch_id: EpochId,
    },
    EpochSyncFinalizationRequest {
        peer_id: PeerId,
        epoch_id: EpochId,
    },
    /// Ban given peer.
    BanPeer {
        peer_id: PeerId,
        ban_reason: ReasonForBan,
    },
    /// Announce account
    AnnounceAccount(AnnounceAccount),

    /// Request chunk parts and/or receipts
    PartialEncodedChunkRequest {
        target: AccountIdOrPeerTrackingShard,
        request: PartialEncodedChunkRequestMsg,
        create_time: time::Instant,
    },
    /// Information about chunk such as its header, some subset of parts and/or incoming receipts
    PartialEncodedChunkResponse {
        route_back: CryptoHash,
        response: PartialEncodedChunkResponseMsg,
    },
    /// Information about chunk such as its header, some subset of parts and/or incoming receipts
    PartialEncodedChunkMessage {
        account_id: AccountId,
        partial_encoded_chunk: PartialEncodedChunkWithArcReceipts,
    },
    /// Forwarding a chunk part to a validator tracking the shard
    PartialEncodedChunkForward {
        account_id: AccountId,
        forward: PartialEncodedChunkForwardMsg,
    },

    /// Valid transaction but since we are not validators we send this transaction to current validators.
    ForwardTx(AccountId, SignedTransaction),
    /// Query transaction status
    TxStatus(AccountId, AccountId, CryptoHash),
    /// A challenge to invalidate a block.
    Challenge(Challenge),
}

/// Combines peer address info, chain and edge information.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FullPeerInfo {
    pub peer_info: PeerInfo,
    pub chain_info: PeerChainInfoV2,
    pub partial_edge_info: PartialEdgeInfo,
}

impl From<&FullPeerInfo> for ConnectedPeerInfo {
    fn from(full_peer_info: &FullPeerInfo) -> Self {
        ConnectedPeerInfo {
            full_peer_info: full_peer_info.clone(),
            received_bytes_per_sec: 0,
            sent_bytes_per_sec: 0,
            last_time_peer_requested: near_network_primitives::time::Instant::now(),
            last_time_received_message: near_network_primitives::time::Instant::now(),
            connection_established_time: near_network_primitives::time::Instant::now(),
            peer_type: PeerType::Outbound,
        }
    }
}

impl From<&ConnectedPeerInfo> for PeerInfoView {
    fn from(connected_peer_info: &ConnectedPeerInfo) -> Self {
        let full_peer_info = &connected_peer_info.full_peer_info;
        PeerInfoView {
            addr: match full_peer_info.peer_info.addr {
                Some(socket_addr) => socket_addr.to_string(),
                None => "N/A".to_string(),
            },
            account_id: full_peer_info.peer_info.account_id.clone(),
            height: full_peer_info.chain_info.height,
            tracked_shards: full_peer_info.chain_info.tracked_shards.clone(),
            archival: full_peer_info.chain_info.archival,
            peer_id: full_peer_info.peer_info.id.public_key().clone(),
            received_bytes_per_sec: connected_peer_info.received_bytes_per_sec,
            sent_bytes_per_sec: connected_peer_info.sent_bytes_per_sec,
            last_time_peer_requested_millis: connected_peer_info
                .last_time_peer_requested
                .elapsed()
                .whole_milliseconds() as u64,
            last_time_received_message_millis: connected_peer_info
                .last_time_received_message
                .elapsed()
                .whole_milliseconds() as u64,
            connection_established_time_millis: connected_peer_info
                .connection_established_time
                .elapsed()
                .whole_milliseconds() as u64,
            is_outbound_peer: connected_peer_info.peer_type == PeerType::Outbound,
        }
    }
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
}

#[derive(Debug, Clone, actix::MessageResponse)]
pub struct NetworkInfo {
    pub connected_peers: Vec<ConnectedPeerInfo>,
    pub num_connected_peers: usize,
    pub peer_max_count: u32,
    pub highest_height_peers: Vec<FullPeerInfo>,
    pub sent_bytes_per_sec: u64,
    pub received_bytes_per_sec: u64,
    /// Accounts of known block and chunk producers from routing table.
    pub known_producers: Vec<KnownProducer>,
    pub tier1_accounts: Vec<Arc<SignedAccountData>>,
}

impl From<NetworkInfo> for NetworkInfoView {
    fn from(network_info: NetworkInfo) -> Self {
        NetworkInfoView {
            peer_max_count: network_info.peer_max_count,
            num_connected_peers: network_info.num_connected_peers,
            connected_peers: network_info
                .connected_peers
                .iter()
                .map(|full_peer_info| full_peer_info.into())
                .collect::<Vec<_>>(),
            known_producers: network_info
                .known_producers
                .iter()
                .map(|it| KnownProducerView {
                    account_id: it.account_id.clone(),
                    peer_id: it.peer_id.public_key().clone(),
                    next_hops: it
                        .next_hops
                        .as_ref()
                        .map(|it| it.iter().map(|peer_id| peer_id.public_key().clone()).collect()),
                })
                .collect(),
        }
    }
}

#[derive(Debug, actix::MessageResponse)]
pub enum NetworkResponses {
    NoResponse,
    PingPongInfo { pings: Vec<Ping>, pongs: Vec<Pong> },
    RouteNotFound,
}

#[derive(actix::Message, Debug, strum::AsRefStr, strum::IntoStaticStr)]
// TODO(#1313): Use Box
#[allow(clippy::large_enum_variant)]
#[rtype(result = "NetworkClientResponses")]
pub enum NetworkClientMessages {
    #[cfg(feature = "test_features")]
    Adversarial(near_network_primitives::types::NetworkAdversarialMessage),

    /// Received transaction.
    Transaction {
        transaction: SignedTransaction,
        /// Whether the transaction is forwarded from other nodes.
        is_forwarded: bool,
        /// Whether the transaction needs to be submitted.
        check_only: bool,
    },
    /// Received block, possibly requested.
    Block(Block, PeerId, bool),
    /// Received list of headers for syncing.
    BlockHeaders(Vec<BlockHeader>, PeerId),
    /// Block approval.
    BlockApproval(Approval, PeerId),
    /// State response.
    StateResponse(StateResponseInfo),
    /// Epoch Sync response for light client block request
    EpochSyncResponse(PeerId, Box<EpochSyncResponse>),
    /// Epoch Sync response for finalization request
    EpochSyncFinalizationResponse(PeerId, Box<EpochSyncFinalizationResponse>),

    /// Request chunk parts and/or receipts.
    PartialEncodedChunkRequest(PartialEncodedChunkRequestMsg, CryptoHash),
    /// Response to a request for  chunk parts and/or receipts.
    PartialEncodedChunkResponse(PartialEncodedChunkResponseMsg, std::time::Instant),
    /// Information about chunk such as its header, some subset of parts and/or incoming receipts
    PartialEncodedChunk(PartialEncodedChunk),
    /// Forwarding parts to those tracking the shard (so they don't need to send requests)
    PartialEncodedChunkForward(PartialEncodedChunkForwardMsg),

    /// A challenge to invalidate the block.
    Challenge(Challenge),

    NetworkInfo(NetworkInfo),
}

// TODO(#1313): Use Box
#[derive(Eq, PartialEq, Debug, actix::MessageResponse)]
#[allow(clippy::large_enum_variant)]
pub enum NetworkClientResponses {
    /// Adv controls.
    #[cfg(feature = "test_features")]
    AdvResult(u64),

    /// No response.
    NoResponse,
    /// Valid transaction inserted into mempool as response to Transaction.
    ValidTx,
    /// Invalid transaction inserted into mempool as response to Transaction.
    InvalidTx(InvalidTxError),
    /// The request is routed to other shards
    RequestRouted,
    /// The node being queried does not track the shard needed and therefore cannot provide userful
    /// response.
    DoesNotTrackShard,
    /// Ban peer for malicious behavior.
    Ban { ban_reason: ReasonForBan },
}

pub trait MsgRecipient<M: actix::Message>: Send + Sync + 'static {
    fn send(&self, msg: M) -> BoxFuture<'static, Result<M::Result, actix::MailboxError>>;
    fn do_send(&self, msg: M);
}

impl<A, M> MsgRecipient<M> for actix::Addr<A>
where
    M: actix::Message + Send + 'static,
    M::Result: Send,
    A: actix::Actor + actix::Handler<M>,
    A::Context: actix::dev::ToEnvelope<A, M>,
{
    fn send(&self, msg: M) -> BoxFuture<'static, Result<M::Result, actix::MailboxError>> {
        actix::Addr::send(self, msg).boxed()
    }
    fn do_send(&self, msg: M) {
        actix::Addr::do_send(self, msg)
    }
}

pub trait PeerManagerAdapter:
    MsgRecipient<PeerManagerMessageRequest> + MsgRecipient<SetChainInfo>
{
}
impl<A: MsgRecipient<PeerManagerMessageRequest> + MsgRecipient<SetChainInfo>> PeerManagerAdapter
    for A
{
}

pub struct NetworkRecipient<T> {
    recipient: OnceCell<Arc<T>>,
}

impl<T> Default for NetworkRecipient<T> {
    fn default() -> Self {
        Self { recipient: OnceCell::default() }
    }
}

impl<T> NetworkRecipient<T> {
    pub fn set_recipient(&self, t: T) {
        self.recipient.set(Arc::new(t)).ok().expect("cannot set recipient twice");
    }
}

impl<M: actix::Message, T: MsgRecipient<M>> MsgRecipient<M> for NetworkRecipient<T> {
    fn send(&self, msg: M) -> BoxFuture<'static, Result<M::Result, actix::MailboxError>> {
        self.recipient.wait().send(msg)
    }
    fn do_send(&self, msg: M) {
        self.recipient.wait().do_send(msg);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // NOTE: this has it's counterpart in `near_network_primitives::types::tests`
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
        assert_size!(NetworkClientMessages);
        assert_size!(NetworkClientResponses);
        assert_size!(Handshake);
        assert_size!(Ping);
        assert_size!(Pong);
        assert_size!(RoutingTableUpdate);
        assert_size!(FullPeerInfo);
        assert_size!(NetworkInfo);
    }
}

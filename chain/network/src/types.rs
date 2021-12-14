use crate::peer::peer_actor::PeerActor;
use crate::routing::network_protocol::SimpleEdge;
use crate::routing::routing::{GetRoutingTableResult, PeerRequestResult, RoutingTableInfo};
use crate::PeerInfo;
use actix::dev::{MessageResponse, ResponseChannel};
use actix::{Actor, Addr, MailboxError, Message, Recipient};
use conqueue::QueueSender;
#[cfg(feature = "deepsize_feature")]
use deepsize::DeepSizeOf;
use futures::future::BoxFuture;
use futures::FutureExt;
use near_network_primitives::types::{
    AccountIdOrPeerTrackingShard, AccountOrPeerIdOrHash, Ban, InboundTcpConnect, KnownProducer,
    OutboundTcpConnect, PartialEncodedChunkForwardMsg, PartialEncodedChunkRequestMsg,
    PartialEncodedChunkResponseMsg, PeerChainInfoV2, PeerType, Ping, Pong, ReasonForBan,
    RoutedMessageBody, RoutedMessageFrom, StateResponseInfo,
};
use near_primitives::block::{Approval, ApprovalMessage, Block, BlockHeader};
use near_primitives::challenge::Challenge;
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::sharding::{PartialEncodedChunk, PartialEncodedChunkWithArcReceipts};
use near_primitives::syncing::{EpochSyncFinalizationResponse, EpochSyncResponse};
use near_primitives::time::Instant;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockReference, EpochId, ShardId};
use near_primitives::version::ProtocolVersion;
use near_primitives::views::QueryRequest;
use near_rate_limiter::ThrottleController;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex, RwLock};
use strum::AsStaticStr;

/// Type that belong to the network protocol.
pub use crate::network_protocol::{
    Edge, Handshake, HandshakeFailureReason, HandshakeV2, PartialEdgeInfo, PeerMessage,
    RoutingTableUpdate,
};
#[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
pub use crate::network_protocol::{PartialSync, RoutingState, RoutingSyncV2, RoutingVersion2};

#[derive(Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct SendMessage {
    pub(crate) message: PeerMessage,
}

/// Actor message which asks `PeerManagerActor` to register peer.
/// Returns `RegisterPeerResult` with `Accepted` if connection should be kept
/// or a reject response otherwise.
#[derive(Clone, Debug)]
pub struct RegisterPeer {
    pub(crate) actor: Addr<PeerActor>,
    pub(crate) peer_info: PeerInfo,
    pub(crate) peer_type: PeerType,
    pub(crate) chain_info: PeerChainInfoV2,
    /// Edge information from this node.
    /// If this is None it implies we are outbound connection, so we need to create our
    /// EdgeInfo part and send it to the other peer.
    pub(crate) this_edge_info: Option<PartialEdgeInfo>,
    /// Edge information from other node.
    pub(crate) other_edge_info: PartialEdgeInfo,
    /// Protocol version of new peer. May be higher than ours.
    pub(crate) peer_protocol_version: ProtocolVersion,
    /// A helper data structure for limiting reading, reporting bandwidth stats.
    pub(crate) throttle_controller: ThrottleController,
}

/// Addr<PeerActor> doesn't implement `DeepSizeOf` waiting for `deepsize` > 0.2.0.
#[cfg(feature = "deepsize_feature")]
impl deepsize::DeepSizeOf for RegisterPeer {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        self.peer_info.deep_size_of_children(context)
            + self.peer_type.deep_size_of_children(context)
            + self.chain_info.deep_size_of_children(context)
            + self.this_edge_info.deep_size_of_children(context)
            + self.other_edge_info.deep_size_of_children(context)
            + self.peer_protocol_version.deep_size_of_children(context)
    }
}

impl Message for RegisterPeer {
    type Result = RegisterPeerResponse;
}

#[derive(MessageResponse, Debug)]
pub enum RegisterPeerResponse {
    Accept(Option<PartialEdgeInfo>),
    InvalidNonce(Box<Edge>),
    Reject,
}

#[cfg(feature = "test_features")]
#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(Clone, Debug)]
pub struct GetPeerId {}

#[cfg(feature = "test_features")]
impl Message for GetPeerId {
    type Result = GetPeerIdResult;
}

#[cfg(feature = "test_features")]
#[derive(MessageResponse, Debug, serde::Serialize)]
pub struct GetPeerIdResult {
    pub(crate) peer_id: PeerId,
}

#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(Debug)]
pub struct GetRoutingTable {}

impl Message for GetRoutingTable {
    type Result = GetRoutingTableResult;
}

#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(Clone, Debug)]
#[cfg(feature = "test_features")]
pub struct StartRoutingTableSync {
    pub peer_id: PeerId,
}

#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(Clone, Debug)]
#[cfg(feature = "test_features")]
pub struct SetAdvOptions {
    pub disable_edge_signature_verification: Option<bool>,
    pub disable_edge_propagation: Option<bool>,
    pub disable_edge_pruning: Option<bool>,
    pub set_max_peers: Option<u64>,
}

#[cfg(feature = "test_features")]
impl Message for SetAdvOptions {
    type Result = ();
}

#[cfg(feature = "test_features")]
impl Message for StartRoutingTableSync {
    type Result = ();
}

/// Unregister message from Peer to PeerManager.
#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct Unregister {
    pub(crate) peer_id: PeerId,
    pub(crate) peer_type: PeerType,
    pub(crate) remove_from_peer_store: bool,
}

#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct StopMsg {}

/// Message from peer to peer manager
#[derive(strum::AsRefStr, Clone, Debug)]
pub enum PeerRequest {
    UpdateEdge((PeerId, u64)),
    RouteBack(Box<RoutedMessageBody>, CryptoHash),
    UpdatePeerInfo(PeerInfo),
    ReceivedMessage(PeerId, Instant),
}

#[cfg(feature = "deepsize_feature")]
impl deepsize::DeepSizeOf for PeerRequest {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        match self {
            PeerRequest::UpdateEdge(x) => x.deep_size_of_children(context),
            PeerRequest::RouteBack(x, y) => {
                x.deep_size_of_children(context) + y.deep_size_of_children(context)
            }
            PeerRequest::UpdatePeerInfo(x) => x.deep_size_of_children(context),
            PeerRequest::ReceivedMessage(x, _) => x.deep_size_of_children(context),
        }
    }
}

impl Message for PeerRequest {
    type Result = PeerResponse;
}

#[derive(MessageResponse, Debug)]
pub enum PeerResponse {
    NoResponse,
    UpdatedEdge(PartialEdgeInfo),
}

/// Requesting peers from peer manager to communicate to a peer.
#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(Clone, Debug)]
pub struct PeersRequest {}

impl Message for PeersRequest {
    type Result = PeerRequestResult;
}

/// Received new peers from another peer.
#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct PeersResponse {
    pub(crate) peers: Vec<PeerInfo>,
}

/// List of all messages, which PeerManagerActor accepts through Actix. There is also another list
/// which contains reply for each message to PeerManager.
/// There is 1 to 1 mapping between an entry in `PeerManagerMessageRequest` and `PeerManagerMessageResponse`.
#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(Debug)]
pub enum PeerManagerMessageRequest {
    RoutedMessageFrom(RoutedMessageFrom),
    NetworkRequests(NetworkRequests),
    RegisterPeer(RegisterPeer),
    PeersRequest(PeersRequest),
    PeersResponse(PeersResponse),
    PeerRequest(PeerRequest),
    #[cfg(feature = "test_features")]
    GetPeerId(GetPeerId),
    OutboundTcpConnect(OutboundTcpConnect),
    InboundTcpConnect(InboundTcpConnect),
    Unregister(Unregister),
    Ban(Ban),
    #[cfg(feature = "test_features")]
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    StartRoutingTableSync(StartRoutingTableSync),
    #[cfg(feature = "test_features")]
    SetAdvOptions(SetAdvOptions),
    #[cfg(feature = "test_features")]
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    SetRoutingTable(SetRoutingTable),
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
            panic!("expected PeerMessageRequest::NetworkRequests(");
        }
    }
}

impl Message for PeerManagerMessageRequest {
    type Result = PeerManagerMessageResponse;
}

/// List of all replies to messages to PeerManager. See `PeerManagerMessageRequest` for more details.
#[derive(MessageResponse, Debug)]
pub enum PeerManagerMessageResponse {
    RoutedMessageFrom(bool),
    NetworkResponses(NetworkResponses),
    RegisterPeerResponse(RegisterPeerResponse),
    PeerRequestResult(PeerRequestResult),
    PeersResponseResult(()),
    PeerResponse(PeerResponse),
    #[cfg(feature = "test_features")]
    GetPeerIdResult(GetPeerIdResult),
    OutboundTcpConnect(()),
    InboundTcpConnect(()),
    Unregister(()),
    Ban(()),
    #[cfg(feature = "test_features")]
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    StartRoutingTableSync(()),
    #[cfg(feature = "test_features")]
    SetAdvOptions(()),
    #[cfg(feature = "test_features")]
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    SetRoutingTable(()),
}

impl PeerManagerMessageResponse {
    pub fn as_routed_message_from(self) -> bool {
        if let PeerManagerMessageResponse::RoutedMessageFrom(item) = self {
            item
        } else {
            panic!("expected PeerMessageRequest::RoutedMessageFrom(");
        }
    }

    pub fn as_network_response(self) -> NetworkResponses {
        if let PeerManagerMessageResponse::NetworkResponses(item) = self {
            item
        } else {
            panic!("expected PeerMessageRequest::NetworkResponses(");
        }
    }

    pub fn as_consolidate_response(self) -> RegisterPeerResponse {
        if let PeerManagerMessageResponse::RegisterPeerResponse(item) = self {
            item
        } else {
            panic!("expected PeerMessageRequest::ConsolidateResponse(");
        }
    }

    pub fn as_peers_request_result(self) -> PeerRequestResult {
        if let PeerManagerMessageResponse::PeerRequestResult(item) = self {
            item
        } else {
            panic!("expected PeerMessageRequest::PeerRequestResult(");
        }
    }

    pub fn as_peer_response(self) -> PeerResponse {
        if let PeerManagerMessageResponse::PeerResponse(item) = self {
            item
        } else {
            panic!("expected PeerMessageRequest::PeerResponse(");
        }
    }

    #[cfg(feature = "test_features")]
    pub fn as_peer_id_result(self) -> GetPeerIdResult {
        if let PeerManagerMessageResponse::GetPeerIdResult(item) = self {
            item
        } else {
            panic!("expected PeerMessageRequest::GetPeerIdResult(");
        }
    }
}

// TODO(#1313): Use Box
#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
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
    /// General query
    Query {
        query_id: String,
        account_id: AccountId,
        block_reference: BlockReference,
        request: QueryRequest,
    },
    /// Request for receipt execution outcome
    ReceiptOutComeRequest(AccountId, CryptoHash),

    /// The following types of requests are used to trigger actions in the Peer Manager for testing.
    /// (Unit tests) Fetch current routing table.
    FetchRoutingTable,
    /// Data to sync routing table from active peer.
    SyncRoutingTable {
        peer_id: PeerId,
        routing_table_update: RoutingTableUpdate,
    },

    RequestUpdateNonce(PeerId, PartialEdgeInfo),
    ResponseUpdateNonce(Edge),

    /// (Unit tests) Start ping to `PeerId` with `nonce`.
    PingTo(usize, PeerId),
    /// (Unit tests) Fetch all received ping and pong so far.
    FetchPingPongInfo,

    /// A challenge to invalidate a block.
    Challenge(Challenge),

    // IbfMessage
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    IbfMessage {
        peer_id: PeerId,
        ibf_msg: RoutingSyncV2,
    },
}

/// List of `Edges`, which we received from `source_peer_id` gor purpose of validation.
/// Those are list of edges received through `NetworkRequests::Sync` or `NetworkRequests::IbfMessage`.
pub struct ValidateEdgeList {
    /// The list of edges is provided by `source_peer_id`, that peer will be banned
    ///if any of these edges are invalid.
    pub(crate) source_peer_id: PeerId,
    /// List of Edges, which will be sent to `EdgeValidatorActor`.
    pub(crate) edges: Vec<Edge>,
    /// A set of edges, which have been verified. This is a cache with all verified edges.
    /// `EdgeValidatorActor`, and is a source of memory leak.
    /// TODO(#5254): Simplify this process.
    pub(crate) edges_info_shared: Arc<Mutex<HashMap<(PeerId, PeerId), u64>>>,
    /// A concurrent queue. After edge become validated it will be sent from `EdgeValidatorActor` back to
    /// `PeerManagetActor`, and then send to `RoutingTableActor`. And then `RoutingTableActor`
    /// will add them.
    /// TODO(#5254): Simplify this process.
    pub(crate) sender: QueueSender<Edge>,
    #[cfg(feature = "test_features")]
    /// Feature to disable edge validation for purpose of testing.
    pub(crate) adv_disable_edge_signature_verification: bool,
}

impl Debug for ValidateEdgeList {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("source_peer_id").finish()
    }
}

impl Message for ValidateEdgeList {
    type Result = bool;
}

/// Combines peer address info, chain and edge information.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FullPeerInfo {
    pub peer_info: PeerInfo,
    pub chain_info: PeerChainInfoV2,
    pub partial_edge_info: PartialEdgeInfo,
}

#[derive(Debug)]
pub struct NetworkInfo {
    pub connected_peers: Vec<FullPeerInfo>,
    pub num_connected_peers: usize,
    pub peer_max_count: u32,
    pub highest_height_peers: Vec<FullPeerInfo>,
    pub sent_bytes_per_sec: u64,
    pub received_bytes_per_sec: u64,
    /// Accounts of known block and chunk producers from routing table.
    pub known_producers: Vec<KnownProducer>,
    pub peer_counter: usize,
}

impl<A, M> MessageResponse<A, M> for NetworkInfo
where
    A: Actor,
    M: Message<Result = NetworkInfo>,
{
    fn handle<R: ResponseChannel<M>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self)
        }
    }
}

#[derive(Debug)]
pub enum NetworkResponses {
    NoResponse,
    RoutingTableInfo(RoutingTableInfo),
    PingPongInfo { pings: HashMap<usize, (Ping, usize)>, pongs: HashMap<usize, (Pong, usize)> },
    BanPeer(ReasonForBan),
    EdgeUpdate(Box<Edge>),
    RouteNotFound,
}

impl From<NetworkResponses> for PeerManagerMessageResponse {
    fn from(msg: NetworkResponses) -> Self {
        PeerManagerMessageResponse::NetworkResponses(msg)
    }
}

impl<A, M> MessageResponse<A, M> for NetworkResponses
where
    A: Actor,
    M: Message<Result = NetworkResponses>,
{
    fn handle<R: ResponseChannel<M>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self)
        }
    }
}

impl Message for NetworkRequests {
    type Result = NetworkResponses;
}

#[derive(Debug, strum::AsRefStr, AsStaticStr)]
// TODO(#1313): Use Box
#[allow(clippy::large_enum_variant)]
pub enum NetworkClientMessages {
    #[cfg(feature = "test_features")]
    Adversarial(near_network_primitives::types::NetworkAdversarialMessage),

    #[cfg(feature = "sandbox")]
    Sandbox(near_network_primitives::types::NetworkSandboxMessage),

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
    PartialEncodedChunkResponse(PartialEncodedChunkResponseMsg),
    /// Information about chunk such as its header, some subset of parts and/or incoming receipts
    PartialEncodedChunk(PartialEncodedChunk),
    /// Forwarding parts to those tracking the shard (so they don't need to send requests)
    PartialEncodedChunkForward(PartialEncodedChunkForwardMsg),

    /// A challenge to invalidate the block.
    Challenge(Challenge),

    NetworkInfo(NetworkInfo),
}

// TODO(#1313): Use Box
#[derive(Eq, PartialEq, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum NetworkClientResponses {
    /// Adv controls.
    #[cfg(feature = "test_features")]
    AdvResult(u64),

    /// Sandbox controls
    #[cfg(feature = "sandbox")]
    SandboxResult(near_network_primitives::types::SandboxResponse),

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

impl<A, M> MessageResponse<A, M> for NetworkClientResponses
where
    A: Actor,
    M: Message<Result = NetworkClientResponses>,
{
    fn handle<R: ResponseChannel<M>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self)
        }
    }
}

impl Message for NetworkClientMessages {
    type Result = NetworkClientResponses;
}

/// Adapter to break dependency of sub-components on the network requests.
/// For tests use MockNetworkAdapter that accumulates the requests to network.
pub trait PeerManagerAdapter: Sync + Send {
    fn send(
        &self,
        msg: PeerManagerMessageRequest,
    ) -> BoxFuture<'static, Result<PeerManagerMessageResponse, MailboxError>>;

    fn do_send(&self, msg: PeerManagerMessageRequest);
}

#[derive(Default)]
pub struct NetworkRecipient {
    peer_manager_recipient: RwLock<Option<Recipient<PeerManagerMessageRequest>>>,
}

unsafe impl Sync for NetworkRecipient {}

impl NetworkRecipient {
    pub fn set_recipient(&self, peer_manager_recipient: Recipient<PeerManagerMessageRequest>) {
        *self.peer_manager_recipient.write().unwrap() = Some(peer_manager_recipient);
    }
}

impl PeerManagerAdapter for NetworkRecipient {
    fn send(
        &self,
        msg: PeerManagerMessageRequest,
    ) -> BoxFuture<'static, Result<PeerManagerMessageResponse, MailboxError>> {
        self.peer_manager_recipient
            .read()
            .unwrap()
            .as_ref()
            .expect("Recipient must be set")
            .send(msg)
            .boxed()
    }

    fn do_send(&self, msg: PeerManagerMessageRequest) {
        let _ = self
            .peer_manager_recipient
            .read()
            .unwrap()
            .as_ref()
            .expect("Recipient must be set")
            .do_send(msg);
    }
}

#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct SetRoutingTable {
    pub add_edges: Option<Vec<Edge>>,
    pub remove_edges: Option<Vec<SimpleEdge>>,
    pub prune_edges: Option<bool>,
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
    fn test_enum_size() {
        assert_size!(HandshakeFailureReason);
        assert_size!(RegisterPeerResponse);
        assert_size!(PeerRequest);
        assert_size!(PeerResponse);
        assert_size!(NetworkRequests);
        assert_size!(NetworkResponses);
        assert_size!(NetworkClientMessages);
        assert_size!(NetworkClientResponses);
    }

    #[test]
    fn test_struct_size() {
        assert_size!(Handshake);
        assert_size!(Ping);
        assert_size!(Pong);
        assert_size!(RoutingTableUpdate);
        assert_size!(SendMessage);
        assert_size!(RegisterPeer);
        assert_size!(FullPeerInfo);
        assert_size!(NetworkInfo);
    }
}

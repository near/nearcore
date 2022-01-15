/// Type that belong to the network protocol.
pub use crate::network_protocol::{
    Handshake, HandshakeFailureReason, PeerMessage, RoutingTableUpdate,
};
#[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
pub use crate::network_protocol::{PartialSync, RoutingState, RoutingSyncV2, RoutingVersion2};
use crate::private_actix::{
    PeerRequestResult, PeersRequest, RegisterPeer, RegisterPeerResponse, Unregister,
};
use crate::routing::routing_table_view::RoutingTableInfo;
use actix::dev::{MessageResponse, ResponseChannel};
use actix::{Actor, MailboxError, Message};
use futures::future::BoxFuture;
use near_network_primitives::types::{
    AccountIdOrPeerTrackingShard, AccountOrPeerIdOrHash, Ban, Edge, InboundTcpConnect,
    OutboundTcpConnect, PartialEdgeInfo, PartialEncodedChunkForwardMsg,
    PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg, PeerInfo, Ping, Pong,
    ReasonForBan, RoutedMessageBody, RoutedMessageFrom, StateResponseInfo,
};
use near_primitives::block::{ApprovalMessage, Block};
use near_primitives::challenge::Challenge;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::sharding::PartialEncodedChunkWithArcReceipts;
use near_primitives::time::Instant;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockReference, EpochId, ShardId};
use near_primitives::views::QueryRequest;
use std::collections::HashMap;
use std::fmt::Debug;

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

/// Received new peers from another peer.
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct PeersResponse {
    pub(crate) peers: Vec<PeerInfo>,
}

/// List of all messages, which PeerManagerActor accepts through Actix. There is also another list
/// which contains reply for each message to PeerManager.
/// There is 1 to 1 mapping between an entry in `PeerManagerMessageRequest` and `PeerManagerMessageResponse`.
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Debug)]
pub enum PeerManagerMessageRequest {
    RoutedMessageFrom(RoutedMessageFrom),
    NetworkRequests(NetworkRequests),
    RegisterPeer(RegisterPeer),
    PeersRequest(PeersRequest),
    PeersResponse(PeersResponse),
    PeerRequest(PeerRequest),
    #[cfg(feature = "test_features")]
    GetPeerId(crate::private_actix::GetPeerId),
    OutboundTcpConnect(OutboundTcpConnect),
    InboundTcpConnect(InboundTcpConnect),
    Unregister(Unregister),
    Ban(Ban),
    #[cfg(feature = "test_features")]
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    StartRoutingTableSync(crate::private_actix::StartRoutingTableSync),
    #[cfg(feature = "test_features")]
    SetAdvOptions(crate::test_utils::SetAdvOptions),
    #[cfg(feature = "test_features")]
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    SetRoutingTable(crate::test_utils::SetRoutingTable),
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
    GetPeerIdResult(crate::private_actix::GetPeerIdResult),
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
    pub fn as_peer_id_result(self) -> crate::private_actix::GetPeerIdResult {
        if let PeerManagerMessageResponse::GetPeerIdResult(item) = self {
            item
        } else {
            panic!("expected PeerMessageRequest::GetPeerIdResult(");
        }
    }
}

impl From<NetworkResponses> for PeerManagerMessageResponse {
    fn from(msg: NetworkResponses) -> Self {
        PeerManagerMessageResponse::NetworkResponses(msg)
    }
}

// TODO(#1313): Use Box
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
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

#[derive(Debug)]
pub enum NetworkResponses {
    NoResponse,
    RoutingTableInfo(RoutingTableInfo),
    PingPongInfo { pings: HashMap<usize, (Ping, usize)>, pongs: HashMap<usize, (Pong, usize)> },
    BanPeer(ReasonForBan),
    EdgeUpdate(Box<Edge>),
    RouteNotFound,
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

/// Adapter to break dependency of sub-components on the network requests.
/// For tests use MockNetworkAdapter that accumulates the requests to network.
pub trait PeerManagerAdapter: Sync + Send {
    fn send(
        &self,
        msg: PeerManagerMessageRequest,
    ) -> BoxFuture<'static, Result<PeerManagerMessageResponse, MailboxError>>;

    fn do_send(&self, msg: PeerManagerMessageRequest);
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
    }

    #[test]
    fn test_struct_size() {
        assert_size!(Handshake);
        assert_size!(Ping);
        assert_size!(Pong);
        assert_size!(RoutingTableUpdate);
        assert_size!(RegisterPeer);
    }
}

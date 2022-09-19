/// This file is contains all types used for communication between `Actors` within this crate.
/// They are not meant to be used outside.
use crate::network_protocol::{
    Edge, PartialEdgeInfo, PeerInfo, PeerMessage, RoutingTableUpdate,
};
use crate::peer_manager::connection::{Connection, PoolError};
use crate::types::{Ban, ReasonForBan};
use conqueue::QueueSender;
use near_primitives::network::PeerId;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

/// Received new peers from another peer.
#[derive(Debug, Clone)]
pub(crate) struct PeersResponse {
    pub(crate) peers: Vec<PeerInfo>,
}

#[derive(actix::Message, Debug, strum::IntoStaticStr, strum::EnumVariantNames)]
#[rtype(result = "PeerToManagerMsgResp")]
pub(crate) enum PeerToManagerMsg {
    RegisterPeer(RegisterPeer),
    PeersRequest(PeersRequest),
    PeersResponse(PeersResponse),
    Unregister(Unregister),
    Ban(Ban),
    RequestUpdateNonce(PeerId, PartialEdgeInfo),
    ResponseUpdateNonce(Edge),
    /// Data to sync routing table from active peer.
    SyncRoutingTable {
        peer_id: PeerId,
        routing_table_update: RoutingTableUpdate,
    },

    // PeerRequest
    UpdatePeerInfo(PeerInfo),
}

/// List of all replies to messages to `PeerManager`. See `PeerManagerMessageRequest` for more details.
#[derive(actix::MessageResponse, Debug)]
pub(crate) enum PeerToManagerMsgResp {
    RegisterPeer(RegisterPeerResponse),
    PeersRequest(PeerRequestResult),

    // RequestUpdateNonce
    // ResponseUpdateNonce
    EdgeUpdate(Box<Edge>),
    BanPeer(ReasonForBan),

    // PeerResponse
    Empty,
}
/// Actor message which asks `PeerManagerActor` to register peer.
/// Returns `RegisterPeerResult` with `Accepted` if connection should be kept
/// or a reject response otherwise.
#[derive(actix::Message, Clone, Debug)]
#[rtype(result = "RegisterPeerResponse")]
pub(crate) struct RegisterPeer {
    pub connection: Arc<Connection>,
}

#[derive(Debug)]
pub(crate) enum RegisterPeerError {
    Blacklisted,
    Banned,
    PoolError(PoolError),
    ConnectionLimitExceeded,
    NotTier1Peer,
}

#[derive(actix::MessageResponse, Debug)]
pub(crate) enum RegisterPeerResponse {
    Accept,
    Reject(RegisterPeerError),
}

/// Unregister message from Peer to PeerManager.
#[derive(actix::Message, Debug, PartialEq, Eq, Clone)]
#[rtype(result = "()")]
pub(crate) struct Unregister {
    pub peer_id: PeerId,
}

/// Requesting peers from peer manager to communicate to a peer.
#[derive(actix::Message, Clone, Debug)]
#[rtype(result = "PeerRequestResult")]
pub(crate) struct PeersRequest {}

#[derive(Debug, actix::MessageResponse)]
pub(crate) struct PeerRequestResult {
    pub peers: Vec<PeerInfo>,
}

#[derive(actix::Message)]
#[rtype(result = "()")]
pub(crate) struct StopMsg {}

#[derive(actix::Message, Clone, Debug)]
#[rtype(result = "()")]
pub(crate) struct SendMessage {
    pub message: Arc<PeerMessage>,
    pub context: opentelemetry::Context,
}

impl Debug for ValidateEdgeList {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("source_peer_id").finish()
    }
}

/// List of `Edges`, which we received from `source_peer_id` to validate.
/// Those are list of edges received through `NetworkRequests::Sync`.
#[derive(actix::Message)]
#[rtype(result = "bool")]
pub(crate) struct ValidateEdgeList {
    /// The list of edges is provided by `source_peer_id`, that peer will be banned
    ///if any of these edges are invalid.
    pub source_peer_id: PeerId,
    /// List of Edges, which will be sent to `EdgeValidatorActor`.
    pub edges: Vec<Edge>,
    /// A set of edges, which have been verified. This is a cache with all verified edges.
    /// `EdgeValidatorActor`, and is a source of memory leak.
    /// TODO(#5254): Simplify this process.
    pub edges_info_shared: Arc<Mutex<HashMap<(PeerId, PeerId), u64>>>,
    /// A concurrent queue. After edge become validated it will be sent from `EdgeValidatorActor` back to
    /// `PeerManagetActor`, and then send to `RoutingTableActor`. And then `RoutingTableActor`
    /// will add them.
    /// TODO(#5254): Simplify this process.
    pub sender: QueueSender<Edge>,
}

impl PeerToManagerMsgResp {
    pub fn unwrap_consolidate_response(self) -> RegisterPeerResponse {
        match self {
            Self::RegisterPeer(item) => item,
            _ => panic!("expected PeerMessageRequest::ConsolidateResponse"),
        }
    }

    pub fn unwrap_peers_request_result(self) -> PeerRequestResult {
        match self {
            PeerToManagerMsgResp::PeersRequest(item) => item,
            _ => panic!("expected PeerMessageRequest::PeerRequestResult"),
        }
    }
}

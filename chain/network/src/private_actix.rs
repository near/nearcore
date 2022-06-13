/// This file is contains all types used for communication between `Actors` within this crate.
/// They are not meant to be used outside.
use crate::network_protocol::PeerMessage;
use crate::peer::peer_actor::PeerActor;
use actix::{Addr, Message};
use conqueue::QueueSender;
use near_network_primitives::types::{
    Edge, PartialEdgeInfo, PeerChainInfoV2, PeerInfo, PeerType, SimpleEdge,
};
use near_primitives::network::PeerId;
use near_primitives::version::ProtocolVersion;
use near_rate_limiter::ThrottleController;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

/// Actor message which asks `PeerManagerActor` to register peer.
/// Returns `RegisterPeerResult` with `Accepted` if connection should be kept
/// or a reject response otherwise.
#[derive(actix::Message, Clone, Debug)]
#[rtype(result = "RegisterPeerResponse")]
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

#[derive(actix::MessageResponse, Debug)]
pub enum RegisterPeerResponse {
    Accept(Option<PartialEdgeInfo>),
    InvalidNonce(Box<Edge>),
    Reject,
}

/// Unregister message from Peer to PeerManager.
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct Unregister {
    pub(crate) peer_id: PeerId,
    pub(crate) peer_type: PeerType,
    pub(crate) remove_from_peer_store: bool,
}

/// Requesting peers from peer manager to communicate to a peer.
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(actix::Message, Clone, Debug)]
#[rtype(result = "PeerRequestResult")]
pub struct PeersRequest {}

#[derive(Debug, actix::MessageResponse)]
pub struct PeerRequestResult {
    pub peers: Vec<PeerInfo>,
}

#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct StopMsg {}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(actix::Message, Clone, Debug)]
#[cfg(feature = "test_features")]
#[rtype(result = "()")]
pub struct StartRoutingTableSync {
    pub peer_id: PeerId,
}

#[cfg(feature = "test_features")]
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(actix::Message, Clone, Debug)]
#[rtype(result = "GetPeerIdResult")]
pub struct GetPeerId {}

#[derive(Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct SendMessage {
    pub(crate) message: PeerMessage,
    pub(crate) context: opentelemetry::Context,
}

#[cfg(feature = "test_features")]
#[derive(actix::MessageResponse, Debug, serde::Serialize)]
pub struct GetPeerIdResult {
    pub(crate) peer_id: PeerId,
}

impl Debug for ValidateEdgeList {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("source_peer_id").finish()
    }
}

/// List of `Edges`, which we received from `source_peer_id` gor purpose of validation.
/// Those are list of edges received through `NetworkRequests::Sync` or `NetworkRequests::IbfMessage`.
#[derive(actix::Message)]
#[rtype(result = "bool")]
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

#[derive(actix::MessageResponse, Debug)]
#[cfg_attr(feature = "test_features", derive(serde::Serialize))]
pub struct GetRoutingTableResult {
    pub edges_info: Vec<SimpleEdge>,
}

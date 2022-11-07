/// This file is contains all types used for communication between `Actors` within this crate.
/// They are not meant to be used outside.
use crate::network_protocol::{PeerInfo, PeerMessage};
use crate::peer_manager::connection;
use std::fmt::Debug;
use std::sync::Arc;

/// Received new peers from another peer.
#[derive(Debug, Clone)]
pub(crate) struct PeersResponse {
    pub(crate) peers: Vec<PeerInfo>,
}

#[derive(actix::Message, Debug, strum::IntoStaticStr, strum::EnumVariantNames)]
#[rtype(result = "PeerToManagerMsgResp")]
pub(crate) enum PeerToManagerMsg {
    PeersRequest(PeersRequest),
    PeersResponse(PeersResponse),
    // PeerRequest
    UpdatePeerInfo(PeerInfo),
}

/// List of all replies to messages to `PeerManager`. See `PeerManagerMessageRequest` for more details.
#[derive(actix::MessageResponse, Debug)]
pub(crate) enum PeerToManagerMsgResp {
    PeersRequest(PeerRequestResult),
    // PeerResponse
    Empty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RegisterPeerError {
    Blacklisted,
    Banned,
    PoolError(connection::PoolError),
    ConnectionLimitExceeded,
    InvalidEdge,
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
}

impl PeerToManagerMsgResp {
    pub fn unwrap_peers_request_result(self) -> PeerRequestResult {
        match self {
            PeerToManagerMsgResp::PeersRequest(item) => item,
            _ => panic!("expected PeerMessageRequest::PeerRequestResult"),
        }
    }
}

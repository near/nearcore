/// This file is contains all types used for communication between `Actors` within this crate.
/// They are not meant to be used outside.
use crate::concurrency::outgoing_queue_limiter::OutgoingPermit;
use crate::network_protocol::PeerMessage;
use crate::peer_manager::connection;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RegisterPeerError {
    Blacklisted,
    Banned,
    PoolError(connection::PoolError),
    ConnectionLimitExceeded,
    NotTier1Peer,
    Tier1InboundDisabled,
    InvalidEdge,
    UnexpectedTier3Connection,
}

#[derive(Debug)]
pub(crate) struct SendMessage {
    pub message: Arc<PeerMessage>,
    /// Optional pre-acquired reservation against the outgoing-queue
    /// limiter. Set by callers that reserved capacity before producing the
    /// message (state/epoch sync responses); other callers leave it None
    /// and acquire on the fly inside PeerActor::send_message.
    pub reserved_permit: Option<OutgoingPermit>,
}

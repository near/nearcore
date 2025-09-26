use near_async::Message;

/// This file is contains all types used for communication between `Actors` within this crate.
/// They are not meant to be used outside.
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
}

#[derive(Message, Clone, Debug)]
pub(crate) struct SendMessage {
    pub message: Arc<PeerMessage>,
}

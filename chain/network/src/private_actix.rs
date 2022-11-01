/// This file is contains all types used for communication between `Actors` within this crate.
/// They are not meant to be used outside.
use crate::network_protocol::{PeerMessage};
use crate::peer_manager::connection;
use std::fmt::{Debug};
use std::sync::{Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RegisterPeerError {
    Blacklisted,
    Banned,
    PoolError(connection::PoolError),
    ConnectionLimitExceeded,
    InvalidEdge,
}

#[derive(actix::Message)]
#[rtype(result = "()")]
pub(crate) struct StopMsg {}

#[derive(actix::Message, Clone, Debug)]
#[rtype(result = "()")]
pub(crate) struct SendMessage {
    pub message: Arc<PeerMessage>,
}

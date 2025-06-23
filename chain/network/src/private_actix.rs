/// This file is contains all types used for communication between `Actors` within this crate.
/// They are not meant to be used outside.
use crate::network_protocol::PeerMessage;
use crate::peer_manager::connection;
use near_o11y::WithSpanContext;
use near_o11y::span_wrapped_msg::SpanWrapped;
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

#[derive(actix::Message, Clone, Debug)]
#[rtype(result = "()")]
pub(crate) struct SendMessageInner {
    pub message: Arc<PeerMessage>,
}

pub(crate) type SendMessage = WithSpanContext<SpanWrapped<SendMessageInner>>;

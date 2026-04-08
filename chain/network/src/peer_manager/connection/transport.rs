use crate::network_protocol::PeerMessage;
use near_primitives::network::PeerId;
use std::sync::Arc;

use super::Pool;

/// Abstraction over the transport layer for sending messages to peers.
///
/// Production: `PoolTransport` wraps `connection::Pool` (TCP connections).
/// Testloop: `TestLoopTransport` uses in-memory dispatch (no TCP).
pub trait NetworkTransport: Send + Sync + 'static {
    /// Send a message to a specific peer. Returns true if the message was sent.
    fn send_message(&self, peer_id: PeerId, msg: Arc<PeerMessage>) -> bool;
    /// Broadcast a message to all connected peers.
    fn broadcast_message(&self, msg: Arc<PeerMessage>);
}

/// Production transport backed by the TCP connection pool.
pub struct PoolTransport {
    pool: Pool,
}

impl PoolTransport {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

impl NetworkTransport for PoolTransport {
    fn send_message(&self, peer_id: PeerId, msg: Arc<PeerMessage>) -> bool {
        self.pool.send_message(peer_id, msg)
    }

    fn broadcast_message(&self, msg: Arc<PeerMessage>) {
        self.pool.broadcast_message(msg)
    }
}

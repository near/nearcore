use crate::network_protocol::PeerMessage;
use crate::peer_manager::connection::Pool;
use near_primitives::network::PeerId;
use std::sync::Arc;

/// Abstraction over network message delivery.
///
/// Production uses `PoolTransport` which delegates to `Pool`. TestLoop can
/// provide an implementation that delivers messages directly to target node
/// actors without TCP.
pub trait NetworkTransport: Send + Sync + 'static {
    /// Send a message to a connected peer. Returns true if delivered.
    fn send_message(&self, peer_id: PeerId, msg: Arc<PeerMessage>) -> bool;
    /// Broadcast a message to all connected peers.
    // TODO: remove allow once broadcast call sites switch to transport (iteration 4).
    #[allow(dead_code)]
    fn broadcast_message(&self, msg: Arc<PeerMessage>);
}

/// Production transport that delegates to a connection `Pool`.
pub(crate) struct PoolTransport {
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

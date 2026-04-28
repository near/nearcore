use crate::network_protocol::PeerInfo;
use crate::tcp;
use crate::types::{PeerMessage, ReasonForBan};
use near_async::time;
use near_primitives::network::PeerId;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

/// The narrow interface between business logic (NetworkState) and message
/// delivery. This is the ONLY thing that differs between production
/// (TcpTransport) and testloop (TestLoopTransport).
///
/// NetworkState receives `&dyn NetworkTransport` (or `Arc<dyn>` for
/// closures) as a method parameter. PMA holds `Arc<dyn NetworkTransport>`.
#[allow(dead_code)]
pub trait NetworkTransport: Send + Sync + 'static {
    /// Deliver a message to a specific peer over the given tier.
    /// Returns true if the message was enqueued; false if not connected.
    fn send_message(&self, tier: tcp::Tier, peer_id: PeerId, msg: Arc<PeerMessage>) -> bool;

    /// Broadcast a message to all connected TIER2 peers. T1 and T3 do
    /// not have broadcast semantics. Must be synchronous (enqueue-only)
    /// — stop_actor calls this and is itself synchronous.
    fn broadcast_message(&self, msg: Arc<PeerMessage>);

    /// Initiate a connection to a peer. Returns a ConnectHandle that can
    /// be awaited or dropped (fire-and-forget).
    /// Idempotent: no-op if already connected or handshaking.
    fn connect_to_peer(
        &self,
        _clock: &time::Clock,
        _peer_info: PeerInfo,
        _tier: tcp::Tier,
    ) -> ConnectHandle {
        ConnectHandle::noop()
    }

    /// Close a connection to a peer.
    /// ban_reason: None = graceful disconnect, Some = ban.
    fn disconnect_peer(&self, _peer_id: &PeerId, _ban_reason: Option<ReasonForBan>) {}

    /// Graceful shutdown.
    fn shutdown(&self) {}
}

/// Error returned by connect_to_peer.
#[allow(dead_code)]
#[derive(Debug)]
pub enum ConnectError {
    Cancelled,
}

/// Handle returned by `connect_to_peer`. Implements `Future` so callers
/// can `.await` it directly, or drop it for fire-and-forget. The
/// spawned dial + handshake task lives on the transport's spawner
/// independently of the handle — dropping the handle doesn't cancel
/// the work, it only stops listening for the result.
#[allow(dead_code)]
pub struct ConnectHandle {
    rx: oneshot::Receiver<Result<(), ConnectError>>,
}

#[allow(dead_code)]
impl ConnectHandle {
    pub(crate) fn new(rx: oneshot::Receiver<Result<(), ConnectError>>) -> Self {
        Self { rx }
    }

    pub fn noop() -> Self {
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Ok(()));
        Self { rx }
    }
}

impl Future for ConnectHandle {
    type Output = Result<(), ConnectError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.rx).poll(cx).map(|r| r.unwrap_or(Err(ConnectError::Cancelled)))
    }
}

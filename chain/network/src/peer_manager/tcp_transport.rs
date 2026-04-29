use crate::network_protocol::PeerInfo;
use crate::peer::peer_actor::PeerActor;
use crate::peer_manager::connection;
use crate::peer_manager::network_state::NetworkState;
use crate::peer_manager::network_transport::{
    ConnectError, ConnectHandle, NetworkTransport, PeerTransportStats, TransportInfo,
};
#[cfg(test)]
use crate::peer_manager::peer_manager_actor::Event;
use crate::peer_manager::peer_manager_actor::PeerManagerActor;
use crate::private_messages::RegisterPeerError;
use crate::tcp;
use crate::types::{PeerMessage, ReasonForBan};
use anyhow::Context as _;
use near_async::ActorSystem;
use near_async::futures::FutureSpawner;
use near_async::time;
use near_async::tokio::TokioRuntimeHandle;
use near_primitives::network::PeerId;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use tokio::sync::oneshot;

/// Production implementation of `NetworkTransport`.
///
/// Owns the connection Pools (tier1/2/3) and the inbound-handshake
/// semaphore. Holds `Arc<NetworkState>` purely for passing it to
/// `PeerActor::spawn` when accepting inbound connections or initiating
/// outbound ones — TcpTransport's own methods never call NetworkState
/// for business logic, only PeerActor does.
pub struct TcpTransport {
    pub(crate) tier1: connection::Pool,
    pub(crate) tier2: connection::Pool,
    pub(crate) tier3: connection::Pool,
    pub(crate) inbound_handshake_permits: Arc<tokio::sync::Semaphore>,
    pub(crate) state: Arc<NetworkState>,
    clock: time::Clock,
    #[allow(dead_code)]
    actor_system: ActorSystem,
    spawner: Box<dyn FutureSpawner>,
    /// Self-reference used by `connect_to_peer` to pass an
    /// `Arc<TcpTransport>` to `PeerActor::spawn_and_handshake`.
    /// Populated at construction via `Arc::new_cyclic`.
    self_weak: Weak<Self>,
    /// Signals the TCP listener to exit. `shutdown()` drops the sender,
    /// which closes the receiver and breaks the accept loop.
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
}

/// Limit number of pending Peer actors to avoid OOM.
pub(crate) const LIMIT_PENDING_PEERS: usize = 60;

impl TcpTransport {
    /// Production constructor.
    pub(crate) fn new(
        state: Arc<NetworkState>,
        clock: time::Clock,
        actor_system: ActorSystem,
        handle: TokioRuntimeHandle<PeerManagerActor>,
    ) -> Arc<Self> {
        Self::new_with_spawner(state, clock, actor_system, handle.future_spawner())
    }

    /// Shared constructor. Production passes a PMA-backed spawner;
    /// tests can pass a standalone tokio spawner.
    pub(crate) fn new_with_spawner(
        state: Arc<NetworkState>,
        clock: time::Clock,
        actor_system: ActorSystem,
        spawner: Box<dyn FutureSpawner>,
    ) -> Arc<Self> {
        let node_id = state.config.node_id();
        Arc::new_cyclic(|self_weak| Self {
            tier1: connection::Pool::new(node_id.clone()),
            tier2: connection::Pool::new(node_id.clone()),
            tier3: connection::Pool::new(node_id),
            inbound_handshake_permits: Arc::new(tokio::sync::Semaphore::new(LIMIT_PENDING_PEERS)),
            state,
            clock,
            actor_system,
            spawner,
            self_weak: self_weak.clone(),
            shutdown_tx: Mutex::new(None),
        })
    }

    /// Insert a newly-registered connection into the tier's Pool.
    /// Called by PeerActor during register (step 2 of the 3-step flow).
    pub(crate) fn pool_insert(
        &self,
        tier: tcp::Tier,
        conn: Arc<connection::Connection>,
    ) -> Result<(), RegisterPeerError> {
        let pool = match tier {
            tcp::Tier::T1 => &self.tier1,
            tcp::Tier::T2 => &self.tier2,
            tcp::Tier::T3 => &self.tier3,
        };
        pool.insert_ready(conn).map_err(RegisterPeerError::PoolError)
    }

    /// Remove a connection from the tier's Pool.
    /// Called by PeerActor during unregister (step 1 of the 2-step flow).
    pub(crate) fn pool_remove(&self, tier: tcp::Tier, conn: &Arc<connection::Connection>) {
        let pool = match tier {
            tcp::Tier::T1 => &self.tier1,
            tcp::Tier::T2 => &self.tier2,
            tcp::Tier::T3 => &self.tier3,
        };
        pool.remove(conn);
    }

    /// Spawns the TCP accept loop if `node_addr` is configured. Intended
    /// to be called exactly once, after PMA construction.
    pub fn start(self: &Arc<Self>) {
        let Some(server_addr) = self.state.config.node_addr else {
            return;
        };
        tracing::debug!(target: "network", at = ?server_addr, "starting public server");
        let listener = match server_addr.listener() {
            Ok(it) => it,
            Err(e) => panic!("failed to start listening on server_addr={server_addr:?} e={e:?}"),
        };
        #[cfg(test)]
        self.state.config.event_sink.send(Event::ServerStarted);

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        *self.shutdown_tx.lock() = Some(shutdown_tx);

        let this = self.clone();
        self.spawner.spawn_boxed("PeerManagerActor listener loop", Box::pin(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    res = listener.accept() => {
                        let Ok(stream) = res else { continue };
                        // Always let the new peer to send a handshake message.
                        // Only then we can decide whether we should accept a connection.
                        // It is expected to be reasonably cheap: eventually, for TIER2 network
                        // we would like to exchange set of connected peers even without establishing
                        // a proper connection.
                        tracing::debug!(target: "network", from = ?stream.peer_addr, "got new connection");
                        if let Err(err) = PeerActor::spawn(
                            this.clock.clone(),
                            this.actor_system.clone(),
                            stream,
                            this.state.clone(),
                            this.clone(),
                        ) {
                            tracing::info!(target:"network", ?err, "peer actor spawn failed");
                        }
                    }
                }
            }
        }));
    }

    /// Spawn a PeerActor from an already-opened stream. Intended for
    /// test fixtures (both unit tests and integration-tests) that need to
    /// exercise the handshake flow without going through the production
    /// `connect_to_peer` dial. Tests drive this method directly via the
    /// `Arc<TcpTransport>` exposed from `PeerManagerActor::spawn`.
    pub fn spawn_outbound_from_stream(self: &Arc<Self>, stream: tcp::Stream) -> anyhow::Result<()> {
        PeerActor::spawn(
            self.clock.clone(),
            self.actor_system.clone(),
            stream,
            self.state.clone(),
            self.clone(),
        )?;
        Ok(())
    }
}

impl NetworkTransport for TcpTransport {
    fn send_message(&self, tier: tcp::Tier, peer_id: PeerId, msg: Arc<PeerMessage>) -> bool {
        match tier {
            tcp::Tier::T1 => self.tier1.send_message(peer_id, msg),
            tcp::Tier::T2 => self.tier2.send_message(peer_id, msg),
            tcp::Tier::T3 => self.tier3.send_message(peer_id, msg),
        }
    }

    fn broadcast_message(&self, msg: Arc<PeerMessage>) {
        self.tier2.broadcast_message(msg);
    }

    fn connect_to_peer(
        &self,
        clock: &time::Clock,
        peer_info: PeerInfo,
        tier: tcp::Tier,
    ) -> ConnectHandle {
        // Idempotency guard: if we already have a ready connection on this
        // tier, skip the dial. The Pool's `start_outbound` inside
        // PeerActor::spawn_inner is the authoritative check against
        // duplicate in-flight handshakes; this early-return just avoids a
        // wasted TCP dial when we already have the peer in the Pool.
        let already_ready = match tier {
            tcp::Tier::T1 => self.tier1.load().ready.contains_key(&peer_info.id),
            tcp::Tier::T2 => self.tier2.load().ready.contains_key(&peer_info.id),
            tcp::Tier::T3 => self.tier3.load().ready.contains_key(&peer_info.id),
        };
        if already_ready {
            return ConnectHandle::noop();
        }

        // Recover Arc<Self> via the stored Weak. The Weak is always live
        // here because PMA holds an Arc to us for the duration of the
        // actor's lifetime.
        let Some(this) = self.self_weak.upgrade() else {
            return ConnectHandle::noop();
        };
        let (tx, rx) = oneshot::channel();
        let clock = clock.clone();
        self.spawner.spawn_boxed("connect_to_peer", Box::pin(async move {
            let result: anyhow::Result<()> = async {
                let stream = tcp::Stream::connect(
                    &peer_info,
                    tier,
                    &this.state.config.socket_options,
                )
                .await
                .context("tcp::Stream::connect()")?;
                PeerActor::spawn_and_handshake(
                    clock,
                    this.actor_system.clone(),
                    stream,
                    this.state.clone(),
                    this.clone(),
                )
                .await
                .context("PeerActor::spawn()")?;
                Ok(())
            }
            .await;
            let result = result.map_err(|err| {
                tracing::info!(target: "network", %err, %peer_info, ?tier, "connect_to_peer failed");
                ConnectError::Failed
            });
            let _ = tx.send(result);
        }));
        ConnectHandle::new(rx)
    }

    fn disconnect_peer(&self, peer_id: &PeerId, ban_reason: Option<ReasonForBan>) {
        // A peer may be connected on multiple tiers simultaneously
        // (e.g. T1 + T2 for a validator). Stop the connection on every
        // tier we find it on — partial disconnect would leave a stale
        // entry on the other tier.
        for pool in [&self.tier1, &self.tier2, &self.tier3] {
            let snapshot = pool.load();
            if let Some(conn) = snapshot.ready.get(peer_id) {
                conn.stop(ban_reason);
            }
        }
    }

    fn shutdown(&self) {
        // Drop the sender — the listener's select! closes and the loop exits.
        drop(self.shutdown_tx.lock().take());
    }

    fn transport_info(&self) -> TransportInfo {
        let tier2 = self.tier2.load();
        // Include T3 peer stats so PMA's idle-connection cleanup can read
        // last_time_received_message via transport_info instead of touching
        // the Pool directly.
        let tier3 = self.tier3.load();

        let mut peer_stats: HashMap<PeerId, PeerTransportStats> = HashMap::new();
        // T2 comes first; if the same peer_id appears in T3, the T2 entry
        // is kept (via `or_insert_with`). Keying by peer_id collapses
        // duplicates naturally.
        for (peer_id, conn) in tier2.ready.iter().chain(tier3.ready.iter()) {
            peer_stats.entry(peer_id.clone()).or_insert_with(|| {
                let s = &conn.stats;
                PeerTransportStats {
                    last_time_received_message: conn.last_time_received_message.load(),
                    last_time_peer_requested: conn.last_time_peer_requested.load(),
                    received_bytes_per_sec: s.received_bytes_per_sec.load(Ordering::Relaxed),
                    received_messages_per_sec: s.received_messages_per_sec.load(Ordering::Relaxed),
                    sent_bytes_per_sec: s.sent_bytes_per_sec.load(Ordering::Relaxed),
                }
            });
        }
        TransportInfo {
            pending_outbound: tier2.outbound_handshakes.iter().cloned().collect(),
            peer_stats,
        }
    }
}

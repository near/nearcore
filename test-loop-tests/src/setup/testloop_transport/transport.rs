use super::registry::TestLoopNodeRegistry;
use super::shared_state::TestLoopNetworkSharedStateV2;
use near_async::futures::{FutureSpawner, FutureSpawnerExt};
use near_async::time;
use near_network::tcp;
use near_network::types::{PeerInfo, PeerMessage, ReasonForBan};
use near_network::{
    ClosingReason, ConnectHandle, NetworkState, NetworkTransport, PeerDisconnectInfo, RoutedAction,
    TransportInfo,
};
use near_primitives::network::PeerId;
use std::sync::{Arc, Weak};

/// Simulated network transit delay applied once per hop. Matches the
/// 10 ms delay the mock applied via `with_delay` on senders.
pub const NETWORK_DELAY: time::Duration = time::Duration::milliseconds(10);

/// In-memory `NetworkTransport` for testloop. Owns nothing more than
/// what it takes to deliver a `PeerMessage` from one peer to another.
/// Routing, dispatch, peer management, and the connect/disconnect
/// lifecycle all live on `NetworkState` — shared with production.
pub struct TestLoopTransport {
    my_peer_id: PeerId,
    state: Arc<NetworkState>,
    shared: TestLoopNetworkSharedStateV2,
    registry: TestLoopNodeRegistry,
    future_spawner: Arc<dyn FutureSpawner>,
    clock: time::Clock,
    /// Self-reference used when handing `Arc<dyn NetworkTransport>` to
    /// NetworkState lifecycle methods (`on_peer_disconnected`,
    /// `disconnect_and_ban`). Mirrors `TcpTransport::self_weak`. The
    /// Weak always upgrades because the testloop env holds an Arc.
    self_weak: Weak<Self>,
}

impl TestLoopTransport {
    pub fn new(
        my_peer_id: PeerId,
        state: Arc<NetworkState>,
        shared: TestLoopNetworkSharedStateV2,
        registry: TestLoopNodeRegistry,
        future_spawner: Arc<dyn FutureSpawner>,
        clock: time::Clock,
    ) -> Arc<Self> {
        Arc::new_cyclic(|self_weak| Self {
            my_peer_id,
            state,
            shared,
            registry,
            future_spawner,
            clock,
            self_weak: self_weak.clone(),
        })
    }

    /// Read-only accessor for the underlying `NetworkState`. Mirrors
    /// the production transport pattern: callers within the testloop
    /// crate use this rather than reaching into a `pub(super)` field.
    pub(super) fn state(&self) -> &Arc<NetworkState> {
        &self.state
    }

    fn self_arc(&self) -> Arc<Self> {
        self.self_weak.upgrade().expect("TestLoopTransport self_weak upgrade")
    }

    /// Spawn an `on_peer_disconnected` task. Used by `disconnect_peer`
    /// to notify both ends of the connection. The state, info, reason,
    /// and transport vary per call; the spawn shape is identical.
    fn spawn_disconnect(
        &self,
        target_state: Arc<NetworkState>,
        info: PeerDisconnectInfo,
        reason: ClosingReason,
        transport: Arc<dyn NetworkTransport>,
        label: &'static str,
    ) {
        let clock = self.clock.clone();
        self.future_spawner.spawn(label, async move {
            target_state.on_peer_disconnected(&clock, &info, reason, transport).await;
        });
    }

    /// Entry point invoked by another node's `send_message` after the
    /// simulated network delay elapses. Mirrors PeerActor's incoming
    /// dispatch: routed messages go through `process_incoming_routed`
    /// (TTL + dedup + route-back), everything else goes straight to
    /// `handle_peer_message`.
    async fn deliver_incoming(
        self: Arc<Self>,
        from: PeerId,
        tier: tcp::Tier,
        msg: Arc<PeerMessage>,
    ) {
        match (*msg).clone() {
            PeerMessage::Routed(routed) => {
                let action = self.state.process_incoming_routed(&self.clock, &from, tier, routed);
                match action {
                    RoutedAction::ForMe(m) => {
                        self.dispatch_to_client(from, PeerMessage::Routed(m), tier).await
                    }
                    RoutedAction::Forward(m) => {
                        let transport: &dyn NetworkTransport = self.as_ref();
                        self.state.send_message_to_peer(&self.clock, tier, m, transport);
                    }
                    RoutedAction::Dropped => {}
                }
            }
            other => self.dispatch_to_client(from, other, tier).await,
        }
    }

    async fn dispatch_to_client(self: &Arc<Self>, from: PeerId, msg: PeerMessage, tier: tcp::Tier) {
        let result = self
            .state
            .handle_peer_message(&self.clock, from.clone(), msg, /* was_requested */ false)
            .await;
        match result {
            Ok(Some(resp)) => {
                self.send_message(tier, from, Arc::new(resp));
            }
            Ok(None) => {}
            Err(ban) => {
                let transport: &dyn NetworkTransport = self.as_ref();
                self.state.disconnect_and_ban(&self.clock, &from, ban, transport);
            }
        }
    }
}

impl NetworkTransport for TestLoopTransport {
    fn send_message(&self, tier: tcp::Tier, peer_id: PeerId, msg: Arc<PeerMessage>) -> bool {
        // Connectivity gate. state.peers is the canonical map; banned
        // peers are absent (peer_store.peer_ban + on_peer_disconnected
        // remove them). Partitions are filters.
        if !self.state.peers.is_connected_on_tier(&peer_id, tier) {
            return false;
        }

        let Some(filtered) = self.shared.apply_filters(&self.my_peer_id, &peer_id, &msg) else {
            return false;
        };
        let extra = self.shared.compute_extra_delay(&self.my_peer_id, &peer_id, &filtered);

        let Some(target) = self.registry.get(&peer_id) else {
            return false;
        };

        let from = self.my_peer_id.clone();
        let clock = self.clock.clone();
        let msg = Arc::new(filtered);
        self.future_spawner.spawn("testloop deliver", async move {
            clock.sleep(NETWORK_DELAY + extra).await;
            target.deliver_incoming(from, tier, msg).await;
        });
        true
    }

    fn broadcast_message(&self, msg: Arc<PeerMessage>) {
        // Trait-level broadcast is T2-only by design.
        let peers: Vec<PeerId> = self.state.peers.tier2().keys().cloned().collect();
        for peer_id in peers {
            self.send_message(tcp::Tier::T2, peer_id, msg.clone());
        }
    }

    fn transport_info(&self) -> TransportInfo {
        TransportInfo::default()
    }

    fn connect_to_peer(
        &self,
        _clock: &time::Clock,
        _peer_info: PeerInfo,
        _tier: tcp::Tier,
    ) -> ConnectHandle {
        // No-op. T4 setup directly populates state.peers, peer_store,
        // and account_announcements; PMA's monitor_peers_trigger and
        // tier1_connect short-circuit on the relevant idempotency /
        // status checks.
        ConnectHandle::noop()
    }

    fn disconnect_peer(&self, peer_id: &PeerId, ban_reason: Option<ReasonForBan>) {
        // The one place the transport touches connectivity state.
        // Mirrors production: PeerActor's stopping() invokes
        // on_peer_disconnected, which writes peer_store + clears
        // state.peers + broadcasts edge removal. We do the same on
        // both sides via direct calls into NetworkState.

        let Some(my_peer_state) = self.state.peers.get(peer_id) else {
            return;
        };
        let my_info = PeerDisconnectInfo {
            peer_info: my_peer_state.peer_info.clone(),
            tier: my_peer_state.tier,
            peer_type: my_peer_state.peer_type,
        };
        // ClosingReason::Ban only on the initiator; target sees graceful
        // disconnect — the ban applies to us about them, not the reverse.
        let my_reason =
            ban_reason.map(ClosingReason::Ban).unwrap_or(ClosingReason::DisconnectMessage);
        self.spawn_disconnect(
            self.state.clone(),
            my_info,
            my_reason,
            self.self_arc(),
            "testloop disconnect (initiator)",
        );

        if let Some(target) = self.registry.get(peer_id) {
            if let Some(their_peer_state) = target.state().peers.get(&self.my_peer_id) {
                let their_info = PeerDisconnectInfo {
                    peer_info: their_peer_state.peer_info.clone(),
                    tier: their_peer_state.tier,
                    peer_type: their_peer_state.peer_type,
                };
                self.spawn_disconnect(
                    target.state().clone(),
                    their_info,
                    ClosingReason::DisconnectMessage,
                    target.clone(),
                    "testloop disconnect (target)",
                );
            }
        }
    }

    fn shutdown(&self) {
        // No-op. kill_node in T4 unregisters from the registry, which
        // is what actually stops delivery.
    }
}

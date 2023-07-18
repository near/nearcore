use super::NetworkState;
use crate::network_protocol::{
    DistanceVector, Edge, EdgeState, PartialEdgeInfo, PeerMessage, RoutedMessageV2,
    RoutingTableUpdate,
};
use crate::peer_manager::connection;
use crate::peer_manager::network_state::PeerIdOrHash;
use crate::peer_manager::peer_manager_actor::Event;
use crate::routing::routing_table_view::FindRouteError;
use crate::routing::NetworkTopologyChange;
use crate::stats::metrics;
use crate::tcp;
use crate::types::ReasonForBan;
use near_async::time;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use std::collections::HashSet;
use std::sync::Arc;

impl NetworkState {
    // TODO(gprusak): eventually, this should be blocking, as it should be up to the caller
    // whether to wait for the broadcast to finish, or run it in parallel with sth else.
    fn broadcast_routing_table_update(&self, mut rtu: RoutingTableUpdate) {
        if rtu == RoutingTableUpdate::default() {
            return;
        }
        rtu.edges = Edge::deduplicate(rtu.edges);
        let msg = Arc::new(PeerMessage::SyncRoutingTable(rtu));
        for conn in self.tier2.load().ready.values() {
            conn.send_message(msg.clone());
        }
    }

    // TODO(saketh-are): eventually, this should be blocking, as it should be up to the caller
    // whether to wait for the broadcast to finish, or run it in parallel with sth else.
    fn broadcast_distance_vector(&self, distance_vector: DistanceVector) {
        let msg = Arc::new(PeerMessage::DistanceVector(distance_vector));
        for conn in self.tier2.load().ready.values() {
            conn.send_message(msg.clone());
        }
    }

    /// Adds AnnounceAccounts (without validating them) to the routing table.
    /// Then it broadcasts all the AnnounceAccounts that haven't been seen before.
    pub async fn add_accounts(self: &Arc<NetworkState>, accounts: Vec<AnnounceAccount>) {
        let this = self.clone();
        self.spawn(async move {
            let new_accounts = this.account_announcements.add_accounts(accounts);
            tracing::debug!(target: "network", account_id = ?this.config.validator.as_ref().map(|v|v.account_id()), ?new_accounts, "Received new accounts");
            this.broadcast_routing_table_update(RoutingTableUpdate::from_accounts(
                new_accounts.clone(),
            ));
            this.config.event_sink.push(Event::AccountsAdded(new_accounts));
        }).await.unwrap()
    }

    /// Constructs a partial edge to the given peer with the nonce specified.
    /// If nonce is None, nonce is selected automatically.
    pub fn propose_edge(
        &self,
        clock: &time::Clock,
        peer1: &PeerId,
        with_nonce: Option<u64>,
    ) -> PartialEdgeInfo {
        // When we create a new edge we increase the latest nonce by 2 in case we miss a removal
        // proposal from our partner.
        let nonce = with_nonce.unwrap_or_else(|| {
            let nonce = Edge::create_fresh_nonce(clock);
            // If we already had a connection to this peer - check that edge's nonce.
            // And use either that one or the one from the current timestamp.
            // We would use existing edge's nonce, if we were trying to connect to a given peer multiple times per second.
            self.graph
                .load()
                .local_edges
                .get(peer1)
                .map_or(nonce, |edge| std::cmp::max(edge.next(), nonce))
        });
        PartialEdgeInfo::new(&self.config.node_id(), peer1, nonce, &self.config.node_key)
    }

    /// Constructs an edge from the partial edge constructed by the peer,
    /// adds it to the graph and then broadcasts it.
    pub async fn finalize_edge(
        self: &Arc<Self>,
        clock: &time::Clock,
        peer_id: PeerId,
        edge_info: PartialEdgeInfo,
    ) -> Result<Edge, ReasonForBan> {
        let edge = Edge::build_with_secret_key(
            self.config.node_id(),
            peer_id.clone(),
            edge_info.nonce,
            &self.config.node_key,
            edge_info.signature,
        );
        self.add_edges(&clock, vec![edge.clone()]).await?;
        self.update_routes(
            &clock,
            NetworkTopologyChange::PeerConnected(peer_id.clone(), edge.clone()),
        )
        .await?;

        Ok(edge)
    }

    /// Validates edges, then adds them to the graph and then broadcasts all the edges that
    /// hasn't been observed before. Returns an error iff any edge was invalid. Even if an
    /// error was returned some of the valid input edges might have been added to the graph.
    pub async fn add_edges(
        self: &Arc<Self>,
        clock: &time::Clock,
        edges: Vec<Edge>,
    ) -> Result<(), ReasonForBan> {
        if edges.is_empty() {
            return Ok(());
        }
        let this = self.clone();
        let clock = clock.clone();
        self.add_edges_demux
            .call(edges, |edges: Vec<Vec<Edge>>| async move {
                let (mut edges, oks) = this.graph.update(&clock, edges).await;
                // Don't send tombstones during the initial time.
                // Most of the network is created during this time, which results
                // in us sending a lot of tombstones to peers.
                // Later, the amount of new edges is a lot smaller.
                if let Some(skip_tombstones_duration) = this.config.skip_tombstones {
                    if clock.now() < this.created_at + skip_tombstones_duration {
                        edges.retain(|edge| edge.edge_type() == EdgeState::Active);
                        metrics::EDGE_TOMBSTONE_SENDING_SKIPPED.inc();
                    }
                }
                // Broadcast new edges to all other peers.
                this.config.event_sink.push(Event::EdgesAdded(edges.clone()));
                this.broadcast_routing_table_update(RoutingTableUpdate::from_edges(edges));
                // Retu
                oks.iter()
                    .map(|ok| match ok {
                        true => Ok(()),
                        false => Err(ReasonForBan::InvalidEdge),
                    })
                    .collect()
            })
            .await
            .unwrap_or(Ok(()))
    }

    pub(crate) fn tier2_find_route(
        &self,
        clock: &time::Clock,
        target: &PeerIdOrHash,
    ) -> Result<PeerId, FindRouteError> {
        match target {
            PeerIdOrHash::PeerId(peer_id) => {
                match self.graph.routing_table.find_next_hop_for_target(peer_id) {
                    Ok(peer_id) => Ok(peer_id),
                    Err(_) => self.graph_v2.routing_table.find_next_hop_for_target(peer_id),
                }
            }
            PeerIdOrHash::Hash(hash) => self
                .tier2_route_back
                .lock()
                .remove(clock, hash)
                .ok_or(FindRouteError::RouteBackNotFound),
        }
    }

    /// Accepts a routed message. If we expect a response for the message, writes an entry in
    /// the appropriate RouteBackCache recording the peer node from which the message came.
    /// The cache entry will later be used to route back the response to the message.
    pub(crate) fn add_route_back(
        &self,
        clock: &time::Clock,
        conn: &connection::Connection,
        msg: &RoutedMessageV2,
    ) {
        if !msg.expect_response() {
            return;
        }

        tracing::trace!(target: "network", route_back = ?msg.clone(), "Received peer message that requires response");
        let from = &conn.peer_info.id;
        match conn.tier {
            tcp::Tier::T1 => self.tier1_route_back.lock().insert(&clock, msg.hash(), from.clone()),
            tcp::Tier::T2 => self.tier2_route_back.lock().insert(&clock, msg.hash(), from.clone()),
        }
    }

    pub(crate) fn compare_route_back(&self, hash: CryptoHash, peer_id: &PeerId) -> bool {
        self.tier2_route_back.lock().get(&hash).map_or(false, |value| value == peer_id)
    }

    /// Accepts NetworkTopologyChange events.
    /// Changes are batched via the `update_routes_demux`, then passed to the V2 routing table.
    /// If an updated DistanceVector is returned by the routing table, broadcasts it to peers.
    /// If an error occurs while processing a DistanceVector advertised by a peer, bans the peer.
    pub async fn update_routes(
        self: &Arc<Self>,
        clock: &time::Clock,
        event: NetworkTopologyChange,
    ) -> Result<(), ReasonForBan> {
        let this = self.clone();
        let clock = clock.clone();
        self.update_routes_demux
            .call(event, |events: Vec<NetworkTopologyChange>| async move {
                let (to_broadcast, oks) =
                    this.graph_v2.batch_process_network_changes(&clock, events).await;

                if let Some(my_distance_vector) = to_broadcast {
                    this.broadcast_distance_vector(my_distance_vector);
                }

                oks.iter()
                    .map(|ok| match ok {
                        true => Ok(()),
                        false => Err(ReasonForBan::InvalidDistanceVector),
                    })
                    .collect()
            })
            .await
            .unwrap_or(Ok(()))
    }

    /// Update the routing protocols with a set of peers to avoid routing through.
    pub fn set_unreliable_peers(&self, unreliable_peers: HashSet<PeerId>) {
        self.graph.set_unreliable_peers(unreliable_peers.clone());
        self.graph_v2.set_unreliable_peers(unreliable_peers);
    }
}

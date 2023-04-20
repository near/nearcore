use super::NetworkState;
use crate::network_protocol::{Edge, EdgeState, PartialEdgeInfo, PeerMessage, RoutingTableUpdate};
use crate::peer_manager::peer_manager_actor::Event;
use crate::stats::metrics;
use crate::types::ReasonForBan;
use near_async::time;
use near_primitives::network::{AnnounceAccount, PeerId};
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

    /// Adds AnnounceAccounts (without validating them) to the routing table.
    /// Then it broadcasts all the AnnounceAccounts that haven't been seen before.
    pub async fn add_accounts(self: &Arc<NetworkState>, accounts: Vec<AnnounceAccount>) {
        let this = self.clone();
        self.spawn(async move {
            let new_accounts = this.graph.routing_table.add_accounts(accounts);
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
}

use crate::network_protocol::Edge;
use crate::private_actix::StopMsg;
use crate::routing;
use crate::stats::metrics;
use crate::store;
use crate::time;
use actix::{Actor as _, ActorContext as _, Context, Running};
use near_o11y::{handler_debug_span, handler_trace_span, OpenTelemetrySpanExt, WithSpanContext};
use near_performance_metrics_macros::perf;
use near_primitives::network::PeerId;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::warn;

impl NetworkState {
    pub async fn add_accounts(self: &Arc<NetworkState>, accounts: Vec<AnnounceAccount>) {
        let this = self.clone();
        self.spawn(async move {
            let new_accounts = this.routing_table_view.add_accounts(accounts);
            tracing::debug!(target: "network", account_id = ?this.config.validator.as_ref().map(|v|v.account_id()), ?new_accounts, "Received new accounts");
            this.broadcast_routing_table_update(Arc::new(RoutingTableUpdate::from_accounts(
                new_accounts.clone(),
            )));
            this.config.event_sink.push(Event::AccountsAdded(new_accounts));
        }).await.unwrap()
    }

    pub fn propose_edge(&self, peer1: &PeerId, with_nonce: Option<u64>) -> PartialEdgeInfo {
        // When we create a new edge we increase the latest nonce by 2 in case we miss a removal
        // proposal from our partner.
        let nonce = with_nonce.unwrap_or_else(|| {
            self.routing_table_view.get_local_edge(peer1).map_or(1, |edge| edge.next())
        });
        PartialEdgeInfo::new(&self.config.node_id(), peer1, nonce, &self.config.node_key)
    }

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

    pub async fn add_edges(
        self: &Arc<Self>,
        clock: &time::Clock,
        edges: Vec<Edge>,
    ) -> Result<(), ReasonForBan> {
        // TODO(gprusak): sending duplicate edges should be considered a malicious behavior
        // instead, however that would be backward incompatible, so it can be introduced in
        // PROTOCOL_VERSION 60 earliest.
        let graph = this.graph.load();
        let mut edges = Edge::deduplicate(edges);
        edges.retain(|x| !graph.has(x));
        // Verify the edges in parallel on rayon.
        let (edges,ok) = concurrency::rayon::run(move || {
            concurrency::rayon::try_map(es,|e| if e.verify() { Some(e) } else { None })
        });
        let this = self.clone();
        let clock = clock.clone();
        self.add_edges_demux.call(edges, |edges| async move {
            let edges = edges.flatten();
            this.routing_table_view.add_local_edges(&edges);
            let (new_edges,next_hops) = graph.update_routing_table(clock,edges).await;
            // Don't send tombstones during the initial time.
            // Most of the network is created during this time, which results
            // in us sending a lot of tombstones to peers.
            // Later, the amount of new edges is a lot smaller.
            if let Some(skip_tombstones_duration) = this.config.skip_tombstones {
                if clock.now() < this.created_at + skip_tombstones_duration {
                    new_edges.retain(|edge| edge.edge_type() == EdgeState::Active);
                    metrics::EDGE_TOMBSTONE_SENDING_SKIPPED.inc();
                }
            }
            // Broadcast new edges to all other peers.
            this.broadcast_routing_table_update(Arc::new(RoutingTableUpdate::from_edges(
                new_edges,
            )));
        }).await;
        match ok {
            true => Ok(()),
            false => Err(ReasonForBan::InvalidEdge),
        }
    }
}

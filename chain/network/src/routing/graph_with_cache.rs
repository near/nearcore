use crate::routing;
use crate::stats::metrics;
use near_network_primitives::types::{Edge, EdgeState};
use near_primitives::network::PeerId;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::trace;

// TODO: make it opaque, so that the key.0 < key.1 invariant is protected.
type EdgeKey = (PeerId, PeerId);
pub type RoutingTable = HashMap<PeerId, Vec<PeerId>>;

pub struct GraphWithCache {
    /// Current view of the network represented by an undirected graph.
    /// Contains only validated edges.
    /// Nodes are Peers and edges are active connections.
    graph: routing::Graph,
    /// Edges of the raw_graph, indexed by Edge::key().
    /// Contains also the edge tombstones.
    edges: HashMap<EdgeKey, Edge>,
    /// Peers of this node, which are on any shortest path to the given node.
    /// Derived from graph.
    cached_routing_table: Mutex<Option<Arc<RoutingTable>>>,
}

impl GraphWithCache {
    pub fn new(my_peer_id: PeerId) -> Self {
        Self {
            graph: routing::Graph::new(my_peer_id),
            edges: Default::default(),
            cached_routing_table: Default::default(),
        }
    }

    pub fn my_peer_id(&self) -> PeerId {
        self.graph.my_peer_id().clone()
    }
    pub fn total_active_edges(&self) -> u64 {
        self.graph.total_active_edges()
    }
    pub fn edges(&self) -> &HashMap<EdgeKey, Edge> {
        &self.edges
    }

    pub fn has(&self, edge: &Edge) -> bool {
        let prev = self.edges.get(&edge.key());
        prev.map_or(false, |x| x.nonce() >= edge.nonce())
    }

    /// Adds an edge without validating the signatures. O(1).
    /// Returns true, iff <edge> was newer than an already known version of this edge.
    pub fn update_edge(&mut self, edge: Edge) -> bool {
        if self.has(&edge) {
            return false;
        }
        let key = edge.key();
        // Add the edge.
        match edge.edge_type() {
            EdgeState::Active => self.graph.add_edge(&key.0, &key.1),
            EdgeState::Removed => self.graph.remove_edge(&key.0, &key.1),
        }
        self.edges.insert(key.clone(), edge);
        // Invalidate cache.
        *self.cached_routing_table.lock() = None;
        true
    }

    pub fn update_edges(&mut self, mut edges: Vec<Edge>) -> Vec<Edge> {
        edges.retain(|e| self.update_edge(e.clone()));
        edges
    }

    /// Removes an edge by key. O(1).
    pub fn remove_edge(&mut self, key: &EdgeKey) {
        if self.edges.remove(key).is_some() {
            self.graph.remove_edge(&key.0, &key.1);
            *self.cached_routing_table.lock() = None;
        }
    }

    /// Computes the routing table, based on the graph. O(|Graph|).
    pub fn routing_table(&self) -> Arc<RoutingTable> {
        if let Some(rt) = self.cached_routing_table.lock().clone() {
            return rt;
        }
        let _d = delay_detector::DelayDetector::new(|| "routing table update".into());
        let _routing_table_recalculation =
            metrics::ROUTING_TABLE_RECALCULATION_HISTOGRAM.start_timer();
        trace!(target: "network", "Update routing table.");
        let rt = Arc::new(self.graph.calculate_distance());
        metrics::ROUTING_TABLE_RECALCULATIONS.inc();
        metrics::PEER_REACHABLE.set(rt.len() as i64);
        *self.cached_routing_table.lock() = Some(rt.clone());
        rt
    }

    pub fn remove_adjacent_edges(&mut self, peers: &HashSet<PeerId>) -> Vec<Edge> {
        let edges: Vec<_> = self
            .edges()
            .values()
            .filter(|edge| {
                let key = edge.key();
                peers.contains(&key.0) || peers.contains(&key.1)
            })
            .cloned()
            .collect();
        for edge in &edges {
            self.remove_edge(edge.key());
        }
        edges
    }
}

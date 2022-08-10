use crate::routing;
use crate::stats::metrics;
use near_network_primitives::time;
use near_network_primitives::types::{Edge, EdgeState};
use near_primitives::network::PeerId;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::trace;

// TODO: make it opaque, so that the key.0 < key.1 invariant is protected.
type EdgeKey = (PeerId, PeerId);
pub type NextHopTable = HashMap<PeerId, Vec<PeerId>>;

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
    cached_next_hops: Mutex<Option<Arc<NextHopTable>>>,
    // Don't allow edges that are before this time (if set)
    prune_edges_before: Option<time::Utc>
}

impl GraphWithCache {
    pub fn new(my_peer_id: PeerId) -> Self {
        Self {
            graph: routing::Graph::new(my_peer_id),
            edges: Default::default(),
            cached_next_hops: Default::default(),
            prune_edges_before: None,
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

    pub fn set_unreliable_peers(&mut self, unreliable_peers: HashSet<PeerId>) {
        self.graph.set_unreliable_peers(unreliable_peers);
        // Invalidate cache.
        *self.cached_next_hops.lock() = None;
    }

    /// Adds an edge without validating the signatures. O(1).
    /// Returns true, iff <edge> was newer than an already known version of this edge.
    pub fn update_edge(&mut self, edge: Edge) -> bool {
        if self.has(&edge) {
            return false;
        }
        if let Some(edge_limit) = self.prune_edges_before {
            // Don't add edges that are older than the limit.
            if edge.is_edge_older_than(edge_limit) {
                return false;
            }
        }

        let key = edge.key();
        // Add the edge.
        match edge.edge_type() {
            EdgeState::Active => self.graph.add_edge(&key.0, &key.1),
            EdgeState::Removed => self.graph.remove_edge(&key.0, &key.1),
        }
        self.edges.insert(key.clone(), edge);
        // Invalidate cache.
        *self.cached_next_hops.lock() = None;
        true
    }

    pub fn update_edges(&mut self, mut edges: Vec<Edge>) -> Vec<Edge> {
        edges.retain(|e| self.update_edge(e.clone()));
        edges
    }

    /// Removes an edge by key. O(1).
    pub fn remove_edge(&mut self, key: &EdgeKey) {
        self.remove_edges(&vec![key])
    }

    // Removes mutiple edges.
    // The main benefit is that we take the cached_next_hops lock only once.
    fn remove_edges(&mut self, edge_keys: &Vec<&EdgeKey>) {
        let mut removed = false;
        for key in edge_keys {
            if self.edges.remove(key).is_some() {
                self.graph.remove_edge(&key.0, &key.1);
                removed = true;
            }
        }
        if removed {
            *self.cached_next_hops.lock() = None;
        }
    }

    /// Computes the next hops table, based on the graph. O(|Graph|).
    pub fn next_hops(&self) -> Arc<NextHopTable> {
        if let Some(rt) = self.cached_next_hops.lock().clone() {
            return rt;
        }
        let _d = delay_detector::DelayDetector::new(|| "routing table update".into());
        let _next_hops_recalculation = metrics::ROUTING_TABLE_RECALCULATION_HISTOGRAM.start_timer();
        trace!(target: "network", "Update routing table.");
        let rt = Arc::new(self.graph.calculate_distance());
        metrics::ROUTING_TABLE_RECALCULATIONS.inc();
        metrics::PEER_REACHABLE.set(rt.len() as i64);
        *self.cached_next_hops.lock() = Some(rt.clone());
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
        self.remove_edges(&edges.iter().map(|edge| edge.key()).collect::<Vec<_>>());
        edges
    }

    pub fn prune_old_edges(&mut self, prune_edges_older_than: time::Utc) {
        self.prune_edges_before = Some(prune_edges_older_than);
        let old_edges = self
            .edges()
            .iter()
            .filter_map(|(edge_key, edge)| {
                if edge.is_edge_older_than(prune_edges_older_than) {
                    Some(edge_key)
                } else {
                    None
                }
            })
            .cloned()
            .collect::<Vec<_>>();
        self.remove_edges(&old_edges.iter().map(|key| key).collect::<Vec<_>>());
    }
}

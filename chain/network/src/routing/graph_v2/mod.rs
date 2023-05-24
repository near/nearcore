use crate::concurrency;
use crate::concurrency::runtime::Runtime;
use crate::network_protocol::{Edge, EdgeState, ShortestPathTree};
use crate::routing::bfs;
use crate::routing::routing_table_view::RoutingTableView;
use crate::stats::metrics;
use arc_swap::ArcSwap;
use near_async::time;
use near_primitives::network::PeerId;
use parking_lot::Mutex;
use rayon::iter::ParallelBridge;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// TODO: make it opaque, so that the key.0 < key.1 invariant is protected.
type EdgeKey = (PeerId, PeerId);
pub type NextHopTable = HashMap<PeerId, Vec<PeerId>>;

#[derive(Clone)]
pub struct GraphConfigV2 {
    pub node_id: PeerId,
    pub prune_unreachable_peers_after: time::Duration,
    pub prune_edges_after: Option<time::Duration>,
}

struct Inner {
    config: GraphConfigV2,

    /// Current view of the network represented by an undirected graph.
    /// Contains only validated edges.
    /// Nodes are Peers and edges are active connections.
    graph: bfs::Graph,

    edges: im::HashMap<EdgeKey, Edge>,
    /// Last time a peer was reachable.
    peer_reachable_at: HashMap<PeerId, time::Instant>,
}

fn has(set: &im::HashMap<EdgeKey, Edge>, edge: &Edge) -> bool {
    set.get(&edge.key()).map_or(false, |x| x.nonce() >= edge.nonce())
}

impl Inner {
    /// Adds an edge without validating the signatures. O(1).
    /// Returns true, iff <edge> was newer than an already known version of this edge.
    fn update_edge(&mut self, now: time::Utc, edge: Edge) -> bool {
        if has(&self.edges, &edge) {
            return false;
        }
        if let Some(prune_edges_after) = self.config.prune_edges_after {
            // Don't add edges that are older than the limit.
            if edge.is_edge_older_than(now - prune_edges_after) {
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
        true
    }

    /// Removes an edge by key. O(1).
    fn remove_edge(&mut self, key: &EdgeKey) {
        if self.edges.remove(key).is_some() {
            self.graph.remove_edge(&key.0, &key.1);
        }
    }

    /// Removes all edges adjacent to the peers from the set.
    /// It is used to prune unreachable connected components from the inmem graph.
    fn remove_adjacent_edges(&mut self, peers: &HashSet<PeerId>) -> Vec<Edge> {
        let mut edges = vec![];
        for e in self.edges.clone().values() {
            if peers.contains(&e.key().0) || peers.contains(&e.key().1) {
                self.remove_edge(e.key());
                edges.push(e.clone());
            }
        }
        edges
    }

    fn prune_old_edges(&mut self, prune_edges_older_than: time::Utc) {
        for e in self.edges.clone().values() {
            if e.is_edge_older_than(prune_edges_older_than) {
                self.remove_edge(e.key());
            }
        }
    }

    /// Prunes peers unreachable since <unreachable_since> (and their adjacent edges)
    /// from the in-mem graph
    fn prune_unreachable_peers(&mut self, unreachable_since: time::Instant) {
        // Select peers to prune.
        let mut peers = HashSet::new();
        for k in self.edges.keys() {
            for peer_id in [&k.0, &k.1] {
                if self
                    .peer_reachable_at
                    .get(peer_id)
                    .map(|t| t < &unreachable_since)
                    .unwrap_or(true)
                {
                    peers.insert(peer_id.clone());
                }
            }
        }
        if peers.is_empty() {
            return;
        }

        // Prune peers from peer_reachable_at.
        for peer_id in &peers {
            self.peer_reachable_at.remove(&peer_id);
        }

        // Prune edges from graph.
        self.remove_adjacent_edges(&peers);
    }

    /// Verifies edges, then adds them to the graph.
    /// Returns a list of newly added edges (not known so far), which should be broadcasted.
    /// Returns true iff all the edges provided were valid.
    ///
    /// This method implements a security measure against an adversary sending invalid edges:
    /// * it deduplicates edges and drops known edges before verification, because verification is expensive.
    /// * it verifies edges in parallel until the first invalid edge is found. It adds the edges
    ///   verified so far (and returns them), but drops all the remaining ones. This way the
    ///   wasted work (verification of invalid edges) is constant, no matter how large the input
    ///   size is.
    fn add_edges(&mut self, clock: &time::Clock, mut edges: Vec<Edge>) -> (Vec<Edge>, bool) {
        metrics::EDGE_UPDATES.inc_by(edges.len() as u64);
        // Start with deduplicating the edges.
        // TODO(gprusak): sending duplicate edges should be considered a malicious behavior
        // instead, however that would be backward incompatible, so it can be introduced in
        // PROTOCOL_VERSION 60 earliest.
        edges = Edge::deduplicate(edges);

        // Retain only new edges.
        edges.retain(|e| !has(&self.edges, e));

        // Verify the edges in parallel on rayon.
        // Stop at first invalid edge.
        let (mut edges, ok) = concurrency::rayon::run_blocking(move || {
            concurrency::rayon::try_map(edges.into_iter().par_bridge(), |e| {
                if e.verify() {
                    Some(e)
                } else {
                    None
                }
            })
        });

        // Add the verified edges to the graph.
        let now = clock.now_utc();
        edges.retain(|e| self.update_edge(now, e.clone()));
        (edges, ok)
    }

    /// 1. Prunes expired edges.
    /// 2. Prunes unreachable graph components.
    /// 3. Recomputes the NextHopTable.
    pub fn update(
        &mut self,
        clock: &time::Clock,
        unreliable_peers: &HashSet<PeerId>,
    ) -> NextHopTable {
        let _update_time = metrics::ROUTING_TABLE_RECALCULATION_HISTOGRAM.start_timer();
        // Update metrics after edge update
        if let Some(prune_edges_after) = self.config.prune_edges_after {
            self.prune_old_edges(clock.now_utc() - prune_edges_after);
        }
        let next_hops = self.graph.calculate_distance(unreliable_peers);

        // Update peer_reachable_at.
        let now = clock.now();
        self.peer_reachable_at.insert(self.config.node_id.clone(), now);
        for peer in next_hops.keys() {
            self.peer_reachable_at.insert(peer.clone(), now);
        }
        if let Some(unreachable_since) = now.checked_sub(self.config.prune_unreachable_peers_after)
        {
            self.prune_unreachable_peers(unreachable_since);
        }
        let mut local_edges = HashMap::new();
        for e in self.edges.clone().values() {
            if let Some(other) = e.other(&self.config.node_id) {
                local_edges.insert(other.clone(), e.clone());
            }
        }
        metrics::ROUTING_TABLE_RECALCULATIONS.inc();
        metrics::PEER_REACHABLE.set(next_hops.len() as i64);
        metrics::EDGE_ACTIVE.set(self.graph.total_active_edges() as i64);
        metrics::EDGE_TOTAL.set(self.edges.len() as i64);
        return next_hops;
    }
}

pub(crate) struct GraphV2 {
    inner: Arc<Mutex<Inner>>,
    unreliable_peers: ArcSwap<HashSet<PeerId>>,
    pub routing_table: RoutingTableView,

    runtime: Runtime,
}

impl GraphV2 {
    pub fn new(config: GraphConfigV2) -> Self {
        Self {
            routing_table: RoutingTableView::new(),
            inner: Arc::new(Mutex::new(Inner {
                graph: bfs::Graph::new(config.node_id.clone()),
                config,
                edges: Default::default(),
                peer_reachable_at: HashMap::new(),
            })),
            unreliable_peers: ArcSwap::default(),
            runtime: Runtime::new(),
        }
    }

    pub fn set_unreliable_peers(&self, unreliable_peers: HashSet<PeerId>) {
        // TODO: unreliable peers are used in computation of next hops;
        // consider whether setting them should trigger immediate re-computation
        self.unreliable_peers.store(Arc::new(unreliable_peers));
    }

    /// Verifies, then adds edges to the graph, then recomputes the routing table.
    /// Each entry of `edges` are edges coming from a different source.
    /// Returns (new_edges,oks) where
    /// * new_edges contains new valid edges that should be broadcasted.
    /// * oks.len() == edges.len() and oks[i] is true iff all edges in edges[i] were valid.
    ///
    /// The validation of each `edges[i]` separately, stops at the first invalid edge,
    /// and all remaining edges of `edges[i]` are discarded.
    ///
    /// Edge verification is expensive, and it would be an attack vector if we dropped on the
    /// floor valid edges verified so far: an attacker could prepare a SyncRoutingTable
    /// containing a lot of valid edges, except for the last one, and send it repeatedly to a
    /// node. The node would then validate all the edges every time, then reject the whole set
    /// because just the last edge was invalid. Instead, we accept all the edges verified so
    /// far and return an error only afterwards.
    pub async fn update(
        self: &Arc<Self>,
        clock: &time::Clock,
        spts: Vec<ShortestPathTree>,
    ) -> (Option<ShortestPathTree>, Vec<bool>) {
        // Computation is CPU heavy and accesses DB so we execute it on a dedicated thread.
        // TODO(gprusak): It would be better to move CPU heavy stuff to rayon and make DB calls async,
        // but that will require further refactor. Or even better: get rid of the Graph all
        // together.
        // TODO(saketh): Consider whether we can move this to rayon now that DB calls are
        // eliminated.
        let this = self.clone();
        let clock = clock.clone();
        self.runtime
            .handle
            .spawn_blocking(move || {
                let mut inner = this.inner.lock();
                let mut new_edges = vec![];
                let mut oks = vec![];

                // TODO: a simple optimization could be to skip all but the latest SPT for each
                // peer; it saves time, but allows the possibility that we skip an invalid SPT
                // fail to ban the node (which seems to be an acceptable tradeoff)
                for spt in spts {
                    let (es, ok) = inner.add_edges(&clock, spt.edges);
                    oks.push(ok);
                    new_edges.extend(es);
                }
                let next_hops = inner.update(&clock, &this.unreliable_peers.load());
                this.routing_table.update(next_hops.into());
                (
                    Some(ShortestPathTree { root: inner.config.node_id.clone(), edges: new_edges }),
                    oks,
                )
            })
            .await
            .unwrap()
    }
}

use crate::network_protocol::{Edge, EdgeState};
use crate::routing::bfs;
use crate::routing::routing_table_view::RoutingTableView;
use crate::stats::metrics;
use ::time::ext::InstantExt as _;
use arc_swap::ArcSwap;
use near_async::messaging::{Actor, CanSendAsync, Handler, Message};
use near_async::multithread::MultithreadRuntimeHandle;
use near_async::time::Clock;
use near_async::{new_owned_multithread_actor, time};
use near_primitives::network::PeerId;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};

#[cfg(test)]
mod tests;

// TODO: make it opaque, so that the key.0 < key.1 invariant is protected.
type EdgeKey = (PeerId, PeerId);
pub type NextHopTable = HashMap<PeerId, Vec<PeerId>>;
pub type DistanceTable = HashMap<PeerId, u32>;

#[derive(Clone)]
pub struct GraphConfig {
    pub node_id: PeerId,
    pub prune_unreachable_peers_after: time::Duration,
    pub prune_edges_after: Option<time::Duration>,
}

#[derive(Default)]
pub struct GraphSnapshot {
    pub edges: im::HashMap<EdgeKey, Edge>,
    pub local_edges: HashMap<PeerId, Edge>,
    pub next_hops: Arc<NextHopTable>,
    pub distances: Arc<DistanceTable>,
}

struct Inner {
    config: GraphConfig,

    /// Current view of the network represented by an undirected graph.
    /// Contains only validated edges.
    /// Nodes are Peers and edges are active connections.
    graph: bfs::Graph,

    edges: im::HashMap<EdgeKey, Edge>,
    /// Last time a peer was reachable.
    peer_reachable_at: HashMap<PeerId, time::Instant>,
}

fn has(set: &im::HashMap<EdgeKey, Edge>, edge: &Edge) -> bool {
    set.get(&edge.key()).is_some_and(|x| x.nonce() >= edge.nonce())
}

impl Inner {
    /// Adds an edge without validating the signatures. O(1).
    /// Returns true, iff <edge> was newer than an already known version of this edge.
    fn update_edge(&mut self, edge: Edge) -> bool {
        if has(&self.edges, &edge) {
            return false;
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
    /// It is used to prune unreachable connected components from the in mem graph.
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
    /// from the in-mem graph.
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
        let now = clock.now_utc();
        edges.retain(|e| {
            if has(&self.edges, e) {
                return false;
            }

            if let Some(prune_edges_after) = self.config.prune_edges_after {
                // Don't add edges that are older than the limit.
                if e.is_edge_older_than(now - prune_edges_after) {
                    return false;
                }
            }

            return true;
        });

        // Stop at first invalid edge.
        let mut valid_edges = Vec::<Edge>::new();
        let mut ok = true;
        for edge in edges {
            if !edge.verify() {
                ok = false;
                break;
            }
            if edge.key().0 == edge.key().1 {
                // Skip self-loops. We don't reject them outright, because in T1 self-discovery we do
                // create self-loop edges; we just don't want to add them to the graph.
                continue;
            }
            valid_edges.push(edge);
        }

        // Add the verified edges to the graph.
        valid_edges.retain(|e| self.update_edge(e.clone()));
        (valid_edges, ok)
    }

    /// 1. Prunes expired edges.
    /// 2. Prunes unreachable graph components.
    /// 3. Recomputes GraphSnapshot.
    #[tracing::instrument(
        target = "network::routing::graph",
        level = "debug",
        "GraphInner::update",
        skip_all
    )]
    pub fn update(
        &mut self,
        clock: &time::Clock,
        unreliable_peers: &HashSet<PeerId>,
    ) -> GraphSnapshot {
        let _update_time = metrics::ROUTING_TABLE_RECALCULATION_HISTOGRAM.start_timer();
        // Update metrics after edge update
        if let Some(prune_edges_after) = self.config.prune_edges_after {
            self.prune_old_edges(clock.now_utc() - prune_edges_after);
        }

        let (next_hops, distances) = self.graph.calculate_next_hops_and_distance(unreliable_peers);
        let next_hops = Arc::new(next_hops);
        let distances = Arc::new(distances);

        // Update peer_reachable_at.
        let now = clock.now();
        self.peer_reachable_at.insert(self.config.node_id.clone(), now);
        for peer in next_hops.keys() {
            self.peer_reachable_at.insert(peer.clone(), now);
        }
        if let Some(unreachable_since) =
            now.checked_sub_signed(self.config.prune_unreachable_peers_after)
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
        GraphSnapshot { edges: self.edges.clone(), local_edges, next_hops, distances }
    }
}

pub(crate) struct Graph {
    snapshot: ArcSwap<GraphSnapshot>,
    unreliable_peers: ArcSwap<HashSet<PeerId>>,
    pub routing_table: RoutingTableView,
    updater: MultithreadRuntimeHandle<GraphActor>,
}

impl Graph {
    pub fn new(clock: Clock, config: GraphConfig) -> Arc<Self> {
        Arc::new_cyclic(|weak| {
            let weak = weak.clone();
            let updater = new_owned_multithread_actor(1, move || GraphActor {
                clock: clock.clone(),
                graph: weak.clone(),
                inner: Inner {
                    graph: bfs::Graph::new(config.node_id.clone()),
                    config: config.clone(),
                    edges: Default::default(),
                    peer_reachable_at: HashMap::new(),
                },
            });
            Self {
                routing_table: RoutingTableView::new(),
                unreliable_peers: ArcSwap::default(),
                snapshot: ArcSwap::default(),
                updater,
            }
        })
    }

    pub fn load(&self) -> Arc<GraphSnapshot> {
        self.snapshot.load_full()
    }

    pub fn set_unreliable_peers(&self, unreliable_peers: HashSet<PeerId>) {
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
    pub async fn update(&self, edges: Vec<Vec<Edge>>) -> (Vec<Edge>, Vec<bool>) {
        self.updater.send_async(UpdateEdges(edges)).await.unwrap()
    }
}

struct GraphActor {
    clock: Clock,
    graph: Weak<Graph>,
    inner: Inner,
}

impl Actor for GraphActor {}

#[derive(Debug)]
struct UpdateEdges(Vec<Vec<Edge>>);

impl Message for UpdateEdges {}

impl Handler<UpdateEdges, (Vec<Edge>, Vec<bool>)> for GraphActor {
    fn handle(&mut self, msg: UpdateEdges) -> (Vec<Edge>, Vec<bool>) {
        let mut new_edges = vec![];
        let mut oks = vec![];
        for es in msg.0 {
            let (es, ok) = self.inner.add_edges(&self.clock, es);
            oks.push(ok);
            new_edges.extend(es);
        }
        if let Some(graph) = self.graph.upgrade() {
            let snapshot = self.inner.update(&self.clock, &graph.unreliable_peers.load());
            let snapshot = Arc::new(snapshot);
            graph.routing_table.update(snapshot.next_hops.clone(), snapshot.distances.clone());
            graph.snapshot.store(snapshot);
        }
        (new_edges, oks)
    }
}

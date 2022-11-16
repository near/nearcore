use crate::network_protocol::{Edge, EdgeState};
use crate::routing;
use crate::stats::metrics;
use crate::time;
use near_primitives::network::PeerId;
use once_cell::sync::OnceCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::trace;

// TODO: make it opaque, so that the key.0 < key.1 invariant is protected.
type EdgeKey = (PeerId, PeerId);
pub type NextHopTable = HashMap<PeerId, Vec<PeerId>>;

pub struct Config {
    unreachable_for: time::Duration,    
}

struct Inner {
    /// Current view of the network represented by an undirected graph.
    /// Contains only validated edges.
    /// Nodes are Peers and edges are active connections.
    graph: tokio::sync::Mutex<routing::Graph>,
    
    edges: im::HashMap<EdgeKey, Edge>,
    /// Don't allow edges that are before this time (if set)
    prune_edges_before: Option<time::Utc>,
    /// Last time a peer was reachable.
    peer_reachable_at: HashMap<PeerId, time::Instant>,
    
    store: store::Store,
}

impl Inner {
    fn has(&self, edge: &Edge) -> bool {
        let prev = self.edges.get(&edge.key());
        prev.map_or(false, |x| x.nonce() >= edge.nonce())
    }

    /// Adds an edge without validating the signatures. O(1).
    /// Returns true, iff <edge> was newer than an already known version of this edge.
    fn update_edge(&mut self, edge: Edge) -> bool {
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
        true
    }

    /// Removes an edge by key. O(1).
    fn remove_edge(&mut self, key: &EdgeKey) {
        if self.edges.remove(key).is_some() {
            self.graph.remove_edge(&key.0, &key.1);
        }
    }

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
        self.prune_edges_before = Some(prune_edges_older_than);
        for e in self.edges.clone() {
            if edge.is_edge_older_than(prune_edges_older_than) {
                self.remove_edge(e.key());
            }
        }
    }

    /// If peer_id is not in memory check if it is on disk in bring it back on memory.
    ///
    /// Note: here an advanced example, which shows what's happening.
    /// Let's say we have a full graph fully connected with nodes `A, B, C, D`.
    /// Step 1 ) `A`, `B` get removed.
    /// We store edges belonging to `A` and `B`: `<A,B>, <A,C>, <A, D>, <B, C>, <B, D>`
    /// into component 1 let's call it `C_1`.
    /// And mapping from `A` to `C_1`, and from `B` to `C_1`
    ///
    /// Note that `C`, `D` is still active.
    ///
    /// Step 2) 'C' gets removed.
    /// We stored edges <C, D> into component 2 `C_2`.
    /// And a mapping from `C` to `C_2`.
    ///
    /// Note that `D` is still active.
    ///
    /// Step 3) An active edge gets added from `D` to `A`.
    /// We will load `C_1` and try to re-add all edges belonging to `C_1`.
    /// We will add `<A,B>, <A,C>, <A, D>, <B, C>, <B, D>`
    ///
    /// Important note: `C_1` also contains an edge from `A` to `C`, though `C` was removed in `C_2`.
    /// - 1) We will not load edges belonging to `C_2`, even though we are adding an edges from `A` to deleted `C`.
    /// - 2) We will not delete mapping from `C` to `C_2`, because `C` doesn't belong to `C_1`.
    /// - 3) Later, `C` will be deleted, because we will figure out it's not reachable.
    /// New component `C_3` will be created.
    /// And mapping from `C` to `C_2` will be overridden by mapping from `C` to `C_3`.
    /// And therefore `C_2` component will become unreachable.
    /// TODO(gprusak): this whole algorithm seems to be leaking stuff to storage and never cleaning up.
    /// What is the point of it? What does it actually gives us?
    async fn load_component(&mut self, peer_id: &PeerId) {
        if *peer_id == self.my_peer_id || self.peer_reachable_at.contains_key(peer_id) {
            return;
        }
        let edges = tokio::spawn_blocking(|| {
            match self.store.pop_component(peer_id) {
                Ok(edges) => edges,
                Err(e) => {
                    warn!("self.store.pop_component({}): {}", peer_id, e);
                    return;
                }
            }
        }).await.unwrap();
        edges.retain(|e| self.update_edge(e.clone()));
    }

    /// Prunes peers unreachable since <unreachable_since> (and their adjacent edges)
    /// from the in-mem graph and stores them in DB.
    fn prune_unreachable_peers(&mut self, unreachable_since: time::Duration) {
        // Select peers to prune.
        let mut peers = HashSet::new();
        for (k, _) in self.edges {
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
            return vec![];
        }

        // Prune peers from peer_reachable_at.
        for peer_id in &peers {
            self.peer_reachable_at.remove(&peer_id);
        }

        // Prune edges from graph.
        let edges = self.remove_adjacent_edges(&peers);

        // Store the pruned data in DB.
        if let Err(e) = self.store.push_component(&peers, &edges) {
            warn!("self.store.push_component(): {}", e);
        }
    }

    /// update_routing_table
    /// 1. recomputes the routing table (if needed)
    /// 2. bumps peer_reachable_at to now() for peers which are still reachable.
    /// 3. prunes peers which are unreachable `prune_unreachable_since`.
    /// Returns the new routing table and the pruned edges - adjacent to the pruned peers.
    /// Should be called periodically.
    pub async fn update_routing_table(
        &mut self,
        clock: &time::Clock,
        edges: Vec<Edge>,
        unreliable_peers: &HashSet,
    ) -> Arc<routing::NextHopTable> {
        let _next_hops_recalculation =
            metrics::ROUTING_TABLE_RECALCULATION_HISTOGRAM.start_timer();
        trace!(target: "network", "Update routing table.");

        let total = edges.len();
        // load the components BEFORE graph.update_edges
        // so that result doesn't contain edges we already have in storage.
        // It is especially important for initial full sync with peers, because
        // we broadcast all the returned edges to all connected peers.
        for edge in &edges {
            let key = edge.key();
            self.load_component(&key.0).await;
            self.load_component(&key.1).await;
        }
        edges.retain(|e| self.update_edge(e.clone()));
        // Update metrics after edge update
        metrics::EDGE_UPDATES.inc_by(total as u64);
        metrics::EDGE_ACTIVE.set(self.graph.total_active_edges() as i64);
        metrics::EDGE_TOTAL.set(self.edges.len() as i64);
        
        self.prune_old_edges(clock);
        let next_hops = concurrency::rayon::run(move ||Arc::new(self.graph.calculate_distance(unreliable_peers))).await;

        // Update peer_reachable_at.
        let now = clock.now();
        self.peer_reachable_at.insert(self.my_peer_id.clone(), now);
        for peer in next_hops.keys() {
            self.peer_reachable_at.insert(peer.clone(), now);
        }
        self.prune_unreachable_peers(now.saturating_sub(self.config.unreachable_for));
        metrics::ROUTING_TABLE_RECALCULATIONS.inc();
        metrics::PEER_REACHABLE.set(next_hops.len() as i64);
        next_hops
    }
}

pub struct GraphWithCache {
    inner: tokio::Mutex<Inner>,
    edges: ArcSwap<im::HashMap<EdgeKey, Edge>>,
    unreliable_peers: ArcSwap<HashSet<PeerId>>,
}

impl GraphWithCache {
    pub fn new(my_peer_id: PeerId) -> Self {
        Self {
            graph: routing::Graph::new(my_peer_id),
            edges: Default::default(),
            cached_next_hops: OnceCell::new(),
            prune_edges_before: None,
        }
    }

    pub fn set_unreliable_peers(&self, unreliable_peers: HashSet<PeerId>) {
        self.unreliable_peers.store(Arc::new(unreliable_peers));
    }

    pub fn edges(&self) -> im::HashMap<EdgeKey, Edge> {
        self.edges.load().clone()
    }

    pub async fn update_routing_table(&self, edges: Vec<Edge>) -> Arc<NextHopTable> {
        let inner = self.inner.lock().await;
        let next_hops = inner.update_routing_table(edges, &*self.unreliable_peers.load()).await;
        self.edges.store(Arc::new(inner.edges.clone()));
        next_hops
    }
}

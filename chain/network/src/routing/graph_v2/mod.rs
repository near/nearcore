use crate::concurrency;
use crate::concurrency::runtime::Runtime;
use crate::network_protocol;
use crate::network_protocol::Edge;
use crate::routing::edge_cache::EdgeCache;
use crate::routing::routing_table_view::RoutingTableView;
use crate::stats::metrics;
use arc_swap::ArcSwap;
use near_async::time;
use near_primitives::network::PeerId;
use parking_lot::Mutex;
use rayon::iter::ParallelBridge;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// TODO: make it opaque, so that the key.0 < key.1 invariant is protected.
type EdgeKey = (PeerId, PeerId);
pub type NextHopTable = HashMap<PeerId, Vec<PeerId>>;

#[derive(Clone)]
pub struct GraphConfigV2 {
    pub node_id: PeerId,
    pub prune_edges_after: Option<time::Duration>,
}

struct ShortestPathTree {
    pub edges: Vec<EdgeKey>,
    /// The lowest nonce among all edges.
    /// For simplicity, used to expire the entire SPT at once.
    pub min_nonce: u64,
    /// Distances within this tree from the root to all other nodes in the network
    pub distance: Vec<u32>,
}

struct Inner {
    config: GraphConfigV2,
    edge_cache: EdgeCache,

    /// Mapping from neighbor to the most recent SPT received from that neighbor
    spts: HashMap<PeerId, ShortestPathTree>,
}

impl Inner {
    /// Verifies edges, then adds them to the graph.
    /// Returns true iff all the edges provided were valid.
    ///
    /// This method implements a security measure against an adversary sending invalid edges.
    /// It verifies edges in parallel until the first invalid edge is found. It adds nonces
    /// for all the edges verified so far to the cache, but drops all the remaining ones. This way
    /// the wasted work (verification of invalid edges) is constant, no matter how large the input
    /// size is.
    ///
    /// Edge verification is expensive, and it would be an attack vector if we dropped on the
    /// floor valid edges verified so far: an attacker could prepare a ShortestPathTree
    /// containing a lot of valid edges, except for the last one, and send it repeatedly to a
    /// node. The node would then validate all the edges every time, then reject the whole set
    /// because just the last edge was invalid. Instead, we cache all the edges verified so
    /// far and return an error only afterwards.
    fn verify_edges(&mut self, edges: &Vec<Edge>) -> bool {
        metrics::EDGE_UPDATES.inc_by(edges.len() as u64);

        // Collect only those edges which are new to us for verification.
        let mut unverified_edges = Vec::<Edge>::new();
        for e in edges {
            if !self.edge_cache.has_edge_nonce_or_newer(e) {
                unverified_edges.push(e.clone());
            }
        }

        // Verify the new edges in parallel on rayon.
        // Stop at first invalid edge.
        let (verified_edges, ok) = concurrency::rayon::run_blocking(move || {
            concurrency::rayon::try_map(unverified_edges.into_iter().par_bridge(), |e| {
                if e.verify() {
                    Some(e)
                } else {
                    None
                }
            })
        });

        // Store the verified nonces in the cache
        verified_edges.iter().for_each(|e| self.edge_cache.write_verified_nonce(e));

        ok
    }

    /// Accepts a list of edges specifying a ShortestPathTree. If the edges form a
    /// valid tree containing the specified `root`, returns a vector of distances from
    /// the root to all other nodes in the tree. The distance vector is indexed
    /// according to the p2id mapping in the edge_cache.
    fn calculate_distances(&self, _root: PeerId, _spt: &Vec<Edge>) -> Option<Vec<u32>> {
        // TODO
        None
    }

    /// Verifies the given ShortestPathTree. If valid, stores it in `self.spts`.
    /// Returns a boolean indicating whether the given SPT was valid.
    pub fn update_shortest_path_tree(
        &mut self,
        _clock: &time::Clock, // TODO: decide what we want to do with too-old trees
        spt: &network_protocol::ShortestPathTree,
    ) -> bool {
        // A valid SPT must contain distinct, correctly signed edges
        let original_len = spt.edges.len();
        let edges = Edge::deduplicate(spt.edges.clone());
        if edges.len() != original_len || !self.verify_edges(&edges) {
            return false;
        }

        // Write the edges to the edge_cache to generate ids and prepare for graph traversal
        for e in &edges {
            self.edge_cache.insert_active_edge(e);
        }

        if let Some(distance) = self.calculate_distances(spt.root.clone(), &edges) {
            // If the edges form a valid tree, store the updated SPT
            let val = ShortestPathTree {
                edges: edges.iter().map(|e| e.key()).cloned().collect(),
                min_nonce: edges.iter().map(|e| e.nonce()).min().unwrap(),
                distance,
            };

            match self.spts.entry(spt.root.clone()) {
                Entry::Occupied(mut occupied) => {
                    // Drop refcounts for edges in the SPT which is being overwritten
                    for e in &occupied.get().edges {
                        self.edge_cache.remove_active_edge(e);
                    }
                    occupied.insert(val);
                }
                Entry::Vacant(vacant) => {
                    vacant.insert(val);
                }
            };
            return true;
        } else {
            // If the SPT was invalid, clear the edges from the edge_cache
            for e in &edges {
                self.edge_cache.remove_active_edge(e.key());
            }
            return false;
        }
    }

    /// Handles disconnection of a peer.
    /// Drops the stored SPT, if there is one, for the specified peer_id.
    pub fn remove_direct_peer(&mut self, peer_id: PeerId) {
        if let Some(spt) = self.spts.remove(&peer_id) {
            for e in &spt.edges {
                self.edge_cache.remove_active_edge(e);
            }
        }
    }

    /// Handles connection of a new peer or nonce refresh for an existing one.
    /// Adds or updates the nonce for the given edge. If we don't already have an
    /// SPT for this peer_id, initializes one with just the given edge.
    pub fn add_or_update_direct_peer(&mut self, clock: &time::Clock, peer_id: PeerId, edge: Edge) {
        match self.spts.entry(peer_id.clone()) {
            Entry::Occupied(_occupied) => {
                if !self.edge_cache.has_edge_nonce_or_newer(&edge) {
                    self.edge_cache.write_verified_nonce(&edge);
                }
            }
            Entry::Vacant(_) => {
                assert!(self.update_shortest_path_tree(
                    &clock,
                    &network_protocol::ShortestPathTree { root: peer_id, edges: vec![edge] }
                ));
            }
        };
    }

    /// General case for all updates, which may really be an SPT message sent by a peer
    /// but may also be events submitted by the local node.
    pub fn handle_shortest_path_tree_message(
        &mut self,
        clock: &time::Clock,
        spt: &network_protocol::ShortestPathTree,
    ) -> bool {
        if spt.edges.is_empty() {
            // Handle peer disconnection submitted by the local node
            self.remove_direct_peer(spt.root.clone());
            true
        } else if spt.root == self.config.node_id {
            // Handle new peer connection or refreshed edge submitted by the local node
            assert!(spt.edges.len() == 1);
            self.add_or_update_direct_peer(
                &clock,
                spt.edges[0].other(&self.config.node_id).unwrap().clone(),
                spt.edges[0].clone(),
            );
            true
        } else {
            // Handle a ShortestPathTree message sent by a neighbor
            self.update_shortest_path_tree(&clock, spt)
        }
    }

    /// Computes and returns "next hops" for all reachable destinations in the network.
    /// Accepts a set of "unreliable peers" to avoid routing through.
    fn compute_next_hops(&mut self, _unreliable_peers: &HashSet<PeerId>) -> NextHopTable {
        let next_hops = HashMap::<PeerId, Vec<PeerId>>::new();
        let _ = self.edge_cache.max_id();
        next_hops
    }

    /// 1. Prunes expired edges.
    /// 2. Recomputes the NextHopTable.
    pub fn update(
        &mut self,
        clock: &time::Clock,
        unreliable_peers: &HashSet<PeerId>,
    ) -> NextHopTable {
        let _update_time = metrics::ROUTING_TABLE_RECALCULATION_HISTOGRAM.start_timer();

        // Prune expired edges
        if let Some(prune_edges_after) = self.config.prune_edges_after {
            let prune_nounces_older_than =
                (clock.now_utc() - prune_edges_after).unix_timestamp() as u64;

            // Drop the entirety of any expired SPTs
            self.spts.retain(|_, spt| {
                let _ = spt.distance;
                if spt.min_nonce < prune_nounces_older_than {
                    for e in &spt.edges {
                        self.edge_cache.remove_active_edge(e);
                    }
                    return false;
                }
                true
            });

            // Prune expired edges from the edge cache
            self.edge_cache.prune_old_edges(prune_nounces_older_than);
        }

        // Recompute the NextHopTable
        let next_hops = self.compute_next_hops(unreliable_peers);

        // Update metrics after update
        metrics::ROUTING_TABLE_RECALCULATIONS.inc();
        metrics::PEER_REACHABLE.set(next_hops.len() as i64);
        metrics::EDGE_TOTAL.set(self.edge_cache.known_edges_ct() as i64);

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
                config,
                edge_cache: EdgeCache::new(),
                spts: HashMap::new(),
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

    /// Accepts and processes a batch of ShortestPathTree messages.
    /// Each ShortestPathTree is verified and, if valid, stored. After all updates are
    /// processed, recomputes the local node's routing table.
    ///
    /// May return a new ShortestPathTree for the local node, to be broadcasted to peers.
    /// Does so iff routing distances have changed due to the processed messages.
    ///
    /// Returns (spt,oks) where
    /// * spt may contain an updated ShortestPathTree for the local node
    /// * oks.len() == spts.len() and oks[i] is true iff spts[i] was valid
    pub async fn update(
        self: &Arc<Self>,
        clock: &time::Clock,
        spts: Vec<network_protocol::ShortestPathTree>,
    ) -> (Option<network_protocol::ShortestPathTree>, Vec<bool>) {
        // TODO(saketh): Consider whether we can move this to rayon.
        let this = self.clone();
        let clock = clock.clone();
        self.runtime
            .handle
            .spawn_blocking(move || {
                let mut inner = this.inner.lock();

                let oks = spts
                    .iter()
                    .map(|spt| inner.handle_shortest_path_tree_message(&clock, spt))
                    .collect();

                let next_hops = inner.update(&clock, &this.unreliable_peers.load());

                this.routing_table.update(next_hops.into());

                (
                    // TODO: fix this
                    Some(network_protocol::ShortestPathTree {
                        root: inner.config.node_id.clone(),
                        edges: vec![],
                    }),
                    oks,
                )
            })
            .await
            .unwrap()
    }
}

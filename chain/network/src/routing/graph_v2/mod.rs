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

/// Internal storage of ShortestPathTrees; augmented with distances
struct ShortestPathTree {
    pub root: PeerId,
    pub edges: Vec<Edge>,

    /// Distances from the root of this tree to all other nodes in the network
    pub distance: Vec<u32>,
}

struct Inner {
    config: GraphConfigV2,
    edge_cache: EdgeCache,

    /// Mapping from neighbor `PeerId` to the most recent `ShortestPathTree`
    /// received from the neighbor
    spts: HashMap<PeerId, ShortestPathTree>,
}

impl Inner {
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
        //edges.retain(|e| !self.edge_cache.has(e));

        // Verify the edges in parallel on rayon.
        // Stop at first invalid edge.
        let (edges, ok) = concurrency::rayon::run_blocking(move || {
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
        //edges.retain(|e| self.edge_cache.add_or_update_edge(now, e.clone()));
        (edges, ok)
    }

    /// Verifies the given ShortestPathTree. If valid, stores the updated SPT.
    /// Returns a boolean indicating whether the given SPT was valid.
    ///
    /// Even if the SPT is invalid overall,
    pub fn update_shortest_path_tree(
        &mut self,
        clock: &time::Clock,
        spt: &network_protocol::ShortestPathTree,
    ) -> bool {
        true
    }

    /// Computes and returns "next hops" for all reachable destinations in the network.
    /// Accepts a set of "unreliable peers" to avoid routing through.
    fn compute_next_hops(&mut self, unreliable_peers: &HashSet<PeerId>) -> NextHopTable {
        let next_hops = HashMap::<PeerId, Vec<PeerId>>::new();

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

    /// Each entry of `spts` are ShortestPathTrees coming from a different source.
    /// Verifies, then stores updated ShortestPathTrees, then recomputes the routing table.
    /// Returns (new_spt,oks) where
    /// * new_spts may contain an updated ShortestPathTree for this node, to be broadcasted
    /// * oks.len() == spts.len() and oks[i] is true iff spts[i] was valid
    ///
    /// The validation of each `edges[i]` separately, stops at the first invalid edge,
    /// and all remaining edges of `edges[i]` are discarded.
    ///
    /// Edge verification is expensive, and it would be an attack vector if we dropped on the
    /// floor valid edges verified so far: an attacker could prepare a ShortestPathTree
    /// containing a lot of valid edges, except for the last one, and send it repeatedly to a
    /// node. The node would then validate all the edges every time, then reject the whole set
    /// because just the last edge was invalid. Instead, we cache all the edges verified so
    /// far and return an error only afterwards.
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

                // TODO: a simple optimization could be to skip all but the latest SPT for each
                // peer; it saves time, but allows the possibility that we skip an invalid SPT and
                // fail to ban the node (which seems to be an acceptable tradeoff)
                let oks =
                    spts.iter().map(|spt| inner.update_shortest_path_tree(&clock, spt)).collect();

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

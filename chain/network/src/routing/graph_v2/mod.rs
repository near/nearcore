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
use std::collections::hash_map::Entry;
use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[cfg(not(test))]
use crate::concurrency;
#[cfg(not(test))]
use rayon::iter::ParallelBridge;

#[cfg(test)]
mod testonly;
#[cfg(test)]
mod tests;

pub type NextHopTable = HashMap<PeerId, Vec<PeerId>>;

#[derive(Clone)]
pub struct GraphConfigV2 {
    pub node_id: PeerId,
    pub prune_edges_after: Option<time::Duration>,
}

// Locally computed properties of a ShortestPathTree
struct ShortestPathTreeInfo {
    /// The lowest nonce among all edges.
    /// For simplicity, used to expire the entire SPT at once.
    pub min_nonce: u64,
    /// Distances within this tree from the root to all other nodes in the network
    pub distance: Vec<i32>,
}

struct Inner {
    config: GraphConfigV2,
    edge_cache: EdgeCache,

    /// Mapping from peer to information about the latest ShortestPathTree shared by the peer
    spts: HashMap<PeerId, ShortestPathTreeInfo>,
    /// Shortest known distances from the local node to other nodes
    distance: HashMap<PeerId, u32>,
    /// The ShortestPathTree rooted at the local node
    local_spt: network_protocol::ShortestPathTree,
}

impl Inner {
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
    #[cfg(not(test))]
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
    /// valid tree containing the specified peer `root`, returns a vector of distances from
    /// the root to all other nodes in the tree. The distance vector is indexed
    /// according to the p2id mapping in the edge_cache. Unreachable nodes have distance -1.
    pub(crate) fn calculate_distances(
        &mut self,
        root: &PeerId,
        spt: &Vec<Edge>,
    ) -> Option<Vec<i32>> {
        // Prepare for graph traversal by ensuring all PeerIds in the tree have a u32 label
        self.edge_cache.create_ids_for_unmapped_peers(spt);

        // Build adjacency-list representation of the edges
        let mut adjacency = vec![Vec::<u32>::new(); self.edge_cache.max_id()];
        for edge in spt {
            let (peer0, peer1) = edge.key();
            let id0 = self.edge_cache.get_id(peer0);
            let id1 = self.edge_cache.get_id(peer1);
            adjacency[id0 as usize].push(id1);
            adjacency[id1 as usize].push(id0);
        }

        // Compute distances from the root by breadth-first search
        let mut distance: Vec<i32> = vec![-1; self.edge_cache.max_id()];
        {
            let root_id = self.edge_cache.get_id(root);

            let mut queue = VecDeque::new();
            queue.push_back(root_id);
            distance[root_id as usize] = 0;

            while let Some(cur_peer) = queue.pop_front() {
                let cur_distance = distance[cur_peer as usize];

                for &neighbor in &adjacency[cur_peer as usize] {
                    if distance[neighbor as usize] == -1 {
                        distance[neighbor as usize] = cur_distance + 1;
                        queue.push_back(neighbor);
                    }
                }
            }
        }

        // Check that the edges in `spt` actually form a tree containing `root`
        let mut num_reachable_nodes = 0;
        for &dist in &distance {
            if dist != -1 {
                num_reachable_nodes += 1;
            }
        }
        if num_reachable_nodes == spt.len() + 1 {
            Some(distance)
        } else {
            None
        }
    }

    /// Verifies the given ShortestPathTree. If valid, stores it in `self.spts`.
    /// Returns a boolean indicating whether the given SPT was valid.
    pub(crate) fn update_shortest_path_tree(
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

        if let Some(distance) = self.calculate_distances(&spt.root, &edges) {
            // If the edges form a valid tree, store the updated SPT
            self.edge_cache.update_shortest_path_tree(&spt.root, &edges);
            self.spts.insert(
                spt.root.clone(),
                ShortestPathTreeInfo {
                    min_nonce: edges.iter().map(|e| e.nonce()).min().unwrap(),
                    distance,
                },
            );
            return true;
        } else {
            // If the SPT was invalid, clean up any u32 labels which may have been assigned
            // during the distance calculation
            self.edge_cache.free_unused_ids();
            return false;
        }
    }

    /// Handles disconnection of a peer.
    /// Drops the stored SPT, if there is one, for the specified peer_id.
    pub(crate) fn remove_direct_peer(&mut self, peer_id: PeerId) {
        self.edge_cache.remove_shortest_path_tree(&peer_id);
        self.spts.remove(&peer_id);
    }

    /// Handles connection of a new peer or nonce refresh for an existing one.
    /// Adds or updates the nonce for the given edge. If we don't already have an
    /// SPT for this peer_id, initializes one with just the given edge.
    pub(crate) fn add_or_update_direct_peer(
        &mut self,
        clock: &time::Clock,
        peer_id: PeerId,
        edge: Edge,
    ) {
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
    pub(crate) fn handle_shortest_path_tree_message(
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
    /// TODO: Actually avoid the unreliable peers
    ///
    /// Returns the NextHopTable along with a mapping from the reachable nodes in the
    /// network to their shortest-path distances.
    pub(crate) fn compute_next_hops(
        &mut self,
        _unreliable_peers: &HashSet<PeerId>,
    ) -> (NextHopTable, HashMap<PeerId, u32>) {
        let max_id = self.edge_cache.max_id();
        let local_node_id = self.edge_cache.get_id(&self.config.node_id);

        // Calculate the min distance to each routable node
        let mut distance_by_id: Vec<i32> = vec![-1; max_id];
        distance_by_id[local_node_id as usize] = 0;
        for (_, spt) in &mut self.spts {
            // The p2id mapping in the edge_cache is dynamic. We can still use previous distance
            // calculations because a node incident to an active edge won't be relabelled. However,
            // we may need to resize the distance vector.
            spt.distance.resize(max_id, -1);

            for id in 0..max_id {
                if spt.distance[id] != -1 {
                    if distance_by_id[id] == -1 || distance_by_id[id] > (spt.distance[id] + 1) {
                        distance_by_id[id] = spt.distance[id] + 1;
                    }
                }
            }
        }

        // Compute the next hop table
        let mut next_hops_by_id: Vec<Vec<PeerId>> = vec![vec![]; self.edge_cache.max_id()];
        for id in 0..max_id {
            if distance_by_id[id] != -1 {
                for (peer_id, spt) in &self.spts {
                    if spt.distance[id] + 1 == distance_by_id[id] {
                        next_hops_by_id[id].push(peer_id.clone());
                    }
                }
            }
        }
        let mut next_hops = HashMap::<PeerId, Vec<PeerId>>::new();
        for (peer_id, id) in self.edge_cache.iter_peers() {
            if !next_hops_by_id[*id as usize].is_empty() {
                next_hops.insert(peer_id.clone(), next_hops_by_id[*id as usize].clone());
            }
        }

        // Build a PeerId-keyed map of distances
        let mut distance: HashMap<PeerId, u32> = HashMap::new();
        for (peer_id, id) in self.edge_cache.iter_peers() {
            if distance_by_id[*id as usize] != -1 {
                distance.insert(peer_id.clone(), distance_by_id[*id as usize] as u32);
            }
        }

        (next_hops, distance)
    }

    /// 1. Prunes expired edges.
    /// 2. Recomputes the NextHopTable.
    ///
    /// Returns the recomputed NextHopTable.
    /// If distances have changed, returns an updated ShortestPathTree to be broadcast.
    pub(crate) fn update(
        &mut self,
        clock: &time::Clock,
        unreliable_peers: &HashSet<PeerId>,
    ) -> (NextHopTable, Option<network_protocol::ShortestPathTree>) {
        let _update_time = metrics::ROUTING_TABLE_RECALCULATION_HISTOGRAM.start_timer();

        // Prune expired edges
        if let Some(prune_edges_after) = self.config.prune_edges_after {
            let prune_nounces_older_than =
                (clock.now_utc() - prune_edges_after).unix_timestamp() as u64;

            // Drop the entirety of any expired SPTs
            let peers_to_remove: Vec<PeerId> = self
                .spts
                .iter()
                .filter_map(|(peer, spt)| {
                    if spt.min_nonce < prune_nounces_older_than {
                        Some(peer.clone())
                    } else {
                        None
                    }
                })
                .collect();

            for peer_id in &peers_to_remove {
                self.edge_cache.remove_shortest_path_tree(peer_id);
                self.spts.remove(peer_id);
            }

            // Prune expired edges from the edge cache
            self.edge_cache.prune_old_edges(prune_nounces_older_than);
        }

        // Recompute the NextHopTable
        let (next_hops, distance) = self.compute_next_hops(unreliable_peers);

        // If distances in the network have changed,
        // construct and return a message to be broadcasted to peers
        let to_broadcast = if distance != self.distance {
            self.distance = distance;
            self.local_spt = network_protocol::ShortestPathTree {
                root: self.config.node_id.clone(),
                edges: self.edge_cache.construct_shortest_path_tree(&self.distance),
            };
            //Some(self.local_spt.clone())
            None
        } else {
            None
        };

        // Update metrics after update
        metrics::ROUTING_TABLE_RECALCULATIONS.inc();
        metrics::PEER_REACHABLE.set(next_hops.len() as i64);
        metrics::EDGE_TOTAL.set(self.edge_cache.known_edges_ct() as i64);

        (next_hops, to_broadcast)
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
        let local_node = config.node_id.clone();
        let edge_cache = EdgeCache::new(local_node.clone());
        Self {
            routing_table: RoutingTableView::new(),
            inner: Arc::new(Mutex::new(Inner {
                config,
                edge_cache,
                spts: HashMap::new(),
                distance: HashMap::new(),
                local_spt: network_protocol::ShortestPathTree { root: local_node, edges: vec![] },
            })),
            unreliable_peers: ArcSwap::default(),
            runtime: Runtime::new(),
        }
    }

    pub fn set_unreliable_peers(&self, unreliable_peers: HashSet<PeerId>) {
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

                let (next_hops, to_broadcast) = inner.update(&clock, &this.unreliable_peers.load());

                this.routing_table.update(next_hops.into());

                (to_broadcast, oks)
            })
            .await
            .unwrap()
    }
}

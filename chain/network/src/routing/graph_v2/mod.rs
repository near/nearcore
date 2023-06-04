use crate::concurrency::runtime::Runtime;
use crate::network_protocol;
use crate::network_protocol::{AdvertisedRoute, Edge};
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

// Locally stored properties of a received DistanceVector message
struct PeerRoutes {
    /// The lowest nonce among all edges used in the advertised routes.
    /// For simplicity, used to expire the entire vector at once.
    pub min_nonce: u64,
    /// Advertised route lengths indexed by the local EdgeCache's mapping
    pub distance: Vec<i32>,
}

struct Inner {
    config: GraphConfigV2,
    edge_cache: EdgeCache,

    /// Mapping from peer to the routes advertised by the peer
    peer_routes: HashMap<PeerId, PeerRoutes>,
    /// Lengths of the shortest known routes from the local node to other nodes
    my_distances: HashMap<PeerId, u32>,
    /// The latest DistanceVector advertised by the local node
    my_distance_vector: network_protocol::DistanceVector,
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
    /// floor valid edges verified so far: an attacker could prepare a message containing
    /// a lot of valid edges, except for the last one, and send it repeatedly to a node.
    /// The node would then validate all the edges every time, then reject the whole set
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

    /// Accepts a root node and a list of edges specifying a tree. If the edges form
    /// a valid tree containing the specified `root`, returns a pair of vectors
    /// (distance, first_step). Otherwise, returns None.
    ///
    /// For each node in the tree, `distance` indicates the length of the path
    /// from the root to the node. Nodes outside the tree have distance -1.
    ///
    /// For each node in the tree, `first_step` indicates the root's neighbor on the path
    /// from the root to the node. The root of the tree, as well as any nodes outside
    /// the tree, have a first_step of -1.
    ///
    /// Nodes are indexed into the vectors according to the `p2id` mapping in the EdgeCache.
    pub(crate) fn calculate_tree_distances(
        &mut self,
        root: &PeerId,
        tree_edges: &Vec<Edge>,
    ) -> Option<(Vec<i32>, Vec<i32>)> {
        // Prepare for graph traversal by ensuring all PeerIds in the tree have a u32 label
        self.edge_cache.create_ids_for_unmapped_peers(tree_edges);

        // Build adjacency-list representation of the edges
        let mut adjacency = vec![Vec::<u32>::new(); self.edge_cache.max_id()];
        for edge in tree_edges {
            let (peer0, peer1) = edge.key();
            let id0 = self.edge_cache.get_id(peer0);
            let id1 = self.edge_cache.get_id(peer1);
            adjacency[id0 as usize].push(id1);
            adjacency[id1 as usize].push(id0);
        }

        // Compute distances from the root by breadth-first search
        let mut distance: Vec<i32> = vec![-1; self.edge_cache.max_id()];
        let mut first_step: Vec<i32> = vec![-1; self.edge_cache.max_id()];
        {
            let root_id = self.edge_cache.get_id(root);
            let mut queue = VecDeque::new();
            queue.push_back(root_id);
            distance[root_id as usize] = 0;

            while let Some(cur_peer) = queue.pop_front() {
                let cur_peer = cur_peer as usize;
                let cur_distance = distance[cur_peer];

                for &neighbor in &adjacency[cur_peer] {
                    let neighbor = neighbor as usize;
                    if distance[neighbor] == -1 {
                        distance[neighbor] = cur_distance + 1;
                        first_step[neighbor] = if (cur_peer as u32) == root_id {
                            neighbor as i32
                        } else {
                            first_step[cur_peer]
                        };
                        queue.push_back(neighbor as u32);
                    }
                }
            }
        }

        // Check that the edges in `tree_edges` actually form a tree containing `root`
        let mut num_reachable_nodes = 0;
        for &dist in &distance {
            if dist != -1 {
                num_reachable_nodes += 1;
            }
        }
        if num_reachable_nodes != tree_edges.len() + 1 {
            return None;
        }

        Some((distance, first_step))
    }

    /// Given a DistanceVector message, validates the advertised routes against the accompanying
    /// tree. If valid, returns a mapping from destination to validated route length. Removes any
    /// advertised routes which go through the local node; it doesn't make sense to forward to
    /// a neighbor who will just sent the message right back to us.
    fn get_validated_routes(
        &mut self,
        distance_vector: &network_protocol::DistanceVector,
    ) -> Option<PeerRoutes> {
        // A valid DistanceVector must contain distinct, correctly signed edges
        let original_len = distance_vector.edges.len();
        let edges = Edge::deduplicate(distance_vector.edges.clone());
        if edges.len() != original_len || !self.verify_edges(&edges) {
            return None;
        }

        // Check validity of the spanning tree and compute its basic properties
        let tree_traversal = self.calculate_tree_distances(&distance_vector.root, &edges);
        if tree_traversal.is_none() {
            return None;
        }
        let (tree_distance, first_step) = tree_traversal.unwrap();

        // Collect the advertised routes and verify that they are consistent with the tree
        let mut advertised_distances: Vec<i32> = vec![-1; self.edge_cache.max_id()];
        for route in &distance_vector.routes {
            let destination_id = self.edge_cache.get_id(&route.destination) as usize;
            advertised_distances[destination_id] = route.length as i32;
        }
        let mut consistent = true;
        for id in 0..self.edge_cache.max_id() {
            if advertised_distances[id] == -1 {
                consistent &= tree_distance[id] == -1;
            } else {
                // It's OK for the tree to "prove" a shorter distance than advertised
                consistent &= tree_distance[id] <= advertised_distances[id];
            }
        }
        if !consistent {
            return None;
        }

        // At this point, we know that the DistanceVector message is valid.
        // Now, prune any advertised routes which go through the local node; it doesn't make
        // sense to forward a message to a neighbor who will send it back to us
        let local_node_id = self.edge_cache.get_id(&self.config.node_id) as i32;
        for id in 0..self.edge_cache.max_id() {
            if first_step[id] == local_node_id {
                advertised_distances[id] = -1;
            }
        }

        Some(PeerRoutes {
            min_nonce: edges.iter().map(|e| e.nonce()).min().unwrap(),
            distance: advertised_distances,
        })
    }

    /// Verifies the given DistanceVector. If valid, stores the advertised routes.
    /// Returns a boolean indicating whether the DistanceVector was valid.
    fn handle_distance_vector(
        &mut self,
        _clock: &time::Clock, // TODO: decide what we want to do with too-old messages
        distance_vector: &network_protocol::DistanceVector,
    ) -> bool {
        if let Some(routes) = self.get_validated_routes(distance_vector) {
            // Store the tree used to validate the routes.
            self.edge_cache.update_tree(&distance_vector.root, &distance_vector.edges);
            // Store the validated routes
            self.peer_routes.insert(distance_vector.root.clone(), routes);
            true
        } else {
            // In case the update was rejected, clean up any ids which may have allocated
            // to perform the tree traversal
            self.edge_cache.free_unused_ids();
            false
        }
    }

    /// Handles disconnection of a peer.
    /// Drops the stored SPT, if there is one, for the specified peer_id.
    pub(crate) fn remove_direct_peer(&mut self, peer_id: &PeerId) {
        self.edge_cache.remove_tree(peer_id);
        self.peer_routes.remove(peer_id);
    }

    /// Handles connection of a new peer or nonce refresh for an existing one.
    /// Adds or updates the nonce for the given edge. If we don't already have an
    /// DistanceVector for this peer_id, initializes one with just the given edge.
    pub(crate) fn add_or_update_direct_peer(
        &mut self,
        clock: &time::Clock,
        peer_id: PeerId,
        edge: Edge,
    ) {
        match self.peer_routes.entry(peer_id.clone()) {
            Entry::Occupied(_occupied) => {
                // Refresh the nonce in the cache for the direct edge
                if !self.edge_cache.has_edge_nonce_or_newer(&edge) {
                    self.edge_cache.write_verified_nonce(&edge);
                }
            }
            Entry::Vacant(_) => {
                // We have no advertised routes for this peer; initialize
                assert!(self.handle_distance_vector(
                    &clock,
                    &network_protocol::DistanceVector {
                        root: peer_id.clone(),
                        routes: vec![AdvertisedRoute { destination: peer_id, length: 0 }],
                        edges: vec![edge]
                    }
                ));
            }
        };
    }

    /// General case for all updates, which may really be an SPT message sent by a peer
    /// but may also be events submitted by the local node.
    /// TODO(saketh): refactor this, it is needlessly clever
    pub(crate) fn handle_message(
        &mut self,
        clock: &time::Clock,
        distance_vector: &network_protocol::DistanceVector,
    ) -> bool {
        if distance_vector.edges.is_empty() {
            // Handle peer disconnection submitted by the local node
            self.remove_direct_peer(&distance_vector.root);
            true
        } else if distance_vector.root == self.config.node_id {
            // Handle new peer connection or refreshed edge submitted by the local node
            assert!(distance_vector.edges.len() == 1);
            self.add_or_update_direct_peer(
                &clock,
                distance_vector.edges[0].other(&self.config.node_id).unwrap().clone(),
                distance_vector.edges[0].clone(),
            );
            true
        } else {
            // Handle a DistanceVector message sent by a neighbor
            self.handle_distance_vector(&clock, distance_vector)
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
        let mut min_distance: Vec<i32> = vec![-1; max_id];
        min_distance[local_node_id as usize] = 0;
        for (_, routes) in &mut self.peer_routes {
            // The p2id mapping in the edge_cache is dynamic. We can still use previous distance
            // calculations because a node incident to an active edge won't be relabelled. However,
            // we may need to resize the distance vector.
            routes.distance.resize(max_id, -1);

            for id in 0..max_id {
                if routes.distance[id] != -1 {
                    let available_distance = routes.distance[id] + 1;
                    if min_distance[id] == -1 || available_distance < min_distance[id] {
                        min_distance[id] = available_distance;
                    }
                }
            }
        }

        // Compute the next hop table
        let mut next_hops_by_id: Vec<Vec<PeerId>> = vec![vec![]; self.edge_cache.max_id()];
        for id in 0..max_id {
            if min_distance[id] != -1 {
                for (peer_id, routes) in &self.peer_routes {
                    if routes.distance[id] != -1 && routes.distance[id] + 1 == min_distance[id] {
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
            if min_distance[*id as usize] != -1 {
                distance.insert(peer_id.clone(), min_distance[*id as usize] as u32);
            }
        }

        (next_hops, distance)
    }

    /// 1. Prunes expired edges.
    /// 2. Recomputes the NextHopTable.
    ///
    /// Returns the recomputed NextHopTable.
    /// If distances have changed, returns an updated DistanceVector to be broadcast.
    pub(crate) fn update(
        &mut self,
        clock: &time::Clock,
        unreliable_peers: &HashSet<PeerId>,
    ) -> (NextHopTable, Option<network_protocol::DistanceVector>) {
        let _update_time = metrics::ROUTING_TABLE_RECALCULATION_HISTOGRAM.start_timer();

        // Prune expired edges
        if let Some(prune_edges_after) = self.config.prune_edges_after {
            let prune_nounces_older_than =
                (clock.now_utc() - prune_edges_after).unix_timestamp() as u64;

            // Drop the entirety of any expired distance vectors
            let peers_to_remove: Vec<PeerId> = self
                .peer_routes
                .iter()
                .filter_map(|(peer, routes)| {
                    if routes.min_nonce < prune_nounces_older_than {
                        Some(peer.clone())
                    } else {
                        None
                    }
                })
                .collect();

            for peer_id in &peers_to_remove {
                self.remove_direct_peer(peer_id);
            }

            // Prune expired edges from the edge cache
            self.edge_cache.prune_old_edges(prune_nounces_older_than);
        }

        // Recompute the NextHopTable
        let (next_hops, distances) = self.compute_next_hops(unreliable_peers);

        // If distances in the network have changed,
        // construct and return a message to be broadcasted to peers
        let to_broadcast = if distances != self.my_distances {
            self.my_distances = distances;
            self.my_distance_vector = network_protocol::DistanceVector {
                root: self.config.node_id.clone(),
                routes: self
                    .my_distances
                    .iter()
                    .map(|(destination, length)| AdvertisedRoute {
                        destination: destination.clone(),
                        length: *length,
                    })
                    .collect(),
                edges: self.edge_cache.construct_spanning_tree(&self.my_distances),
            };
            Some(self.my_distance_vector.clone())
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

        let my_distance_vector = network_protocol::DistanceVector {
            root: local_node.clone(),
            routes: vec![AdvertisedRoute { destination: local_node, length: 0 }],
            edges: vec![],
        };

        Self {
            routing_table: RoutingTableView::new(),
            inner: Arc::new(Mutex::new(Inner {
                config,
                edge_cache,
                peer_routes: HashMap::new(),
                my_distances: HashMap::new(),
                my_distance_vector,
            })),
            unreliable_peers: ArcSwap::default(),
            runtime: Runtime::new(),
        }
    }

    pub fn set_unreliable_peers(&self, unreliable_peers: HashSet<PeerId>) {
        self.unreliable_peers.store(Arc::new(unreliable_peers));
    }

    /// Accepts and processes a batch of network_protocol::DistanceVector messages.
    /// Each DistanceVector is verified and, if valid, the advertised routes are stored.
    /// After all updates are processed, recomputes the local node's next hop table.
    ///
    /// May return a new DistanceVector for the local node, to be broadcasted to peers.
    /// Does so iff routing distances have changed due to the processed updates.
    ///
    /// Returns (distance_vector, oks) where
    /// * distance_vector is an Option<DistanceVector> to be broadcasted
    /// * oks.len() == distance_vectors.len() and oks[i] is true iff distance_vectors[i] was valid
    pub async fn update(
        self: &Arc<Self>,
        clock: &time::Clock,
        distance_vectors: Vec<network_protocol::DistanceVector>,
    ) -> (Option<network_protocol::DistanceVector>, Vec<bool>) {
        // TODO(saketh): Consider whether we can move this to rayon.
        let this = self.clone();
        let clock = clock.clone();
        self.runtime
            .handle
            .spawn_blocking(move || {
                let mut inner = this.inner.lock();

                let oks =
                    distance_vectors.iter().map(|dv| inner.handle_message(&clock, dv)).collect();

                let (next_hops, to_broadcast) = inner.update(&clock, &this.unreliable_peers.load());

                this.routing_table.update(next_hops.into());

                (to_broadcast, oks)
            })
            .await
            .unwrap()
    }
}

use crate::concurrency::runtime::Runtime;
use crate::network_protocol;
use crate::network_protocol::{AdvertisedPeerDistance, Edge, EdgeState};
use crate::routing::edge_cache::EdgeCache;
use crate::routing::routing_table_view::RoutingTableView;
use crate::stats::metrics;
use arc_swap::ArcSwap;
use near_async::time;
use near_primitives::network::PeerId;
use near_primitives::views::{EdgeView, NetworkRoutesView, PeerDistancesView};
use parking_lot::Mutex;
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

pub enum NetworkTopologyChange {
    PeerConnected(PeerId, Edge),
    PeerDisconnected(PeerId),
    PeerAdvertisedDistances(network_protocol::DistanceVector),
}

/// Locally stored properties of a received network_protocol::DistanceVector message
struct PeerDistances {
    /// Advertised distances indexed by the local EdgeCache's peer to id mapping.
    pub distance: Vec<Option<u32>>,
    /// The lowest nonce among all edges used to validate the distances.
    /// For simplicity, used to expire the entire distance vector at once.
    pub min_nonce: u64,
}

struct Inner {
    config: GraphConfigV2,

    /// Data structure maintaing information about the entire known network
    edge_cache: EdgeCache,

    /// Edges of the local node's direct connections
    local_edges: HashMap<PeerId, Edge>,
    /// Distances advertised by the local node's direct peers
    peer_distances: HashMap<PeerId, PeerDistances>,

    /// Distances from the local node to other nodes
    my_distances: HashMap<PeerId, u32>,
    /// The latest DistanceVector advertised by the local node
    my_distance_vector: network_protocol::DistanceVector,
}

impl Inner {
    /// Function which verifies signed edges.
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
    fn verify_and_cache_edge_nonces(&mut self, edges: &Vec<Edge>) -> bool {
        metrics::EDGE_UPDATES.inc_by(edges.len() as u64);

        // Collect only those edges which are new to us for verification.
        let mut unverified_edges = Vec::<Edge>::new();
        for e in edges {
            // V2 routing protocol only shares Active edges
            // TODO(saketh): deprecate tombstones entirely
            if e.edge_type() != EdgeState::Active {
                return false;
            }

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

    /// Function computing basic properties of a tree.
    ///
    /// Accepts a root node and a list of edges specifying a tree. If the edges form
    /// a valid tree containing the specified `root`, returns a pair of vectors
    /// (distance, first_step). Otherwise, returns None.
    ///
    /// Nodes are indexed into the vectors according to the peer to id mapping in the EdgeCache.
    /// If `tree_edges` contain some previously unseen peers, new ids are allocated for them.
    ///
    /// For each node in the tree, `distance` indicates the length of the path
    /// from the root to the node. Nodes outside the tree have distance None.
    ///
    /// For each node in the tree, `first_step` indicates the root's neighbor on the path
    /// from the root to the node. The root of the tree, as well as any nodes outside
    /// the tree, have a first_step of None.
    pub(crate) fn calculate_tree_distances(
        &mut self,
        root: &PeerId,
        tree_edges: &Vec<Edge>,
    ) -> Option<(Vec<Option<u32>>, Vec<Option<u32>>)> {
        // Prepare for graph traversal by ensuring all PeerIds in the tree have a u32 label
        self.edge_cache.create_ids_for_tree(root, tree_edges);

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
        let mut distance: Vec<Option<u32>> = vec![None; self.edge_cache.max_id()];
        let mut first_step: Vec<Option<u32>> = vec![None; self.edge_cache.max_id()];
        {
            let root_id = self.edge_cache.get_id(root);
            let mut queue = VecDeque::new();
            queue.push_back(root_id);
            distance[root_id as usize] = Some(0);

            while let Some(cur_peer) = queue.pop_front() {
                let cur_peer = cur_peer as usize;
                // The unwrap here is safe because anything pushed to the queue has a distance
                let cur_distance = distance[cur_peer].unwrap();

                for &neighbor in &adjacency[cur_peer] {
                    let neighbor = neighbor as usize;
                    if distance[neighbor].is_none() {
                        distance[neighbor] = Some(cur_distance + 1);
                        first_step[neighbor] = first_step[cur_peer].or(Some(neighbor as u32));
                        queue.push_back(neighbor as u32);
                    }
                }
            }
        }

        // Check that the edges in `tree_edges` actually form a tree containing `root`
        let mut num_reachable_nodes = 0;
        for &dist in &distance {
            if dist.is_some() {
                num_reachable_nodes += 1;
            }
        }
        if num_reachable_nodes != tree_edges.len() + 1 {
            return None;
        }

        Some((distance, first_step))
    }

    /// Given a DistanceVector message, validates the advertised distances against the spanning tree.
    ///
    /// If valid, returns a vector of distances indexed according to the local node's EdgeCache's
    /// peer to id mapping. Otherwise, returns None.
    ///
    /// Removes any advertised routes which go through the local node; it doesn't make sense
    /// to forward to a neighbor who will just sent the message right back to us.
    pub(crate) fn validate_routing_distances(
        &mut self,
        distance_vector: &network_protocol::DistanceVector,
    ) -> Option<Vec<Option<u32>>> {
        // A valid DistanceVector must contain distinct, correctly signed edges
        let original_len = distance_vector.edges.len();
        let edges = Edge::deduplicate(distance_vector.edges.clone());
        if edges.len() != original_len || !self.verify_and_cache_edge_nonces(&edges) {
            return None;
        }

        // Check validity of the spanning tree and compute its basic properties
        let tree_traversal = self.calculate_tree_distances(&distance_vector.root, &edges);
        let (tree_distance, first_step) = tree_traversal?;

        // Verify that the advertised distances are corroborated by the spanning tree distances
        let mut advertised_distances: Vec<Option<u32>> = vec![None; self.edge_cache.max_id()];
        for entry in &distance_vector.distances {
            let destination_id = self.edge_cache.get_or_create_id(&entry.destination) as usize;
            advertised_distances[destination_id] = Some(entry.distance);
        }
        let mut consistent = true;
        for id in 0..self.edge_cache.max_id() {
            if let Some(advertised_distance) = advertised_distances[id] {
                // The tree must have a route, but it can be shorter than the advertised distance
                consistent &= tree_distance[id]
                    .is_some_and(|tree_distance| tree_distance <= advertised_distance);
            } else {
                consistent &= tree_distance[id].is_none();
            }
        }
        // After this point, we know that the DistanceVector message is valid
        if !consistent {
            return None;
        }

        // Now, prune any advertised routes which go through the local node; it doesn't make
        // sense to forward a message to a neighbor who will send it back to us
        let local_node_id = self.edge_cache.get_local_node_id() as usize;
        for id in 0..self.edge_cache.max_id() {
            if id != local_node_id
                && first_step[id].is_some_and(|first_step| first_step == local_node_id as u32)
            {
                advertised_distances[id] = None;
            }
        }

        Some(advertised_distances)
    }

    /// Accepts a validated DistanceVector and its `advertised_distances`.
    /// Updates the status of the direct connection between the local node and the direct peer.
    /// If the peer can be used for forwarding, stores the advertised distances.
    /// Returns true iff the distances are stored.
    fn store_validated_peer_distances(
        &mut self,
        distance_vector: &network_protocol::DistanceVector,
        mut advertised_distances: Vec<Option<u32>>,
    ) -> bool {
        let local_node_id = self.edge_cache.get_local_node_id() as usize;

        // A direct peer's distance vector which advertises an indirect path to the local node
        // is outdated and can be ignored.
        if advertised_distances[local_node_id].is_some_and(|distance| distance > 1) {
            // TODO(saketh): We could try to be more clever here and do some surgery on the tree
            // to replace the indirect path and speed up convergence of the routing protocol.
            return false;
        }

        // Look in the spanning tree for the direct edge between the local node and the root
        let tree_edge = distance_vector.edges.iter().find(|edge| {
            edge.contains_peer(&self.config.node_id) && edge.contains_peer(&distance_vector.root)
        });

        // If the tree has more recent state for the direct edge, replace the local state
        if let Some(tree_edge) = tree_edge {
            self.local_edges
                .entry(distance_vector.root.clone())
                .and_modify(|local_edge| {
                    if tree_edge.nonce() > local_edge.nonce() {
                        *local_edge = tree_edge.clone();
                    }
                })
                .or_insert(tree_edge.clone());
        }

        // Without a direct edge, we cannot use the distances advertised by the peer
        let Some(local_edge) = self.local_edges.get(&distance_vector.root) else {
            return false;
        };
        if local_edge.edge_type() == EdgeState::Removed {
            return false;
        }

        // If the spanning tree doesn't already include the direct edge, add it
        let mut spanning_tree = distance_vector.edges.clone();
        if tree_edge.is_none() {
            if !advertised_distances[local_node_id].is_none() {
                debug_assert!(false);
                return false;
            }

            spanning_tree.push(local_edge.clone());
            advertised_distances[local_node_id] = Some(1);
        }

        // .min().unwrap() is safe here because the tree is now guaranteed to at least
        // include the direct edge between the local node and the peer
        debug_assert!(!spanning_tree.is_empty());
        let min_nonce = spanning_tree.iter().map(|e| e.nonce()).min().unwrap();

        // Store the tree used to validate the distances.
        self.edge_cache.update_tree(&distance_vector.root, &spanning_tree);
        // Store the validated distances
        self.peer_distances.insert(
            distance_vector.root.clone(),
            PeerDistances { distance: advertised_distances, min_nonce },
        );

        true
    }

    /// Verifies the given DistanceVector.
    /// Returns a boolean indicating whether the DistanceVector was valid.
    /// If applicable, stores the advertised distances for forwarding.
    fn handle_distance_vector(
        &mut self,
        distance_vector: &network_protocol::DistanceVector,
    ) -> bool {
        // Basic sanity check; `distance_vector` should come from some other peer
        if self.config.node_id == distance_vector.root {
            return false;
        }

        // Validate the advertised distances against the accompanying spanning tree
        let validated_distances = self.validate_routing_distances(distance_vector);

        let is_valid = validated_distances.is_some();

        let stored = match validated_distances {
            Some(distances) => self.store_validated_peer_distances(&distance_vector, distances),
            None => false,
        };

        if !stored {
            // Free ids which may have been allocated to perform validation
            self.edge_cache.free_unused_ids();
        }

        return is_valid;
    }

    /// Handles disconnection of a peer.
    /// - Updates the state of `local_edges`.
    /// - Erases the peer's latest spanning tree, if there is one, from `edge_cache`.
    /// - Erases the advertised distances for the peer.
    pub(crate) fn remove_direct_peer(&mut self, peer_id: &PeerId) {
        if let Some(edge) = self.local_edges.get_mut(peer_id) {
            // TODO(saketh): refactor Edge once the old routing protocol is deprecated
            if edge.edge_type() != EdgeState::Removed {
                let (peer0, peer1) = edge.key().clone();
                // V2 routing protocol doesn't broadcast tombstones; don't bother to sign them
                *edge = Edge::make_fake_edge(peer0, peer1, edge.nonce() + 1);
            }
            assert!(edge.edge_type() == EdgeState::Removed);
        }

        self.edge_cache.remove_tree(peer_id);
        self.peer_distances.remove(peer_id);
    }

    /// Handles connection of a new peer or nonce refresh for an existing one.
    /// - Updates the state of `local_edges`.
    /// - Adds or updates the nonce in the `edge_cache`.
    /// - If we don't already have a DistanceVector for this peer, initializes one.
    pub(crate) fn add_or_update_direct_peer(&mut self, peer_id: PeerId, edge: Edge) -> bool {
        assert_eq!(edge.edge_type(), EdgeState::Active);

        // We have this nonce or a newer one already; ignore the update entirely
        if self.edge_cache.has_edge_nonce_or_newer(&edge) {
            return true;
        }

        // Reject invalid edge
        if !self.verify_and_cache_edge_nonces(&vec![edge.clone()]) {
            return false;
        }

        // Update the state of `local_edges`
        self.local_edges.insert(peer_id.clone(), edge.clone());

        // If we don't already have a DistanceVector received from this peer,
        // create one for it and process it as if we received it
        if !self.peer_distances.contains_key(&peer_id) {
            self.handle_distance_vector(&network_protocol::DistanceVector {
                root: peer_id.clone(),
                distances: vec![
                    // The peer has distance 0 to itself
                    AdvertisedPeerDistance { destination: peer_id, distance: 0 },
                    // The peer is distance 1 from this node
                    AdvertisedPeerDistance {
                        destination: self.config.node_id.clone(),
                        distance: 1,
                    },
                ],
                edges: vec![edge],
            });
        }

        true
    }

    pub(crate) fn handle_network_change(
        &mut self,
        _clock: &time::Clock,
        update: &NetworkTopologyChange,
    ) -> bool {
        match update {
            NetworkTopologyChange::PeerConnected(peer_id, edge) => {
                self.add_or_update_direct_peer(peer_id.clone(), edge.clone())
            }
            NetworkTopologyChange::PeerDisconnected(peer_id) => {
                self.remove_direct_peer(peer_id);
                true
            }
            NetworkTopologyChange::PeerAdvertisedDistances(distance_vector) => {
                self.handle_distance_vector(distance_vector)
            }
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
        let local_node_id = self.edge_cache.get_local_node_id() as usize;

        // Calculate the min distance to each routable node
        let mut min_distance: Vec<Option<u32>> = vec![None; max_id];
        min_distance[local_node_id] = Some(0);
        for (_, entry) in &mut self.peer_distances {
            // The peer to id mapping in the edge_cache is dynamic. We can still use previous distance
            // calculations because a node incident to an active edge won't be relabelled. However,
            // we may need to resize the distance vector.
            entry.distance.resize(max_id, None);

            for id in 0..max_id {
                if let Some(peer_distance) = entry.distance[id] {
                    if !min_distance[id]
                        .is_some_and(|min_distance| min_distance <= peer_distance + 1)
                    {
                        min_distance[id] = Some(peer_distance + 1);
                    }
                }
            }
        }

        // Compute the next hop table
        let mut next_hops_by_id: Vec<Vec<PeerId>> = vec![vec![]; self.edge_cache.max_id()];
        for id in 0..max_id {
            if let Some(id_distance) = min_distance[id] {
                for (peer_id, entry) in &self.peer_distances {
                    if entry.distance[id]
                        .is_some_and(|peer_distance| peer_distance + 1 == id_distance)
                    {
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
            if let Some(peer_distance) = min_distance[*id as usize] {
                distance.insert(peer_id.clone(), peer_distance);
            }
        }

        (next_hops, distance)
    }

    /// Each DistanceVector advertised by a peer includes a collection of edges
    /// used to validate the advertised distances.
    ///
    /// Edges are timestamped when signed and we consider them to be expired
    /// once a duration of `self.config.prune_edges_after` has passed.
    ///
    /// This function checks `peer_distances` for any DistanceVectors containing
    /// expired edges. Any such DistanceVectors are removed in their entirety.
    ///
    /// Also removes old edges from `local_edges` and from the EdgeCache.
    fn prune_expired_peer_distances(&mut self, clock: &time::Clock) {
        if let Some(prune_edges_after) = self.config.prune_edges_after {
            let prune_nonces_older_than =
                (clock.now_utc() - prune_edges_after).unix_timestamp() as u64;

            let peers_to_remove: Vec<PeerId> = self
                .peer_distances
                .iter()
                .filter_map(|(peer, entry)| {
                    if entry.min_nonce < prune_nonces_older_than {
                        Some(peer.clone())
                    } else {
                        None
                    }
                })
                .collect();

            for peer_id in &peers_to_remove {
                self.remove_direct_peer(peer_id);
            }

            self.local_edges.retain(|_, edge| edge.nonce() >= prune_nonces_older_than);

            self.edge_cache.prune_old_edges(prune_nonces_older_than);
        }
    }

    /// Constructs an instance of network_protocol::DistanceVector advertising the given distances.
    /// Returns None iff the `edge_cache` cannot construct a spanning tree achieving the distances.
    fn construct_distance_vector_message(
        &self,
        distances: &HashMap<PeerId, u32>,
    ) -> Option<network_protocol::DistanceVector> {
        Some(network_protocol::DistanceVector {
            root: self.config.node_id.clone(),
            // Collect distances for all known reachable nodes
            distances: distances
                .iter()
                .map(|(destination, distance)| AdvertisedPeerDistance {
                    destination: destination.clone(),
                    distance: *distance,
                })
                .collect(),
            // Construct a spanning tree of signed edges achieving the claimed distances
            edges: self.edge_cache.construct_spanning_tree(distances)?,
        })
    }

    /// Given the latest computed `distances`, updates `my_distances` and `my_distance_vector`.
    /// If distances have changed, returns a DistanceVector message to be broadcast to peers.
    fn update_distances(
        &mut self,
        distances: HashMap<PeerId, u32>,
    ) -> Option<network_protocol::DistanceVector> {
        if self.my_distances == distances {
            return None;
        }

        let distance_vector = self.construct_distance_vector_message(&distances)?;

        self.my_distances = distances;
        self.my_distance_vector = distance_vector;

        Some(self.my_distance_vector.clone())
    }

    /// Prunes expired peer distances, then recomputes the distances for the local node.
    /// Returns the recomputed NextHopTable.
    /// If distances have changed, returns an updated DistanceVector to be broadcast.
    pub(crate) fn compute_routes(
        &mut self,
        clock: &time::Clock,
        unreliable_peers: &HashSet<PeerId>,
    ) -> (NextHopTable, Option<network_protocol::DistanceVector>) {
        let _update_time = metrics::ROUTING_TABLE_RECALCULATION_HISTOGRAM.start_timer();

        // First prune any peer distances which have expired
        self.prune_expired_peer_distances(&clock);

        // Recompute the NextHopTable
        let (next_hops, distances) = self.compute_next_hops(unreliable_peers);

        // Store the newly computed distances and construct a DistanceVector message for broadcast
        let to_broadcast = self.update_distances(distances);

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
            distances: vec![AdvertisedPeerDistance {
                destination: local_node.clone(),
                distance: 0,
            }],
            edges: vec![],
        };

        Self {
            routing_table: RoutingTableView::new(),
            inner: Arc::new(Mutex::new(Inner {
                config,
                edge_cache,
                local_edges: HashMap::new(),
                peer_distances: HashMap::new(),
                my_distances: HashMap::from([(local_node, 0)]),
                my_distance_vector,
            })),
            unreliable_peers: ArcSwap::default(),
            runtime: Runtime::new(),
        }
    }

    pub fn set_unreliable_peers(&self, unreliable_peers: HashSet<PeerId>) {
        self.unreliable_peers.store(Arc::new(unreliable_peers));
    }

    /// Accepts and processes a batch of NetworkTopologyChanges.
    /// Each update is verified and, if valid, the advertised distances are stored.
    /// After all updates are processed, recomputes the local node's next hop table.
    ///
    /// May return a new DistanceVector for the local node, to be broadcasted to peers.
    /// Does so iff routing distances have changed due to the processed updates.
    ///
    /// Returns (distance_vector, oks) where
    /// * distance_vector is an Option<DistanceVector> to be broadcasted
    /// * oks.len() == distance_vectors.len() and oks[i] is true iff distance_vectors[i] was valid
    pub async fn batch_process_network_changes(
        self: &Arc<Self>,
        clock: &time::Clock,
        updates: Vec<NetworkTopologyChange>,
    ) -> (Option<network_protocol::DistanceVector>, Vec<bool>) {
        // TODO(saketh): Consider whether we can move this to rayon.
        let this = self.clone();
        let clock = clock.clone();
        self.runtime
            .handle
            .spawn_blocking(move || {
                let mut inner = this.inner.lock();

                let oks = updates
                    .iter()
                    .map(|update| inner.handle_network_change(&clock, update))
                    .collect();

                let (next_hops, to_broadcast) =
                    inner.compute_routes(&clock, &this.unreliable_peers.load());

                this.routing_table.update(next_hops.into());

                (to_broadcast, oks)
            })
            .await
            .unwrap()
    }

    pub(crate) fn get_debug_view(&self) -> NetworkRoutesView {
        let inner = self.inner.lock();
        NetworkRoutesView {
            edge_cache: inner.edge_cache.get_debug_view(),
            local_edges: inner
                .local_edges
                .iter()
                .map(|(peer_id, edge)| {
                    let (peer0, peer1) = edge.key().clone();
                    (peer_id.clone(), EdgeView { peer0, peer1, nonce: edge.nonce() })
                })
                .collect(),
            peer_distances: inner
                .peer_distances
                .iter()
                .map(|(peer_id, routes)| {
                    (
                        peer_id.clone(),
                        PeerDistancesView {
                            distance: routes.distance.clone(),
                            min_nonce: routes.min_nonce,
                        },
                    )
                })
                .collect(),
            my_distances: inner.my_distances.clone(),
        }
    }
}

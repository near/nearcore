use crate::network_protocol::Edge;
use near_primitives::network::PeerId;
use near_primitives::views::{EdgeCacheView, LabeledEdgeView};
use std::collections::hash_map::{Entry, Iter};
use std::collections::{HashMap, HashSet};

#[cfg(test)]
mod testonly;
#[cfg(test)]
mod tests;

// Connections in the network are bi-directional between a pair of peers (peer0, peer1).
// For keys in the EdgeCache, we maintain the invariant that peer0 < peer1
#[derive(Clone, Eq, Hash, PartialEq)]
pub(crate) struct EdgeKey {
    peer0: PeerId,
    peer1: PeerId,
}

impl From<&(PeerId, PeerId)> for EdgeKey {
    fn from(peers: &(PeerId, PeerId)) -> EdgeKey {
        let (peer0, peer1) = peers.clone();
        if peer0 < peer1 {
            Self { peer0, peer1 }
        } else {
            Self { peer1, peer0 }
        }
    }
}

/// The EdgeCache stores the latest spanning tree shared by each direct peer of the local node.
/// The trees are stored in `active_trees` as lists of EdgeKeys.
/// A separate map `active_edges` is kept mapping EdgeKeys to complete signed Edge objects.
/// This struct is used to store a signed Edge object along with a `refcount`; the number of
/// spanning trees which contain the edge.
#[derive(Clone)]
struct ActiveEdge {
    edge: Edge,
    refcount: u32,
}

/// Cache of all known edges in the network.
///
/// Edges in the network come to us in the form of signed tuples
///     (PeerId, PeerId, nonce: u64)
/// The two nodes connected by the edge both sign the tuple as proof of the connection's
/// existence. The nonce is a unix timestamp, letting us know the time at which the
/// connection was signed.
///
/// We maintain multiple representations of the network, each serving different purposes.
///
/// 1) `verified_nonces`:
/// A mapping from (PeerId, PeerId) to the latest nonce we have verified ourselves
/// for that pair of nodes. It allows the local node to avoid repeating computationally
/// expensive cryptographic verification of signatures.
///
/// Storing only the verified nonce (and not the actual Edge object with signatures)
/// is a memory optimization. We can trust existence of these edges as we have seen and
/// verified the signatures locally at some point, but we cannot provide trustless proof
/// to a peer that these edges exist in the network.
///
/// 2) `active_edges`
/// A mapping from (PeerId, PeerId) to complete Edge objects. It does not contain all known
/// edges, but rather a subset which the local node may wish to subsequently re-broadcast
/// to peers in the network.
///
/// In particular, for each direct peer of the local node, the set of edges appearing in the
/// most recent spanning tree advertised by the peer are kept in memory.
///
/// 3) `p2id`
/// A mapping from known PeerIds to distinct integer (u32) ids 0,1,2,...
/// The `p2id` mapping is used to allow indexing nodes into Vecs rather than HashMaps,
/// improving performance and reducing memory usage of the routing protocol implementation.
pub struct EdgeCache {
    verified_nonces: im::HashMap<EdgeKey, u64>,
    active_edges: im::HashMap<EdgeKey, ActiveEdge>,

    // Mapping from neighbor PeerId to the latest spanning tree advertised by the peer,
    // used to decide which edges are active. The key set of `active_edges` is the
    // union of the value set of `active_trees`.
    active_trees: HashMap<PeerId, Vec<EdgeKey>>,

    /// Mapping from PeerId to assigned u32 id
    p2id: HashMap<PeerId, u32>,
    /// Mapping from u32 id to the number of distinct active edges for the node
    degree: Vec<u32>,
    /// List of unused u32 ids
    unused: Vec<u32>,
}

impl EdgeCache {
    pub fn new(local_node_id: PeerId) -> Self {
        // Initializes the EdgeCache assigning id 0 to the local node
        Self {
            verified_nonces: Default::default(),
            active_edges: Default::default(),
            active_trees: HashMap::new(),
            p2id: HashMap::from([(local_node_id, 0)]),
            degree: vec![0],
            unused: vec![],
        }
    }

    /// Accepts a verified edge and updates the state of `verified_nonces`.
    pub fn write_verified_nonce(&mut self, edge: &Edge) {
        self.verified_nonces.insert(edge.key().into(), edge.nonce());
    }

    /// Accepts an edge. Returns true iff we already have a verified nonce
    /// for the edge's key which is at least as new as the edge's nonce.
    pub fn has_edge_nonce_or_newer(&self, edge: &Edge) -> bool {
        self.verified_nonces
            .get(&edge.key().into())
            .map_or(false, |cached_nonce| cached_nonce >= &edge.nonce())
    }

    /// Returns the u32 id associated with the given PeerId.
    /// Expects that an id was already assigned; will error otherwise.
    pub(crate) fn get_id(&self, peer: &PeerId) -> u32 {
        *self.p2id.get(peer).unwrap()
    }

    /// Id 0 is always assigned to the local node.
    pub(crate) fn get_local_node_id(&self) -> u32 {
        0
    }

    /// Returns the u32 id associated with the given PeerId, assigning one if necessary.
    pub(crate) fn get_or_create_id(&mut self, peer: &PeerId) -> u32 {
        match self.p2id.entry(peer.clone()) {
            Entry::Occupied(occupied) => *occupied.get(),
            Entry::Vacant(vacant) => {
                let id = if let Some(id) = self.unused.pop() {
                    // If some unused id is available, take it
                    assert!(self.degree[id as usize] == 0);
                    id
                } else {
                    // Otherwise, create a new one
                    let id = self.degree.len() as u32;
                    self.degree.push(0);
                    id
                };

                vacant.insert(id);
                id
            }
        }
    }

    /// Iterates over all peers appearing in the given spanning tree,
    /// assigning ids to those which don't have one already.
    pub(crate) fn create_ids_for_tree(&mut self, root: &PeerId, edges: &Vec<Edge>) {
        self.get_or_create_id(root);

        edges.iter().for_each(|edge| {
            let (peer0, peer1) = edge.key();
            self.get_or_create_id(peer0);
            self.get_or_create_id(peer1);
        });
    }

    /// Checks for and frees any assigned ids which have degree 0.
    /// Id 0 remains assigned to the local node, regardless of degree.
    pub(crate) fn free_unused_ids(&mut self) {
        assert!(self.get_local_node_id() == 0);

        // Erase entries from the `p2id` map
        self.p2id.retain(|_, id| *id == 0 || self.degree[*id as usize] != 0);

        // Shrink max_id if possible
        let mut max_id = self.max_id();
        while max_id >= 2 && self.degree[max_id - 1] == 0 {
            max_id -= 1;
        }
        self.degree.truncate(max_id);

        // Reconstruct the list of unused ids
        self.unused.clear();
        for id in 1..self.max_id() {
            if self.degree[id] == 0 {
                self.unused.push(id as u32);
            }
        }
    }

    /// Called when storing an active edge.
    /// Increments the degrees for the connected peers, assigning ids if necessary.
    fn increment_degrees_for_key(&mut self, key: &EdgeKey) {
        let id0 = self.get_or_create_id(&key.peer0) as usize;
        let id1 = self.get_or_create_id(&key.peer1) as usize;
        self.degree[id0] += 1;
        self.degree[id1] += 1;
    }

    /// Called when erasing an active edge.
    /// Decrements the degrees for the connected peers.
    fn decrement_degrees_for_key(&mut self, key: &EdgeKey) {
        self.decrement_degree(&key.peer0);
        self.decrement_degree(&key.peer1);
    }

    /// Decrements the degree for the given peer.
    /// If the degree reaches 0, frees the peer's id for reuse.
    fn decrement_degree(&mut self, peer_id: &PeerId) {
        let id = self.get_id(peer_id) as usize;
        assert!(self.degree[id] > 0);
        self.degree[id] -= 1;

        // If the degree for the id reaches 0, free it.
        // The local node is always mapped to 0.
        if self.degree[id] == 0 && id != 0 {
            self.p2id.remove(peer_id);
            self.unused.push(id as u32);
        }
    }

    /// Inserts a copy of the given edge to `active_edges`.
    /// If it's the first copy, increments degrees for the incident nodes.
    fn insert_active_edge(&mut self, edge: &Edge) {
        let is_newly_active = match self.active_edges.entry(edge.key().into()) {
            im::hashmap::Entry::Occupied(mut occupied) => {
                let val: &mut ActiveEdge = occupied.get_mut();
                if edge.nonce() > val.edge.nonce() {
                    val.edge = edge.clone();
                }
                val.refcount += 1;
                false
            }
            im::hashmap::Entry::Vacant(vacant) => {
                vacant.insert(ActiveEdge { edge: edge.clone(), refcount: 1 });
                true
            }
        };
        if is_newly_active {
            self.increment_degrees_for_key(&edge.key().into());
        }
    }

    /// Removes an edge with the given EdgeKey from the active edge cache.
    /// If the last such edge is removed, decrements degrees for the incident nodes.
    fn remove_active_edge(&mut self, key: &EdgeKey) {
        let is_newly_inactive = match self.active_edges.entry(key.clone()) {
            im::hashmap::Entry::Occupied(mut occupied) => {
                let val: &mut ActiveEdge = occupied.get_mut();
                assert!(val.refcount > 0);
                if val.refcount == 1 {
                    occupied.remove_entry();
                    true
                } else {
                    val.refcount -= 1;
                    false
                }
            }
            im::hashmap::Entry::Vacant(_) => {
                assert!(false);
                false
            }
        };
        if is_newly_inactive {
            self.decrement_degrees_for_key(key);
        }
    }

    /// Stores the key-value pair (peer_id, edges) in the EdgeCache's `active_trees` map, overwriting
    /// any previous entry for the same peer. Updates `active_edges` accordingly.
    pub fn update_tree(&mut self, peer_id: &PeerId, tree: &Vec<Edge>) {
        // Insert the new edges before removing any old ones.
        // Nodes are pruned from the `p2id` mapping as soon as all edges incident with them are
        // removed. If we removed the edges in the old tree first, we might unlabel and relabel a
        // node unnecessarily. Inserting the new edges first minimizes churn on `p2id`.
        for edge in tree {
            self.insert_active_edge(edge);
        }

        let edge_keys: Vec<EdgeKey> = tree.iter().map(|edge| edge.key().into()).collect();

        if let Some(old_edge_keys) = self.active_trees.insert(peer_id.clone(), edge_keys) {
            // If a previous tree was present, process removal of its edges
            for key in &old_edge_keys {
                self.remove_active_edge(key);
            }
        }
    }

    /// Removes the tree stored for the given peer, if there is one.
    pub fn remove_tree(&mut self, peer_id: &PeerId) {
        if let Some(edges) = self.active_trees.remove(peer_id) {
            for e in &edges {
                self.remove_active_edge(e);
            }
        }
    }

    /// Upper bound on mapped u32 ids; not inclusive
    pub fn max_id(&self) -> usize {
        self.degree.len()
    }

    /// Iterator over the (PeerId, u32) mapping
    pub fn iter_peers(&self) -> Iter<'_, PeerId, u32> {
        self.p2id.iter()
    }

    /// Number of known edges in the network
    pub fn known_edges_ct(&self) -> usize {
        self.verified_nonces.len()
    }

    /// Prunes entries with nonces older than `prune_nonces_older_than`
    /// from `verified_nonces`
    pub fn prune_old_edges(&mut self, prune_nonces_older_than: u64) {
        // Drop any entries with old nonces from the verified_nonces cache
        self.verified_nonces.retain(|_, nonce| nonce >= &prune_nonces_older_than);
    }

    /// Accepts a mapping over the set of reachable PeerIds in the network
    /// to the shortest path lengths to those peers.
    ///
    /// Constructs a tree from among the `active_edges` which has the same
    /// reachability and the same distances or better.
    ///
    /// Returns None if the input is inconsistent with the state of the cache
    /// (reachability or distances are not consistent with the `active_edges`).
    pub fn construct_spanning_tree(&self, distance: &HashMap<PeerId, u32>) -> Option<Vec<Edge>> {
        let mut edges = Vec::<Edge>::new();
        let mut has_edge = HashSet::<PeerId>::new();

        // Make sure some node has distance 0
        let mut has_root = false;
        for (_, val) in distance {
            if *val == 0 {
                has_root = true;
            }
        }

        // Iterate through the known useful edges.
        // We want to find for each node in the tree some edge
        // which connects it to another node with smaller distance.
        for (edge_key, active_edge) in &self.active_edges {
            if let (Some(dist0), Some(dist1)) =
                (distance.get(&edge_key.peer0), distance.get(&edge_key.peer1))
            {
                if dist0 < dist1 && !has_edge.contains(&edge_key.peer1) {
                    has_edge.insert(edge_key.peer1.clone());
                    edges.push(active_edge.edge.clone());
                }

                if dist1 < dist0 && !has_edge.contains(&edge_key.peer0) {
                    has_edge.insert(edge_key.peer0.clone());
                    edges.push(active_edge.edge.clone());
                }
            }
        }

        if has_root && has_edge.len() + 1 == distance.len() {
            Some(edges)
        } else {
            None
        }
    }

    pub(crate) fn get_debug_view(&self) -> EdgeCacheView {
        EdgeCacheView {
            peer_labels: self.p2id.clone(),
            spanning_trees: self
                .active_trees
                .iter()
                .map(|(peer_id, edge_keys)| {
                    (
                        self.get_id(&peer_id),
                        edge_keys
                            .iter()
                            .map(|key| LabeledEdgeView {
                                peer0: self.get_id(&key.peer0),
                                peer1: self.get_id(&key.peer1),
                                nonce: *self.verified_nonces.get(&key).unwrap(),
                            })
                            .collect(),
                    )
                })
                .collect(),
        }
    }
}

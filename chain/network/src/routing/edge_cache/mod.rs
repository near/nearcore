use crate::network_protocol::Edge;
use near_primitives::network::PeerId;
use std::collections::hash_map::{Entry, Iter};
use std::collections::{HashMap, HashSet};

#[cfg(test)]
mod testonly;
#[cfg(test)]
mod tests;

// TODO: make it opaque, so that the key.0 < key.1 invariant is protected.
type EdgeKey = (PeerId, PeerId);

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

    /// Returns the u32 id associated with the given PeerId, assigning one if necessary.
    fn get_or_create_id(&mut self, peer: &PeerId) -> u32 {
        match self.p2id.entry(peer.clone()) {
            Entry::Occupied(occupied) => *occupied.get(),
            Entry::Vacant(vacant) => {
                let val = if let Some(val) = self.unused.pop() {
                    assert!(self.degree[val as usize] == 0);
                    val
                } else {
                    let val = self.degree.len() as u32;
                    self.degree.push(0);
                    val
                };

                vacant.insert(val);
                val
            }
        }
    }

    fn decrement_degree(&mut self, peer_id: &PeerId) {
        let id = self.get_or_create_id(peer_id);
        self.degree[id as usize] -= 1;

        // If this id is no longer incident with any active edges, free it
        // The local node should always have id 0, so it's never freed
        if self.degree[id as usize] == 0 && id != 0 {
            self.p2id.remove(peer_id);

            // TODO(saketh): Unused ids are simply kept in a list for reuse; `max_id` only ever
            // increases. It it is not a completely trivial concern because `max_id` influences
            // the amount of storage used by downstream consumers of the mapping (they are typically
            // allocating Vecs of length `max_id`).
            //
            // We should consider doing something like rebuilding the mapping from scratch if more
            // than half the allocated ids are freed. The downstream consumers would also need to
            // modify their data structures in such a situation, so we would need to think carefully
            // about how to implement it.
            self.unused.push(id);
        }
    }

    fn decrement_degrees_for_key(&mut self, key: &EdgeKey) {
        let (peer0, peer1) = key;
        self.decrement_degree(peer0);
        self.decrement_degree(peer1);
    }

    fn increment_degrees_for_key(&mut self, key: &EdgeKey) {
        let (peer0, peer1) = key;
        let id0 = self.get_or_create_id(peer0) as usize;
        let id1 = self.get_or_create_id(peer1) as usize;
        self.degree[id0] += 1;
        self.degree[id1] += 1;
    }

    /// Returns the u32 id associated with the given PeerId.
    /// Expects that such a mapping already exists; will error otherwise.
    pub(crate) fn get_id(&self, peer: &PeerId) -> u32 {
        *self.p2id.get(peer).unwrap()
    }

    /// Populates entries in the `p2id` mapping, if necessary, for the peers appearing among the
    /// given list of edges.
    pub(crate) fn create_ids_for_unmapped_peers(&mut self, edges: &Vec<Edge>) {
        edges.iter().for_each(|edge| {
            let (peer0, peer1) = edge.key();
            self.get_or_create_id(peer0);
            self.get_or_create_id(peer1);
        });
    }

    pub(crate) fn free_unused_ids(&mut self) {
        for id in 0..self.max_id() {
            if self.degree[id] == 0 {
                self.unused.push(id as u32);
            }
        }
    }

    /// Returns true iff we already verified a nonce for this key
    /// which is at least as new as the given one
    pub fn has_edge_nonce_or_newer(&self, edge: &Edge) -> bool {
        self.verified_nonces
            .get(&edge.key())
            .map_or(false, |cached_nonce| cached_nonce >= &edge.nonce())
    }

    pub fn write_verified_nonce(&mut self, edge: &Edge) {
        self.verified_nonces.insert(edge.key().clone(), edge.nonce());
    }

    /// Inserts a copy of the given edge to the active edge cache.
    /// If it's the first copy, increments degrees for the incident nodes.
    fn insert_active_edge(&mut self, edge: &Edge) {
        let key = edge.key();
        let is_newly_active = match self.active_edges.entry(key.clone()) {
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
            self.increment_degrees_for_key(key);
        }
    }

    /// Removes an edge with the given EdgeKey from the active edge cache.
    /// If the last such edge is removed, decrements degrees for the incident nodes.
    fn remove_active_edge(&mut self, key: &EdgeKey) {
        let is_newly_inactive = match self.active_edges.entry(key.clone()) {
            im::hashmap::Entry::Occupied(mut occupied) => {
                let val: &mut ActiveEdge = occupied.get_mut();
                if val.refcount <= 1 {
                    occupied.remove_entry();
                    true
                } else {
                    val.refcount -= 1;
                    false
                }
            }
            im::hashmap::Entry::Vacant(_) => false,
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

        let edge_keys: Vec<EdgeKey> = tree.iter().map(|edge| edge.key()).cloned().collect();

        // If a previous tree was present, process removal of its edges
        if let Some(old_edge_keys) = self.active_trees.insert(peer_id.clone(), edge_keys) {
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
    /// from both `verified_nonces` and `active_edges`
    pub fn prune_old_edges(&mut self, prune_nonces_older_than: u64) {
        // Drop any entries with old nonces from the verified_nonces cache
        self.verified_nonces.retain(|_, nonce| nonce >= &prune_nonces_older_than);

        // Drop edges with old nonces from the active_edges cache
        let mut pruned_keys = vec![];
        self.active_edges.retain(|key, e| {
            if e.edge.nonce() < prune_nonces_older_than {
                pruned_keys.push(key.clone());
                return false;
            }
            return true;
        });

        for key in pruned_keys {
            self.decrement_degrees_for_key(&key);
        }
    }

    /// Accepts a mapping from the set of reachable PeerIds in the network
    /// to the shortest path lengths to those peers.
    ///
    /// Constructs a tree from among the `active_edges` which has the same
    /// reachability and the same distances or better.
    ///
    /// May error if the input is incorrect (reachability or distances are
    /// not consistent with the `active_edge` set stored in the cache).
    pub fn construct_spanning_tree(&self, distance: &HashMap<PeerId, u32>) -> Vec<Edge> {
        let mut edges = Vec::<Edge>::new();
        let mut has_edge = HashSet::<PeerId>::new();

        // Iterate through the known useful edges.
        // We want to find for each node in the tree some edge
        // which connects it to another node with smaller distance.
        for (edge_key, active_edge) in &self.active_edges {
            let (peer0, peer1) = edge_key;
            if let (Some(dist0), Some(dist1)) = (distance.get(peer0), distance.get(peer1)) {
                if dist0 < dist1 && !has_edge.contains(peer1) {
                    has_edge.insert(peer1.clone());
                    edges.push(active_edge.edge.clone());
                }

                if dist1 < dist0 && !has_edge.contains(peer0) {
                    has_edge.insert(peer0.clone());
                    edges.push(active_edge.edge.clone());
                }
            }
        }

        assert!(has_edge.len() + 1 == distance.len());
        edges
    }
}

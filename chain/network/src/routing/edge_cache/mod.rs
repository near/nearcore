use crate::network_protocol::Edge;
use near_primitives::network::PeerId;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

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
/// It maintains multiple representations of the network, each serving different purposes.
///
/// 1) `verified_nonces`:
/// A mapping from (PeerId, PeerId) to the latest locally verified nonce for that pair of
/// nodes. It allows the local node to avoid repeating computationally expensive
/// cryptographic verification of signatures on signed edges.
///
/// Storing only the verified nonce (and not the actual Edge object with signatures)
/// is a memory optimization. We can trust existence of these edges as we have seen and
/// verified the signatures locally at some point, but we cannot provide proof to a peer
/// that they exist.
///
/// 2) `active_edges`
/// A mapping from (PeerId, PeerId) to complete Edge objects. It does not contain all known
/// edges, but rather a subset which the local node may wish to subsequently re-broadcast
/// to peers in the network.
///
/// In particular, for each direct peer of the local node, the set of edges appearing in the
/// most recent ShortestPathTree advertised by the peer are kept in memory. These objects
/// are deduplicated across all peers of the local node.
///
/// 3) `p2id`
/// A mapping from known PeerIds to distinct integer (u32) ids 0,1,2,...
/// The set of mapped PeerIds is precisely those which appear at least once among the
/// `active_edges`.
///
/// The `p2id` mapping is used to allow routing computations to be performed over Vecs
/// rather than over HashMaps, improving performance and reducing memory usage of the
/// routing protocol implementation.
pub struct EdgeCache {
    verified_nonces: im::HashMap<EdgeKey, u64>,
    active_edges: im::HashMap<EdgeKey, ActiveEdge>,

    /// Mapping from PeerId to assigned u32 id
    pub(crate) p2id: HashMap<PeerId, u32>,
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
    pub fn get_id(&self, peer: &PeerId) -> u32 {
        *self.p2id.get(peer).unwrap()
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
    pub fn insert_active_edge(&mut self, edge: &Edge) {
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
    pub fn remove_active_edge(&mut self, key: &EdgeKey) {
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

    /// Upper bound on mapped u32 ids; not inclusive
    pub fn max_id(&self) -> usize {
        self.degree.len()
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
}

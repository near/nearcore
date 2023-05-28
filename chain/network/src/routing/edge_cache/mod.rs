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
///
/// For each known EdgeKey, we store within `verified_nonces` the most recent nonce which was
/// already verified locally. Verification of signatures is computationally expensive;
/// the `verified_nonces` cache is used to avoid unnecessary re-verification. Storing only
/// the verified nonce (and not the actual signatures) is a memory optimization. We can trust
/// existence of these edges as we have seen the signatures at some point, but we cannot
/// provide proof to a peer that they exist.
///
/// Within `active_edges`, we maintain a subset of edges which the local node wishes to keep
/// fully in memory. The entire Edge object is kept so that we may subsequently re-broadcast
/// it to peers in the network.
///
/// Within `p2id`, we maintain a mapping from PeerIds in the network to u32 ids 0,1,2,...
/// The set of mapped PeerIds is precisely those which appear at least once among the
/// set of active edges. This mapping of PeerIds to u32s is used to allow routing computations
/// to be performed over Vecs rather than HashMaps, improving performance and reducing memory
/// usage of the routing protocol implementation.
///
/// In total, EdgeCache serves three purposes:
/// 1) It is used to avoid repeating verification of signatures on edges.
/// 2) It stores Edge objects which the local node may re-broadcast to peers.
/// 3) It maintains a mapping from PeerIds of interest to u32 ids 0,1,2,...
pub struct EdgeCache {
    /// Mapping from EdgeKey to the most recent verified nonce
    verified_nonces: im::HashMap<EdgeKey, u64>,

    /// Mapping from EdgeKey to Edge object
    active_edges: im::HashMap<EdgeKey, ActiveEdge>,

    /// Mapping from PeerId to assigned u32 id
    p2id: HashMap<PeerId, u32>,
    /// Mapping from u32 id to the number of distinct active edges for the node
    degree: Vec<u32>,
    /// List of unused u32 ids
    unused: Vec<u32>,
}

impl EdgeCache {
    pub fn new() -> Self {
        Self {
            verified_nonces: Default::default(),
            active_edges: Default::default(),
            p2id: HashMap::new(),
            degree: vec![],
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
        if self.degree[id as usize] == 0 {
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
    pub fn _get_id(&self, peer: &PeerId) -> u32 {
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

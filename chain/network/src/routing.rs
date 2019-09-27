use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use byteorder::WriteBytesExt;
use bytes::LittleEndian;
use log::debug;

use near_crypto::Signature;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::types::AccountId;
use near_primitives::unwrap_obj_or_return;

use crate::types::PeerId;
use borsh::{BorshDeserialize, BorshSerialize};

/// Information that will be ultimately used to create a new edge.
/// It contains nonce proposed for the edge with signature from peer.
#[derive(Clone, BorshSerialize, BorshDeserialize)]
pub struct EdgeInfo {
    pub nonce: u64,
    pub signature: Signature,
}

/// Status of the edge
#[derive(Clone, PartialEq, Eq, Debug)]
enum EdgeType {
    Added,
    Removed,
}

impl EdgeType {
    fn value(&self) -> u8 {
        match self {
            EdgeType::Added => 0,
            EdgeType::Removed => 1,
        }
    }
}

/// Edge object. Contains information relative to a new edge that is being added or removed
/// from the network. This is the information that is required
#[derive(Clone, Debug)]
pub struct Edge {
    /// Since edges are not directed `peer0 < peer1` should hold.
    peer0: PeerId,
    peer1: PeerId,
    /// Nonce to keep tracking of the last update on this edge.
    nonce: u64,
    /// Specify status of the edge in the last update.
    edge_type: EdgeType,
    /// Signature from parties validating the edge.
    /// If the edge_type is Removed, only one signature is required.
    signature0: Option<Signature>,
    signature1: Option<Signature>,
}

impl Edge {
    fn hash(peer0: &PeerId, peer1: &PeerId, nonce: u64, edge_type: &EdgeType) -> CryptoHash {
        let mut buffer = Vec::<u8>::new();
        let peer0: Vec<u8> = peer0.clone().into();
        buffer.extend_from_slice(peer0.as_slice());
        let peer1: Vec<u8> = peer1.clone().into();
        buffer.extend_from_slice(peer1.as_slice());
        buffer.write_u64::<LittleEndian>(nonce).unwrap();
        buffer.push(edge_type.value());
        hash(buffer.as_slice())
    }

    fn verify(&self) -> bool {
        if self.peer0 > self.peer1 {
            return false;
        }

        let data = Edge::hash(&self.peer0, &self.peer1, self.nonce, &self.edge_type);

        match self.edge_type {
            EdgeType::Added => {
                let signature0 = unwrap_obj_or_return!(&self.signature0, false);
                let signature1 = unwrap_obj_or_return!(&self.signature1, false);

                signature0.verify(data.as_ref(), &self.peer0.public_key())
                    && signature1.verify(data.as_ref(), &self.peer1.public_key())
            }
            EdgeType::Removed => {
                // Only one edge needs to be available for edge removed
                if let Some(signature0) = &self.signature0 {
                    return signature0.verify(data.as_ref(), &self.peer0.public_key());
                }
                if let Some(signature1) = &self.signature1 {
                    return signature1.verify(data.as_ref(), &self.peer1.public_key());
                }
                false
            }
        }
    }

    pub fn key(peer0: PeerId, peer1: PeerId) -> (PeerId, PeerId) {
        if peer0 < peer1 {
            (peer0, peer1)
        } else {
            (peer1, peer0)
        }
    }

    /// Helper function when adding a new edge and we receive information from new potential peer
    /// to verify the signature.
    pub fn partial_verify(peer0: PeerId, peer1: PeerId, edge_info: EdgeInfo) -> bool {
        let pk = peer1.public_key();
        let (peer0, peer1) = Edge::key(peer0, peer1);
        let data = Edge::hash(&peer0, &peer1, edge_info.nonce, &EdgeType::Added);
        edge_info.signature.verify(data.as_ref(), &pk)
    }

    fn get_pair(&self) -> (PeerId, PeerId) {
        (self.peer0.clone(), self.peer1.clone())
    }
}

#[derive(Clone)]
pub struct RoutingTable {
    // TODO(MarX): Use cache and file storing to keep this information.
    /// PeerId associated for every known account id.
    pub account_peers: HashMap<AccountId, PeerId>,
    /// Active PeerId that are part of the shortest path to each PeerId.
    pub peer_forwarding: HashMap<PeerId, HashSet<PeerId>>,
    /// Store last update for known edges.
    pub edges_info: HashMap<(PeerId, PeerId), Edge>,
    /// Current view of the network. Nodes are Peers and edges are active connections.
    raw_graph: Graph,
}

#[derive(Debug)]
pub enum FindRouteError {
    Disconnected,
    PeerNotFound,
    AccountNotFound,
}

impl RoutingTable {
    pub fn new(peer_id: PeerId) -> Self {
        Self {
            account_peers: HashMap::new(),
            peer_forwarding: HashMap::new(),
            edges_info: HashMap::new(),
            raw_graph: Graph::new(peer_id),
        }
    }

    /// Find peer that is connected to `source` and belong to the shortest path
    /// from `source` to `peer_id`.
    pub fn find_route(&self, peer_id: &PeerId) -> Result<PeerId, FindRouteError> {
        if let Some(routes) = self.peer_forwarding.get(&peer_id) {
            if routes.is_empty() {
                Err(FindRouteError::Disconnected)
            } else {
                // TODO(MarX): Do Round Robin
                Ok(routes.iter().next().unwrap().clone())
            }
        } else {
            Err(FindRouteError::PeerNotFound)
        }
    }

    /// Find peer that owns this AccountId.
    pub fn account_owner(&self, account_id: &AccountId) -> Result<PeerId, FindRouteError> {
        self.account_peers.get(account_id).cloned().ok_or_else(|| FindRouteError::AccountNotFound)
    }

    pub fn add_peer(&mut self, peer_id: PeerId) {
        self.peer_forwarding.entry(peer_id).or_insert_with(HashSet::new);
    }

    /// Add (account id, peer id) to routing table.
    /// Returns a bool indicating whether this is a new entry or not.
    /// Note: There is at most on peer id per account id.
    pub fn add_account(&mut self, account_id: AccountId, peer_id: PeerId) -> bool {
        self.add_peer(peer_id.clone());

        match self.account_peers.entry(account_id) {
            Entry::Occupied(_) => false,
            Entry::Vacant(entry) => {
                entry.insert(peer_id);
                true
            }
        }
    }

    /// This edge is assumed to be valid at this point.
    /// Return true if the edge contains new information about the network. Old if this information
    /// is outdated.
    pub fn process_edge(&mut self, edge: Edge) -> bool {
        let key = edge.get_pair();

        if self.find_nonce(&key) >= edge.nonce {
            // We already have a newer information about this edge. Discard this information.
            debug!(target:"network", "Received outdated edge: {:?}", edge);
            return false;
        }

        match edge.edge_type {
            EdgeType::Added => {
                self.raw_graph.add_edge(key.0.clone(), key.1.clone());
            }
            EdgeType::Removed => {
                self.raw_graph.remove_edge(&key.0, &key.1);
            }
        }

        self.edges_info.insert(key, edge);
        // TODO(MarX): Don't recalculate all the time
        self.peer_forwarding = self.raw_graph.calculate_distance();
        true
    }

    pub fn sample_peers(&self) -> Vec<PeerId> {
        // TODO(MarX): Sample instead of reporting all peers
        self.peer_forwarding.keys().map(|key| key.clone()).collect()
    }

    pub fn find_nonce(&self, edge: &(PeerId, PeerId)) -> u64 {
        self.edges_info.get(&edge).map_or(0, |x| x.nonce)
    }
}

#[derive(Clone)]
pub struct Graph {
    pub source: PeerId,
    adjacency: HashMap<PeerId, HashSet<PeerId>>,
}

impl Graph {
    pub fn new(source: PeerId) -> Self {
        Self { source, adjacency: HashMap::new() }
    }

    fn contains_edge(&mut self, peer0: &PeerId, peer1: &PeerId) -> bool {
        if let Some(adj) = self.adjacency.get(&peer0) {
            if adj.contains(&peer1) {
                return true;
            }
        }

        false
    }

    fn add_directed_edge(&mut self, peer0: PeerId, peer1: PeerId) {
        self.adjacency.entry(peer0).or_insert_with(HashSet::new).insert(peer1);
    }

    fn remove_directed_edge(&mut self, peer0: &PeerId, peer1: &PeerId) {
        self.adjacency.get_mut(&peer0).unwrap().remove(&peer1);
    }

    pub fn add_edge(&mut self, peer0: PeerId, peer1: PeerId) {
        if !self.contains_edge(&peer0, &peer1) {
            self.add_directed_edge(peer0.clone(), peer1.clone());
            self.add_directed_edge(peer1, peer0);
        }
    }

    pub fn remove_edge(&mut self, peer0: &PeerId, peer1: &PeerId) {
        if self.contains_edge(&peer0, &peer1) {
            self.remove_directed_edge(&peer0, &peer1);
            self.remove_directed_edge(&peer1, &peer0);
        }
    }

    // TODO(MarX): This is too slow right now. (See benchmarks)
    /// Compute for every node `u` on the graph (other than `source`) which are the neighbors of
    /// `sources` which belong to the shortest path from `source` to `u`. Nodes that are
    /// not connected to `source` will not appear in the result.
    pub fn calculate_distance(&self) -> HashMap<PeerId, HashSet<PeerId>> {
        let mut queue = vec![];
        let mut distance = HashMap::new();
        // TODO(MarX): Represent routes more efficiently at least while calculating distances
        let mut routes: HashMap<PeerId, HashSet<PeerId>> = HashMap::new();

        distance.insert(&self.source, 0);

        // Add active connections
        if let Some(neighbors) = self.adjacency.get(&self.source) {
            for neighbor in neighbors {
                queue.push(neighbor);
                distance.insert(neighbor, 1);
                routes.insert(neighbor.clone(), vec![neighbor.clone()].drain(..).collect());
            }
        }

        let mut head = 0;

        while head < queue.len() {
            let cur_peer = queue[head];
            let cur_distance = *distance.get(cur_peer).unwrap();
            head += 1;

            if let Some(neighbors) = self.adjacency.get(&cur_peer) {
                for neighbor in neighbors {
                    if !distance.contains_key(&neighbor) {
                        queue.push(neighbor);
                        distance.insert(neighbor, cur_distance + 1);
                        routes.insert(neighbor.clone(), HashSet::new());
                    }

                    // If this edge belong to a shortest path, all paths to
                    // the closer nodes are also valid for the current node.
                    if *distance.get(neighbor).unwrap() == cur_distance + 1 {
                        let adding_routes = routes.get(cur_peer).unwrap().clone();
                        let target_routes = routes.get_mut(neighbor).unwrap();

                        for route in adding_routes {
                            target_routes.insert(route.clone());
                        }
                    }
                }
            }
        }

        routes
    }
}

#[cfg(test)]
mod test {
    use std::collections::{HashMap, HashSet};

    use near_crypto::{KeyType, PublicKey, SecretKey};

    use crate::routing::Graph;
    use crate::test_utils::{expected_routing_tables, random_peer_id};
    use crate::types::PeerId;

    #[test]
    fn graph_contains_edge() {
        let source = random_peer_id();

        let node0 = random_peer_id();
        let node1 = random_peer_id();

        let mut graph = Graph::new(source.clone());

        assert_eq!(graph.contains_edge(&source, &node0), false);
        assert_eq!(graph.contains_edge(&source, &node1), false);
        assert_eq!(graph.contains_edge(&node0, &node1), false);
        assert_eq!(graph.contains_edge(&node1, &node0), false);

        graph.add_edge(node0.clone(), node1.clone());

        assert_eq!(graph.contains_edge(&source, &node0), false);
        assert_eq!(graph.contains_edge(&source, &node1), false);
        assert_eq!(graph.contains_edge(&node0, &node1), true);
        assert_eq!(graph.contains_edge(&node1, &node0), true);

        graph.remove_edge(&node1, &node0);

        assert_eq!(graph.contains_edge(&node0, &node1), false);
        assert_eq!(graph.contains_edge(&node1, &node0), false);
    }

    #[test]
    fn graph_distance0() {
        let source = random_peer_id();
        let node0 = random_peer_id();

        let mut graph = Graph::new(source.clone());
        graph.add_edge(source.clone(), node0.clone());

        assert!(expected_routing_tables(
            graph.calculate_distance(),
            vec![(node0.clone(), vec![node0.clone()])],
        ));
    }

    #[test]
    fn graph_distance1() {
        let source = random_peer_id();
        let nodes: Vec<_> = (0..3).map(|_| random_peer_id()).collect();

        let mut graph = Graph::new(source.clone());

        graph.add_edge(nodes[0].clone(), nodes[1].clone());
        graph.add_edge(nodes[2].clone(), nodes[1].clone());
        graph.add_edge(nodes[1].clone(), nodes[2].clone());

        assert!(expected_routing_tables(graph.calculate_distance(), vec![]));
    }

    #[test]
    fn graph_distance2() {
        let source = random_peer_id();
        let nodes: Vec<_> = (0..3).map(|_| random_peer_id()).collect();

        let mut graph = Graph::new(source.clone());

        graph.add_edge(nodes[0].clone(), nodes[1].clone());
        graph.add_edge(nodes[2].clone(), nodes[1].clone());
        graph.add_edge(nodes[1].clone(), nodes[2].clone());
        graph.add_edge(source.clone(), nodes[0].clone());

        assert!(expected_routing_tables(
            graph.calculate_distance(),
            vec![
                (nodes[0].clone(), vec![nodes[0].clone()]),
                (nodes[1].clone(), vec![nodes[0].clone()]),
                (nodes[2].clone(), vec![nodes[0].clone()]),
            ],
        ));
    }

    #[test]
    fn graph_distance3() {
        let source = random_peer_id();
        let nodes: Vec<_> = (0..3).map(|_| random_peer_id()).collect();

        let mut graph = Graph::new(source.clone());

        graph.add_edge(nodes[0].clone(), nodes[1].clone());
        graph.add_edge(nodes[2].clone(), nodes[1].clone());
        graph.add_edge(nodes[0].clone(), nodes[2].clone());
        graph.add_edge(source.clone(), nodes[0].clone());
        graph.add_edge(source.clone(), nodes[1].clone());

        assert!(expected_routing_tables(
            graph.calculate_distance(),
            vec![
                (nodes[0].clone(), vec![nodes[0].clone()]),
                (nodes[1].clone(), vec![nodes[1].clone()]),
                (nodes[2].clone(), vec![nodes[0].clone(), nodes[1].clone()]),
            ],
        ));
    }

    /// Test the following graph
    ///     0 - 3 - 6
    ///   /   x   x
    /// s - 1 - 4 - 7  
    ///   \   x   x
    ///     2 - 5 - 8
    ///
    ///    9 - 10 (Dummy edge disconnected)
    ///
    /// There is a shortest path to nodes [3..9) going through 0, 1, and 2.
    #[test]
    fn graph_distance4() {
        let source = random_peer_id();
        let nodes: Vec<_> = (0..11).map(|_| random_peer_id()).collect();

        let mut graph = Graph::new(source.clone());

        for i in 0..3 {
            graph.add_edge(source.clone(), nodes[i].clone());
        }

        for level in 0..2 {
            for i in 0..3 {
                for j in 0..3 {
                    graph.add_edge(nodes[level * 3 + i].clone(), nodes[level * 3 + 3 + j].clone());
                }
            }
        }

        // Dummy edge.
        graph.add_edge(nodes[9].clone(), nodes[10].clone());

        let mut next_hops: Vec<_> =
            (0..3).map(|i| (nodes[i].clone(), vec![nodes[i].clone()])).collect();
        let target: Vec<_> = (0..3).map(|i| nodes[i].clone()).collect();

        for i in 3..9 {
            next_hops.push((nodes[i].clone(), target.clone()));
        }

        assert!(expected_routing_tables(graph.calculate_distance(), next_hops));
    }
}

// TODO(MarX): Test graph is synced between nodes after starting connection

// TODO(MarX): Test graph is readjusted after edges are deleted

// TODO(MarX): Test routing (between peers / between validator)

// TODO(MarX): Test when one node joins late

// TODO(MarX): Test when one node quit gracefully/(non gracefully)

// TODO(MarX): Implement connection between validators/block producers.

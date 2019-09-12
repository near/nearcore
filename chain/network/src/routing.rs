use crate::peer::Peer;
use crate::types::{AnnounceAccount, PeerId};
use near_primitives::types::AccountId;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct RoutingTable {
    /// PeerId associated for every known account id.
    pub account_peers: HashMap<AccountId, PeerId>,
    /// Active PeerId that are part of the shortest path to each PeerId.
    pub peer_forwarding: HashMap<PeerId, HashSet<PeerId>>,
    /// Current view of the network. Nodes are Peers and edges are active connections.
    pub raw_graph: Graph,
}

enum FindRouteError {
    Disconnected,
    PeerNotFound,
    AccountNotFound,
}

impl RoutingTable {
    pub fn new(peer_id: PeerId) -> Self {
        Self {
            account_peers: HashMap::new(),
            peer_forwarding: HashMap::new(),
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

    /// Find peer that is connected to `source` and belong to the shortest path
    /// from `source` to peer associated with `account_id`.
    pub fn find_route_to_account(&self, account_id: &AccountId) -> Result<PeerId, FindRouteError> {
        if let Some(peer_id) = self.account_peers.get(&account_id) {
            self.find_route(&peer_id)
        } else {
            Err(FindRouteError::AccountNotFound)
        }
    }

    pub fn add_peer(&mut self, peer_id: PeerId) {
        self.peer_forwarding.entry(peer_id).or_insert_with(HashMap::new);
    }

    pub fn add_account(&mut self, account_id: AccountId, peer_id: PeerId) {
        self.add_peer(peer_id);
        self.account_peers.insert(account_id, peer_id);
    }

    pub fn add_connection(&mut self, peer0: PeerId, peer1: PeerId) {
        self.raw_graph.add_edge(peer0, peer1);
        // TODO(MarX): Don't recalculate all the time
        self.peer_forwarding = self.raw_graph.calculate_distance();
    }

    pub fn remove_connection(&mut self, peer0: &PeerId, peer1: &PeerId) {
        self.raw_graph.remove_edge(&peer0, &peer1);
        // TODO(MarX): Don't recalculate all the time
        self.peer_forwarding = self.raw_graph.calculate_distance();
    }

    pub fn register_neighbor(&mut self, peer: PeerId) {
        self.add_connection(self.raw_graph.source, peer);
    }

    pub fn unregister_neighbor(&mut self, peer: &PeerId) {
        self.remove_connection(&self.raw_graph.source, &peer);
    }

    pub fn sample_peers(&self) -> Vec<PeerId> {
        // TODO(MarX): Sample instead of reporting all peers
        self.peer_forwarding.keys().collect().clone()
    }
}

struct Graph {
    pub source: PeerId,
    adjacency: HashMap<PeerId, HashSet<PeerId>>,
}

impl Graph {
    fn new(source: PeerId) -> Self {
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
        self.adjacency.get(&peer0).unwrap().remove(&peer1);
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

    pub fn calculate_distance(&self) -> HashMap<PeerId, HashSet<PeerId>> {
        let mut queue = vec![];
        let mut distance = HashMap::new();
        // TODO(MarX): Represent routes more efficiently at least while calculating distances
        let mut routes = HashMap::new();

        distance.insert(&self.source, 0);
        routes.insert(self.source.clone(), HashSet::new());

        // Add active connections
        for neighbor in self.adjacency.get(&self.source).unwrap_or_else(HashSet::new) {
            queue.push(neighbor);
            distance.insert(neighbor, 1);
            routes.insert(neighbor.clone(), vec![neighbor.clone()].drain(..).collect());
        }

        let mut head = 0;

        while head < queue.len() {
            let cur_peer = queue[head];
            let cur_distance = distance.get(cur_peer).unwrap();
            head += 1;

            for neighbor in self.adjacency.get(&cur_peer).unwrap_or_else(HashSet::new) {
                if !distance.contains_key(&neighbor) {
                    queue.push(neighbor);
                    distance.insert(neighbor, cur_distance + 1);
                }

                if distance.get(neighbor).unwrap() == cur_distance + 1 {
                    let neighbor_routes = routes.get_mut(neighbor).unwrap();
                    for route in routes.get(cur_peer).unwrap() {
                        neighbor_routes.insert(route.clone());
                    }
                }
            }
        }

        routes
    }
}

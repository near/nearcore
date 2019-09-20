use crate::types::PeerId;
use near_primitives::types::AccountId;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

#[derive(Clone)]
pub struct RoutingTable {
    // TODO(MarX): Use cache and file storing to keep this information.
    /// PeerId associated for every known account id.
    pub account_peers: HashMap<AccountId, PeerId>,
    /// Active PeerId that are part of the shortest path to each PeerId.
    pub peer_forwarding: HashMap<PeerId, HashSet<PeerId>>,
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
        if let Some(peer_id) = self.account_peers.get(account_id) {
            Ok(peer_id.clone())
        } else {
            Err(FindRouteError::AccountNotFound)
        }
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

    pub fn add_connection(&mut self, peer0: PeerId, peer1: PeerId) {
        self.add_peer(peer0.clone());
        self.add_peer(peer1.clone());
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
        self.add_connection(self.raw_graph.source.clone(), peer);
    }

    pub fn unregister_neighbor(&mut self, peer: &PeerId) {
        let source = self.raw_graph.source.clone();
        self.remove_connection(&source, &peer);
    }

    pub fn sample_peers(&self) -> Vec<PeerId> {
        // TODO(MarX): Sample instead of reporting all peers
        self.peer_forwarding.keys().map(|key| key.clone()).collect()
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
    use crate::routing::Graph;
    use crate::test_utils::random_peer_id;
    use crate::types::PeerId;
    use near_crypto::{KeyType, PublicKey, SecretKey};
    use std::collections::{HashMap, HashSet};

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

    fn expected(current: HashMap<PeerId, HashSet<PeerId>>, expected: Vec<(PeerId, Vec<PeerId>)>) {
        assert_eq!(current.len(), expected.len());

        for (peer, paths) in expected.into_iter() {
            let cur_paths = current.get(&peer);
            assert!(cur_paths.is_some());
            let cur_paths = cur_paths.unwrap();
            assert_eq!(cur_paths.len(), paths.len());
            for next_hop in paths.into_iter() {
                assert!(cur_paths.contains(&next_hop));
            }
        }
    }

    #[test]
    fn graph_distance0() {
        let source = random_peer_id();
        let node0 = random_peer_id();

        let mut graph = Graph::new(source.clone());
        graph.add_edge(source.clone(), node0.clone());

        expected(graph.calculate_distance(), vec![(node0.clone(), vec![node0.clone()])]);
    }

    #[test]
    fn graph_distance1() {
        let source = random_peer_id();
        let nodes: Vec<_> = (0..3).map(|_| random_peer_id()).collect();

        let mut graph = Graph::new(source.clone());

        graph.add_edge(nodes[0].clone(), nodes[1].clone());
        graph.add_edge(nodes[2].clone(), nodes[1].clone());
        graph.add_edge(nodes[1].clone(), nodes[2].clone());

        expected(graph.calculate_distance(), vec![]);
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

        expected(
            graph.calculate_distance(),
            vec![
                (nodes[0].clone(), vec![nodes[0].clone()]),
                (nodes[1].clone(), vec![nodes[0].clone()]),
                (nodes[2].clone(), vec![nodes[0].clone()]),
            ],
        );
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

        expected(
            graph.calculate_distance(),
            vec![
                (nodes[0].clone(), vec![nodes[0].clone()]),
                (nodes[1].clone(), vec![nodes[1].clone()]),
                (nodes[2].clone(), vec![nodes[0].clone(), nodes[1].clone()]),
            ],
        );
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

        expected(graph.calculate_distance(), next_hops);
    }
}

// TODO(MarX): Test graph is synced between nodes after starting connection

// TODO(MarX): Test graph is readjusted after edges are deleted

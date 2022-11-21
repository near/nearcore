use crate::peer_manager::peer_manager_actor::MAX_TIER2_PEERS;
use near_primitives::network::PeerId;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use tracing::warn;

/// `Graph` is used to compute `peer_routing`, which contains information how to route messages to
/// all known peers. That is, for each `peer`, we get a sub-set of peers to which we are connected
/// to that are on the shortest path between us as destination `peer`.
#[derive(Clone)]
pub struct Graph {
    /// `id` as integer corresponding to `my_peer_id`.
    /// We use u32 to reduce both improve performance, and reduce memory usage.
    source_id: u32,
    /// Mapping from `PeerId` to `id`
    p2id: HashMap<PeerId, u32>,
    /// List of existing `PeerId`s
    id2p: Vec<PeerId>,
    /// Which ids are currently in use
    used: Vec<bool>,
    /// List of unused peer ids
    unused: Vec<u32>,
    /// Compressed adjacency table, we use 32 bit integer as ids instead of using full `PeerId`.
    /// This is undirected graph, we store edges in both directions.
    adjacency: Vec<Vec<u32>>,

    /// Total number of edges used for stats.
    total_active_edges: u64,
}

impl Graph {
    pub fn new(source: PeerId) -> Self {
        let mut res = Self {
            source_id: 0,
            p2id: HashMap::default(),
            id2p: Vec::default(),
            used: Vec::default(),
            unused: Vec::default(),
            adjacency: Vec::default(),
            total_active_edges: 0,
        };
        res.id2p.push(source.clone());
        res.adjacency.push(Vec::default());
        res.p2id.insert(source, res.source_id);
        res.used.push(true);

        res
    }

    pub fn total_active_edges(&self) -> u64 {
        self.total_active_edges
    }

    // Compute number of active edges. We divide by 2 to remove duplicates.
    #[cfg(test)]
    pub fn compute_total_active_edges(&self) -> u64 {
        let result: u64 = self.adjacency.iter().map(|x| x.len() as u64).sum();
        assert_eq!(result % 2, 0);
        result / 2
    }

    fn contains_edge(&self, peer0: &PeerId, peer1: &PeerId) -> bool {
        if let Some(&id0) = self.p2id.get(peer0) {
            if let Some(&id1) = self.p2id.get(peer1) {
                return self.adjacency[id0 as usize].contains(&id1);
            }
        }
        false
    }

    fn remove_if_unused(&mut self, id: u32) {
        let entry = &self.adjacency[id as usize];

        if entry.is_empty() && id != self.source_id {
            self.used[id as usize] = false;
            self.unused.push(id);
            self.p2id.remove(&self.id2p[id as usize]);
        }
    }

    fn get_id(&mut self, peer: &PeerId) -> u32 {
        match self.p2id.entry(peer.clone()) {
            Entry::Occupied(occupied) => *occupied.get(),
            Entry::Vacant(vacant) => {
                let val = if let Some(val) = self.unused.pop() {
                    assert!(!self.used[val as usize]);
                    assert!(self.adjacency[val as usize].is_empty());
                    self.id2p[val as usize] = peer.clone();
                    self.used[val as usize] = true;
                    val
                } else {
                    let val = self.id2p.len() as u32;
                    self.id2p.push(peer.clone());
                    self.used.push(true);
                    self.adjacency.push(Vec::default());
                    val
                };

                vacant.insert(val);
                val
            }
        }
    }

    pub fn add_edge(&mut self, peer0: &PeerId, peer1: &PeerId) {
        assert_ne!(peer0, peer1);
        if !self.contains_edge(peer0, peer1) {
            let id0 = self.get_id(peer0);
            let id1 = self.get_id(peer1);

            self.adjacency[id0 as usize].push(id1);
            self.adjacency[id1 as usize].push(id0);

            self.total_active_edges += 1;
        }
    }

    pub fn remove_edge(&mut self, peer0: &PeerId, peer1: &PeerId) {
        assert_ne!(peer0, peer1);
        if self.contains_edge(peer0, peer1) {
            let id0 = self.get_id(peer0);
            let id1 = self.get_id(peer1);

            self.adjacency[id0 as usize].retain(|&x| x != id1);
            self.adjacency[id1 as usize].retain(|&x| x != id0);

            self.remove_if_unused(id0);
            self.remove_if_unused(id1);

            self.total_active_edges -= 1;
        }
    }

    /// Compute for every node `u` on the graph (other than `source`) which are the neighbors of
    /// `sources` which belong to the shortest path from `source` to `u`. Nodes that are
    /// not connected to `source` will not appear in the result.
    pub fn calculate_distance(
        &self,
        unreliable_peers: &HashSet<PeerId>,
    ) -> HashMap<PeerId, Vec<PeerId>> {
        // TODO add removal of unreachable nodes

        let unreliable_peers: HashSet<_> =
            unreliable_peers.iter().filter_map(|peer_id| self.p2id.get(peer_id).cloned()).collect();

        let mut queue = VecDeque::new();

        let nodes = self.id2p.len();
        let mut distance: Vec<i32> = vec![-1; nodes];
        let mut routes: Vec<u128> = vec![0; nodes];

        distance[self.source_id as usize] = 0;

        {
            let neighbors = &self.adjacency[self.source_id as usize];
            for (id, &neighbor) in neighbors.iter().enumerate().take(MAX_TIER2_PEERS) {
                if !unreliable_peers.contains(&neighbor) {
                    queue.push_back(neighbor);
                }

                distance[neighbor as usize] = 1;
                routes[neighbor as usize] = 1u128 << id;
            }
        }

        while let Some(cur_peer) = queue.pop_front() {
            let cur_distance = distance[cur_peer as usize];

            for &neighbor in &self.adjacency[cur_peer as usize] {
                if distance[neighbor as usize] == -1 {
                    distance[neighbor as usize] = cur_distance + 1;
                    queue.push_back(neighbor);
                }
                // If this edge belong to a shortest path, all paths to
                // the closer nodes are also valid for the current node.
                if distance[neighbor as usize] == cur_distance + 1 {
                    routes[neighbor as usize] |= routes[cur_peer as usize];
                }
            }
        }

        // This takes 75% of the total time computation time of this function.
        self.compute_result(&routes, &distance)
    }

    /// Converts representation of the result, from an array representation, to
    /// a hashmap of PeerId -> Vec<PeerIds>
    /// Arguments:
    ///   - routes - for node given node at index `i`, give list of connected peers, which
    ///     are on the optimal path
    ///   - distances - not really needed: TODO remove this argument
    fn compute_result(&self, routes: &[u128], distance: &[i32]) -> HashMap<PeerId, Vec<PeerId>> {
        let mut res = HashMap::with_capacity(routes.len());

        let neighbors = &self.adjacency[self.source_id as usize];
        let mut unreachable_nodes = 0;

        for (key, &cur_route) in routes.iter().enumerate() {
            if distance[key] == -1 && self.used[key] {
                unreachable_nodes += 1;
            }
            if key as u32 == self.source_id
                || distance[key] == -1
                || cur_route == 0u128
                || !self.used[key]
            {
                continue;
            }
            // We convert list of peers, which are represented as bits
            // to a list of Vec<PeerId>
            // This is a bit wasteful representation, but that's ok.
            let peer_set = neighbors
                .iter()
                .enumerate()
                .take(MAX_TIER2_PEERS)
                .filter(|(id, _)| (cur_route & (1u128 << id)) != 0)
                .map(|(_, &neighbor)| self.id2p[neighbor as usize].clone())
                .collect();
            res.insert(self.id2p[key].clone(), peer_set);
        }
        if unreachable_nodes > 1000 {
            warn!("We store more than 1000 unreachable nodes: {}", unreachable_nodes);
        }
        res
    }
}

#[cfg(test)]
mod test {
    use super::Graph;
    use crate::test_utils::{expected_routing_tables, random_peer_id};
    use std::collections::HashSet;
    use std::ops::Not;

    #[test]
    fn graph_contains_edge() {
        let source = random_peer_id();

        let node0 = random_peer_id();
        let node1 = random_peer_id();

        let mut graph = Graph::new(source.clone());

        assert!(graph.contains_edge(&source, &node0).not());
        assert!(graph.contains_edge(&source, &node1).not());
        assert!(graph.contains_edge(&node0, &node1).not());
        assert!(graph.contains_edge(&node1, &node0).not());

        graph.add_edge(&node0, &node1);

        assert!(graph.contains_edge(&source, &node0).not());
        assert!(graph.contains_edge(&source, &node1).not());
        assert!(graph.contains_edge(&node0, &node1));
        assert!(graph.contains_edge(&node1, &node0));

        graph.remove_edge(&node1, &node0);

        assert!(graph.contains_edge(&node0, &node1).not());
        assert!(graph.contains_edge(&node1, &node0).not());

        assert_eq!(0, graph.total_active_edges() as usize);
        assert_eq!(0, graph.compute_total_active_edges() as usize);
    }

    #[test]
    fn graph_distance0() {
        let source = random_peer_id();
        let node0 = random_peer_id();

        let mut graph = Graph::new(source.clone());
        graph.add_edge(&source, &node0);
        graph.remove_edge(&source, &node0);
        graph.add_edge(&source, &node0);

        assert!(expected_routing_tables(
            &graph.calculate_distance(&HashSet::new()),
            &[(node0.clone(), vec![node0.clone()])],
        ));

        assert_eq!(1, graph.total_active_edges() as usize);
        assert_eq!(1, graph.compute_total_active_edges() as usize);
    }

    #[test]
    fn graph_distance1() {
        let source = random_peer_id();
        let nodes: Vec<_> = (0..3).map(|_| random_peer_id()).collect();

        let mut graph = Graph::new(source);

        graph.add_edge(&nodes[0], &nodes[1]);
        graph.add_edge(&nodes[2], &nodes[1]);
        graph.add_edge(&nodes[1], &nodes[2]);

        assert!(expected_routing_tables(&graph.calculate_distance(&HashSet::new()), &[]));

        assert_eq!(2, graph.total_active_edges() as usize);
        assert_eq!(2, graph.compute_total_active_edges() as usize);
    }

    #[test]
    fn graph_distance2() {
        let source = random_peer_id();
        let nodes: Vec<_> = (0..3).map(|_| random_peer_id()).collect();

        let mut graph = Graph::new(source.clone());

        graph.add_edge(&nodes[0], &nodes[1]);
        graph.add_edge(&nodes[2], &nodes[1]);
        graph.add_edge(&nodes[1], &nodes[2]);
        graph.add_edge(&source, &nodes[0]);

        assert!(expected_routing_tables(
            &graph.calculate_distance(&HashSet::new()),
            &[
                (nodes[0].clone(), vec![nodes[0].clone()]),
                (nodes[1].clone(), vec![nodes[0].clone()]),
                (nodes[2].clone(), vec![nodes[0].clone()]),
            ],
        ));

        assert_eq!(3, graph.total_active_edges() as usize);
        assert_eq!(3, graph.compute_total_active_edges() as usize);
    }
    #[test]
    fn graph_distance3() {
        let source = random_peer_id();
        let nodes: Vec<_> = (0..3).map(|_| random_peer_id()).collect();

        let mut graph = Graph::new(source.clone());

        graph.add_edge(&nodes[0], &nodes[1]);
        graph.add_edge(&nodes[2], &nodes[1]);
        graph.add_edge(&nodes[0], &nodes[2]);
        graph.add_edge(&source, &nodes[0]);
        graph.add_edge(&source, &nodes[1]);

        assert!(expected_routing_tables(
            &graph.calculate_distance(&HashSet::new()),
            &[
                (nodes[0].clone(), vec![nodes[0].clone()]),
                (nodes[1].clone(), vec![nodes[1].clone()]),
                (nodes[2].clone(), vec![nodes[0].clone(), nodes[1].clone()]),
            ],
        ));

        assert_eq!(5, graph.total_active_edges() as usize);
        assert_eq!(5, graph.compute_total_active_edges() as usize);
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

        for node in &nodes[0..3] {
            graph.add_edge(&source, node);
        }

        for level in 0..2 {
            for i in 0..3 {
                for j in 0..3 {
                    graph.add_edge(&nodes[level * 3 + i], &nodes[level * 3 + 3 + j]);
                }
            }
        }

        // Dummy edge.
        graph.add_edge(&nodes[9], &nodes[10]);

        let mut next_hops: Vec<_> =
            (0..3).map(|i| (nodes[i].clone(), vec![nodes[i].clone()])).collect();
        let target: Vec<_> = (0..3).map(|i| nodes[i].clone()).collect();

        for node in &nodes[3..9] {
            next_hops.push((node.clone(), target.clone()));
        }

        assert!(expected_routing_tables(&graph.calculate_distance(&HashSet::new()), &next_hops));

        assert_eq!(22, graph.total_active_edges() as usize);
        assert_eq!(22, graph.compute_total_active_edges() as usize);
    }

    // Same test as above, but node 0 is marked as bad.
    // So it can be used only for its own messages.
    #[test]
    fn graph_distance4_with_unreliable_nodes() {
        let source = random_peer_id();
        let nodes: Vec<_> = (0..11).map(|_| random_peer_id()).collect();

        let mut graph = Graph::new(source.clone());

        for node in &nodes[0..3] {
            graph.add_edge(&source, node);
        }

        for level in 0..2 {
            for i in 0..3 {
                for j in 0..3 {
                    graph.add_edge(&nodes[level * 3 + i], &nodes[level * 3 + 3 + j]);
                }
            }
        }

        // Dummy edge.
        graph.add_edge(&nodes[9], &nodes[10]);
        let unreliable_peers = HashSet::from([nodes[0].clone()]);

        let mut next_hops: Vec<_> =
            (0..3).map(|i| (nodes[i].clone(), vec![nodes[i].clone()])).collect();
        let target: Vec<_> = (1..3).map(|i| nodes[i].clone()).collect();

        for node in &nodes[3..9] {
            next_hops.push((node.clone(), target.clone()));
        }

        assert!(expected_routing_tables(&graph.calculate_distance(&unreliable_peers), &next_hops));

        assert_eq!(22, graph.total_active_edges() as usize);
        assert_eq!(22, graph.compute_total_active_edges() as usize);
    }

    // Test looks like this:
    // s - 0 ----- 1
    //  \--2 - 3 --/
    // When 0 is marked as  unreliable, the calls to 1 should go via 2.
    #[test]
    fn graph_longer_distance_with_unreliable_nodes() {
        let source = random_peer_id();
        let nodes: Vec<_> = (0..4).map(|_| random_peer_id()).collect();

        let mut graph = Graph::new(source.clone());
        graph.add_edge(&source, &nodes[0]);
        graph.add_edge(&source, &nodes[2]);
        graph.add_edge(&nodes[2], &nodes[3]);
        graph.add_edge(&nodes[3], &nodes[1]);
        graph.add_edge(&nodes[0], &nodes[1]);

        let unreliable_peers = HashSet::from([nodes[0].clone()]);

        let next_hops = vec![
            (nodes[0].clone(), vec![nodes[0].clone()]),
            (nodes[1].clone(), vec![nodes[2].clone()]),
            (nodes[2].clone(), vec![nodes[2].clone()]),
            (nodes[3].clone(), vec![nodes[2].clone()]),
        ];
        assert!(expected_routing_tables(&graph.calculate_distance(&unreliable_peers), &next_hops));
    }
}

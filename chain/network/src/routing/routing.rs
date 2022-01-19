use crate::routing::edge::{Edge, SimpleEdge};
use crate::routing::route_back_cache::RouteBackCache;
use crate::routing::utils::cache_to_hashmap;
use crate::PeerInfo;
use actix::dev::{MessageResponse, ResponseChannel};
use actix::{Actor, Message};
use cached::{Cached, SizedCache};
use near_network_primitives::types::{PeerIdOrHash, Ping, Pong};
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::time::Clock;
use near_primitives::types::AccountId;
use near_store::{ColAccountAnnouncements, Store};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::warn;

const ANNOUNCE_ACCOUNT_CACHE_SIZE: usize = 10_000;
const PING_PONG_CACHE_SIZE: usize = 1_000;
const ROUND_ROBIN_MAX_NONCE_DIFFERENCE_ALLOWED: usize = 10;
const ROUND_ROBIN_NONCE_CACHE_SIZE: usize = 10_000;
/// Routing table will clean edges if there is at least one node that is not reachable
/// since `SAVE_PEERS_MAX_TIME` seconds. All peers disconnected since `SAVE_PEERS_AFTER_TIME`
/// seconds will be removed from cache and persisted in disk.
pub const SAVE_PEERS_MAX_TIME: Duration = Duration::from_secs(7_200);
pub const DELETE_PEERS_AFTER_TIME: Duration = Duration::from_secs(3_600);
/// Graph implementation supports up to 128 peers.
pub const MAX_NUM_PEERS: usize = 128;

#[derive(Debug)]
#[cfg_attr(feature = "test_features", derive(serde::Serialize))]
pub struct PeerRequestResult {
    pub peers: Vec<PeerInfo>,
}

impl<A, M> MessageResponse<A, M> for PeerRequestResult
where
    A: Actor,
    M: Message<Result = PeerRequestResult>,
{
    fn handle<R: ResponseChannel<M>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self)
        }
    }
}

#[derive(MessageResponse, Debug)]
#[cfg_attr(feature = "test_features", derive(serde::Serialize))]
pub struct GetRoutingTableResult {
    pub edges_info: Vec<SimpleEdge>,
}

pub struct RoutingTableView {
    /// PeerId associated with this instance.
    my_peer_id: PeerId,
    /// PeerId associated for every known account id.
    account_peers: SizedCache<AccountId, AnnounceAccount>,
    /// Active PeerId that are part of the shortest path to each PeerId.
    pub peer_forwarding: Arc<HashMap<PeerId, Vec<PeerId>>>,
    /// Store last update for known edges. This is limited to list of adjacent edges to `my_peer_id`.
    pub local_edges_info: HashMap<(PeerId, PeerId), Edge>,
    /// Hash of messages that requires routing back to respective previous hop.
    pub route_back: RouteBackCache,
    /// Access to store on disk
    store: Store,
    /// Number of times each active connection was used to route a message.
    /// If there are several options use route with minimum nonce.
    /// New routes are added with minimum nonce.
    route_nonce: SizedCache<PeerId, usize>,
    /// Ping received by nonce.
    ping_info: SizedCache<usize, (Ping, usize)>,
    /// Ping received by nonce.
    pong_info: SizedCache<usize, (Pong, usize)>,
    /// List of pings sent for which we haven't received any pong yet.
    waiting_pong: SizedCache<PeerId, SizedCache<usize, Instant>>,
    /// Last nonce sent to each peer through pings.
    last_ping_nonce: SizedCache<PeerId, usize>,
}

#[derive(Debug)]
pub enum FindRouteError {
    Disconnected,
    PeerNotFound,
    AccountNotFound,
    RouteBackNotFound,
}

impl RoutingTableView {
    pub fn new(my_peer_id: PeerId, store: Store) -> Self {
        // Find greater nonce on disk and set `component_nonce` to this value.

        Self {
            my_peer_id,
            account_peers: SizedCache::with_size(ANNOUNCE_ACCOUNT_CACHE_SIZE),
            peer_forwarding: Default::default(),
            local_edges_info: Default::default(),
            route_back: RouteBackCache::default(),
            store,
            route_nonce: SizedCache::with_size(ROUND_ROBIN_NONCE_CACHE_SIZE),
            ping_info: SizedCache::with_size(PING_PONG_CACHE_SIZE),
            pong_info: SizedCache::with_size(PING_PONG_CACHE_SIZE),
            waiting_pong: SizedCache::with_size(PING_PONG_CACHE_SIZE),
            last_ping_nonce: SizedCache::with_size(PING_PONG_CACHE_SIZE),
        }
    }

    /// Checks whenever edge is newer than the one we already have.
    /// Works only for local edges.
    pub fn is_local_edge_newer(&self, key: &(PeerId, PeerId), nonce: u64) -> bool {
        assert!(key.0 == self.my_peer_id || key.1 == self.my_peer_id);
        self.local_edges_info.get(key).map_or(0, |x| x.nonce()) < nonce
    }

    pub fn reachable_peers(&self) -> impl Iterator<Item = &PeerId> {
        self.peer_forwarding.keys()
    }

    /// Find peer that is connected to `source` and belong to the shortest path
    /// from `source` to `peer_id`.
    pub fn find_route_from_peer_id(&mut self, peer_id: &PeerId) -> Result<PeerId, FindRouteError> {
        if let Some(routes) = self.peer_forwarding.get(peer_id).cloned() {
            if routes.is_empty() {
                return Err(FindRouteError::Disconnected);
            }

            // Strategy similar to Round Robin. Select node with least nonce and send it. Increase its
            // nonce by one. Additionally if the difference between the highest nonce and the lowest
            // nonce is greater than some threshold increase the lowest nonce to be at least
            // max nonce - threshold.
            let nonce_peer = routes
                .iter()
                .map(|peer_id| (self.route_nonce.cache_get(peer_id).cloned().unwrap_or(0), peer_id))
                .collect::<Vec<_>>();

            // Neighbor with minimum and maximum nonce respectively.
            let min_v = nonce_peer.iter().min().cloned().unwrap();
            let max_v = nonce_peer.into_iter().max().unwrap();

            if min_v.0 + ROUND_ROBIN_MAX_NONCE_DIFFERENCE_ALLOWED < max_v.0 {
                self.route_nonce
                    .cache_set(min_v.1.clone(), max_v.0 - ROUND_ROBIN_MAX_NONCE_DIFFERENCE_ALLOWED);
            }

            let next_hop = min_v.1;
            let nonce = self.route_nonce.cache_get(next_hop).cloned();
            self.route_nonce.cache_set(next_hop.clone(), nonce.map_or(1, |nonce| nonce + 1));
            Ok(next_hop.clone())
        } else {
            Err(FindRouteError::PeerNotFound)
        }
    }

    pub fn find_route(&mut self, target: &PeerIdOrHash) -> Result<PeerId, FindRouteError> {
        match target {
            PeerIdOrHash::PeerId(peer_id) => self.find_route_from_peer_id(peer_id),
            PeerIdOrHash::Hash(hash) => {
                self.fetch_route_back(*hash).ok_or(FindRouteError::RouteBackNotFound)
            }
        }
    }

    /// Find peer that owns this AccountId.
    pub fn account_owner(&mut self, account_id: &AccountId) -> Result<PeerId, FindRouteError> {
        self.get_announce(account_id)
            .map(|announce_account| announce_account.peer_id)
            .ok_or(FindRouteError::AccountNotFound)
    }

    /// Add (account id, peer id) to routing table.
    /// Note: There is at most on peer id per account id.
    pub fn add_account(&mut self, announce_account: AnnounceAccount) {
        let account_id = announce_account.account_id.clone();
        self.account_peers.cache_set(account_id.clone(), announce_account.clone());

        // Add account to store
        let mut update = self.store.store_update();
        if let Err(e) = update
            .set_ser(ColAccountAnnouncements, account_id.as_ref().as_bytes(), &announce_account)
            .and_then(|_| update.commit())
        {
            warn!(target: "network", "Error saving announce account to store: {:?}", e);
        }
    }

    // TODO(MarX, #1694): Allow one account id to be routed to several peer id.
    pub fn contains_account(&mut self, announce_account: &AnnounceAccount) -> bool {
        self.get_announce(&announce_account.account_id).map_or(false, |current_announce_account| {
            current_announce_account.epoch_id == announce_account.epoch_id
        })
    }

    pub fn remove_edges(&mut self, edges: &Vec<Edge>) {
        for edge in edges.iter() {
            assert!(edge.key().0 == self.my_peer_id || edge.key().1 == self.my_peer_id);
            self.local_edges_info.remove(edge.key());
        }
    }

    pub fn add_route_back(&mut self, hash: CryptoHash, peer_id: PeerId) {
        self.route_back.insert(hash, peer_id);
    }

    // Find route back with given hash and removes it from cache.
    fn fetch_route_back(&mut self, hash: CryptoHash) -> Option<PeerId> {
        self.route_back.remove(&hash)
    }

    pub fn compare_route_back(&mut self, hash: CryptoHash, peer_id: &PeerId) -> bool {
        self.route_back.get(&hash).map_or(false, |value| value == peer_id)
    }

    pub fn add_ping(&mut self, ping: Ping) {
        let cnt = self.ping_info.cache_get(&(ping.nonce as usize)).map(|v| v.1).unwrap_or(0);

        self.ping_info.cache_set(ping.nonce as usize, (ping, cnt + 1));
    }

    /// Return time of the round trip of ping + pong
    pub fn add_pong(&mut self, pong: Pong) -> Option<f64> {
        let mut res = None;

        if let Some(nonces) = self.waiting_pong.cache_get_mut(&pong.source) {
            res = nonces.cache_remove(&(pong.nonce as usize)).map(|sent| {
                Clock::instant().saturating_duration_since(sent).as_secs_f64() * 1000f64
            });
        }

        let cnt = self.pong_info.cache_get(&(pong.nonce as usize)).map(|v| v.1).unwrap_or(0);

        self.pong_info.cache_set(pong.nonce as usize, (pong, (cnt + 1)));

        res
    }

    // for unit tests
    pub fn sending_ping(&mut self, nonce: usize, target: PeerId) {
        let entry = if let Some(entry) = self.waiting_pong.cache_get_mut(&target) {
            entry
        } else {
            self.waiting_pong.cache_set(target.clone(), SizedCache::with_size(10));
            self.waiting_pong.cache_get_mut(&target).unwrap()
        };

        entry.cache_set(nonce, Clock::instant());
    }

    pub fn get_ping(&mut self, peer_id: PeerId) -> usize {
        if let Some(entry) = self.last_ping_nonce.cache_get_mut(&peer_id) {
            *entry += 1;
            *entry - 1
        } else {
            self.last_ping_nonce.cache_set(peer_id, 1);
            0
        }
    }

    // for unit tests
    pub fn fetch_ping_pong(
        &self,
    ) -> (HashMap<usize, (Ping, usize)>, HashMap<usize, (Pong, usize)>) {
        (cache_to_hashmap(&self.ping_info), cache_to_hashmap(&self.pong_info))
    }

    pub fn info(&mut self) -> RoutingTableInfo {
        let account_peers = self
            .get_announce_accounts()
            .into_iter()
            .map(|announce_account| (announce_account.account_id, announce_account.peer_id))
            .collect();
        RoutingTableInfo { account_peers, peer_forwarding: self.peer_forwarding.clone() }
    }

    /// Public interface for `account_peers`
    ///
    /// Get keys currently on cache.
    pub fn get_accounts_keys(&mut self) -> Vec<AccountId> {
        self.account_peers.key_order().cloned().collect()
    }

    /// Get announce accounts on cache.
    pub fn get_announce_accounts(&mut self) -> Vec<AnnounceAccount> {
        self.account_peers.value_order().cloned().collect()
    }

    /// Get number of accounts
    pub fn get_announce_accounts_size(&mut self) -> usize {
        self.account_peers.cache_size()
    }

    /// Get account announce from
    pub fn get_announce(&mut self, account_id: &AccountId) -> Option<AnnounceAccount> {
        if let Some(announce_account) = self.account_peers.cache_get(account_id) {
            Some(announce_account.clone())
        } else {
            self.store
                .get_ser(ColAccountAnnouncements, account_id.as_ref().as_bytes())
                .map(|res: Option<AnnounceAccount>| {
                    if let Some(announce_account) = res {
                        self.add_account(announce_account.clone());
                        Some(announce_account)
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|e| {
                    warn!(target: "network", "Error loading announce account from store: {:?}", e);
                    None
                })
        }
    }

    pub fn get_edge(&self, peer0: PeerId, peer1: PeerId) -> Option<&Edge> {
        assert!(peer0 == self.my_peer_id || peer1 == self.my_peer_id);

        let key = Edge::make_key(peer0, peer1);
        self.local_edges_info.get(&key)
    }
}
#[derive(Debug)]
pub struct RoutingTableInfo {
    pub account_peers: HashMap<AccountId, PeerId>,
    pub peer_forwarding: Arc<HashMap<PeerId, Vec<PeerId>>>,
}

/// `Graph` is used to compute `peer_routing`, which contains information how to route messages to
/// all known peers. That is, for each `peer`, we get a sub-set of peers to which we are connected
/// to that are on the shortest path between us as destination `peer`.
#[derive(Clone)]
pub struct Graph {
    /// peer_id of current peer
    my_peer_id: PeerId,
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
            my_peer_id: source.clone(),
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

    pub fn my_peer_id(&self) -> &PeerId {
        &self.my_peer_id
    }

    pub fn total_active_edges(&self) -> u64 {
        self.total_active_edges
    }

    // Compute number of active edges. We divide by 2 to remove duplicates.
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
    pub fn calculate_distance(&self) -> HashMap<PeerId, Vec<PeerId>> {
        // TODO add removal of unreachable nodes

        let mut queue = VecDeque::new();

        let nodes = self.id2p.len();
        let mut distance: Vec<i32> = vec![-1; nodes];
        let mut routes: Vec<u128> = vec![0; nodes];

        distance[self.source_id as usize] = 0;

        {
            let neighbors = &self.adjacency[self.source_id as usize];
            for (id, &neighbor) in neighbors.iter().enumerate().take(MAX_NUM_PEERS) {
                queue.push_back(neighbor);
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

        self.compute_result(&routes, &distance)
    }

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
            let mut peer_set: Vec<PeerId> = Vec::with_capacity(cur_route.count_ones() as usize);

            for (id, &neighbor) in neighbors.iter().enumerate().take(MAX_NUM_PEERS) {
                if (cur_route & (1u128 << id)) != 0 {
                    peer_set.push(self.id2p[neighbor as usize].clone());
                };
            }
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
    use crate::routing::routing::Graph;
    use crate::test_utils::{expected_routing_tables, random_peer_id};

    #[test]
    fn graph_contains_edge() {
        let source = random_peer_id();

        let node0 = random_peer_id();
        let node1 = random_peer_id();

        let mut graph = Graph::new(source.clone());

        assert!(!graph.contains_edge(&source, &node0));
        assert!(!graph.contains_edge(&source, &node1));
        assert!(!graph.contains_edge(&node0, &node1));
        assert!(!graph.contains_edge(&node1, &node0));

        graph.add_edge(&node0, &node1);

        assert!(!graph.contains_edge(&source, &node0));
        assert!(!graph.contains_edge(&source, &node1));
        assert!(graph.contains_edge(&node0, &node1));
        assert!(graph.contains_edge(&node1, &node0));

        graph.remove_edge(&node1, &node0);

        assert!(!graph.contains_edge(&node0, &node1));
        assert!(!graph.contains_edge(&node1, &node0));

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
            graph.calculate_distance(),
            vec![(node0.clone(), vec![node0.clone()])],
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

        assert!(expected_routing_tables(graph.calculate_distance(), vec![]));

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
            graph.calculate_distance(),
            vec![
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
            graph.calculate_distance(),
            vec![
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

        for node in nodes.iter().take(3) {
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

        for node in nodes.iter().take(9).skip(3) {
            next_hops.push((node.clone(), target.clone()));
        }

        assert!(expected_routing_tables(graph.calculate_distance(), next_hops));

        assert_eq!(22, graph.total_active_edges() as usize);
        assert_eq!(22, graph.compute_total_active_edges() as usize);
    }
}

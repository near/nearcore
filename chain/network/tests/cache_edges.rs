use chrono::{DateTime, Duration, Utc};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use borsh::de::BorshDeserialize;

use near_crypto::Signature;
use near_network::routing::{
    Edge, EdgeType, RoutingTable, SAVE_PEERS_AFTER_TIME, SAVE_PEERS_MAX_TIME,
};
use near_network::test_utils::random_peer_id;
use near_primitives::network::PeerId;
use near_store::test_utils::create_test_store;
use near_store::{ColComponentEdges, ColPeerComponent, Store};

#[derive(Eq, PartialEq, Hash)]
struct EdgeDescription(usize, usize, EdgeType);

impl EdgeDescription {
    fn from(data: (usize, usize, bool)) -> Self {
        let (u, v, t) = data;
        Self(u, v, if t { EdgeType::Added } else { EdgeType::Removed })
    }
}

struct RoutingTableTest {
    routing_table: RoutingTable,
    store: Arc<Store>,
    peers: Vec<PeerId>,
    rev_peers: HashMap<PeerId, usize>,
    times: Vec<DateTime<Utc>>,
}

impl RoutingTableTest {
    fn new() -> Self {
        let me = random_peer_id();
        let store = create_test_store();
        let now = Utc::now();

        Self {
            routing_table: RoutingTable::new(me.clone(), store.clone()),
            store,
            peers: vec![me.clone()],
            rev_peers: vec![(me, 0)].into_iter().collect(),
            times: vec![
                now.checked_sub_signed(Duration::seconds((SAVE_PEERS_AFTER_TIME / 2) as i64))
                    .unwrap(),
                now.checked_sub_signed(Duration::seconds(
                    ((SAVE_PEERS_AFTER_TIME + SAVE_PEERS_MAX_TIME) / 2) as i64,
                ))
                .unwrap(),
                now.checked_sub_signed(Duration::seconds(
                    (SAVE_PEERS_MAX_TIME * 3 / 2 - SAVE_PEERS_AFTER_TIME / 2) as i64,
                ))
                .unwrap(),
            ],
        }
    }

    fn set_times(&mut self, times: Vec<(usize, usize)>) {
        for (peer_ix, time) in times.iter() {
            let peer_id = self.get_peer(*peer_ix).clone();
            let instant = self.times.get(*time).cloned().unwrap();
            self.routing_table.peer_last_time_reachable.insert(peer_id, instant);
        }
    }

    fn get_peer(&mut self, index: usize) -> &PeerId {
        while self.peers.len() <= index {
            let peer_id = random_peer_id();
            self.rev_peers.insert(peer_id.clone(), self.peers.len());
            self.peers.push(peer_id);
        }
        self.peers.get(index).unwrap()
    }

    fn get_edge_description(&self, edge: &Edge) -> EdgeDescription {
        let peer0 = self.rev_peers.get(&edge.peer0).unwrap();
        let peer1 = self.rev_peers.get(&edge.peer1).unwrap();
        let edge_type = edge.edge_type();
        EdgeDescription(*peer0, *peer1, edge_type)
    }

    fn check(
        &mut self,
        on_memory: Vec<(usize, usize, bool)>,
        on_disk_edges: Vec<(u64, Vec<(usize, usize, bool)>)>,
        on_disk_peers: Vec<(usize, u64)>,
    ) {
        let on_memory = on_memory.into_iter().map(EdgeDescription::from).collect::<HashSet<_>>();
        let on_disk_edges = on_disk_edges
            .into_iter()
            .map(|(key, value)| {
                (
                    key,
                    value
                        .into_iter()
                        .map(|(u, v, t)| {
                            let peer0 = self.get_peer(u).clone();
                            let peer1 = self.get_peer(v).clone();
                            let (u, v) = if peer1 < peer0 { (v, u) } else { (u, v) };
                            EdgeDescription::from((u, v, t))
                        })
                        .collect::<HashSet<_>>(),
                )
            })
            .collect::<HashMap<_, _>>();
        let on_disk_peers = on_disk_peers.into_iter().collect::<HashMap<_, _>>();

        // Check memory edges
        for EdgeDescription(peer0, peer1, edge_type) in on_memory.iter() {
            let peer0 = self.get_peer(*peer0).clone();
            let peer1 = self.get_peer(*peer1).clone();
            let (peer0, peer1) = Edge::key(peer0, peer1);

            let res = self.routing_table.edges_info.get(&(peer0, peer1));
            assert!(res.is_some());
            let edge = res.unwrap();
            assert_eq!(edge.edge_type(), *edge_type);
        }
        assert_eq!(on_memory.len(), self.routing_table.edges_info.len());

        // Check for peers on disk
        let mut total_peers = 0;
        for (peer, nonce) in self.store.iter(ColPeerComponent) {
            total_peers += 1;

            let peer = PeerId::try_from_slice(peer.as_ref()).unwrap();
            let nonce = u64::try_from_slice(nonce.as_ref()).unwrap();
            let peer_ix = self.rev_peers.get(&peer).unwrap();
            let res = on_disk_peers.get(peer_ix).unwrap();
            assert_eq!(*res, nonce);
        }
        assert_eq!(total_peers, on_disk_peers.len());

        // Check for edges on disk
        let mut total_nonces = 0;
        for (nonce, edges) in self.store.iter(ColComponentEdges) {
            total_nonces += 1;

            let nonce = u64::try_from_slice(nonce.as_ref()).unwrap();

            let edges = Vec::<Edge>::try_from_slice(edges.as_ref()).unwrap();
            let current_edges = on_disk_edges.get(&nonce).unwrap();

            assert_eq!(edges.len(), current_edges.len());

            for edge in edges.iter() {
                let edge_description = self.get_edge_description(edge);
                assert!(current_edges.contains(&edge_description));
            }
        }
        assert_eq!(total_nonces, on_disk_edges.len());
    }

    fn add_edge(&mut self, peer0: usize, peer1: usize, nonce: u64) {
        let peer0 = self.get_peer(peer0).clone();
        let peer1 = self.get_peer(peer1).clone();
        let edge = Edge::new(peer0, peer1, nonce, Signature::default(), Signature::default());
        self.routing_table.process_edges(vec![edge]);
    }

    fn update(&mut self) {
        self.routing_table.update();
    }
}

#[test]
fn empty() {
    let mut test = RoutingTableTest::new();
    test.check(vec![], vec![], vec![]);
    assert_eq!(test.routing_table.component_nonce, 0);
}

#[test]
fn one_edge() {
    let mut test = RoutingTableTest::new();
    test.add_edge(0, 1, 1);
    test.check(vec![(0, 1, true)], vec![], vec![]);
}

#[test]
fn active_old_edge() {
    let mut test = RoutingTableTest::new();
    test.add_edge(0, 1, 1);
    test.set_times(vec![(1, 2)]);
    test.update();
    test.check(vec![(0, 1, true)], vec![], vec![]);
}

#[test]
fn inactive_old_edge() {
    let mut test = RoutingTableTest::new();
    test.add_edge(0, 1, 2);
    test.set_times(vec![(1, 2)]);
    test.update();
    test.check(vec![], vec![(0, vec![(0, 1, false)])], vec![(1, 0)]);
}

#[test]
fn inactive_recent_edge() {
    let mut test = RoutingTableTest::new();
    test.add_edge(0, 1, 2);
    test.set_times(vec![(1, 1)]);
    test.update();
    test.check(vec![(0, 1, false)], vec![], vec![]);
}

#[test]
fn load_component_nonce_on_start() {
    let mut test = RoutingTableTest::new();
    test.add_edge(0, 1, 2);
    test.set_times(vec![(1, 2)]);
    test.update();
    let routing_table = RoutingTable::new(random_peer_id(), test.store.clone());
    assert_eq!(routing_table.component_nonce, 1);
}

#[test]
fn load_component_nonce_2_on_start() {
    let mut test = RoutingTableTest::new();
    test.add_edge(0, 1, 2);
    test.set_times(vec![(1, 2)]);
    test.update();
    test.add_edge(0, 2, 2);
    test.set_times(vec![(2, 2)]);
    test.update();
    test.check(
        vec![],
        vec![(0, vec![(0, 1, false)]), (1, vec![(0, 2, false)])],
        vec![(1, 0), (2, 1)],
    );
    let routing_table = RoutingTable::new(random_peer_id(), test.store.clone());
    assert_eq!(routing_table.component_nonce, 2);
}

#[test]
fn two_components() {
    let mut test = RoutingTableTest::new();
    test.add_edge(0, 1, 2);
    test.set_times(vec![(1, 2)]);
    test.update();
    test.add_edge(0, 2, 2);
    test.set_times(vec![(2, 2)]);
    test.update();
    test.add_edge(1, 2, 1);
    test.update();
    test.check(
        vec![],
        vec![(2, vec![(0, 1, false), (0, 2, false), (1, 2, true)])],
        vec![(1, 2), (2, 2)],
    );
}

#[test]
fn overwrite_edge() {
    let mut test = RoutingTableTest::new();
    test.add_edge(0, 1, 2);
    test.set_times(vec![(1, 2)]);
    test.update();
    test.add_edge(0, 2, 2);
    test.set_times(vec![(2, 2)]);
    test.update();
    test.add_edge(1, 2, 1);
    test.update();
    test.add_edge(0, 1, 3);
    test.update();
    test.check(vec![(0, 1, true), (1, 2, true), (0, 2, false)], vec![], vec![]);
}

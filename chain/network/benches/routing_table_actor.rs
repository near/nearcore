#[macro_use]
extern crate bencher;
extern crate actix;

use bencher::{black_box, Bencher};
use near_crypto::{KeyType, SecretKey, Signature};
use near_network::test_utils::random_peer_id;
use near_network::RoutingTableActor;
use near_network_primitives::types::Edge;
use near_primitives::network::PeerId;
use near_store::test_utils::create_test_store;
use std::collections::HashMap;
use std::sync::Arc;

fn build_graph(depth: usize, size: usize) -> RoutingTableActor {
    // This is needed for `RoutingTableActor` not to crash.
    // `RoutingTableActor` stats `EdgeValidatorActor.
    let _system = actix::System::new();

    let source = random_peer_id();
    let nodes: Vec<_> = (0..depth * size).map(|_| random_peer_id()).collect();

    let store = create_test_store();
    let mut routing_table_actor = RoutingTableActor::new(source.clone(), store);

    let mut edges: Vec<Edge> = Vec::new();
    for node in &nodes[..size] {
        edges.push(Edge::make_fake_edge(source.clone(), node.clone(), 1));
    }

    for layer in 0..depth - 1 {
        for u in 0..size {
            for v in 0..size {
                let peer0 = nodes[layer * size + u].clone();
                let peer1 = nodes[(layer + 1) * size + v].clone();
                edges.push(Edge::make_fake_edge(peer0, peer1, (layer + u + v) as u64));
            }
        }
    }
    routing_table_actor.add_verified_edges_to_routing_table(edges);

    routing_table_actor
}

#[allow(dead_code)]
fn get_all_edges_bench_old(bench: &mut Bencher) {
    // 1000 nodes, 10m edges
    let routing_table_actor = build_graph(10, 100);
    bench.iter(|| {
        let result = routing_table_actor.get_all_edges();
        black_box(result);
    });
}

#[allow(dead_code)]
fn get_all_edges_bench_new2(bench: &mut Bencher) {
    // this is how we efficient we could make get_all_edges by using Arc

    // 1000 nodes, 10m edges
    let routing_table_actor = build_graph(10, 100);
    let all_edges = routing_table_actor.get_all_edges();
    let mut new_edges_info = HashMap::new();
    for edge in all_edges {
        let edge = EdgeNew {
            key: Arc::new(edge.key().clone()),
            nonce: edge.nonce(),
            signature0: edge.signature0().clone(),
            signature1: edge.signature1().clone(),
            removal_info: edge.removal_info().cloned(),
        };

        new_edges_info.insert(edge.key.clone(), Arc::new(edge));
    }

    bench.iter(|| {
        let result: Vec<Arc<EdgeNew>> = new_edges_info.iter().map(|x| x.1.clone()).collect();
        black_box(result);
    });
}

#[allow(dead_code)]
fn get_all_edges_bench_new3(bench: &mut Bencher) {
    // this is how we efficient we could make get_all_edges by using Arc

    // 1000 nodes, 10m edges
    let routing_table_actor = build_graph(10, 100);
    let all_edges = routing_table_actor.get_all_edges();
    let mut new_edges_info = HashMap::new();
    for edge in all_edges {
        let edge = EdgeNew2 {
            key: (Arc::new(edge.key().0.clone()), Arc::new(edge.key().1.clone())),
            nonce: edge.nonce(),
            signature0: edge.signature0().clone(),
            signature1: edge.signature1().clone(),
            removal_info: edge.removal_info().cloned(),
        };

        new_edges_info.insert(edge.key.clone(), Arc::new(edge));
    }

    bench.iter(|| {
        let result: Vec<Arc<EdgeNew2>> = new_edges_info.iter().map(|x| x.1.clone()).collect();
        black_box(result);
    });
}

#[allow(dead_code)]
fn benchmark_sign_edge(bench: &mut Bencher) {
    let sk = SecretKey::from_seed(KeyType::ED25519, "1234");

    let p0 = PeerId::new(sk.public_key());
    let p1 = PeerId::random();

    bench.iter(|| {
        let ei = Edge::build_hash(&p0, &p1, 123);
        black_box(ei);
    });
}

benchmark_group!(
    benches,
    get_all_edges_bench_old,
    get_all_edges_bench_new2,
    get_all_edges_bench_new3,
    benchmark_sign_edge
);

benchmark_main!(benches);

// running 3 tests
// test get_all_edges_bench_old  ... bench:   1,296,045 ns/iter (+/- 601,626)
// replace key with Arc
// test get_all_edges_bench_new2 ... bench:   1,001,090 ns/iter (+/- 25,434)
// replace PeerId with Arc (Preferred)
// test get_all_edges_bench_new3 ... bench:   1,017,563 ns/iter (+/- 37,675)

pub struct EdgeNew {
    /// Since edges are not directed `peer0 < peer1` should hold.
    pub key: Arc<(PeerId, PeerId)>,
    /// Nonce to keep tracking of the last update on this edge.
    /// It must be even
    pub nonce: u64,
    /// Signature from parties validating the edge. These are signature of the added edge.
    #[allow(unused)]
    signature0: Signature,
    #[allow(unused)]
    signature1: Signature,
    /// Info necessary to declare an edge as removed.
    /// The bool says which party is removing the edge: false for Peer0, true for Peer1
    /// The signature from the party removing the edge.
    #[allow(unused)]
    removal_info: Option<(bool, Signature)>,
}

pub struct EdgeNew2 {
    /// Since edges are not directed `peer0 < peer1` should hold.
    pub key: (Arc<PeerId>, Arc<PeerId>),
    /// Nonce to keep tracking of the last update on this edge.
    /// It must be even
    pub nonce: u64,
    /// Signature from parties validating the edge. These are signature of the added edge.
    #[allow(unused)]
    signature0: Signature,
    #[allow(unused)]
    signature1: Signature,
    /// Info necessary to declare an edge as removed.
    /// The bool says which party is removing the edge: false for Peer0, true for Peer1
    /// The signature from the party removing the edge.
    #[allow(unused)]
    removal_info: Option<(bool, Signature)>,
}

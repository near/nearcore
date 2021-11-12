#[macro_use]
extern crate bencher;

use bencher::black_box;
use bencher::Bencher;
use near_crypto::Signature;
use std::collections::HashMap;
use std::sync::Arc;

use near_network::routing::routing::Edge;
use near_network::test_utils::random_peer_id;
use near_network::RoutingTableActor;
use near_primitives::network::PeerId;
use near_store::test_utils::create_test_store;

fn build_graph(depth: usize, size: usize) -> RoutingTableActor {
    let source = random_peer_id();
    let nodes: Vec<_> = (0..depth * size).map(|_| random_peer_id()).collect();

    let store = create_test_store();
    let mut routing_table_actor = RoutingTableActor::new(source.clone(), store);

    let mut edges: Vec<Edge> = Vec::new();
    for i in 0..size {
        edges.push(Edge::make_fake_edge(source.clone(), nodes[i].clone(), 1));
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
fn get_all_edges_bench_new(bench: &mut Bencher) {
    // this is how we efficient we could make get_all_edges by using Arc

    // 1000 nodes, 10m edges
    let routing_table_actor = build_graph(10, 100);
    let all_edges = routing_table_actor.get_all_edges();
    let mut new_edges_info = HashMap::new();
    for edge in all_edges {
        new_edges_info.insert((edge.peer0.clone(), edge.peer1.clone()), Arc::new(edge.clone()));
    }

    bench.iter(|| {
        let result: Vec<Arc<Edge>> = new_edges_info.iter().map(|x| x.1.clone()).collect();
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
            key: Arc::new((edge.peer0, edge.peer1)),
            nonce: edge.nonce,
            signature0: edge.signature0,
            signature1: edge.signature1,
            removal_info: edge.removal_info,
        };

        new_edges_info.insert(edge.key.clone(), Arc::new(edge));
    }

    bench.iter(|| {
        let result: Vec<Arc<EdgeNew>> = new_edges_info.iter().map(|x| x.1.clone()).collect();
        black_box(result);
    });
}

benchmark_group!(
    benches,
    get_all_edges_bench_old,
    get_all_edges_bench_new,
    get_all_edges_bench_new2
);

benchmark_main!(benches);

// running 2 tests
// test get_all_edges_bench_old ... bench:   5,045,404 ns/iter (+/- 168,446)
// test get_all_edges_bench_new ...bench:   1,231,155 ns/iter (+/- 95,776)
// using new Edge Representation - EdgeNew
// test get_all_edges_bench_new2 ... bench:     943,015 ns/iter (+/- 15,679)

pub struct EdgeNew {
    /// Since edges are not directed `peer0 < peer1` should hold.
    pub key: Arc<(PeerId, PeerId)>,
    /// Nonce to keep tracking of the last update on this edge.
    /// It must be even
    pub nonce: u64,
    /// Signature from parties validating the edge. These are signature of the added edge.
    signature0: Signature,
    signature1: Signature,
    /// Info necessary to declare an edge as removed.
    /// The bool says which party is removing the edge: false for Peer0, true for Peer1
    /// The signature from the party removing the edge.
    removal_info: Option<(bool, Signature)>,
}

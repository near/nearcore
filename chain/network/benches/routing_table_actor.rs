#[macro_use]
extern crate bencher;

use bencher::black_box;
use bencher::Bencher;
use std::collections::HashMap;
use std::sync::Arc;

use near_network::routing::routing::Edge;
use near_network::test_utils::random_peer_id;
use near_network::RoutingTableActor;
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
fn get_all_edges_bench(bench: &mut Bencher) {
    let routing_table_actor = build_graph(100, 100);
    bench.iter(|| {
        let result = routing_table_actor.get_all_edges();
        black_box(result);
    });
}

#[allow(dead_code)]
fn get_all_edges_bench2(bench: &mut Bencher) {
    // this is how we efficient we could make get_all_edges by using Arc

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

benchmark_group!(benches, get_all_edges_bench,);

benchmark_main!(benches);

// running 3 tests
// test get_all_edges_bench  ... bench:   1,503,222 ns/iter (+/- 126,811)

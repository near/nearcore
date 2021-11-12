#[macro_use]
extern crate bencher;

use bencher::black_box;
use bencher::Bencher;

use near_network::routing::routing::{Edge, Graph};
use near_network::test_utils::random_peer_id;
use near_network::RoutingTableActor;

fn build_graph(depth: usize, size: usize) -> RoutingTableActor {
    let source = random_peer_id();
    let nodes: Vec<_> = (0..depth * size).map(|_| random_peer_id()).collect();

    let mut routing_table_actor = RoutingTableActor::new(source.clone());

    for i in 0..size {
        graph.add_edge(source.clone(), nodes[i].clone());
    }

    let mut edges: Vec<Edges> = Vec::new();
    for layer in 0..depth - 1 {
        for u in 0..size {
            for v in 0..size {
                let peer0 = nodes[layer * size + u].clone();
                let peer1 = nodes[(layer + 1) * size + v].clone();
                edges.push(Edge::make_fake_edge(peer0, peer1, lay + u + v));
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

benchmark_group!(benches, get_all_edges_bench,);

benchmark_main!(benches);

// running 3 tests
// test calculate_distance_10_10  ... bench:   1,503,222 ns/iter (+/- 126,811)
// test calculate_distance_10_100 ... bench: 988,705,595 ns/iter (+/- 118,318,208)
// test calculate_distance_3_3    ... bench:      18,928 ns/iter (+/- 3,174)

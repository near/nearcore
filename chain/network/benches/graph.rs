#[macro_use]
extern crate bencher;

use bencher::Bencher;

use near_network::routing::Graph;
use near_network::test_utils::random_peer_id;

fn build_graph(depth: usize, size: usize) -> Graph {
    let source = random_peer_id();
    let nodes: Vec<_> = (0..depth * size).map(|_| random_peer_id()).collect();

    let mut graph = Graph::new(source.clone());

    for i in 0..size {
        graph.add_edge(source.clone(), nodes[i].clone());
    }

    for layer in 0..depth - 1 {
        for u in 0..size {
            for v in 0..size {
                graph.add_edge(
                    nodes[layer * size + u].clone(),
                    nodes[(layer + 1) * size + v].clone(),
                );
            }
        }
    }

    graph
}

fn calculate_distance_3_3(bench: &mut Bencher) {
    let graph = build_graph(3, 3);
    bench.iter(|| {
        let _ = graph.calculate_distance();
    });
}

fn calculate_distance_10_10(bench: &mut Bencher) {
    let graph = build_graph(10, 10);
    bench.iter(|| {
        let _ = graph.calculate_distance();
    });
}

fn calculate_distance_10_100(bench: &mut Bencher) {
    let graph = build_graph(10, 100);
    bench.iter(|| {
        let _ = graph.calculate_distance();
    });
}

#[allow(dead_code)]
fn calculate_distance_100_100(bench: &mut Bencher) {
    let graph = build_graph(100, 100);
    bench.iter(|| {
        let _ = graph.calculate_distance();
    });
}

benchmark_group!(
    benches,
    calculate_distance_3_3,
    calculate_distance_10_10,
    //    calculate_distance_100_100,
    calculate_distance_10_100
);

benchmark_main!(benches);

// running 3 tests
// test calculate_distance_10_10  ... bench:   1,503,222 ns/iter (+/- 126,811)
// test calculate_distance_10_100 ... bench: 988,705,595 ns/iter (+/- 118,318,208)
// test calculate_distance_3_3    ... bench:      18,928 ns/iter (+/- 3,174)

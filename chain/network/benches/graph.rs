#[macro_use]
extern crate criterion;

use criterion::{black_box, Criterion};
use near_network::routing::Graph;
use near_network::test_utils::random_peer_id;

fn build_graph(depth: usize, size: usize) -> Graph {
    let source = random_peer_id();
    let nodes: Vec<_> = (0..depth * size).map(|_| random_peer_id()).collect();

    let mut graph = Graph::new(source.clone());

    for node in &nodes[..size] {
        graph.add_edge(&source, node);
    }

    for layer in 0..depth - 1 {
        for u in 0..size {
            for v in 0..size {
                graph.add_edge(&nodes[layer * size + u], &nodes[(layer + 1) * size + v]);
            }
        }
    }

    graph
}

fn calculate_distance_3_3(c: &mut Criterion) {
    let graph = build_graph(3, 3);
    c.bench_function("calculate_distance_3_3", |bench| {
        bench.iter(|| {
            black_box(graph.calculate_distance());
        })
    });
}

fn calculate_distance_10_10(c: &mut Criterion) {
    let graph = build_graph(10, 10);
    c.bench_function("calculate_distance_10_10", |bench| {
        bench.iter(|| {
            black_box(graph.calculate_distance());
        })
    });
}

fn calculate_distance_10_100(c: &mut Criterion) {
    c.bench_function("calculate_distance_10_100", |bench| {
        let graph = build_graph(10, 100);
        bench.iter(|| {
            black_box(graph.calculate_distance());
        })
    });
}

#[allow(dead_code)]
fn calculate_distance_100_100(c: &mut Criterion) {
    let graph = build_graph(100, 100);
    c.bench_function("calculate_distance_100_100", |bench| {
        bench.iter(|| {
            black_box(graph.calculate_distance());
        })
    });
}

criterion_group!(
    benches,
    calculate_distance_3_3,
    calculate_distance_10_10,
    //    calculate_distance_100_100,
    calculate_distance_10_100
);

criterion_main!(benches);

// running 3 tests
// calculate_distance_3_3    time:   [566.42 ns 571.50 ns 578.62 ns]
// calculate_distance_10_10  time:   [10.631 us 10.651 us 10.679 us]
// calculate_distance_10_100 time:   [607.36 us 610.44 us 613.75 us]

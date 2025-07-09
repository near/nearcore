//! Benchmark for Borsh encoding/decoding of ChunkStateWitness.
//!
//! Run with `cargo bench --bench state_witness_borsh`

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use std::time::Duration;

use near_primitives::stateless_validation::state_witness::ChunkStateWitness;
use testlib::state_witness_test_data;

fn borsh_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_witness_borsh");
    group
        .measurement_time(Duration::from_secs(5))
        .warm_up_time(Duration::from_millis(1500))
        .confidence_level(0.90)
        .significance_level(0.10)
        .noise_threshold(0.05);

    // Test different sizes
    let sizes = &[("5MB", 5_000_000), ("15MB", 15_000_000)];

    for &(size_name, size_bytes) in sizes {
        let witness = state_witness_test_data::generate_realistic_state_witness(size_bytes);

        // Pre-serialize to get actual serialized size
        let serialized = borsh::to_vec(&witness).unwrap();
        let actual_size = serialized.len();

        println!(
            "Created {} witness: target={} bytes, actual={} bytes ({:.2}x)",
            size_name,
            size_bytes,
            actual_size,
            actual_size as f64 / size_bytes as f64
        );

        group.throughput(Throughput::Bytes(actual_size as u64));

        // Benchmark Borsh serialization
        group.bench_with_input(BenchmarkId::new("serialize", size_name), &witness, |b, witness| {
            b.iter(|| {
                let serialized = borsh::to_vec(black_box(witness)).unwrap();
                black_box(serialized)
            });
        });

        // Benchmark Borsh deserialization
        group.bench_with_input(
            BenchmarkId::new("deserialize", size_name),
            &serialized,
            |b, data| {
                b.iter(|| {
                    let deserialized: ChunkStateWitness =
                        borsh::from_slice(black_box(data)).unwrap();
                    // Prevent deallocation by forgetting the value - we only want to measure deserialization
                    std::mem::forget(deserialized);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, borsh_benchmark);
criterion_main!(benches);

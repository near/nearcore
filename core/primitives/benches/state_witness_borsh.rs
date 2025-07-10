//! Benchmark for Borsh encoding/decoding of ChunkStateWitness.
//!
//! Run with `cargo bench --bench state_witness_borsh`

use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use std::time::Duration;

use near_primitives::stateless_validation::lazy_state_witness::LazyChunkStateWitness;
use near_primitives::stateless_validation::state_witness::ChunkStateWitness;
use testlib::state_witness_test_data;

fn witness_benchmark(c: &mut Criterion) {
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

        // Benchmark LazyChunkStateWitness conversion
        group.bench_with_input(
            BenchmarkId::new("lazy_convert", size_name),
            &serialized,
            |b, data| {
                b.iter_batched(
                    || {
                        // Setup phase (not timed): create lazy witness and wait for it to be ready
                        let lazy_witness = LazyChunkStateWitness::from_bytes(data).unwrap();
                        while !lazy_witness.is_ready() {
                            std::thread::sleep(Duration::from_millis(2));
                        }
                        lazy_witness
                    },
                    |lazy_witness| {
                        // Benchmark phase (timed): just the conversion
                        let normal_witness = black_box(lazy_witness).into_chunk_state_witness();

                        // Access some data to prevent optimization
                        let hash_bytes = normal_witness.chunk_header().chunk_hash().as_ref().len();
                        let tx_count = normal_witness.transactions().len();
                        black_box((hash_bytes, tx_count));

                        std::mem::forget(normal_witness);
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // Benchmark LazyChunkStateWitness creation (header parsing only)
        group.bench_with_input(
            BenchmarkId::new("lazy_create", size_name),
            &serialized,
            |b, data| {
                b.iter_batched(
                    || {
                        // Setup phase: small delay to let previous background tasks progress
                        std::thread::sleep(Duration::from_millis(50));
                        data
                    },
                    |data| {
                        // Benchmark phase (timed): create lazy witness and access header
                        let lazy_witness =
                            LazyChunkStateWitness::from_bytes(black_box(data)).unwrap();

                        let hash_bytes =
                            black_box(lazy_witness.chunk_header().chunk_hash().as_ref().len());
                        let shard_id = black_box(lazy_witness.chunk_production_key().shard_id);

                        black_box((hash_bytes, shard_id))
                        // Background task continues but they should complete during next iteration's setup
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(benches, witness_benchmark);
criterion_main!(benches);

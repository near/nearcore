//! Benchmark for encoding/decoding of State Witness.
//!
//! Run with `cargo bench --bench compression`

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use std::io::Write;
use std::time::Duration;

/// Compression levels to test
const COMPRESSION_LEVELS: &[i32] = &[1, 3];

/// Number of threads
const NUM_THREADS: u32 = 4;

/// Test data sizes (in bytes)
const DATA_SIZES: &[(&str, usize)] = &[("5MB", 5_000_000), ("15MB", 15_000_000)];

/// Generates test data aiming for 1.9x-2.0x compression ratio (what we observe in forknet for state witness)
#[allow(unused)]
fn generate_test_data(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let mut rng = 12345u64;

    for _ in 0..size {
        rng = rng.wrapping_mul(1103515245).wrapping_add(12345);

        match rng % 100 {
            0..=60 => data.push(b'A' + (rng % 12) as u8), // 61% letters
            61..=75 => data.push(b'0' + (rng % 5) as u8), // 15% digits
            76..=92 => data.push(b' '),                   // 17% spaces
            93..=97 => data.push(b'\n'),                  // 5% newlines
            _ => data.push(b'A'),                         // 2% consistent 'A'
        }
    }

    data
}

use testlib::state_witness_test_data;

fn compression_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression_analysis");
    group
        .measurement_time(Duration::from_secs(5))
        .warm_up_time(Duration::from_millis(1500))
        .confidence_level(0.90)
        .significance_level(0.10)
        .noise_threshold(0.05);

    // Benchmark all combinations of levels and sizes
    for &(size_name, size_bytes) in DATA_SIZES {
        let test_data = state_witness_test_data::generate_realistic_test_data(size_bytes);
        group.throughput(Throughput::Bytes(size_bytes as u64));

        for &level in COMPRESSION_LEVELS {
            // Pre-compress to show compression info
            let mut encoder = zstd::stream::Encoder::new(Vec::new(), level).unwrap();
            encoder.write_all(&test_data).unwrap();
            let compressed_once = encoder.finish().unwrap();
            let ratio = size_bytes as f64 / compressed_once.len() as f64;

            println!(
                "Level {}, {}: {} -> {} bytes (ratio: {:.2}x)",
                level,
                size_name,
                size_bytes,
                compressed_once.len(),
                ratio
            );

            // Single-threaded compression benchmark
            group.bench_with_input(
                BenchmarkId::new(format!("compress_L{}_1T", level), size_name),
                &(&test_data, level),
                |b, (data, level)| {
                    b.iter(|| {
                        let mut encoder = zstd::stream::Encoder::new(Vec::new(), *level).unwrap();
                        encoder.write_all(black_box(data)).unwrap();
                        let compressed = encoder.finish().unwrap();
                        black_box(compressed)
                    });
                },
            );

            // Multi-threaded compression benchmark
            group.bench_with_input(
                BenchmarkId::new(format!("compress_L{}_{}T", level, NUM_THREADS), size_name),
                &(&test_data, level),
                |b, (data, level)| {
                    b.iter(|| {
                        let mut encoder = zstd::stream::Encoder::new(Vec::new(), *level).unwrap();
                        encoder.multithread(NUM_THREADS).unwrap();
                        encoder.write_all(black_box(data)).unwrap();
                        let compressed = encoder.finish().unwrap();
                        black_box(compressed)
                    });
                },
            );

            // Use the already compressed data for decompression benchmark
            let compressed_data = compressed_once;

            // Decompression benchmark
            group.bench_with_input(
                BenchmarkId::new(format!("decompress_L{}", level), size_name),
                &compressed_data,
                |b, compressed| {
                    b.iter(|| {
                        let mut decoder =
                            zstd::stream::Decoder::new(compressed.as_slice()).unwrap();
                        let mut decompressed = Vec::new();
                        std::io::copy(&mut decoder, &mut decompressed).unwrap();
                        black_box(decompressed)
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, compression_benchmark);
criterion_main!(benches);

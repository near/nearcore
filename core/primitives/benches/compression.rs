use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::io::Write;
use std::time::Duration;

/// Compression levels to test
const COMPRESSION_LEVELS: &[i32] = &[1, 3, 6];

/// Test data sizes (in bytes)
const DATA_SIZES: &[(&str, usize)] = &[
    ("100KB", 100_000),
    ("1MB", 1_000_000), 
    ("5MB", 5_000_000),
];

/// Generate test data using a simple LCG
fn generate_test_data(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let mut rng = 12345u64;
    
    for _ in 0..size {
        rng = rng.wrapping_mul(1103515245).wrapping_add(12345);
        
        match rng % 100 {
            0..=89 => data.push(b"ABCDEFGHIJKLMNOP"[(rng >> 8) as usize % 16]),
            _ => data.push((rng >> 16) as u8),
        }
    }
    
    data
}

fn compression_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression_analysis");
    group.measurement_time(Duration::from_secs(5))
        .warm_up_time(Duration::from_millis(1500))
        .confidence_level(0.90)  
        .significance_level(0.10)
        .noise_threshold(0.05);
    
    // Benchmark all combinations of levels and sizes
    for &(size_name, size_bytes) in DATA_SIZES {
        let test_data = generate_test_data(size_bytes);
        group.throughput(Throughput::Bytes(size_bytes as u64));
        
        for &level in COMPRESSION_LEVELS {
            // Pre-compress to show compression info
            let mut encoder = zstd::stream::Encoder::new(Vec::new(), level).unwrap();
            encoder.write_all(&test_data).unwrap();
            let compressed_once = encoder.finish().unwrap();
            let ratio = size_bytes as f64 / compressed_once.len() as f64;
            
            println!("Level {}, {}: {} -> {} bytes (ratio: {:.2}x)", 
                     level, size_name, size_bytes, compressed_once.len(), ratio);
            
            // Compression benchmark
            group.bench_with_input(
                BenchmarkId::new(format!("compress_L{}", level), size_name),
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
            
            // Use the already compressed data for decompression benchmark
            let compressed_data = compressed_once;
            
            // Decompression benchmark
            group.bench_with_input(
                BenchmarkId::new(format!("decompress_L{}", level), size_name),
                &compressed_data,
                |b, compressed| {
                    b.iter(|| {
                        let mut decoder = zstd::stream::Decoder::new(compressed.as_slice()).unwrap();
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
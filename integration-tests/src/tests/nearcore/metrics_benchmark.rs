use near_network_primitives::time::Clock;
use near_o11y::metrics::{try_create_int_counter_vec, IntCounter, IntCounterVec};
use once_cell::sync::Lazy;

pub static COUNTERS_1: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec("near_test_counters_1", "Just counters", &["shard_id"]).unwrap()
});

pub static COUNTERS_2: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec("near_test_counters_2", "Just counters", &["shard_id"]).unwrap()
});

pub static COUNTERS_3: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec("near_test_counters_3", "Just counters", &["shard_id"]).unwrap()
});

pub static COUNTERS_4: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec("near_test_counters_4", "Just counters", &["shard_id"]).unwrap()
});

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn benchmark_counter_vec_with_label_values() {
    const NUM_ITERATIONS: usize = 1_000_000;
    const NUM_SHARDS: usize = 8;
    let start = Clock::real().now();
    for _ in 0..NUM_ITERATIONS {
        for shard_id in 0..NUM_SHARDS {
            COUNTERS_1.with_label_values(&[&format!("{}", shard_id)]).inc();
        }
    }
    let time_per_inc =
        start.elapsed().as_seconds_f64() / (NUM_ITERATIONS as f64) / (NUM_SHARDS as f64);
    println!(
        "Time per inc() call when using `with_label_values()`: {} microseconds",
        time_per_inc * 1e6
    );
}

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn benchmark_counter_vec_cached() {
    const NUM_ITERATIONS: usize = 1_000_000;
    const NUM_SHARDS: usize = 8;
    let counters: Vec<IntCounter> = (0..NUM_SHARDS)
        .map(|shard_id| COUNTERS_2.with_label_values(&[&format!("{}", shard_id)]))
        .collect();
    let start = Clock::real().now();
    for _ in 0..NUM_ITERATIONS {
        for shard_id in 0..NUM_SHARDS {
            counters[shard_id].inc();
        }
    }
    let time_per_inc =
        start.elapsed().as_seconds_f64() / (NUM_ITERATIONS as f64) / (NUM_SHARDS as f64);
    println!("Time per inc() call when caching counters: {} microseconds", time_per_inc * 1e6);
}

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn benchmark_counter_vec_cached_str_but_not_counters() {
    const NUM_ITERATIONS: usize = 1_000_000;
    const NUM_SHARDS: usize = 8;
    let shard_ids: Vec<String> = (0..NUM_SHARDS).map(|shard_id| format!("{}", shard_id)).collect();
    let start = Clock::real().now();
    for _ in 0..NUM_ITERATIONS {
        for shard_id in 0..NUM_SHARDS {
            COUNTERS_3.with_label_values(&[&shard_ids[shard_id]]).inc();
        }
    }
    let time_per_inc =
        start.elapsed().as_seconds_f64() / (NUM_ITERATIONS as f64) / (NUM_SHARDS as f64);
    println!("Time per inc() call when caching strings: {} microseconds", time_per_inc * 1e6);
}

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn benchmark_counter_vec_cached_labels() {
    const NUM_ITERATIONS: usize = 1_000_000;
    const NUM_SHARDS: usize = 8;
    let shard_ids: Vec<String> = (0..NUM_SHARDS).map(|shard_id| format!("{}", shard_id)).collect();
    let labels_values: Vec<Vec<&str>> = (0..NUM_SHARDS)
        .map(|shard_id| {
            let v: Vec<&str> = vec![&shard_ids[shard_id]];
            v
        })
        .collect();
    let labels: Vec<&[&str]> = (0..NUM_SHARDS)
        .map(|shard_id| {
            let v: &[&str] = &labels_values[shard_id];
            v
        })
        .collect();
    let start = Clock::real().now();
    for _ in 0..NUM_ITERATIONS {
        for shard_id in 0..NUM_SHARDS {
            COUNTERS_4.with_label_values(&(labels[shard_id])).inc();
        }
    }
    let time_per_inc =
        start.elapsed().as_seconds_f64() / (NUM_ITERATIONS as f64) / (NUM_SHARDS as f64);
    println!("Time per inc() call when caching label values: {} microseconds", time_per_inc * 1e6);
}

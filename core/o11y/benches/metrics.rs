#[macro_use]
extern crate bencher;

use bencher::Bencher;
use near_o11y::metrics::{try_create_int_counter_vec, IntCounter, IntCounterVec};
use once_cell::sync::Lazy;

pub static COUNTERS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec("near_test_counters_1", "Just counters", &["shard_id"]).unwrap()
});

fn inc_counter_vec_with_label_values(bench: &mut Bencher) {
    const NUM_SHARDS: usize = 8;
    bench.iter(|| {
        for shard_id in 0..NUM_SHARDS {
            COUNTERS.with_label_values(&[&format!("{}", shard_id)]).inc();
        }
    });
}

fn inc_counter_vec_cached(bench: &mut Bencher) {
    const NUM_SHARDS: usize = 8;
    let counters: Vec<IntCounter> = (0..NUM_SHARDS)
        .map(|shard_id| COUNTERS.with_label_values(&[&format!("{}", shard_id)]))
        .collect();
    bench.iter(|| {
        for shard_id in 0..NUM_SHARDS {
            counters[shard_id].inc();
        }
    });
}

fn inc_counter_vec_cached_str(bench: &mut Bencher) {
    const NUM_SHARDS: usize = 8;
    let shard_ids: Vec<String> = (0..NUM_SHARDS).map(|shard_id| format!("{}", shard_id)).collect();
    bench.iter(|| {
        for shard_id in 0..NUM_SHARDS {
            COUNTERS.with_label_values(&[&shard_ids[shard_id]]).inc();
        }
    });
}

benchmark_group!(
    benches,
    inc_counter_vec_with_label_values,
    inc_counter_vec_cached_str,
    inc_counter_vec_cached,
);
benchmark_main!(benches);

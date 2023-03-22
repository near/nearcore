#[macro_use]
extern crate bencher;

use bencher::Bencher;
use near_o11y::metrics::{try_create_int_counter_vec, IntCounter, IntCounterVec};
use once_cell::sync::Lazy;

static COUNTERS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec("near_test_counters_1", "Just counters", &["shard_id"]).unwrap()
});

const NUM_SHARDS: usize = 8;

fn inc_counter_vec_with_label_values(bench: &mut Bencher) {
    bench.iter(|| {
        for shard_id in 0..NUM_SHARDS {
            COUNTERS.with_label_values(&[&shard_id.to_string()]).inc();
        }
    });
}

fn inc_counter_vec_with_label_values_itoa(bench: &mut Bencher) {
    bench.iter(|| {
        for shard_id in 0..NUM_SHARDS {
            let mut buffer = itoa::Buffer::new();
            let printed = buffer.format(shard_id);
            COUNTERS.with_label_values(&[printed]).inc();
        }
    });
}

fn inc_counter_vec_with_label_values_smartstring(bench: &mut Bencher) {
    use std::fmt::Write;
    bench.iter(|| {
        for shard_id in 0..NUM_SHARDS {
            let mut label = smartstring::alias::String::new();
            write!(label, "{shard_id}").unwrap();
            COUNTERS.with_label_values(&[&label]).inc();
        }
    });
}

fn inc_counter_vec_with_label_values_stack(bench: &mut Bencher) {
    use std::io::Write;
    bench.iter(|| {
        for shard_id in 0..NUM_SHARDS {
            let mut buf = [0u8; 10];
            let mut cursor = std::io::Cursor::new(&mut buf[..]);
            write!(cursor, "{shard_id}").unwrap();
            let len = cursor.position() as usize;
            let label = unsafe { std::str::from_utf8_unchecked(&buf[..len]) };
            COUNTERS.with_label_values(&[label]).inc();
        }
    });
}

fn inc_counter_vec_with_label_values_stack_no_format(bench: &mut Bencher) {
    bench.iter(|| {
        for shard_id in 0..NUM_SHARDS {
            let mut buf = [0u8; 16];
            let mut idx = buf.len();
            let mut n = shard_id;
            loop {
                idx -= 1;
                buf[idx] = b'0' + (n % 10) as u8;
                n = n / 10;
                if n == 0 {
                    break;
                }
            }
            let label = unsafe { std::str::from_utf8_unchecked(&buf[idx..]) };
            COUNTERS.with_label_values(&[label]).inc();
        }
    });
}

fn inc_counter_vec_cached(bench: &mut Bencher) {
    const NUM_SHARDS: usize = 8;
    let counters: Vec<IntCounter> = (0..NUM_SHARDS)
        .map(|shard_id| COUNTERS.with_label_values(&[&shard_id.to_string()]))
        .collect();
    bench.iter(|| {
        for counter in &counters {
            counter.inc();
        }
    });
}

fn inc_counter_vec_cached_str(bench: &mut Bencher) {
    const NUM_SHARDS: usize = 8;
    let shard_ids: Vec<String> = (0..NUM_SHARDS).map(|shard_id| shard_id.to_string()).collect();
    bench.iter(|| {
        for shard_id in &shard_ids {
            COUNTERS.with_label_values(&[shard_id]).inc();
        }
    });
}

benchmark_group!(
    benches,
    inc_counter_vec_with_label_values,
    inc_counter_vec_with_label_values_itoa,
    inc_counter_vec_with_label_values_smartstring,
    inc_counter_vec_with_label_values_stack,
    inc_counter_vec_with_label_values_stack_no_format,
    inc_counter_vec_cached_str,
    inc_counter_vec_cached,
);
benchmark_main!(benches);

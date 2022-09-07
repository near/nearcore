#[macro_use]
extern crate bencher;
#[macro_use]
extern crate metrics;

use bencher::Bencher;
use near_o11y::metrics::{try_create_int_counter_vec, IntCounter, IntCounterVec};
use once_cell::sync::Lazy;
use once_cell::unsync::OnceCell;

static COUNTERS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec("near_test_counters", "Just counters", &["shard_id"]).unwrap()
});

const NUM_SHARDS: usize = 8;

fn inc_counter_vec_with_label_values(bench: &mut Bencher) {
    bench.iter(|| {
        for shard_id in 0..NUM_SHARDS {
            COUNTERS.with_label_values(&[&format!("{}", shard_id)]).inc();
        }
    });
}

fn inc_counter_vec_with_label_values_to_string(bench: &mut Bencher) {
    bench.iter(|| {
        for shard_id in 0..NUM_SHARDS {
            COUNTERS.with_label_values(&[&shard_id.to_string()]).inc();
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

fn inc_counter_vec_itoa(bench: &mut Bencher) {
    const NUM_SHARDS: usize = 8;
    bench.iter(|| {
        for shard_id in 0..NUM_SHARDS {
            let mut buffer = itoa::Buffer::new();
            let printed = buffer.format(shard_id);
            COUNTERS.with_label_values(&[&printed]).inc();
        }
    });
}

fn inc_counter_vec_cached_lazy(bench: &mut Bencher) {
    const NUM_SHARDS: usize = 8;
    let lazy_counters: Vec<OnceCell<IntCounter>> = vec![OnceCell::new(); NUM_SHARDS];
    bench.iter(|| {
        for shard_id in 0..NUM_SHARDS {
            lazy_counters[shard_id]
                .get_or_init(|| COUNTERS.with_label_values(&[&format!("{}", shard_id)]))
                .inc();
        }
    });
}

fn inc_counter_vec_metrics1(bench: &mut Bencher) {
    const NUM_SHARDS: usize = 8;
    bench.iter(|| {
        for shard_id in 0..NUM_SHARDS {
            increment_counter!("near_test_counters", "shard_id" => format!("{}",shard_id));
        }
    });
}

fn inc_counter_vec_metrics2(bench: &mut Bencher) {
    const NUM_SHARDS: usize = 8;
    bench.iter(|| {
        for shard_id in 0..NUM_SHARDS {
            let labels = [("shard_id", format!("{}", shard_id))];
            increment_counter!("near_test_counters", &labels);
        }
    });
}

benchmark_group!(
    benches,
    inc_counter_vec_with_label_values,
    inc_counter_vec_with_label_values_to_string,
    inc_counter_vec_with_label_values_smartstring,
    inc_counter_vec_with_label_values_stack,
    inc_counter_vec_with_label_values_stack_no_format,
    inc_counter_vec_cached_str,
    inc_counter_vec_cached,
    inc_counter_vec_itoa,
    inc_counter_vec_cached_lazy,
    inc_counter_vec_metrics1,
    inc_counter_vec_metrics2,
);
benchmark_main!(benches);

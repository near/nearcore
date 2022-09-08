#[macro_use]
extern crate bencher;
#[macro_use]
extern crate metrics;

use bencher::Bencher;
use near_o11y::metrics::{try_create_int_counter_vec, IntCounter, IntCounterVec};
use metrics_exporter_prometheus::PrometheusBuilder;
use once_cell::sync::Lazy;
use once_cell::unsync::OnceCell;

const NUM_SHARDS: usize = 8;

fn inc_counter_vec_metrics1(bench: &mut Bencher) {
    let builder = PrometheusBuilder::new();
    builder.install().expect("failed to install recorder/exporter");
    // let builder = PrometheusBuilder::new();
    // builder.install().expect("failed to install recorder/exporter");
    // let handle = builder.install_recorder().expect("failed to install recorder");

    const NUM_SHARDS: usize = 8;
    for shard_id in 0..NUM_SHARDS {
        register_counter!("near_test_counters_2", "shard_id" => format!("{}",shard_id));
    }
    // let before = handle.render();
    bench.iter(|| {
        for shard_id in 0..NUM_SHARDS {
            increment_counter!("near_test_counters_2", "shard_id" => format!("{}",shard_id));
        }
    });
    // let after = handle.render();
    // assert_ne!(before,after);
}

benchmark_group!(
    benches,
    inc_counter_vec_metrics1,
);
benchmark_main!(benches);

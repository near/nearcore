use std::sync::atomic::AtomicU32;
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use near_chain::ApplyChunksSpawner;
use near_chain::runtime::apply_chunk_test_utils::{
    self, TestApplyChunkParams, test_apply_new_chunk_impl, test_apply_new_chunk_setup,
};

fn bench_apply_chunk(c: &mut Criterion) {
    let mut group = c.benchmark_group("slow group");
    group.sample_size(20); // reduces sample count

    let params = TestApplyChunkParams {
        num_txs_per_chunk: 4000 * 20,
        num_shards: 20,
        num_accounts: 50000 * 20,
    };
    let setup = test_apply_new_chunk_setup(params);
    //let verbose = false;

    let do_once = AtomicU32::new(1);
    group.bench_function("apply_chunk", |b| {
        b.iter(|| {
            let verbose = do_once.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) == 1;
            test_apply_new_chunk_impl(&setup, verbose);
        });
    });
    group.finish();
}

fn bench_apply_chunk_parallel(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply_chunk_parallel");
    let params = TestApplyChunkParams {
        num_txs_per_chunk: 4000 * 20,
        num_shards: 20,
        num_accounts: 50000 * 20,
    };
    let setup = test_apply_new_chunk_setup(params);

    let spawner = ApplyChunksSpawner::Default.into_spawner(20);
    let runtime = setup.env.runtime.clone();
    group.bench_function("apply_chunk_parallel", move |b| {
        let runtime = runtime.clone();
        let spawner = spawner.clone();

        b.iter_batched(
            || {
                setup.new_case(
                    vec![
                        (0, Duration::from_millis(0)),
                        (1, Duration::from_millis(0)),
                        (2, Duration::from_millis(0)),
                        (3, Duration::from_millis(0)),
                        (4, Duration::from_millis(0)),
                        (5, Duration::from_millis(0)),
                        (6, Duration::from_millis(0)),
                    ],
                    vec![(7, Duration::from_millis(0))],
                )
            },
            move |case| {
                apply_chunk_test_utils::run(runtime.clone(), case, spawner.clone());
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_apply_chunk_parallel);
criterion_main!(benches);

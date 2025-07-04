use std::sync::atomic::AtomicU32;

use criterion::{Criterion, criterion_group, criterion_main};
use near_chain::runtime::apply_chunk_test_utils::{
    TestApplyChunkParams, test_apply_new_chunk_impl, test_apply_new_chunk_setup,
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

criterion_group!(benches, bench_apply_chunk);
criterion_main!(benches);

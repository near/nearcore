use bencher::{benchmark_group, benchmark_main, Bencher};
use near_chain::{ChainGenesis, RuntimeAdapter};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::NumBlocks;
use near_store::create_store;
use neard::config::GenesisExt;
use neard::get_store_path;
use state_viewer::gc::GCRuntime;
use std::sync::Arc;
use tempfile::TempDir;

fn setup(num_blocks: NumBlocks, num_tx_per_block: u64) -> (TestEnv, TempDir) {
    let mut genesis = Genesis::test(vec!["test0"], 1);
    let epoch_length = 5;
    genesis.config.epoch_length = epoch_length;
    let dir = tempfile::Builder::new().prefix("gc").tempdir().unwrap();
    let store = create_store(&get_store_path(&dir.path()));
    let nightshade_runtime =
        neard::NightshadeRuntime::new(dir.path(), store, Arc::new(genesis.clone()), vec![], vec![]);
    let runtime_adapter: Arc<dyn RuntimeAdapter> = Arc::new(GCRuntime::new(nightshade_runtime, 0));
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![runtime_adapter];
    let mut env = TestEnv::new_with_runtime(ChainGenesis::test(), 1, 1, runtimes, true);
    let signer = InMemorySigner::from_seed("test0", KeyType::ED25519, "test0");
    let mut nonce = 1;
    let mut last_block_hash = env.clients[0].chain.head().unwrap().last_block_hash;
    for i in 1..num_blocks {
        for _ in 0..num_tx_per_block {
            let tx = SignedTransaction::create_account(
                nonce,
                "test0".to_string(),
                format!("test{}", nonce),
                10u128.pow(22),
                signer.public_key(),
                &signer,
                last_block_hash,
            );
            env.clients[0].process_tx(tx, false, false);
            nonce += 1;
        }
        env.produce_block(0, i);
        last_block_hash = env.clients[0].chain.head().unwrap().last_block_hash;
    }
    (env, dir)
}

fn benchmark_gc_1_block(bench: &mut Bencher) {
    let mut head_height = 10000;
    let (mut test_env, _dir) = setup(head_height, 10);
    let gc_num_blocks = 1;
    bench.iter(move || {
        for _ in 0..gc_num_blocks {
            test_env.produce_block(0, head_height);
            head_height += 1;
        }
        let tries = test_env.clients[0].runtime_adapter.get_tries();
        test_env.clients[0].chain.clear_data(tries, gc_num_blocks).unwrap();
    })
}

fn benchmark_gc_2_block(bench: &mut Bencher) {
    let mut head_height = 10000;
    let (mut test_env, _dir) = setup(head_height, 10);
    let gc_num_blocks = 2;
    bench.iter(move || {
        for _ in 0..gc_num_blocks {
            test_env.produce_block(0, head_height);
            head_height += 1;
        }
        let tries = test_env.clients[0].runtime_adapter.get_tries();
        test_env.clients[0].chain.clear_data(tries, gc_num_blocks).unwrap();
    })
}

fn benchmark_gc_10_block(bench: &mut Bencher) {
    let mut head_height = 10000;
    let (mut test_env, _dir) = setup(head_height, 10);
    let gc_num_blocks = 10;
    bench.iter(move || {
        for _ in 0..gc_num_blocks {
            test_env.produce_block(0, head_height);
            head_height += 1;
        }
        let tries = test_env.clients[0].runtime_adapter.get_tries();
        test_env.clients[0].chain.clear_data(tries, gc_num_blocks).unwrap();
    })
}

fn benchmark_gc_100_block(bench: &mut Bencher) {
    let mut head_height = 10000;
    let (mut test_env, _dir) = setup(head_height, 10);
    let gc_num_blocks = 100;
    bench.iter(move || {
        for _ in 0..gc_num_blocks {
            test_env.produce_block(0, head_height);
            head_height += 1;
        }
        let tries = test_env.clients[0].runtime_adapter.get_tries();
        test_env.clients[0].chain.clear_data(tries, gc_num_blocks).unwrap();
    })
}

benchmark_group!(
    benches,
    benchmark_gc_1_block,
    benchmark_gc_2_block,
    benchmark_gc_10_block,
    benchmark_gc_100_block
);
benchmark_main!(benches);

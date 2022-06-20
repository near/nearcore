#[macro_use]
extern crate bencher;

use bencher::Bencher;
use near_chain::{ChainStore, ChainStoreAccess, RuntimeAdapter};
use near_chain_configs::GenesisValidationMode;
use near_logger_utils::init_integration_logger;
use near_primitives::types::StateRoot;
use near_store::{db::Mode, TrieIterator};
use nearcore::{get_default_home, load_config, NightshadeRuntime};
use std::time::{Duration, Instant};

/// Read `TrieItem`s - nodes containing values - using Trie iterator, stop when 10k items were read.
/// Note that the first run populates OS caches, so all next runs will be faster. You may want to run
/// `sudo sh -c "/usr/bin/echo 1 > /proc/sys/vm/drop_caches"` before running the benchmark.
/// As of 25/03/2022, it shows the following results for both read-only and read-write modes:
/// ```
/// took on avg 6.169248ms op per sec 162 items read 10000
/// took on avg 1.424615ms op per sec 701 items read 10000
/// took on avg 1.416562ms op per sec 705 items read 10000
/// ```
fn read_trie_items(bench: &mut Bencher, shard_id: usize, mode: Mode) {
    init_integration_logger();
    let home_dir = get_default_home();
    let near_config = load_config(&home_dir, GenesisValidationMode::UnsafeFast)
        .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));
    let num_trie_items = 10_000;

    bench.iter(move || {
        tracing::info!(target: "neard", "{:?}", home_dir);
        let store = near_store::Store::opener(&home_dir, &near_config.config.store)
            .mode(mode.clone())
            .open();

        let chain_store =
            ChainStore::new(store.clone(), near_config.genesis.config.genesis_height, true);

        let runtime = NightshadeRuntime::from_config(&home_dir, store, &near_config);
        let head = chain_store.head().unwrap();
        let last_block = chain_store.get_block(&head.last_block_hash).unwrap();
        let state_roots: Vec<StateRoot> =
            last_block.chunks().iter().map(|chunk| chunk.prev_state_root()).collect();
        let header = last_block.header();

        let state_root = state_roots[shard_id];
        let trie = runtime.get_trie_for_shard(shard_id as u64, header.prev_hash()).unwrap();
        let trie = TrieIterator::new(&trie, &state_root).unwrap();

        let start = Instant::now();
        let num_items_read = trie
            .enumerate()
            .map(|(i, _)| {
                if i % 500 == 0 {
                    tracing::info!(target: "neard", "{}", i)
                }
            })
            .take(num_trie_items)
            .count();
        let took = start.elapsed();

        println!(
            "took on avg {:?} op per sec {} items read {}",
            took / (num_items_read as u32),
            (num_items_read as u128) * Duration::from_secs(1).as_nanos() / took.as_nanos(),
            num_items_read
        );
    });
}

/// Read first 10k trie items from shard 0.
fn read_trie_items_10k(bench: &mut Bencher) {
    read_trie_items(bench, 0, Mode::ReadWrite);
}

/// Read first 10k trie items from shard 0 in read-only mode.
fn read_trie_items_10k_read_only(bench: &mut Bencher) {
    // Read trie items until 10k items found from shard 0.
    read_trie_items(bench, 0, Mode::ReadOnly);
}

benchmark_group!(benches, read_trie_items_10k, read_trie_items_10k_read_only);

benchmark_main!(benches);

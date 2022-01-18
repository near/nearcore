#[macro_use]
extern crate bencher;

use bencher::Bencher;
use near_chain::{ChainStore, ChainStoreAccess, RuntimeAdapter};
use near_chain_configs::GenesisValidationMode;
use near_primitives::types::StateRoot;
use near_store::{create_store, TrieIterator};
use nearcore::{get_default_home, get_store_path, load_config, NightshadeRuntime};
use std::time::{Duration, Instant};

/// Read `TrieItem`s - nodes containing values - using Trie iterator, stop when `num_trie_items` items were read.
fn read_trie_items(bench: &mut Bencher, num_trie_items: usize, shard_id: usize) {
    let home_dir = get_default_home();
    let near_config = load_config(&home_dir, GenesisValidationMode::UnsafeFast);

    bench.iter(move || {
        let store = create_store(&get_store_path(&home_dir));

        let mut chain_store =
            ChainStore::new(store.clone(), near_config.genesis.config.genesis_height);

        let runtime = NightshadeRuntime::with_config(
            &home_dir,
            store,
            &near_config,
            None,
            near_config.client_config.max_gas_burnt_view,
        );
        let head = chain_store.head().unwrap();
        let last_block = chain_store.get_block(&head.last_block_hash).unwrap().clone();
        let state_roots: Vec<StateRoot> =
            last_block.chunks().iter().map(|chunk| chunk.prev_state_root()).collect();
        let header = last_block.header();

        let state_root = state_roots[shard_id];
        let trie = runtime.get_trie_for_shard(shard_id as u64, header.prev_hash()).unwrap();
        let trie = TrieIterator::new(&trie, &state_root).unwrap();

        let start = Instant::now();
        let _ = trie
            .enumerate()
            .map(|(i, _)| {
                if i % 100 == 0 {
                    tracing::info!(target: "neard", "{}", i)
                }
            })
            .take(num_trie_items);
        let took = start.elapsed();

        println!(
            "took on avg {:?} op per sec {} assuming {} items",
            took / (num_trie_items as u32),
            (num_trie_items as u128) * Duration::from_secs(1).as_nanos() / took.as_nanos(),
            num_trie_items
        );
    });
}

fn read_trie_items_10k(bench: &mut Bencher) {
    // Read trie items until 10k items found from shard 0.
    read_trie_items(bench, 10_000, 0);
}

benchmark_group!(benches, read_trie_items_10k);

benchmark_main!(benches);

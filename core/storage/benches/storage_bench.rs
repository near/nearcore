#[macro_use]
extern crate bencher;
extern crate rand;

use bencher::Bencher;

extern crate storage;

use std::sync::Arc;

use std::path::Path;
use std::sync::RwLock;
use storage::create_storage;
use storage::BeaconChainStorage;
use storage::ShardChainStorage;
use primitives::beacon::SignedBeaconBlock;
use primitives::hash::CryptoHash;
use storage::storages::GenericStorage;

const TMP_DIR: &str = "./tmp_bench/";

fn get_storage(
    test_name: &str,
) -> (Arc<RwLock<BeaconChainStorage>>, Arc<RwLock<ShardChainStorage>>) {
    let mut base_path = Path::new(TMP_DIR).to_owned();
    base_path.push(test_name);
    if base_path.exists() {
        std::fs::remove_dir_all(base_path.clone()).unwrap();
    }
    let (beacon_chain, mut shard_chains) = create_storage(base_path.to_str().unwrap(), 1);
    let shard_chain = shard_chains.pop().unwrap();
    (beacon_chain, shard_chain)
}

fn storage_save_block(bench: &mut Bencher) {
    let (beacon_chain, _) = get_storage("storage_save");
    let mut blocks = vec![];
    let mut prev_hash = CryptoHash::default();
    for i in 0..10 {
        let block = SignedBeaconBlock::new(i, prev_hash, vec![], CryptoHash::default());
        prev_hash = block.hash;
        if i == 0 {
            beacon_chain.write().unwrap().blockchain_storage_mut().set_genesis(block.clone()).unwrap();
        }
        blocks.push(block);
    }
    bench.iter(move || {
        for b in blocks.drain(..) {
            beacon_chain.write().unwrap().blockchain_storage_mut().add_block(b).unwrap();
        }
    });
}

fn storage_save_get_block(bench: &mut Bencher) {
    let (beacon_chain, _) = get_storage("storage_save");
    let mut blocks_hashes = vec![];
    let mut prev_hash = CryptoHash::default();
    for i in 0..10 {
        let block = SignedBeaconBlock::new(i, prev_hash, vec![], CryptoHash::default());
        prev_hash = block.hash;
        blocks_hashes.push(block.hash);
        if i == 0 {
            beacon_chain.write().unwrap().blockchain_storage_mut().set_genesis(block.clone()).unwrap();
        }
        beacon_chain.write().unwrap().blockchain_storage_mut().add_block(block).unwrap();
    }
    bench.iter(move || {
        for h in &blocks_hashes {
            beacon_chain.write().unwrap().blockchain_storage_mut().block(h).unwrap();
        }
    });
}

benchmark_group!(benches, storage_save_block, storage_save_get_block);
benchmark_main!(benches);

#[macro_use]
extern crate bencher;

use bencher::{black_box, Bencher};
use near_primitives::borsh::maybestd::sync::Arc;
use near_primitives::errors::StorageError;
use near_store::db::DBCol::ColBlockMerkleTree;
use near_store::{create_store, Store};
use std::time::{Duration, Instant};

// try to write to db `10m` keys and then read all of them in random order
fn benchmark_write_then_read_successful(bench: &mut Bencher) {
    let store = create_store_in_random_folder();
    let num_keys = 10000000;
    let keys = generate_keys(num_keys);
    write_to_db(&store, &keys);

    bench.iter(move || {
        let start = Instant::now();

        let read_records = read_from_db(&store, &keys);
        let took = start.elapsed();
        println!(
            "took on avg {:?} op per sec {} got {}/{}",
            took / (num_keys as u32),
            (num_keys as u128) * Duration::from_secs(1).as_nanos() / took.as_nanos(),
            read_records,
            keys.len()
        );
    });
}

// create `Store` in a random folder
fn create_store_in_random_folder() -> Arc<Store> {
    let tmp_dir = tempfile::Builder::new().prefix("_test_clear_column").tempdir().unwrap();
    let store = create_store(tmp_dir.path());
    store
}

// generate `count` random 40 bytes keys
fn generate_keys(count: usize) -> Vec<Vec<u8>> {
    let mut res: Vec<Vec<u8>> = Vec::new();
    for _k in 0..count {
        let key: Vec<u8> = (0..40).map(|_| rand::random::<u8>()).collect();

        res.push(key)
    }
    res
}

// read from DB keys in random order
fn read_from_db(store: &Arc<Store>, keys: &Vec<Vec<u8>>) -> usize {
    let mut read = 0;
    for _k in 0..keys.len() {
        let r = rand::random::<u32>() % (keys.len() as u32);
        let key = &keys[r as usize];

        let val = store
            .get(ColBlockMerkleTree, key.as_ref())
            .map_err(|_| StorageError::StorageInternalError);

        if let Ok(Some(x)) = val {
            black_box(x);
            read += 1;
        }
    }
    read
}

// write a value of random size between 0..333 for each key to db.
fn write_to_db(store: &Arc<Store>, keys: &[Vec<u8>]) {
    let mut store_update = store.store_update();
    for key in keys.iter() {
        let x: u32 = rand::random::<u32>() % 333;
        let val: Vec<u8> = (0..x).map(|_| rand::random::<u8>()).collect();
        store_update.set(ColBlockMerkleTree, key.as_slice().clone(), &val);
    }
    store_update.commit().unwrap();
}

benchmark_group!(benches, benchmark_write_then_read_successful);

benchmark_main!(benches);

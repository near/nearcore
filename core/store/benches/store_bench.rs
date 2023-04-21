#[macro_use]
extern crate bencher;

use bencher::{black_box, Bencher};
use near_primitives::errors::StorageError;
use near_store::{DBCol, NodeStorage, Store};
use std::time::{Duration, Instant};

/// Run a benchmark to generate `num_keys` keys, each of size `key_size`, then write then
/// in random order to column `col` in store, and then read keys back from `col` in random order.
/// Works only for column configured without reference counting, that is `.is_rc() == false`.
fn benchmark_write_then_read_successful(
    bench: &mut Bencher,
    num_keys: usize,
    key_size: usize,
    max_value_size: usize,
    col: DBCol,
) {
    let tmp_dir = tempfile::tempdir().unwrap();
    // Use default StoreConfig rather than NodeStorage::test_opener so we’re using the
    // same configuration as in production.
    let store = NodeStorage::opener(tmp_dir.path(), false, &Default::default(), None)
        .open()
        .unwrap()
        .get_hot_store();
    let keys = generate_keys(num_keys, key_size);
    write_to_db(&store, &keys, max_value_size, col);

    bench.iter(move || {
        let start = Instant::now();

        let read_records = read_from_db(&store, &keys, col);
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

/// Generate `count` keys of `key_size` length.
fn generate_keys(count: usize, key_size: usize) -> Vec<Vec<u8>> {
    let mut res: Vec<Vec<u8>> = Vec::new();
    for _k in 0..count {
        let key: Vec<u8> = (0..key_size).map(|_| rand::random::<u8>()).collect();

        res.push(key)
    }
    res
}

/// Read from DB value for given `kyes` in random order for `col`.
/// Works only for column configured without reference counting, that is `.is_rc() == false`.
fn read_from_db(store: &Store, keys: &[Vec<u8>], col: DBCol) -> usize {
    let mut read = 0;
    for _k in 0..keys.len() {
        let r = rand::random::<u32>() % (keys.len() as u32);
        let key = &keys[r as usize];

        let val = store.get(col, key.as_ref()).map_err(|_| StorageError::StorageInternalError);

        if let Ok(Some(x)) = val {
            black_box(x);
            read += 1;
        }
    }
    read
}

/// Write random value of size between `0` and `max_value_size` to given `keys` at specific column
/// `col.`
/// Works only for column configured without reference counting, that is `.is_rc() == false`.
fn write_to_db(store: &Store, keys: &[Vec<u8>], max_value_size: usize, col: DBCol) {
    let mut store_update = store.store_update();
    for key in keys.iter() {
        let x: usize = rand::random::<usize>() % max_value_size;
        let val: Vec<u8> = (0..x).map(|_| rand::random::<u8>()).collect();
        // NOTE:  this
        store_update.set(col, &key, &val);
    }
    store_update.commit().unwrap();
}

fn benchmark_write_then_read_successful_10m(bench: &mut Bencher) {
    // By adding logs, I've seen a lot of write to keys with size 40, an values with sizes
    // between 10 .. 333.
    // NOTE: DBCol::BlockMerkleTree was chosen to be a column, where `.is_rc() == false`.
    benchmark_write_then_read_successful(bench, 10_000_000, 40, 333, DBCol::BlockMerkleTree);
}

benchmark_group!(benches, benchmark_write_then_read_successful_10m);

benchmark_main!(benches);

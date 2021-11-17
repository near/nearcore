#[macro_use]
extern crate bencher;

use bencher::{black_box, Bencher};
use std::time::{Duration, Instant};

use near_network::routing::ibf_peer_set::SlotMapId;
use near_network::routing::ibf_set::IbfSet;
use near_primitives::errors::StorageError;
use near_store::create_store;
use near_store::db::DBCol::ColState;

#[allow(dead_code)]
fn test_measure_adding_edges_to_ibf(bench: &mut Bencher) {
    bench.iter(|| {
        let mut a = IbfSet::<u64>::new(12);
        for i in 0..40 * 8 * 3 {
            a.add_edge(&(i as u64), (i + 1000000) as SlotMapId);
        }
    });
}

fn benchmark_db(bench: &mut Bencher) {
    let tmp_dir = tempfile::Builder::new().prefix("_test_clear_column").tempdir().unwrap();

    let store = create_store(tmp_dir.path());

    // 331 75 50 46

    let num_keys = 10000000;
    let mut store_update = store.store_update();
    let mut keys: Vec<Vec<u8>> = Vec::new();
    for _x in 0..num_keys {
        let key: Vec<u8> = (0..40).map(|_| rand::random::<u8>()).collect();

        let x: u32 = rand::random();
        let x = x % 333;
        let val: Vec<u8> = (0..x).map(|_| rand::random::<u8>()).collect();
        store_update.set(ColState, key.as_slice().clone(), &val);
        //}
        //    for _x in 0..num_keys {
        //     let key: Vec<u8> = (0..40).map(|_| rand::random::<u8>()).collect();
        keys.push(key);
    }
    store_update.commit().unwrap();

    // .. let keys = Arc::new(keys);

    bench.iter(move || {
        let start = Instant::now();
        // ..let keys = keys.clone();
        let mut got = 0;
        for _k in 0..keys.len() {
            let r = rand::random::<u32>() % (keys.len() as u32);
            let key = &keys[r as usize];

            let val =
                store.get(ColState, key.as_ref()).map_err(|_| StorageError::StorageInternalError);

            if let Ok(Some(x)) = val {
                // there is a bug, only half of entries were returned :/
                //println!("{:?}", val);
                //            val.unwrap().unwrap();
                black_box(x);
                got += 1;
            }
        }
        let took = start.elapsed();
        println!(
            "took on avg {:?} op per sec {} got {}/{}",
            took / (num_keys as u32),
            (num_keys as u128) * Duration::from_secs(1).as_nanos() / took.as_nanos(),
            got,
            keys.len()
        );
    });
}

benchmark_group!(benches, benchmark_db);

benchmark_main!(benches);

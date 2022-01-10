use std::io::prelude::*;

use rand::Rng;
use rocksdb::DB;

use crate::{config::Config, gas_cost::GasCost};

const MEMTABLE_SIZE: u64 = 256 * bytesize::MIB;

pub(crate) fn rocks_db_sequential_inserts_cost(
    config: &Config,
    force_compaction: bool,
    value_size: usize,
    inserts: usize,
) -> GasCost {
    let tmp_dir = tempfile::TempDir::new().expect("Failed to create directory for temp DB");

    let db = new_empty_db(&tmp_dir);

    const DATA_SIZE: usize = bytesize::MIB as usize;
    let data = input_data(config, DATA_SIZE);

    // Build up a store that includes at least 3 layers in the underlying LSM tree
    // Since we use the same target size on all layers, writing one memtable worth of data will create *at least* one node (usually substantially more due to meta data and overhead).
    // Using 8 memtables, I got a reasonably structured tree like this (before compaction):
    // 3 files at level 0
    // 8 files at level 1
    // 16 files at level 2
    let setup_inserts = 8 * MEMTABLE_SIZE as usize / value_size;
    sequential_inserts(setup_inserts, value_size, &data, 0, &db, force_compaction);

    if config.debug_rocksb {
        println!("# After setup:");
        print_levels_info(&db);
    }

    // Wait for potential background threads to terminate:
    std::thread::sleep(std::time::Duration::from_millis(500));
    // Note on better solutions:
    // Thread info would be ideal but is not exposed on C interface. (http://rocksdb.org/blog/2015/10/27/getthreadlist.html)
    // Cancelling all bkg threads as blow almost works. But it initiates shutdown, which means the DB is no longer usable afterwards.
    // db.cancel_all_background_work(true);

    let gas_counter = GasCost::measure(config.metric);

    sequential_inserts(inserts, value_size, &data, setup_inserts, &db, force_compaction);

    let cost = gas_counter.elapsed();

    if config.debug_rocksb {
        println!("# Cost: {:?}", cost);
        print_levels_info(&db);
    }

    drop(db);
    tmp_dir.close().expect("Could not clean up temp DB");

    // Store generated input data in a file for reproducibility of any results
    if config.pr_data_path.is_none() {
        let mut stats_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open("names-to-stats.txt")
            .unwrap();
        let stats_num = std::io::BufReader::new(&stats_file).lines().count();
        let data_dump_path = format!("data_dump_{:<04}.bin", stats_num);

        let mut dump = std::fs::File::create(&data_dump_path).unwrap();
        dump.write_all(&data).unwrap();

        writeln!(stats_file, "# DATA {} written to {}", stats_num, data_dump_path)
            .expect("Writing to \"names-to-stats.txt\" failed");
    }

    cost
}

/// Sequentially insert a number of generated key-value pairs and flushes
///
/// Keys are {"1", "2", ... } starting at `key_offset`
/// Values are different slices taken from `input_data`
fn sequential_inserts(
    inserts: usize,
    value_size: usize,
    input_data: &[u8],
    key_offset: usize,
    db: &DB,
    force_compaction: bool,
) {
    for i in 0..inserts {
        let key = (key_offset + i).to_string();
        let start = (i * value_size) % (input_data.len() - value_size);
        let value = &input_data[start..(start + value_size)];
        db.put(&key, value).expect("Put failed");
    }
    db.flush().expect("Flush failed");
    if force_compaction {
        db.compact_range::<&[u8], &[u8]>(None, None);
    }
}

fn input_data(config: &Config, data_size: usize) -> Vec<u8> {
    let mut data = vec![0u8; data_size];
    if let Some(path) = config.pr_data_path.as_ref() {
        let mut input = std::fs::File::open(path).unwrap();
        input.read_exact(&mut data).unwrap();
    } else {
        rand::thread_rng().fill(data.as_mut_slice());
    }
    data
}

fn new_empty_db(tmp_dir: impl AsRef<std::path::Path>) -> DB {
    let mut opts = rocksdb::Options::default();

    opts.create_if_missing(true);

    // Options as used in nearcore
    opts.set_bytes_per_sync(bytesize::MIB);
    opts.set_write_buffer_size(MEMTABLE_SIZE as usize);
    opts.set_max_bytes_for_level_base(MEMTABLE_SIZE);

    // Using default (as in nearcore)
    // opts.set_target_file_size_multiplier(1);

    // Simplify DB a bit for more consistency:
    // * Only have one memtable at the time
    opts.set_max_write_buffer_number(1);
    // * Never slow down writes due to increased number of L0 files
    opts.set_level_zero_slowdown_writes_trigger(-1);

    rocksdb::DB::open(&opts, tmp_dir).expect("Failed to create RocksDB")
}

fn print_levels_info(db: &DB) {
    for n in 0..3 {
        let int =
            db.property_int_value(&format!("rocksdb.num-files-at-level{}", n)).unwrap().unwrap();
        println!("{} files at level {}", int, n);
    }
}

use std::io::prelude::*;

use rand::{prelude::SliceRandom, Rng};
use rand_xorshift::XorShiftRng;
use rocksdb::DB;

use crate::{config::Config, gas_cost::GasCost};

const MEMTABLE_SIZE: u64 = 256 * bytesize::MIB;
const INPUT_DATA_BUFFER_SIZE: usize = bytesize::MIB as usize;

/// The goal is to build up a store that stretches all 3 layers in the underlying LSM tree
/// Since we use the same target size on all layers, writing one memtable worth of data will create *at least* one node (usually substantially more due to meta data and overhead).
/// Using 8 memtables, I got a reasonably structured tree like this (before compaction):
/// 3 files at level 0
/// 8 files at level 1
/// 16 files at level 2
const SETUP_INSERTION_BYTES: u64 = 8 * MEMTABLE_SIZE;

const SETUP_PRANDOM_SEED: u64 = 0x1d9f5711fc8b0117;
const ANOTHER_PRANDOM_SEED: u64 = 0x0465b6733af62af0;

pub(crate) fn rocks_db_inserts_cost(
    config: &Config,
    force_compaction: bool,
    sequential: bool,
    value_size: usize,
    inserts: usize,
) -> GasCost {
    let data = input_data(config, INPUT_DATA_BUFFER_SIZE);
    let tmp_dir = tempfile::TempDir::new().expect("Failed to create directory for temp DB");
    let db = new_test_db(&tmp_dir, &data, value_size, force_compaction);

    if config.debug_rocksb {
        println!("# After setup / before measurement:");
        print_levels_info(&db);
    }

    let gas_counter = GasCost::measure(config.metric);

    if sequential {
        sequential_inserts(
            inserts,
            value_size,
            &data,
            SETUP_INSERTION_BYTES as usize / value_size,
            &db,
            force_compaction,
        );
    } else {
        prandom_inserts(inserts, value_size, &data, ANOTHER_PRANDOM_SEED, &db, force_compaction);
    }

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

pub(crate) fn rocks_db_read_cost(
    config: &Config,
    force_compaction: bool,
    sequential: bool,
    value_size: usize,
    ops: usize,
) -> GasCost {
    let tmp_dir = tempfile::TempDir::new().expect("Failed to create directory for temp DB");
    let data = input_data(config, INPUT_DATA_BUFFER_SIZE);
    let db = new_test_db(&tmp_dir, &data, value_size, force_compaction);

    if config.debug_rocksb {
        println!("# After setup / before measurement:");
        print_levels_info(&db);
    }

    let keys_available = SETUP_INSERTION_BYTES as usize / value_size;
    let mut prng: XorShiftRng = rand::SeedableRng::seed_from_u64(SETUP_PRANDOM_SEED);
    let mut keys: Vec<usize> = (0..keys_available).map(|_| prng.gen()).collect();
    if sequential {
        keys.sort();
    } else {
        // give it another shuffle to make lookup order different from insertion order
        keys.shuffle(&mut prng);
    }

    let gas_counter = GasCost::measure(config.metric);

    for i in 0..ops {
        let key = keys[i % keys.len()];
        db.get(&key.to_string()).unwrap();
    }

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

/// Insert a number of generated key-value pairs and flushes
///
/// Keys are pseudo-random and deterministic based on the seed
/// Values are different slices taken from `input_data`
fn prandom_inserts(
    inserts: usize,
    value_size: usize,
    input_data: &[u8],
    key_seed: u64,
    db: &DB,
    force_compaction: bool,
) {
    let mut prng: XorShiftRng = rand::SeedableRng::seed_from_u64(key_seed);
    for i in 0..inserts {
        let key = prng.gen::<u64>().to_string();
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

fn new_test_db(
    db_dir: impl AsRef<std::path::Path>,
    data: &[u8],
    value_size: usize,
    force_compaction: bool,
) -> DB {
    let mut opts = rocksdb::Options::default();

    opts.create_if_missing(true);

    // Options as used in nearcore
    opts.set_bytes_per_sync(bytesize::MIB);
    opts.set_write_buffer_size(MEMTABLE_SIZE as usize);
    opts.set_max_bytes_for_level_base(MEMTABLE_SIZE);

    // Simplify DB a bit for more consistency:
    // * Only have one memtable at the time
    opts.set_max_write_buffer_number(1);
    // * Never slow down writes due to increased number of L0 files
    opts.set_level_zero_slowdown_writes_trigger(-1);

    // TODO: Maybe add option to enable/disable cache
    // let mut block_opts = rocksdb::BlockBasedOptions::default();
    // block_opts.disable_cache();
    // opts.set_block_based_table_factory(&block_opts);

    let db = rocksdb::DB::open(&opts, db_dir).expect("Failed to create RocksDB");

    prandom_inserts(
        SETUP_INSERTION_BYTES as usize / value_size,
        value_size,
        &data,
        SETUP_PRANDOM_SEED,
        &db,
        force_compaction,
    );

    db
}

fn print_levels_info(db: &DB) {
    for n in 0..3 {
        let int =
            db.property_int_value(&format!("rocksdb.num-files-at-level{}", n)).unwrap().unwrap();
        println!("{} files at level {}", int, n);
    }
}

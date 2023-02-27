use rand::{prelude::SliceRandom, Rng};
use rand_xorshift::XorShiftRng;
use rocksdb::DB;
use std::{io::prelude::*, iter, path::PathBuf};

use crate::{config::Config, gas_cost::GasCost};

#[derive(Debug, Clone, clap::Parser)]
pub struct RocksDBTestConfig {
    /// Value size used for all DB operations in RocksDB tests
    /// (`RocksDb*` estimations only)
    #[clap(name = "rdb-value-size", long, default_value = "1000")]
    pub value_size: usize,
    /// Number of insertions/reads performed in RocksDB tests
    /// (`RocksDb*` estimations only)
    #[clap(name = "rdb-op-count", long, default_value = "1000000")]
    pub op_count: usize,
    /// Size of memtable used in RocksDB tests
    /// (`RocksDb*` estimations only)
    #[clap(name = "rdb-memtable-size", long, default_value = "256000000")]
    pub memtable_size: usize,
    /// Number of insertions into test DB before measurement begins
    /// (`RocksDb*` estimations only)
    #[clap(name = "rdb-setup-insertions", long, default_value = "2000000")]
    pub setup_insertions: usize,
    /// Keys will be ordered sequentially if this is set, randomized otherwise.
    /// (`RocksDb*` estimations only)
    #[clap(name = "rdb-sequential-keys", long)]
    pub sequential_keys: bool,
    /// Flush after a bulk of write operations.
    /// The flush time will be included in the reported measurement.
    /// (`RocksDb*` estimations only)
    #[clap(long, name = "rdb-force-flush", long)]
    pub force_flush: bool,
    /// Force compactions after a bulk of operations.
    /// The compaction time will be included in the reported measurement.
    /// (`RocksDb*` estimations only)
    #[clap(long, name = "rdb-force-compaction", long)]
    pub force_compaction: bool,
    /// Enable the default block cache used for reads, disabled by default.
    /// (`RocksDb*` estimations only)
    #[clap(long, name = "rdb-block-cache", long)]
    pub block_cache: bool,
    /// Print RocksDB debug output where available
    #[clap(skip)]
    pub debug_rocksdb: bool,
    /// Pseudo-random input data dump
    /// (`RocksDb*` estimations only)
    #[clap(long, name = "rdb-input-data-path", long)]
    pub input_data_path: Option<PathBuf>,
    /// Drop OS cache before measurements for better IO accuracy.
    #[clap(skip)]
    pub drop_os_cache: bool,
}

// These tests make use of reproducible pseud-randomness.
// Two different strategies are used for keys and data values.
//
// > Keys: XorShiftRng with an initial seed value to produce a series of pseudo-random keys
// > Values: A buffer of random bytes is loaded into memory.
//           The values are slices from this buffer at different offsets.
//           The initial buffer can be dynamically generated from thread_rng or loaded from a dump from previous runs.
//
// The rational behind this setup is to have random keys/values readily available during benchmarks without consuming much memory or CPU time.

const SETUP_PRANDOM_SEED: u64 = 0x1d9f5711fc8b0117;
const ANOTHER_PRANDOM_SEED: u64 = 0x0465b6733af62af0;
const INPUT_DATA_BUFFER_SIZE: usize = (bytesize::MIB as usize) - 1;

pub(crate) fn rocks_db_inserts_cost(config: &Config) -> GasCost {
    let db_config = &config.rocksdb_test_config;
    let data = input_data(db_config, INPUT_DATA_BUFFER_SIZE);
    let tmp_dir = tempfile::TempDir::new().expect("Failed to create directory for temp DB");
    let db = new_test_db(&tmp_dir, &data, &db_config);

    if db_config.debug_rocksdb {
        eprintln!("# {:?}", db_config);
        println!("# After setup / before measurement:");
        print_levels_info(&db);
    }

    let gas_counter = GasCost::measure(config.metric);

    if db_config.sequential_keys {
        sequential_inserts(
            db_config.op_count,
            db_config.value_size,
            &data,
            db_config.setup_insertions,
            &db,
            db_config.force_compaction,
            db_config.force_flush,
        );
    } else {
        prandom_inserts(
            db_config.op_count,
            db_config.value_size,
            &data,
            ANOTHER_PRANDOM_SEED,
            &db,
            db_config.force_compaction,
            db_config.force_flush,
        );
    }

    let cost = gas_counter.elapsed();

    if db_config.debug_rocksdb {
        println!("# Cost: {:?}", cost);
        print_levels_info(&db);
    }

    drop(db);
    tmp_dir.close().expect("Could not clean up temp DB");

    if db_config.input_data_path.is_none() {
        backup_input_data(&data);
    }

    cost
}

pub(crate) fn rocks_db_read_cost(config: &Config) -> GasCost {
    let db_config = &config.rocksdb_test_config;
    let tmp_dir = tempfile::TempDir::new().expect("Failed to create directory for temp DB");
    let data = input_data(db_config, INPUT_DATA_BUFFER_SIZE);
    let db = new_test_db(&tmp_dir, &data, &db_config);

    if db_config.debug_rocksdb {
        eprintln!("# {:?}", db_config);
        println!("# After setup / before measurement:");
        print_levels_info(&db);
    }

    let mut prng: XorShiftRng = rand::SeedableRng::seed_from_u64(SETUP_PRANDOM_SEED);
    let mut keys: Vec<usize> =
        iter::repeat_with(|| prng.gen()).take(db_config.setup_insertions).collect();
    if db_config.sequential_keys {
        keys.sort();
    } else {
        // give it another shuffle to make lookup order different from insertion order
        keys.shuffle(&mut prng);
    }

    let gas_counter = GasCost::measure(config.metric);

    for i in 0..db_config.op_count {
        let key = keys[i as usize % keys.len()];
        db.get(&key.to_string()).unwrap();
    }

    let cost = gas_counter.elapsed();

    if db_config.debug_rocksdb {
        println!("# Cost: {:?}", cost);
        print_levels_info(&db);
    }

    drop(db);
    tmp_dir.close().expect("Could not clean up temp DB");

    if db_config.input_data_path.is_none() {
        backup_input_data(&data);
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
    force_flush: bool,
) {
    for i in 0..inserts {
        let key = (key_offset + i).to_string();
        let start = (i * value_size) % (input_data.len() - value_size);
        let value = &input_data[start..(start + value_size)];
        db.put(&key, value).expect("Put failed");
    }
    if force_flush {
        db.flush().expect("Flush failed");
    }
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
    force_flush: bool,
) {
    let mut prng: XorShiftRng = rand::SeedableRng::seed_from_u64(key_seed);
    for i in 0..inserts {
        let key = prng.gen::<u64>().to_string();
        let start = (i * value_size) % (input_data.len() - value_size);
        let value = &input_data[start..(start + value_size)];
        db.put(&key, value).expect("Put failed");
    }
    if force_flush {
        db.flush().expect("Flush failed");
    }
    if force_compaction {
        db.compact_range::<&[u8], &[u8]>(None, None);
    }
}

fn input_data(db_config: &RocksDBTestConfig, data_size: usize) -> Vec<u8> {
    if let Some(path) = &db_config.input_data_path {
        let data = std::fs::read(path).unwrap();
        assert_eq!(data.len(), data_size, "Provided input file has wrong size");
        data
    } else {
        let mut data = vec![0u8; data_size];
        rand::thread_rng().fill(data.as_mut_slice());
        data
    }
}

/// Store generated input data in a file for reproducibility of any results
fn backup_input_data(data: &[u8]) {
    let mut stats_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .append(true)
        .create(true)
        .open("names-to-stats.txt")
        .unwrap();
    let stats_num = std::io::BufReader::new(&stats_file).lines().count();
    let data_dump_path = format!("data_dump_{:<04}.bin", stats_num);

    std::fs::write(&data_dump_path, data).unwrap();
    writeln!(stats_file, "# DATA {} written to {}", stats_num, data_dump_path)
        .expect("Writing to \"names-to-stats.txt\" failed");
}

fn new_test_db(
    db_dir: impl AsRef<std::path::Path>,
    data: &[u8],
    db_config: &RocksDBTestConfig,
) -> DB {
    let mut opts = rocksdb::Options::default();

    opts.create_if_missing(true);

    // Options as used in nearcore
    opts.set_bytes_per_sync(bytesize::MIB);
    opts.set_write_buffer_size(db_config.memtable_size);
    opts.set_max_bytes_for_level_base(db_config.memtable_size as u64);

    // Simplify DB a bit for more consistency:
    // * Only have one memtable at the time
    opts.set_max_write_buffer_number(1);
    // * Never slow down writes due to increased number of L0 files
    opts.set_level_zero_slowdown_writes_trigger(-1);

    if !db_config.block_cache {
        let mut block_opts = rocksdb::BlockBasedOptions::default();
        block_opts.disable_cache();
        opts.set_block_based_table_factory(&block_opts);
    }

    let db = rocksdb::DB::open(&opts, db_dir).expect("Failed to create RocksDB");

    prandom_inserts(
        db_config.setup_insertions,
        db_config.value_size,
        &data,
        SETUP_PRANDOM_SEED,
        &db,
        db_config.force_compaction,
        true, // always force-flush in setup
    );

    #[cfg(target_os = "linux")]
    if db_config.drop_os_cache {
        crate::utils::clear_linux_page_cache().expect(
            "Failed to drop OS caches. Are you root and is /proc mounted with write access?",
        );
    }

    db
}

fn print_levels_info(db: &DB) {
    for n in 0..3 {
        let int =
            db.property_int_value(&format!("rocksdb.num-files-at-level{}", n)).unwrap().unwrap();
        println!("{} files at level {}", int, n);
    }
}

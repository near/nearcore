use super::StoreConfig;
use crate::db::refcount::merge_refcounted_records;
use crate::DBCol;
use near_primitives::version::DbVersion;
use once_cell::sync::Lazy;
use rocksdb::checkpoint::Checkpoint;
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamily, ColumnFamilyDescriptor, Direction, Env, IteratorMode,
    Options, ReadOptions, WriteBatch, DB,
};
use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Condvar, Mutex, RwLock};
use std::{cmp, fmt};
use strum::EnumCount;
use tracing::{error, info, warn};

pub(crate) mod refcount;

#[derive(Debug, Clone, PartialEq)]
pub struct DBError(String);

impl fmt::Display for DBError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for DBError {}

impl From<rocksdb::Error> for DBError {
    fn from(err: rocksdb::Error) -> Self {
        DBError(err.into_string())
    }
}

impl From<DBError> for io::Error {
    fn from(err: DBError) -> io::Error {
        io::Error::new(io::ErrorKind::Other, err)
    }
}

pub const HEAD_KEY: &[u8; 4] = b"HEAD";
pub const TAIL_KEY: &[u8; 4] = b"TAIL";
pub const CHUNK_TAIL_KEY: &[u8; 10] = b"CHUNK_TAIL";
pub const FORK_TAIL_KEY: &[u8; 9] = b"FORK_TAIL";
pub const HEADER_HEAD_KEY: &[u8; 11] = b"HEADER_HEAD";
pub const FINAL_HEAD_KEY: &[u8; 10] = b"FINAL_HEAD";
pub const LATEST_KNOWN_KEY: &[u8; 12] = b"LATEST_KNOWN";
pub const LARGEST_TARGET_HEIGHT_KEY: &[u8; 21] = b"LARGEST_TARGET_HEIGHT";
pub const VERSION_KEY: &[u8; 7] = b"VERSION";
pub const GENESIS_JSON_HASH_KEY: &[u8; 17] = b"GENESIS_JSON_HASH";
pub const GENESIS_STATE_ROOTS_KEY: &[u8; 19] = b"GENESIS_STATE_ROOTS";

pub(crate) struct DBTransaction {
    pub(crate) ops: Vec<DBOp>,
}

pub(crate) enum DBOp {
    Insert { col: DBCol, key: Vec<u8>, value: Vec<u8> },
    UpdateRefcount { col: DBCol, key: Vec<u8>, value: Vec<u8> },
    Delete { col: DBCol, key: Vec<u8> },
    DeleteAll { col: DBCol },
}

impl DBTransaction {
    pub(crate) fn insert(&mut self, col: DBCol, key: Vec<u8>, value: Vec<u8>) {
        self.ops.push(DBOp::Insert { col, key, value });
    }

    pub(crate) fn update_refcount(&mut self, col: DBCol, key: Vec<u8>, value: Vec<u8>) {
        self.ops.push(DBOp::UpdateRefcount { col, key, value });
    }

    pub(crate) fn delete(&mut self, col: DBCol, key: Vec<u8>) {
        self.ops.push(DBOp::Delete { col, key });
    }

    pub(crate) fn delete_all(&mut self, col: DBCol) {
        self.ops.push(DBOp::DeleteAll { col });
    }

    pub(crate) fn merge(&mut self, other: DBTransaction) {
        self.ops.extend(other.ops)
    }
}

pub struct RocksDB {
    db: DB,
    db_opt: Options,
    cfs: Vec<*const ColumnFamily>,

    check_free_space_counter: std::sync::atomic::AtomicU16,
    check_free_space_interval: u16,
    free_space_threshold: bytesize::ByteSize,

    // RAII-style of keeping track of the number of instances of RocksDB in a global variable.
    _instance_counter: InstanceCounter,
}

// DB was already Send+Sync. cf and read_options are const pointers using only functions in
// this file and safe to share across threads.
unsafe impl Send for RocksDB {}
unsafe impl Sync for RocksDB {}

fn col_name(col: DBCol) -> String {
    format!("col{}", col as usize)
}

/// Ensures that NOFILE limit can accommodate `max_open_files` plus some small margin
/// of file descriptors.
///
/// A RocksDB instance can keep up to the configured `max_open_files` number of
/// file descriptors.  In addition, we need handful more for other processing
/// (such as network sockets to name just one example).  If NOFILE is too small
/// opening files may start failing which would prevent us from operating
/// correctly.
///
/// To avoid such failures, this method ensures that NOFILE limit is large
/// enough to accommodate `max_open_file` plus another 1000 file descriptors.
/// If current limit is too low, it will attempt to set it to a higher value.
///
/// Returns error if NOFILE limit could not be read or set.  In practice the
/// only thing that can happen is hard limit being too low such that soft limit
/// cannot be increased to required value.
fn ensure_max_open_files_limit(max_open_files: u32) -> Result<(), DBError> {
    let required = max_open_files as u64 + 1000;
    let (soft, hard) = rlimit::Resource::NOFILE.get().map_err(|err| {
        DBError(format!("Unable to get limit for the number of open files (NOFILE): {err}"))
    })?;
    if required <= soft {
        Ok(())
    } else {
        rlimit::Resource::NOFILE.set(required, hard).map_err(|err| {
            DBError(format!(
                "Unable to set limit for the number of open files (NOFILE) to \
                 {required} (for configured max_open_files={max_open_files}): \
                 {err}"
            ))
        })
    }
}

impl RocksDB {
    /// Opens the database either in read only or in read/write mode depending
    /// on the read_only parameter specified in the store_config.
    pub fn open(path: impl AsRef<Path>, store_config: &StoreConfig) -> Result<RocksDB, DBError> {
        use strum::IntoEnumIterator;

        ensure_max_open_files_limit(store_config.max_open_files)?;

        let (db, db_opt) = if store_config.read_only {
            Self::open_read_only(path.as_ref(), store_config)
        } else {
            Self::open_read_write(path.as_ref(), store_config)
        }?;

        let cfs = DBCol::iter()
            .map(|col| db.cf_handle(&col_name(col)).unwrap() as *const ColumnFamily)
            .collect();
        Ok(Self {
            db,
            db_opt,
            cfs,
            check_free_space_interval: 256,
            check_free_space_counter: std::sync::atomic::AtomicU16::new(0),
            free_space_threshold: bytesize::ByteSize::mb(16),
            _instance_counter: InstanceCounter::new(),
        })
    }

    /// Opens a read only database.
    fn open_read_only(path: &Path, store_config: &StoreConfig) -> Result<(DB, Options), DBError> {
        use strum::IntoEnumIterator;
        let options = rocksdb_options(store_config);
        let cf_with_opts =
            DBCol::iter().map(|col| (col_name(col), rocksdb_column_options(col, store_config)));
        let db = DB::open_cf_with_opts_for_read_only(&options, path, cf_with_opts, false)?;
        Ok((db, options))
    }

    /// Opens the database in read/write mode.
    fn open_read_write(path: &Path, store_config: &StoreConfig) -> Result<(DB, Options), DBError> {
        use strum::IntoEnumIterator;
        let mut options = rocksdb_options(store_config);
        if store_config.enable_statistics {
            options = enable_statistics(options);
        }
        let cf_descriptors = DBCol::iter()
            .map(|col| {
                ColumnFamilyDescriptor::new(
                    col_name(col),
                    rocksdb_column_options(col, store_config),
                )
            })
            .collect::<Vec<_>>();
        let db = DB::open_cf_descriptors(&options, path, cf_descriptors)?;
        if cfg!(feature = "single_thread_rocksdb") {
            // These have to be set after open db
            let mut env = Env::default().unwrap();
            env.set_bottom_priority_background_threads(0);
            env.set_high_priority_background_threads(0);
            env.set_low_priority_background_threads(0);
            env.set_background_threads(0);
            println!("Disabled all background threads in rocksdb");
        }
        Ok((db, options))
    }
}

pub struct TestDB {
    db: RwLock<Vec<HashMap<Vec<u8>, Vec<u8>>>>,
}

pub(crate) trait Database: Sync + Send {
    fn transaction(&self) -> DBTransaction {
        DBTransaction { ops: Vec::new() }
    }
    fn get(&self, col: DBCol, key: &[u8]) -> Result<Option<Vec<u8>>, DBError>;
    fn iter<'a>(&'a self, column: DBCol) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a>;
    fn iter_raw_bytes<'a>(
        &'a self,
        column: DBCol,
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a>;
    fn iter_prefix<'a>(
        &'a self,
        col: DBCol,
        key_prefix: &'a [u8],
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a>;
    fn write(&self, batch: DBTransaction) -> Result<(), DBError>;
    fn as_rocksdb(&self) -> Option<&RocksDB> {
        None
    }
    fn get_store_statistics(&self) -> Option<StoreStatistics> {
        None
    }
}

impl Database for RocksDB {
    fn get(&self, col: DBCol, key: &[u8]) -> Result<Option<Vec<u8>>, DBError> {
        let read_options = rocksdb_read_options();
        let result = self.db.get_cf_opt(unsafe { &*self.cfs[col as usize] }, key, &read_options)?;
        Ok(RocksDB::get_with_rc_logic(col, result))
    }

    fn iter_raw_bytes<'a>(
        &'a self,
        col: DBCol,
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        let read_options = rocksdb_read_options();
        unsafe {
            let cf_handle = &*self.cfs[col as usize];
            let iterator = self.db.iterator_cf_opt(cf_handle, read_options, IteratorMode::Start);
            Box::new(iterator)
        }
    }

    fn iter<'a>(&'a self, col: DBCol) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        let read_options = rocksdb_read_options();
        unsafe {
            let cf_handle = &*self.cfs[col as usize];
            let iterator = self.db.iterator_cf_opt(cf_handle, read_options, IteratorMode::Start);
            RocksDB::iter_with_rc_logic(col, iterator)
        }
    }

    fn iter_prefix<'a>(
        &'a self,
        col: DBCol,
        key_prefix: &'a [u8],
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        // NOTE: There is no Clone implementation for ReadOptions, so we cannot really reuse
        // `self.read_options` here.
        let mut read_options = rocksdb_read_options();
        read_options.set_prefix_same_as_start(true);
        unsafe {
            let cf_handle = &*self.cfs[col as usize];
            // This implementation is copied from RocksDB implementation of `prefix_iterator_cf` since
            // there is no `prefix_iterator_cf_opt` method.
            let iterator = self
                .db
                .iterator_cf_opt(
                    cf_handle,
                    read_options,
                    IteratorMode::From(key_prefix, Direction::Forward),
                )
                .take_while(move |(key, _value)| key.starts_with(key_prefix));
            RocksDB::iter_with_rc_logic(col, iterator)
        }
    }

    fn write(&self, transaction: DBTransaction) -> Result<(), DBError> {
        if let Err(check) = self.pre_write_check() {
            if check.is_io() {
                warn!("unable to verify remaing disk space: {:?}, continueing write without verifying (this may result in unrecoverable data loss if disk space is exceeded", check)
            } else {
                panic!("{:?}", check)
            }
        }

        let mut batch = WriteBatch::default();
        for op in transaction.ops {
            match op {
                DBOp::Insert { col, key, value } => unsafe {
                    batch.put_cf(&*self.cfs[col as usize], key, value);
                },
                DBOp::UpdateRefcount { col, key, value } => unsafe {
                    batch.merge_cf(&*self.cfs[col as usize], key, value);
                },
                DBOp::Delete { col, key } => unsafe {
                    batch.delete_cf(&*self.cfs[col as usize], key);
                },
                DBOp::DeleteAll { col } => {
                    let cf_handle = unsafe { &*self.cfs[col as usize] };
                    let opt_first = self.db.iterator_cf(cf_handle, IteratorMode::Start).next();
                    let opt_last = self.db.iterator_cf(cf_handle, IteratorMode::End).next();
                    assert_eq!(opt_first.is_some(), opt_last.is_some());
                    if let (Some((min_key, _)), Some((max_key, _))) = (opt_first, opt_last) {
                        batch.delete_range_cf(cf_handle, &min_key, &max_key);
                        // delete_range_cf deletes ["begin_key", "end_key"), so need one more delete
                        batch.delete_cf(cf_handle, max_key)
                    }
                }
            }
        }
        Ok(self.db.write(batch)?)
    }

    fn as_rocksdb(&self) -> Option<&RocksDB> {
        Some(self)
    }

    fn get_store_statistics(&self) -> Option<StoreStatistics> {
        if let Some(stats_str) = self.db_opt.get_statistics() {
            match parse_statistics(&stats_str) {
                Ok(parsed_statistics) => {
                    return Some(parsed_statistics);
                }
                Err(err) => {
                    warn!(target: "store", "Failed to parse store statistics: {:?}", err);
                }
            }
        }
        None
    }
}

impl Database for TestDB {
    fn get(&self, col: DBCol, key: &[u8]) -> Result<Option<Vec<u8>>, DBError> {
        let result = self.db.read().unwrap()[col as usize].get(key).cloned();
        Ok(RocksDB::get_with_rc_logic(col, result))
    }

    fn iter<'a>(&'a self, col: DBCol) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        let iterator = self.iter_raw_bytes(col);
        RocksDB::iter_with_rc_logic(col, iterator)
    }

    fn iter_raw_bytes<'a>(
        &'a self,
        col: DBCol,
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        let iterator = self.db.read().unwrap()[col as usize]
            .clone()
            .into_iter()
            .map(|(k, v)| (k.into_boxed_slice(), v.into_boxed_slice()));
        Box::new(iterator)
    }

    fn iter_prefix<'a>(
        &'a self,
        col: DBCol,
        key_prefix: &'a [u8],
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        RocksDB::iter_with_rc_logic(
            col,
            self.iter(col).filter(move |(key, _value)| key.starts_with(key_prefix)),
        )
    }

    fn write(&self, transaction: DBTransaction) -> Result<(), DBError> {
        let mut db = self.db.write().unwrap();
        for op in transaction.ops {
            match op {
                DBOp::Insert { col, key, value } => {
                    db[col as usize].insert(key, value);
                }
                DBOp::UpdateRefcount { col, key, value } => {
                    let mut val = db[col as usize].get(&key).cloned().unwrap_or_default();
                    merge_refcounted_records(&mut val, &value);
                    if !val.is_empty() {
                        db[col as usize].insert(key, val);
                    } else {
                        db[col as usize].remove(&key);
                    }
                }
                DBOp::Delete { col, key } => {
                    db[col as usize].remove(&key);
                }
                DBOp::DeleteAll { col } => db[col as usize].clear(),
            };
        }
        Ok(())
    }
}

fn set_compression_options(opts: &mut Options) {
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
    // RocksDB documenation says that 16KB is a typical dictionary size.
    // We've empirically tuned the dicionary size to twice of that 'typical' size.
    // Having train data size x100 from dictionary size is a recommendation from RocksDB.
    // See: https://rocksdb.org/blog/2021/05/31/dictionary-compression.html?utm_source=dbplatz
    let dict_size = 2 * 16384;
    let max_train_bytes = dict_size * 100;
    // We use default parameters of RocksDB here:
    //      window_bits is -14 and is unused (Zlib-specific parameter),
    //      compression_level is 32767 meaning the default compression level for ZSTD,
    //      compression_strategy is 0 and is unused (Zlib-specific parameter).
    // See: https://github.com/facebook/rocksdb/blob/main/include/rocksdb/advanced_options.h#L176:
    opts.set_bottommost_compression_options(
        /*window_bits */ -14, /*compression_level */ 32767,
        /*compression_strategy */ 0, dict_size, /*enabled */ true,
    );
    opts.set_bottommost_zstd_max_train_bytes(max_train_bytes, true);
}

/// DB level options
fn rocksdb_options(store_config: &StoreConfig) -> Options {
    let mut opts = Options::default();

    set_compression_options(&mut opts);
    opts.create_missing_column_families(true);
    opts.create_if_missing(true);
    opts.set_use_fsync(false);
    opts.set_max_open_files(store_config.max_open_files.try_into().unwrap_or(i32::MAX));
    opts.set_keep_log_file_num(1);
    opts.set_bytes_per_sync(bytesize::MIB);
    opts.set_write_buffer_size(256 * bytesize::MIB as usize);
    opts.set_max_bytes_for_level_base(256 * bytesize::MIB);
    if cfg!(feature = "single_thread_rocksdb") {
        opts.set_disable_auto_compactions(true);
        opts.set_max_background_jobs(0);
        opts.set_stats_dump_period_sec(0);
        opts.set_stats_persist_period_sec(0);
        opts.set_level_zero_slowdown_writes_trigger(-1);
        opts.set_level_zero_file_num_compaction_trigger(-1);
        opts.set_level_zero_stop_writes_trigger(100000000);
    } else {
        opts.increase_parallelism(cmp::max(1, num_cpus::get() as i32 / 2));
        opts.set_max_total_wal_size(bytesize::GIB);
    }

    opts
}

pub fn enable_statistics(mut opts: Options) -> Options {
    // Rust API doesn't permit choosing stats level. The default stats level is
    // `kExceptDetailedTimers`, which is described as:
    // "Collects all stats except time inside mutex lock AND time spent on compression."
    opts.enable_statistics();
    // Disabling dumping stats to files because the stats are exported to Prometheus.
    opts.set_stats_persist_period_sec(0);
    opts.set_stats_dump_period_sec(0);

    opts
}

fn rocksdb_read_options() -> ReadOptions {
    let mut read_options = ReadOptions::default();
    read_options.set_verify_checksums(false);
    read_options
}

fn rocksdb_block_based_options(block_size: usize, cache_size: usize) -> BlockBasedOptions {
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_block_size(block_size);
    // We create block_cache for each of 47 columns, so the total cache size is 32 * 47 = 1504mb
    block_opts.set_block_cache(&Cache::new_lru_cache(cache_size).unwrap());
    block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
    block_opts.set_cache_index_and_filter_blocks(true);
    block_opts.set_bloom_filter(10.0, true);
    block_opts
}

fn choose_cache_size(col: DBCol, store_config: &StoreConfig) -> usize {
    match col {
        DBCol::State => store_config.col_state_cache_size,
        _ => 32 * 1024 * 1024,
    }
}

fn rocksdb_column_options(col: DBCol, store_config: &StoreConfig) -> Options {
    let mut opts = Options::default();
    set_compression_options(&mut opts);
    opts.set_level_compaction_dynamic_level_bytes(true);
    let cache_size = choose_cache_size(col, &store_config);
    opts.set_block_based_table_factory(&rocksdb_block_based_options(
        store_config.block_size,
        cache_size,
    ));

    // Note that this function changes a lot of rustdb parameters including:
    //      write_buffer_size = memtable_memory_budget / 4
    //      min_write_buffer_number_to_merge = 2
    //      max_write_buffer_number = 6
    //      level0_file_num_compaction_trigger = 2
    //      target_file_size_base = memtable_memory_budget / 8
    //      max_bytes_for_level_base = memtable_memory_budget
    //      compaction_style = kCompactionStyleLevel
    // Also it sets compression_per_level in a way that the first 2 levels have no compression and
    // the rest use LZ4 compression.
    // See the implementation here:
    //      https://github.com/facebook/rocksdb/blob/c18c4a081c74251798ad2a1abf83bad417518481/options/options.cc#L588.
    let memtable_memory_budget = 128 * bytesize::MIB as usize;
    opts.optimize_level_style_compaction(memtable_memory_budget);

    opts.set_target_file_size_base(64 * bytesize::MIB);
    if col.is_rc() {
        opts.set_merge_operator("refcount merge", RocksDB::refcount_merge, RocksDB::refcount_merge);
        opts.set_compaction_filter("empty value filter", RocksDB::empty_value_compaction_filter);
    }
    opts
}

// Number of RocksDB instances in the process.
pub(crate) static ROCKSDB_INSTANCES_COUNTER: Lazy<(Mutex<usize>, Condvar)> =
    Lazy::new(|| (Mutex::new(0), Condvar::new()));

impl RocksDB {
    /// Blocks until all RocksDB instances (usually 0 or 1) gracefully shutdown.
    pub fn block_until_all_instances_are_dropped() {
        let (lock, cvar) = &*ROCKSDB_INSTANCES_COUNTER;
        let mut num_instances = lock.lock().unwrap();
        while *num_instances != 0 {
            info!(target: "db", "Waiting for the {} remaining RocksDB instances to gracefully shutdown", *num_instances);
            num_instances = cvar.wait(num_instances).unwrap();
        }
        info!(target: "db", "All RocksDB instances performed a graceful shutdown");
    }

    /// Returns version of the database state on disk.
    pub fn get_version(path: &Path) -> Result<DbVersion, DBError> {
        let value = RocksDB::open(path, &StoreConfig::read_only())?
            .get(DBCol::DbVersion, VERSION_KEY)?
            .ok_or_else(|| {
                DBError(
                    "Failed to read database version; \
                     it’s not a neard database or database is corrupted."
                        .into(),
                )
            })?;
        serde_json::from_slice(&value).map_err(|_err| {
            DBError(format!(
                "Failed to parse database version: {value:?}; \
                 it’s not a neard database or database is corrupted."
            ))
        })
    }

    /// Checks if there is enough memory left to perform a write. Not having enough memory left can
    /// lead to difficult to recover from state, thus a PreWriteCheckErr is pretty much
    /// unrecoverable in most cases.
    fn pre_write_check(&self) -> Result<(), PreWriteCheckErr> {
        let counter = self.check_free_space_counter.fetch_add(1, Ordering::Relaxed);
        if self.check_free_space_interval >= counter {
            return Ok(());
        }
        self.check_free_space_counter.swap(0, Ordering::Relaxed);

        let available = available_space(self.db.path())?;

        if available < 16_u64 * self.free_space_threshold {
            warn!("remaining disk space is running low ({} left)", available);
        }

        if available < self.free_space_threshold {
            Err(PreWriteCheckErr::LowDiskSpace(available))
        } else {
            Ok(())
        }
    }

    /// Creates a Checkpoint object that can be used to actually create a checkpoint on disk.
    pub fn checkpoint(&self) -> Result<Checkpoint, DBError> {
        Checkpoint::new(&self.db).map_err(DBError::from)
    }

    /// Synchronously flush all Memtables to SST files on disk
    pub fn flush(&self) -> Result<(), DBError> {
        self.db.flush().map_err(DBError::from)
    }
}

fn available_space(path: &Path) -> io::Result<bytesize::ByteSize> {
    let available = fs2::available_space(path)?;
    Ok(bytesize::ByteSize::b(available))
}

#[derive(Debug, thiserror::Error)]
pub enum PreWriteCheckErr {
    #[error("error checking filesystem: {0}")]
    IO(#[from] std::io::Error),
    #[error("low disk memory ({0} available)")]
    LowDiskSpace(bytesize::ByteSize),
}

impl PreWriteCheckErr {
    pub fn is_io(&self) -> bool {
        matches!(self, PreWriteCheckErr::IO(_))
    }

    pub fn is_low_disk_space(&self) -> bool {
        matches!(self, PreWriteCheckErr::LowDiskSpace(_))
    }
}

impl Drop for RocksDB {
    fn drop(&mut self) {
        if cfg!(feature = "single_thread_rocksdb") {
            // RocksDB with only one thread stuck on wait some condition var
            // Turn on additional threads to proceed
            let mut env = Env::default().unwrap();
            env.set_background_threads(4);
        }
        self.db.cancel_all_background_work(true);
    }
}

// We've seen problems with RocksDB corruptions. InstanceCounter lets us gracefully shutdown the
// process letting RocksDB to finish all operations and leaving the instances in a valid
// non-corrupted state.
struct InstanceCounter {}

impl InstanceCounter {
    fn new() -> Self {
        let (lock, cvar) = &*ROCKSDB_INSTANCES_COUNTER;
        let mut num_instances = lock.lock().unwrap();
        *num_instances += 1;
        info!(target: "db", num_instances=%*num_instances, "Created a new RocksDB instance.");
        cvar.notify_all();
        Self {}
    }
}

impl Drop for InstanceCounter {
    fn drop(&mut self) {
        let (lock, cvar) = &*ROCKSDB_INSTANCES_COUNTER;
        let mut num_instances = lock.lock().unwrap();
        *num_instances -= 1;
        info!(target: "db", num_instances=%*num_instances, "Dropped a RocksDB instance.");
        cvar.notify_all();
    }
}

impl TestDB {
    pub fn new() -> Self {
        let db: Vec<_> = (0..DBCol::COUNT).map(|_| HashMap::new()).collect();
        Self { db: RwLock::new(db) }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StatsValue {
    Count(i64),
    Sum(i64),
    Percentile(u32, f64),
}

#[derive(Debug, PartialEq)]
pub struct StoreStatistics {
    pub data: Vec<(String, Vec<StatsValue>)>,
}

/// Parses a string containing RocksDB statistics.
fn parse_statistics(statistics: &str) -> Result<StoreStatistics, Box<dyn std::error::Error>> {
    let mut result = vec![];
    // Statistics are given one per line.
    for line in statistics.lines() {
        // Each line follows one of two formats:
        // 1) <stat_name> COUNT : <value>
        // 2) <stat_name> P50 : <value> P90 : <value> COUNT : <value> SUM : <value>
        // Each line gets split into words and we parse statistics according to this format.
        if let Some((stat_name, words)) = line.split_once(' ') {
            let mut values = vec![];
            let mut words = words.split(" : ").flat_map(|v| v.split(" "));
            while let (Some(key), Some(val)) = (words.next(), words.next()) {
                match key {
                    "COUNT" => values.push(StatsValue::Count(val.parse::<i64>()?)),
                    "SUM" => values.push(StatsValue::Sum(val.parse::<i64>()?)),
                    p if p.starts_with("P") => values.push(StatsValue::Percentile(
                        key[1..].parse::<u32>()?,
                        val.parse::<f64>()?,
                    )),
                    _ => {
                        warn!(target: "stats", "Unsupported stats value: {key} in {line}");
                    }
                }
            }
            result.push((stat_name.to_string(), values));
        }
    }
    Ok(StoreStatistics { data: result })
}
#[cfg(test)]
mod tests {
    use crate::db::StatsValue::{Count, Percentile, Sum};
    use crate::db::{parse_statistics, rocksdb_read_options, DBError, Database, RocksDB};
    use crate::{create_store, DBCol, StoreConfig, StoreStatistics};

    impl RocksDB {
        #[cfg(not(feature = "single_thread_rocksdb"))]
        fn compact(&self, col: DBCol) {
            self.db.compact_range_cf(
                unsafe { &*self.cfs[col as usize] },
                Option::<&[u8]>::None,
                Option::<&[u8]>::None,
            );
        }

        fn get_no_empty_filtering(
            &self,
            col: DBCol,
            key: &[u8],
        ) -> Result<Option<Vec<u8>>, DBError> {
            let read_options = rocksdb_read_options();
            let result =
                self.db.get_cf_opt(unsafe { &*self.cfs[col as usize] }, key, &read_options)?;
            Ok(result)
        }
    }

    #[test]
    fn test_prewrite_check() {
        let tmp_dir = tempfile::Builder::new().prefix("_test_prewrite_check").tempdir().unwrap();
        let store = RocksDB::open(tmp_dir.path(), &StoreConfig::read_write()).unwrap();
        store.pre_write_check().unwrap()
    }

    #[test]
    fn test_clear_column() {
        let tmp_dir = tempfile::Builder::new().prefix("_test_clear_column").tempdir().unwrap();
        let store = create_store(tmp_dir.path());
        assert_eq!(store.get(DBCol::State, &[1]).unwrap(), None);
        {
            let mut store_update = store.store_update();
            store_update.update_refcount(DBCol::State, &[1], &[1], 1);
            store_update.update_refcount(DBCol::State, &[2], &[2], 1);
            store_update.update_refcount(DBCol::State, &[3], &[3], 1);
            store_update.commit().unwrap();
        }
        assert_eq!(store.get(DBCol::State, &[1]).unwrap(), Some(vec![1]));
        {
            let mut store_update = store.store_update();
            store_update.delete_all(DBCol::State);
            store_update.commit().unwrap();
        }
        assert_eq!(store.get(DBCol::State, &[1]).unwrap(), None);
    }

    #[test]
    fn rocksdb_merge_sanity() {
        let tmp_dir = tempfile::Builder::new().prefix("_test_snapshot_sanity").tempdir().unwrap();
        let store = create_store(tmp_dir.path());
        let ptr = (&*store.storage) as *const (dyn Database + 'static);
        let rocksdb = unsafe { &*(ptr as *const RocksDB) };
        assert_eq!(store.get(DBCol::State, &[1]).unwrap(), None);
        {
            let mut store_update = store.store_update();
            store_update.update_refcount(DBCol::State, &[1], &[1], 1);
            store_update.commit().unwrap();
        }
        {
            let mut store_update = store.store_update();
            store_update.update_refcount(DBCol::State, &[1], &[1], 1);
            store_update.commit().unwrap();
        }
        assert_eq!(store.get(DBCol::State, &[1]).unwrap(), Some(vec![1]));
        assert_eq!(
            rocksdb.get_no_empty_filtering(DBCol::State, &[1]).unwrap(),
            Some(vec![1, 2, 0, 0, 0, 0, 0, 0, 0])
        );
        {
            let mut store_update = store.store_update();
            store_update.update_refcount(DBCol::State, &[1], &[1], -1);
            store_update.commit().unwrap();
        }
        assert_eq!(store.get(DBCol::State, &[1]).unwrap(), Some(vec![1]));
        assert_eq!(
            rocksdb.get_no_empty_filtering(DBCol::State, &[1]).unwrap(),
            Some(vec![1, 1, 0, 0, 0, 0, 0, 0, 0])
        );
        {
            let mut store_update = store.store_update();
            store_update.update_refcount(DBCol::State, &[1], &[1], -1);
            store_update.commit().unwrap();
        }
        // Refcount goes to 0 -> get() returns None
        assert_eq!(store.get(DBCol::State, &[1]).unwrap(), None);
        // Internally there is an empty value
        assert_eq!(rocksdb.get_no_empty_filtering(DBCol::State, &[1]).unwrap(), Some(vec![]));

        #[cfg(not(feature = "single_thread_rocksdb"))]
        {
            // single_thread_rocksdb makes compact hang forever
            rocksdb.compact(DBCol::State);
            rocksdb.compact(DBCol::State);

            // After compaction the empty value disappears
            assert_eq!(rocksdb.get_no_empty_filtering(DBCol::State, &[1]).unwrap(), None);
            assert_eq!(store.get(DBCol::State, &[1]).unwrap(), None);
        }
    }

    #[test]
    fn test_parse_statistics() {
        let statistics = "rocksdb.cold.file.read.count COUNT : 999\n\
         rocksdb.db.get.micros P50 : 9.171086 P95 : 222.678751 P99 : 549.611652 P100 : 45816.000000 COUNT : 917578 SUM : 38313754";
        let result = parse_statistics(statistics);
        assert_eq!(
            result.unwrap(),
            StoreStatistics {
                data: vec![
                    ("rocksdb.cold.file.read.count".to_string(), vec![Count(999)]),
                    (
                        "rocksdb.db.get.micros".to_string(),
                        vec![
                            Percentile(50, 9.171086),
                            Percentile(95, 222.678751),
                            Percentile(99, 549.611652),
                            Percentile(100, 45816.0),
                            Count(917578),
                            Sum(38313754)
                        ]
                    )
                ]
            }
        );
    }
}

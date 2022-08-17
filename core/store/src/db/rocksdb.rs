use std::io;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Condvar, Mutex};

use ::rocksdb::checkpoint::Checkpoint;
use ::rocksdb::{
    BlockBasedOptions, Cache, ColumnFamily, Direction, Env, IteratorMode, Options, ReadOptions,
    WriteBatch, DB,
};
use once_cell::sync::Lazy;
use strum::IntoEnumIterator;
use tracing::{error, info, warn};

use near_primitives::version::DbVersion;

use crate::config::Mode;
use crate::db::{refcount, DBIterator, DBOp, DBTransaction, Database, StatsValue};
use crate::{metrics, DBCol, StoreConfig, StoreStatistics};

/// List of integer RocskDB properties we’re reading when collecting statistics.
///
/// In the end, they are exported as Prometheus metrics.
pub const CF_STAT_NAMES: [&'static str; 1] = [::rocksdb::properties::LIVE_SST_FILES_SIZE];

pub struct RocksDB {
    db: DB,
    db_opt: Options,

    /// Map from [`DBCol`] to a column family handler in the RocksDB.
    ///
    /// Rather than accessing this field directly, use [`RocksDB::cf_handle`]
    /// method instead.  It returns `&ColumnFamily` which is what you usually
    /// want.
    cf_handles: enum_map::EnumMap<DBCol, std::ptr::NonNull<ColumnFamily>>,

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
fn ensure_max_open_files_limit(max_open_files: u32) -> Result<(), String> {
    let required = max_open_files as u64 + 1000;
    let (soft, hard) = rlimit::Resource::NOFILE.get().map_err(|err| {
        format!("Unable to get limit for the number of open files (NOFILE): {err}")
    })?;
    if required <= soft {
        Ok(())
    } else if required <= hard {
        rlimit::Resource::NOFILE.set(required, hard).map_err(|err| {
            format!(
                "Unable to change limit for the number of open files (NOFILE) \
                 from ({soft}, {hard}) to ({required}, {hard}) (for configured \
                 max_open_files={max_open_files}): {err}"
            )
        })
    } else {
        Err(format!(
            "Hard limit for the number of open files (NOFILE) is too low \
             ({hard}).  At least {required} is required (for configured \
             max_open_files={max_open_files}).  Set ‘ulimit -Hn’ accordingly \
             and restart the node."
        ))
    }
}

impl RocksDB {
    /// Opens the database either in read only or in read/write mode depending
    /// on the `mode` parameter specified in the store_config.
    pub fn open(path: &Path, store_config: &StoreConfig, mode: Mode) -> io::Result<RocksDB> {
        ensure_max_open_files_limit(store_config.max_open_files).map_err(other_error)?;
        let (db, db_opt) = Self::open_db(path, store_config, mode)?;
        let cf_handles = Self::get_cf_handles(&db);

        Ok(Self {
            db,
            db_opt,
            cf_handles,
            check_free_space_interval: 256,
            check_free_space_counter: std::sync::atomic::AtomicU16::new(0),
            free_space_threshold: bytesize::ByteSize::mb(16),
            _instance_counter: InstanceCounter::new(),
        })
    }

    /// Opens the database with all column families configured.
    fn open_db(path: &Path, store_config: &StoreConfig, mode: Mode) -> io::Result<(DB, Options)> {
        let options = rocksdb_options(store_config, mode);
        let cf_descriptors = DBCol::iter()
            .map(|col| {
                rocksdb::ColumnFamilyDescriptor::new(
                    col_name(col),
                    rocksdb_column_options(col, store_config),
                )
            })
            .collect::<Vec<_>>();
        let db = match mode {
            Mode::ReadOnly => {
                DB::open_cf_descriptors_read_only(&options, path, cf_descriptors, false)
            }
            Mode::ReadWrite => DB::open_cf_descriptors(&options, path, cf_descriptors),
        }
        .map_err(into_other)?;
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

    /// Returns mapping from [`DBCol`] to cf handle used with RocksDB calls.
    fn get_cf_handles(db: &DB) -> enum_map::EnumMap<DBCol, std::ptr::NonNull<ColumnFamily>> {
        let mut cf_handles = enum_map::EnumMap::default();
        for col in DBCol::iter() {
            let ptr = db
                .cf_handle(&col_name(col))
                .map_or(std::ptr::null(), |cf| cf as *const ColumnFamily);
            cf_handles[col] = std::ptr::NonNull::new(ptr as *mut ColumnFamily);
        }
        cf_handles.map(|col, ptr| {
            ptr.unwrap_or_else(|| {
                panic!("Missing cf handle for {}", col.variant_name());
            })
        })
    }

    /// Returns column family handler to use with RocsDB for given column.
    fn cf_handle(&self, col: DBCol) -> &ColumnFamily {
        let ptr = self.cf_handles[col];
        // SAFETY: The pointers are valid so long as self.db is valid.
        unsafe { ptr.as_ref() }
    }

    fn iter_raw_bytes_impl<'a>(
        &'a self,
        col: DBCol,
        prefix: Option<&'a [u8]>,
    ) -> RocksDBIterator<'a> {
        let cf_handle = self.cf_handle(col);
        let mut read_options = rocksdb_read_options();
        let mode = if let Some(prefix) = prefix {
            // prefix_same_as_start doesn’t do anything for us.  It takes effect
            // only if prefix extractor is configured for the column family
            // which is something we’re not doing.  Setting this option is
            // therefore pointless.
            //     read_options.set_prefix_same_as_start(true);

            // We’re running the iterator in From mode so there’s no need to set
            // the lower bound.
            //    read_options.set_iterate_lower_bound(key_prefix);

            // Upper bound is exclusive so if we set it to the next prefix
            // iterator will stop once keys no longer start with our desired
            // prefix.
            if let Some(upper) = next_prefix(prefix) {
                read_options.set_iterate_upper_bound(upper);
            }

            IteratorMode::From(prefix, Direction::Forward)
        } else {
            IteratorMode::Start
        };
        let iter = self.db.iterator_cf_opt(cf_handle, read_options, mode);
        RocksDBIterator(Some(iter))
    }
}

struct RocksDBIterator<'a>(Option<rocksdb::DBIteratorWithThreadMode<'a, DB>>);

impl<'a> Iterator for RocksDBIterator<'a> {
    type Item = io::Result<(Box<[u8]>, Box<[u8]>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let iter = self.0.as_mut()?;
        if let Some(item) = iter.next() {
            Some(Ok(item))
        } else {
            let status = iter.status();
            self.0 = None;
            status.err().map(into_other).map(Result::Err)
        }
    }
}

impl<'a> std::iter::FusedIterator for RocksDBIterator<'a> {}

impl Database for RocksDB {
    fn get_raw_bytes(&self, col: DBCol, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let timer =
            metrics::DATABASE_OP_LATENCY_HIST.with_label_values(&["get", col.into()]).start_timer();
        let read_options = rocksdb_read_options();
        let result =
            self.db.get_cf_opt(self.cf_handle(col), key, &read_options).map_err(into_other)?;
        timer.observe_duration();
        Ok(result)
    }

    fn iter_raw_bytes<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        Box::new(self.iter_raw_bytes_impl(col, None))
    }

    fn iter<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        refcount::iter_with_rc_logic(col, self.iter_raw_bytes_impl(col, None))
    }

    fn iter_prefix<'a>(&'a self, col: DBCol, key_prefix: &'a [u8]) -> DBIterator<'a> {
        let iter = self.iter_raw_bytes_impl(col, Some(key_prefix));
        refcount::iter_with_rc_logic(col, iter)
    }

    fn write(&self, transaction: DBTransaction) -> io::Result<()> {
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
                DBOp::Set { col, key, value } => {
                    batch.put_cf(self.cf_handle(col), key, value);
                }
                DBOp::Insert { col, key, value } => {
                    if cfg!(debug_assertions) {
                        if let Ok(Some(old_value)) = self.get_raw_bytes(col, &key) {
                            super::assert_no_overwrite(col, &key, &value, &*old_value)
                        }
                    }
                    batch.put_cf(self.cf_handle(col), key, value);
                }
                DBOp::UpdateRefcount { col, key, value } => {
                    batch.merge_cf(self.cf_handle(col), key, value);
                }
                DBOp::Delete { col, key } => {
                    batch.delete_cf(self.cf_handle(col), key);
                }
                DBOp::DeleteAll { col } => {
                    let cf_handle = self.cf_handle(col);
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
        self.db.write(batch).map_err(into_other)
    }

    fn flush(&self) -> io::Result<()> {
        self.db.flush().map_err(into_other)
    }

    /// Trying to get
    /// 1. RocksDB statistics
    /// 2. Selected RockdDB properties for column families
    fn get_store_statistics(&self) -> Option<StoreStatistics> {
        let mut result = StoreStatistics { data: vec![] };
        if let Some(stats_str) = self.db_opt.get_statistics() {
            if let Err(err) = parse_statistics(&stats_str, &mut result) {
                warn!(target: "store", "Failed to parse store statistics: {:?}", err);
            }
        }
        self.get_cf_statistics(&mut result);
        if result.data.is_empty() {
            None
        } else {
            Some(result)
        }
    }
}

/// Returns lowest value following largest value with given prefix.
///
/// In other words, computes upper bound for a prefix scan over list of keys
/// sorted in lexicographical order.  This means that a prefix scan can be
/// expressed as range scan over a right-open `[prefix, next_prefix(prefix))`
/// range.
///
/// For example, for prefix `foo` the function returns `fop`.
///
/// Returns `None` if there is no value which can follow value with given
/// prefix.  This happens when prefix consists entirely of `'\xff'` bytes (or is
/// empty).
fn next_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let ffs = prefix.iter().rev().take_while(|&&byte| byte == u8::MAX).count();
    let next = &prefix[..(prefix.len() - ffs)];
    if next.is_empty() {
        // Prefix consisted of \xff bytes.  There is no prefix that
        // follows it.
        None
    } else {
        let mut next = next.to_vec();
        *next.last_mut().unwrap() += 1;
        Some(next)
    }
}

/// DB level options
fn rocksdb_options(store_config: &StoreConfig, mode: Mode) -> Options {
    let read_write = matches!(mode, Mode::ReadWrite);
    let mut opts = Options::default();

    set_compression_options(&mut opts);
    opts.create_missing_column_families(true);
    opts.create_if_missing(read_write);
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
        opts.increase_parallelism(std::cmp::max(1, num_cpus::get() as i32 / 2));
        opts.set_max_total_wal_size(bytesize::GIB);
    }

    if read_write && store_config.enable_statistics {
        // Rust API doesn't permit choosing stats level. The default stats level
        // is `kExceptDetailedTimers`, which is described as: "Collects all
        // stats except time inside mutex lock AND time spent on compression."
        opts.enable_statistics();
        // Disabling dumping stats to files because the stats are exported to
        // Prometheus.
        opts.set_stats_persist_period_sec(0);
        opts.set_stats_dump_period_sec(0);
    }

    opts
}

fn rocksdb_read_options() -> ReadOptions {
    let mut read_options = ReadOptions::default();
    read_options.set_verify_checksums(false);
    read_options
}

fn rocksdb_block_based_options(
    block_size: bytesize::ByteSize,
    cache_size: bytesize::ByteSize,
    col: DBCol,
) -> BlockBasedOptions {
    let mut block_opts = BlockBasedOptions::default();
    // request no checksumming
    block_opts.set_checksum('\0');
    block_opts.set_block_size(block_size.as_u64().try_into().unwrap());
    // We create block_cache for each of 47 columns, so the total cache size is 32 * 47 = 1504mb
    if col == DBCol::BlockHeight
        || col == DBCol::ChunkHashesByHeight
        || col == DBCol::HeaderHashesByHeight
        || col == DBCol::BlockPerHeight
        || col == DBCol::BlockRefCount
    {
        // num_shards_bits = 0 will lead to LRU cache having (1 << 0) = 1 shards.
        block_opts.set_block_cache(
            &Cache::new_lru_cache_with_shard_bits(
                cache_size.as_u64().try_into().unwrap(),
                /*num_shard_bits */ 0,
            )
            .unwrap(),
        );
    } else {
        block_opts.set_block_cache(
            &Cache::new_lru_cache(cache_size.as_u64().try_into().unwrap()).unwrap(),
        );
    }

    if col == DBCol::ProcessedBlockHeights {
        // Since we user little endian for heights and this column doesn't have any
        // values all keys end up in the same SST file. The latter leads to bloom
        // filter growing huge and we need to make sure we don't reread it every time.
        // Estimated size if this extra cache would be: (`h` * `10` / 8)MB,
        //   where `h` is the current chain height and `10` is the number of bits
        //   we use in bloom filters for a single key.
        // Since explicit rocksdb cache doesn't user sharding we don't need to
        // specify num_shard_bits = 1 as for other height-indexed columns (see above).
        block_opts.set_cache_index_and_filter_blocks(false);
    } else {
        block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
        block_opts.set_cache_index_and_filter_blocks(true);
    }
    block_opts.set_bloom_filter(10.0, true);
    block_opts
}

fn rocksdb_column_options(col: DBCol, store_config: &StoreConfig) -> Options {
    let mut opts = Options::default();
    set_compression_options(&mut opts);
    opts.set_level_compaction_dynamic_level_bytes(true);
    let cache_size = store_config.col_cache_size(col);
    opts.set_block_based_table_factory(&rocksdb_block_based_options(
        store_config.block_size,
        cache_size,
        col,
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
    pub(crate) fn get_version(path: &Path, config: &StoreConfig) -> io::Result<DbVersion> {
        let value = RocksDB::open(path, config, Mode::ReadOnly)?
            .get_raw_bytes(DBCol::DbVersion, crate::db::VERSION_KEY)?
            .ok_or_else(|| {
                other_error(
                    "Failed to read database version; \
                     it’s not a neard database or database is corrupted."
                        .into(),
                )
            })?;
        serde_json::from_slice(&value).map_err(|_err| {
            other_error(format!(
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
    pub fn checkpoint(&self) -> io::Result<Checkpoint> {
        Checkpoint::new(&self.db).map_err(into_other)
    }

    /// Gets every int property in CF_STAT_NAMES for every column in DBCol.
    fn get_cf_statistics(&self, result: &mut StoreStatistics) {
        for stat_name in CF_STAT_NAMES {
            let mut values = vec![];
            for col in DBCol::iter() {
                let size = self.db.property_int_value_cf(self.cf_handle(col), stat_name);
                if let Ok(Some(value)) = size {
                    values.push(StatsValue::ColumnValue(col, value as i64));
                }
            }
            if !values.is_empty() {
                result.data.push((stat_name.to_string(), values));
            }
        }
    }
}

fn available_space(path: &Path) -> io::Result<bytesize::ByteSize> {
    let available = fs2::available_space(path)?;
    Ok(bytesize::ByteSize::b(available))
}

#[derive(Debug, thiserror::Error)]
pub enum PreWriteCheckErr {
    #[error("error checking filesystem: {0}")]
    IO(#[from] io::Error),
    #[error("low disk memory ({0} available)")]
    LowDiskSpace(bytesize::ByteSize),
}

impl PreWriteCheckErr {
    pub fn is_io(&self) -> bool {
        matches!(self, PreWriteCheckErr::IO(_))
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

/// Parses a string containing RocksDB statistics.
/// Results are added into provided 'result' parameter.
fn parse_statistics(
    statistics: &str,
    result: &mut StoreStatistics,
) -> Result<(), Box<dyn std::error::Error>> {
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
            // We push some stats to result, even if later parsing will fail.
            result.data.push((stat_name.to_string(), values));
        }
    }
    Ok(())
}

fn other_error(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::Other, msg)
}

fn into_other(error: rocksdb::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, error.into_string())
}

fn col_name(col: DBCol) -> String {
    format!("col{}", col as usize)
}

#[cfg(test)]
mod tests {
    use crate::db::{Database, StatsValue};
    use crate::{DBCol, Store, StoreConfig, StoreStatistics};

    use super::*;

    #[test]
    fn test_prewrite_check() {
        let tmp_dir = tempfile::Builder::new().prefix("prewrite_check").tempdir().unwrap();
        let store = RocksDB::open(
            tmp_dir.path(),
            &StoreConfig::test_config(),
            crate::config::Mode::ReadWrite,
        )
        .unwrap();
        store.pre_write_check().unwrap()
    }

    #[test]
    fn rocksdb_merge_sanity() {
        let (_tmp_dir, opener) = Store::test_opener();
        let store = opener.open();
        let ptr = (&*store.storage) as *const (dyn Database + 'static);
        let rocksdb = unsafe { &*(ptr as *const RocksDB) };
        assert_eq!(store.get(DBCol::State, &[1]).unwrap(), None);
        {
            let mut store_update = store.store_update();
            store_update.increment_refcount(DBCol::State, &[1], &[1]);
            store_update.commit().unwrap();
        }
        {
            let mut store_update = store.store_update();
            store_update.increment_refcount(DBCol::State, &[1], &[1]);
            store_update.commit().unwrap();
        }
        assert_eq!(store.get(DBCol::State, &[1]).unwrap(), Some(vec![1]));
        assert_eq!(
            rocksdb.get_raw_bytes(DBCol::State, &[1]).unwrap(),
            Some(vec![1, 2, 0, 0, 0, 0, 0, 0, 0])
        );
        {
            let mut store_update = store.store_update();
            store_update.decrement_refcount(DBCol::State, &[1]);
            store_update.commit().unwrap();
        }
        assert_eq!(store.get(DBCol::State, &[1]).unwrap(), Some(vec![1]));
        assert_eq!(
            rocksdb.get_raw_bytes(DBCol::State, &[1]).unwrap(),
            Some(vec![1, 1, 0, 0, 0, 0, 0, 0, 0])
        );
        {
            let mut store_update = store.store_update();
            store_update.decrement_refcount(DBCol::State, &[1]);
            store_update.commit().unwrap();
        }
        // Refcount goes to 0 -> get() returns None
        assert_eq!(store.get(DBCol::State, &[1]).unwrap(), None);
        // Internally there is an empty value
        assert_eq!(rocksdb.get_raw_bytes(DBCol::State, &[1]).unwrap(), Some(vec![]));

        // single_thread_rocksdb makes compact hang forever
        if !cfg!(feature = "single_thread_rocksdb") {
            let none = Option::<&[u8]>::None;
            let cf = rocksdb.cf_handle(DBCol::State);

            // I’m not sure why but we need to run compaction twice.  If we run
            // it only once, we end up with an empty value for the key.  This is
            // surprising because I assumed that compaction filter would discard
            // empty values.
            rocksdb.db.compact_range_cf(cf, none, none);
            assert_eq!(rocksdb.get_raw_bytes(DBCol::State, &[1]).unwrap(), Some(vec![]));
            assert_eq!(store.get(DBCol::State, &[1]).unwrap(), None);

            rocksdb.db.compact_range_cf(cf, none, none);
            assert_eq!(rocksdb.get_raw_bytes(DBCol::State, &[1]).unwrap(), None);
            assert_eq!(store.get(DBCol::State, &[1]).unwrap(), None);
        }
    }

    #[test]
    fn test_parse_statistics() {
        let statistics = "rocksdb.cold.file.read.count COUNT : 999\n\
         rocksdb.db.get.micros P50 : 9.171086 P95 : 222.678751 P99 : 549.611652 P100 : 45816.000000 COUNT : 917578 SUM : 38313754";
        let mut result = StoreStatistics { data: vec![] };
        let parse_result = parse_statistics(statistics, &mut result);
        // We should be able to parse stats and the result should be Ok(()).
        assert_eq!(parse_result.unwrap(), ());
        assert_eq!(
            result,
            StoreStatistics {
                data: vec![
                    ("rocksdb.cold.file.read.count".to_string(), vec![StatsValue::Count(999)]),
                    (
                        "rocksdb.db.get.micros".to_string(),
                        vec![
                            StatsValue::Percentile(50, 9.171086),
                            StatsValue::Percentile(95, 222.678751),
                            StatsValue::Percentile(99, 549.611652),
                            StatsValue::Percentile(100, 45816.0),
                            StatsValue::Count(917578),
                            StatsValue::Sum(38313754)
                        ]
                    )
                ]
            }
        );
    }

    #[test]
    fn next_prefix() {
        fn test(want: Option<&[u8]>, arg: &[u8]) {
            assert_eq!(want, super::next_prefix(arg).as_ref().map(Vec::as_ref));
        }

        test(None, b"");
        test(None, b"\xff");
        test(None, b"\xff\xff\xff\xff");
        test(Some(b"b"), b"a");
        test(Some(b"b"), b"a\xff\xff\xff");
    }
}

use super::StoreConfig;
use crate::{metrics, DBCol};
use near_primitives::version::DbVersion;
use once_cell::sync::Lazy;
use rocksdb::checkpoint::Checkpoint;
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamily, Direction, Env, IteratorMode, Options, ReadOptions,
    WriteBatch, DB,
};
use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Condvar, Mutex, RwLock};

use strum::IntoEnumIterator;
use tracing::{error, info, warn};

pub mod refcount;

pub const VERSION_KEY: &[u8; 7] = b"VERSION";

pub const HEAD_KEY: &[u8; 4] = b"HEAD";
pub const TAIL_KEY: &[u8; 4] = b"TAIL";
pub const CHUNK_TAIL_KEY: &[u8; 10] = b"CHUNK_TAIL";
pub const FORK_TAIL_KEY: &[u8; 9] = b"FORK_TAIL";
pub const HEADER_HEAD_KEY: &[u8; 11] = b"HEADER_HEAD";
pub const FINAL_HEAD_KEY: &[u8; 10] = b"FINAL_HEAD";
pub const LATEST_KNOWN_KEY: &[u8; 12] = b"LATEST_KNOWN";
pub const LARGEST_TARGET_HEIGHT_KEY: &[u8; 21] = b"LARGEST_TARGET_HEIGHT";
pub const GENESIS_JSON_HASH_KEY: &[u8; 17] = b"GENESIS_JSON_HASH";
pub const GENESIS_STATE_ROOTS_KEY: &[u8; 19] = b"GENESIS_STATE_ROOTS";
pub const CF_STAT_NAMES: [&'static str; 1] = ["rocksdb.total-sst-files-size"];
/// Boolean stored in DBCol::BlockMisc indicating whether the database is for an
/// archival node.  The default value (if missing) is false.
pub const IS_ARCHIVE_KEY: &[u8; 10] = b"IS_ARCHIVE";

#[derive(Default)]
pub struct DBTransaction {
    pub(crate) ops: Vec<DBOp>,
}

pub(crate) enum DBOp {
    /// Sets `key` to `value`, without doing any checks.
    Set { col: DBCol, key: Vec<u8>, value: Vec<u8> },
    /// Sets `key` to `value`, and additionally debug-checks that the value is
    /// not overwritten.
    Insert { col: DBCol, key: Vec<u8>, value: Vec<u8> },
    /// Modifies a reference-counted column. `value` includes both the value per
    /// se and a refcount at the end.
    UpdateRefcount { col: DBCol, key: Vec<u8>, value: Vec<u8> },
    /// Deletes sepecific `key`.
    Delete { col: DBCol, key: Vec<u8> },
    /// Deletes all data from a column.
    DeleteAll { col: DBCol },
}

impl DBTransaction {
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    pub fn set(&mut self, col: DBCol, key: Vec<u8>, value: Vec<u8>) {
        self.ops.push(DBOp::Set { col, key, value });
    }

    pub fn insert(&mut self, col: DBCol, key: Vec<u8>, value: Vec<u8>) {
        self.ops.push(DBOp::Insert { col, key, value });
    }

    pub fn update_refcount(&mut self, col: DBCol, key: Vec<u8>, value: Vec<u8>) {
        self.ops.push(DBOp::UpdateRefcount { col, key, value });
    }

    pub fn delete(&mut self, col: DBCol, key: Vec<u8>) {
        self.ops.push(DBOp::Delete { col, key });
    }

    pub fn delete_all(&mut self, col: DBCol) {
        self.ops.push(DBOp::DeleteAll { col });
    }

    pub fn merge(&mut self, other: DBTransaction) {
        self.ops.extend(other.ops)
    }
}

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

fn other_error(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::Other, msg)
}

fn into_other(error: rocksdb::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, error.into_string())
}

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

#[derive(Clone, Copy)]
pub enum Mode {
    ReadOnly,
    ReadWrite,
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
}

pub struct TestDB {
    // In order to ensure determinism when iterating over column's results
    // a BTreeMap is used since it is an ordered map. A HashMap would
    // give the aforementioned guarantee, and therefore is discarded.
    db: RwLock<enum_map::EnumMap<DBCol, BTreeMap<Vec<u8>, Vec<u8>>>>,
}

pub type DBIterator<'a> = Box<dyn Iterator<Item = io::Result<(Box<[u8]>, Box<[u8]>)>> + 'a>;

pub trait Database: Sync + Send {
    /// Returns raw bytes for given `key` ignoring any reference count decoding
    /// if any.
    ///
    /// Note that when reading reference-counted column, the reference count
    /// will not be decoded or stripped from the value.  Similarly, cells with
    /// non-positive reference count will be returned as existing.
    ///
    /// You most likely will want to use [`refcount::get_with_rc_logic`] to
    /// properly handle reference-counted columns.
    fn get_raw_bytes(&self, col: DBCol, key: &[u8]) -> io::Result<Option<Vec<u8>>>;

    /// Iterate over all items in given column in lexicographical order sorted
    /// by the key.
    ///
    /// When reading reference-counted column, the reference count will be
    /// correctly stripped.  Furthermore, elements with non-positive reference
    /// count will be treated as non-existing (i.e. they’re going to be
    /// skipped).  For all other columns, the value is returned directly from
    /// the database.
    fn iter<'a>(&'a self, column: DBCol) -> DBIterator<'a>;

    /// Iterate over items in given column whose keys start with given prefix.
    ///
    /// This is morally equivalent to [`Self::iter`] with a filter discarding
    /// keys which do not start with given `key_prefix` (but faster).  The items
    /// are returned in lexicographical order sorted by the key.
    fn iter_prefix<'a>(&'a self, col: DBCol, key_prefix: &'a [u8]) -> DBIterator<'a>;

    /// Iterate over items in given column bypassing reference count decoding if
    /// any.
    ///
    /// This is like [`Self::iter`] but it returns raw bytes as stored in the
    /// database.  For reference-counted columns this means that the reference
    /// count will not be decoded or stripped from returned value and elements
    /// with non-positive reference count will be included in the iterator.
    ///
    /// If in doubt, use [`Self::iter`] instead.  Unless you’re doing something
    /// low-level with the database (e.g. doing a migration), you probably don’t
    /// want this method.
    fn iter_raw_bytes<'a>(&'a self, column: DBCol) -> DBIterator<'a>;

    /// Atomically apply all operations in given batch at once.
    fn write(&self, batch: DBTransaction) -> io::Result<()>;

    /// Flush all in-memory data to disk.
    ///
    /// This is a no-op for in-memory databases.
    fn flush(&self) -> io::Result<()>;

    /// Returns statistics about the database if available.
    fn get_store_statistics(&self) -> Option<StoreStatistics>;
}

impl RocksDB {
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
                            assert_no_overwrite(col, &key, &value, &*old_value)
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

    fn get_store_statistics(&self) -> Option<StoreStatistics> {
        let mut stats = vec![];
        if let Some(stats_str) = self.db_opt.get_statistics() {
            match parse_statistics(&stats_str) {
                Ok(parsed_statistics) => {
                    stats.push(parsed_statistics);
                }
                Err(err) => {
                    warn!(target: "store", "Failed to parse store statistics: {:?}", err);
                }
            }
        }
        if let Some(cf_stats) = self.get_cf_statistics() {
            stats.push(cf_stats);
        }
        if !stats.is_empty() {
            Some(StoreStatistics { data: stats.into_iter().map(|x| x.data).flatten().collect() })
        } else {
            None
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

impl Database for TestDB {
    fn get_raw_bytes(&self, col: DBCol, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        Ok(self.db.read().unwrap()[col].get(key).cloned())
    }

    fn iter<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        let iterator = self.iter_raw_bytes(col);
        refcount::iter_with_rc_logic(col, iterator)
    }

    fn iter_raw_bytes<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        let iterator = self.db.read().unwrap()[col]
            .clone()
            .into_iter()
            .map(|(k, v)| Ok((k.into_boxed_slice(), v.into_boxed_slice())));
        Box::new(iterator)
    }

    fn iter_prefix<'a>(&'a self, col: DBCol, key_prefix: &'a [u8]) -> DBIterator<'a> {
        let iterator = self.db.read().unwrap()[col]
            .range(key_prefix.to_vec()..)
            .take_while(move |(k, _)| k.starts_with(&key_prefix))
            .map(|(k, v)| Ok((k.clone().into_boxed_slice(), v.clone().into_boxed_slice())))
            .collect::<Vec<io::Result<_>>>();
        refcount::iter_with_rc_logic(col, iterator.into_iter())
    }

    fn write(&self, transaction: DBTransaction) -> io::Result<()> {
        let mut db = self.db.write().unwrap();
        for op in transaction.ops {
            match op {
                DBOp::Set { col, key, value } => {
                    db[col].insert(key, value);
                }
                DBOp::Insert { col, key, value } => {
                    if cfg!(debug_assertions) {
                        if let Some(old_value) = db[col].get(&key) {
                            assert_no_overwrite(col, &key, &value, &*old_value)
                        }
                    }
                    db[col].insert(key, value);
                }
                DBOp::UpdateRefcount { col, key, value } => {
                    let existing = db[col].get(&key).map(Vec::as_slice);
                    let operands = [value.as_slice()];
                    let merged = refcount::refcount_merge(existing, operands);
                    if merged.is_empty() {
                        db[col].remove(&key);
                    } else {
                        debug_assert!(
                            refcount::decode_value_with_rc(&merged).1 > 0,
                            "Inserting value with non-positive refcount"
                        );
                        db[col].insert(key, merged);
                    }
                }
                DBOp::Delete { col, key } => {
                    db[col].remove(&key);
                }
                DBOp::DeleteAll { col } => db[col].clear(),
            };
        }
        Ok(())
    }

    fn flush(&self) -> io::Result<()> {
        Ok(())
    }

    fn get_store_statistics(&self) -> Option<StoreStatistics> {
        None
    }
}

fn assert_no_overwrite(col: DBCol, key: &[u8], value: &[u8], old_value: &[u8]) {
    assert_eq!(
        value, old_value,
        "\
write once column overwritten
col: {col}
key: {key:?}
old value: {old_value:?}
new value: {value:?}
"
    )
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
) -> BlockBasedOptions {
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_block_size(block_size.as_u64().try_into().unwrap());
    // We create block_cache for each of 47 columns, so the total cache size is 32 * 47 = 1504mb
    block_opts
        .set_block_cache(&Cache::new_lru_cache(cache_size.as_u64().try_into().unwrap()).unwrap());
    block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
    block_opts.set_cache_index_and_filter_blocks(true);
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
    pub fn get_version(path: &Path) -> io::Result<DbVersion> {
        let value = RocksDB::open(path, &StoreConfig::default(), Mode::ReadOnly)?
            .get_raw_bytes(DBCol::DbVersion, VERSION_KEY)?
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

    fn get_cf_statistics(&self) -> Option<StoreStatistics> {
        let mut result = vec![];
        for stat_name in CF_STAT_NAMES {
            let mut values = vec![];
            for col in DBCol::iter() {
                let size =
                    self.db.property_int_value_cf(self.cf_handle(col), stat_name);
                if let Ok(Some(value)) = size {
                    values.push(StatsValue::ColumnValue(col_name(col), value as i64));
                }
            }
            if !values.is_empty() {
                result.push((stat_name.to_string(), values));
            }
        }
        if !result.is_empty() {
            Some(StoreStatistics { data: result })
        } else {
            None
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
        Self { db: Default::default() }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum StatsValue {
    Count(i64),
    Sum(i64),
    Percentile(u32, f64),
    ColumnValue(String, i64),
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
    use crate::db::{parse_statistics, Database, RocksDB};
    use crate::{DBCol, Store, StoreConfig, StoreStatistics};

    use super::Mode;

    #[test]
    fn test_prewrite_check() {
        let tmp_dir = tempfile::Builder::new().prefix("prewrite_check").tempdir().unwrap();
        let store =
            RocksDB::open(tmp_dir.path(), &StoreConfig::test_config(), Mode::ReadWrite).unwrap();
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

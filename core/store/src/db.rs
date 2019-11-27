use rocksdb::{
    BlockBasedOptions, ColumnFamily, ColumnFamilyDescriptor, DBCompactionStyle, IteratorMode,
    Options, ReadOptions, WriteBatch, DB,
};
use std::cmp;
use std::collections::HashMap;
use std::io;
use std::sync::RwLock;

#[derive(Debug, Clone, PartialEq)]
pub struct DBError(rocksdb::Error);

impl std::error::Error for DBError {
    fn description(&self) -> &str {
        &self.0.description()
    }
}

impl std::fmt::Display for DBError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        self.0.fmt(formatter)
    }
}

impl From<rocksdb::Error> for DBError {
    fn from(err: rocksdb::Error) -> Self {
        DBError(err)
    }
}

impl Into<io::Error> for DBError {
    fn into(self) -> io::Error {
        io::Error::new(io::ErrorKind::Other, self)
    }
}

pub struct DBTransaction {
    pub ops: Vec<DBOp>,
}

pub enum DBOp {
    Insert { col: usize, key: Vec<u8>, value: Vec<u8> },
    Delete { col: usize, key: Vec<u8> },
}

impl DBTransaction {
    pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, col: usize, key: K, value: V) {
        self.ops.push(DBOp::Insert {
            col,
            key: key.as_ref().to_owned(),
            value: value.as_ref().to_owned(),
        });
    }

    pub fn delete<K: AsRef<[u8]>>(&mut self, col: usize, key: K) {
        self.ops.push(DBOp::Delete { col, key: key.as_ref().to_owned() });
    }
}

pub struct RocksDB {
    db: DB,
    cfs: Vec<*const ColumnFamily>,
    read_options: ReadOptions,
}

// DB was already Send+Sync. cf and read_options are const pointers using only functions in
// this file and safe to share across threads.
unsafe impl Send for RocksDB {}
unsafe impl Sync for RocksDB {}

pub struct TestDB {
    db: RwLock<Vec<HashMap<Vec<u8>, Vec<u8>>>>,
}

pub trait Database: Sync + Send {
    fn transaction(&self) -> DBTransaction {
        DBTransaction { ops: Vec::new() }
    }
    fn get(&self, col: usize, key: &[u8]) -> Result<Option<Vec<u8>>, DBError>;
    fn iter<'a>(&'a self, column: usize) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a>;
    fn write(&self, batch: DBTransaction) -> Result<(), DBError>;
}

impl Database for RocksDB {
    fn get(&self, col: usize, key: &[u8]) -> Result<Option<Vec<u8>>, DBError> {
        unsafe { Ok(self.db.get_cf_opt(&*self.cfs[col], key, &self.read_options)?) }
    }

    fn iter<'a>(&'a self, col: usize) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        unsafe {
            let cf_handle = &*self.cfs[col];
            let iterator = self
                .db
                .iterator_cf_opt(cf_handle, &self.read_options, IteratorMode::Start)
                .unwrap();
            Box::new(iterator)
        }
    }

    fn write(&self, transaction: DBTransaction) -> Result<(), DBError> {
        let mut batch = WriteBatch::default();
        for op in transaction.ops {
            match op {
                DBOp::Insert { col, key, value } => unsafe {
                    batch.put_cf(&*self.cfs[col], key, value)?;
                },
                DBOp::Delete { col, key } => unsafe {
                    batch.delete_cf(&*self.cfs[col], key)?;
                },
            }
        }
        Ok(self.db.write(batch)?)
    }
}

impl Database for TestDB {
    fn get(&self, col: usize, key: &[u8]) -> Result<Option<Vec<u8>>, DBError> {
        Ok(self.db.read().unwrap()[col].get(key).cloned())
    }

    fn iter<'a>(&'a self, col: usize) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        let iterator = self.db.read().unwrap()[col]
            .clone()
            .into_iter()
            .map(|(k, v)| (k.into_boxed_slice(), v.into_boxed_slice()));
        Box::new(iterator)
    }

    fn write(&self, transaction: DBTransaction) -> Result<(), DBError> {
        let mut db = self.db.write().unwrap();
        for op in transaction.ops {
            match op {
                DBOp::Insert { col, key, value } => db[col].insert(key, value),
                DBOp::Delete { col, key } => db[col].remove(&key),
            };
        }
        Ok(())
    }
}

fn rocksdb_read_options() -> ReadOptions {
    let mut read_options = ReadOptions::default();
    read_options.set_verify_checksums(false);
    read_options
}

/// DB level options
fn rocksdb_options() -> Options {
    let mut opts = Options::default();

    opts.create_missing_column_families(true);
    opts.create_if_missing(true);
    opts.set_use_fsync(false);
    opts.set_max_open_files(512);
    opts.set_keep_log_file_num(1);
    opts.set_bytes_per_sync(1048576);
    opts.set_write_buffer_size(1024 * 1024 * 512 / 2);
    opts.set_max_bytes_for_level_base(1024 * 1024 * 512 / 2);
    opts.increase_parallelism(cmp::max(1, num_cpus::get() as i32 / 2));

    return opts;
}

fn rocksdb_block_based_options() -> BlockBasedOptions {
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_block_size(1024 * 1024 * 8);
    let cache_size = 1024 * 1024 * 512 / 3;
    block_opts.set_lru_cache(cache_size);
    block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
    block_opts.set_cache_index_and_filter_blocks(true);

    block_opts
}

fn rocksdb_column_options() -> Options {
    let mut opts = Options::default();
    opts.set_level_compaction_dynamic_level_bytes(true);
    opts.set_block_based_table_factory(&rocksdb_block_based_options());
    opts.optimize_level_style_compaction(1024 * 1024 * 128);
    opts.set_target_file_size_base(1024 * 1024 * 64);
    opts.set_compression_per_level(&[]);
    opts
}

impl RocksDB {
    pub fn new<P: AsRef<std::path::Path>>(path: P, cols: usize) -> Result<Self, DBError> {
        let options = rocksdb_options();
        let cf_names: Vec<_> = (0..cols).map(|col| format!("col{}", col)).collect();
        let cf_descriptors =
            cf_names.iter().map(|cf_name| ColumnFamilyDescriptor::new(cf_name, rocksdb_options()));
        let db = DB::open_cf_descriptors(&options, path, cf_descriptors)?;
        let cfs = cf_names
            .iter()
            .map(|n| {
                let ptr: *const ColumnFamily = db.cf_handle(n).unwrap();
                ptr
            })
            .collect();
        Ok(Self { db, cfs, read_options: rocksdb_read_options() })
    }
}

impl TestDB {
    pub fn new(cols: usize) -> Self {
        let db: Vec<_> = (0..cols).map(|_| HashMap::new()).collect();
        Self { db: RwLock::new(db) }
    }
}

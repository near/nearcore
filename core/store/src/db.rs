use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DBCompactionStyle, IteratorMode, Options, ReadOptions,
    WriteBatch, DB,
};
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

fn rocksdb_options() -> Options {
    // TODO: experiment here
    let mut opts = Options::default();
    opts.create_missing_column_families(true);
    opts.create_if_missing(true);
    opts.set_max_open_files(10000);
    opts.set_use_fsync(false);
    opts.set_bytes_per_sync(8388608);
    opts.optimize_for_point_lookup(1024);
    opts.set_table_cache_num_shard_bits(6);
    opts.set_max_write_buffer_number(32);
    opts.set_write_buffer_size(536870912);
    opts.set_target_file_size_base(1073741824);
    opts.set_min_write_buffer_number_to_merge(4);
    opts.set_level_zero_stop_writes_trigger(2000);
    opts.set_level_zero_slowdown_writes_trigger(0);
    opts.set_compaction_style(DBCompactionStyle::Universal);
    opts.set_max_background_compactions(4);
    opts.set_max_background_flushes(4);
    opts.set_disable_auto_compactions(true);
    return opts;
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
        Ok(Self { db, cfs, read_options: ReadOptions::default() })
    }
}

impl TestDB {
    pub fn new(cols: usize) -> Self {
        let db: Vec<_> = (0..cols).map(|_| HashMap::new()).collect();
        Self { db: RwLock::new(db) }
    }
}

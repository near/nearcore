use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DBCompactionStyle, IteratorMode, Options, ReadOptions,
    WriteBatch, DB,
};
use std::collections::HashMap;
use std::io;
use std::sync::RwLock;

fn other_io_err<E>(e: E) -> io::Error
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    io::Error::new(io::ErrorKind::Other, e)
}

pub struct DBTransaction {
    pub ops: Vec<DBOp>,
}

pub enum DBOp {
    Insert { col: Option<u32>, key: Vec<u8>, value: Vec<u8> },
    Delete { col: Option<u32>, key: Vec<u8> },
}

impl DBTransaction {
    pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, col: Option<u32>, key: K, value: V) {
        self.ops.push(DBOp::Insert { col, key: key.as_ref().to_owned(), value: value.as_ref().to_owned() });
    }

    pub fn delete<K: AsRef<[u8]>>(&self, col: Option<u32>, key: K) {
        self.ops.push(DBOp::Delete { col, key: key.as_ref().to_owned() });
    }
}

pub struct RocksDB {
    db: RwLock<DB>,
    columns: Vec<ColumnFamily>,
    read_options: ReadOptions,
}

unsafe impl Sync for RocksDB {}

pub struct TestDB {
    default_db: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
    dbs: RwLock<Vec<HashMap<Vec<u8>, Vec<u8>>>>,
}

pub trait Database: Sync + Send {
    fn transaction(&self) -> DBTransaction {
        DBTransaction { ops: Vec::new() }
    }
    fn get(&self, col: Option<u32>, key: &[u8]) -> Result<Option<Vec<u8>>, io::Error>;
    fn iter<'a>(
        &'a self,
        column: Option<u32>,
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a>;
    fn write(&self, batch: DBTransaction) -> io::Result<()>;
}

impl Database for RocksDB {
    fn get(&self, col: Option<u32>, key: &[u8]) -> Result<Option<Vec<u8>>, io::Error> {
        match col {
            Some(col) => {
                Ok(self.db.read().unwrap().get_cf_opt(&self.columns[col as usize], key, &self.read_options).map_err(other_io_err)?)
            }
            None => Ok(self.db.read().unwrap().get_opt(key, &self.read_options).map_err(other_io_err)?),
        }
    }

    fn iter<'a>(
        &'a self,
        column: Option<u32>,
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        match column {
            Some(col) => Box::new(self.db.read().unwrap().iterator_cf_opt(
                &self.columns[col as usize],
                &self.read_options,
                IteratorMode::Start,
            ).unwrap()),
            None => Box::new(self.db.read().unwrap().iterator_opt(IteratorMode::End, &self.read_options)),
        }
    }

    fn write(&self, transaction: DBTransaction) -> io::Result<()> {
        let mut batch = WriteBatch::default();
        for op in transaction.ops {
            match op {
                DBOp::Insert { col, key, value } => match col {
                    Some(col) => batch.put_cf(&self.columns[col as usize], key, value).map_err(other_io_err)?,
                    None => batch.put(key, value).map_err(other_io_err)?,
                },
                DBOp::Delete { col, key } => match col {
                    Some(col) => batch.delete_cf(&self.columns[col as usize], key).map_err(other_io_err)?,
                    None => batch.delete(key).map_err(other_io_err)?,
                },
            }
        }
        Ok(self.db.write().unwrap().write(batch).map_err(other_io_err)?)
    }
}

impl Database for TestDB {
    fn get(&self, col: Option<u32>, key: &[u8]) -> Result<Option<Vec<u8>>, io::Error> {
        match col {
            Some(col) => Ok(self.dbs.read().unwrap()[col as usize].get(key).cloned()),
            None => Ok(self.default_db.read().unwrap().get(key).cloned()),
        }
    }

    fn iter<'a>(
        &'a self,
        column: Option<u32>,
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        match column {
            Some(col) => Box::new(self.dbs.read().unwrap()[col as usize].into_iter().map(|(k, v)| (k.into_boxed_slice(), v.into_boxed_slice()))),
            None => Box::new(self.default_db.read().unwrap().into_iter().map(|(k, v)| (k.into_boxed_slice(), v.into_boxed_slice()))),
        }
    }

    fn write(&self, transaction: DBTransaction) -> io::Result<()> {
        for op in transaction.ops {
            match op {
                DBOp::Insert { col, key, value } => match col {
                    Some(col) => self.dbs.write().unwrap()[col as usize].insert(key, value),
                    None => self.default_db.write().unwrap().insert(key, value),
                },
                DBOp::Delete { col, key } => match col {
                    Some(col) => self.dbs.write().unwrap()[col as usize].remove(&key),
                    None => self.default_db.write().unwrap().remove(&key),
                },
            };
        }
        Ok(())
    }
}

fn rocksdb_options() -> Options {
    // TODO: experiment here
    let mut opts = Options::default();
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

pub fn open_rocksdb<P: AsRef<std::path::Path>>(
    path: P,
    cols: u32,
) -> Result<RocksDB, rocksdb::Error> {
    let options = rocksdb_options();
    let cf_names: Vec<_> = (0..cols).map(|col| format!("col{}", col)).collect();
    let cf_descriptors = cf_names.iter().map(|cf_name| ColumnFamilyDescriptor::new(cf_name, options));
    let db = DB::open_cf_descriptors(&options, path, cf_descriptors)?;
    let cfs = cf_names.iter().map(|n| db.cf_handle(n).unwrap().cloned()).collect();
    Ok(RocksDB { db: RwLock::new(db), columns: cfs, read_options: ReadOptions::default() })
}

pub fn open_testdb(cols: u32) -> TestDB {
    let dbs: Vec<_> = (0..cols).map(|i| HashMap::new()).into();
    TestDB { default_db: RwLock::new(HashMap::new()), dbs: RwLock::new(dbs) }
}

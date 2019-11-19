use rocksdb::{ColumnFamily, WriteBatch, DB, IteratorMode, Options};
use std::collections::{HashMap};
use std::sync::RwLock;

pub enum Transaction {
    RocksDBTransaction(BatchWrite),
    TestDBTransaction(RwLock(Vec<TestDBOp>))
}

enum TestDBOp {
    PutOp{column: Option<u32>, key: Vec<u8>, value: Vec<u8>}
    DeleteOp{column: Option<u32>, key: Vec<u8>}
}

impl Transaction {
    use Self::*;

    pub fn put(&self, col: Option<u32>, key: AsRef<[u8]>, value: AsRef<[u8]>) {
        match *self {
            RocksDBTransaction(ref batch_write) => match col {
                Some(col) => batch_write.put_cf(col, key, value).unwrap(),
                None => batch_write.put(key, value).unwrap(),
            },
            TestDBTransaction(ref ops) => ops.write().push(Test)
        }
    }

    pub fn delete(&self, col: Option<u32>, key: AsRef<[u8]>) {
        match *self {
            RocksDBTransaction(ref batch_write) => match col {
                Some(col) => batch_write.delete_cf(col, key).unwrap(),
                None => batch_write.delete(key).unwrap(),
            },
            TestDBTransaction(ref ops) => ops.write().push(TestDBOP::DeleteOp{col, key})
        }
    }

    pub fn merge(&self, transaction: Transaction) {
        match *self {
            RocksDBTransaction(ref batch_write) => match(transaction) {
                RocksDBTransaction(batch_write2) => batch_write.merge(batch_write2),
                batch_write.merge()
            TestDBTransaction(ref ops) => ops.write().extend(&transaction.read())
        }
    }
}

pub struct RocksDB {
    db: DB,
    columns: Vec<ColumnFamily>,
    read_options: ReadOptions,
}

pub struct TestDB {
    default_db: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
    dbs: RwLock<Vec<HashMap<Vec[u8], Vec[u8]>>>,
}

trait Database {
    fn transaction() -> Transaction;
    fn get(&self, col: Option<u32>, key: AsRef<[u8]>) -> Result<Option<Vec<u8>>, io::Error>;
    fn iter<'a>(&'a self, column: Option<u32>) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a>;
    fn write(&self, batch: Transaction) -> io::Result<()>;
}

impl Database for RocksDB {
    fn transaction() -> Transaction {
        Transaction::RocksDBTransaction(BatchWrite::new())
    }

    fn get(&self, col: Option<u32>, key: AsRef<[u8]>) -> Result<Option<Vec<u8>>, io::Error> {
        match col {
            Some(col) => self.db.get_cf_opt(self.columns[col], key, &self.read_options)?,
            None => self.db.get_opt(key, &self.opts)?,
        }
    }

    fn iter<'a>(&'a self, column: Option<u32>) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        match col {
            Some(col) => self.db.iterator_cf_opt(self.columns[col], IteratorMode::Start, &self.read_options),
            None => self.db.iterator_opt(IteratorMode::End, &self.read_options),
        }
    }

    fn write(&self, batch: Transaction) -> io::Result<()> {
        match batch {
            RocksDBTransaction(batch) => self.db.write(batch)?,
            _ => panic!()
        }
    }
}

impl Database for TestDB {
    fn transaction() -> Transaction {
        Transaction::TestDBTransaction(RwLock::new(Vec::new()))
    }
    
    fn get(&self, col: Option<u32>, key: AsRef<[u8]>) -> Result<Option<Vec<u8>>, io::Error> {
        match col {
            Some(col) => ok(self.dbs.read()[col].get(key)),
            None => ok(self.default_db.read().get(key))
        }
    }
    
    pub fn iter<'a>(&'a self, column: Option<u32>) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        match col {
            Some(col) => self.dbs.read()[col].into_iter(),
            None => self.default_db.read().into_iter(),
        }
    }

    pub fn write(&self, batch: Transaction) -> io::Result<()> {
        match batch {
            Transaction::TestDBTransaction(ops) => {
                let ops = ops.write()
                for op in ops.iter() {
                    match op {
                        TestDBOp::PutOp{col, key, value} => match col {
                            Some(col) => self.dbs.write()[col].insert(key, value),
                            None => self.default_db.write()[col].insert(key, value),
                        },
                        TestDBOp::DeleteOp(col, key) => match col {
                            Some(col) => self.dbs.write()[col].delete(key),
                            None => self.default_db.write()[col].delete(key),
                        }
                    }
                }
            }
            _ => panic!()
        }
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
    return opts
}

pub fn open_rocksdb(path: AsRef<Path>, cols: u32) -> Result<RocksDB, Error> {

}
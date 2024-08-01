use std::collections::HashSet;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Context};
use near_store::db::{
    DBIterator, DBSlice, DBTransaction, Database, SplitDB, StoreStatistics, TestDB,
};
use near_store::{DBCol, Mode, NodeStorage};
use nearcore::NearConfig;

pub struct ReplayDB {
    /// Read-only DB.
    read_db: Arc<dyn Database>,
    /// DB for writes.
    write_db: Arc<dyn Database>,

    /// Columns that should not be written to the write_db.
    /// Attempting to write to one of these columns will create an error.
    read_only_columns: Arc<HashSet<DBCol>>,
    /// Columns that should not be read from the read_db.
    /// In other words, reads from these columns will only be answered from the write_db.
    write_only_columns: Arc<HashSet<DBCol>>,

    /// Columns that are read from the DB.
    columns_read: Arc<Mutex<HashSet<DBCol>>>,
    /// Columns that are written to the DB.
    columns_written: Arc<Mutex<HashSet<DBCol>>>,
}

impl ReplayDB {
    pub fn new(
        read_db: Arc<dyn Database>,
        write_db: Arc<dyn Database>,
        read_only_columns: HashSet<DBCol>,
        write_only_columns: HashSet<DBCol>,
    ) -> Arc<Self> {
        return Arc::new(ReplayDB {
            read_db,
            write_db,
            read_only_columns: Arc::new(read_only_columns),
            write_only_columns: Arc::new(write_only_columns),
            columns_read: Default::default(),
            columns_written: Default::default(),
        });
    }

    /// Returns the set of columns read from the store since its creation.
    pub fn get_columns_read(&self) -> HashSet<DBCol> {
        self.columns_read.lock().unwrap().clone()
    }

    /// Returns the set of columns read from the store since its creation.
    pub fn get_columns_written(&self) -> HashSet<DBCol> {
        self.columns_written.lock().unwrap().clone()
    }

    /// This function is imported from SplitDB, but we may want to refactor them later.
    fn merge_iter<'a>(a: DBIterator<'a>, b: DBIterator<'a>) -> DBIterator<'a> {
        SplitDB::merge_iter(a, b)
    }
}

impl Database for ReplayDB {
    fn get_raw_bytes(&self, col: DBCol, key: &[u8]) -> io::Result<Option<DBSlice<'_>>> {
        self.columns_read.lock().unwrap().insert(col);
        if !self.write_only_columns.contains(&col) {
            if let Some(result) = self.read_db.get_raw_bytes(col, key)? {
                return Ok(Some(result));
            }
        }
        self.write_db.get_raw_bytes(col, key)
    }

    fn get_with_rc_stripped(&self, col: DBCol, key: &[u8]) -> io::Result<Option<DBSlice<'_>>> {
        assert!(col.is_rc());
        self.columns_read.lock().unwrap().insert(col);
        if !self.write_only_columns.contains(&col) {
            if let Some(result) = self.read_db.get_with_rc_stripped(col, key)? {
                return Ok(Some(result));
            }
        }
        self.write_db.get_with_rc_stripped(col, key)
    }

    fn iter<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        self.columns_read.lock().unwrap().insert(col);
        if self.write_only_columns.contains(&col) {
            self.write_db.iter(col)
        } else {
            Self::merge_iter(self.read_db.iter(col), self.write_db.iter(col))
        }
    }

    fn iter_prefix<'a>(&'a self, col: DBCol, key_prefix: &'a [u8]) -> DBIterator<'a> {
        self.columns_read.lock().unwrap().insert(col);
        if self.write_only_columns.contains(&col) {
            self.write_db.iter_prefix(col, key_prefix)
        } else {
            Self::merge_iter(
                self.read_db.iter_prefix(col, key_prefix),
                self.write_db.iter_prefix(col, key_prefix),
            )
        }
    }

    fn iter_range<'a>(
        &'a self,
        col: DBCol,
        lower_bound: Option<&[u8]>,
        upper_bound: Option<&[u8]>,
    ) -> DBIterator<'a> {
        self.columns_read.lock().unwrap().insert(col);
        if self.write_only_columns.contains(&col) {
            self.write_db.iter_range(col, lower_bound, upper_bound)
        } else {
            Self::merge_iter(
                self.read_db.iter_range(col, lower_bound, upper_bound),
                self.write_db.iter_range(col, lower_bound, upper_bound),
            )
        }
    }

    fn iter_raw_bytes<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        self.columns_read.lock().unwrap().insert(col);
        if self.write_only_columns.contains(&col) {
            self.write_db.iter_raw_bytes(col)
        } else {
            Self::merge_iter(self.read_db.iter_raw_bytes(col), self.write_db.iter_raw_bytes(col))
        }
    }

    fn write(&self, batch: DBTransaction) -> io::Result<()> {
        let columns = batch.columns();
        if !self.read_only_columns.is_disjoint(&columns) {
            return Err(std::io::Error::new(
                io::ErrorKind::Other,
                format!("Attempted to write to a read-only column: {:?}", columns),
            ));
        }
        self.columns_written.lock().unwrap().extend(columns);
        self.write_db.write(batch)
    }

    fn flush(&self) -> io::Result<()> {
        unreachable!()
    }

    fn compact(&self) -> io::Result<()> {
        unreachable!()
    }

    fn get_store_statistics(&self) -> Option<StoreStatistics> {
        unreachable!()
    }

    fn create_checkpoint(
        &self,
        _path: &Path,
        _columns_to_keep: Option<&[DBCol]>,
    ) -> anyhow::Result<()> {
        unreachable!()
    }
}

pub(crate) fn open_storage_for_replay(
    home_dir: &Path,
    near_config: &NearConfig,
    read_only_columns: HashSet<DBCol>,
    write_only_columns: HashSet<DBCol>,
) -> anyhow::Result<Arc<ReplayDB>> {
    let opener = NodeStorage::opener(
        home_dir,
        near_config.client_config.archive,
        &near_config.config.store,
        near_config.config.cold_store.as_ref(),
    );
    let split_storage = opener.open_in_mode(Mode::ReadOnly).context("Failed to open storage")?;
    match split_storage.get_split_db() {
        Some(split_db) => {
            Ok(ReplayDB::new(split_db, TestDB::new(), read_only_columns, write_only_columns))
        }
        None => Err(anyhow!("Failed to get split store for archival node")),
    }
}

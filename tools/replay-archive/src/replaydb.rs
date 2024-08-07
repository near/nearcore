use std::collections::HashSet;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Context};
use itertools::Itertools;
use near_store::db::{
    DBIterator, DBSlice, DBTransaction, Database, SplitDB, StoreStatistics, TestDB,
};
use near_store::{DBCol, Mode, NodeStorage};
use nearcore::NearConfig;

/// Database layer for replaying the chain using the archival storage for reads and a temporary storage for writes.
/// For archival data we use split db ad the read source and for temporary storage we use an in-memory DB.
pub struct ReplayDB {
    /// SplitDB used to read archival data that is previously recorded.
    /// Opened in read-only mode.
    split_db: Arc<SplitDB>,
    /// TestDB used to store data generated during the replay.
    /// Opened in read/write mode.
    write_db: Arc<TestDB>,

    /// Columns that contain archival data.
    /// These columns will be read from split_db, while other columns will be read from write_db.
    /// Attempting to write to one of these columns will create an error.
    archival_columns: HashSet<DBCol>,

    /// Columns that are read from the DB (either from split_db or write_db).
    columns_read: Arc<Mutex<HashSet<DBCol>>>,
    /// Columns that are written to the DB.
    columns_written: Arc<Mutex<HashSet<DBCol>>>,
}

impl ReplayDB {
    pub fn new(split_db: Arc<SplitDB>, archival_columns: HashSet<DBCol>) -> Arc<Self> {
        Arc::new(Self {
            split_db,
            write_db: TestDB::new(),
            archival_columns,
            columns_read: Default::default(),
            columns_written: Default::default(),
        })
    }

    /// Returns the database to read from based on whether the column is archival or not.
    fn read_db(&self, col: DBCol) -> &dyn Database {
        if self.archival_columns.contains(&col) {
            self.split_db.as_ref()
        } else {
            self.write_db.as_ref()
        }
    }

    /// Returns the set of columns read from the store since its creation.
    pub fn get_columns_read(&self) -> HashSet<DBCol> {
        self.columns_read.lock().unwrap().clone()
    }

    /// Returns the set of columns read from the store since its creation.
    pub fn get_columns_written(&self) -> HashSet<DBCol> {
        self.columns_written.lock().unwrap().clone()
    }
}

impl Database for ReplayDB {
    fn get_raw_bytes(&self, col: DBCol, key: &[u8]) -> io::Result<Option<DBSlice<'_>>> {
        self.columns_read.lock().unwrap().insert(col);
        self.read_db(col).get_raw_bytes(col, key)
    }

    fn get_with_rc_stripped(&self, col: DBCol, key: &[u8]) -> io::Result<Option<DBSlice<'_>>> {
        assert!(col.is_rc());
        self.columns_read.lock().unwrap().insert(col);
        self.read_db(col).get_with_rc_stripped(col, key)
    }

    fn iter<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        self.columns_read.lock().unwrap().insert(col);
        self.read_db(col).iter(col)
    }

    fn iter_prefix<'a>(&'a self, col: DBCol, key_prefix: &'a [u8]) -> DBIterator<'a> {
        self.columns_read.lock().unwrap().insert(col);
        self.read_db(col).iter_prefix(col, key_prefix)
    }

    fn iter_range<'a>(
        &'a self,
        col: DBCol,
        lower_bound: Option<&[u8]>,
        upper_bound: Option<&[u8]>,
    ) -> DBIterator<'a> {
        self.columns_read.lock().unwrap().insert(col);
        self.read_db(col).iter_range(col, lower_bound, upper_bound)
    }

    fn iter_raw_bytes<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        self.columns_read.lock().unwrap().insert(col);
        self.read_db(col).iter_raw_bytes(col)
    }

    fn write(&self, batch: DBTransaction) -> io::Result<()> {
        let columns = batch.columns();
        assert!(
            columns.is_disjoint(&self.archival_columns),
            "Attempted to write archival columns: {:?}",
            columns.intersection(&self.archival_columns).collect_vec()
        );
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
) -> anyhow::Result<Arc<ReplayDB>> {
    let archival_columns = HashSet::from([
        DBCol::BlockMisc,
        DBCol::IncomingReceipts,
        DBCol::Chunks,
        DBCol::ChunkExtra,
        DBCol::BlockHeight,
        DBCol::BlockHeader,
        DBCol::OutgoingReceipts,
        DBCol::State,
        DBCol::Block,
    ]);

    let opener = NodeStorage::opener(
        home_dir,
        near_config.client_config.archive,
        &near_config.config.store,
        near_config.config.cold_store.as_ref(),
    );
    let split_storage = opener.open_in_mode(Mode::ReadOnly).context("Failed to open storage")?;
    match split_storage.get_split_db() {
        Some(split_db) => Ok(ReplayDB::new(split_db, archival_columns)),
        None => Err(anyhow!("Failed to get split store for archival node")),
    }
}

use std::io;
use std::sync::Arc;

use crate::db::{DBIterator, DBSlice, DBTransaction, Database, StoreStatistics};
use crate::DBCol;

#[allow(dead_code)]
#[derive(Clone, Copy, Debug)]
pub enum ReadOrder {
    ReadDBFirst,
    WriteDBFirst,
}

/// MixedDB allows to have dedicated read-only db, and specify the order of data retrieval.
/// With `ReadOrder::ReadDBFirst` you can overwrite some information in DB, without actually modifying it.
/// With `ReadOrder::WriteDBFirst` you can record results of any operations only in a separate DB.
/// This way you can conduct several experiments on data, without having to create checkpoints or restoring data.
/// And you can compare data from different experiments faster, as your write DB will be smaller.
///
/// SplitDB can be considered a `MixedDB { read_db: cold_db, write_db: hot_db, read_order: ReadOrder::WriteDBFirst }`
/// But it also has several assertions about types of columns we can retrieve from `read_db`,
/// And it is suitable for production,
///
/// MixedDB is designed to be used with neard tools, and is not planned for integration into production.
pub struct MixedDB {
    /// Read-only DB.
    read_db: Arc<dyn Database>,
    /// DB for writes.
    write_db: Arc<dyn Database>,
    /// order of data lookup.
    read_order: ReadOrder,
}

impl MixedDB {
    #[allow(dead_code)]
    pub fn new(
        read_db: Arc<dyn Database>,
        write_db: Arc<dyn Database>,
        read_order: ReadOrder,
    ) -> Arc<Self> {
        return Arc::new(MixedDB { read_db, write_db, read_order });
    }

    /// Return the first DB in the order of data lookup
    fn first_db(&self) -> &Arc<dyn Database> {
        match self.read_order {
            ReadOrder::ReadDBFirst => &self.read_db,
            ReadOrder::WriteDBFirst => &self.write_db,
        }
    }

    /// Return the second DB in the order of data lookup
    fn second_db(&self) -> &Arc<dyn Database> {
        match self.read_order {
            ReadOrder::ReadDBFirst => &self.write_db,
            ReadOrder::WriteDBFirst => &self.read_db,
        }
    }

    /// This function is imported from SplitDB, but we may want to refactor them later.
    fn merge_iter<'a>(a: DBIterator<'a>, b: DBIterator<'a>) -> DBIterator<'a> {
        crate::db::SplitDB::merge_iter(a, b)
    }
}

impl Database for MixedDB {
    fn get_raw_bytes(&self, col: DBCol, key: &[u8]) -> io::Result<Option<DBSlice<'_>>> {
        // tracing::debug!(target: "db", ?col, "read raw bytes");

        if let Some(first_result) = self.first_db().get_raw_bytes(col, key)? {
            // tracing::debug!(target: "db", ?col, "read from first");
            return Ok(Some(first_result));
        }

        let second_result = self.second_db().get_raw_bytes(col, key)?;
        // tracing::debug!(target: "db", ?col, "read from second");

        Ok(second_result)
    }

    fn get_with_rc_stripped(&self, col: DBCol, key: &[u8]) -> io::Result<Option<DBSlice<'_>>> {
        assert!(col.is_rc());

        if let Some(first_result) = self.first_db().get_with_rc_stripped(col, key)? {
            return Ok(Some(first_result));
        }
        self.second_db().get_with_rc_stripped(col, key)
    }

    fn iter<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        Self::merge_iter(self.read_db.iter(col), self.write_db.iter(col))
    }

    fn iter_prefix<'a>(&'a self, col: DBCol, key_prefix: &'a [u8]) -> DBIterator<'a> {
        return Self::merge_iter(
            self.first_db().iter_prefix(col, key_prefix),
            self.second_db().iter_prefix(col, key_prefix),
        );
    }

    fn iter_range<'a>(
        &'a self,
        col: DBCol,
        lower_bound: Option<&[u8]>,
        upper_bound: Option<&[u8]>,
    ) -> DBIterator<'a> {
        return Self::merge_iter(
            self.first_db().iter_range(col, lower_bound, upper_bound),
            self.second_db().iter_range(col, lower_bound, upper_bound),
        );
    }

    fn iter_raw_bytes<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        return Self::merge_iter(
            self.first_db().iter_raw_bytes(col),
            self.second_db().iter_raw_bytes(col),
        );
    }

    fn write(&self, batch: DBTransaction) -> io::Result<()> {
        self.write_db.write(batch)
    }

    /// There is no need to flush a read-only DB.
    fn flush(&self) -> io::Result<()> {
        self.write_db.flush()
    }

    /// There is no need to compact an immutable DB.
    fn compact(&self) -> io::Result<()> {
        self.write_db.compact()
    }

    /// Write DB actually has real changes,
    /// so exporting it's statistics is a reasonable request.
    fn get_store_statistics(&self) -> Option<StoreStatistics> {
        self.write_db.get_store_statistics()
    }

    /// There is no need to create checkpoint of an immutable DB.
    fn create_checkpoint(
        &self,
        path: &std::path::Path,
        columns_to_keep: Option<&[DBCol]>,
    ) -> anyhow::Result<()> {
        self.write_db.create_checkpoint(path, columns_to_keep)
    }
}

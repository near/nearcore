use std::io;
use std::sync::Arc;

use crate::db::{DBIterator, DBSlice, DBTransaction, Database, StoreStatistics};
use crate::DBCol;

pub struct AmendedDB {
    /// Read-only db that is read before main.
    amendments: Arc<dyn Database>,
    /// DB for writes and info missing from amendments.
    main: Arc<dyn Database>,
}

impl AmendedDB {
    #[allow(dead_code)]
    pub fn new(amendments: Arc<dyn Database>, main: Arc<dyn Database>) -> Arc<Self> {
        return Arc::new(AmendedDB { amendments, main });
    }
}

impl Database for AmendedDB {
    fn get_raw_bytes(&self, col: DBCol, key: &[u8]) -> io::Result<Option<DBSlice<'_>>> {
        if let Some(amendments_result) = self.amendments.get_raw_bytes(col, key)? {
            return Ok(Some(amendments_result));
        }
        self.main.get_raw_bytes(col, key)
    }

    fn get_with_rc_stripped(&self, col: DBCol, key: &[u8]) -> io::Result<Option<DBSlice<'_>>> {
        assert!(col.is_rc());

        if let Some(amendments_result) = self.amendments.get_with_rc_stripped(col, key)? {
            return Ok(Some(amendments_result));
        }
        self.main.get_with_rc_stripped(col, key)
    }

    /// TODO(posvyatokum): None of the iteration works for this DB
    fn iter<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        tracing::warn!(target: "store", "Iteration is not properly implemented for AmendedDB");
        self.main.iter(col)
    }

    /// TODO(posvyatokum): None of the iteration works for this DB
    fn iter_prefix<'a>(&'a self, col: DBCol, key_prefix: &'a [u8]) -> DBIterator<'a> {
        tracing::warn!(target: "store", "Iteration is not properly implemented for AmendedDB");
        self.main.iter_prefix(col, key_prefix)
    }

    /// TODO(posvyatokum): None of the iteration works for this DB
    fn iter_range<'a>(
        &'a self,
        col: DBCol,
        lower_bound: Option<&[u8]>,
        upper_bound: Option<&[u8]>,
    ) -> DBIterator<'a> {
        tracing::warn!(target: "store", "Iteration is not properly implemented for AmendedDB");
        self.main.iter_range(col, lower_bound, upper_bound)
    }

    /// TODO(posvyatokum): None of the iteration works for this DB
    fn iter_raw_bytes<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        tracing::warn!(target: "store", "Iteration is not properly implemented for AmendedDB");
        self.main.iter_raw_bytes(col)
    }

    /// The split db, in principle, should be read only and only used in view client.
    /// However the view client *does* write to the db in order to update cache.
    /// Hence we need to allow writing to the split db but only write to the hot db.
    fn write(&self, batch: DBTransaction) -> io::Result<()> {
        self.main.write(batch)
    }

    fn flush(&self) -> io::Result<()> {
        self.main.flush()
    }

    fn compact(&self) -> io::Result<()> {
        self.main.compact()
    }

    fn get_store_statistics(&self) -> Option<StoreStatistics> {
        self.main.get_store_statistics()
    }

    fn create_checkpoint(
        &self,
        path: &std::path::Path,
        columns_to_keep: Option<&[DBCol]>,
    ) -> anyhow::Result<()> {
        self.main.create_checkpoint(path, columns_to_keep)
    }
}

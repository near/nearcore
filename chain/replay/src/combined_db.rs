use near_store::db::{DBIterator, DBSlice, DBTransaction, Database, StoreStatistics};
use near_store::{DBCol, Store};
use std::path::Path;
use std::sync::Arc;

/// A combined database that reads flat-state columns from one store
/// and everything else from another.
///
/// This is used for replay scenarios where memtries need to be loaded from
/// an earlier snapshot (whose flat storage is at a lower height) while all
/// other data (trie nodes, blocks, chunks, etc.) comes from the main store.
pub struct CombinedDatabase {
    flat_state_store: Store,
    main_store: Store,
}

impl CombinedDatabase {
    /// Creates a `Store` backed by a `CombinedDatabase`.
    pub fn new(flat_state_store: Store, main_store: Store) -> Store {
        Store::new(Arc::new(Self { flat_state_store, main_store }))
    }

    fn db_for_col(&self, col: DBCol) -> &dyn Database {
        if Self::is_flat_state_col(col) {
            self.flat_state_store.database()
        } else {
            self.main_store.database()
        }
    }

    fn is_flat_state_col(col: DBCol) -> bool {
        matches!(
            col,
            DBCol::FlatState
                | DBCol::FlatStateChanges
                | DBCol::FlatStateDeltaMetadata
                | DBCol::FlatStorageStatus
        )
    }
}

impl Database for CombinedDatabase {
    fn get_raw_bytes(&self, col: DBCol, key: &[u8]) -> Option<DBSlice<'_>> {
        self.db_for_col(col).get_raw_bytes(col, key)
    }

    fn get_with_rc_stripped(&self, col: DBCol, key: &[u8]) -> Option<DBSlice<'_>> {
        self.db_for_col(col).get_with_rc_stripped(col, key)
    }

    fn iter<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        self.db_for_col(col).iter(col)
    }

    fn iter_prefix<'a>(&'a self, col: DBCol, key_prefix: &'a [u8]) -> DBIterator<'a> {
        self.db_for_col(col).iter_prefix(col, key_prefix)
    }

    fn iter_range<'a>(
        &'a self,
        col: DBCol,
        lower_bound: Option<&[u8]>,
        upper_bound: Option<&[u8]>,
    ) -> DBIterator<'a> {
        self.db_for_col(col).iter_range(col, lower_bound, upper_bound)
    }

    fn iter_raw_bytes<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        self.db_for_col(col).iter_raw_bytes(col)
    }

    fn write(&self, _batch: DBTransaction) {
        unimplemented!("CombinedDatabase is read-only")
    }

    fn flush(&self) {
        unimplemented!("CombinedDatabase is read-only")
    }

    fn compact(&self) {
        unimplemented!("CombinedDatabase is read-only")
    }

    fn get_store_statistics(&self) -> Option<StoreStatistics> {
        unimplemented!("CombinedDatabase is read-only")
    }

    fn create_checkpoint(
        &self,
        _path: &Path,
        _columns_to_keep: Option<&[DBCol]>,
    ) -> anyhow::Result<()> {
        unimplemented!("CombinedDatabase is read-only")
    }
}

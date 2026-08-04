use crate::db::{DBIterator, DBSlice, DBTransaction, Database, StoreStatistics};
use crate::{DBCol, Store, deserialized_column};
use std::sync::{Arc, OnceLock};

/// Reads a cold database for a caller whose column is not known statically, such as the
/// entity debug endpoint where the column follows from a request field.
///
/// Cold storage holds only the columns `DBCol::is_in_colddb` admits, so a read of any other
/// column can never return data. [`crate::db::ColdDB`] expects its callers to pick such a
/// column in code and does not serve those reads. This wrapper instead records the column and
/// reports it absent, leaving the caller to turn [`Self::rejected_column`] into an error.
pub struct ColumnCheckedColdDB {
    cold: Store,
    rejected: OnceLock<DBCol>,
}

impl ColumnCheckedColdDB {
    pub fn new(cold: Store) -> Arc<Self> {
        Arc::new(Self { cold, rejected: OnceLock::new() })
    }

    /// The first column asked for that cold storage never holds.
    pub fn rejected_column(&self) -> Option<DBCol> {
        self.rejected.get().copied()
    }

    fn is_readable(&self, col: DBCol) -> bool {
        if col.is_in_colddb() {
            return true;
        }
        self.rejected.get_or_init(|| col);
        false
    }

    fn cold(&self) -> &dyn Database {
        self.cold.database()
    }
}

impl Database for ColumnCheckedColdDB {
    fn get_raw_bytes(&self, col: DBCol, key: &[u8]) -> Option<DBSlice<'_>> {
        if !self.is_readable(col) {
            return None;
        }
        self.cold().get_raw_bytes(col, key)
    }

    fn get_with_rc_stripped(&self, col: DBCol, key: &[u8]) -> Option<DBSlice<'_>> {
        if !self.is_readable(col) {
            return None;
        }
        self.cold().get_with_rc_stripped(col, key)
    }

    fn iter<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        if !self.is_readable(col) {
            return Box::new(std::iter::empty());
        }
        self.cold().iter(col)
    }

    fn iter_prefix<'a>(&'a self, col: DBCol, key_prefix: &'a [u8]) -> DBIterator<'a> {
        if !self.is_readable(col) {
            return Box::new(std::iter::empty());
        }
        self.cold().iter_prefix(col, key_prefix)
    }

    fn iter_range<'a>(
        &'a self,
        col: DBCol,
        lower_bound: Option<&[u8]>,
        upper_bound: Option<&[u8]>,
    ) -> DBIterator<'a> {
        if !self.is_readable(col) {
            return Box::new(std::iter::empty());
        }
        self.cold().iter_range(col, lower_bound, upper_bound)
    }

    fn iter_raw_bytes<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        if !self.is_readable(col) {
            return Box::new(std::iter::empty());
        }
        self.cold().iter_raw_bytes(col)
    }

    fn write(&self, batch: DBTransaction) {
        self.cold().write(batch)
    }

    fn flush(&self) {
        self.cold().flush()
    }

    fn compact(&self) {
        self.cold().compact()
    }

    fn get_store_statistics(&self) -> Option<StoreStatistics> {
        self.cold().get_store_statistics()
    }

    fn create_checkpoint(
        &self,
        path: &std::path::Path,
        columns_to_keep: Option<&[DBCol]>,
    ) -> anyhow::Result<()> {
        self.cold().create_checkpoint(path, columns_to_keep)
    }

    fn deserialized_column_cache(&self) -> Arc<deserialized_column::Cache> {
        self.cold().deserialized_column_cache()
    }
}

#[cfg(test)]
mod tests {
    use super::ColumnCheckedColdDB;
    use crate::DBCol;
    use crate::Store;
    use crate::db::metadata::{DB_VERSION, DbKind};
    use crate::db::{DBTransaction, Database, TestDB};
    use crate::test_utils::create_test_node_storage_with_cold;
    use std::sync::Arc;

    fn cold_store_and_db() -> (Store, Arc<TestDB>) {
        let (storage, _hot, cold) = create_test_node_storage_with_cold(DB_VERSION, DbKind::Hot);
        (storage.get_cold_store().unwrap(), cold)
    }

    #[test]
    fn test_column_absent_from_cold_storage_is_reported() {
        let checked = ColumnCheckedColdDB::new(cold_store_and_db().0);
        let store = Store::new(checked.clone());

        assert!(!DBCol::BlockHeight.is_in_colddb());
        assert_eq!(store.get(DBCol::BlockHeight, &[0]), None);
        assert_eq!(store.iter(DBCol::BlockHeight).count(), 0);
        assert_eq!(checked.rejected_column(), Some(DBCol::BlockHeight));
    }

    #[test]
    fn test_columns_present_in_cold_storage_are_served() {
        let (cold, cold_db) = cold_store_and_db();
        let checked = ColumnCheckedColdDB::new(cold);
        let store = Store::new(checked.clone());

        // `Block` is copied into cold storage; `BlockMisc` is not copied but holds cold
        // storage's own head bookkeeping, so both must be readable.
        assert!(DBCol::Block.is_in_colddb() && DBCol::BlockMisc.is_in_colddb());
        let mut transaction = DBTransaction::new();
        transaction.set(DBCol::Block, vec![1], b"block".to_vec());
        transaction.set(DBCol::BlockMisc, vec![2], b"misc".to_vec());
        cold_db.write(transaction);

        assert_eq!(store.get(DBCol::Block, &[1]).as_deref(), Some(b"block".as_slice()));
        assert_eq!(store.get(DBCol::BlockMisc, &[2]).as_deref(), Some(b"misc".as_slice()));
        assert_eq!(checked.rejected_column(), None);
    }
}

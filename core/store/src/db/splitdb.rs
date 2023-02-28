// TODO remove the allows
#![allow(dead_code)]
#![allow(unused_variables)]

use std::io;
use std::sync::Arc;

use near_o11y::log_assert;

use crate::db::{DBIterator, DBSlice, DBTransaction, Database};
use crate::DBCol;

use super::StoreStatistics;

/// A database that provides access to the hot and cold databases.
///
/// For hot-only columns it always reads from the hot database only. For cold
/// columns it reads from hot first and if the value is present it returns it.
/// If the value is not present it reads from the cold database.
///
/// The iter and iter_prefix methods return either the iterator of the hot
/// database or of the cold database depending on if the column is hot or cold.
///
/// The iter_raw_bytes method is not supported but it falls back to returning
/// the hot storage iterator.
///
/// This is a read-only database that doesn't support writing. This is because
/// it would be not clear what database should be written to.
pub struct SplitDB {
    hot: Arc<dyn Database>,
    cold: Arc<dyn Database>,
}

impl SplitDB {
    pub fn new(hot: Arc<dyn Database>, cold: Arc<dyn Database>) -> Arc<Self> {
        return Arc::new(SplitDB { hot, cold });
    }
}

impl Database for SplitDB {
    /// Returns raw bytes for given `key` ignoring any reference count decoding
    /// if any.
    ///
    /// First tries to read the data from the hot db and returns it if found.
    /// Then it tries to read the data from the cold db and returns the result.
    fn get_raw_bytes(&self, col: DBCol, key: &[u8]) -> io::Result<Option<DBSlice<'_>>> {
        if let Some(hot_result) = self.hot.get_raw_bytes(col, key)? {
            return Ok(Some(hot_result));
        }
        if col.is_cold() {
            return self.cold.get_raw_bytes(col, key);
        }
        Ok(None)
    }

    /// Returns value for given `key` forcing a reference count decoding.
    ///
    /// **Panics** if the column is not reference counted.
    ///
    /// First tries to read the data from the hot db and returns it if found.
    /// Then it tries to read the data from the cold db and returns the result.
    fn get_with_rc_stripped(&self, col: DBCol, key: &[u8]) -> io::Result<Option<DBSlice<'_>>> {
        assert!(col.is_rc());

        if let Some(hot_result) = self.hot.get_with_rc_stripped(col, key)? {
            return Ok(Some(hot_result));
        }
        if col.is_cold() {
            return self.cold.get_with_rc_stripped(col, key);
        }
        Ok(None)
    }

    /// Iterate over all items in given column in lexicographical order sorted
    /// by the key.
    ///
    /// For cold columns returns the cold iterator.
    /// For other columns returns the hot iterator.
    ///
    /// Keep in mind that the cold db is typically behind by a few blocks. It
    /// may be behind by up to gc_num_epochs_to_keep epochs under acceptable
    /// conditions or more if the cold store loop is falling behind under
    /// erroneous conditions.
    fn iter<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        match col.is_cold() {
            false => self.hot.iter(col),
            true => self.cold.iter(col),
        }
    }

    /// Iterate over items in given column whose keys start with given prefix.
    ///
    /// For cold columns returns the cold iterator.
    /// For other columns returns the hot iterator.
    fn iter_prefix<'a>(&'a self, col: DBCol, key_prefix: &'a [u8]) -> DBIterator<'a> {
        match col.is_cold() {
            false => self.hot.iter_prefix(col, key_prefix),
            true => self.cold.iter_prefix(col, key_prefix),
        }
    }

    /// Iterate over items in given column whose keys are between [lower_bound, upper_bound)
    ///
    /// Upper_bound key is not included.
    /// If lower_bound is None - the iterator starts from the first key.
    /// If upper_bound is None - iterator continues to the last key.
    fn iter_range<'a>(
        &'a self,
        col: DBCol,
        lower_bound: Option<&'a [u8]>,
        upper_bound: Option<&'a [u8]>,
    ) -> DBIterator<'a> {
        match col.is_cold() {
            false => self.hot.iter_range(col, lower_bound, upper_bound),
            true => self.cold.iter_range(col, lower_bound, upper_bound),
        }
    }

    /// Iterate over items in given column bypassing reference count decoding if
    /// any. This method falls back to the hot iter_raw_bytes because ColdDB
    /// doesn't implement it.
    fn iter_raw_bytes<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        log_assert!(false, "unexpected call to iter_raw_bytes on the split storage");
        self.hot.iter_raw_bytes(col)
    }

    fn write(&self, batch: DBTransaction) -> io::Result<()> {
        let msg = "write is not allowed - the split storage is read only.";
        log_assert!(false, "{}", msg);
        return Err(std::io::Error::new(std::io::ErrorKind::Other, msg));
    }

    fn flush(&self) -> io::Result<()> {
        let msg = "flush is not allowed - the split storage is read only.";
        log_assert!(false, "{}", msg);
        return Err(std::io::Error::new(std::io::ErrorKind::Other, msg));
    }

    fn compact(&self) -> io::Result<()> {
        let msg = "compact is not allowed - the split storage is read only.";
        log_assert!(false, "{}", msg);
        return Err(std::io::Error::new(std::io::ErrorKind::Other, msg));
    }

    fn get_store_statistics(&self) -> Option<StoreStatistics> {
        log_assert!(
            false,
            "get_store_statistics is not allowed - the split storage has two stores"
        );
        None
    }
}

mod test {
    use super::*;

    use crate::db::{testdb::TestDB, ColdDB, DBOp, DBTransaction};

    const FOO: &[u8] = b"FOO";
    const BAR: &[u8] = b"BAR";
    const BAZ: &[u8] = b"BAZ";
    const NOT_FOO: &[u8] = b"NOT_FOO";

    fn create_hot() -> Arc<dyn Database> {
        TestDB::new()
    }

    fn create_cold() -> Arc<dyn Database> {
        Arc::new(ColdDB::new(TestDB::new()))
    }

    fn create_split() -> Arc<dyn Database> {
        SplitDB::new(create_hot(), create_cold())
    }

    fn set(db: &Arc<dyn Database>, col: DBCol, key: &[u8], value: &[u8]) -> () {
        let op = DBOp::Set { col, key: key.to_vec(), value: value.to_vec() };
        db.write(DBTransaction { ops: vec![op] }).unwrap();
    }

    fn set_rc(db: &Arc<dyn Database>, col: DBCol, key: &[u8], value: &[u8]) -> () {
        const ONE: &[u8] = &1i64.to_le_bytes();
        let op = DBOp::UpdateRefcount { col, key: key.to_vec(), value: [&value, ONE].concat() };
        db.write(DBTransaction { ops: vec![op] }).unwrap();
    }

    #[test]
    fn test_get_raw_bytes() {
        let hot = create_hot();
        let cold = create_cold();
        let split = SplitDB::new(hot.clone(), cold.clone());

        // Block is a nice column for testing because is is a cold column but
        // cold doesn't do anything funny to it.
        let col = DBCol::Block;

        // Test 1: Write two different values to the hot db and to the cold db
        // and verify we can read the hot value from the split db.
        let key = FOO;
        set(&hot, col, key, FOO);
        set(&cold, col, key, NOT_FOO);

        let value = split.get_raw_bytes(col, key).unwrap();
        assert_eq!(value.as_deref(), Some(FOO));

        // Test 2: Write to the cold db only and verify that we can read the
        // value from the split db.
        let key = BAR;
        set(&cold, col, key, BAR);

        let value = split.get_raw_bytes(col, key).unwrap();
        assert_eq!(value.as_deref(), Some(BAR));

        // Test 3: Try reading from a non-cold column and verify it returns None
        // even if the value is set in the cold db.
        let col = DBCol::BlockHeader;
        let key = BAZ;

        set(&cold, col, key, BAZ);
        let value = split.get_raw_bytes(col, key).unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn test_get_with_rc_stripped() {
        let hot = create_hot();
        let cold = create_cold();
        let split = SplitDB::new(hot.clone(), cold.clone());

        // Transactions is a nice reference counted column for testing because
        // is is a cold column but cold doesn't do anything funny to it.
        let col = DBCol::Transactions;

        // Test 1: Write two different values to the hot db and to the cold db
        // and verify we can read the hot value from the split db.
        let key = FOO;
        set_rc(&hot, col, key, FOO);
        set_rc(&cold, col, key, NOT_FOO);

        let value = split.get_with_rc_stripped(col, key).unwrap();
        assert_eq!(value.as_deref(), Some(FOO));

        // Test 2: Write to the cold db only and verify that we can read the
        // value from the split db.
        let key = BAR;
        set_rc(&cold, col, key, BAR);

        let value = split.get_with_rc_stripped(col, key).unwrap();
        assert_eq!(value.as_deref(), Some(BAR));

        // Test 3: nothing, there aren't any non-cold reference counted columns.
    }
}

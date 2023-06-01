use itertools::{self, EitherOrBoth};
use std::cmp::Ordering;
use std::io;
use std::sync::Arc;

use near_o11y::log_assert_fail;

use crate::db::{DBIterator, DBIteratorItem, DBSlice, DBTransaction, Database, StoreStatistics};
use crate::DBCol;

/// A database that provides access to the hot and cold databases.
///
/// For hot-only columns it always reads from the hot database only. For cold
/// columns it reads from hot first and if the value is present it returns it.
/// If the value is not present it reads from the cold database.
///
/// The iter* methods return a merge iterator of hot and cold iterators.
///
/// This database should be treated as read-only but it is not enforced because
/// even the view client writes to the database in order to update caches.
pub struct SplitDB {
    hot: Arc<dyn Database>,
    cold: Arc<dyn Database>,
}

impl SplitDB {
    pub fn new(hot: Arc<dyn Database>, cold: Arc<dyn Database>) -> Arc<Self> {
        return Arc::new(SplitDB { hot, cold });
    }

    /// The cmp function for the DBIteratorItems.
    ///
    /// Note that this does not implement total ordering because there isn't a
    /// clear way to compare errors to errors or errors to values. Instead it
    /// implements total order on values but always compares the error on the
    /// left as lesser. This isn't even partial order. It is fine for merging
    /// lists but should not be used for anything more complex like sorting.
    fn db_iter_item_cmp(a: &DBIteratorItem, b: &DBIteratorItem) -> Ordering {
        match (a, b) {
            // Always put errors first.
            (Err(_), _) => Ordering::Less,
            (_, Err(_)) => Ordering::Greater,
            // When comparing two (key, value) paris only compare the keys.
            // - values in hot and cold may differ in rc but they are still the same
            // - values written to cold should be immutable anyway
            // - values writted to cold should be final
            (Ok((a_key, _)), Ok((b_key, _))) => Ord::cmp(a_key, b_key),
        }
    }

    /// Returns merge iterator for the given two DBIterators. The returned
    /// iterator will contain unique and sorted items from both input iterators.
    ///
    /// All errors from both inputs will be returned.
    fn merge_iter<'a>(a: DBIterator<'a>, b: DBIterator<'a>) -> DBIterator<'a> {
        // Merge the two iterators using the cmp function. The result will be an
        // iter of EitherOrBoth.
        let iter = itertools::merge_join_by(a, b, Self::db_iter_item_cmp);
        // Flatten the EitherOrBoth while discarding duplicates. Each input
        // iterator should only contain unique and sorted items. If any item is
        // present in both iterors before the merge, it will get mapped to a
        // EitherOrBoth::Both in the merged iter. Pick either and discard the
        // other - this will guarantee uniqueness of the resulting iterator.
        let iter = iter.map(|item| match item {
            EitherOrBoth::Both(item, _) => item,
            EitherOrBoth::Left(item) => item,
            EitherOrBoth::Right(item) => item,
        });
        Box::new(iter)
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
    /// The returned iterator will iterate through items in both the cold store
    /// and the hot store. The items will be deduplicated and sorted.
    fn iter<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        if !col.is_cold() {
            return self.hot.iter(col);
        }

        Self::merge_iter(self.hot.iter(col), self.cold.iter(col))
    }

    /// Iterate over items in given column, whose keys start with given prefix,
    /// in lexicographical order sorted by the key.
    ///
    /// The returned iterator will iterate through items in both the cold store
    /// and the hot store. The items will be unique and sorted.
    fn iter_prefix<'a>(&'a self, col: DBCol, key_prefix: &'a [u8]) -> DBIterator<'a> {
        if !col.is_cold() {
            return self.hot.iter_prefix(col, key_prefix);
        }

        return Self::merge_iter(
            self.hot.iter_prefix(col, key_prefix),
            self.cold.iter_prefix(col, key_prefix),
        );
    }

    /// Iterate over items in given column whose keys are between [lower_bound, upper_bound)
    ///
    /// Upper_bound key is not included.
    /// If lower_bound is None - the iterator starts from the first key.
    /// If upper_bound is None - iterator continues to the last key.
    ///
    /// The returned iterator will iterate through items in both the cold store
    /// and the hot store. The items will be unique and sorted.
    fn iter_range<'a>(
        &'a self,
        col: DBCol,
        lower_bound: Option<&[u8]>,
        upper_bound: Option<&[u8]>,
    ) -> DBIterator<'a> {
        if !col.is_cold() {
            return self.hot.iter_range(col, lower_bound, upper_bound);
        }

        return Self::merge_iter(
            self.hot.iter_range(col, lower_bound, upper_bound),
            self.cold.iter_range(col, lower_bound, upper_bound),
        );
    }

    /// Iterate over items in given column bypassing reference count decoding if
    /// any.
    ///
    /// The returned iterator will iterate through items in both the cold store
    /// and the hot store. The items will be unique and sorted.
    fn iter_raw_bytes<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        if !col.is_cold() {
            return self.hot.iter_raw_bytes(col);
        }

        return Self::merge_iter(self.hot.iter_raw_bytes(col), self.cold.iter_raw_bytes(col));
    }

    /// The split db, in principle, should be read only and only used in view client.
    /// However the view client *does* write to the db in order to update cache.
    /// Hence we need to allow writing to the split db but only write to the hot db.
    fn write(&self, batch: DBTransaction) -> io::Result<()> {
        self.hot.write(batch)
    }

    fn flush(&self) -> io::Result<()> {
        let msg = "flush is not allowed - the split storage is read only.";
        log_assert_fail!("{}", msg);
        self.hot.flush()?;
        self.cold.flush()?;
        Ok(())
    }

    fn compact(&self) -> io::Result<()> {
        let msg = "compact is not allowed - the split storage is read only.";
        log_assert_fail!("{}", msg);
        self.hot.compact()?;
        self.cold.compact()?;
        Ok(())
    }

    fn get_store_statistics(&self) -> Option<StoreStatistics> {
        log_assert_fail!("get_store_statistics is not allowed - the split storage has two stores");
        None
    }

    fn create_checkpoint(&self, _path: &std::path::Path) -> anyhow::Result<()> {
        log_assert_fail!("create_checkpoint is not allowed - the split storage has two stores");
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;

    use super::*;

    use crate::db::{testdb::TestDB, ColdDB, DBOp, DBTransaction};

    const FOO: &[u8] = b"FOO";
    const BAR: &[u8] = b"BAR";
    const BAZ: &[u8] = b"BAZ";
    const NOT_FOO: &[u8] = b"NOT_FOO";

    const FOO_VALUE: &[u8] = b"FOO_VALUE";
    const BAR_VALUE: &[u8] = b"BAR_VALUE";
    const BAZ_VALUE: &[u8] = b"BAZ_VALUE";

    fn create_hot() -> Arc<dyn Database> {
        TestDB::new()
    }

    fn create_cold() -> Arc<dyn Database> {
        Arc::new(ColdDB::new(TestDB::new()))
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

    fn bx<const SIZE: usize>(literal: &[u8; SIZE]) -> Box<[u8]> {
        Box::new(*literal)
    }

    fn append_rc(value: &[u8]) -> Box<[u8]> {
        const ONE: &[u8] = &1i64.to_le_bytes();
        [&value, ONE].concat().into()
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

    #[test]
    fn test_iter() {
        let hot = create_hot();
        let cold = create_cold();
        let split = SplitDB::new(hot.clone(), cold.clone());

        let col = DBCol::Transactions;

        // Set values so that hot has foo and bar and cold has foo and baz.

        set_rc(&hot, col, FOO, FOO_VALUE);
        set_rc(&hot, col, BAR, BAR_VALUE);

        set_rc(&cold, col, FOO, FOO_VALUE);
        set_rc(&cold, col, BAZ, BAZ_VALUE);

        // Check that the resulting iterator is sorted and unique.

        let iter = split.iter(col);
        let iter = iter.map(|item| item.unwrap());
        let result = iter.collect_vec();
        let expected_result: Vec<(Box<[u8]>, Box<[u8]>)> = vec![
            (BAR.into(), BAR_VALUE.into()),
            (BAZ.into(), BAZ_VALUE.into()),
            (FOO.into(), FOO_VALUE.into()),
        ];
        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_iter_raw_bytes() {
        let hot = create_hot();
        let cold = create_cold();
        let split = SplitDB::new(hot.clone(), cold.clone());

        let col = DBCol::Transactions;

        // Set values so that hot has foo and bar and cold has foo and baz.

        set_rc(&hot, col, FOO, FOO_VALUE);
        set_rc(&hot, col, BAR, BAR_VALUE);

        set_rc(&cold, col, FOO, FOO_VALUE);
        set_rc(&cold, col, BAZ, BAZ_VALUE);

        let iter = split.iter_raw_bytes(col);
        let iter = iter.map(|item| item.unwrap());
        let result = iter.collect_vec();
        let expected_result: Vec<(Box<[u8]>, Box<[u8]>)> = vec![
            (BAR.into(), append_rc(BAR_VALUE)),
            (BAZ.into(), append_rc(BAZ_VALUE)),
            (FOO.into(), append_rc(FOO_VALUE)),
        ];
        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_iter_prefix() {
        let hot = create_hot();
        let cold = create_cold();
        let split = SplitDB::new(hot.clone(), cold.clone());

        let col = DBCol::Transactions;

        // Set values so that hot has foo and bar and cold has foo and baz.

        set_rc(&hot, col, FOO, FOO_VALUE);
        set_rc(&hot, col, BAR, BAR_VALUE);

        set_rc(&cold, col, FOO, FOO_VALUE);
        set_rc(&cold, col, BAZ, BAZ_VALUE);

        // Check that the resulting iterator contains only keys starting with
        // the prefix.

        let key_prefix = b"BA";
        let iter = split.iter_prefix(col, key_prefix);
        let iter = iter.map(|item| item.unwrap());
        let result = iter.collect_vec();
        let expected_result: Vec<(Box<[u8]>, Box<[u8]>)> =
            vec![(BAR.into(), BAR_VALUE.into()), (BAZ.into(), BAZ_VALUE.into())];
        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_iter_range() {
        let hot = create_hot();
        let cold = create_cold();
        let split = SplitDB::new(hot.clone(), cold.clone());

        let col = DBCol::Transactions;

        // Set values so that hot has the lower FOOs and cold has the higher
        // FOOs, with some overlap.

        set_rc(&hot, col, b"FOO1", b"FOO1_VALUE");
        set_rc(&hot, col, b"FOO2", b"FOO2_VALUE");
        set_rc(&hot, col, b"FOO3", b"FOO3_VALUE");
        set_rc(&hot, col, b"FOO4", b"FOO4_VALUE");
        set_rc(&hot, col, b"FOO5", b"FOO5_VALUE");

        set_rc(&cold, col, b"FOO4", b"FOO4_VALUE");
        set_rc(&cold, col, b"FOO5", b"FOO5_VALUE");
        set_rc(&cold, col, b"FOO6", b"FOO6_VALUE");
        set_rc(&cold, col, b"FOO7", b"FOO7_VALUE");

        // Check that the resulting iterator contains only keys starting with
        // the prefix from both hot and cold.
        let lower_bound = Some(b"FOO2".as_slice());
        let upper_bound = Some(b"FOO7".as_slice());

        let iter = split.iter_range(col, lower_bound, upper_bound);
        let iter = iter.map(|item| item.unwrap());
        let result = iter.collect_vec();
        let expected_result: Vec<(Box<[u8]>, Box<[u8]>)> = vec![
            (bx(b"FOO2"), bx(b"FOO2_VALUE")),
            (bx(b"FOO3"), bx(b"FOO3_VALUE")),
            (bx(b"FOO4"), bx(b"FOO4_VALUE")),
            (bx(b"FOO5"), bx(b"FOO5_VALUE")),
            (bx(b"FOO6"), bx(b"FOO6_VALUE")),
        ];
        assert_eq!(result, expected_result);
    }
}

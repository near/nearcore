use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use crate::db::{DBIterator, DBOp, DBSlice, DBTransaction, Database};
use crate::DBCol;

use super::{ColdDB, StatsValue};

/// A database built on top of the cold storage, designed specifically for data recovery.
/// DO NOT USE IN PRODUCTION 🔥🐉.
pub struct RecoveryDB {
    cold: Arc<ColdDB>,
    ops_written: AtomicI64,
}

impl Database for RecoveryDB {
    /// Returns raw bytes for given `key` ignoring any reference count decoding if any.
    fn get_raw_bytes(&self, col: DBCol, key: &[u8]) -> std::io::Result<Option<DBSlice<'_>>> {
        self.cold.get_raw_bytes(col, key)
    }

    /// Returns value for given `key` forcing a reference count decoding.
    fn get_with_rc_stripped(&self, col: DBCol, key: &[u8]) -> std::io::Result<Option<DBSlice<'_>>> {
        self.cold.get_with_rc_stripped(col, key)
    }

    /// Iterates over all values in a column.
    fn iter<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        self.cold.iter(col)
    }

    /// Iterates over values in a given column whose key has given prefix.
    fn iter_prefix<'a>(&'a self, col: DBCol, key_prefix: &'a [u8]) -> DBIterator<'a> {
        self.cold.iter_prefix(col, key_prefix)
    }

    /// Iterate over items in given column bypassing reference count decoding if any.
    fn iter_raw_bytes<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        self.cold.iter_raw_bytes(col)
    }

    /// Iterate over items in given column whose keys are between [lower_bound, upper_bound)
    fn iter_range<'a>(
        &'a self,
        col: DBCol,
        lower_bound: Option<&[u8]>,
        upper_bound: Option<&[u8]>,
    ) -> DBIterator<'a> {
        self.cold.iter_range(col, lower_bound, upper_bound)
    }

    /// Atomically applies operations in given transaction.
    fn write(&self, mut transaction: DBTransaction) -> std::io::Result<()> {
        self.filter_db_ops(&mut transaction);
        if !transaction.ops.is_empty() {
            self.ops_written.fetch_add(transaction.ops.len() as i64, Ordering::Relaxed);
            self.cold.write(transaction)
        } else {
            Ok(())
        }
    }

    fn compact(&self) -> std::io::Result<()> {
        self.cold.compact()
    }

    fn flush(&self) -> std::io::Result<()> {
        self.cold.flush()
    }

    fn get_store_statistics(&self) -> Option<crate::StoreStatistics> {
        let ops_written = (
            "ops_written".to_string(),
            vec![StatsValue::Count(self.ops_written.load(Ordering::Relaxed))],
        );
        let stats = crate::StoreStatistics { data: vec![ops_written] };
        Some(stats)
    }

    fn create_checkpoint(
        &self,
        path: &std::path::Path,
        columns_to_keep: Option<&[DBCol]>,
    ) -> anyhow::Result<()> {
        self.cold.create_checkpoint(path, columns_to_keep)
    }
}

impl RecoveryDB {
    pub fn new(cold: Arc<ColdDB>) -> Self {
        let ops_written = AtomicI64::new(0);
        Self { cold, ops_written }
    }

    /// Filters out deletes and other operation which aren't adding new data to the DB.
    fn filter_db_ops(&self, transaction: &mut DBTransaction) {
        let mut idx = 0;
        while idx < transaction.ops.len() {
            if self.keep_db_op(&mut transaction.ops[idx]) {
                idx += 1;
            } else {
                transaction.ops.swap_remove(idx);
            }
        }
    }

    /// Returns whether the operation should be kept or dropped.
    fn keep_db_op(&self, op: &mut DBOp) -> bool {
        let overwrites_same_data = |col: &mut DBCol, key: &mut Vec<u8>, value: &mut Vec<u8>| {
            if col.is_rc() {
                if let Ok(Some(old_value)) = self.get_with_rc_stripped(*col, &key) {
                    let value = DBSlice::from_vec(value.clone()).strip_refcount();
                    if let Some(value) = value {
                        if value == old_value {
                            return true;
                        }
                    }
                }
            } else {
                if let Ok(Some(old_value)) = self.get_raw_bytes(*col, &key) {
                    if *old_value == *value {
                        return true;
                    }
                }
            }
            false
        };

        match op {
            DBOp::Set { col, key, value }
            | DBOp::Insert { col, key, value }
            | DBOp::UpdateRefcount { col, key, value } => {
                if !matches!(col, DBCol::State) {
                    return false;
                }
                !overwrites_same_data(col, key, value)
            }
            DBOp::Delete { .. } | DBOp::DeleteAll { .. } | DBOp::DeleteRange { .. } => false,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const HEIGHT_LE: &[u8] = &42u64.to_le_bytes();
    const HASH: &[u8] = [0u8; 32].as_slice();
    const VALUE: &[u8] = "FooBar".as_bytes();
    const ONE: &[u8] = &1i64.to_le_bytes();
    const COL: DBCol = DBCol::State;

    /// Constructs test in-memory database.
    fn create_test_recovery_db() -> RecoveryDB {
        let cold = crate::db::testdb::TestDB::new();
        RecoveryDB::new(Arc::new(ColdDB::new(cold)))
    }

    fn assert_op_writes_only_once(generate_op: impl Fn() -> DBOp, db: RecoveryDB, col: DBCol) {
        // The first time the operation is allowed.
        db.write(DBTransaction { ops: vec![generate_op()] }).unwrap();
        let got = db.cold.get_raw_bytes(col, HASH).unwrap();
        assert_eq!(got.as_deref(), Some([VALUE, ONE].concat().as_slice()));

        // Repeat the same operation: DB write should get dropped.
        let mut tx = DBTransaction { ops: vec![generate_op()] };
        db.filter_db_ops(&mut tx);
        assert_eq!(tx.ops.len(), 0);
    }

    /// Verify that delete-like operations are ignored.
    #[test]
    fn test_deletes() {
        let db = create_test_recovery_db();
        let col = COL;

        let delete = DBOp::Delete { col, key: HASH.to_vec() };
        let delete_all = DBOp::DeleteAll { col };
        let delete_range = DBOp::DeleteRange { col, from: HASH.to_vec(), to: HASH.to_vec() };

        let mut tx = DBTransaction { ops: vec![delete, delete_all, delete_range] };
        db.filter_db_ops(&mut tx);
        assert_eq!(tx.ops.len(), 0);
    }

    #[test]
    fn columns_other_than_state_are_ignored() {
        let db = create_test_recovery_db();
        let col = DBCol::Block;

        let op = DBOp::Set { col, key: HASH.to_vec(), value: [VALUE, ONE].concat() };

        let mut tx = DBTransaction { ops: vec![op] };
        db.filter_db_ops(&mut tx);
        assert_eq!(tx.ops.len(), 0);
    }

    /// Verify that the same value is not overwritten.
    #[test]
    fn test_set() {
        let db = create_test_recovery_db();
        let col = COL;

        let generate_op = || DBOp::Set { col, key: HASH.to_vec(), value: [VALUE, ONE].concat() };

        assert_op_writes_only_once(generate_op, db, col);
    }

    /// Verify that the same value is not overwritten.
    #[test]
    fn test_insert() {
        let db = create_test_recovery_db();
        let col = COL;

        let generate_op = || DBOp::Insert { col, key: HASH.to_vec(), value: [VALUE, ONE].concat() };

        assert_op_writes_only_once(generate_op, db, col);
    }

    /// Verify that the same value is not overwritten.
    #[test]
    fn test_refcount() {
        let db = create_test_recovery_db();
        let col = COL;

        let generate_op =
            || DBOp::UpdateRefcount { col, key: HASH.to_vec(), value: [VALUE, HEIGHT_LE].concat() };

        assert_op_writes_only_once(generate_op, db, col);
    }
}

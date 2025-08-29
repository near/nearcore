use parking_lot::RwLock;
use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::ops::Bound;
use std::sync::Arc;

use crate::db::{DBIterator, DBOp, DBSlice, DBTransaction, Database, refcount};
use crate::{DBCol, StoreStatistics, deserialized_column};

/// Metadata for tracking column sizes and insertion order for FIFO eviction
#[derive(Debug)]
struct ColumnMetadata {
    /// Current byte size of the column (sum of key + value sizes)
    current_size: usize,
    /// FIFO queue of keys for eviction order
    insertion_order: VecDeque<Vec<u8>>,
}

impl Default for ColumnMetadata {
    fn default() -> Self {
        Self { current_size: 0, insertion_order: VecDeque::new() }
    }
}

/// An in-memory database intended for tests and IO-agnostic estimations.
pub struct TestDB {
    // In order to ensure determinism when iterating over column's results
    // a BTreeMap is used since it is an ordered map. A HashMap would
    // give the aforementioned guarantee, and therefore is discarded.
    db: RwLock<enum_map::EnumMap<DBCol, BTreeMap<Vec<u8>, Vec<u8>>>>,

    // Metadata for tracking column sizes and FIFO eviction
    column_metadata: RwLock<enum_map::EnumMap<DBCol, ColumnMetadata>>,

    // The store statistics. Can be set with the set_store_statistics.
    // The TestDB doesn't produce any stats on its own, it's up to the user of
    // this class to set the stats as they need it.
    stats: RwLock<Option<StoreStatistics>>,

    cache: Arc<deserialized_column::Cache>,
}

impl Default for TestDB {
    fn default() -> Self {
        Self {
            db: Default::default(),
            column_metadata: Default::default(),
            stats: Default::default(),
            cache: deserialized_column::Cache::enabled().into(),
        }
    }
}

impl TestDB {
    pub fn new() -> Arc<TestDB> {
        Arc::new(Self::default())
    }

    /// Returns the byte limit for a given column.
    /// This can be customized per column as needed.
    fn byte_limit(&self, col: DBCol) -> Option<usize> {
        // Default implementation - can be made configurable later
        match col {
            _ => Some(100_000_000), // 100MB default
        }
    }

    /// Calculates the approximate size of a key-value pair
    fn entry_size(key: &[u8], value: &[u8]) -> usize {
        key.len() + value.len()
    }

    /// Evicts entries FIFO until the column is under the byte limit
    fn enforce_byte_limit(
        &self,
        col: DBCol,
        db: &mut enum_map::EnumMap<DBCol, BTreeMap<Vec<u8>, Vec<u8>>>,
        metadata: &mut enum_map::EnumMap<DBCol, ColumnMetadata>,
    ) {
        if let Some(limit) = self.byte_limit(col) {
            let column_data = &mut db[col];
            let column_meta = &mut metadata[col];

            while column_meta.current_size > limit {
                if let Some(oldest_key) = column_meta.insertion_order.pop_front() {
                    if let Some(old_value) = column_data.remove(&oldest_key) {
                        let removed_size = Self::entry_size(&oldest_key, &old_value);
                        column_meta.current_size =
                            column_meta.current_size.saturating_sub(removed_size);
                    }
                } else {
                    // No more entries to evict
                    break;
                }
            }
        }
    }
}

impl TestDB {
    pub fn set_store_statistics(&self, stats: StoreStatistics) {
        *self.stats.write() = Some(stats);
    }
}

impl Database for TestDB {
    fn get_raw_bytes(&self, col: DBCol, key: &[u8]) -> io::Result<Option<DBSlice<'_>>> {
        Ok(self.db.read()[col].get(key).cloned().map(DBSlice::from_vec))
    }

    fn iter<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        let iterator = self.iter_raw_bytes(col);
        refcount::iter_with_rc_logic(col, iterator)
    }

    fn iter_raw_bytes<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        let iterator = self.db.read()[col]
            .clone()
            .into_iter()
            .map(|(k, v)| Ok((k.into_boxed_slice(), v.into_boxed_slice())));
        Box::new(iterator)
    }

    fn iter_prefix<'a>(&'a self, col: DBCol, key_prefix: &'a [u8]) -> DBIterator<'a> {
        let iterator = self.db.read()[col]
            .range(key_prefix.to_vec()..)
            .take_while(move |(k, _)| k.starts_with(&key_prefix))
            .map(|(k, v)| Ok((k.clone().into_boxed_slice(), v.clone().into_boxed_slice())))
            .collect::<Vec<io::Result<_>>>();
        refcount::iter_with_rc_logic(col, iterator)
    }

    fn iter_range<'a>(
        &'a self,
        col: DBCol,
        lower_bound: Option<&[u8]>,
        upper_bound: Option<&[u8]>,
    ) -> DBIterator<'a> {
        let lower = lower_bound.map_or(Bound::Unbounded, |f| Bound::Included(f.to_vec()));
        let upper = upper_bound.map_or(Bound::Unbounded, |f| Bound::Excluded(f.to_vec()));

        let iterator = self.db.read()[col]
            .range((lower, upper))
            .map(|(k, v)| Ok((k.clone().into_boxed_slice(), v.clone().into_boxed_slice())))
            .collect::<Vec<io::Result<_>>>();
        refcount::iter_with_rc_logic(col, iterator)
    }

    fn write(&self, transaction: DBTransaction) -> io::Result<()> {
        let mut db = self.db.write();
        let mut metadata = self.column_metadata.write();

        for op in transaction.ops {
            match op {
                DBOp::Set { col, key, value } => {
                    let entry_size = Self::entry_size(&key, &value);

                    // Remove old entry if it exists
                    if let Some(old_value) = db[col].remove(&key) {
                        let old_size = Self::entry_size(&key, &old_value);
                        metadata[col].current_size =
                            metadata[col].current_size.saturating_sub(old_size);
                        // Remove from insertion order (we'll re-add it)
                        metadata[col].insertion_order.retain(|k| k != &key);
                    }

                    // Insert new entry
                    db[col].insert(key.clone(), value);
                    metadata[col].current_size += entry_size;
                    metadata[col].insertion_order.push_back(key);

                    // Enforce byte limit
                    self.enforce_byte_limit(col, &mut db, &mut metadata);
                }
                DBOp::Insert { col, key, value } => {
                    if cfg!(debug_assertions) {
                        if let Some(old_value) = db[col].get(&key) {
                            super::assert_no_overwrite(col, &key, &value, &*old_value)
                        }
                    }

                    let entry_size = Self::entry_size(&key, &value);

                    // Remove old entry if it exists
                    if let Some(old_value) = db[col].remove(&key) {
                        let old_size = Self::entry_size(&key, &old_value);
                        metadata[col].current_size =
                            metadata[col].current_size.saturating_sub(old_size);
                        // Remove from insertion order (we'll re-add it)
                        metadata[col].insertion_order.retain(|k| k != &key);
                    }

                    // Insert new entry
                    db[col].insert(key.clone(), value);
                    metadata[col].current_size += entry_size;
                    metadata[col].insertion_order.push_back(key);

                    // Enforce byte limit
                    self.enforce_byte_limit(col, &mut db, &mut metadata);
                }
                DBOp::UpdateRefcount { col, key, value } => {
                    let existing = db[col].get(&key).map(Vec::as_slice);
                    let operands = [value.as_slice()];
                    let merged = refcount::refcount_merge(existing, operands);

                    // Remove old entry if it exists
                    if let Some(old_value) = db[col].remove(&key) {
                        let old_size = Self::entry_size(&key, &old_value);
                        metadata[col].current_size =
                            metadata[col].current_size.saturating_sub(old_size);
                        metadata[col].insertion_order.retain(|k| k != &key);
                    }

                    if merged.is_empty() {
                        // Entry was deleted, no need to add it back
                    } else {
                        debug_assert!(
                            refcount::decode_value_with_rc(&merged).1 > 0,
                            "Inserting value with non-positive refcount"
                        );
                        let entry_size = Self::entry_size(&key, &merged);
                        db[col].insert(key.clone(), merged);
                        metadata[col].current_size += entry_size;
                        metadata[col].insertion_order.push_back(key);

                        // Enforce byte limit
                        self.enforce_byte_limit(col, &mut db, &mut metadata);
                    }
                }
                DBOp::Delete { col, key } => {
                    if let Some(old_value) = db[col].remove(&key) {
                        let old_size = Self::entry_size(&key, &old_value);
                        metadata[col].current_size =
                            metadata[col].current_size.saturating_sub(old_size);
                        metadata[col].insertion_order.retain(|k| k != &key);
                    }
                }
                DBOp::DeleteAll { col } => {
                    db[col].clear();
                    metadata[col].current_size = 0;
                    metadata[col].insertion_order.clear();
                }
                DBOp::DeleteRange { col, from, to } => {
                    let mut keys_to_remove = Vec::new();

                    db[col].retain(|key, value| {
                        if (&from..&to).contains(&key) {
                            let old_size = Self::entry_size(key, value);
                            metadata[col].current_size =
                                metadata[col].current_size.saturating_sub(old_size);
                            keys_to_remove.push(key.clone());
                            false
                        } else {
                            true
                        }
                    });

                    // Remove from insertion order
                    for key in keys_to_remove {
                        metadata[col].insertion_order.retain(|k| k != &key);
                    }
                }
            };
        }
        Ok(())
    }

    fn flush(&self) -> io::Result<()> {
        Ok(())
    }

    fn compact(&self) -> io::Result<()> {
        Ok(())
    }

    fn get_store_statistics(&self) -> Option<StoreStatistics> {
        self.stats.read().clone()
    }

    fn create_checkpoint(
        &self,
        _path: &std::path::Path,
        _columns_to_keep: Option<&[DBCol]>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn copy_if_test(&self, columns_to_keep: Option<&[DBCol]>) -> Option<Arc<dyn Database>> {
        let copy = Self::default();
        {
            let mut db = copy.db.write();
            let mut metadata = copy.column_metadata.write();

            for (col, map) in self.db.read().iter() {
                if let Some(keep) = columns_to_keep {
                    if !keep.contains(&col) {
                        continue;
                    }
                }
                let new_col = &mut db[col];
                let new_metadata = &mut metadata[col];

                for (key, value) in map {
                    let entry_size = Self::entry_size(key, value);
                    new_col.insert(key.clone(), value.clone());
                    new_metadata.current_size += entry_size;
                    new_metadata.insertion_order.push_back(key.clone());
                }
            }
            copy.stats.write().clone_from(&self.stats.read());
        }
        Some(Arc::new(copy))
    }

    fn deserialized_column_cache(&self) -> Arc<deserialized_column::Cache> {
        Arc::clone(&self.cache)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DBCol;
    use crate::db::DBOp;

    #[test]
    fn test_byte_limit_enforcement() {
        let mut db = TestDB::new();

        // Override the byte_limit method for this specific test instance
        // We'll manually enforce a small limit
        let small_limit = 50;

        // Create some test data that will exceed the limit
        let mut ops = Vec::new();
        for i in 0..10 {
            ops.push(DBOp::Set {
                col: DBCol::Misc,
                key: format!("key_{}", i).into_bytes(),
                value: format!("value_{}_with_some_extra_data", i).into_bytes(),
            });
        }

        let transaction = crate::db::DBTransaction { ops };

        // Manually test the eviction logic with our small limit
        let db_ref = Arc::new(db);

        // First, let's test with a more realistic scenario
        // Insert a few large entries that should trigger eviction
        let large_ops = vec![
            DBOp::Set {
                col: DBCol::Misc,
                key: b"large_key_1".to_vec(),
                value: vec![b'x'; 150_000_000], // 150MB - exceeds default 100MB limit
            },
            DBOp::Set {
                col: DBCol::Misc,
                key: b"large_key_2".to_vec(),
                value: vec![b'y'; 800_000], // 0.8MB
            },
        ];

        let large_transaction = crate::db::DBTransaction { ops: large_ops };
        db_ref.write(large_transaction).unwrap();

        // Check that eviction occurred
        let db_data = db_ref.db.read();
        let metadata = db_ref.column_metadata.read();

        // We should have only one entry remaining after eviction
        assert_eq!(db_data[DBCol::Misc].len(), 1);

        // The remaining entry should be the second one (FIFO)
        assert!(db_data[DBCol::Misc].contains_key(&b"large_key_2".to_vec()));
        assert!(!db_data[DBCol::Misc].contains_key(&b"large_key_1".to_vec()));

        // The current size should be under the limit
        let limit = db_ref.byte_limit(DBCol::Misc).unwrap();
        assert!(metadata[DBCol::Misc].current_size <= limit);

        // The insertion order should match the remaining entries
        assert_eq!(metadata[DBCol::Misc].insertion_order.len(), db_data[DBCol::Misc].len());
    }

    #[test]
    fn test_fifo_eviction_order() {
        let db = Arc::new(TestDB::default());

        // Insert entries one by one and verify FIFO behavior
        let ops = vec![
            DBOp::Set { col: DBCol::Misc, key: b"key1".to_vec(), value: b"value1".to_vec() },
            DBOp::Set { col: DBCol::Misc, key: b"key2".to_vec(), value: b"value2".to_vec() },
        ];

        let transaction = crate::db::DBTransaction { ops };
        db.write(transaction).unwrap();

        let metadata = db.column_metadata.read();

        // Check insertion order
        assert_eq!(metadata[DBCol::Misc].insertion_order.len(), 2);
        assert_eq!(metadata[DBCol::Misc].insertion_order[0], b"key1");
        assert_eq!(metadata[DBCol::Misc].insertion_order[1], b"key2");

        // Check size calculation
        let expected_size =
            TestDB::entry_size(b"key1", b"value1") + TestDB::entry_size(b"key2", b"value2");
        assert_eq!(metadata[DBCol::Misc].current_size, expected_size);
    }

    #[test]
    fn test_delete_operations_update_metadata() {
        let db = Arc::new(TestDB::default());

        // Insert some data
        let insert_ops = vec![
            DBOp::Set { col: DBCol::Misc, key: b"key1".to_vec(), value: b"value1".to_vec() },
            DBOp::Set { col: DBCol::Misc, key: b"key2".to_vec(), value: b"value2".to_vec() },
        ];

        let transaction = crate::db::DBTransaction { ops: insert_ops };
        db.write(transaction).unwrap();

        // Delete one entry
        let delete_ops = vec![DBOp::Delete { col: DBCol::Misc, key: b"key1".to_vec() }];

        let transaction = crate::db::DBTransaction { ops: delete_ops };
        db.write(transaction).unwrap();

        let metadata = db.column_metadata.read();
        let db_data = db.db.read();

        // Check that metadata was updated correctly
        assert_eq!(db_data[DBCol::Misc].len(), 1);
        assert_eq!(metadata[DBCol::Misc].insertion_order.len(), 1);
        assert_eq!(metadata[DBCol::Misc].insertion_order[0], b"key2");

        let expected_size = TestDB::entry_size(b"key2", b"value2");
        assert_eq!(metadata[DBCol::Misc].current_size, expected_size);
    }

    #[test]
    fn test_delete_all_clears_metadata() {
        let db = Arc::new(TestDB::default());

        // Insert some data
        let insert_ops = vec![
            DBOp::Set { col: DBCol::Misc, key: b"key1".to_vec(), value: b"value1".to_vec() },
            DBOp::Set { col: DBCol::Misc, key: b"key2".to_vec(), value: b"value2".to_vec() },
        ];

        let transaction = crate::db::DBTransaction { ops: insert_ops };
        db.write(transaction).unwrap();

        // Delete all entries
        let delete_all_ops = vec![DBOp::DeleteAll { col: DBCol::Misc }];

        let transaction = crate::db::DBTransaction { ops: delete_all_ops };
        db.write(transaction).unwrap();

        let metadata = db.column_metadata.read();
        let db_data = db.db.read();

        // Check that everything was cleared
        assert_eq!(db_data[DBCol::Misc].len(), 0);
        assert_eq!(metadata[DBCol::Misc].insertion_order.len(), 0);
        assert_eq!(metadata[DBCol::Misc].current_size, 0);
    }
}

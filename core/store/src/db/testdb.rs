use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::io;
use std::ops::Bound;
use std::sync::Arc;

use crate::db::{DBIterator, DBOp, DBSlice, DBTransaction, Database, refcount};
use crate::{DBCol, StoreStatistics};

/// An in-memory database intended for tests and IO-agnostic estimations.
#[derive(Default)]
pub struct TestDB {
    // In order to ensure determinism when iterating over column's results
    // a BTreeMap is used since it is an ordered map. A HashMap would
    // give the aforementioned guarantee, and therefore is discarded.
    db: RwLock<enum_map::EnumMap<DBCol, BTreeMap<Vec<u8>, Vec<u8>>>>,

    // The store statistics. Can be set with the set_store_statistics.
    // The TestDB doesn't produce any stats on its own, it's up to the user of
    // this class to set the stats as they need it.
    stats: RwLock<Option<StoreStatistics>>,
}

impl TestDB {
    pub fn new() -> Arc<TestDB> {
        Arc::new(Self::default())
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
        for op in transaction.ops {
            match op {
                DBOp::Set { col, key, value } => {
                    db[col].insert(key, value);
                }
                DBOp::Insert { col, key, value } => {
                    if cfg!(debug_assertions) {
                        if let Some(old_value) = db[col].get(&key) {
                            super::assert_no_overwrite(col, &key, &value, &*old_value)
                        }
                    }
                    db[col].insert(key, value);
                }
                DBOp::UpdateRefcount { col, key, value } => {
                    let existing = db[col].get(&key).map(Vec::as_slice);
                    let operands = [value.as_slice()];
                    let merged = refcount::refcount_merge(existing, operands);
                    if merged.is_empty() {
                        db[col].remove(&key);
                    } else {
                        debug_assert!(
                            refcount::decode_value_with_rc(&merged).1 > 0,
                            "Inserting value with non-positive refcount"
                        );
                        db[col].insert(key, merged);
                    }
                }
                DBOp::Delete { col, key } => {
                    db[col].remove(&key);
                }
                DBOp::DeleteAll { col } => db[col].clear(),
                DBOp::DeleteRange { col, from, to } => {
                    db[col].retain(|key, _| !(&from..&to).contains(&key));
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
            for (col, map) in self.db.read().iter() {
                if let Some(keep) = columns_to_keep {
                    if !keep.contains(&col) {
                        continue;
                    }
                }
                let new_col = &mut db[col];
                for (key, value) in map {
                    new_col.insert(key.clone(), value.clone());
                }
            }
            copy.stats.write().clone_from(&self.stats.read());
        }
        Some(Arc::new(copy))
    }
}

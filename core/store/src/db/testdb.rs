use std::collections::BTreeMap;
use std::io;
use std::sync::RwLock;

use crate::db::{refcount, DBBytes, DBIterator, DBOp, DBTransaction, Database};
use crate::{DBCol, StoreStatistics};

/// An in-memory database intended for tests.
pub struct TestDB {
    // In order to ensure determinism when iterating over column's results
    // a BTreeMap is used since it is an ordered map. A HashMap would
    // give the aforementioned guarantee, and therefore is discarded.
    db: RwLock<enum_map::EnumMap<DBCol, BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl TestDB {
    pub fn new() -> Self {
        Self { db: Default::default() }
    }
}

impl Database for TestDB {
    fn get_raw_bytes(&self, col: DBCol, key: &[u8]) -> io::Result<Option<DBBytes<'_>>> {
        Ok(self.db.read().unwrap()[col].get(key).map(|vec| DBBytes::from_vec(vec.clone())))
    }

    fn iter<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        let iterator = self.iter_raw_bytes(col);
        refcount::iter_with_rc_logic(col, iterator)
    }

    fn iter_raw_bytes<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        let iterator = self.db.read().unwrap()[col]
            .clone()
            .into_iter()
            .map(|(k, v)| Ok((k.into_boxed_slice(), v.into_boxed_slice())));
        Box::new(iterator)
    }

    fn iter_prefix<'a>(&'a self, col: DBCol, key_prefix: &'a [u8]) -> DBIterator<'a> {
        let iterator = self.db.read().unwrap()[col]
            .range(key_prefix.to_vec()..)
            .take_while(move |(k, _)| k.starts_with(&key_prefix))
            .map(|(k, v)| Ok((k.clone().into_boxed_slice(), v.clone().into_boxed_slice())))
            .collect::<Vec<io::Result<_>>>();
        refcount::iter_with_rc_logic(col, iterator.into_iter())
    }

    fn write(&self, transaction: DBTransaction) -> io::Result<()> {
        let mut db = self.db.write().unwrap();
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
        None
    }
}

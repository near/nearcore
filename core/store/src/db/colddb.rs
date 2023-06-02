use near_o11y::{log_assert, log_assert_fail};

use crate::db::refcount::set_refcount;
use crate::db::{DBIterator, DBOp, DBSlice, DBTransaction, Database};
use crate::DBCol;

/// A database which provides access to the cold storage.
///
/// The data is stored in the same format as in the regular database but for the
/// reference counted columns the rc is always set to 1. This struct handles
/// setting the rc to one transparently to the user.
///
/// Lastly, since no data is ever deleted from cold storage, trying to decrease
/// reference of a value count or delete data is ignored and if debug assertions
/// are enabled will cause a panic.
pub struct ColdDB {
    cold: std::sync::Arc<dyn Database>,
}

impl ColdDB {
    pub fn new(cold: std::sync::Arc<dyn Database>) -> Self {
        Self { cold }
    }

    fn err_msg(col: DBCol) -> String {
        format!("Reading from column missing from cold storage. {col:?}")
    }

    // Checks if the column is is the cold db and returns an error if not.
    fn check_is_in_colddb(col: DBCol) -> std::io::Result<()> {
        if !col.is_in_colddb() {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, Self::err_msg(col)));
        }
        Ok(())
    }

    // Checks if the column is is the cold db and panics if not.
    fn log_assert_is_in_colddb(col: DBCol) {
        log_assert!(col.is_in_colddb(), "{}", Self::err_msg(col));
    }
}

impl Database for ColdDB {
    /// Returns raw bytes for given `key` ignoring any reference count decoding if any.
    fn get_raw_bytes(&self, col: DBCol, key: &[u8]) -> std::io::Result<Option<DBSlice<'_>>> {
        Self::check_is_in_colddb(col)?;
        self.cold.get_raw_bytes(col, key)
    }

    /// Returns value for given `key` forcing a reference count decoding.
    fn get_with_rc_stripped(&self, col: DBCol, key: &[u8]) -> std::io::Result<Option<DBSlice<'_>>> {
        Self::check_is_in_colddb(col)?;
        self.cold.get_with_rc_stripped(col, key)
    }

    /// Iterates over all values in a column.
    fn iter<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        Self::log_assert_is_in_colddb(col);
        self.cold.iter(col)
    }

    /// Iterates over values in a given column whose key has given prefix.
    fn iter_prefix<'a>(&'a self, col: DBCol, key_prefix: &'a [u8]) -> DBIterator<'a> {
        Self::log_assert_is_in_colddb(col);
        self.cold.iter_prefix(col, key_prefix)
    }

    /// Iterate over items in given column bypassing reference count decoding if any.
    fn iter_raw_bytes<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        Self::log_assert_is_in_colddb(col);
        self.cold.iter_raw_bytes(col)
    }

    /// Iterate over items in given column whose keys are between [lower_bound, upper_bound)
    fn iter_range<'a>(
        &'a self,
        col: DBCol,
        lower_bound: Option<&[u8]>,
        upper_bound: Option<&[u8]>,
    ) -> DBIterator<'a> {
        Self::log_assert_is_in_colddb(col);
        self.cold.iter_range(col, lower_bound, upper_bound)
    }

    /// Atomically applies operations in given transaction.
    ///
    /// If debug assertions are enabled, panics if there are any delete
    /// operations or operations decreasing reference count of a value.  If
    /// debug assertions are not enabled, such operations are filtered out.
    fn write(&self, mut transaction: DBTransaction) -> std::io::Result<()> {
        let mut idx = 0;
        while idx < transaction.ops.len() {
            if adjust_op(&mut transaction.ops[idx]) {
                idx += 1;
            } else {
                transaction.ops.swap_remove(idx);
            }
        }
        self.cold.write(transaction)
    }

    fn compact(&self) -> std::io::Result<()> {
        self.cold.compact()
    }

    fn flush(&self) -> std::io::Result<()> {
        self.cold.flush()
    }

    fn get_store_statistics(&self) -> Option<crate::StoreStatistics> {
        self.cold.get_store_statistics()
    }

    fn create_checkpoint(&self, path: &std::path::Path) -> anyhow::Result<()> {
        self.cold.create_checkpoint(path)
    }
}

/// Adjust database operation to be performed on cold storage.
///
/// Returns whether the operation should be kept or dropped.  Generally, dropped
/// columns indicate an unexpected operation which should have never been issued
/// for cold storage.
fn adjust_op(op: &mut DBOp) -> bool {
    if !op.col().is_in_colddb() {
        return false;
    }

    match op {
        DBOp::Set { .. } | DBOp::Insert { .. } => true,
        DBOp::UpdateRefcount { col, key, value } => {
            assert!(col.is_rc());
            // There is no point in keeping track of ref count in the cold store so
            // just always hardcode it to 1.
            let mut value = core::mem::take(value);
            match set_refcount(&mut value, 1) {
                Ok(_) => {
                    *op = DBOp::Set { col: *col, key: core::mem::take(key), value };
                    return true;
                }
                Err(err) => {
                    log_assert_fail!(
                        "Failed to set refcount when writing to cold store. Error: {err}"
                    );
                    return false;
                }
            };
        }
        DBOp::Delete { col, key } => {
            log_assert_fail!("Unexpected delete from {col} in cold store: {key:?}");
            false
        }
        DBOp::DeleteAll { col } => {
            log_assert_fail!("Unexpected delete from {col} in cold store");
            false
        }
        DBOp::DeleteRange { col, from, to } => {
            log_assert_fail!("Unexpected delete range from {col} in cold store: {from:?} {to:?}");
            false
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const HEIGHT_LE: &[u8] = &42u64.to_le_bytes();
    const SHARD: &[u8] = "ShardUId".as_bytes();
    const HASH: &[u8] = [0u8; 32].as_slice();
    const VALUE: &[u8] = "FooBar".as_bytes();
    const ONE: &[u8] = &1i64.to_le_bytes();

    /// Constructs test in-memory database.
    fn create_test_cold_db() -> ColdDB {
        let cold = crate::db::testdb::TestDB::new();
        ColdDB::new(cold)
    }

    fn set(col: DBCol, key: &[u8]) -> DBOp {
        DBOp::Set { col, key: key.to_vec(), value: VALUE.to_vec() }
    }

    fn set_with_rc(col: DBCol, key: &[u8]) -> DBOp {
        let value = [VALUE, ONE].concat();
        DBOp::Set { col, key: key.to_vec(), value: value }
    }

    /// Prettifies raw key for display.
    fn pretty_key(key: &[u8]) -> String {
        fn chunk(chunk: &[u8]) -> String {
            let num = u64::from_le_bytes(chunk.try_into().unwrap());
            if num < 256 {
                format!("le({num})")
            } else if num.swap_bytes() < 256 {
                format!("be({})", num.swap_bytes())
            } else if let Ok(value) = std::str::from_utf8(chunk) {
                value.to_string()
            } else {
                format!("{chunk:x?}")
            }
        }
        fn hash(chunk: &[u8]) -> String {
            crate::CryptoHash::from(chunk.try_into().unwrap()).to_string()
        }
        match key.len() {
            8 => chunk(key),
            16 => format!("`{} || {}`", chunk(&key[..8]), chunk(&key[8..])),
            32 => hash(key),
            40 => format!("`{} || {}`", chunk(&key[..8]), hash(&key[8..])),
            _ => format!("{key:x?}"),
        }
    }

    /// Prettifies raw value for display.
    fn pretty_value(value: Option<&[u8]>, refcount: bool) -> String {
        match value {
            None => "∅".to_string(),
            Some(value) if refcount => {
                let decoded = crate::db::refcount::decode_value_with_rc(value);
                format!("{}; rc: {}", pretty_value(decoded.0, false), decoded.1)
            }
            Some(value) => std::str::from_utf8(value).unwrap().to_string(),
        }
    }

    /// Tests that keys are correctly adjusted when saved in cold store.
    #[test]
    fn test_adjust_key() {
        let db = create_test_cold_db();

        // Test on two columns, one with rc and one without.
        // State -> rc
        // Block -> no rc

        let mut ops = vec![];
        ops.push(set_with_rc(DBCol::State, &[SHARD, HASH].concat()));
        ops.push(set(DBCol::Block, HASH));
        db.write(DBTransaction { ops }).unwrap();

        // Fetch data
        let mut result = Vec::<String>::new();
        let mut fetch = |col, key: &[u8], raw_only: bool| {
            result.push(format!("{col} {}", pretty_key(key)));
            let dbs: [(bool, &dyn Database); 2] = [(false, &db), (true, &*db.cold)];
            for (is_raw, db) in dbs {
                if raw_only && !is_raw {
                    continue;
                }

                let name = if is_raw { "raw " } else { "cold" };
                let value = db.get_raw_bytes(col, &key).unwrap();
                // When fetching reference counted column ColdDB adds reference
                // count to it.
                let value = pretty_value(value.as_deref(), col.is_rc());
                result.push(format!("    [{name}] get_raw_bytes        → {value}"));

                if col.is_rc() {
                    let value = db.get_with_rc_stripped(col, &key).unwrap();
                    let value = pretty_value(value.as_deref(), false);
                    result.push(format!("    [{name}] get_with_rc_stripped → {value}"));
                }
            }
        };

        fetch(DBCol::State, &[SHARD, HASH].concat(), false);
        fetch(DBCol::State, &[HASH].concat(), true);
        fetch(DBCol::Block, HASH, false);

        // Check expected value.  Use cargo-insta to update the expected value:
        //     cargo install cargo-insta
        //     cargo insta test --accept -p near-store  -- db::colddb
        insta::assert_display_snapshot!(result.join("\n"), @r###"
        State `ShardUId || 11111111111111111111111111111111`
            [cold] get_raw_bytes        → FooBar; rc: 1
            [cold] get_with_rc_stripped → FooBar
            [raw ] get_raw_bytes        → FooBar; rc: 1
            [raw ] get_with_rc_stripped → FooBar
        State 11111111111111111111111111111111
            [raw ] get_raw_bytes        → ∅
            [raw ] get_with_rc_stripped → ∅
        Block 11111111111111111111111111111111
            [cold] get_raw_bytes        → FooBar
            [raw ] get_raw_bytes        → FooBar
        "###);
    }

    /// Tests that iterator works correctly and adjusts keys when necessary.
    #[test]
    fn test_iterator() {
        let db = create_test_cold_db();

        // Test on two columns, one with rc and one without.
        // State -> rc
        // Block -> no rc

        // Populate data
        let mut ops = vec![];
        ops.push(set_with_rc(DBCol::State, &[SHARD, HASH].concat()));
        ops.push(set(DBCol::Block, HASH));
        db.write(DBTransaction { ops }).unwrap();

        let mut result = Vec::<String>::new();
        for col in [DBCol::State, DBCol::Block] {
            let dbs: [(bool, &dyn Database); 2] = [(false, &db), (true, &*db.cold)];
            result.push(col.to_string());
            for (is_raw, db) in dbs {
                let name = if is_raw { "raw " } else { "cold" };
                for item in db.iter(col) {
                    let (key, value) = item.unwrap();
                    let value = pretty_value(Some(value.as_ref()), false);
                    let key = pretty_key(&key);
                    result.push(format!("[{name}] ({key}, {value})"));
                }
            }
        }

        // Check expected value.  Use cargo-insta to update the expected value:
        //     cargo install cargo-insta
        //     cargo insta test --accept -p near-store  -- db::colddb
        insta::assert_display_snapshot!(result.join("\n"), @r###"
        State
        [cold] (`ShardUId || 11111111111111111111111111111111`, FooBar)
        [raw ] (`ShardUId || 11111111111111111111111111111111`, FooBar)
        Block
        [cold] (11111111111111111111111111111111, FooBar)
        [raw ] (11111111111111111111111111111111, FooBar)
        "###);
    }

    #[test]
    fn test_refcount() {
        let db = create_test_cold_db();
        let col = DBCol::Transactions;
        let key = HASH;

        let op =
            DBOp::UpdateRefcount { col, key: key.to_vec(), value: [VALUE, HEIGHT_LE].concat() };
        db.write(DBTransaction { ops: vec![op] }).unwrap();

        // Refcount is set to 1 in the underlying database.
        let got = db.cold.get_raw_bytes(col, key).unwrap();
        assert_eq!(Some([VALUE, ONE].concat().as_slice()), got.as_deref());

        // When fetching raw bytes, refcount of 1 is returned.
        let got = db.get_raw_bytes(col, key).unwrap();
        assert_eq!(Some([VALUE, ONE].concat().as_slice()), got.as_deref());
    }
}

use crate::db::{DBIterator, DBOp, DBTransaction, Database};
use crate::DBCol;

/// A database which provides access to the cold storage.
///
/// Some of the data we’re storing in cold storage is saved in slightly
/// different format than it is in the main database.  Specifically:
///
/// - Reference counts are not saved.  Since data is never removed from cold
///   storage, reference counts are not preserved and when fetching data are
///   always assumed to be one.
///
/// - Keys of DBCol::State column are not prefixed by ShardUId.  It is not
///   necessary to disambiguate the value and is used on hot storage to but
///   different shards in different ranges of keys.
///
/// - Keys of columns which include block height are encoded using big-endian
///   rather than little-endian as in hot storage.  Big-endian has the advantage
///   of being sorted in the same order the numbers themselves which in turn
///   means that when we add new heights they are always appended at the end of
///   the column.
///
/// This struct handles all those transformations transparently to the user so
/// that they don’t need to worry about accessing hot and cold storage
/// differently.
///
/// Note however that this also means that some operations, specifically
/// iterations, are limited and not implemented for all columns.  When trying to
/// implement over unsupported column the code will panic.  Refer to particular
/// iteration method in Database trait implementation for more details.
///
/// Lastly, since no data is ever deleted from cold storage, trying to decrease
/// reference of a value count or delete data is ignored and if debug assertions
/// are enabled will cause a panic.
struct ColdDatabase<D: Database = crate::db::RocksDB>(D);

// impl<D: Database + 'static> ColdDatabase<D> {
//     pub(crate) fn new(db: D) -> std::sync::Arc<dyn Database> {
//         std::sync::Arc::new(Self(db))
//     }
// }

impl<D: Database> super::Database for ColdDatabase<D> {
    fn get_raw_bytes(&self, col: DBCol, key: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        let mut buffer = [0; 32];
        let key = get_cold_key(col, key, &mut buffer).unwrap_or(key);
        let mut result = self.0.get_raw_bytes(col, key);
        if let Ok(Some(ref mut value)) = result {
            // Since we’ve stripped the reference count from the data stored in
            // the database, we need to reintroduce it.
            //
            // TODO(mina86): In most cases higher level will strip the refcount.
            // It’s rather silly that we’re adding it here just to get rid of it
            // later.  Figure out a way to avoid this.
            if col.is_rc() {
                value.extend_from_slice(&1i64.to_le_bytes());
            }
        }
        result
    }

    /// Iterates over all values in a column.
    ///
    /// This is implemented only for a few columns.  Specifically for Block,
    /// BlockHeader, ChunkHashesByHeight, EpochInfo and StateDlInfos.  It’ll
    /// panic if used for any other column.
    ///
    /// Furthermore, because key of ChunkHashesByHeight is modified in cold
    /// storage, the order of iteration of that column is different than if it
    /// would be in hot storage.
    fn iter<'a>(&'a self, column: DBCol) -> DBIterator<'a> {
        // Those are the only columns we’re ever iterating over.
        assert!(
            matches!(
                column,
                DBCol::StateDlInfos
                    | DBCol::BlockHeader
                    | DBCol::Block
                    | DBCol::ChunkHashesByHeight
                    | DBCol::EpochInfo
            ),
            "iter on cold storage is not supported for {column}"
        );
        let it = self.0.iter_raw_bytes(column);
        if column == DBCol::ChunkHashesByHeight {
            // For the column we need to swap bytes in the key.
            Box::new(it.map(|result| {
                let (mut key, value) = result?;
                let num = u64::from_le_bytes(key.as_ref().try_into().unwrap());
                key.as_mut().copy_from_slice(&num.to_be_bytes());
                Ok((key, value))
            }))
        } else {
            it
        }
    }

    /// Iterates over values in a given column whose key has given prefix.
    ///
    /// This is only implemented for StateChanges column and will panic if used
    /// for any other column.
    fn iter_prefix<'a>(&'a self, col: DBCol, key_prefix: &'a [u8]) -> super::DBIterator<'a> {
        // We only ever call iter_prefix on DBCol::StateChanges so we don’t need
        // to worry about implementing it for any other column.
        assert_eq!(
            DBCol::StateChanges,
            col,
            "iter_prefix on cold storage is supported for StateChanges only; \
             tried to iterate over {col}"
        );
        // StateChanges is neither reference counted, nor do we do any
        // adjustments to that column’s keys so we can pass the iter_prefix
        // request directly to the underlying database.
        self.0.iter_prefix(col, key_prefix)
    }

    /// Unimplemented; always panics.
    fn iter_raw_bytes<'a>(&'a self, _column: DBCol) -> super::DBIterator<'a> {
        // We’re actually never call iter_raw_bytes on cold store.
        unreachable!();
    }

    fn write(&self, mut transaction: DBTransaction) -> std::io::Result<()> {
        let mut idx = 0;
        while idx < transaction.ops.len() {
            if adjust_op(&mut transaction.ops[idx]) {
                idx += 1;
            } else {
                transaction.ops.swap_remove(idx);
            }
        }
        self.0.write(transaction)
    }

    fn flush(&self) -> std::io::Result<()> {
        self.0.flush()
    }

    fn get_store_statistics(&self) -> Option<crate::StoreStatistics> {
        self.0.get_store_statistics()
    }
}

/// Returns key as used in cold database for given column in hot database.
///
/// Some columns use different keys in cold storage compared to what they use in
/// hot storage.  Most column use the same key and for those this function
/// returns None.  However, there are two adjustments that the method performs:
///
/// 1. Due to historical reasons, when integers are keys they are stored as
///    little-endian.  In cold storage we’re swapping the bytes for those
///    identifiers to use big-endian instead.  Big-endian is a much better
///    choice since it sorts in the same order as the numbers themselves.
///
/// 2. In hot storage, entries in the DBCol::State are prefixed by 8-byte
///    ShardUId.  Keys in the column are hashes of the stored value so the
///    ShardUId isn’t necessary to identify or disambiguate different values.
///    The prefix can be removed which will also help with deduplicating values.
///
/// When doing the transformations of the key, the new value is stored in the
/// provided `buffer` and the function returns a slice pointing at it.
fn get_cold_key<'a>(col: DBCol, key: &[u8], buffer: &'a mut [u8; 32]) -> Option<&'a [u8]> {
    match col {
        DBCol::BlockHeight
        | DBCol::BlockPerHeight
        | DBCol::ChunkHashesByHeight
        | DBCol::ProcessedBlockHeights
        | DBCol::HeaderHashesByHeight => {
            // Key is `little_endian(height)`
            let num = u64::from_le_bytes(key.try_into().unwrap());
            buffer[..8].copy_from_slice(&num.to_be_bytes());
            Some(&buffer[..8])
        }
        DBCol::ChunkPerHeightShard => {
            // Key is `little_endian(height) || ShardUId`.  We’re leaving
            // ShardUId alone.
            let num = u64::from_le_bytes(key[..8].try_into().unwrap());
            buffer[..8].copy_from_slice(&num.to_be_bytes());
            buffer[8..16].copy_from_slice(&key[8..16]);
            Some(&buffer[..16])
        }
        DBCol::State => {
            // Key is `ShardUId || CryptoHash(node_or_value)`.  We’re stripping
            // the ShardUId.
            buffer[..32].copy_from_slice(&key[8..]);
            Some(&buffer[..32])
        }
        _ => None,
    }
}

/// Adjusts cold storage key as described in [`get_cold_key`].
fn adjust_key(col: DBCol, key: &mut Vec<u8>) {
    let mut buffer = [0; 32];
    if let Some(new_key) = get_cold_key(col, key.as_slice(), &mut buffer) {
        key.truncate(new_key.len());
        key.copy_from_slice(new_key);
    }
}

/// Adjust database operation to be performed on cold storage.
///
/// Returns whether the operation should be kept or dropped.  Generally, dropped
/// columns indicate an unexpected operation which should have never been issued
/// for cold storage.
fn adjust_op(op: &mut DBOp) -> bool {
    match op {
        DBOp::Set { col, key, .. } | DBOp::Insert { col, key, .. } => {
            adjust_key(*col, key);
            true
        }
        DBOp::UpdateRefcount { col, key, value } => {
            assert!(col.is_rc());
            let rc = crate::db::refcount::strip_refcount(value);
            if rc <= 0 {
                near_o11y::log_assert!(
                    false,
                    "Unexpected refcount {rc} in {col} in cold store: {key:?}"
                );
                false
            } else {
                let dummy = DBOp::Set { col: DBCol::DbVersion, key: Vec::new(), value: Vec::new() };
                let tmp = core::mem::replace(op, dummy);
                if let DBOp::UpdateRefcount { col, key, value } = tmp {
                    *op = DBOp::Set { col, key, value };
                    true
                } else {
                    unreachable!()
                }
            }
        }
        DBOp::Delete { col, key } => {
            near_o11y::log_assert!(false, "Unexpected delete from {col} in cold store: {key:?}");
            false
        }
        DBOp::DeleteAll { col } => {
            near_o11y::log_assert!(false, "Unexpected delete of {col} in cold store");
            false
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const HEIGHT_LE: &[u8] = &42u64.to_le_bytes();
    const HEIGHT_BE: &[u8] = &42u64.to_be_bytes();
    const SHARD: &[u8] = "ShardUId".as_bytes();
    const HASH: &[u8] = [0u8; 32].as_slice();
    const VALUE: &[u8] = "FooBar".as_bytes();

    /// Constructs test in-memory database.
    fn create_test_db() -> ColdDatabase<crate::db::TestDB> {
        ColdDatabase(crate::db::testdb::TestDB::new())
    }

    fn set(col: DBCol, key: &[u8]) -> DBOp {
        DBOp::Set { col: col, key: key.to_vec(), value: VALUE.to_vec() }
    }

    /// Prettifies raw key for display.
    fn pretty_key(key: &[u8]) -> String {
        match key.len() {
            8 if &key[0..7] == &[0; 7] => format!("be({})", key[7]),
            8 if &key[1..8] == &[0; 7] => format!("le({})", key[0]),
            16 if &key[0..7] == &[0; 7] => {
                format!("`be({}) || {}`", key[7], std::str::from_utf8(&key[8..]).unwrap())
            }
            16 if &key[1..8] == &[0; 7] => {
                format!("`le({}) || {}`", key[0], std::str::from_utf8(&key[8..]).unwrap())
            }
            32 => crate::CryptoHash::from(key.try_into().unwrap()).to_string(),
            40 => format!(
                "`{} || {}`",
                std::str::from_utf8(&key[..8]).unwrap(),
                crate::CryptoHash::from(key[8..].try_into().unwrap())
            ),
            _ => format!("{key:x?}"),
        }
    }

    /// Prettifies raw value for display.
    fn pretty_value(value: Option<Vec<u8>>, refcount: bool) -> String {
        match value {
            None => "∅".to_string(),
            Some(mut value) if refcount => {
                let rc = crate::db::refcount::strip_refcount(&mut value);
                let value = String::from_utf8(value).unwrap();
                format!("{value}; rc: {rc}")
            }
            Some(value) => String::from_utf8(value).unwrap(),
        }
    }

    /// Tests that keys are correctly adjusted when saved in cold store.
    #[test]
    fn test_adjust_key() {
        let db = create_test_db();

        // Populate data
        let height_columns = [
            DBCol::BlockHeight,
            DBCol::BlockPerHeight,
            DBCol::ChunkHashesByHeight,
            DBCol::ProcessedBlockHeights,
            DBCol::HeaderHashesByHeight,
        ];
        let mut ops: Vec<_> = height_columns.iter().map(|col| set(*col, HEIGHT_LE)).collect();
        ops.push(set(DBCol::ChunkPerHeightShard, &[HEIGHT_LE, SHARD].concat()));
        ops.push(set(DBCol::State, &[SHARD, HASH].concat()));
        ops.push(set(DBCol::Block, HASH));
        db.write(DBTransaction { ops }).unwrap();

        // Fetch data
        let mut result = Vec::<String>::new();
        let mut fetch = |col, key: &[u8], raw_only: bool| {
            let dbs: [(bool, &dyn Database); 2] = [(false, &db), (true, &db.0)];
            for (is_raw, db) in dbs {
                if raw_only && !is_raw {
                    continue;
                }
                let value = db.get_raw_bytes(col, &key).unwrap();
                // When fetching reference counted column ColdDatabase adds
                // reference count to it.
                let value = pretty_value(value, col.is_rc() && !is_raw);
                let key = pretty_key(key);
                let name = if is_raw { "raw " } else { "cold" };
                result.push(format!("[{name}] {col}, {key} → {value}"));
            }
        };

        for col in height_columns {
            fetch(col, HEIGHT_LE, false);
            fetch(col, HEIGHT_BE, false);
        }
        fetch(DBCol::ChunkPerHeightShard, &[HEIGHT_LE, SHARD].concat(), false);
        fetch(DBCol::ChunkPerHeightShard, &[HEIGHT_BE, SHARD].concat(), false);
        fetch(DBCol::State, &[SHARD, HASH].concat(), false);
        fetch(DBCol::State, &[HASH].concat(), true);
        fetch(DBCol::Block, HASH, false);

        // Check expected value.  Use cargo-insta to update the expected value:
        //     cargo install cargo-insta
        //     cargo insta test --accept -p near-store  -- db::colddb
        insta::assert_display_snapshot!(result.join("\n"), @r###"
        [cold] BlockHeight, le(42) → FooBar
        [raw ] BlockHeight, le(42) → ∅
        [cold] BlockHeight, be(42) → ∅
        [raw ] BlockHeight, be(42) → FooBar
        [cold] BlockPerHeight, le(42) → FooBar
        [raw ] BlockPerHeight, le(42) → ∅
        [cold] BlockPerHeight, be(42) → ∅
        [raw ] BlockPerHeight, be(42) → FooBar
        [cold] ChunkHashesByHeight, le(42) → FooBar
        [raw ] ChunkHashesByHeight, le(42) → ∅
        [cold] ChunkHashesByHeight, be(42) → ∅
        [raw ] ChunkHashesByHeight, be(42) → FooBar
        [cold] ProcessedBlockHeights, le(42) → FooBar
        [raw ] ProcessedBlockHeights, le(42) → ∅
        [cold] ProcessedBlockHeights, be(42) → ∅
        [raw ] ProcessedBlockHeights, be(42) → FooBar
        [cold] HeaderHashesByHeight, le(42) → FooBar
        [raw ] HeaderHashesByHeight, le(42) → ∅
        [cold] HeaderHashesByHeight, be(42) → ∅
        [raw ] HeaderHashesByHeight, be(42) → FooBar
        [cold] ChunkPerHeightShard, `le(42) || ShardUId` → FooBar
        [raw ] ChunkPerHeightShard, `le(42) || ShardUId` → ∅
        [cold] ChunkPerHeightShard, `be(42) || ShardUId` → ∅
        [raw ] ChunkPerHeightShard, `be(42) || ShardUId` → FooBar
        [cold] State, `ShardUId || 11111111111111111111111111111111` → FooBar; rc: 1
        [raw ] State, `ShardUId || 11111111111111111111111111111111` → ∅
        [raw ] State, 11111111111111111111111111111111 → FooBar
        [cold] Block, 11111111111111111111111111111111 → FooBar
        [raw ] Block, 11111111111111111111111111111111 → FooBar
        "###);
    }

    /// Tests that iterator works correctly and adjusts keys when necessary.
    #[test]
    fn test_iterator() {
        let db = create_test_db();

        let mut ops: Vec<_> =
            [DBCol::StateDlInfos, DBCol::BlockHeader, DBCol::Block, DBCol::EpochInfo]
                .iter()
                .map(|col| set(*col, HASH))
                .collect();
        ops.push(set(DBCol::ChunkHashesByHeight, HEIGHT_LE));
        db.write(DBTransaction { ops }).unwrap();

        let mut result = Vec::<String>::new();
        for col in [
            DBCol::StateDlInfos,
            DBCol::BlockHeader,
            DBCol::Block,
            DBCol::EpochInfo,
            DBCol::ChunkHashesByHeight,
        ] {
            let dbs: [(bool, &dyn Database); 2] = [(false, &db), (true, &db.0)];
            for (is_raw, db) in dbs {
                let name = if is_raw { "raw " } else { "cold" };
                result.push(format!("[{name}] {col}"));
                for item in db.iter(col) {
                    let (key, value) = item.unwrap();
                    let value = pretty_value(Some(value.into_vec()), false);
                    let key = pretty_key(&key);
                    result.push(format!("{key} → {value}"));
                }
            }
        }

        // Check expected value.  Use cargo-insta to update the expected value:
        //     cargo install cargo-insta
        //     cargo insta test --accept -p near-store  -- db::colddb
        insta::assert_display_snapshot!(result.join("\n"), @r###"
        [cold] StateDlInfos
        11111111111111111111111111111111 → FooBar
        [raw ] StateDlInfos
        11111111111111111111111111111111 → FooBar
        [cold] BlockHeader
        11111111111111111111111111111111 → FooBar
        [raw ] BlockHeader
        11111111111111111111111111111111 → FooBar
        [cold] Block
        11111111111111111111111111111111 → FooBar
        [raw ] Block
        11111111111111111111111111111111 → FooBar
        [cold] EpochInfo
        11111111111111111111111111111111 → FooBar
        [raw ] EpochInfo
        11111111111111111111111111111111 → FooBar
        [cold] ChunkHashesByHeight
        le(42) → FooBar
        [raw ] ChunkHashesByHeight
        be(42) → FooBar
        "###);
    }

    /// Tests that stripping and adding refcount works correctly.
    #[test]
    fn test_refcount() {
        let db = create_test_db();
        let col = DBCol::Transactions;
        let key = HASH;

        let op =
            DBOp::UpdateRefcount { col, key: key.to_vec(), value: [VALUE, HEIGHT_LE].concat() };
        db.write(DBTransaction { ops: vec![op] }).unwrap();

        // Refcount is not kept in the underlying database.
        let got = db.0.get_raw_bytes(col, key).unwrap();
        assert_eq!(Some(VALUE), got.as_deref());

        // When fetching raw bytes, refcount of 1 is returned.
        let got = db.get_raw_bytes(col, key).unwrap();
        assert_eq!(Some([VALUE, &1i64.to_le_bytes()].concat()), got);
    }
}

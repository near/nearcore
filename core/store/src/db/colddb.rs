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
/// - Keys of DBCol::State column are not prefixed by ShardUId.  The prefix
///   isn’t needed to disambiguate the value.  It is used on hot storage to
///   provide some isolation by putting data of different shards in different
///   ranges of keys.  This is not beneficial is cold storage so we’re not using
///   the prefix to reduce space usage and avoid heavy data migration in case of
///   resharding.
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

impl<D: Database> ColdDatabase<D> {
    /// Returns raw bytes from the underlying storage.
    ///
    /// Adjusts the key if necessary (see [`get_cold_key`]) and retrieves data
    /// corresponding to it from the database and returns it as it resides in
    /// the database.  This is common code used by [`Self::get_raw_bytes`] and
    /// [`Self::get_with_rc_stripped`] methods.
    fn get_impl(&self, col: DBCol, key: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        let mut buffer = [0; 32];
        let key = get_cold_key(col, key, &mut buffer).unwrap_or(key);
        self.0.get_raw_bytes(col, key)
    }
}

impl<D: Database> super::Database for ColdDatabase<D> {
    fn get_raw_bytes(&self, col: DBCol, key: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        let mut result = self.get_impl(col, key);
        if let Ok(Some(ref mut value)) = result {
            // Since we’ve stripped the reference count from the data stored in
            // the database, we need to reintroduce it.  Note that in practice
            // this should be never called in production since reading of
            // reference counted columns is done by get_with_rc_stripped.
            if col.is_rc() {
                value.extend_from_slice(&1i64.to_le_bytes());
            }
        }
        result
    }

    fn get_with_rc_stripped(&self, col: DBCol, key: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        assert!(col.is_rc());
        self.get_impl(col, key)
    }

    /// Iterates over all values in a column.
    ///
    /// This is implemented only for a few columns.  Specifically for Block,
    /// BlockHeader, ChunkHashesByHeight and EpochInfo.  It’ll
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
                DBCol::BlockHeader | DBCol::Block | DBCol::ChunkHashesByHeight | DBCol::EpochInfo
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
    fn iter_prefix<'a>(&'a self, col: DBCol, key_prefix: &'a [u8]) -> DBIterator<'a> {
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
    fn iter_raw_bytes<'a>(&'a self, _column: DBCol) -> DBIterator<'a> {
        // We’re actually never call iter_raw_bytes on cold store.
        unreachable!();
    }

    /// Atomically applies operations in given transaction.
    ///
    /// If debug assertions are enabled, panics if there are any delete
    /// operations or operations decreasing reference count of a value.  If
    /// debug assertions are not enabled, such operations are filtered out.
    ///
    /// As per documentation of [the type](Self), some keys and values are
    /// adjusted before they are written to the database.  In particular,
    /// ShardUId is removed from keys of DBCol::State column.  This means that
    /// write of hash α to shard X and to shard Y will result in the same write.
    /// If convenient at transaction generation time, it’s beneficial to
    /// deduplicate such writes.
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
            let value = core::mem::take(value);
            if let Some(value) = crate::db::refcount::strip_refcount(value) {
                *op = DBOp::Set { col: *col, key: core::mem::take(key), value };
                true
            } else {
                near_o11y::log_assert!(
                    false,
                    "Unexpected non-positive reference count in {col} in cold store: {key:?}"
                );
                false
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
            result.push(format!("{col} {}", pretty_key(key)));
            let dbs: [(bool, &dyn Database); 2] = [(false, &db), (true, &db.0)];
            for (is_raw, db) in dbs {
                if raw_only && !is_raw {
                    continue;
                }

                let name = if is_raw { "raw " } else { "cold" };
                let value = db.get_raw_bytes(col, &key).unwrap();
                // When fetching reference counted column ColdDatabase adds
                // reference count to it.
                let value = pretty_value(value.as_deref(), col.is_rc() && !is_raw);
                result.push(format!("    [{name}] get_raw_bytes → {value}"));

                if col.is_rc() && !is_raw {
                    let value = db.get_with_rc_stripped(col, &key).unwrap();
                    let value = pretty_value(value.as_deref(), false);
                    result.push(format!("    [{name}] get_sans_rc   → {value}"));
                }
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
        BlockHeight le(42)
            [cold] get_raw_bytes → FooBar
            [raw ] get_raw_bytes → ∅
        BlockHeight be(42)
            [cold] get_raw_bytes → ∅
            [raw ] get_raw_bytes → FooBar
        BlockPerHeight le(42)
            [cold] get_raw_bytes → FooBar
            [raw ] get_raw_bytes → ∅
        BlockPerHeight be(42)
            [cold] get_raw_bytes → ∅
            [raw ] get_raw_bytes → FooBar
        ChunkHashesByHeight le(42)
            [cold] get_raw_bytes → FooBar
            [raw ] get_raw_bytes → ∅
        ChunkHashesByHeight be(42)
            [cold] get_raw_bytes → ∅
            [raw ] get_raw_bytes → FooBar
        ProcessedBlockHeights le(42)
            [cold] get_raw_bytes → FooBar
            [raw ] get_raw_bytes → ∅
        ProcessedBlockHeights be(42)
            [cold] get_raw_bytes → ∅
            [raw ] get_raw_bytes → FooBar
        HeaderHashesByHeight le(42)
            [cold] get_raw_bytes → FooBar
            [raw ] get_raw_bytes → ∅
        HeaderHashesByHeight be(42)
            [cold] get_raw_bytes → ∅
            [raw ] get_raw_bytes → FooBar
        ChunkPerHeightShard `le(42) || ShardUId`
            [cold] get_raw_bytes → FooBar
            [raw ] get_raw_bytes → ∅
        ChunkPerHeightShard `be(42) || ShardUId`
            [cold] get_raw_bytes → ∅
            [raw ] get_raw_bytes → FooBar
        State `ShardUId || 11111111111111111111111111111111`
            [cold] get_raw_bytes → FooBar; rc: 1
            [cold] get_sans_rc   → FooBar
            [raw ] get_raw_bytes → ∅
        State 11111111111111111111111111111111
            [raw ] get_raw_bytes → FooBar
        Block 11111111111111111111111111111111
            [cold] get_raw_bytes → FooBar
            [raw ] get_raw_bytes → FooBar
        "###);
    }

    /// Tests that iterator works correctly and adjusts keys when necessary.
    #[test]
    fn test_iterator() {
        let db = create_test_db();

        let mut ops: Vec<_> = [DBCol::BlockHeader, DBCol::Block, DBCol::EpochInfo]
            .iter()
            .map(|col| set(*col, HASH))
            .collect();
        ops.push(set(DBCol::ChunkHashesByHeight, HEIGHT_LE));
        db.write(DBTransaction { ops }).unwrap();

        let mut result = Vec::<String>::new();
        for col in [DBCol::BlockHeader, DBCol::Block, DBCol::EpochInfo, DBCol::ChunkHashesByHeight]
        {
            let dbs: [(bool, &dyn Database); 2] = [(false, &db), (true, &db.0)];
            result.push(format!("{col}"));
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
        BlockHeader
        [cold] (11111111111111111111111111111111, FooBar)
        [raw ] (11111111111111111111111111111111, FooBar)
        Block
        [cold] (11111111111111111111111111111111, FooBar)
        [raw ] (11111111111111111111111111111111, FooBar)
        EpochInfo
        [cold] (11111111111111111111111111111111, FooBar)
        [raw ] (11111111111111111111111111111111, FooBar)
        ChunkHashesByHeight
        [cold] (le(42), FooBar)
        [raw ] (be(42), FooBar)
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

use std::io;
use std::sync::Arc;

use crate::DBCol;

pub mod refcount;
pub(crate) mod rocksdb;
mod testdb;

pub use self::rocksdb::RocksDB;
pub use self::testdb::TestDB;

pub const VERSION_KEY: &[u8; 7] = b"VERSION";

pub const HEAD_KEY: &[u8; 4] = b"HEAD";
pub const TAIL_KEY: &[u8; 4] = b"TAIL";
pub const CHUNK_TAIL_KEY: &[u8; 10] = b"CHUNK_TAIL";
pub const FORK_TAIL_KEY: &[u8; 9] = b"FORK_TAIL";
pub const HEADER_HEAD_KEY: &[u8; 11] = b"HEADER_HEAD";
pub const FINAL_HEAD_KEY: &[u8; 10] = b"FINAL_HEAD";
pub const LATEST_KNOWN_KEY: &[u8; 12] = b"LATEST_KNOWN";
pub const LARGEST_TARGET_HEIGHT_KEY: &[u8; 21] = b"LARGEST_TARGET_HEIGHT";
pub const GENESIS_JSON_HASH_KEY: &[u8; 17] = b"GENESIS_JSON_HASH";
pub const GENESIS_STATE_ROOTS_KEY: &[u8; 19] = b"GENESIS_STATE_ROOTS";
/// Boolean stored in DBCol::BlockMisc indicating whether the database is for an
/// archival node.  The default value (if missing) is false.
pub const IS_ARCHIVE_KEY: &[u8; 10] = b"IS_ARCHIVE";

#[derive(Default)]
pub struct DBTransaction {
    pub(crate) ops: Vec<DBOp>,
}

pub(crate) enum DBOp {
    /// Sets `key` to `value`, without doing any checks.
    Set { col: DBCol, key: Vec<u8>, value: Vec<u8> },
    /// Sets `key` to `value`, and additionally debug-checks that the value is
    /// not overwritten.
    Insert { col: DBCol, key: Vec<u8>, value: Vec<u8> },
    /// Modifies a reference-counted column. `value` includes both the value per
    /// se and a refcount at the end.
    UpdateRefcount { col: DBCol, key: Vec<u8>, value: Vec<u8> },
    /// Deletes sepecific `key`.
    Delete { col: DBCol, key: Vec<u8> },
    /// Deletes all data from a column.
    DeleteAll { col: DBCol },
}

impl DBTransaction {
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    pub fn set(&mut self, col: DBCol, key: Vec<u8>, value: Vec<u8>) {
        self.ops.push(DBOp::Set { col, key, value });
    }

    pub fn insert(&mut self, col: DBCol, key: Vec<u8>, value: Vec<u8>) {
        assert!(col.is_insert_only(), "can't insert: {col:?}");
        self.ops.push(DBOp::Insert { col, key, value });
    }

    pub fn update_refcount(&mut self, col: DBCol, key: Vec<u8>, value: Vec<u8>) {
        assert!(col.is_rc(), "can't update refcount: {col:?}");
        self.ops.push(DBOp::UpdateRefcount { col, key, value });
    }

    pub fn delete(&mut self, col: DBCol, key: Vec<u8>) {
        self.ops.push(DBOp::Delete { col, key });
    }

    pub fn delete_all(&mut self, col: DBCol) {
        self.ops.push(DBOp::DeleteAll { col });
    }

    pub fn merge(&mut self, other: DBTransaction) {
        self.ops.extend(other.ops)
    }
}

pub type DBIterator<'a> = Box<dyn Iterator<Item = io::Result<(Box<[u8]>, Box<[u8]>)>> + 'a>;

/// Data returned from the database.
///
/// Abstraction layer for data returned by [`Database::get_raw_bytes`] and
/// [`Database::get_with_rc_stripped`] methods.  Operating on the value as
/// a slice is free while converting it to a vector on an arc may requires an
/// allocation and memory copy.
pub struct DBBytes<'a>(DBBytesInner<'a>);

enum DBBytesInner<'a> {
    /// Data held as an owned vector.
    Vec(Vec<u8>),

    /// Data held as RocksDB-specific pinnable slice type.
    Rocks(::rocksdb::DBPinnableSlice<'a>),
}

impl<'a> DBBytes<'a> {
    pub fn as_slice(&self) -> &[u8] {
        match self.0 {
            DBBytesInner::Vec(ref bytes) => bytes.as_slice(),
            DBBytesInner::Rocks(ref bytes) => &*bytes,
        }
    }
}

impl<'a> std::ops::Deref for DBBytes<'a> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<'a> From<Vec<u8>> for DBBytes<'a> {
    fn from(bytes: Vec<u8>) -> Self {
        Self(DBBytesInner::Vec(bytes))
    }
}

impl<'a> From<&[u8]> for DBBytes<'a> {
    fn from(bytes: &[u8]) -> Self {
        Self(DBBytesInner::Vec(bytes.to_vec()))
    }
}

impl<'a> From<DBBytes<'a>> for Arc<[u8]> {
    fn from(bytes: DBBytes<'a>) -> Self {
        bytes.as_slice().into()
    }
}

impl<'a> From<DBBytes<'a>> for Vec<u8> {
    fn from(bytes: DBBytes<'a>) -> Self {
        match bytes.0 {
            DBBytesInner::Vec(bytes) => bytes,
            DBBytesInner::Rocks(bytes) => bytes.to_vec(),
        }
    }
}

impl<'a> std::fmt::Debug for DBBytes<'a> {
    fn fmt(&self, fmtr: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.as_slice(), fmtr)
    }
}

impl<'a> std::cmp::PartialEq<DBBytes<'a>> for DBBytes<'a> {
    fn eq(&self, other: &DBBytes<'a>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<'a> std::cmp::PartialEq<[u8]> for DBBytes<'a> {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_slice() == other
    }
}

pub trait Database: Sync + Send {
    /// Returns raw bytes for given `key` ignoring any reference count decoding
    /// if any.
    ///
    /// Note that when reading reference-counted column, the reference count
    /// will not be decoded or stripped from the value.  Similarly, cells with
    /// non-positive reference count will be returned as existing.
    ///
    /// You most likely will want to use [`refcount::get_with_rc_logic`] to
    /// properly handle reference-counted columns.
    fn get_raw_bytes(&self, col: DBCol, key: &[u8]) -> io::Result<Option<DBBytes<'_>>>;

    /// Returns value for given `key` forcing a reference count decoding.
    ///
    /// **Panics** if the column is not reference counted.
    fn get_with_rc_stripped(&self, col: DBCol, key: &[u8]) -> io::Result<Option<DBBytes<'_>>> {
        assert!(col.is_rc());
        Ok(self
            .get_raw_bytes(col, key)?
            .and_then(|bytes| refcount::decode_value_with_rc(&bytes).0.map(DBBytes::from)))
    }

    /// Iterate over all items in given column in lexicographical order sorted
    /// by the key.
    ///
    /// When reading reference-counted column, the reference count will be
    /// correctly stripped.  Furthermore, elements with non-positive reference
    /// count will be treated as non-existing (i.e. they’re going to be
    /// skipped).  For all other columns, the value is returned directly from
    /// the database.
    fn iter<'a>(&'a self, column: DBCol) -> DBIterator<'a>;

    /// Iterate over items in given column whose keys start with given prefix.
    ///
    /// This is morally equivalent to [`Self::iter`] with a filter discarding
    /// keys which do not start with given `key_prefix` (but faster).  The items
    /// are returned in lexicographical order sorted by the key.
    fn iter_prefix<'a>(&'a self, col: DBCol, key_prefix: &'a [u8]) -> DBIterator<'a>;

    /// Iterate over items in given column bypassing reference count decoding if
    /// any.
    ///
    /// This is like [`Self::iter`] but it returns raw bytes as stored in the
    /// database.  For reference-counted columns this means that the reference
    /// count will not be decoded or stripped from returned value and elements
    /// with non-positive reference count will be included in the iterator.
    ///
    /// If in doubt, use [`Self::iter`] instead.  Unless you’re doing something
    /// low-level with the database (e.g. doing a migration), you probably don’t
    /// want this method.
    fn iter_raw_bytes<'a>(&'a self, column: DBCol) -> DBIterator<'a>;

    /// Atomically apply all operations in given batch at once.
    fn write(&self, batch: DBTransaction) -> io::Result<()>;

    /// Flush all in-memory data to disk.
    ///
    /// This is a no-op for in-memory databases.
    fn flush(&self) -> io::Result<()>;

    /// Returns statistics about the database if available.
    fn get_store_statistics(&self) -> Option<StoreStatistics>;
}

fn assert_no_overwrite(col: DBCol, key: &[u8], value: &[u8], old_value: &[u8]) {
    assert!(
        value == old_value,
        "\
write once column overwritten
col: {col}
key: {key:?}
"
    )
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StatsValue {
    Count(i64),
    Sum(i64),
    Percentile(u32, f64),
    ColumnValue(DBCol, i64),
}

#[derive(Debug, PartialEq)]
pub struct StoreStatistics {
    pub data: Vec<(String, Vec<StatsValue>)>,
}

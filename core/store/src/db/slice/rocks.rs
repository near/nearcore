use super::{DBSlice, Inner};
use crate::db::refcount;

impl<'a> DBSlice<'a> {
    /// Constructs the object from RocksDB pinnable slice representation.
    ///
    /// This is internal API for the [`crate::db::rocksdb::RocksDB`]
    /// implementation of the database interface.
    pub(in crate::db) fn from_rocksdb_slice(db_slice: ::rocksdb::DBPinnableSlice<'a>) -> Self {
        DBSlice(Inner::Rocks(RocksSlice::new(db_slice)))
    }
}

/// A slice owned by the RocksDB with RAII mechanism for letting RocksDB know
/// when it can be freed.
pub(super) struct RocksSlice<'a> {
    /// Pointer at the bytes.
    ///
    /// The data is held by `db_slice` and as such this field must not
    /// outlive `db_slice`.  Nor can `db_slice` be modified.
    ///
    /// We keep a separate pointer because we want to be able to modify
    /// truncate the value without having to allocate new buffers.
    data: *const [u8],

    /// This is what owns the data.
    ///
    /// This is never modified and has a stable deref which is why we can
    /// have `data` field pointing at the buffer this object holds.
    db_slice: ::rocksdb::DBPinnableSlice<'a>,
}

impl<'a> RocksSlice<'a> {
    pub fn new(db_slice: ::rocksdb::DBPinnableSlice<'a>) -> Self {
        let data = &*db_slice as *const [u8];
        Self { data, db_slice }
    }

    pub(super) fn strip_refcount(self) -> Option<Self> {
        let data = refcount::decode_value_with_rc(self.as_slice()).0?;
        Some(Self { data: data as *const _, db_slice: self.db_slice })
    }

    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: data references a pinned buffer owned by db_slice which has
        // not been modified since we got the pointer.
        unsafe { &*self.data }
    }
}

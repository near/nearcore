use std::sync::Arc;

use super::refcount;

/// Data returned from the database.
///
/// Abstraction layer for data returned by [`Database::get_raw_bytes`] and
/// [`Database::get_with_rc_stripped`] methods.  Operating on the value as
/// a slice is free while converting it to a vector on an arc may requires an
/// allocation and memory copy.
///
/// This is essentially a reference into a buffer owned by the database with an
/// RAII guard which notifies the database when the buffer can be freed.
pub struct DBSlice<'a>(Inner<'a>);

enum Inner<'a> {
    /// Data held as a vector.
    Vec(Vec<u8>),

    /// Data held as RocksDB-specific pinnable slice.
    Rocks {
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
    },
}

impl<'a> DBSlice<'a> {
    /// Returns slice view of the data.
    pub fn as_slice(&self) -> &[u8] {
        match self.0 {
            Inner::Vec(ref bytes) => bytes.as_slice(),
            Inner::Rocks { data, .. } => {
                // SAFETY: data references at buffer owned by db_slice which has
                // not been modified since we got the pointer.
                unsafe { &*data }
            }
        }
    }

    /// Constructs the object from a vector.
    ///
    /// In the current implementation, this is a zero-copy operation.
    pub(super) fn from_vec(vec: Vec<u8>) -> Self {
        Self(Inner::Vec(vec))
    }

    /// Constructs the object from RocskDB pinnable slice representation.
    ///
    /// This is internal API for the [`crate::db::rocksdb::RocksDB`]
    /// implementation of the database interface.
    pub(super) fn from_rocksdb_slice(db_slice: ::rocksdb::DBPinnableSlice<'a>) -> Self {
        let data = &*db_slice as *const [u8];
        DBSlice(Inner::Rocks { data, db_slice })
    }

    /// Decodes and strips reference count from the data.
    ///
    /// Returns `None` if the reference count is non-positive (or the value was
    /// empty).  See [`refcount::decode_value_with_rc`] for more detail about
    /// decoding the refcount.
    pub(super) fn strip_refcount(self) -> Option<Self> {
        match self.0 {
            Inner::Vec(bytes) => {
                refcount::strip_refcount(bytes).map(|bytes| Self(Inner::Vec(bytes)))
            }
            Inner::Rocks { data, db_slice } => {
                // SAFETY: See as_slice method.
                let data = refcount::decode_value_with_rc(unsafe { &*data }).0?;
                Some(Self(Inner::Rocks { data: data as *const _, db_slice }))
            }
        }
    }
}

impl<'a> std::ops::Deref for DBSlice<'a> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<'a> From<DBSlice<'a>> for Arc<[u8]> {
    /// Converts `DBSlice` into a thread-safe reference counted slice.
    ///
    /// This may need to allocate and copy data.
    fn from(bytes: DBSlice<'a>) -> Self {
        bytes.as_slice().into()
    }
}

impl<'a> From<DBSlice<'a>> for Vec<u8> {
    /// Converts `DBSlice` into a vector.
    ///
    /// This may need to allocate and copy data.
    fn from(bytes: DBSlice<'a>) -> Self {
        match bytes.0 {
            Inner::Vec(bytes) => bytes,
            Inner::Rocks { data, .. } => {
                // SAFETY: See as_slice method.
                unsafe { &*data }.to_vec()
            }
        }
    }
}

impl<'a> std::fmt::Debug for DBSlice<'a> {
    fn fmt(&self, fmtr: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.as_slice(), fmtr)
    }
}

impl<'a> std::cmp::PartialEq<DBSlice<'a>> for DBSlice<'a> {
    fn eq(&self, other: &DBSlice<'a>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<'a> std::cmp::PartialEq<[u8]> for DBSlice<'a> {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_slice() == other
    }
}

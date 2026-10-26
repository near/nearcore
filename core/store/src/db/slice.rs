use super::refcount;
use rocks::RocksSlice;
use std::sync::Arc;

#[cfg(feature = "rocksdb")]
mod rocks;

/// Uninhabited stand-in so `DBSlice` keeps its shape without the `rocksdb`
/// feature; a RocksDB slice can never be constructed.
#[cfg(not(feature = "rocksdb"))]
mod rocks {
    use std::convert::Infallible;
    use std::marker::PhantomData;

    pub(super) struct RocksSlice<'a>(Infallible, PhantomData<&'a ()>);

    impl<'a> RocksSlice<'a> {
        pub(super) fn strip_refcount(self) -> Option<Self> {
            match self.0 {}
        }

        pub fn as_slice(&self) -> &[u8] {
            match self.0 {}
        }
    }
}

/// Data returned from the database.
///
/// Abstraction layer for data returned by [`super::Database::get_raw_bytes`]
/// and [`super::Database::get_with_rc_stripped`] methods.  Operating on the
/// value as a slice is free while converting it to a vector on an arc may
/// requires an allocation and memory copy.
///
/// This is essentially a reference into a buffer owned by the database with an
/// RAII guard which notifies the database when the buffer can be freed.
pub struct DBSlice<'a>(Inner<'a>);

enum Inner<'a> {
    /// Data held as a vector.
    Vec(Vec<u8>),

    /// Data held as RocksDB-specific pinnable slice.
    #[cfg_attr(not(feature = "rocksdb"), allow(dead_code))]
    Rocks(RocksSlice<'a>),
}

impl<'a> DBSlice<'a> {
    /// Returns slice view of the data.
    pub fn as_slice(&self) -> &[u8] {
        match self.0 {
            Inner::Vec(ref bytes) => bytes.as_slice(),
            Inner::Rocks(ref rocks) => rocks.as_slice(),
        }
    }

    /// Constructs the object from a vector.
    ///
    /// In the current implementation, this is a zero-copy operation.
    pub(crate) fn from_vec(vec: Vec<u8>) -> Self {
        Self(Inner::Vec(vec))
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
            Inner::Rocks(rocks) => rocks.strip_refcount().map(|rocks| Self(Inner::Rocks(rocks))),
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
    fn from(slice: DBSlice<'a>) -> Self {
        slice.as_slice().into()
    }
}

impl<'a> From<DBSlice<'a>> for Vec<u8> {
    /// Converts `DBSlice` into a vector.
    ///
    /// This may need to allocate and copy data.
    fn from(slice: DBSlice<'a>) -> Self {
        match slice.0 {
            Inner::Vec(bytes) => bytes,
            Inner::Rocks(rocks) => rocks.as_slice().to_vec(),
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

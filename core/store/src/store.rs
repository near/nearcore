use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::{fmt, io};

use borsh::{BorshDeserialize, BorshSerialize};
use near_fmt::{AbbrBytes, StorageKey};

use crate::DBCol;
use crate::adapter::{StoreAdapter, StoreUpdateAdapter};
use crate::db::metadata::{DbKind, DbMetadata, DbVersion, KIND_KEY, VERSION_KEY};
use crate::db::{DBIterator, DBOp, DBSlice, DBTransaction, Database, StoreStatistics, refcount};

const STATE_COLUMNS: [DBCol; 2] = [DBCol::State, DBCol::FlatState];
const STATE_FILE_END_MARK: u8 = 255;

/// Node’s single storage source.
///
/// The Store holds one of the possible databases:
/// - The hot database - access to the hot database only
/// - The cold database - access to the cold database only
/// - The split database - access to both hot and cold databases
#[derive(Clone)]
pub struct Store {
    pub(crate) storage: Arc<dyn Database>,
}

impl StoreAdapter for Store {
    fn store_ref(&self) -> &Store {
        self
    }
}

impl Store {
    pub fn new(storage: Arc<dyn Database>) -> Self {
        Self { storage }
    }

    /// Fetches value from given column.
    ///
    /// If the key does not exist in the column returns `None`.  Otherwise
    /// returns the data as [`DBSlice`] object.  The object dereferences into
    /// a slice, for cases when caller doesn’t need to own the value, and
    /// provides conversion into a vector or an Arc.
    pub fn get(&self, column: DBCol, key: &[u8]) -> io::Result<Option<DBSlice<'_>>> {
        let value = if column.is_rc() {
            self.storage.get_with_rc_stripped(column, &key)
        } else {
            self.storage.get_raw_bytes(column, &key)
        }?;
        tracing::trace!(
            target: "store",
            db_op = "get",
            col = %column,
            key = %StorageKey(key),
            size = value.as_deref().map(<[u8]>::len)
        );
        Ok(value)
    }

    pub fn get_ser<T: BorshDeserialize>(&self, column: DBCol, key: &[u8]) -> io::Result<Option<T>> {
        self.get(column, key)?.as_deref().map(T::try_from_slice).transpose()
    }

    pub fn exists(&self, column: DBCol, key: &[u8]) -> io::Result<bool> {
        self.get(column, key).map(|value| value.is_some())
    }

    pub fn store_update(&self) -> StoreUpdate {
        StoreUpdate { transaction: DBTransaction::new(), store: self.clone() }
    }

    pub fn iter<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        self.storage.iter(col)
    }

    pub fn iter_ser<'a, T: BorshDeserialize>(
        &'a self,
        col: DBCol,
    ) -> impl Iterator<Item = io::Result<(Box<[u8]>, T)>> + 'a {
        self.storage
            .iter(col)
            .map(|item| item.and_then(|(key, value)| Ok((key, T::try_from_slice(value.as_ref())?))))
    }

    /// Fetches raw key/value pairs from the database.
    ///
    /// Practically, this means that for rc columns rc is included in the value.
    /// This method is a deliberate escape hatch, and shouldn't be used outside
    /// of auxiliary code like migrations which wants to hack on the database
    /// directly.
    pub fn iter_raw_bytes<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        self.storage.iter_raw_bytes(col)
    }

    pub fn iter_prefix<'a>(&'a self, col: DBCol, key_prefix: &'a [u8]) -> DBIterator<'a> {
        assert!(col != DBCol::State, "can't iter prefix of State column");
        self.storage.iter_prefix(col, key_prefix)
    }

    /// Iterates over a range of keys. Upper bound key is not included.
    pub fn iter_range<'a>(
        &'a self,
        col: DBCol,
        lower_bound: Option<&[u8]>,
        upper_bound: Option<&[u8]>,
    ) -> DBIterator<'a> {
        // That would fail if called `ScanDbColumnCmd`` for the `State` column.
        assert!(col != DBCol::State, "can't range iter State column");
        self.storage.iter_range(col, lower_bound, upper_bound)
    }

    pub fn iter_prefix_ser<'a, T: BorshDeserialize>(
        &'a self,
        col: DBCol,
        key_prefix: &'a [u8],
    ) -> impl Iterator<Item = io::Result<(Box<[u8]>, T)>> + 'a {
        assert!(col != DBCol::State, "can't iter prefix ser of State column");
        self.storage
            .iter_prefix(col, key_prefix)
            .map(|item| item.and_then(|(key, value)| Ok((key, T::try_from_slice(value.as_ref())?))))
    }

    /// Saves state (`State` and `FlatState` columns) to given file.
    ///
    /// The format of the file is a list of `(column_index as u8, key_length as
    /// u32, key, value_length as u32, value)` records terminated by a single
    /// 255 byte.  `column_index` refers to state columns listed in
    /// `STATE_COLUMNS` array.
    pub fn save_state_to_file(&self, filename: &Path) -> io::Result<()> {
        let file = File::create(filename)?;
        let mut file = std::io::BufWriter::new(file);
        for (column_index, &column) in STATE_COLUMNS.iter().enumerate() {
            assert!(column_index < STATE_FILE_END_MARK.into());
            let column_index: u8 = column_index.try_into().unwrap();
            for item in self.storage.iter_raw_bytes(column) {
                let (key, value) = item?;
                (column_index, key, value).serialize(&mut file)?;
            }
        }
        STATE_FILE_END_MARK.serialize(&mut file)
    }

    /// Loads state (`State` and `FlatState` columns) from given file.
    ///
    /// See [`Self::save_state_to_file`] for description of the file format.
    #[tracing::instrument(
        level = "info",
        // FIXME: start moving things into tighter modules so that its easier to selectively trace
        // specific things.
        target = "store",
        "Store::load_state_from_file",
        skip_all,
        fields(filename = %filename.display())
    )]
    pub fn load_state_from_file(&self, filename: &Path) -> io::Result<()> {
        let file = File::open(filename)?;
        let mut file = std::io::BufReader::new(file);
        let mut transaction = DBTransaction::new();
        loop {
            let column = u8::deserialize_reader(&mut file)?;
            if column == STATE_FILE_END_MARK {
                break;
            }
            let (key, value) = BorshDeserialize::deserialize_reader(&mut file)?;
            transaction.set(STATE_COLUMNS[usize::from(column)], key, value);
        }
        self.storage.write(transaction)
    }

    /// If the storage is backed by disk, flushes any in-memory data to disk.
    pub fn flush(&self) -> io::Result<()> {
        self.storage.flush()
    }

    /// Blocking compaction request if supported by storage.
    pub fn compact(&self) -> io::Result<()> {
        self.storage.compact()
    }

    pub fn get_store_statistics(&self) -> Option<StoreStatistics> {
        self.storage.get_store_statistics()
    }
}

impl Store {
    pub fn get_db_version(&self) -> io::Result<Option<DbVersion>> {
        DbMetadata::maybe_read_version(self.storage.as_ref())
    }

    pub fn set_db_version(&self, version: DbVersion) -> io::Result<()> {
        let mut store_update = self.store_update();
        store_update.set(DBCol::DbVersion, VERSION_KEY, version.to_string().as_bytes());
        store_update.commit()
    }

    pub fn get_db_kind(&self) -> io::Result<Option<DbKind>> {
        DbMetadata::maybe_read_kind(self.storage.as_ref())
    }

    pub fn set_db_kind(&self, kind: DbKind) -> io::Result<()> {
        let mut store_update = self.store_update();
        store_update.set(DBCol::DbVersion, KIND_KEY, <&str>::from(kind).as_bytes());
        store_update.commit()
    }
}

/// Keeps track of current changes to the database and can commit all of them to the database.
pub struct StoreUpdate {
    transaction: DBTransaction,
    pub(crate) store: Store,
}

impl StoreUpdateAdapter for StoreUpdate {
    fn store_update(&mut self) -> &mut StoreUpdate {
        self
    }
}

impl StoreUpdate {
    const ONE: std::num::NonZeroU32 = match std::num::NonZeroU32::new(1) {
        Some(num) => num,
        None => panic!(),
    };

    /// Inserts a new value into the database.
    ///
    /// It is a programming error if `insert` overwrites an existing, different
    /// value. Use it for insert-only columns.
    pub fn insert(&mut self, column: DBCol, key: Vec<u8>, value: Vec<u8>) {
        assert!(column.is_insert_only(), "can't insert: {column}");
        self.transaction.insert(column, key, value)
    }

    /// Borsh-serializes a value and inserts it into the database.
    ///
    /// It is a programming error if `insert` overwrites an existing, different
    /// value. Use it for insert-only columns.
    ///
    /// Note on performance: The key is always copied into a new allocation,
    /// which is generally bad. However, the insert-only columns use
    /// `CryptoHash` as key, which has the data in a small fixed-sized array.
    /// Copying and allocating that is not prohibitively expensive and we have
    /// to do it either way. Thus, we take a slice for the key for the nice API.
    pub fn insert_ser<T: BorshSerialize>(
        &mut self,
        column: DBCol,
        key: &[u8],
        value: &T,
    ) -> io::Result<()> {
        assert!(column.is_insert_only(), "can't insert_ser: {column}");
        let data = borsh::to_vec(&value)?;
        self.insert(column, key.to_vec(), data);
        Ok(())
    }

    /// Inserts a new reference-counted value or increases its reference count
    /// if it’s already there.
    ///
    /// It is a programming error if `increment_refcount_by` supplies a different
    /// value than the one stored in the database.  It may lead to data
    /// corruption or panics.
    ///
    /// Panics if this is used for columns which are not reference-counted
    /// (see [`DBCol::is_rc`]).
    pub fn increment_refcount_by(
        &mut self,
        column: DBCol,
        key: &[u8],
        data: &[u8],
        increase: std::num::NonZeroU32,
    ) {
        assert!(column.is_rc(), "can't update refcount: {column}");
        let value = refcount::add_positive_refcount(data, increase);
        self.transaction.update_refcount(column, key.to_vec(), value);
    }

    /// Same as `self.increment_refcount_by(column, key, data, 1)`.
    pub fn increment_refcount(&mut self, column: DBCol, key: &[u8], data: &[u8]) {
        self.increment_refcount_by(column, key, data, Self::ONE)
    }

    /// Decreases value of an existing reference-counted value.
    ///
    /// Since decrease of reference count is encoded without the data, only key
    /// and reference count delta arguments are needed.
    ///
    /// Panics if this is used for columns which are not reference-counted
    /// (see [`DBCol::is_rc`]).
    pub fn decrement_refcount_by(
        &mut self,
        column: DBCol,
        key: &[u8],
        decrease: std::num::NonZeroU32,
    ) {
        assert!(column.is_rc(), "can't update refcount: {column}");
        let value = refcount::encode_negative_refcount(decrease);
        self.transaction.update_refcount(column, key.to_vec(), value.to_vec())
    }

    /// Same as `self.decrement_refcount_by(column, key, 1)`.
    pub fn decrement_refcount(&mut self, column: DBCol, key: &[u8]) {
        self.decrement_refcount_by(column, key, Self::ONE)
    }

    /// Modifies a value in the database.
    ///
    /// Unlike `insert`, `increment_refcount` or `decrement_refcount`, arbitrary
    /// modifications are allowed, and extra care must be taken to avoid
    /// consistency anomalies.
    ///
    /// Must not be used for reference-counted columns; use
    /// ['Self::increment_refcount'] or [`Self::decrement_refcount`] instead.
    pub fn set(&mut self, column: DBCol, key: &[u8], value: &[u8]) {
        assert!(!(column.is_rc() || column.is_insert_only()), "can't set: {column}");
        self.transaction.set(column, key.to_vec(), value.to_vec())
    }

    /// Saves a BorshSerialized value.
    ///
    /// Must not be used for reference-counted columns; use
    /// ['Self::increment_refcount'] or [`Self::decrement_refcount`] instead.
    pub fn set_ser<T: BorshSerialize + ?Sized>(
        &mut self,
        column: DBCol,
        key: &[u8],
        value: &T,
    ) -> io::Result<()> {
        assert!(!(column.is_rc() || column.is_insert_only()), "can't set_ser: {column}");
        let data = borsh::to_vec(&value)?;
        self.set(column, key, &data);
        Ok(())
    }

    /// Modify raw value stored in the database, without doing any sanity checks
    /// for ref counts.
    ///
    /// This method is a deliberate escape hatch, and shouldn't be used outside
    /// of auxiliary code like migrations which wants to hack on the database
    /// directly.
    pub fn set_raw_bytes(&mut self, column: DBCol, key: &[u8], value: &[u8]) {
        self.transaction.set(column, key.to_vec(), value.to_vec())
    }

    /// Deletes the given key from the database.
    ///
    /// Must not be used for reference-counted columns; use
    /// ['Self::increment_refcount'] or [`Self::decrement_refcount`] instead.
    pub fn delete(&mut self, column: DBCol, key: &[u8]) {
        // It would panic if called with `State` column, as it is refcounted.
        assert!(!column.is_rc(), "can't delete: {column}");
        self.transaction.delete(column, key.to_vec());
    }

    pub fn delete_all(&mut self, column: DBCol) {
        self.transaction.delete_all(column);
    }

    /// Deletes the given key range from the database including `from` and excluding `to` keys.
    ///
    /// Be aware when using with `DBCol::State`! Keys prefixed with a `ShardUId` might be used
    /// by a descendant shard. See `DBCol::StateShardUIdMapping` for more context.
    pub fn delete_range(&mut self, column: DBCol, from: &[u8], to: &[u8]) {
        self.transaction.delete_range(column, from.to_vec(), to.to_vec());
    }

    /// Merge another store update into this one.
    ///
    /// Panics if `self`’s and `other`’s storage are incompatible.
    pub fn merge(&mut self, other: StoreUpdate) {
        assert!(core::ptr::addr_eq(
            Arc::as_ptr(&self.store.storage),
            Arc::as_ptr(&other.store.storage)
        ));
        self.transaction.merge(other.transaction)
    }

    #[tracing::instrument(
        level = "trace",
        target = "store::update",
        // FIXME: start moving things into tighter modules so that its easier to selectively trace
        // specific things.
        "StoreUpdate::commit",
        skip_all,
        fields(
            transaction.ops.len = self.transaction.ops.len(),
            total_bytes,
            inserts,
            sets,
            rc_ops,
            deletes,
            delete_all_ops,
            delete_range_ops
        )
    )]
    pub fn commit(self) -> io::Result<()> {
        debug_assert!(
            {
                let non_refcount_keys = self
                    .transaction
                    .ops
                    .iter()
                    .filter_map(|op| match op {
                        DBOp::Set { col, key, .. }
                        | DBOp::Insert { col, key, .. }
                        | DBOp::Delete { col, key } => Some((*col as u8, key)),
                        DBOp::UpdateRefcount { .. }
                        | DBOp::DeleteAll { .. }
                        | DBOp::DeleteRange { .. } => None,
                    })
                    .collect::<Vec<_>>();
                non_refcount_keys.len()
                    == non_refcount_keys.iter().collect::<std::collections::HashSet<_>>().len()
            },
            "Transaction overwrites itself: {:?}",
            self
        );
        let span = tracing::Span::current();
        if !span.is_disabled() {
            let [mut insert_count, mut set_count, mut update_rc_count] = [0u64; 3];
            let [mut delete_count, mut delete_all_count, mut delete_range_count] = [0u64; 3];
            let mut total_bytes = 0;
            for op in &self.transaction.ops {
                total_bytes += op.bytes();
                let count = match op {
                    DBOp::Set { .. } => &mut set_count,
                    DBOp::Insert { .. } => &mut insert_count,
                    DBOp::UpdateRefcount { .. } => &mut update_rc_count,
                    DBOp::Delete { .. } => &mut delete_count,
                    DBOp::DeleteAll { .. } => &mut delete_all_count,
                    DBOp::DeleteRange { .. } => &mut delete_range_count,
                };
                *count += 1;
            }
            span.record("inserts", insert_count);
            span.record("sets", set_count);
            span.record("rc_ops", update_rc_count);
            span.record("deletes", delete_count);
            span.record("delete_all_ops", delete_all_count);
            span.record("delete_range_ops", delete_range_count);
            span.record("total_bytes", total_bytes);
        }
        if tracing::event_enabled!(target: "store::update::transactions", tracing::Level::TRACE) {
            for op in &self.transaction.ops {
                match op {
                    DBOp::Insert { col, key, value } => tracing::trace!(
                        target: "store::update::transactions",
                        db_op = "insert",
                        %col,
                        key = %StorageKey(key),
                        size = value.len(),
                        value = %AbbrBytes(value),
                    ),
                    DBOp::Set { col, key, value } => tracing::trace!(
                        target: "store::update::transactions",
                        db_op = "set",
                        %col,
                        key = %StorageKey(key),
                        size = value.len(),
                        value = %AbbrBytes(value)
                    ),
                    DBOp::UpdateRefcount { col, key, value } => tracing::trace!(
                        target: "store::update::transactions",
                        db_op = "update_rc",
                        %col,
                        key = %StorageKey(key),
                        size = value.len(),
                        value = %AbbrBytes(value)
                    ),
                    DBOp::Delete { col, key } => tracing::trace!(
                        target: "store::update::transactions",
                        db_op = "delete",
                        %col,
                        key = %StorageKey(key)
                    ),
                    DBOp::DeleteAll { col } => tracing::trace!(
                        target: "store::update::transactions",
                        db_op = "delete_all",
                        %col
                    ),
                    DBOp::DeleteRange { col, from, to } => tracing::trace!(
                        target: "store::update::transactions",
                        db_op = "delete_range",
                        %col,
                        from = %StorageKey(from),
                        to = %StorageKey(to)
                    ),
                }
            }
        }
        self.store.storage.write(self.transaction)
    }
}

impl fmt::Debug for StoreUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Store Update {{")?;
        for op in &self.transaction.ops {
            match op {
                DBOp::Insert { col, key, .. } => writeln!(f, "  + {col} {}", StorageKey(key))?,
                DBOp::Set { col, key, .. } => writeln!(f, "  = {col} {}", StorageKey(key))?,
                DBOp::UpdateRefcount { col, key, .. } => {
                    writeln!(f, "  ± {col} {}", StorageKey(key))?
                }
                DBOp::Delete { col, key } => writeln!(f, "  - {col} {}", StorageKey(key))?,
                DBOp::DeleteAll { col } => writeln!(f, "  - {col} (all)")?,
                DBOp::DeleteRange { col, from, to } => {
                    writeln!(f, "  - {col} [{}, {})", StorageKey(from), StorageKey(to))?
                }
            }
        }
        writeln!(f, "}}")
    }
}

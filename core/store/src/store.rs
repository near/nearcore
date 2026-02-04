use crate::DBCol;
use crate::adapter::{StoreAdapter, StoreUpdateAdapter};
use crate::db::metadata::{DbKind, DbMetadata, DbVersion, KIND_KEY, VERSION_KEY};
use crate::db::{DBIterator, DBOp, DBSlice, DBTransaction, Database, StoreStatistics, refcount};
use crate::deserialized_column;
use borsh::{BorshDeserialize, BorshSerialize};
use enum_map::EnumMap;
use near_fmt::{AbbrBytes, StorageKey};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::{fmt, io};
use strum::IntoEnumIterator;

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
    storage: Arc<dyn Database>,
    cache: Arc<deserialized_column::Cache>,
}

impl StoreAdapter for Store {
    fn store_ref(&self) -> &Store {
        self
    }
}

impl Store {
    pub fn new(storage: Arc<dyn Database>) -> Self {
        let cache = storage.deserialized_column_cache();
        Self { storage, cache }
    }

    pub fn database(&self) -> &dyn Database {
        &*self.storage
    }

    /// Fetches value from given column.
    ///
    /// If the key does not exist in the column returns `None`.  Otherwise
    /// returns the data as [`DBSlice`] object.  The object dereferences into
    /// a slice, for cases when caller doesn’t need to own the value, and
    /// provides conversion into a vector or an Arc.
    pub fn get(&self, column: DBCol, key: &[u8]) -> Option<DBSlice<'_>> {
        let value = if column.is_rc() {
            self.storage.get_with_rc_stripped(column, &key)
        } else {
            self.storage.get_raw_bytes(column, &key)
        };
        tracing::trace!(
            target: "store",
            db_op = "get",
            col = %column,
            key = %StorageKey(key),
            size = value.as_deref().map(<[u8]>::len)
        );
        value
    }

    pub fn get_ser<T: BorshDeserialize>(&self, column: DBCol, key: &[u8]) -> io::Result<Option<T>> {
        self.get(column, key).as_deref().map(T::try_from_slice).transpose()
    }

    pub fn caching_get_ser<T: BorshDeserialize + Send + Sync + 'static>(
        &self,
        column: DBCol,
        key: &[u8],
    ) -> io::Result<Option<Arc<T>>> {
        let Some(cache) = self.cache.work_with(column) else {
            return self.get_ser::<T>(column, key).map(|v| v.map(Into::into));
        };

        let generation_before = {
            let mut lock = cache.lock();
            if let Some(value) = lock.values.get(key) {
                if let Some(value) = value {
                    // If the value is already cached, try to downcast it to the requested type.
                    // If it fails, we log a debug message and continue to fetch from the database.
                    match Arc::downcast::<T>(Arc::clone(value)) {
                        Ok(result) => return Ok(Some(result)),
                        Err(_) => {
                            tracing::debug!(
                                target: "store",
                                requested = std::any::type_name::<T>(),
                                "could not downcast an available cached deserialized value"
                            );
                        }
                    }
                } else {
                    // Value is cached as `None`, which means it was previously fetched
                    // but was not found in the database.
                    return Ok(None);
                }
            }
            lock.generation
        };

        let value = match self.get_ser::<T>(column, key) {
            Ok(Some(value)) => Some(Arc::from(value)),
            Ok(None) => None,
            Err(e) => return Err(e),
        };

        let mut lock = cache.lock();
        if lock.active_flushes == 0 && lock.generation == generation_before {
            if let Some(v) = value.as_ref() {
                lock.values.put(key.into(), Some(Arc::clone(v) as _));
            } else if lock.store_none_values() {
                // If the cache is configured to store `None` values, we store it.
                lock.values.put(key.into(), None);
            }
        }
        Ok(value)
    }

    pub fn exists(&self, column: DBCol, key: &[u8]) -> bool {
        self.get(column, key).is_some()
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
        self.storage.iter(col).map(|(key, value)| Ok((key, T::try_from_slice(value.as_ref())?)))
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
            .map(|(key, value)| Ok((key, T::try_from_slice(value.as_ref())?)))
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
            for (key, value) in self.storage.iter_raw_bytes(column) {
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
        self.write(transaction)
    }

    pub fn write(&self, transaction: DBTransaction) -> io::Result<()> {
        let mut keys_flushed = EnumMap::<DBCol, u64>::from_fn(|_| 0);

        for op in &transaction.ops {
            match op {
                DBOp::Set { col, key, .. }
                | DBOp::Insert { col, key, .. }
                | DBOp::UpdateRefcount { col, key, .. }
                | DBOp::Delete { col, key } => {
                    // FIXME(nagisa): investigate if collecting all the keys to discard into a
                    // vector and then flushing everything in a single lock would be more
                    // performant.
                    let Some(cache) = self.cache.work_with(*col) else { continue };
                    let mut lock = cache.lock();
                    lock.active_flushes += 1;
                    lock.generation += 1;
                    keys_flushed[*col] += 1;
                    lock.values.pop(key);
                }
                DBOp::DeleteAll { col } | DBOp::DeleteRange { col, .. } => {
                    let Some(cache) = self.cache.work_with(*col) else { continue };
                    let mut lock = cache.lock();
                    lock.active_flushes += 1;
                    lock.generation += 1;
                    keys_flushed[*col] += 1;
                    lock.values.clear();
                }
            }
        }
        self.storage.write(transaction);
        for col in DBCol::iter() {
            let flushed = keys_flushed[col];
            if flushed != 0 {
                let Some(cache) = self.cache.work_with(col) else { continue };
                cache.lock().active_flushes -= flushed;
            }
        }
        Ok(())
    }

    /// If the storage is backed by disk, flushes any in-memory data to disk.
    pub fn flush(&self) -> io::Result<()> {
        self.storage.flush();
        Ok(())
    }

    /// Blocking compaction request if supported by storage.
    pub fn compact(&self) -> io::Result<()> {
        self.storage.compact();
        Ok(())
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
        self.store.write(self.transaction)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::TestDB;
    use std::sync::{Arc, Barrier};

    /// A Database wrapper that pauses during reads to allow testing race conditions
    /// in the caching layer. After reading from the inner DB, it waits for a
    /// barrier synchronization before returning, giving another thread time to
    /// complete a write in between.
    struct SyncReadDatabase {
        inner: TestDB,
        /// (read_done_barrier, continue_barrier, armed)
        /// When armed, get_raw_bytes will: read value, wait on read_done_barrier,
        /// then wait on continue_barrier, then return.
        sync: parking_lot::Mutex<Option<(Arc<Barrier>, Arc<Barrier>)>>,
    }

    impl SyncReadDatabase {
        fn new() -> Self {
            Self { inner: TestDB::default(), sync: parking_lot::Mutex::new(None) }
        }

        fn arm(&self, read_done: Arc<Barrier>, cont: Arc<Barrier>) {
            *self.sync.lock() = Some((read_done, cont));
        }
    }

    impl Database for SyncReadDatabase {
        fn get_raw_bytes(&self, col: DBCol, key: &[u8]) -> Option<crate::db::DBSlice<'_>> {
            let value = self.inner.get_raw_bytes(col, key);
            if let Some((read_done, cont)) = self.sync.lock().take() {
                read_done.wait();
                cont.wait();
            }
            value
        }

        fn iter<'a>(&'a self, col: DBCol) -> crate::db::DBIterator<'a> {
            self.inner.iter(col)
        }
        fn iter_prefix<'a>(&'a self, col: DBCol, p: &'a [u8]) -> crate::db::DBIterator<'a> {
            self.inner.iter_prefix(col, p)
        }
        fn iter_range<'a>(
            &'a self,
            col: DBCol,
            lo: Option<&[u8]>,
            hi: Option<&[u8]>,
        ) -> crate::db::DBIterator<'a> {
            self.inner.iter_range(col, lo, hi)
        }
        fn iter_raw_bytes<'a>(&'a self, col: DBCol) -> crate::db::DBIterator<'a> {
            self.inner.iter_raw_bytes(col)
        }
        fn write(&self, batch: crate::db::DBTransaction) {
            self.inner.write(batch)
        }
        fn flush(&self) {}
        fn compact(&self) {}
        fn get_store_statistics(&self) -> Option<crate::StoreStatistics> {
            None
        }
        fn create_checkpoint(
            &self,
            _: &std::path::Path,
            _: Option<&[DBCol]>,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        fn deserialized_column_cache(&self) -> Arc<deserialized_column::Cache> {
            self.inner.deserialized_column_cache()
        }
    }

    /// Regression test for a race condition in `caching_get_ser`:
    ///
    /// Thread A reads an old value from DB (cache miss, lock released during IO).
    /// Thread B completes a full write cycle (pop cache, write DB, decrement
    /// active_flushes). Thread A then tries to cache its stale value and sees
    /// active_flushes == 0, so it stores the stale entry. Subsequent reads hit
    /// the cache and return stale data.
    ///
    /// The fix adds a `generation` counter that is bumped on every write. The
    /// reader records the generation before its DB read and refuses to cache if
    /// the generation has changed.
    #[test]
    fn test_caching_race_stale_value() {
        let col = DBCol::BlockMisc; // cached column
        let key = b"test_key";

        let db = Arc::new(SyncReadDatabase::new());
        let store = Store::new(db.clone() as Arc<dyn Database>);

        // Seed the DB with value 1. Don't go through cache.
        {
            let mut su = store.store_update();
            su.set_ser(col, key, &1u64).unwrap();
            store.write(su.transaction).unwrap();
        }

        // Set up synchronization: two barriers of size 2.
        let read_done = Arc::new(Barrier::new(2));
        let cont = Arc::new(Barrier::new(2));
        db.arm(read_done.clone(), cont.clone());

        // Spawn a reader thread. It will:
        // 1. caching_get_ser → cache miss (first read of this key)
        // 2. Read from DB → gets 1
        // 3. Wait on read_done barrier
        // 4. Wait on cont barrier
        // 5. Try to cache the value
        let store2 = store.clone();
        let reader = std::thread::spawn(move || {
            let result: Option<Arc<u64>> = store2.caching_get_ser(col, key).unwrap();
            *result.unwrap()
        });

        // Main thread: wait for reader to complete its DB read.
        read_done.wait();

        // While the reader is paused, write value 2 through the Store.
        // This pops the key from cache, increments active_flushes,
        // writes to DB, and decrements active_flushes back to 0.
        {
            let mut su = store.store_update();
            su.set_ser(col, key, &2u64).unwrap();
            su.commit().unwrap();
        }

        // Let the reader continue. It will now try to cache the old value (1).
        cont.wait();

        // The reader's in-flight result is 1 (it read before the write). That's OK.
        let reader_result = reader.join().unwrap();
        assert_eq!(reader_result, 1);

        // The critical check: a fresh read must return the NEW value (2).
        // Without the generation counter fix, the cache would contain 1.
        let fresh: Arc<u64> = store.caching_get_ser(col, key).unwrap().unwrap();
        assert_eq!(*fresh, 2, "cache returned stale value");
    }
}

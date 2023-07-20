extern crate core;

use std::fs::File;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::{fmt, io};

use borsh::{BorshDeserialize, BorshSerialize};
use metadata::{DbKind, DbVersion, KIND_KEY, VERSION_KEY};
use once_cell::sync::Lazy;
use strum;

pub use columns::DBCol;
pub use db::{
    CHUNK_TAIL_KEY, COLD_HEAD_KEY, FINAL_HEAD_KEY, FORK_TAIL_KEY, GENESIS_JSON_HASH_KEY,
    GENESIS_STATE_ROOTS_KEY, HEADER_HEAD_KEY, HEAD_KEY, LARGEST_TARGET_HEIGHT_KEY,
    LATEST_KNOWN_KEY, STATE_SYNC_DUMP_KEY, TAIL_KEY,
};
use near_crypto::PublicKey;
use near_fmt::{AbbrBytes, StorageKey};
use near_primitives::account::{AccessKey, Account};
use near_primitives::contract::ContractCode;
pub use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{DelayedReceiptIndices, Receipt, ReceivedData};
pub use near_primitives::shard_layout::ShardUId;
use near_primitives::trie_key::{trie_key_parsers, TrieKey};
use near_primitives::types::{AccountId, StateRoot};
use near_vm_runner::logic::{CompiledContract, CompiledContractCache};

use crate::db::{refcount, DBIterator, DBOp, DBSlice, DBTransaction, Database, StoreStatistics};
pub use crate::trie::iterator::{TrieIterator, TrieTraversalItem};
pub use crate::trie::update::{TrieUpdate, TrieUpdateIterator, TrieUpdateValuePtr};
pub use crate::trie::{
    estimator, split_state, ApplyStatePartResult, KeyForStateChanges, KeyLookupMode, NibbleSlice,
    PartialStorage, PrefetchApi, PrefetchError, RawTrieNode, RawTrieNodeWithSize, ShardTries,
    StateSnapshot, StateSnapshotConfig, Trie, TrieAccess, TrieCache, TrieCachingStorage,
    TrieChanges, TrieConfig, TrieDBStorage, TrieStorage, WrappedTrieChanges,
};

pub mod cold_storage;
mod columns;
pub mod config;
pub mod db;
pub mod flat;
pub mod genesis;
pub mod metadata;
pub mod metrics;
pub mod migrations;
mod opener;
mod rocksdb_metrics;
mod sync_utils;
pub mod test_utils;
pub mod trie;

pub use crate::config::{Mode, StoreConfig};
pub use crate::opener::{
    checkpoint_hot_storage_and_cleanup_columns, StoreMigrator, StoreOpener, StoreOpenerError,
};

/// Specifies temperature of a storage.
///
/// Since currently only hot storage is implemented, this has only one variant.
/// In the future, certain parts of the code may need to access hot or cold
/// storage.  Specifically, querying an old block will require reading it from
/// the cold storage.
#[derive(Clone, Copy, Debug, Eq, PartialEq, strum::IntoStaticStr)]
pub enum Temperature {
    Hot,
    Cold,
}

impl FromStr for Temperature {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let s = s.to_lowercase();
        match s.as_str() {
            "hot" => Ok(Temperature::Hot),
            "cold" => Ok(Temperature::Cold),
            _ => Err(String::from(format!("invalid temperature string {s}"))),
        }
    }
}

const STATE_COLUMNS: [DBCol; 2] = [DBCol::State, DBCol::FlatState];
const STATE_FILE_END_MARK: u8 = 255;

/// Node’s storage holding chain and all other necessary data.
///
/// Provides access to hot storage, cold storage and split storage. Typically
/// users will want to use one of the above via the Store abstraction.
pub struct NodeStorage {
    hot_storage: Arc<dyn Database>,
    cold_storage: Option<Arc<crate::db::ColdDB>>,
}

/// Node’s single storage source.
///
/// The Store holds one of the possible databases:
/// - The hot database - access to the hot database only
/// - The cold database - access to the cold database only
/// - The split database - access to both hot and cold databases
#[derive(Clone)]
pub struct Store {
    storage: Arc<dyn Database>,
}

impl NodeStorage {
    /// Initialises a new opener with given home directory and hot and cold
    /// store config.
    pub fn opener<'a>(
        home_dir: &std::path::Path,
        archive: bool,
        config: &'a StoreConfig,
        cold_config: Option<&'a StoreConfig>,
    ) -> StoreOpener<'a> {
        StoreOpener::new(home_dir, archive, config, cold_config)
    }

    /// Constructs new object backed by given database.
    fn from_rocksdb(
        hot_storage: crate::db::RocksDB,
        cold_storage: Option<crate::db::RocksDB>,
    ) -> Self {
        let hot_storage = Arc::new(hot_storage);
        let cold_storage = cold_storage.map(|storage| Arc::new(storage));

        let cold_db = if let Some(cold_storage) = cold_storage {
            Some(Arc::new(crate::db::ColdDB::new(cold_storage)))
        } else {
            None
        };

        Self { hot_storage: hot_storage, cold_storage: cold_db }
    }

    /// Initialises an opener for a new temporary test store.
    ///
    /// As per the name, this is meant for tests only.  The created store will
    /// use test configuration (which may differ slightly from default config).
    /// The function **panics** if a temporary directory cannot be created.
    ///
    /// Note that the caller must hold the temporary directory returned as first
    /// element of the tuple while the store is open.
    pub fn test_opener() -> (tempfile::TempDir, StoreOpener<'static>) {
        static CONFIG: Lazy<StoreConfig> = Lazy::new(StoreConfig::test_config);
        let dir = tempfile::tempdir().unwrap();
        let opener = StoreOpener::new(dir.path(), false, &CONFIG, None);
        (dir, opener)
    }

    /// Constructs new object backed by given database.
    ///
    /// Note that you most likely don’t want to use this method.  If you’re
    /// opening an on-disk storage, you want to use [`Self::opener`] instead
    /// which takes care of opening the on-disk database and applying all the
    /// necessary configuration.  If you need an in-memory database for testing,
    /// you want either [`crate::test_utils::create_test_node_storage`] or
    /// possibly [`crate::test_utils::create_test_store`] (depending whether you
    /// need [`NodeStorage`] or [`Store`] object.
    pub fn new(storage: Arc<dyn Database>) -> Self {
        Self { hot_storage: storage, cold_storage: None }
    }
}

impl NodeStorage {
    /// Returns the hot store. The hot store is always available and it provides
    /// direct access to the hot database.
    ///
    /// For RPC nodes this is the only store available and it should be used for
    /// all the use cases.
    ///
    /// For archival nodes that do not have split storage configured this is the
    /// only store available and it should be used for all the use cases.
    ///
    /// For archival nodes that do have split storage configured there are three
    /// stores available: hot, cold and split. The client should use the hot
    /// store, the view client should use the split store and the cold store
    /// loop should use cold store.
    pub fn get_hot_store(&self) -> Store {
        Store { storage: self.hot_storage.clone() }
    }

    /// Returns the cold store. The cold store is only available in archival
    /// nodes with split storage configured.
    ///
    /// For archival nodes that do have split storage configured there are three
    /// stores available: hot, cold and split. The client should use the hot
    /// store, the view client should use the split store and the cold store
    /// loop should use cold store.
    pub fn get_cold_store(&self) -> Option<Store> {
        match &self.cold_storage {
            Some(cold_storage) => Some(Store { storage: cold_storage.clone() }),
            None => None,
        }
    }

    /// Returns the split store. The split store is only available in archival
    /// nodes with split storage configured.
    ///
    /// For archival nodes that do have split storage configured there are three
    /// stores available: hot, cold and split. The client should use the hot
    /// store, the view client should use the split store and the cold store
    /// loop should use cold store.
    pub fn get_split_store(&self) -> Option<Store> {
        match &self.cold_storage {
            Some(cold_storage) => Some(Store {
                storage: crate::db::SplitDB::new(self.hot_storage.clone(), cold_storage.clone()),
            }),
            None => None,
        }
    }

    /// Returns underlying database for given temperature.
    ///
    /// This allows accessing underlying hot and cold databases directly
    /// bypassing any abstractions offered by [`NodeStorage`] or [`Store`] interfaces.
    ///
    /// This is useful for certain data which only lives in hot storage and
    /// interfaces which deal with it.  For example, peer store uses hot
    /// storage’s [`Database`] interface directly.
    ///
    /// Note that this is not appropriate for code which only ever accesses hot
    /// storage but touches information kinds which live in cold storage as
    /// well.  For example, garbage collection only ever touches hot storage but
    /// it should go through [`Store`] interface since data it manipulates
    /// (e.g. blocks) are live in both databases.
    ///
    /// This method panics if trying to access cold store but it wasn't configured.
    pub fn into_inner(self, temp: Temperature) -> Arc<dyn Database> {
        match temp {
            Temperature::Hot => self.hot_storage,
            Temperature::Cold => self.cold_storage.unwrap(),
        }
    }
}

impl NodeStorage {
    /// Returns whether the storage has a cold database.
    pub fn has_cold(&self) -> bool {
        self.cold_storage.is_some()
    }

    /// Reads database metadata and returns whether the storage is archival.
    pub fn is_archive(&self) -> io::Result<bool> {
        if self.cold_storage.is_some() {
            return Ok(true);
        }
        Ok(match metadata::DbMetadata::read(self.hot_storage.as_ref())?.kind.unwrap() {
            metadata::DbKind::RPC => false,
            metadata::DbKind::Archive => true,
            metadata::DbKind::Hot | metadata::DbKind::Cold => todo!(),
        })
    }

    pub fn new_with_cold(hot: Arc<dyn Database>, cold: Arc<dyn Database>) -> Self {
        Self { hot_storage: hot, cold_storage: Some(Arc::new(crate::db::ColdDB::new(cold))) }
    }

    pub fn cold_db(&self) -> Option<&Arc<crate::db::ColdDB>> {
        self.cold_storage.as_ref()
    }
}

impl Store {
    /// Fetches value from given column.
    ///
    /// If the key does not exist in the column returns `None`.  Otherwise
    /// returns the data as [`DBSlice`] object.  The object dereferences into
    /// a slice, for cases when caller doesn’t need to own the value, and
    /// provides conversion into a vector or an Arc.
    pub fn get(&self, column: DBCol, key: &[u8]) -> io::Result<Option<DBSlice<'_>>> {
        let value = if column.is_rc() {
            self.storage.get_with_rc_stripped(column, key)
        } else {
            self.storage.get_raw_bytes(column, key)
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
        StoreUpdate::new(Arc::clone(&self.storage))
    }

    pub fn iter<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        self.storage.iter(col)
    }

    /// Fetches raw key/value pairs from the database.
    ///
    /// Practically, this means that for rc columns rc is included in the value.
    /// This method is a deliberate escape hatch, and shouldn't be used outside
    /// of auxilary code like migrations which wants to hack on the database
    /// directly.
    pub fn iter_raw_bytes<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        self.storage.iter_raw_bytes(col)
    }

    pub fn iter_prefix<'a>(&'a self, col: DBCol, key_prefix: &'a [u8]) -> DBIterator<'a> {
        self.storage.iter_prefix(col, key_prefix)
    }

    /// Iterates over a range of keys. Upper bound key is not included.
    pub fn iter_range<'a>(
        &'a self,
        col: DBCol,
        lower_bound: Option<&[u8]>,
        upper_bound: Option<&[u8]>,
    ) -> DBIterator<'a> {
        self.storage.iter_range(col, lower_bound, upper_bound)
    }

    pub fn iter_prefix_ser<'a, T: BorshDeserialize>(
        &'a self,
        col: DBCol,
        key_prefix: &'a [u8],
    ) -> impl Iterator<Item = io::Result<(Box<[u8]>, T)>> + 'a {
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
        metadata::DbMetadata::maybe_read_version(self.storage.as_ref())
    }

    pub fn set_db_version(&self, version: DbVersion) -> io::Result<()> {
        let mut store_update = self.store_update();
        store_update.set(DBCol::DbVersion, VERSION_KEY, version.to_string().as_bytes());
        store_update.commit()
    }

    pub fn get_db_kind(&self) -> io::Result<Option<DbKind>> {
        metadata::DbMetadata::maybe_read_kind(self.storage.as_ref())
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
    storage: Arc<dyn Database>,
}

impl StoreUpdate {
    const ONE: std::num::NonZeroU32 = match std::num::NonZeroU32::new(1) {
        Some(num) => num,
        None => panic!(),
    };

    pub(crate) fn new(db: Arc<dyn Database>) -> Self {
        StoreUpdate { transaction: DBTransaction::new(), storage: db }
    }

    /// Inserts a new value into the database.
    ///
    /// It is a programming error if `insert` overwrites an existing, different
    /// value. Use it for insert-only columns.
    pub fn insert(&mut self, column: DBCol, key: &[u8], value: &[u8]) {
        assert!(column.is_insert_only(), "can't insert: {column}");
        self.transaction.insert(column, key.to_vec(), value.to_vec())
    }

    pub fn insert_ser<T: BorshSerialize>(
        &mut self,
        column: DBCol,
        key: &[u8],
        value: &T,
    ) -> io::Result<()> {
        assert!(column.is_insert_only(), "can't insert_ser: {column}");
        let data = value.try_to_vec()?;
        self.insert(column, key, &data);
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
        self.transaction.update_refcount(column, key.to_vec(), value)
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
        let data = value.try_to_vec()?;
        self.set(column, key, &data);
        Ok(())
    }

    /// Modify raw value stored in the database, without doing any sanity checks
    /// for ref counts.
    ///
    /// This method is a deliberate escape hatch, and shouldn't be used outside
    /// of auxilary code like migrations which wants to hack on the database
    /// directly.
    pub fn set_raw_bytes(&mut self, column: DBCol, key: &[u8], value: &[u8]) {
        self.transaction.set(column, key.to_vec(), value.to_vec())
    }

    /// Deletes the given key from the database.
    ///
    /// Must not be used for reference-counted columns; use
    /// ['Self::increment_refcount'] or [`Self::decrement_refcount`] instead.
    pub fn delete(&mut self, column: DBCol, key: &[u8]) {
        assert!(!column.is_rc(), "can't delete: {column}");
        self.transaction.delete(column, key.to_vec());
    }

    pub fn delete_all(&mut self, column: DBCol) {
        self.transaction.delete_all(column);
    }

    /// Deletes the given key range from the database including `from`
    /// and excluding `to` keys.
    pub fn delete_range(&mut self, column: DBCol, from: &[u8], to: &[u8]) {
        self.transaction.delete_range(column, from.to_vec(), to.to_vec());
    }

    /// Merge another store update into this one.
    ///
    /// Panics if `self`’s and `other`’s storage are incompatible.
    pub fn merge(&mut self, other: StoreUpdate) {
        assert!(same_db(&self.storage, &other.storage));
        self.transaction.merge(other.transaction)
    }

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
        let _span = tracing::trace_span!(target: "store", "commit").entered();
        for op in &self.transaction.ops {
            match op {
                DBOp::Insert { col, key, value } => {
                    tracing::trace!(target: "store", db_op = "insert", col = %col, key = %StorageKey(key), size = value.len(), value = %AbbrBytes(value),)
                }
                DBOp::Set { col, key, value } => {
                    tracing::trace!(target: "store", db_op = "set", col = %col, key = %StorageKey(key), size = value.len(), value = %AbbrBytes(value))
                }
                DBOp::UpdateRefcount { col, key, value } => {
                    tracing::trace!(target: "store", db_op = "update_rc", col = %col, key = %StorageKey(key), size = value.len(), value = %AbbrBytes(value))
                }
                DBOp::Delete { col, key } => {
                    tracing::trace!(target: "store", db_op = "delete", col = %col, key = %StorageKey(key))
                }
                DBOp::DeleteAll { col } => {
                    tracing::trace!(target: "store", db_op = "delete_all", col = %col)
                }
                DBOp::DeleteRange { col, from, to } => {
                    tracing::trace!(target: "store", db_op = "delete_range", col = %col, from = %StorageKey(from), to = %StorageKey(to))
                }
            }
        }
        self.storage.write(self.transaction)
    }
}

fn same_db(lhs: &Arc<dyn Database>, rhs: &Arc<dyn Database>) -> bool {
    // Note: avoid comparing wide pointers here to work-around
    // https://github.com/rust-lang/rust/issues/69757
    let addr = |arc| Arc::as_ptr(arc) as *const u8;
    return addr(lhs) == addr(rhs);
}

impl fmt::Debug for StoreUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Store Update {{")?;
        for op in self.transaction.ops.iter() {
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

/// Reads an object from Trie.
/// # Errors
/// see StorageError
pub fn get<T: BorshDeserialize>(
    trie: &dyn TrieAccess,
    key: &TrieKey,
) -> Result<Option<T>, StorageError> {
    match trie.get(key)? {
        None => Ok(None),
        Some(data) => match T::try_from_slice(&data) {
            Err(_err) => {
                Err(StorageError::StorageInconsistentState("Failed to deserialize".to_string()))
            }
            Ok(value) => Ok(Some(value)),
        },
    }
}

/// Writes an object into Trie.
pub fn set<T: BorshSerialize>(state_update: &mut TrieUpdate, key: TrieKey, value: &T) {
    let data = value.try_to_vec().expect("Borsh serializer is not expected to ever fail");
    state_update.set(key, data);
}

pub fn set_account(state_update: &mut TrieUpdate, account_id: AccountId, account: &Account) {
    set(state_update, TrieKey::Account { account_id }, account)
}

pub fn get_account(
    trie: &dyn TrieAccess,
    account_id: &AccountId,
) -> Result<Option<Account>, StorageError> {
    get(trie, &TrieKey::Account { account_id: account_id.clone() })
}

pub fn set_received_data(
    state_update: &mut TrieUpdate,
    receiver_id: AccountId,
    data_id: CryptoHash,
    data: &ReceivedData,
) {
    set(state_update, TrieKey::ReceivedData { receiver_id, data_id }, data);
}

pub fn get_received_data(
    trie: &dyn TrieAccess,
    receiver_id: &AccountId,
    data_id: CryptoHash,
) -> Result<Option<ReceivedData>, StorageError> {
    get(trie, &TrieKey::ReceivedData { receiver_id: receiver_id.clone(), data_id })
}

pub fn set_postponed_receipt(state_update: &mut TrieUpdate, receipt: &Receipt) {
    let key = TrieKey::PostponedReceipt {
        receiver_id: receipt.receiver_id.clone(),
        receipt_id: receipt.receipt_id,
    };
    set(state_update, key, receipt);
}

pub fn remove_postponed_receipt(
    state_update: &mut TrieUpdate,
    receiver_id: &AccountId,
    receipt_id: CryptoHash,
) {
    state_update.remove(TrieKey::PostponedReceipt { receiver_id: receiver_id.clone(), receipt_id });
}

pub fn get_postponed_receipt(
    trie: &dyn TrieAccess,
    receiver_id: &AccountId,
    receipt_id: CryptoHash,
) -> Result<Option<Receipt>, StorageError> {
    get(trie, &TrieKey::PostponedReceipt { receiver_id: receiver_id.clone(), receipt_id })
}

pub fn get_delayed_receipt_indices(
    trie: &dyn TrieAccess,
) -> Result<DelayedReceiptIndices, StorageError> {
    Ok(get(trie, &TrieKey::DelayedReceiptIndices)?.unwrap_or_default())
}

// Adds the given receipt into the end of the delayed receipt queue in the state.
pub fn set_delayed_receipt(
    state_update: &mut TrieUpdate,
    delayed_receipts_indices: &mut DelayedReceiptIndices,
    receipt: &Receipt,
) {
    set(
        state_update,
        TrieKey::DelayedReceipt { index: delayed_receipts_indices.next_available_index },
        receipt,
    );
    delayed_receipts_indices.next_available_index = delayed_receipts_indices
        .next_available_index
        .checked_add(1)
        .expect("Next available index for delayed receipt exceeded the integer limit");
}

pub fn set_access_key(
    state_update: &mut TrieUpdate,
    account_id: AccountId,
    public_key: PublicKey,
    access_key: &AccessKey,
) {
    set(state_update, TrieKey::AccessKey { account_id, public_key }, access_key);
}

pub fn remove_access_key(
    state_update: &mut TrieUpdate,
    account_id: AccountId,
    public_key: PublicKey,
) {
    state_update.remove(TrieKey::AccessKey { account_id, public_key });
}

pub fn get_access_key(
    trie: &dyn TrieAccess,
    account_id: &AccountId,
    public_key: &PublicKey,
) -> Result<Option<AccessKey>, StorageError> {
    get(
        trie,
        &TrieKey::AccessKey { account_id: account_id.clone(), public_key: public_key.clone() },
    )
}

pub fn get_access_key_raw(
    trie: &dyn TrieAccess,
    raw_key: &[u8],
) -> Result<Option<AccessKey>, StorageError> {
    get(
        trie,
        &trie_key_parsers::parse_trie_key_access_key_from_raw_key(raw_key)
            .expect("access key in the state should be correct"),
    )
}

pub fn set_code(state_update: &mut TrieUpdate, account_id: AccountId, code: &ContractCode) {
    state_update.set(TrieKey::ContractCode { account_id }, code.code().to_vec());
}

pub fn get_code(
    trie: &dyn TrieAccess,
    account_id: &AccountId,
    code_hash: Option<CryptoHash>,
) -> Result<Option<ContractCode>, StorageError> {
    let key = TrieKey::ContractCode { account_id: account_id.clone() };
    trie.get(&key).map(|opt| opt.map(|code| ContractCode::new(code, code_hash)))
}

/// Removes account, code and all access keys associated to it.
pub fn remove_account(
    state_update: &mut TrieUpdate,
    account_id: &AccountId,
) -> Result<(), StorageError> {
    state_update.remove(TrieKey::Account { account_id: account_id.clone() });
    state_update.remove(TrieKey::ContractCode { account_id: account_id.clone() });

    // Removing access keys
    let public_keys = state_update
        .iter(&trie_key_parsers::get_raw_prefix_for_access_keys(account_id))?
        .map(|raw_key| {
            trie_key_parsers::parse_public_key_from_access_key_key(&raw_key?, account_id).map_err(
                |_e| {
                    StorageError::StorageInconsistentState(
                        "Can't parse public key from raw key for AccessKey".to_string(),
                    )
                },
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    for public_key in public_keys {
        state_update.remove(TrieKey::AccessKey { account_id: account_id.clone(), public_key });
    }

    // Removing contract data
    let data_keys = state_update
        .iter(&trie_key_parsers::get_raw_prefix_for_contract_data(account_id, &[]))?
        .map(|raw_key| {
            trie_key_parsers::parse_data_key_from_contract_data_key(&raw_key?, account_id)
                .map_err(|_e| {
                    StorageError::StorageInconsistentState(
                        "Can't parse data key from raw key for ContractData".to_string(),
                    )
                })
                .map(Vec::from)
        })
        .collect::<Result<Vec<_>, _>>()?;
    for key in data_keys {
        state_update.remove(TrieKey::ContractData { account_id: account_id.clone(), key });
    }
    Ok(())
}

pub fn get_genesis_state_roots(store: &Store) -> io::Result<Option<Vec<StateRoot>>> {
    store.get_ser::<Vec<StateRoot>>(DBCol::BlockMisc, GENESIS_STATE_ROOTS_KEY)
}

pub fn get_genesis_hash(store: &Store) -> io::Result<Option<CryptoHash>> {
    store.get_ser::<CryptoHash>(DBCol::BlockMisc, GENESIS_JSON_HASH_KEY)
}

pub fn set_genesis_hash(store_update: &mut StoreUpdate, genesis_hash: &CryptoHash) {
    store_update
        .set_ser::<CryptoHash>(DBCol::BlockMisc, GENESIS_JSON_HASH_KEY, genesis_hash)
        .expect("Borsh cannot fail");
}

pub fn set_genesis_state_roots(store_update: &mut StoreUpdate, genesis_roots: &[StateRoot]) {
    store_update
        .set_ser(DBCol::BlockMisc, GENESIS_STATE_ROOTS_KEY, genesis_roots)
        .expect("Borsh cannot fail");
}

pub struct StoreCompiledContractCache {
    db: Arc<dyn Database>,
}

impl StoreCompiledContractCache {
    pub fn new(store: &Store) -> Self {
        Self { db: store.storage.clone() }
    }
}

/// Cache for compiled contracts code using Store for keeping data.
/// We store contracts in VM-specific format in DBCol::CachedContractCode.
/// Key must take into account VM being used and its configuration, so that
/// we don't cache non-gas metered binaries, for example.
impl CompiledContractCache for StoreCompiledContractCache {
    fn put(&self, key: &CryptoHash, value: CompiledContract) -> io::Result<()> {
        let mut update = crate::db::DBTransaction::new();
        // We intentionally use `.set` here, rather than `.insert`. We don't yet
        // guarantee deterministic compilation, so, if we happen to compile the
        // same contract concurrently on two threads, the `value`s might differ,
        // but this doesn't matter.
        update.set(DBCol::CachedContractCode, key.as_ref().to_vec(), value.try_to_vec().unwrap());
        self.db.write(update)
    }

    fn get(&self, key: &CryptoHash) -> io::Result<Option<CompiledContract>> {
        match self.db.get_raw_bytes(DBCol::CachedContractCode, key.as_ref()) {
            Ok(Some(bytes)) => Ok(Some(CompiledContract::try_from_slice(&bytes)?)),
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn has(&self, key: &CryptoHash) -> io::Result<bool> {
        self.db.get_raw_bytes(DBCol::CachedContractCode, key.as_ref()).map(|entry| entry.is_some())
    }
}

#[cfg(test)]
mod tests {
    use near_primitives::hash::CryptoHash;

    use super::{DBCol, NodeStorage, Store};

    #[test]
    fn test_no_cache_disabled() {
        #[cfg(feature = "no_cache")]
        panic!("no cache is enabled");
    }

    fn test_clear_column(store: Store) {
        assert_eq!(store.get(DBCol::State, &[1]).unwrap(), None);
        {
            let mut store_update = store.store_update();
            store_update.increment_refcount(DBCol::State, &[1], &[1]);
            store_update.increment_refcount(DBCol::State, &[2], &[2]);
            store_update.increment_refcount(DBCol::State, &[3], &[3]);
            store_update.commit().unwrap();
        }
        assert_eq!(store.get(DBCol::State, &[1]).unwrap().as_deref(), Some(&[1][..]));
        {
            let mut store_update = store.store_update();
            store_update.delete_all(DBCol::State);
            store_update.commit().unwrap();
        }
        assert_eq!(store.get(DBCol::State, &[1]).unwrap(), None);
    }

    #[test]
    fn clear_column_rocksdb() {
        let (_tmp_dir, opener) = NodeStorage::test_opener();
        test_clear_column(opener.open().unwrap().get_hot_store());
    }

    #[test]
    fn clear_column_testdb() {
        test_clear_column(crate::test_utils::create_test_store());
    }

    /// Asserts that elements in the vector are sorted.
    #[track_caller]
    fn assert_sorted(want_count: usize, keys: Vec<Box<[u8]>>) {
        assert_eq!(want_count, keys.len());
        for (pos, pair) in keys.windows(2).enumerate() {
            let (fst, snd) = (&pair[0], &pair[1]);
            assert!(fst <= snd, "{fst:?} > {snd:?} at {pos}");
        }
    }

    /// Checks that keys are sorted when iterating.
    fn test_iter_order_impl(store: Store) {
        use rand::Rng;

        // An arbitrary non-rc non-insert-only column we can write data into.
        const COLUMN: DBCol = DBCol::RecentOutboundConnections;
        assert!(!COLUMN.is_rc());
        assert!(!COLUMN.is_insert_only());

        const COUNT: usize = 10_000;
        const PREFIXES: [[u8; 4]; 6] =
            [*b"foo0", *b"foo1", *b"foo2", *b"foo\xff", *b"fop\0", *b"\xff\xff\xff\xff"];

        // Fill column with random keys.  We're inserting multiple sets of keys
        // with different four-byte prefixes..  Each set is `COUNT` keys (for
        // total of `PREFIXES.len()*COUNT` keys).
        let mut rng: rand::rngs::StdRng = rand::SeedableRng::seed_from_u64(0x3243f6a8885a308d);
        let mut update = store.store_update();
        let mut buf = [0u8; 20];
        for prefix in PREFIXES.iter() {
            buf[..prefix.len()].clone_from_slice(prefix);
            for _ in 0..COUNT {
                rng.fill(&mut buf[prefix.len()..]);
                update.set(COLUMN, &buf, &buf);
            }
        }
        update.commit().unwrap();

        fn collect<'a>(iter: crate::db::DBIterator<'a>) -> Vec<Box<[u8]>> {
            iter.map(Result::unwrap).map(|(key, _)| key).collect()
        }

        // Check that full scan produces keys in proper order.
        assert_sorted(PREFIXES.len() * COUNT, collect(store.iter(COLUMN)));
        assert_sorted(PREFIXES.len() * COUNT, collect(store.iter_raw_bytes(COLUMN)));
        assert_sorted(PREFIXES.len() * COUNT, collect(store.iter_prefix(COLUMN, b"")));

        // Check that prefix scan produces keys in proper order.
        for prefix in PREFIXES.iter() {
            let keys = collect(store.iter_prefix(COLUMN, prefix));
            for (pos, key) in keys.iter().enumerate() {
                assert_eq!(
                    prefix,
                    &key[0..4],
                    "Expected {prefix:?} prefix but got {key:?} key at {pos}"
                );
            }
            assert_sorted(COUNT, keys);
        }
    }

    #[test]
    fn rocksdb_iter_order() {
        let (_tempdir, opener) = NodeStorage::test_opener();
        test_iter_order_impl(opener.open().unwrap().get_hot_store());
    }

    #[test]
    fn testdb_iter_order() {
        test_iter_order_impl(crate::test_utils::create_test_store());
    }

    /// Check StoreCompiledContractCache implementation.
    #[test]
    fn test_store_compiled_contract_cache() {
        use near_vm_runner::logic::{CompiledContract, CompiledContractCache};
        use std::str::FromStr;

        let store = crate::test_utils::create_test_store();
        let cache = super::StoreCompiledContractCache::new(&store);
        let key = CryptoHash::from_str("75pAU4CJcp8Z9eoXcL6pSU8sRK5vn3NEpgvUrzZwQtr3").unwrap();

        assert_eq!(None, cache.get(&key).unwrap());
        assert_eq!(false, cache.has(&key).unwrap());

        let record = CompiledContract::Code(b"foo".to_vec());
        cache.put(&key, record.clone()).unwrap();
        assert_eq!(Some(record), cache.get(&key).unwrap());
        assert_eq!(true, cache.has(&key).unwrap());
    }

    /// Check saving and reading columns to/from a file.
    #[test]
    fn test_save_to_file() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();

        {
            let store = crate::test_utils::create_test_store();
            let mut store_update = store.store_update();
            store_update.increment_refcount(DBCol::State, &[1], &[1]);
            store_update.increment_refcount(DBCol::State, &[2], &[2]);
            store_update.increment_refcount(DBCol::State, &[2], &[2]);
            store_update.commit().unwrap();
            store.save_state_to_file(tmp.path()).unwrap();
        }

        // Verify expected encoding.
        {
            let mut buffer = Vec::new();
            std::io::Read::read_to_end(tmp.as_file_mut(), &mut buffer).unwrap();
            #[rustfmt::skip]
            assert_eq!(&[
                /* column: */ 0, /* key len: */ 1, 0, 0, 0, /* key: */ 1,
                                 /* val len: */ 9, 0, 0, 0, /* val: */ 1, 1, 0, 0, 0, 0, 0, 0, 0,
                /* column: */ 0, /* key len: */ 1, 0, 0, 0, /* key: */ 2,
                                 /* val len: */ 9, 0, 0, 0, /* val: */ 2, 2, 0, 0, 0, 0, 0, 0, 0,
                /* end mark: */ 255,
            ][..], buffer.as_slice());
        }

        {
            // Fresh storage, should have no data.
            let store = crate::test_utils::create_test_store();
            assert_eq!(None, store.get(DBCol::State, &[1]).unwrap());
            assert_eq!(None, store.get(DBCol::State, &[2]).unwrap());

            // Read data from file.
            store.load_state_from_file(tmp.path()).unwrap();
            assert_eq!(Some(&[1u8][..]), store.get(DBCol::State, &[1]).unwrap().as_deref());
            assert_eq!(Some(&[2u8][..]), store.get(DBCol::State, &[2]).unwrap().as_deref());

            // Key &[2] should have refcount of two so once decreased it should
            // still exist.
            let mut store_update = store.store_update();
            store_update.decrement_refcount(DBCol::State, &[1]);
            store_update.decrement_refcount(DBCol::State, &[2]);
            store_update.commit().unwrap();
            assert_eq!(None, store.get(DBCol::State, &[1]).unwrap());
            assert_eq!(Some(&[2u8][..]), store.get(DBCol::State, &[2]).unwrap().as_deref());
        }

        // Verify detection of corrupt file.
        let file = std::fs::File::options().write(true).open(tmp.path()).unwrap();
        let len = file.metadata().unwrap().len();
        file.set_len(len.saturating_sub(1)).unwrap();
        core::mem::drop(file);
        let store = crate::test_utils::create_test_store();
        assert_eq!(
            std::io::ErrorKind::InvalidInput,
            store.load_state_from_file(tmp.path()).unwrap_err().kind()
        );
    }
}

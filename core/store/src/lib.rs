use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use std::{fmt, io};

use borsh::{BorshDeserialize, BorshSerialize};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use once_cell::sync::Lazy;

pub use columns::DBCol;
pub use db::{
    CHUNK_TAIL_KEY, FINAL_HEAD_KEY, FORK_TAIL_KEY, HEADER_HEAD_KEY, HEAD_KEY,
    LARGEST_TARGET_HEIGHT_KEY, LATEST_KNOWN_KEY, TAIL_KEY,
};
use near_crypto::PublicKey;
use near_o11y::pretty;
use near_primitives::account::{AccessKey, Account};
use near_primitives::contract::ContractCode;
pub use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{DelayedReceiptIndices, Receipt, ReceivedData};
pub use near_primitives::shard_layout::ShardUId;
use near_primitives::trie_key::{trie_key_parsers, TrieKey};
use near_primitives::types::{AccountId, CompiledContract, CompiledContractCache, StateRoot};

use crate::db::{
    refcount, DBIterator, DBOp, DBSlice, DBTransaction, Database, StoreStatistics,
    GENESIS_JSON_HASH_KEY, GENESIS_STATE_ROOTS_KEY,
};
pub use crate::trie::iterator::{TrieIterator, TrieTraversalItem};
pub use crate::trie::update::{TrieUpdate, TrieUpdateIterator, TrieUpdateValuePtr};
pub use crate::trie::{
    estimator, split_state, ApplyStatePartResult, KeyForStateChanges, KeyLookupMode, NibbleSlice,
    PartialStorage, PrefetchApi, PrefetchError, RawTrieNode, RawTrieNodeWithSize, ShardTries, Trie,
    TrieAccess, TrieCache, TrieCachingStorage, TrieChanges, TrieConfig, TrieDBStorage, TrieStorage,
    WrappedTrieChanges,
};
pub use flat_state::FlatStateDelta;

#[cfg(feature = "cold_store")]
pub mod cold_storage;
mod columns;
pub mod config;
pub mod db;
pub mod flat_state;
pub mod metadata;
mod metrics;
pub mod migrations;
mod opener;
mod sync_utils;
pub mod test_utils;
mod trie;

pub use crate::config::{Mode, StoreConfig};
pub use crate::opener::{StoreMigrator, StoreOpener, StoreOpenerError};

/// Specifies temperature of a storage.
///
/// Since currently only hot storage is implemented, this has only one variant.
/// In the future, certain parts of the code may need to access hot or cold
/// storage.  Specifically, querying an old block will require reading it from
/// the cold storage.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Temperature {
    Hot,
    #[cfg(feature = "cold_store")]
    Cold,
}

/// Node’s storage holding chain and all other necessary data.
///
/// The eventual goal is to implement cold storage at which point this structure
/// will provide interface to access hot and cold storage.  This is in contrast
/// to [`Store`] which will abstract access to only one of the temperatures of
/// the storage.
pub struct NodeStorage<D = crate::db::RocksDB> {
    hot_storage: Arc<dyn Database>,
    #[cfg(feature = "cold_store")]
    cold_storage: Option<Arc<crate::db::ColdDB<D>>>,
    #[cfg(not(feature = "cold_store"))]
    cold_storage: Option<std::convert::Infallible>,
    _phantom: PhantomData<D>,
}

/// Node’s single storage source.
///
/// Currently, this is somewhat equivalent to [`NodeStorage`] in that for given
/// note storage you can get only a single [`Store`] object.  This will change
/// as we implement cold storage in which case this structure will provide an
/// interface to access either hot or cold data.  At that point, [`NodeStorage`]
/// will map to one of two [`Store`] objects depending on the temperature of the
/// data.
#[derive(Clone)]
pub struct Store {
    storage: Arc<dyn Database>,
}

// Those are temporary.  While cold_store feature is stabilised, remove those
// type aliases and just use the type directly.
#[cfg(feature = "cold_store")]
pub type ColdConfig<'a> = Option<&'a StoreConfig>;
#[cfg(not(feature = "cold_store"))]
pub type ColdConfig<'a> = Option<std::convert::Infallible>;

impl NodeStorage {
    /// Initialises a new opener with given home directory and hot and cold
    /// store config.
    pub fn opener<'a>(
        home_dir: &std::path::Path,
        config: &'a StoreConfig,
        #[allow(unused_variables)] cold_config: ColdConfig<'a>,
    ) -> StoreOpener<'a> {
        StoreOpener::new(
            home_dir,
            config,
            #[cfg(feature = "cold_store")]
            cold_config,
        )
    }

    /// Constructs new object backed by given database.
    fn from_rocksdb(
        hot_storage: crate::db::RocksDB,
        #[cfg(feature = "cold_store")] cold_storage: Option<crate::db::RocksDB>,
        #[cfg(not(feature = "cold_store"))] cold_storage: Option<std::convert::Infallible>,
    ) -> Self {
        let hot_storage = Arc::new(hot_storage);
        #[cfg(feature = "cold_store")]
        let cold_storage = cold_storage
            .map(|cold_db| Arc::new(crate::db::ColdDB::new(hot_storage.clone(), cold_db)));
        #[cfg(not(feature = "cold_store"))]
        let cold_storage = cold_storage.map(|_| unreachable!());
        Self { hot_storage, cold_storage, _phantom: PhantomData {} }
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
        let opener = StoreOpener::new(
            dir.path(),
            &CONFIG,
            #[cfg(feature = "cold_store")]
            None,
        );
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
        Self { hot_storage: storage, cold_storage: None, _phantom: PhantomData {} }
    }
}

impl<D: Database + 'static> NodeStorage<D> {
    /// Returns storage for given temperature.
    ///
    /// Some data live only in hot and some only in cold storage (which is at
    /// the moment not implemented but is planned soon).  Hot data is anything
    /// at the head of the chain.  Cold data, if node is configured with split
    /// storage, is anything archival.
    ///
    /// Based on block in whose context database access are going to be made,
    /// you will either need to access hot or cold storage.  Temperature of the
    /// data is, simplifying slightly, determined based on height of the block.
    /// Anything above the tail of hot storage is hot and everything else is
    /// cold.
    pub fn get_store(&self, temp: Temperature) -> Store {
        match temp {
            Temperature::Hot => Store { storage: self.hot_storage.clone() },
            #[cfg(feature = "cold_store")]
            Temperature::Cold => Store { storage: self.cold_storage.as_ref().unwrap().clone() },
        }
    }

    /// Returns underlying database for given temperature.
    ///
    /// With (currently unimplemented) cold storage, this allows accessing
    /// underlying hot and cold databases directly bypassing any abstractions
    /// offered by [`NodeStorage`] or [`Store`] interfaces.
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
    pub fn _get_inner(&self, temp: Temperature) -> &Arc<dyn Database> {
        match temp {
            Temperature::Hot => &self.hot_storage,
            #[cfg(feature = "cold_store")]
            Temperature::Cold => todo!(),
        }
    }

    /// Returns underlying database for given temperature.
    ///
    /// This is like [`Self::get_inner`] but consumes `self` thus avoiding
    /// `Arc::clone`.
    pub fn into_inner(self, temp: Temperature) -> Arc<dyn Database> {
        match temp {
            Temperature::Hot => self.hot_storage,
            #[cfg(feature = "cold_store")]
            Temperature::Cold => self.cold_storage.unwrap(),
        }
    }
}

impl<D> NodeStorage<D> {
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
            #[cfg(feature = "cold_store")]
            metadata::DbKind::Hot | metadata::DbKind::Cold => unreachable!(),
        })
    }

    #[cfg(feature = "cold_store")]
    pub fn new_with_cold(hot: Arc<dyn Database>, cold: D) -> Self {
        Self {
            hot_storage: hot.clone(),
            cold_storage: Some(Arc::new(crate::db::ColdDB::<D>::new(hot, cold))),
            _phantom: PhantomData::<D> {},
        }
    }

    #[cfg(feature = "cold_store")]
    pub fn cold_db(&self) -> io::Result<&Arc<crate::db::ColdDB<D>>> {
        self.cold_storage
            .as_ref()
            .map_or(Err(io::Error::new(io::ErrorKind::NotFound, "ColdDB Not Found")), |c| Ok(c))
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
            key = %pretty::StorageKey(key),
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

    pub fn iter<'a>(&'a self, column: DBCol) -> DBIterator<'a> {
        self.storage.iter(column)
    }

    /// Fetches raw key/value pairs from the database.
    ///
    /// Practically, this means that for rc columns rc is included in the value.
    /// This method is a deliberate escape hatch, and shouldn't be used outside
    /// of auxilary code like migrations which wants to hack on the database
    /// directly.
    pub fn iter_raw_bytes<'a>(&'a self, column: DBCol) -> DBIterator<'a> {
        self.storage.iter_raw_bytes(column)
    }

    pub fn iter_prefix<'a>(&'a self, column: DBCol, key_prefix: &'a [u8]) -> DBIterator<'a> {
        self.storage.iter_prefix(column, key_prefix)
    }

    pub fn iter_prefix_ser<'a, T: BorshDeserialize>(
        &'a self,
        column: DBCol,
        key_prefix: &'a [u8],
    ) -> impl Iterator<Item = io::Result<(Box<[u8]>, T)>> + 'a {
        self.storage
            .iter_prefix(column, key_prefix)
            .map(|item| item.and_then(|(key, value)| Ok((key, T::try_from_slice(value.as_ref())?))))
    }

    pub fn save_to_file(&self, column: DBCol, filename: &Path) -> io::Result<()> {
        let file = File::create(filename)?;
        let mut file = BufWriter::new(file);
        for item in self.storage.iter_raw_bytes(column) {
            let (key, value) = item?;
            file.write_u32::<LittleEndian>(key.len() as u32)?;
            file.write_all(&key)?;
            file.write_u32::<LittleEndian>(value.len() as u32)?;
            file.write_all(&value)?;
        }
        Ok(())
    }

    pub fn load_from_file(&self, column: DBCol, filename: &Path) -> io::Result<()> {
        let file = File::open(filename)?;
        let mut file = BufReader::new(file);
        let mut transaction = DBTransaction::new();
        loop {
            let key_len = match file.read_u32::<LittleEndian>() {
                Ok(key_len) => key_len as usize,
                Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(err) => return Err(err),
            };
            let mut key = vec![0; key_len];
            file.read_exact(&mut key)?;

            let value_len = file.read_u32::<LittleEndian>()? as usize;
            let mut value = vec![0; value_len];
            file.read_exact(&mut value)?;

            transaction.set(column, key, value);
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

/// Keeps track of current changes to the database and can commit all of them to the database.
pub struct StoreUpdate {
    transaction: DBTransaction,
    storage: StoreUpdateStorage,
}

enum StoreUpdateStorage {
    DB(Arc<dyn Database>),
    Tries(ShardTries),
}

impl StoreUpdate {
    const ONE: std::num::NonZeroU32 = match std::num::NonZeroU32::new(1) {
        Some(num) => num,
        None => panic!(),
    };

    pub(crate) fn new(db: Arc<dyn Database>) -> Self {
        StoreUpdate { transaction: DBTransaction::new(), storage: StoreUpdateStorage::DB(db) }
    }

    pub fn new_with_tries(tries: ShardTries) -> Self {
        StoreUpdate { transaction: DBTransaction::new(), storage: StoreUpdateStorage::Tries(tries) }
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

    /// Sets reference to the trie to clear cache on the commit.
    ///
    /// Panics if shard_tries are already set to a different object.
    fn set_shard_tries(&mut self, tries: &ShardTries) {
        assert!(self.check_compatible_shard_tries(tries));
        self.storage = StoreUpdateStorage::Tries(tries.clone())
    }

    /// Merge another store update into this one.
    ///
    /// Panics if `self`’s and `other`’s storage are incompatible.
    pub fn merge(&mut self, other: StoreUpdate) {
        match other.storage {
            StoreUpdateStorage::Tries(tries) => {
                assert!(self.check_compatible_shard_tries(&tries));
                self.storage = StoreUpdateStorage::Tries(tries);
            }
            StoreUpdateStorage::DB(other_db) => {
                let self_db = match &self.storage {
                    StoreUpdateStorage::Tries(tries) => tries.get_db(),
                    StoreUpdateStorage::DB(db) => &db,
                };
                assert!(same_db(self_db, &other_db));
            }
        }
        self.transaction.merge(other.transaction)
    }

    /// Verifies that given shard tries are compatible with this object.
    ///
    /// The [`ShardTries`] object is compatible if a) this object’s storage is
    /// a database which is the same as tries’ one or b) this object’s storage
    /// is the given shard tries.
    fn check_compatible_shard_tries(&self, tries: &ShardTries) -> bool {
        match &self.storage {
            StoreUpdateStorage::DB(db) => same_db(&db, tries.get_db()),
            StoreUpdateStorage::Tries(our) => our.is_same(tries),
        }
    }

    pub fn update_cache(&self) -> io::Result<()> {
        if let StoreUpdateStorage::Tries(tries) = &self.storage {
            tries.update_cache(&self.transaction)
        } else {
            Ok(())
        }
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
                        DBOp::UpdateRefcount { .. } | DBOp::DeleteAll { .. } => None,
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
                    tracing::trace!(target: "store", db_op = "insert", col = %col, key = %pretty::StorageKey(key), size = value.len())
                }
                DBOp::Set { col, key, value } => {
                    tracing::trace!(target: "store", db_op = "set", col = %col, key = %pretty::StorageKey(key), size = value.len())
                }
                DBOp::UpdateRefcount { col, key, value } => {
                    tracing::trace!(target: "store", db_op = "update_rc", col = %col, key = %pretty::StorageKey(key), size = value.len())
                }
                DBOp::Delete { col, key } => {
                    tracing::trace!(target: "store", db_op = "delete", col = %col, key = %pretty::StorageKey(key))
                }
                DBOp::DeleteAll { col } => {
                    tracing::trace!(target: "store", db_op = "delete_all", col = %col)
                }
            }
        }
        let storage = match &self.storage {
            StoreUpdateStorage::Tries(tries) => {
                tries.update_cache(&self.transaction)?;
                tries.get_db()
            }
            StoreUpdateStorage::DB(db) => &db,
        };
        storage.write(self.transaction)
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
                DBOp::Insert { col, key, .. } => {
                    writeln!(f, "  + {col} {}", pretty::StorageKey(key))?
                }
                DBOp::Set { col, key, .. } => writeln!(f, "  = {col} {}", pretty::StorageKey(key))?,
                DBOp::UpdateRefcount { col, key, .. } => {
                    writeln!(f, "  ± {col} {}", pretty::StorageKey(key))?
                }
                DBOp::Delete { col, key } => writeln!(f, "  - {col} {}", pretty::StorageKey(key))?,
                DBOp::DeleteAll { col } => writeln!(f, "  - {col} (all)")?,
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

    use super::{DBCol, NodeStorage, Store, Temperature};

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
        test_clear_column(opener.open().unwrap().get_store(Temperature::Hot));
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
        const COLUMN: DBCol = DBCol::Peers;
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
        test_iter_order_impl(opener.open().unwrap().get_store(Temperature::Hot));
    }

    #[test]
    fn testdb_iter_order() {
        test_iter_order_impl(crate::test_utils::create_test_store());
    }

    /// Check StoreCompiledContractCache implementation.
    #[test]
    fn test_store_compiled_contract_cache() {
        use near_primitives::types::{CompiledContract, CompiledContractCache};
        use std::str::FromStr;

        let store = crate::test_utils::create_test_store();
        let cache = super::StoreCompiledContractCache::new(&store);
        let key = CryptoHash::from_str("75pAU4CJcp8Z9eoXcL6pSU8sRK5vn3NEpgvUrzZwQtr3").unwrap();

        assert_eq!(None, cache.get(&key).unwrap());
        assert_eq!(false, cache.has(&key).unwrap());

        let record = CompiledContract::Code(b"foo".to_vec());
        assert_eq!((), cache.put(&key, record.clone()).unwrap());
        assert_eq!(Some(record), cache.get(&key).unwrap());
        assert_eq!(true, cache.has(&key).unwrap());
    }
}

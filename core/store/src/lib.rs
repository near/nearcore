use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
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
use near_primitives::account::{AccessKey, Account};
use near_primitives::contract::ContractCode;
pub use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{DelayedReceiptIndices, Receipt, ReceivedData};
use near_primitives::serialize::to_base;
pub use near_primitives::shard_layout::ShardUId;
use near_primitives::trie_key::{trie_key_parsers, TrieKey};
use near_primitives::types::{AccountId, CompiledContractCache, StateRoot};

use crate::db::{
    refcount, DBIterator, DBOp, DBTransaction, Database, RocksDB, StoreStatistics,
    GENESIS_JSON_HASH_KEY, GENESIS_STATE_ROOTS_KEY,
};
pub use crate::trie::iterator::TrieIterator;
pub use crate::trie::update::{TrieUpdate, TrieUpdateIterator, TrieUpdateValuePtr};
pub use crate::trie::{
    estimator, split_state, ApplyStatePartResult, KeyForStateChanges, PartialStorage, ShardTries,
    Trie, TrieAccess, TrieCache, TrieCacheFactory, TrieCachingStorage, TrieChanges, TrieStorage,
    WrappedTrieChanges,
};

mod columns;
mod config;
pub mod db;
pub mod flat_state;
mod metrics;
pub mod migrations;
pub mod test_utils;
mod trie;

pub use crate::config::{Mode, StoreConfig, StoreOpener};

#[derive(Clone)]
pub struct Store {
    storage: Arc<dyn Database>,
}

impl Store {
    /// Initialises a new opener with given home directory and store config.
    pub fn opener<'a>(home_dir: &std::path::Path, config: &'a StoreConfig) -> StoreOpener<'a> {
        StoreOpener::new(home_dir, config)
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
        let opener = Self::opener(dir.path(), &CONFIG);
        (dir, opener)
    }

    pub(crate) fn new(storage: Arc<dyn Database>) -> Store {
        Store { storage }
    }

    pub fn into_inner(self) -> Arc<dyn Database> {
        self.storage
    }

    pub fn get(&self, column: DBCol, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let value = self
            .storage
            .get_raw_bytes(column, key)
            .map(|result| refcount::get_with_rc_logic(column, result))?;
        tracing::trace!(
            target: "store",
            db_op = "get",
            col = %column,
            key = %to_base(key),
            size = value.as_ref().map(Vec::len)
        );
        Ok(value)
    }

    pub fn get_ser<T: BorshDeserialize>(&self, column: DBCol, key: &[u8]) -> io::Result<Option<T>> {
        match self.get(column, key)? {
            Some(bytes) => Ok(Some(T::try_from_slice(&bytes)?)),
            None => Ok(None),
        }
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

    pub fn get_store_statistics(&self) -> Option<StoreStatistics> {
        self.storage.get_store_statistics()
    }
}

/// Keeps track of current changes to the database and can commit all of them to the database.
pub struct StoreUpdate {
    storage: Arc<dyn Database>,
    transaction: DBTransaction,
    /// Optionally has reference to the trie to clear cache on the commit.
    shard_tries: Option<ShardTries>,
}

impl StoreUpdate {
    const ONE: std::num::NonZeroU32 = match std::num::NonZeroU32::new(1) {
        Some(num) => num,
        None => panic!(),
    };

    pub(crate) fn new(storage: Arc<dyn Database>) -> Self {
        StoreUpdate { storage, transaction: DBTransaction::new(), shard_tries: None }
    }

    pub fn new_with_tries(tries: ShardTries) -> Self {
        StoreUpdate {
            storage: Arc::clone(&tries.get_store().storage),
            transaction: DBTransaction::new(),
            shard_tries: Some(tries),
        }
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
    /// modifications are allowed, and extra care must be taken to aviod
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
    pub fn set_ser<T: BorshSerialize>(
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

    /// Set shard_tries to given object.
    ///
    /// Panics if shard_tries are already set to a different object.
    fn set_shard_tries(&mut self, tries: &ShardTries) {
        if let Some(our) = &self.shard_tries {
            assert!(tries.is_same(our));
        } else {
            self.shard_tries = Some(tries.clone());
        }
    }

    /// Merge another store update into this one.
    ///
    /// Panics if `self` and `other` both have shard_tries set to different
    /// objects.
    pub fn merge(&mut self, other: StoreUpdate) {
        match (&self.shard_tries, other.shard_tries) {
            (Some(our), Some(other)) => assert!(our.is_same(&other)),
            (None, other) => self.shard_tries = other,
            _ => (),
        }
        self.transaction.merge(other.transaction)
    }

    pub fn update_cache(&self) -> io::Result<()> {
        if let Some(tries) = &self.shard_tries {
            // Note: avoid comparing wide pointers here to work-around
            // https://github.com/rust-lang/rust/issues/69757
            let addr = |arc| Arc::as_ptr(arc) as *const u8;
            assert_eq!(addr(&tries.get_store().storage), addr(&self.storage),);
            tries.update_cache(&self.transaction)?;
        }
        Ok(())
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
        self.update_cache()?;
        let _span = tracing::trace_span!(target: "store", "commit").entered();
        for op in &self.transaction.ops {
            match op {
                DBOp::Insert { col, key, value } => {
                    tracing::trace!(target: "store", db_op = "insert", col = %col, key =  %to_base(key), size = value.len())
                }
                DBOp::Set { col, key, value } => {
                    tracing::trace!(target: "store", db_op = "set", col = %col, key =  %to_base(key), size = value.len())
                }
                DBOp::UpdateRefcount { col, key, value } => {
                    tracing::trace!(target: "store", db_op = "update_rc", col = %col, key =  %to_base(key), size = value.len())
                }
                DBOp::Delete { col, key } => {
                    tracing::trace!(target: "store", db_op = "delete", col = %col, key =  %to_base(key))
                }
                DBOp::DeleteAll { col } => {
                    tracing::trace!(target: "store", db_op = "delete_all", col = %col)
                }
            }
        }
        self.storage.write(self.transaction)
    }
}

impl fmt::Debug for StoreUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Store Update {{")?;
        for op in self.transaction.ops.iter() {
            match op {
                DBOp::Insert { col, key, .. } => writeln!(f, "  + {col} {}", to_base(key))?,
                DBOp::Set { col, key, .. } => writeln!(f, "  = {col} {}", to_base(key))?,
                DBOp::UpdateRefcount { col, key, .. } => writeln!(f, "  ± {col} {}", to_base(key))?,
                DBOp::Delete { col, key } => writeln!(f, "  - {col} {}", to_base(key))?,
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

pub fn set_genesis_state_roots(store_update: &mut StoreUpdate, genesis_roots: &Vec<StateRoot>) {
    store_update
        .set_ser::<Vec<StateRoot>>(DBCol::BlockMisc, GENESIS_STATE_ROOTS_KEY, genesis_roots)
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
    fn put(&self, key: &CryptoHash, value: Vec<u8>) -> io::Result<()> {
        let mut update = crate::db::DBTransaction::new();
        // We intentionally use `.set` here, rather than `.insert`. We don't yet
        // guarantee deterministic compilation, so, if we happen to compile the
        // same contract concurrently on two threads, the `value`s might differ,
        // but this doesn't matter.
        update.set(DBCol::CachedContractCode, key.as_ref().to_vec(), value);
        self.db.write(update)
    }

    fn get(&self, key: &CryptoHash) -> io::Result<Option<Vec<u8>>> {
        self.db.get_raw_bytes(DBCol::CachedContractCode, key.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use near_primitives::hash::CryptoHash;

    use super::{DBCol, Store};

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
        assert_eq!(store.get(DBCol::State, &[1]).unwrap(), Some(vec![1]));
        {
            let mut store_update = store.store_update();
            store_update.delete_all(DBCol::State);
            store_update.commit().unwrap();
        }
        assert_eq!(store.get(DBCol::State, &[1]).unwrap(), None);
    }

    #[test]
    fn clear_column_rocksdb() {
        let (_tmp_dir, opener) = Store::test_opener();
        test_clear_column(opener.open());
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
        test_iter_order_impl(Store::test_opener().1.open());
    }

    #[test]
    fn testdb_iter_order() {
        test_iter_order_impl(crate::test_utils::create_test_store());
    }

    /// Check StoreCompiledContractCache implementation.
    #[test]
    fn test_store_compiled_contract_cache() {
        use near_primitives::types::CompiledContractCache;
        use std::str::FromStr;

        let store = crate::test_utils::create_test_store();
        let cache = super::StoreCompiledContractCache::new(&store);
        let key = CryptoHash::from_str("75pAU4CJcp8Z9eoXcL6pSU8sRK5vn3NEpgvUrzZwQtr3").unwrap();
        assert_eq!(None, cache.get(&key).unwrap());
        assert_eq!((), cache.put(&key, b"foo".to_vec()).unwrap());
        assert_eq!(Some(&b"foo"[..]), cache.get(&key).unwrap().as_deref());
    }
}

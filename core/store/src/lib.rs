use near_cache::CellLruCache;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::{fmt, io};

use borsh::{BorshDeserialize, BorshSerialize};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

pub use columns::DBCol;
pub use db::{
    CHUNK_TAIL_KEY, FINAL_HEAD_KEY, FORK_TAIL_KEY, HEADER_HEAD_KEY, HEAD_KEY,
    LARGEST_TARGET_HEIGHT_KEY, LATEST_KNOWN_KEY, TAIL_KEY,
};
use near_crypto::PublicKey;
use near_o11y::log_assert;
use near_primitives::account::{AccessKey, Account};
use near_primitives::contract::ContractCode;
pub use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{DelayedReceiptIndices, Receipt, ReceivedData};
use near_primitives::serialize::to_base;
pub use near_primitives::shard_layout::ShardUId;
use near_primitives::trie_key::{trie_key_parsers, TrieKey};
use near_primitives::types::{AccountId, CompiledContractCache, StateRoot};

pub use crate::db::refcount::decode_value_with_rc;
use crate::db::refcount::encode_value_with_rc;
use crate::db::{
    DBOp, DBTransaction, Database, RocksDB, StoreStatistics, GENESIS_JSON_HASH_KEY,
    GENESIS_STATE_ROOTS_KEY,
};
pub use crate::trie::iterator::TrieIterator;
pub use crate::trie::update::{TrieUpdate, TrieUpdateIterator, TrieUpdateValuePtr};
pub use crate::trie::{
    estimator, split_state, ApplyStatePartResult, KeyForStateChanges, PartialStorage, ShardTries,
    Trie, TrieCache, TrieCachingStorage, TrieChanges, TrieStorage, WrappedTrieChanges,
};

mod columns;
mod config;
pub mod db;
mod metrics;
pub mod migrations;
pub mod test_utils;
mod trie;

pub use crate::config::{get_store_path, StoreConfig, StoreOpener};

#[derive(Clone)]
pub struct Store {
    storage: Arc<dyn Database>,
}

impl Store {
    pub(crate) fn new(storage: Arc<dyn Database>) -> Store {
        Store { storage }
    }

    pub fn get(&self, column: DBCol, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.storage.get(column, key).map_err(io::Error::from)
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

    pub fn iter<'a>(
        &'a self,
        column: DBCol,
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        self.storage.iter(column)
    }

    /// Fetches raw key/value pairs from the database.
    ///
    /// Practically, this means that for rc columns rc is included in the value.
    /// This method is a deliberate escape hatch, and shouldn't be used outside
    /// of auxilary code like migrations which wants to hack on the database
    /// directly.
    pub fn iter_raw_bytes<'a>(
        &'a self,
        column: DBCol,
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        self.storage.iter_raw_bytes(column)
    }

    pub fn iter_prefix<'a>(
        &'a self,
        column: DBCol,
        key_prefix: &'a [u8],
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        self.storage.iter_prefix(column, key_prefix)
    }

    pub fn iter_prefix_ser<'a, T: BorshDeserialize>(
        &'a self,
        column: DBCol,
        key_prefix: &'a [u8],
    ) -> impl Iterator<Item = io::Result<(Box<[u8]>, T)>> + 'a {
        self.storage
            .iter_prefix(column, key_prefix)
            .map(|(key, value)| Ok((key, T::try_from_slice(value.as_ref())?)))
    }

    pub fn save_to_file(&self, column: DBCol, filename: &Path) -> io::Result<()> {
        let file = File::create(filename)?;
        let mut file = BufWriter::new(file);
        for (key, value) in self.storage.iter_raw_bytes(column) {
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
        let mut transaction = self.storage.transaction();
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
        self.storage.write(transaction).map_err(io::Error::from)
    }

    pub fn get_rocksdb(&self) -> Option<&RocksDB> {
        self.storage.as_rocksdb()
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
    tries: Option<ShardTries>,
}

impl StoreUpdate {
    pub(crate) fn new(storage: Arc<dyn Database>) -> Self {
        let transaction = storage.transaction();
        StoreUpdate { storage, transaction, tries: None }
    }

    pub fn new_with_tries(tries: ShardTries) -> Self {
        let storage = Arc::clone(&tries.get_store().storage);
        let transaction = storage.transaction();
        StoreUpdate { storage, transaction, tries: Some(tries) }
    }

    /// Inserts a new value into the database.
    ///
    /// It is a programming error if `insert` overwrites an existing, different
    /// value. Use it for insert-only columns.
    pub fn insert(&mut self, column: DBCol, key: &[u8], value: &[u8]) {
        assert!(column.is_insert_only(), "can't insert: {column:?}");
        self.transaction.insert(column, key.to_vec(), value.to_vec())
    }

    pub fn insert_ser<T: BorshSerialize>(
        &mut self,
        column: DBCol,
        key: &[u8],
        value: &T,
    ) -> io::Result<()> {
        assert!(column.is_insert_only(), "can't insert_ser: {column:?}");
        let data = value.try_to_vec()?;
        self.insert(column, key, &data);
        Ok(())
    }

    /// Inserts a new reference-counted value into the database, or adjusts its
    /// reference count if it is already there.
    ///
    /// It is a programming error if `update_refcount` supplies a different
    /// value than the one stored in te database. Use it for rc columns.
    pub fn update_refcount(&mut self, column: DBCol, key: &[u8], value: &[u8], rc_delta: i64) {
        assert!(column.is_rc(), "can't update refcount: {column:?}");
        let value = encode_value_with_rc(value, rc_delta);
        self.transaction.update_refcount(column, key.to_vec(), value)
    }

    /// Modifies a value in the database.
    ///
    /// Unlike `insert` or `update_refcount`, arbitrary modifications are
    /// allowed, and extra care must be taken to aviod consistency anomalies.
    pub fn set(&mut self, column: DBCol, key: &[u8], value: &[u8]) {
        assert!(!(column.is_rc() || column.is_insert_only()), "can't set: {column:?}");
        self.transaction.set(column, key.to_vec(), value.to_vec())
    }

    /// Saves a BorshSerialized value.
    /// Must not be used for RC columns - please use 'update_refcount' instead.
    pub fn set_ser<T: BorshSerialize>(
        &mut self,
        column: DBCol,
        key: &[u8],
        value: &T,
    ) -> io::Result<()> {
        assert!(!(column.is_rc() || column.is_insert_only()), "can't set_ser: {column:?}");
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
        self.transaction.insert(column, key.to_vec(), value.to_vec())
    }

    /// Deletes the given key from the database.
    /// Must not be used for RC columns (use update_refcount instead).
    pub fn delete(&mut self, column: DBCol, key: &[u8]) {
        assert!(!column.is_rc(), "can't delete: {column:?}");
        self.transaction.delete(column, key.to_vec());
    }

    pub fn delete_all(&mut self, column: DBCol) {
        self.transaction.delete_all(column);
    }

    /// Merge another store update into this one.
    pub fn merge(&mut self, other: StoreUpdate) {
        match (&self.tries, other.tries) {
            (_, None) => (),
            (None, Some(tries)) => self.tries = Some(tries),
            (Some(t1), Some(t2)) => log_assert!(t1.is_same(&t2)),
        }

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
                        DBOp::UpdateRefcount { .. } | DBOp::DeleteAll { .. } => None,
                    })
                    .collect::<Vec<_>>();
                non_refcount_keys.len()
                    == non_refcount_keys.iter().collect::<std::collections::HashSet<_>>().len()
            },
            "Transaction overwrites itself: {:?}",
            self
        );
        if let Some(tries) = self.tries {
            // Note: avoid comparing wide pointers here to work-around
            // https://github.com/rust-lang/rust/issues/69757
            let addr = |arc| Arc::as_ptr(arc) as *const u8;
            assert_eq!(addr(&tries.get_store().storage), addr(&self.storage),);
            tries.update_cache(&self.transaction)?;
        }
        self.storage.write(self.transaction).map_err(io::Error::from)
    }
}

impl fmt::Debug for StoreUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Store Update {{")?;
        for op in self.transaction.ops.iter() {
            match op {
                DBOp::Insert { col, key, .. } => writeln!(f, "  + {:?} {}", col, to_base(key))?,
                DBOp::Set { col, key, .. } => writeln!(f, "  * {:?} {}", col, to_base(key))?,
                DBOp::UpdateRefcount { col, key, .. } => {
                    writeln!(f, "  +- {:?} {}", col, to_base(key))?
                }
                DBOp::Delete { col, key } => writeln!(f, "  - {:?} {}", col, to_base(key))?,
                DBOp::DeleteAll { col } => writeln!(f, "  delete all {:?}", col)?,
            }
        }
        writeln!(f, "}}")
    }
}

pub fn read_with_cache<'a, T: BorshDeserialize + Clone + 'a>(
    storage: &Store,
    col: DBCol,
    cache: &'a CellLruCache<Vec<u8>, T>,
    key: &[u8],
) -> io::Result<Option<T>> {
    if let Some(value) = cache.get(key) {
        return Ok(Some(value));
    }
    if let Some(result) = storage.get_ser::<T>(col, key)? {
        cache.put(key.to_vec(), result.clone());
        return Ok(Some(result));
    }
    Ok(None)
}

/// Reads an object from Trie.
/// # Errors
/// see StorageError
pub fn get<T: BorshDeserialize>(
    state_update: &TrieUpdate,
    key: &TrieKey,
) -> Result<Option<T>, StorageError> {
    match state_update.get(key)? {
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
    state_update: &TrieUpdate,
    account_id: &AccountId,
) -> Result<Option<Account>, StorageError> {
    get(state_update, &TrieKey::Account { account_id: account_id.clone() })
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
    state_update: &TrieUpdate,
    receiver_id: &AccountId,
    data_id: CryptoHash,
) -> Result<Option<ReceivedData>, StorageError> {
    get(state_update, &TrieKey::ReceivedData { receiver_id: receiver_id.clone(), data_id })
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
    state_update: &TrieUpdate,
    receiver_id: &AccountId,
    receipt_id: CryptoHash,
) -> Result<Option<Receipt>, StorageError> {
    get(state_update, &TrieKey::PostponedReceipt { receiver_id: receiver_id.clone(), receipt_id })
}

pub fn get_delayed_receipt_indices(
    state_update: &TrieUpdate,
) -> Result<DelayedReceiptIndices, StorageError> {
    Ok(get(state_update, &TrieKey::DelayedReceiptIndices)?.unwrap_or_default())
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
    state_update: &TrieUpdate,
    account_id: &AccountId,
    public_key: &PublicKey,
) -> Result<Option<AccessKey>, StorageError> {
    get(
        state_update,
        &TrieKey::AccessKey { account_id: account_id.clone(), public_key: public_key.clone() },
    )
}

pub fn get_access_key_raw(
    state_update: &TrieUpdate,
    raw_key: &[u8],
) -> Result<Option<AccessKey>, StorageError> {
    get(
        state_update,
        &trie_key_parsers::parse_trie_key_access_key_from_raw_key(raw_key)
            .expect("access key in the state should be correct"),
    )
}

pub fn set_code(state_update: &mut TrieUpdate, account_id: AccountId, code: &ContractCode) {
    state_update.set(TrieKey::ContractCode { account_id }, code.code().to_vec());
}

pub fn get_code(
    state_update: &TrieUpdate,
    account_id: &AccountId,
    code_hash: Option<CryptoHash>,
) -> Result<Option<ContractCode>, StorageError> {
    state_update
        .get(&TrieKey::ContractCode { account_id: account_id.clone() })
        .map(|opt| opt.map(|code| ContractCode::new(code, code_hash)))
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
    pub store: Store,
}

/// Cache for compiled contracts code using Store for keeping data.
/// We store contracts in VM-specific format in DBCol::CachedContractCode.
/// Key must take into account VM being used and its configuration, so that
/// we don't cache non-gas metered binaries, for example.
impl CompiledContractCache for StoreCompiledContractCache {
    fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let mut store_update = self.store.store_update();
        store_update.set(DBCol::CachedContractCode, key, value);
        store_update.commit()
    }

    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.store.get(DBCol::CachedContractCode, key)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_no_cache_disabled() {
        #[cfg(feature = "no_cache")]
        panic!("no cache is enabled");
    }
}

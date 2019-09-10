use std::sync::Arc;
use std::{fmt, io};

use borsh::{BorshDeserialize, BorshSerialize};
use cached::{Cached, SizedCache};
pub use kvdb::DBValue;
use kvdb::{DBOp, DBTransaction, KeyValueDB};
use kvdb_rocksdb::{Database, DatabaseConfig};

use near_crypto::PublicKey;
use near_primitives::account::{AccessKey, Account};
use near_primitives::contract::ContractCode;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{Receipt, ReceivedData};
use near_primitives::serialize::to_base;
use near_primitives::types::{AccountId, StorageUsage};
use near_primitives::utils::{
    key_for_access_key, key_for_account, key_for_code, key_for_postponed_receipt,
    key_for_received_data, prefix_for_access_key, prefix_for_data,
};

pub use crate::trie::{
    update::TrieUpdate, update::TrieUpdateIterator, PartialStorage, Trie, TrieChanges,
    TrieIterator, WrappedTrieChanges,
};

pub mod test_utils;
mod trie;

pub const COL_BLOCK_MISC: Option<u32> = Some(0);
pub const COL_BLOCK: Option<u32> = Some(1);
pub const COL_BLOCK_HEADER: Option<u32> = Some(2);
pub const COL_BLOCK_INDEX: Option<u32> = Some(3);
pub const COL_STATE: Option<u32> = Some(4);
pub const COL_CHUNK_EXTRA: Option<u32> = Some(5);
pub const COL_TRANSACTION_RESULT: Option<u32> = Some(6);
pub const COL_OUTGOING_RECEIPTS: Option<u32> = Some(7);
pub const COL_INCOMING_RECEIPTS: Option<u32> = Some(8);
pub const COL_PEERS: Option<u32> = Some(9);
pub const COL_EPOCH_INFO: Option<u32> = Some(10);
pub const COL_BLOCK_INFO: Option<u32> = Some(11);
pub const COL_CHUNKS: Option<u32> = Some(12);
pub const COL_CHUNK_ONE_PARTS: Option<u32> = Some(13);
/// Blocks for which chunks need to be applied after the state is downloaded for a particular epoch
pub const COL_BLOCKS_TO_CATCHUP: Option<u32> = Some(14);
/// Blocks for which the state is being downloaded
pub const COL_STATE_DL_INFOS: Option<u32> = Some(15);
pub const COL_CHALLENGED_BLOCKS: Option<u32> = Some(16);
const NUM_COLS: u32 = 17;

pub struct Store {
    storage: Arc<dyn KeyValueDB>,
}

impl Store {
    pub fn new(storage: Arc<dyn KeyValueDB>) -> Store {
        Store { storage }
    }

    pub fn get(&self, column: Option<u32>, key: &[u8]) -> Result<Option<Vec<u8>>, io::Error> {
        self.storage.get(column, key).map(|a| a.map(|b| b.to_vec()))
    }

    pub fn get_ser<T: BorshDeserialize>(
        &self,
        column: Option<u32>,
        key: &[u8],
    ) -> Result<Option<T>, io::Error> {
        match self.storage.get(column, key) {
            Ok(Some(bytes)) => match T::try_from_slice(bytes.as_ref()) {
                Ok(result) => Ok(Some(result)),
                Err(e) => Err(e),
            },
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn exists(&self, column: Option<u32>, key: &[u8]) -> Result<bool, io::Error> {
        self.storage.get(column, key).map(|value| value.is_some())
    }

    pub fn store_update(&self) -> StoreUpdate {
        StoreUpdate::new(self.storage.clone())
    }

    pub fn iter<'a>(
        &'a self,
        column: Option<u32>,
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        self.storage.iter(column)
    }
}

/// Keeps track of current changes to the database and can commit all of them to the database.
pub struct StoreUpdate {
    storage: Arc<dyn KeyValueDB>,
    transaction: DBTransaction,
    /// Optionally has reference to the trie to clear cache on the commit.
    trie: Option<Arc<Trie>>,
}

impl StoreUpdate {
    pub fn new(storage: Arc<dyn KeyValueDB>) -> Self {
        let transaction = storage.transaction();
        StoreUpdate { storage, transaction, trie: None }
    }

    pub fn new_with_trie(storage: Arc<dyn KeyValueDB>, trie: Arc<Trie>) -> Self {
        let transaction = storage.transaction();
        StoreUpdate { storage, transaction, trie: Some(trie) }
    }

    pub fn set(&mut self, column: Option<u32>, key: &[u8], value: &[u8]) {
        self.transaction.put(column, key, value)
    }

    pub fn set_ser<T: BorshSerialize>(
        &mut self,
        column: Option<u32>,
        key: &[u8],
        value: &T,
    ) -> Result<(), io::Error> {
        let data = value.try_to_vec()?;
        self.set(column, key, &data);
        Ok(())
    }

    pub fn delete(&mut self, column: Option<u32>, key: &[u8]) {
        self.transaction.delete(column, key);
    }

    /// Merge another store update into this one.
    pub fn merge(&mut self, other: StoreUpdate) {
        if self.trie.is_none() {
            if let Some(trie) = other.trie {
                self.trie = Some(trie);
            }
        }
        self.merge_transaction(other.transaction);
    }

    /// Merge DB Transaction.
    pub fn merge_transaction(&mut self, transaction: DBTransaction) {
        for op in transaction.ops {
            match op {
                DBOp::Insert { col, key, value } => self.transaction.put(col, &key, &value),
                DBOp::Delete { col, key } => self.transaction.delete(col, &key),
            }
        }
    }

    pub fn commit(self) -> Result<(), io::Error> {
        if let Some(trie) = self.trie {
            trie.update_cache(&self.transaction)?;
        }
        self.storage.write(self.transaction)
    }
}

impl fmt::Debug for StoreUpdate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Store Update {{")?;
        for op in self.transaction.ops.iter() {
            match op {
                DBOp::Insert { col, key, .. } => writeln!(f, "  + {:?} {}", col, to_base(key))?,
                DBOp::Delete { col, key } => writeln!(f, "  - {:?} {}", col, to_base(key))?,
            }
        }
        writeln!(f, "}}")
    }
}

pub fn read_with_cache<'a, T: BorshDeserialize + 'a>(
    storage: &Store,
    col: Option<u32>,
    cache: &'a mut SizedCache<Vec<u8>, T>,
    key: &[u8],
) -> io::Result<Option<&'a T>> {
    let key_vec = key.to_vec();
    if cache.cache_get(&key_vec).is_some() {
        return Ok(Some(cache.cache_get(&key_vec).unwrap()));
    }
    if let Some(result) = storage.get_ser(col, key)? {
        cache.cache_set(key.to_vec(), result);
        return Ok(cache.cache_get(&key_vec));
    }
    Ok(None)
}

pub fn create_store(path: &str) -> Arc<Store> {
    let db_config = DatabaseConfig::with_columns(Some(NUM_COLS));
    let db = Arc::new(Database::open(&db_config, path).expect("Failed to open the database"));
    Arc::new(Store::new(db))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    /// Key-value db internal failure
    StorageInternalError,
    /// Storage is PartialStorage and requested a missing trie node
    TrieNodeMissing,
    /// Either invalid state or key-value db is corrupted.
    /// For PartialStorage it cannot be corrupted.
    /// Error message is unreliable and for debugging purposes only. It's also probably ok to
    /// panic in every place that produces this error.
    /// We can check if db is corrupted by verifying everything in the state trie.
    StorageInconsistentState(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        f.write_str(&format!("{:?}", self))
    }
}

impl std::error::Error for StorageError {}

/// Reads an object from Trie.
/// # Errors
/// see StorageError
pub fn get<T: BorshDeserialize>(
    state_update: &TrieUpdate,
    key: &[u8],
) -> Result<Option<T>, StorageError> {
    state_update.get(key).and_then(|opt| {
        opt.map_or_else(
            || Ok(None),
            |data| {
                T::try_from_slice(&data)
                    .map_err(|_| {
                        StorageError::StorageInconsistentState("Failed to deserialize".to_string())
                    })
                    .map(Some)
            },
        )
    })
}

/// Writes an object into Trie.
pub fn set<T: BorshSerialize>(state_update: &mut TrieUpdate, key: Vec<u8>, value: &T) {
    value
        .try_to_vec()
        .ok()
        .map(|data| state_update.set(key, DBValue::from_vec(data)))
        .or_else(|| None);
}

/// Number of bytes account and all of it's other data occupies in the storage.
pub fn total_account_storage(_account_id: &AccountId, account: &Account) -> StorageUsage {
    account.storage_usage
}

pub fn set_account(state_update: &mut TrieUpdate, key: &AccountId, account: &Account) {
    set(state_update, key_for_account(key), account)
}

pub fn get_account(
    state_update: &TrieUpdate,
    key: &AccountId,
) -> Result<Option<Account>, StorageError> {
    get(state_update, &key_for_account(key))
}

pub fn set_received_data(
    state_update: &mut TrieUpdate,
    account_id: &AccountId,
    data_id: &CryptoHash,
    data: &ReceivedData,
) {
    set(state_update, key_for_received_data(account_id, data_id), data);
}

pub fn get_received_data(
    state_update: &TrieUpdate,
    account_id: &AccountId,
    data_id: &CryptoHash,
) -> Result<Option<ReceivedData>, StorageError> {
    get(state_update, &key_for_received_data(account_id, data_id))
}

pub fn set_receipt(state_update: &mut TrieUpdate, receipt: &Receipt) {
    let key = key_for_postponed_receipt(&receipt.receiver_id, &receipt.receipt_id);
    set(state_update, key, receipt);
}

pub fn get_receipt(
    state_update: &TrieUpdate,
    account_id: &AccountId,
    receipt_id: &CryptoHash,
) -> Result<Option<Receipt>, StorageError> {
    get(state_update, &key_for_postponed_receipt(account_id, receipt_id))
}

pub fn set_access_key(
    state_update: &mut TrieUpdate,
    account_id: &AccountId,
    public_key: &PublicKey,
    access_key: &AccessKey,
) {
    set(state_update, key_for_access_key(account_id, public_key), access_key);
}

pub fn get_access_key(
    state_update: &TrieUpdate,
    account_id: &AccountId,
    public_key: &PublicKey,
) -> Result<Option<AccessKey>, StorageError> {
    get(state_update, &key_for_access_key(account_id, public_key))
}

pub fn get_access_key_raw(
    state_update: &TrieUpdate,
    key: &[u8],
) -> Result<Option<AccessKey>, StorageError> {
    get(state_update, key)
}

pub fn set_code(state_update: &mut TrieUpdate, account_id: &AccountId, code: &ContractCode) {
    state_update.set(key_for_code(account_id), DBValue::from_vec(code.code.clone()));
}

pub fn get_code(
    state_update: &TrieUpdate,
    account_id: &AccountId,
) -> Result<Option<ContractCode>, StorageError> {
    state_update
        .get(&key_for_code(account_id))
        .map(|opt| opt.map(|code| ContractCode::new(code.to_vec())))
}

/// Removes account, code and all access keys associated to it.
pub fn remove_account(
    state_update: &mut TrieUpdate,
    account_id: &AccountId,
) -> Result<(), Box<dyn std::error::Error>> {
    state_update.remove(&key_for_account(account_id));
    state_update.remove(&key_for_code(account_id));
    state_update.remove_starts_with(&prefix_for_access_key(account_id))?;
    state_update.remove_starts_with(&prefix_for_data(account_id))?;
    Ok(())
}

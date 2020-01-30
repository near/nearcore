use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::{fmt, io};

use borsh::{BorshDeserialize, BorshSerialize};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use cached::{Cached, SizedCache};

pub use db::DBCol::{self, *};
use near_crypto::PublicKey;
use near_primitives::account::{AccessKey, Account};
use near_primitives::contract::ContractCode;
pub use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{Receipt, ReceivedData};
use near_primitives::serialize::to_base;
use near_primitives::types::{AccountId, StorageUsage};
use near_primitives::utils::{
    key_for_access_key, key_for_account, key_for_code, key_for_postponed_receipt,
    key_for_received_data, prefix_for_access_key, prefix_for_data,
};

use crate::db::{DBOp, DBTransaction, Database, RocksDB};
pub use crate::trie::{
    iterator::TrieIterator, update::PrefixKeyValueChanges, update::TrieUpdate,
    update::TrieUpdateIterator, update::TrieUpdateValuePtr, PartialStorage, Trie, TrieChanges,
    WrappedTrieChanges,
};

mod db;
pub mod test_utils;
mod trie;

pub struct Store {
    storage: Arc<dyn Database>,
}

impl Store {
    pub fn new(storage: Arc<dyn Database>) -> Store {
        Store { storage }
    }

    pub fn get(&self, column: DBCol, key: &[u8]) -> Result<Option<Vec<u8>>, io::Error> {
        self.storage.get(column, key).map_err(|e| e.into())
    }

    pub fn get_ser<T: BorshDeserialize>(
        &self,
        column: DBCol,
        key: &[u8],
    ) -> Result<Option<T>, io::Error> {
        match self.storage.get(column, key) {
            Ok(Some(bytes)) => match T::try_from_slice(bytes.as_ref()) {
                Ok(result) => Ok(Some(result)),
                Err(e) => Err(e),
            },
            Ok(None) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn exists(&self, column: DBCol, key: &[u8]) -> Result<bool, io::Error> {
        self.storage.get(column, key).map(|value| value.is_some()).map_err(|e| e.into())
    }

    pub fn store_update(&self) -> StoreUpdate {
        StoreUpdate::new(self.storage.clone())
    }

    pub fn iter<'a>(
        &'a self,
        column: DBCol,
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        self.storage.iter(column)
    }

    pub fn iter_prefix_ser<'a, T: BorshDeserialize>(
        &'a self,
        column: DBCol,
        key_prefix: &'a [u8],
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, T), io::Error>> + 'a> {
        Box::new(
            self.storage
                .iter_prefix(column, key_prefix)
                .map(|(key, value)| Ok((key.to_vec(), T::try_from_slice(value.as_ref())?))),
        )
    }

    pub fn save_to_file(&self, column: DBCol, filename: &Path) -> Result<(), std::io::Error> {
        let mut file = File::create(filename)?;
        for (key, value) in self.storage.iter(column) {
            file.write_u32::<LittleEndian>(key.len() as u32)?;
            file.write_all(&key)?;
            file.write_u32::<LittleEndian>(value.len() as u32)?;
            file.write_all(&value)?;
        }
        Ok(())
    }

    pub fn load_from_file(&self, column: DBCol, filename: &Path) -> Result<(), std::io::Error> {
        let mut file = File::open(filename)?;
        let mut transaction = self.storage.transaction();
        loop {
            let key_len = match file.read_u32::<LittleEndian>() {
                Ok(key_len) => key_len as usize,
                Err(ref err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(err) => return Err(err),
            };
            let mut key = Vec::<u8>::with_capacity(key_len);
            Read::by_ref(&mut file).take(key_len as u64).read_to_end(&mut key)?;
            let value_len = file.read_u32::<LittleEndian>()? as usize;
            let mut value = Vec::<u8>::with_capacity(value_len);
            Read::by_ref(&mut file).take(value_len as u64).read_to_end(&mut value)?;
            transaction.put(column, &key, &value);
        }
        self.storage.write(transaction).map_err(|e| e.into())
    }
}

/// Keeps track of current changes to the database and can commit all of them to the database.
pub struct StoreUpdate {
    storage: Arc<dyn Database>,
    transaction: DBTransaction,
    /// Optionally has reference to the trie to clear cache on the commit.
    trie: Option<Arc<Trie>>,
}

impl StoreUpdate {
    pub fn new(storage: Arc<dyn Database>) -> Self {
        let transaction = storage.transaction();
        StoreUpdate { storage, transaction, trie: None }
    }

    pub fn new_with_trie(storage: Arc<dyn Database>, trie: Arc<Trie>) -> Self {
        let transaction = storage.transaction();
        StoreUpdate { storage, transaction, trie: Some(trie) }
    }

    pub fn set(&mut self, column: DBCol, key: &[u8], value: &[u8]) {
        self.transaction.put(column, key, value)
    }

    pub fn set_ser<T: BorshSerialize>(
        &mut self,
        column: DBCol,
        key: &[u8],
        value: &T,
    ) -> Result<(), io::Error> {
        let data = value.try_to_vec()?;
        self.set(column, key, &data);
        Ok(())
    }

    pub fn delete(&mut self, column: DBCol, key: &[u8]) {
        self.transaction.delete(column, key);
    }

    /// Merge another store update into this one.
    pub fn merge(&mut self, other: StoreUpdate) {
        if self.trie.is_none() {
            if let Some(trie) = other.trie {
                assert_eq!(
                    trie.storage.as_caching_storage().unwrap().store.storage.as_ref() as *const _,
                    self.storage.as_ref() as *const _
                );
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
            assert_eq!(
                trie.storage.as_caching_storage().unwrap().store.storage.as_ref() as *const _,
                self.storage.as_ref() as *const _
            );
            trie.update_cache(&self.transaction)?;
        }
        self.storage.write(self.transaction).map_err(|e| e.into())
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
    col: DBCol,
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
    let db = Arc::new(RocksDB::new(path).expect("Failed to open the database"));
    Arc::new(Store::new(db))
}

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
    value.try_to_vec().ok().map(|data| state_update.set(key, data)).or_else(|| None);
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
    state_update.set(key_for_code(account_id), code.code.clone());
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
) -> Result<(), StorageError> {
    state_update.remove(&key_for_account(account_id));
    state_update.remove(&key_for_code(account_id));
    state_update.remove_starts_with(&prefix_for_access_key(account_id))?;
    state_update.remove_starts_with(&prefix_for_data(account_id))?;
    Ok(())
}

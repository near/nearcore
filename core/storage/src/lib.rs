extern crate hash256_std_hasher;
extern crate hash_db;
extern crate kvdb;
extern crate kvdb_memorydb;
extern crate kvdb_rocksdb;
extern crate primitives;
extern crate serde;
extern crate substrate_state_machine;
extern crate wasm;

#[cfg(test)]
extern crate hex_literal;
#[cfg(test)]
extern crate memory_db;

pub use kvdb::{DBValue, KeyValueDB};
use kvdb_rocksdb::{Database, DatabaseConfig};
use primitives::hash::CryptoHash;
use primitives::types::MerkleHash;
use std::sync::Arc;
use substrate_storage::{CryptoHasher, Externalities, OverlayedChanges, StateExt, TrieBackend, Backend};
pub use substrate_storage::TrieBackendTransaction;

mod substrate_storage;
pub mod test_utils;

pub const COL_STATE: Option<u32> = Some(0);
pub const COL_EXTRA: Option<u32> = Some(1);
pub const COL_BLOCKS: Option<u32> = Some(2);
pub const COL_HEADERS: Option<u32> = Some(3);
pub const COL_BLOCK_INDEX: Option<u32> = Some(4);
pub const TOTAL_COLUMNS: Option<u32> = Some(5);

/// Provides a way to access Storage and record changes with future commit.
pub struct StateDbUpdate<'a> {
    overlay: Box<OverlayedChanges>,
    _backend: Box<TrieBackend>,
    ext: StateExt<'a>,
}

impl<'a> StateDbUpdate<'a> {
    pub fn new(state_db: Arc<StateDb>, root: MerkleHash) -> Self {
        let backend = Box::new(TrieBackend::new(
            state_db as Arc<substrate_state_machine::Storage<CryptoHasher>>,
            root,
        ));
        let backend_ptr = backend.as_ref() as *const TrieBackend;
        let mut overlay = Box::new(OverlayedChanges::default());
        let overlay_ptr = overlay.as_mut() as *mut OverlayedChanges;
        StateDbUpdate {
            overlay,
            _backend: backend,
            ext: StateExt::new(unsafe { &mut *overlay_ptr }, unsafe { &*backend_ptr }, None),
        }
    }
    pub fn get(&self, key: &[u8]) -> Option<DBValue> {
        self.ext.storage(key).map(|v| DBValue::from_slice(&v))
    }
    pub fn set(&mut self, key: &[u8], value: &DBValue) {
        self.ext.place_storage(key.to_vec(), Some(value.to_vec()));
    }
    pub fn delete(&mut self, key: &[u8]) {
        self.ext.clear_storage(key);
    }
    pub fn for_keys_with_prefix<F: FnMut(&[u8])>(&self, prefix: &[u8], f: F) {
        self._backend.for_keys_with_prefix(prefix, f);
    }
    pub fn commit(&mut self) {
        self.overlay.commit_prospective();
    }
    pub fn rollback(&mut self) {
        self.overlay.discard_prospective();
    }
    pub fn finalize(mut self) -> (TrieBackendTransaction, MerkleHash) {
        let root_after = self.ext.storage_root();
        let (storage_transaction, _changes_trie_transaction) = self.ext.transaction();
        (storage_transaction, root_after)
    }
}

pub type Storage = KeyValueDB;
pub type DiskStorageConfig = DatabaseConfig;
pub type DiskStorage = Database;

#[allow(dead_code)]
pub struct StateDb {
    storage: Arc<KeyValueDB>,
    // TODO: for now.
    hashed_null_node: CryptoHash,
    null_node_data: DBValue,
}

impl StateDb {
    pub fn new(storage: Arc<KeyValueDB>) -> Self {
        StateDb {
            storage,
            hashed_null_node: CryptoHash::default(),
            null_node_data: [0u8][..].into(),
        }
    }
    pub fn commit(&self, transaction: &mut TrieBackendTransaction) -> std::io::Result<()> {
        let mut db_transaction = self.storage.transaction();
        for (k, (v, rc)) in transaction.drain() {
            if rc > 0 {
                db_transaction.put(COL_STATE, k.as_ref(), &v.to_vec());
            } else if rc < 0 {
                db_transaction.delete(COL_STATE, k.as_ref());
            }
        }
        self.storage.write(db_transaction)
    }
}

impl substrate_state_machine::Storage<CryptoHasher> for StateDb {
    fn get(&self, key: &CryptoHash) -> std::result::Result<Option<DBValue>, std::string::String> {
        // Initialize with empty trie. Alternative: insert (0,0) into KeyValueDB
        if *key == self.hashed_null_node {
            return Ok(Some(self.null_node_data.clone()));
        }
        self.storage
            .get(COL_STATE, key.as_ref())
            .map(|r| r.map(|v| DBValue::from_slice(&v)))
            .map_err(|e| format!("Database backend error: {:?}", e))
    }
}

pub fn open_database(storage_path: &str) -> Database {
    let storage_config = DiskStorageConfig::with_columns(TOTAL_COLUMNS);
    DiskStorage::open(&storage_config, storage_path).expect("Database wasn't open")
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_utils::create_state_db;

    #[test]
    fn state_db() {
        let state_db = Arc::new(create_state_db());
        let root = CryptoHash::default();
        let mut state_db_update = StateDbUpdate::new(state_db.clone(), root);
        state_db_update.set(b"dog", &DBValue::from_slice(b"puppy"));
        state_db_update.set(b"dog2", &DBValue::from_slice(b"puppy"));
        state_db_update.set(b"xxx", &DBValue::from_slice(b"puppy"));
        let (mut transaction, new_root) = state_db_update.finalize();
        state_db.commit(&mut transaction).ok();
        let state_db_update2 = StateDbUpdate::new(state_db.clone(), new_root);
        assert_eq!(state_db_update2.get(b"dog").unwrap(), DBValue::from_slice(b"puppy"));
        let mut values = vec![];
        state_db_update2.for_keys_with_prefix(b"dog", |key| { values.push(key.to_vec()) });
        assert_eq!(values, vec![b"dog".to_vec(), b"dog2".to_vec()]);
    }
}

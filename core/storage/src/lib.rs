extern crate bincode;
extern crate byteorder;
extern crate hash256_std_hasher;
extern crate hash_db;
#[cfg(test)]
extern crate hex_literal;
extern crate kvdb;
extern crate kvdb_memorydb;
extern crate kvdb_rocksdb;
#[cfg(test)]
extern crate memory_db;
extern crate parity_rocksdb;
extern crate parking_lot;
extern crate primitives;
extern crate serde;
extern crate substrate_primitives;
extern crate substrate_state_machine;
extern crate substrate_trie;
extern crate trie_db;

use std::sync::Arc;

pub use kvdb::{DBValue, KeyValueDB};
pub use kvdb_memorydb::create as create_memory_db;
use kvdb_memorydb::InMemory;
use kvdb_rocksdb::{Database, DatabaseConfig};

use primitives::hash::CryptoHash;
use primitives::types::MerkleHash;
use substrate_storage::{CryptoHasher, Externalities, OverlayedChanges, StateExt, TrieBackend};
pub use substrate_storage::TrieBackendTransaction;

#[cfg(test)]
mod tests;
mod substrate_storage;

pub const COL_STATE: u32 = 0;
pub const COL_BEACON_EXTRA: u32 = 1;
pub const COL_BEACON_BLOCKS: u32 = 2;
pub const COL_BEACON_HEADERS: u32 = 3;
pub const COL_BEACON_INDEX: u32 = 4;


/// Provides a way to access Storage and record changes with future commit.
pub struct StateDbUpdate<'a> {
    overlay: Box<OverlayedChanges>,
    _backend: Box<TrieBackend>,
    ext: StateExt<'a>,
}

impl<'a> StateDbUpdate<'a> {
    pub fn new(state_db: Arc<StateDb>, root: MerkleHash) -> Self {
        let backend = Box::new(TrieBackend::new(state_db as Arc<substrate_state_machine::Storage<CryptoHasher>>, root));
        let backend_ptr = backend.as_ref() as *const TrieBackend;
        let mut overlay = Box::new(OverlayedChanges::default());
        let overlay_ptr = overlay.as_mut() as *mut OverlayedChanges;
        StateDbUpdate {
            overlay,
            _backend: backend,
            ext: StateExt::new(unsafe {&mut *overlay_ptr}, unsafe { &*backend_ptr }, None)
        }
    }
    pub fn get(&self, key: &[u8]) -> Option<DBValue> {
        self.ext.storage(key).map(|v| DBValue::from_slice(&v))
    }
    pub fn set(&mut self, key: &[u8], value: DBValue) {
        self.ext.place_storage(key.to_vec(), Some(value.to_vec()));
    }
    pub fn delete(&mut self, key: &[u8]) {
        self.ext.clear_storage(key);
    }
    pub fn commit(&mut self) {
        self.overlay.commit_prospective();
    }
    pub fn rollback(&mut self) {
        self.overlay.discard_prospective();
    }
    pub fn finalize(mut self) -> (TrieBackendTransaction, MerkleHash) {
        let root_after = self.ext.storage_root();
        println!("! {:?}", root_after);
        let (storage_transaction, _changes_trie_transaction) = self.ext.transaction();
        (storage_transaction, root_after)
    }
}

pub type Storage = KeyValueDB;
pub type MemoryStorage = InMemory;
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
                db_transaction.put(Some(COL_STATE), k.as_ref(), &v.to_vec());
            } else if rc < 0 {
                db_transaction.delete(Some(COL_STATE), k.as_ref());
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
        self.storage.get(Some(COL_STATE), key.as_ref())
            .map(|r| r.map(|v| DBValue::from_slice(&v)))
            .map_err(|e| format!("Database backend error: {:?}", e))
    }
}

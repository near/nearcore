use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use cached::{Cached, SizedCache};

use near_primitives::hash::CryptoHash;

use crate::trie::{decode_trie_node_with_rc, POISONED_LOCK_ERR};
use crate::{ColState, StorageError, Store};

pub trait TrieStorage: Send + Sync {
    /// Get bytes of a serialized TrieNode.
    /// # Errors
    /// StorageError if the storage fails internally or the hash is not present.
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Vec<u8>, StorageError>;

    fn as_caching_storage(&self) -> Option<&TrieCachingStorage> {
        None
    }

    fn as_recording_storage(&self) -> Option<&TrieRecordingStorage> {
        None
    }

    fn as_partial_storage(&self) -> Option<&TrieMemoryPartialStorage> {
        None
    }
}

pub struct TrieRecordingStorage {
    pub(crate) storage: TrieCachingStorage,
    pub(crate) recorded: Arc<Mutex<HashMap<CryptoHash, Vec<u8>>>>,
}

impl TrieStorage for TrieRecordingStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Vec<u8>, StorageError> {
        let result = self.storage.retrieve_raw_bytes(hash);
        if let Ok(val) = &result {
            self.recorded.lock().expect(POISONED_LOCK_ERR).insert(*hash, val.clone());
        }
        result
    }

    fn as_recording_storage(&self) -> Option<&TrieRecordingStorage> {
        Some(self)
    }
}

/// Storage for validating recorded partial storage.
/// visited_nodes are to validate that partial storage doesn't contain unnecessary nodes.
pub struct TrieMemoryPartialStorage {
    pub(crate) recorded_storage: HashMap<CryptoHash, Vec<u8>>,
    pub(crate) visited_nodes: Arc<Mutex<HashSet<CryptoHash>>>,
}

impl TrieStorage for TrieMemoryPartialStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Vec<u8>, StorageError> {
        let result = self
            .recorded_storage
            .get(hash)
            .map_or_else(|| Err(StorageError::TrieNodeMissing), |val| Ok(val.clone()));
        if result.is_ok() {
            self.visited_nodes.lock().expect(POISONED_LOCK_ERR).insert(*hash);
        }
        result
    }

    fn as_partial_storage(&self) -> Option<&TrieMemoryPartialStorage> {
        Some(self)
    }
}

pub struct TrieCachingStorage {
    pub(crate) store: Arc<Store>,
    pub(crate) cache: Arc<Mutex<SizedCache<CryptoHash, Option<Vec<u8>>>>>,
}

impl TrieCachingStorage {
    pub fn new(store: Arc<Store>) -> TrieCachingStorage {
        // TODO defend from huge values in cache
        TrieCachingStorage { store, cache: Arc::new(Mutex::new(SizedCache::with_size(10000))) }
    }

    fn vec_to_rc(val: &Option<Vec<u8>>) -> Result<u32, StorageError> {
        val.as_ref()
            .map(|vec| {
                decode_trie_node_with_rc(&vec).map(|(_bytes, rc)| rc).map_err(|_| {
                    StorageError::StorageInconsistentState("Decode node with RC failed".to_string())
                })
            })
            .unwrap_or_else(|| Ok(0))
    }

    fn vec_to_bytes(val: &Option<Vec<u8>>) -> Result<Vec<u8>, StorageError> {
        val.as_ref()
            .map(|vec| {
                decode_trie_node_with_rc(&vec).map(|(bytes, _rc)| bytes.to_vec()).map_err(|_| {
                    StorageError::StorageInconsistentState("Decode node with RC failed".to_string())
                })
            })
            // not StorageError::TrieNodeMissing because it's only for TrieMemoryPartialStorage
            .unwrap_or_else(|| {
                Err(StorageError::StorageInconsistentState("Trie node missing".to_string()))
            })
    }

    /// Get storage refcount, or 0 if hash is not present
    /// # Errors
    /// StorageError::StorageInternalError if the storage fails internally.
    pub fn retrieve_rc(&self, hash: &CryptoHash) -> Result<u32, StorageError> {
        let mut guard = self.cache.lock().expect(POISONED_LOCK_ERR);
        if let Some(val) = (*guard).cache_get(hash) {
            Self::vec_to_rc(val)
        } else {
            let val = self
                .store
                .get(ColState, hash.as_ref())
                .map_err(|_| StorageError::StorageInternalError)?;
            let rc = Self::vec_to_rc(&val);
            (*guard).cache_set(*hash, val);
            rc
        }
    }
}

impl TrieStorage for TrieCachingStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Vec<u8>, StorageError> {
        let mut guard = self.cache.lock().expect(POISONED_LOCK_ERR);
        if let Some(val) = (*guard).cache_get(hash) {
            Self::vec_to_bytes(val)
        } else {
            let val = self
                .store
                .get(ColState, hash.as_ref())
                .map_err(|_| StorageError::StorageInternalError)?;
            let raw_node = Self::vec_to_bytes(&val);
            (*guard).cache_set(*hash, val);
            raw_node
        }
    }

    fn as_caching_storage(&self) -> Option<&TrieCachingStorage> {
        Some(self)
    }
}

/// Runtime counts the number of touched trie nodes for the purpose of gas calculation.
/// Trie increments it on every call to TrieStorage::retrieve_raw_bytes()
#[derive(Default)]
pub struct TouchedNodesCounter {
    counter: AtomicU64,
}

impl TouchedNodesCounter {
    pub fn increment(&self) {
        self.counter.fetch_add(1, Ordering::SeqCst);
    }

    pub fn reset(&self) {
        self.counter.store(0, Ordering::SeqCst);
    }

    pub fn get(&self) -> u64 {
        self.counter.load(Ordering::SeqCst)
    }
}

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use cached::{Cached, SizedCache};

use near_primitives::hash::CryptoHash;

use crate::db::refcount::decode_value_with_rc;
use crate::trie::POISONED_LOCK_ERR;
use crate::{ColState, StorageError, Store};
use near_primitives::types::ShardId;
use std::convert::{TryFrom, TryInto};
use std::io::ErrorKind;

#[derive(Clone)]
pub struct TrieCache(Arc<Mutex<SizedCache<CryptoHash, Vec<u8>>>>);

impl TrieCache {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(SizedCache::with_size(TRIE_MAX_CACHE_SIZE))))
    }

    pub fn update_cache(&self, ops: Vec<(CryptoHash, Option<Vec<u8>>)>) {
        let mut guard = self.0.lock().expect(POISONED_LOCK_ERR);
        for (hash, opt_value_rc) in ops {
            if let Some(value_rc) = opt_value_rc {
                if let (Some(value), _rc) = decode_value_with_rc(&value_rc) {
                    if value.len() < TRIE_LIMIT_CACHED_VALUE_SIZE {
                        guard.cache_set(hash, value.to_vec());
                    }
                } else {
                    guard.cache_remove(&hash);
                }
            } else {
                guard.cache_remove(&hash);
            }
        }
    }
}

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

/// Maximum number of cache entries.
#[cfg(not(feature = "no_cache"))]
const TRIE_MAX_CACHE_SIZE: usize = 10000;

#[cfg(feature = "no_cache")]
const TRIE_MAX_CACHE_SIZE: usize = 1;

/// Values above this size (in bytes) are never cached.
/// Note that Trie inner nodes are always smaller than this.
const TRIE_LIMIT_CACHED_VALUE_SIZE: usize = 4000;

pub struct TrieCachingStorage {
    pub(crate) store: Arc<Store>,
    pub(crate) cache: TrieCache,
    pub(crate) shard_id: ShardId,
}

impl TrieCachingStorage {
    pub fn new(store: Arc<Store>, cache: TrieCache, shard_id: ShardId) -> TrieCachingStorage {
        TrieCachingStorage { store, cache, shard_id }
    }

    pub(crate) fn get_shard_id_and_hash_from_key(
        key: &[u8],
    ) -> Result<(u64, CryptoHash), std::io::Error> {
        if key.len() != 40 {
            return Err(std::io::Error::new(ErrorKind::Other, "Key is always shard_id + hash"));
        }
        let shard_id = u64::from_le_bytes(key[0..8].try_into().unwrap());
        let hash = CryptoHash::try_from(&key[8..]).unwrap();
        Ok((shard_id, hash))
    }

    pub(crate) fn get_key_from_shard_id_and_hash(shard_id: ShardId, hash: &CryptoHash) -> [u8; 40] {
        let mut key = [0; 40];
        key[0..8].copy_from_slice(&u64::to_le_bytes(shard_id));
        key[8..].copy_from_slice(hash.as_ref());
        key
    }
}

impl TrieStorage for TrieCachingStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Vec<u8>, StorageError> {
        let mut guard = self.cache.0.lock().expect(POISONED_LOCK_ERR);
        if let Some(val) = guard.cache_get(hash) {
            Ok(val.clone())
        } else {
            let key = Self::get_key_from_shard_id_and_hash(self.shard_id, hash);
            let val = self
                .store
                .get(ColState, key.as_ref())
                .map_err(|_| StorageError::StorageInternalError)?;
            if let Some(val) = val {
                if val.len() < TRIE_LIMIT_CACHED_VALUE_SIZE {
                    guard.cache_set(*hash, val.clone());
                }
                Ok(val)
            } else {
                // not StorageError::TrieNodeMissing because it's only for TrieMemoryPartialStorage
                Err(StorageError::StorageInconsistentState("Trie node missing".to_string()))
            }
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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use near_primitives::hash::CryptoHash;

use crate::db::refcount::decode_value_with_rc;
use crate::trie::POISONED_LOCK_ERR;
use crate::{ColState, StorageError, Store};
use lru::LruCache;
use near_primitives::shard_layout::ShardUId;
use std::cell::{Cell, RefCell};
use std::io::ErrorKind;

#[derive(Clone)]
pub struct TrieCache(Arc<Mutex<LruCache<CryptoHash, Arc<[u8]>>>>);

impl TrieCache {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(LruCache::new(TRIE_MAX_CACHE_SIZE))))
    }

    pub fn clear(&self) {
        self.0.lock().expect(POISONED_LOCK_ERR).clear()
    }

    pub fn update_cache(&self, ops: Vec<(CryptoHash, Option<&Vec<u8>>)>) {
        let mut guard = self.0.lock().expect(POISONED_LOCK_ERR);
        for (hash, opt_value_rc) in ops {
            if let Some(value_rc) = opt_value_rc {
                if let (Some(value), _rc) = decode_value_with_rc(&value_rc) {
                    if value.len() < TRIE_LIMIT_CACHED_VALUE_SIZE {
                        guard.put(hash, value.into());
                    }
                } else {
                    guard.pop(&hash);
                }
            } else {
                guard.pop(&hash);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        let guard = self.0.lock().expect(POISONED_LOCK_ERR);
        guard.len()
    }
}

pub trait TrieStorage {
    /// Get bytes of a serialized TrieNode.
    /// # Errors
    /// StorageError if the storage fails internally or the hash is not present.
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError>;

    fn as_caching_storage(&self) -> Option<&TrieCachingStorage> {
        None
    }

    fn as_recording_storage(&self) -> Option<&TrieRecordingStorage> {
        None
    }

    fn as_partial_storage(&self) -> Option<&TrieMemoryPartialStorage> {
        None
    }

    fn get_touched_nodes_count(&self) -> u64;
}

/// Records every value read by retrieve_raw_bytes.
/// Used for obtaining state parts (and challenges in the future).
/// TODO (#6316): implement proper nodes counting logic as in TrieCachingStorage
pub struct TrieRecordingStorage {
    pub(crate) store: Store,
    pub(crate) shard_uid: ShardUId,
    pub(crate) recorded: RefCell<HashMap<CryptoHash, Vec<u8>>>,
}

impl TrieStorage for TrieRecordingStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
        if let Some(val) = self.recorded.borrow().get(hash) {
            return Ok(val.as_slice().into());
        }
        let key = TrieCachingStorage::get_key_from_shard_uid_and_hash(self.shard_uid, hash);
        let val = self
            .store
            .get(ColState, key.as_ref())
            .map_err(|_| StorageError::StorageInternalError)?;
        if let Some(val) = val {
            self.recorded.borrow_mut().insert(*hash, val.clone());
            Ok(val.into())
        } else {
            Err(StorageError::StorageInconsistentState("Trie node missing".to_string()))
        }
    }

    fn as_recording_storage(&self) -> Option<&TrieRecordingStorage> {
        Some(self)
    }

    fn get_touched_nodes_count(&self) -> u64 {
        unimplemented!();
    }
}

/// Storage for validating recorded partial storage.
/// visited_nodes are to validate that partial storage doesn't contain unnecessary nodes.
pub struct TrieMemoryPartialStorage {
    pub(crate) recorded_storage: HashMap<CryptoHash, Vec<u8>>,
    pub(crate) visited_nodes: RefCell<HashSet<CryptoHash>>,
}

impl TrieStorage for TrieMemoryPartialStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
        let result = self
            .recorded_storage
            .get(hash)
            .map_or_else(|| Err(StorageError::TrieNodeMissing), |val| Ok(val.as_slice().into()));
        if result.is_ok() {
            self.visited_nodes.borrow_mut().insert(*hash);
        }
        result
    }

    fn as_partial_storage(&self) -> Option<&TrieMemoryPartialStorage> {
        Some(self)
    }

    fn get_touched_nodes_count(&self) -> u64 {
        unimplemented!();
    }
}

/// Maximum number of cache entries.
/// It was chosen to fit into RAM well. RAM spend on trie cache should not exceed
/// 50_000 * 4 (number of shards) * TRIE_LIMIT_CACHED_VALUE_SIZE = 800 MB.
/// In our tests on a single shard, it barely occupied 40 MB, which is dominated by state cache size
/// with 512 MB limit. The total RAM usage for a single shard was 1 GB.
#[cfg(not(feature = "no_cache"))]
const TRIE_MAX_CACHE_SIZE: usize = 50000;

#[cfg(feature = "no_cache")]
const TRIE_MAX_CACHE_SIZE: usize = 1;

/// Values above this size (in bytes) are never cached.
/// Note that Trie inner nodes are always smaller than this.
const TRIE_LIMIT_CACHED_VALUE_SIZE: usize = 4000;

pub struct TrieCachingStorage {
    pub(crate) store: Store,
    pub(crate) cache: TrieCache,
    pub(crate) shard_uid: ShardUId,

    pub(crate) counter: Cell<u64>,
}

impl TrieCachingStorage {
    pub fn new(store: Store, cache: TrieCache, shard_uid: ShardUId) -> TrieCachingStorage {
        TrieCachingStorage { store, cache, shard_uid, counter: Cell::new(0u64) }
    }

    pub(crate) fn get_shard_uid_and_hash_from_key(
        key: &[u8],
    ) -> Result<(ShardUId, CryptoHash), std::io::Error> {
        if key.len() != 40 {
            return Err(std::io::Error::new(ErrorKind::Other, "Key is always shard_uid + hash"));
        }
        let id = ShardUId::try_from(&key[..8]).unwrap();
        let hash = CryptoHash::try_from(&key[8..]).unwrap();
        Ok((id, hash))
    }

    pub(crate) fn get_key_from_shard_uid_and_hash(
        shard_uid: ShardUId,
        hash: &CryptoHash,
    ) -> [u8; 40] {
        let mut key = [0; 40];
        key[0..8].copy_from_slice(&shard_uid.to_bytes());
        key[8..].copy_from_slice(hash.as_ref());
        key
    }

    fn inc_counter(&self) {
        self.counter.set(self.counter.get() + 1);
    }
}

impl TrieStorage for TrieCachingStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
        self.inc_counter();
        let mut guard = self.cache.0.lock().expect(POISONED_LOCK_ERR);
        if let Some(val) = guard.get(hash) {
            Ok(val.clone())
        } else {
            let key = Self::get_key_from_shard_uid_and_hash(self.shard_uid, hash);
            let val = self
                .store
                .get(ColState, key.as_ref())
                .map_err(|_| StorageError::StorageInternalError)?;
            if let Some(val) = val {
                let val: Arc<[u8]> = val.into();
                if val.len() < TRIE_LIMIT_CACHED_VALUE_SIZE {
                    guard.put(*hash, val.clone());
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

    fn get_touched_nodes_count(&self) -> u64 {
        self.counter.get()
    }
}

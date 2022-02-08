use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use near_primitives::hash::CryptoHash;

use crate::db::refcount::decode_value_with_rc;
use crate::trie::POISONED_LOCK_ERR;
use crate::{ColState, StorageError, Store};
use lru::LruCache;
use near_primitives::shard_layout::ShardUId;
use std::cell::RefCell;
use std::io::ErrorKind;
use near_primitives::block::CacheState;

#[derive(Clone)]
pub struct SyncTrieCache(Arc<Mutex<TrieCache>>);

struct TrieCache {
    cache_state: CacheState,
    shard_cache: LruCache<CryptoHash, Vec<u8>>,
    chunk_cache: HashMap<CryptoHash, Vec<u8>>,
}

enum CachePosition {
    None,
    ShardCache(Vec<u8>),
    ChunkCache(Vec<u8>),
}

pub enum TrieNodeRetrievalCost {
    Free,
    Full,
}

impl TrieCache {
    fn get_cache_position(&mut self, hash: &CryptoHash) -> CachePosition {
        match self.chunk_cache.get(hash) {
            Some(value) => CachePosition::ChunkCache(value.clone()),
            None => match self.shard_cache.get(hash) {
                Some(value) => CachePosition::ShardCache(value.clone()),
                None => CachePosition::None,
            },
        }
    }

    pub fn get_with_cost(&mut self, hash: &CryptoHash) -> (Option<Vec<u8>>, TrieNodeRetrievalCost) {
        match self.get_cache_position(hash) {
            CachePosition::None => (None, TrieNodeRetrievalCost::Full),
            CachePosition::ShardCache(value) => {
                if let CacheState::CachingChunk = &self.cache_state {
                    let value = self
                        .shard_cache
                        .pop(hash)
                        .expect("If position is ShardCache then value must be presented");
                    self.chunk_cache.insert(hash.clone(), value);
                };
                (Some(value), TrieNodeRetrievalCost::Full)
            }
            CachePosition::ChunkCache(value) => (Some(value), TrieNodeRetrievalCost::Free),
        }
    }

    fn put(&mut self, hash: CryptoHash, value: &[u8]) {
        // TODO: put TRIE_LIMIT_CACHED_VALUE_SIZE to runtime config
        if value.len() >= TRIE_LIMIT_CACHED_VALUE_SIZE {
            return;
        }
        let value = value.to_vec();

        if let CacheState::CachingChunk = &self.cache_state {
            self.shard_cache.pop(&hash);
            self.chunk_cache.insert(hash, value);
        } else {
            if self.chunk_cache.contains_key(&hash) {
                self.chunk_cache.insert(hash, value);
            } else {
                self.shard_cache.put(hash, value);
            }
        }
    }

    fn pop(&mut self, hash: &CryptoHash) -> Option<Vec<u8>> {
        match self.chunk_cache.remove(hash) {
            Some(value) => Some(value),
            None => self.shard_cache.pop(hash),
        }
    }

    fn drain_chunk_cache(&mut self) {
        self.chunk_cache.drain().for_each(|(hash, value)| {
            self.shard_cache.put(hash, value);
        });
    }
}

impl SyncTrieCache {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(TrieCache {
            cache_state: CacheState::CachingShard,
            shard_cache: LruCache::new(TRIE_MAX_CACHE_SIZE),
            chunk_cache: Default::default(),
        })))
    }

    pub fn clear(&self) {
        let mut guard = self.0.lock().expect(POISONED_LOCK_ERR);
        guard.cache_state = CacheState::CachingShard;
        guard.shard_cache.clear();
        guard.chunk_cache.clear();
    }

    pub fn set_chunk_cache_state(&self, state: CacheState) {
        let mut guard = self.0.lock().expect(POISONED_LOCK_ERR);
        guard.cache_state = state;
    }

    pub fn update_cache(&self, ops: Vec<(CryptoHash, Option<&Vec<u8>>)>) {
        let mut guard = self.0.lock().expect(POISONED_LOCK_ERR);
        for (hash, opt_value_rc) in ops {
            if let Some(value_rc) = opt_value_rc {
                if let (Some(value), _rc) = decode_value_with_rc(&value_rc) {
                    guard.put(hash, value);
                } else {
                    guard.pop(&hash);
                }
            } else {
                guard.pop(&hash);
            }
        }
    }
}

pub trait TrieStorage {
    /// Get bytes of a serialized TrieNode.
    /// # Errors
    /// StorageError if the storage fails internally or the hash is not present.
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Vec<u8>, StorageError>;

    /// Get bytes of a serialized TrieNode with the node retrieval cost.
    /// # Errors
    /// StorageError if the storage fails internally or the hash is not present.
    fn retrieve_raw_bytes_with_cost(
        &self,
        hash: &CryptoHash,
    ) -> Result<(Vec<u8>, TrieNodeRetrievalCost), StorageError> {
        self.retrieve_raw_bytes(hash).map(|value| (value, TrieNodeRetrievalCost::Full))
    }

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

/// Records every value read by retrieve_raw_bytes.
/// Used for obtaining state parts (and challenges in the future).
pub struct TrieRecordingStorage {
    pub(crate) store: Store,
    pub(crate) shard_uid: ShardUId,
    pub(crate) recorded: RefCell<HashMap<CryptoHash, Vec<u8>>>,
}

impl TrieStorage for TrieRecordingStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Vec<u8>, StorageError> {
        if let Some(val) = self.recorded.borrow().get(hash) {
            return Ok(val.clone());
        }
        let key = TrieCachingStorage::get_key_from_shard_uid_and_hash(self.shard_uid, hash);
        let val = self
            .store
            .get(ColState, key.as_ref())
            .map_err(|_| StorageError::StorageInternalError)?;
        if let Some(val) = val {
            self.recorded.borrow_mut().insert(*hash, val.clone());
            Ok(val)
        } else {
            Err(StorageError::StorageInconsistentState("Trie node missing".to_string()))
        }
    }

    fn as_recording_storage(&self) -> Option<&TrieRecordingStorage> {
        Some(self)
    }
}

/// Storage for validating recorded partial storage.
/// visited_nodes are to validate that partial storage doesn't contain unnecessary nodes.
pub struct TrieMemoryPartialStorage {
    pub(crate) recorded_storage: HashMap<CryptoHash, Vec<u8>>,
    pub(crate) visited_nodes: RefCell<HashSet<CryptoHash>>,
}

impl TrieStorage for TrieMemoryPartialStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Vec<u8>, StorageError> {
        let result = self
            .recorded_storage
            .get(hash)
            .map_or_else(|| Err(StorageError::TrieNodeMissing), |val| Ok(val.clone()));
        if result.is_ok() {
            self.visited_nodes.borrow_mut().insert(*hash);
        }
        result
    }

    fn as_partial_storage(&self) -> Option<&TrieMemoryPartialStorage> {
        Some(self)
    }
}

/// Maximum number of cache entries.
/// It was chosen to fit into RAM well. RAM spend on trie cache should not exceed
/// 100_000 * 4 (number of shards) * TRIE_LIMIT_CACHED_VALUE_SIZE = 400 MB.
/// In our tests on a single shard, it barely occupied 40 MB, which is dominated by state cache size
/// with 512 MB limit. The total RAM usage for a single shard was 1 GB.
#[cfg(not(feature = "no_cache"))]
const TRIE_MAX_CACHE_SIZE: usize = 100_000;

#[cfg(feature = "no_cache")]
const TRIE_MAX_CACHE_SIZE: usize = 1;

/// Values above this size (in bytes) are never cached.
/// Note that Trie inner nodes are always smaller than this.
const TRIE_LIMIT_CACHED_VALUE_SIZE: usize = 1000;

pub struct TrieCachingStorage {
    pub(crate) store: Store,
    pub(crate) cache: SyncTrieCache,
    pub(crate) shard_uid: ShardUId,
}

impl TrieCachingStorage {
    pub fn new(store: Store, cache: SyncTrieCache, shard_uid: ShardUId) -> TrieCachingStorage {
        TrieCachingStorage { store, cache, shard_uid }
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

    pub fn prepare_trie_cache(&self) {
        let mut guard = self.cache.0.lock().expect(POISONED_LOCK_ERR);
        guard.cache_state = CacheState::CachingShard;
        guard.drain_chunk_cache();
    }
}

impl TrieStorage for TrieCachingStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Vec<u8>, StorageError> {
        self.retrieve_raw_bytes_with_cost(hash).map(|(val, _)| val)
    }

    fn retrieve_raw_bytes_with_cost(
        &self,
        hash: &CryptoHash,
    ) -> Result<(Vec<u8>, TrieNodeRetrievalCost), StorageError> {
        let mut guard = self.cache.0.lock().expect(POISONED_LOCK_ERR);
        if let (Some(val), cost) = guard.get_with_cost(hash) {
            tracing::debug!(target: "runtime", cost = ?cost);
            Ok((val, cost))
        } else {
            let key = Self::get_key_from_shard_uid_and_hash(self.shard_uid, hash);
            let val = self
                .store
                .get(ColState, key.as_ref())
                .map_err(|_| StorageError::StorageInternalError)?;
            if let Some(val) = val {
                guard.put(*hash, &val);
                Ok((val, TrieNodeRetrievalCost::Full))
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

    pub fn get(&self) -> u64 {
        self.counter.load(Ordering::SeqCst)
    }
}

use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use near_primitives::hash::CryptoHash;

use crate::db::refcount::decode_value_with_rc;
use crate::trie::POISONED_LOCK_ERR;
use crate::{ColState, StorageError, Store};
use lru::LruCache;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::TrieCacheState;
use std::cell::{Cell, RefCell};
use std::io::ErrorKind;

/// Wrapper over LruCache which doesn't hold too large elements.
#[derive(Clone)]
pub struct TrieCache(Arc<Mutex<LruCache<CryptoHash, Arc<[u8]>>>>);

impl TrieCache {
    pub fn new() -> Self {
        Self::with_capacity(TRIE_MAX_SHARD_CACHE_SIZE)
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self(Arc::new(Mutex::new(LruCache::new(cap))))
    }

    pub fn get(&self, key: &CryptoHash) -> Option<Arc<[u8]>> {
        self.0.lock().expect(POISONED_LOCK_ERR).get(key).cloned()
    }

    pub fn put(&self, key: CryptoHash, value: Arc<[u8]>) {
        self.0.lock().expect(POISONED_LOCK_ERR).put(key, value);
    }

    pub fn pop(&self, key: &CryptoHash) -> Option<Arc<[u8]>> {
        self.0.lock().expect(POISONED_LOCK_ERR).pop(key)
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
/// It was chosen to fit into RAM well. RAM spend on trie cache should not exceed 50_000 * 4 (number of shards) *
/// TRIE_LIMIT_CACHED_VALUE_SIZE * 2 (number of caches - for regular and view client) = 1.6 GB.
/// In our tests on a single shard, it barely occupied 40 MB, which is dominated by state cache size
/// with 512 MB limit. The total RAM usage for a single shard was 1 GB.
#[cfg(not(feature = "no_cache"))]
const TRIE_MAX_SHARD_CACHE_SIZE: usize = 50000;

#[cfg(feature = "no_cache")]
const TRIE_MAX_SHARD_CACHE_SIZE: usize = 1;

/// Values above this size (in bytes) are never cached.
/// Note that Trie inner nodes are always smaller than this.
pub(crate) const TRIE_LIMIT_CACHED_VALUE_SIZE: usize = 4000;

pub struct TrieCachingStorage {
    pub(crate) store: Store,
    pub(crate) shard_uid: ShardUId,

    /// Caches ever requested items for the shard `shard_uid`. Used to speed up DB operations, presence of any item is
    /// not guaranteed.
    pub(crate) shard_cache: TrieCache,
    /// Caches all items requested in the `TrieCacheState::CachingChunk` state. It must be empty when we start to apply
    /// txs and receipts in the chunk. All items placed here must remain until applying txs/receipts ends.
    /// Note that for both caches key is the hash of value, so for the fixed key the value is unique.
    /// TODO (#5920): enable chunk nodes caching in Runtime::apply.
    pub(crate) chunk_cache: RefCell<HashMap<CryptoHash, Arc<[u8]>>>,
    pub(crate) cache_state: Cell<TrieCacheState>,

    /// Counts retrieved trie nodes. Used to compute gas cost for touching trie nodes.
    pub(crate) counter: Cell<u64>,
}

impl TrieCachingStorage {
    pub fn new(store: Store, shard_cache: TrieCache, shard_uid: ShardUId) -> TrieCachingStorage {
        TrieCachingStorage {
            store,
            shard_uid,
            shard_cache,
            cache_state: Cell::new(TrieCacheState::CachingShard),
            chunk_cache: RefCell::new(Default::default()),
            counter: Cell::new(0u64),
        }
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

    /// If we are in the caching chunk state, put item to the chunk cache.
    fn try_put_to_chunk_cache(&self, key: CryptoHash, value: Arc<[u8]>) {
        if let TrieCacheState::CachingChunk = self.cache_state.borrow().get() {
            self.chunk_cache.borrow_mut().insert(key, value);
        };
    }

    /// Set cache state.
    pub fn set_state(&self, state: TrieCacheState) {
        self.cache_state.set(state);
    }
}

impl TrieStorage for TrieCachingStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
        // Try to get value from chunk cache containing free of charge nodes, otherwise increment the nodes counter.
        // let RawBytesWithCost { value, cost } = self.get_from_cache(hash);
        if let Some(val) = self.chunk_cache.borrow_mut().get(hash) {
            return Ok(val.clone());
        }
        self.inc_counter();

        // If full cost for retrieving the node must be charged, increment the counter.
        if let Some(val) = self.shard_cache.get(hash) {
            self.try_put_to_chunk_cache(*hash, val.clone());
            return Ok(val);
        }

        // If value is not present in cache, get it from the storage.
        let key = Self::get_key_from_shard_uid_and_hash(self.shard_uid, hash);
        let val = self
            .store
            .get(ColState, key.as_ref())
            .map_err(|_| StorageError::StorageInternalError)?
            .ok_or_else(|| {
                StorageError::StorageInconsistentState("Trie node missing".to_string())
            })?;
        let val: Arc<[u8]> = val.into();

        // Insert value to shard and chunk caches.
        if val.len() < TRIE_LIMIT_CACHED_VALUE_SIZE {
            self.shard_cache.put(*hash, val.clone());
        }
        self.try_put_to_chunk_cache(*hash, val.clone());

        Ok(val)
    }

    fn as_caching_storage(&self) -> Option<&TrieCachingStorage> {
        Some(self)
    }

    fn get_touched_nodes_count(&self) -> u64 {
        self.counter.get()
    }
}

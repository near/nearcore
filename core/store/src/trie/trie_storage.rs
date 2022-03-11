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

#[derive(Clone)]
pub struct TrieCache(Arc<Mutex<LruCache<CryptoHash, Arc<[u8]>>>>);

impl TrieCache {
    pub fn new() -> Self {
        Self::new_with_cap(TRIE_MAX_SHARD_CACHE_SIZE)
    }

    pub fn new_with_cap(cap: usize) -> Self {
        Self(Arc::new(Mutex::new(LruCache::new(cap))))
    }

    pub fn get(&self, key: &CryptoHash) -> Option<Arc<[u8]>> {
        self.0.lock().expect(POISONED_LOCK_ERR).get(key).map(Clone::clone)
    }

    pub fn put(&self, key: CryptoHash, value: Arc<[u8]>) {
        self.0.lock().unwrap().put(key, value);
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
const TRIE_LIMIT_CACHED_VALUE_SIZE: usize = 4000;

/// Position of the value in cache.
#[derive(Debug)]
pub(crate) enum CachePosition {
    /// Value is not present.
    None,
    /// Value is present **only** in the shard cache.
    ShardCache(Arc<[u8]>),
    /// Value is present in the chunk cache.
    ChunkCache(Arc<[u8]>),
}

/// Cost of retrieving trie node from the storage. Used to compute gas cost for touching trie nodes.
#[derive(Debug, Eq, PartialEq)]
pub enum TrieNodeRetrievalCost {
    Free,
    Full,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct RawBytesWithCost {
    /// Bytes of the retrieved node. None if no value was found in cache.
    pub(crate) value: Option<Arc<[u8]>>,
    /// Cost of node retrieval.
    pub(crate) cost: TrieNodeRetrievalCost,
}

pub struct TrieCachingStorage {
    pub(crate) store: Store,
    pub(crate) shard_uid: ShardUId,

    pub(crate) cache_state: Cell<TrieCacheState>,
    /// Caches ever requested items for the shard `shard_uid`. Used to speed up DB operations, presence of any item is
    /// not guaranteed.
    pub(crate) shard_cache: TrieCache,
    /// Caches all items requested in the `TrieCacheState::CachingChunk` state. It must be empty when we start to apply
    /// txs and receipts in the chunk. All items placed here must remain until applying txs/receipts ends.
    /// Note that for both caches key is the hash of value, so for the fixed key the value is unique.
    /// TODO (#5920): enable chunk nodes caching in Runtime::apply.
    pub(crate) chunk_cache: RefCell<HashMap<CryptoHash, Arc<[u8]>>>,

    /// Counts retrieved trie nodes. Used to compute gas cost for touching trie nodes.
    pub(crate) counter: Cell<u64>,
}

impl TrieCachingStorage {
    pub fn new(store: Store, shard_cache: TrieCache, shard_uid: ShardUId) -> TrieCachingStorage {
        TrieCachingStorage {
            store,
            shard_uid,
            cache_state: Cell::new(TrieCacheState::CachingShard),
            shard_cache,
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

    /// Get position of the given key in the cache.
    pub(crate) fn get_cache_position(&self, key: &CryptoHash) -> CachePosition {
        match self.chunk_cache.borrow_mut().get(key) {
            Some(value) => CachePosition::ChunkCache(value.clone()),
            None => match self.shard_cache.get(key) {
                Some(value) => CachePosition::ShardCache(value.clone()),
                None => CachePosition::None,
            },
        }
    }

    /// If we are in the caching chunk state, put item to the chunk cache.
    fn try_put_to_chunk_cache(&self, key: CryptoHash, value: Arc<[u8]>) {
        if let TrieCacheState::CachingChunk = self.cache_state.borrow().get() {
            self.chunk_cache.borrow_mut().insert(key, value);
        };
    }

    /// Get value for the given key from cache.
    /// Return value if it is present and cost of retrieving the trie node corresponding to this key.
    pub(crate) fn get_from_cache(&self, key: &CryptoHash) -> RawBytesWithCost {
        match self.get_cache_position(key) {
            // If no value is present, return None. If node for this key will be retrieved from DB, we have to charge
            // full cost for it.
            CachePosition::None => {
                RawBytesWithCost { value: None, cost: TrieNodeRetrievalCost::Full }
            }
            // If value is present only in shard cache, we can copy value to the chunk cache. The cost must be full
            // anyway because we didn't access the node in the chunk yet.
            CachePosition::ShardCache(value) => {
                self.try_put_to_chunk_cache(key.clone(), value.clone());
                RawBytesWithCost { value: Some(value), cost: TrieNodeRetrievalCost::Full }
            }
            // If value is present in chunk cache, the cost is free.
            CachePosition::ChunkCache(value) => {
                RawBytesWithCost { value: Some(value), cost: TrieNodeRetrievalCost::Free }
            }
        }
    }

    /// Put a key-value pair into cache.
    pub(crate) fn put_to_cache(&self, key: CryptoHash, value: Arc<[u8]>) {
        self.shard_cache.put(key.clone(), value.clone());
        self.try_put_to_chunk_cache(key, value.clone());
    }

    /// Set cache state.
    pub fn set_state(&self, state: TrieCacheState) {
        self.cache_state.set(state);
    }
}

impl TrieStorage for TrieCachingStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
        // Try to get value from cache and determine node retrieval cost.
        let RawBytesWithCost { value, cost } = self.get_from_cache(hash);

        // If full cost for retrieving the node must be charged, increment the counter.
        if let TrieNodeRetrievalCost::Full = cost {
            self.inc_counter();
        }

        match value {
            // If value is not present, get it from the store and put into the cache.
            None => {
                let key = Self::get_key_from_shard_uid_and_hash(self.shard_uid, hash);
                let val = self
                    .store
                    .get(ColState, key.as_ref())
                    .map_err(|_| StorageError::StorageInternalError)?;
                if let Some(val) = val {
                    let val: Arc<[u8]> = val.into();
                    if val.len() < TRIE_LIMIT_CACHED_VALUE_SIZE {
                        self.put_to_cache(*hash, val.clone());
                    }
                    Ok(val)
                } else {
                    // not StorageError::TrieNodeMissing because it's only for TrieMemoryPartialStorage
                    Err(StorageError::StorageInconsistentState("Trie node missing".to_string()))
                }
            }
            // If value is present, return it.
            Some(val) => Ok(val),
        }
    }

    fn as_caching_storage(&self) -> Option<&TrieCachingStorage> {
        Some(self)
    }

    fn get_touched_nodes_count(&self) -> u64 {
        self.counter.get()
    }
}

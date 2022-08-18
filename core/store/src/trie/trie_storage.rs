use std::borrow::Borrow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use near_primitives::hash::CryptoHash;

use crate::db::refcount::decode_value_with_rc;
use crate::trie::POISONED_LOCK_ERR;
use crate::{DBCol, StorageError, Store};
use lru::LruCache;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::{TrieCacheMode, TrieNodesCount};
use std::cell::{Cell, RefCell};
use std::io::ErrorKind;

pub struct TrieDeletionsQueue {
    queue: VecDeque<CryptoHash>,
    /// Upper bound for the deletions queue size.
    capacity: usize,
}

impl TrieDeletionsQueue {
    pub fn new(capacity: usize) -> Self {
        Self { queue: VecDeque::with_capacity(capacity), capacity }
    }

    pub fn clear(&mut self) {
        self.queue.clear();
    }

    pub fn pop(&mut self) -> Option<CryptoHash> {
        self.queue.pop_front()
    }

    pub fn put(&mut self, key: CryptoHash) -> Option<CryptoHash> {
        let result = if self.queue.len() == self.capacity {
            let key_to_remove = self.pop().expect("Queue cannot be empty");
            Some(key_to_remove)
        } else {
            None
        };
        self.queue.push_back(key);
        result
    }
}

/// In-memory cache for trie items - nodes and values. All nodes are stored in the LRU cache with three modifications.
/// 1) Size of each value must not exceed `SHARD_CACHE_VALUE_SIZE_LIMIT`.
/// Needed to avoid caching large values like contract codes.
/// 2) If we put new value to LRU cache and total size of existing values exceeds `total_sizes_capacity`, we evict
/// values from it until that is no longer the case. So the actual total size should never exceed
/// `total_size_limit` + `SHARD_CACHE_VALUE_SIZE_LIMIT`.
/// Needed because value sizes generally vary from 1 B to 500 B and we want to count cache size precisely.
/// 3) If value is popped, it is put to the `deletions` queue with `deletions_queue_capacity` first. If popped value
/// doesn't fit in the queue, the last value is removed from the queue and LRU cache, and newly popped value is inserted
/// to the queue.
/// Needed to delay deletions when we have forks. In such case, many blocks can share same parent, and we want to keep
/// old nodes in cache for a while to process all new roots. For example, it helps to read old state root.
pub struct SyncTrieCache {
    /// LRU cache keeping mapping from keys to values.
    cache: LruCache<CryptoHash, Arc<[u8]>>,
    /// Queue of items which were popped, which postpones deletion of old nodes.
    deletions: TrieDeletionsQueue,
    /// Current total size of all values in the cache.
    total_size: u64,
    /// Upper bound for the total size.
    total_size_limit: u64,
}

impl SyncTrieCache {
    pub fn new(
        cache_capacity: usize,
        deletions_queue_capacity: usize,
        total_size_limit: u64,
    ) -> Self {
        Self {
            cache: LruCache::new(cache_capacity),
            deletions: TrieDeletionsQueue::new(deletions_queue_capacity),
            total_size: 0,
            total_size_limit,
        }
    }

    pub fn get(&mut self, key: &CryptoHash) -> Option<Arc<[u8]>> {
        self.cache.get(key).cloned()
    }

    pub fn clear(&mut self) {
        self.total_size = 0;
        self.deletions.clear();
        self.cache.clear();
    }

    pub fn put(&mut self, key: CryptoHash, value: Arc<[u8]>) {
        while self.total_size > self.total_size_limit {
            let evicted_value = match self.deletions.pop() {
                // First, try to evict value using the key from deletions queue.
                Some(key) => self.cache.pop(&key),
                // Second, pop LRU value.
                None => Some(
                    self.cache.pop_lru().expect("Cannot fail because total size capacity is > 0").1,
                ),
            };
            match evicted_value {
                Some(value) => {
                    self.total_size -= value.len() as u64;
                }
                None => {}
            };
        }
        // Add value to the cache.
        self.total_size += value.len() as u64;
        match self.cache.push(key, value) {
            Some((_, evicted_value)) => {
                self.total_size -= evicted_value.len() as u64;
            }
            None => {}
        };
    }

    pub fn pop(&mut self, key: &CryptoHash) {
        // Do nothing if key was removed before.
        if self.cache.contains(key) {
            // Put key to the queue of deletions and possibly remove another key we have to delete.
            match self.deletions.put(key.clone()) {
                Some(key_to_delete) => match self.cache.pop(&key_to_delete) {
                    Some(evicted_value) => {
                        self.total_size -= evicted_value.len() as u64;
                    }
                    None => {}
                },
                None => {}
            }
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.cache.len()
    }
}

/// Wrapper over LruCache to handle concurrent access.
#[derive(Clone)]
pub struct TrieCache(Arc<Mutex<SyncTrieCache>>);

impl TrieCache {
    pub fn new() -> Self {
        Self::with_capacities(DEFAULT_SHARD_CACHE_CAPACITY)
    }

    pub fn with_capacities(cap: usize) -> Self {
        Self(Arc::new(Mutex::new(SyncTrieCache::new(
            cap,
            DEFAULT_SHARD_CACHE_DELETIONS_QUEUE_CAPACITY,
            DEFAULT_SHARD_CACHE_TOTAL_SIZE_LIMIT,
        ))))
    }

    pub fn get(&self, key: &CryptoHash) -> Option<Arc<[u8]>> {
        self.0.lock().expect(POISONED_LOCK_ERR).get(key)
    }

    pub fn clear(&self) {
        self.0.lock().expect(POISONED_LOCK_ERR).clear()
    }

    pub fn update_cache(&self, ops: Vec<(CryptoHash, Option<&Vec<u8>>)>) {
        let mut guard = self.0.lock().expect(POISONED_LOCK_ERR);
        for (hash, opt_value_rc) in ops {
            if let Some(value_rc) = opt_value_rc {
                if let (Some(value), _rc) = decode_value_with_rc(&value_rc) {
                    if value.len() < SHARD_CACHE_VALUE_SIZE_LIMIT {
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

    fn get_trie_nodes_count(&self) -> TrieNodesCount;
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
            .get(DBCol::State, key.as_ref())
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

    fn get_trie_nodes_count(&self) -> TrieNodesCount {
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

    fn get_trie_nodes_count(&self) -> TrieNodesCount {
        unimplemented!();
    }
}

/// Default number of cache entries.
/// It was chosen to fit into RAM well. RAM spend on trie cache should not exceed 50_000 * 4 (number of shards) *
/// SHARD_CACHE_VALUE_SIZE_LIMIT * 2 (number of caches - for regular and view client) = 0.4 GB.
/// In our tests on a single shard, it barely occupied 40 MB, which is dominated by state cache size
/// with 512 MB limit. The total RAM usage for a single shard was 1 GB.
#[cfg(not(feature = "no_cache"))]
const DEFAULT_SHARD_CACHE_CAPACITY: usize = 50_000;
#[cfg(feature = "no_cache")]
const DEFAULT_SHARD_CACHE_CAPACITY: usize = 1;

#[cfg(not(feature = "no_cache"))]
const DEFAULT_SHARD_CACHE_TOTAL_SIZE_LIMIT: u64 = 50_000_000; // consider 4_500_000_000
#[cfg(feature = "no_cache")]
const DEFAULT_SHARD_CACHE_TOTAL_SIZE_LIMIT: u64 = 1;

/// Capacity for the deletions queue.
#[cfg(feature = "cache")]
pub const DEFAULT_SHARD_CACHE_DELETIONS_QUEUE_CAPACITY: usize = 100_000;
#[cfg(feature = "no_cache")]
pub const DEFAULT_SHARD_CACHE_DELETIONS_QUEUE_CAPACITY: usize = 1;

/// Values above this size (in bytes) are never cached.
/// Note that most of Trie inner nodes are smaller than this - e.g. branches use around 32 * 16 = 512 bytes.
pub(crate) const SHARD_CACHE_VALUE_SIZE_LIMIT: usize = 1000;

pub struct TrieCachingStorage {
    pub(crate) store: Store,
    pub(crate) shard_uid: ShardUId,

    /// Caches ever requested items for the shard `shard_uid`. Used to speed up DB operations, presence of any item is
    /// not guaranteed.
    pub(crate) shard_cache: TrieCache,
    /// Caches all items requested in the mode `TrieCacheMode::CachingChunk`. It is created in
    /// `apply_transactions_with_optional_storage_proof` by calling `get_trie_for_shard`. Before we start to apply
    /// txs and receipts in the chunk, it must be empty, and all items placed here must remain until applying
    /// txs/receipts ends. Then cache is removed automatically in `apply_transactions_with_optional_storage_proof` when
    /// `TrieCachingStorage` is removed.
    /// Note that for both caches key is the hash of value, so for the fixed key the value is unique.
    pub(crate) chunk_cache: RefCell<HashMap<CryptoHash, Arc<[u8]>>>,
    pub(crate) cache_mode: Cell<TrieCacheMode>,

    /// Counts potentially expensive trie node reads which are served from disk in the worst case. Here we count reads
    /// from DB or shard cache.
    pub(crate) db_read_nodes: Cell<u64>,
    /// Counts trie nodes retrieved from the chunk cache.
    pub(crate) mem_read_nodes: Cell<u64>,
}

impl TrieCachingStorage {
    pub fn new(store: Store, shard_cache: TrieCache, shard_uid: ShardUId) -> TrieCachingStorage {
        TrieCachingStorage {
            store,
            shard_uid,
            shard_cache,
            cache_mode: Cell::new(TrieCacheMode::CachingShard),
            chunk_cache: RefCell::new(Default::default()),
            db_read_nodes: Cell::new(0),
            mem_read_nodes: Cell::new(0),
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

    fn inc_db_read_nodes(&self) {
        self.db_read_nodes.set(self.db_read_nodes.get() + 1);
    }

    fn inc_mem_read_nodes(&self) {
        self.mem_read_nodes.set(self.mem_read_nodes.get() + 1);
    }

    /// Set cache mode.
    pub fn set_mode(&self, state: TrieCacheMode) {
        self.cache_mode.set(state);
    }
}

impl TrieStorage for TrieCachingStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
        // Try to get value from chunk cache containing nodes with cheaper access. We can do it for any `TrieCacheMode`,
        // because we charge for reading nodes only when `CachingChunk` mode is enabled anyway.
        if let Some(val) = self.chunk_cache.borrow_mut().get(hash) {
            self.inc_mem_read_nodes();
            return Ok(val.clone());
        }

        // Try to get value from shard cache containing most recently touched nodes.
        let mut guard = self.shard_cache.0.lock().expect(POISONED_LOCK_ERR);
        let val = match guard.get(hash) {
            Some(val) => {
                near_o11y::io_trace!(count: "shard_cache_hit");
                val.clone()
            }
            None => {
                near_o11y::io_trace!(count: "shard_cache_miss");
                // If value is not present in cache, get it from the storage.
                let key = Self::get_key_from_shard_uid_and_hash(self.shard_uid, hash);
                let val = self
                    .store
                    .get(DBCol::State, key.as_ref())
                    .map_err(|_| StorageError::StorageInternalError)?
                    .ok_or_else(|| {
                        StorageError::StorageInconsistentState("Trie node missing".to_string())
                    })?;
                let val: Arc<[u8]> = val.into();

                // Insert value to shard cache, if its size is small enough.
                // It is fine to have a size limit for shard cache and **not** have a limit for chunk cache, because key
                // is always a value hash, so for each key there could be only one value, and it is impossible to have
                // **different** values for the given key in shard and chunk caches.
                if val.len() < SHARD_CACHE_VALUE_SIZE_LIMIT {
                    guard.put(*hash, val.clone());
                } else {
                    near_o11y::io_trace!(count: "shard_cache_too_large");
                }

                val
            }
        };

        // Because node is not present in chunk cache, increment the nodes counter and optionally insert it into the
        // chunk cache.
        // Note that we don't have a size limit for values in the chunk cache. There are two reasons:
        // - for nodes, value size is an implementation detail. If we change internal representation of a node (e.g.
        // change `memory_usage` field from `RawTrieNodeWithSize`), this would have to be a protocol upgrade.
        // - total size of all values is limited by the runtime fees. More thoroughly:
        // - - number of nodes is limited by receipt gas limit / touching trie node fee ~= 500 Tgas / 16 Ggas = 31_250;
        // - - size of trie keys and values is limited by receipt gas limit / lowest per byte fee
        // (`storage_read_value_byte`) ~= (500 * 10**12 / 5611005) / 2**20 ~= 85 MB.
        // All values are given as of 16/03/2022. We may consider more precise limit for the chunk cache as well.
        self.inc_db_read_nodes();
        if let TrieCacheMode::CachingChunk = self.cache_mode.borrow().get() {
            self.chunk_cache.borrow_mut().insert(*hash, val.clone());
        };

        Ok(val)
    }

    fn as_caching_storage(&self) -> Option<&TrieCachingStorage> {
        Some(self)
    }

    fn get_trie_nodes_count(&self) -> TrieNodesCount {
        TrieNodesCount { db_reads: self.db_read_nodes.get(), mem_reads: self.mem_read_nodes.get() }
    }
}

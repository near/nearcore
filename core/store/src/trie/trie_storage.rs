use crate::adapter::trie_store::TrieStoreAdapter;
use crate::trie::config::TrieConfig;
use crate::trie::prefetching_trie_storage::PrefetcherResult;
use crate::trie::POISONED_LOCK_ERR;
use crate::{metrics, MissingTrieValueContext, PrefetchApi, StorageError};
use lru::LruCache;
use near_o11y::log_assert;
use near_o11y::metrics::prometheus;
use near_o11y::metrics::prometheus::core::{GenericCounter, GenericGauge};
use near_primitives::challenge::PartialState;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::ShardId;
use std::collections::{HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

pub(crate) struct BoundedQueue<T> {
    queue: VecDeque<T>,
    /// If queue size exceeds capacity, item from the tail is removed.
    capacity: usize,
}

impl<T> BoundedQueue<T> {
    pub(crate) fn new(capacity: usize) -> Self {
        // Reserve space for one extra element to simplify `put`.
        Self { queue: VecDeque::with_capacity(capacity + 1), capacity }
    }

    pub(crate) fn clear(&mut self) {
        self.queue.clear();
    }

    pub(crate) fn pop(&mut self) -> Option<T> {
        self.queue.pop_front()
    }

    pub(crate) fn put(&mut self, key: T) -> Option<T> {
        self.queue.push_back(key);
        if self.queue.len() > self.capacity {
            Some(self.pop().expect("Queue cannot be empty"))
        } else {
            None
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.queue.len()
    }
}

/// In-memory cache for trie items - nodes and values. All nodes are stored in the LRU cache with three modifications.
/// 1) Size of each value must not exceed `TRIE_LIMIT_CACHED_VALUE_SIZE`.
/// Needed to avoid caching large values like contract codes.
/// 2) If we put new value to LRU cache and total size of existing values exceeds `total_sizes_capacity`, we evict
/// values from it until that is no longer the case. So the actual total size should never exceed
/// `total_size_limit` + `TRIE_LIMIT_CACHED_VALUE_SIZE`.
/// Needed because value sizes generally vary from 1 B to 500 B and we want to count cache size precisely.
/// 3) If value is popped, it is put to the `deletions` queue with `deletions_queue_capacity` first. If popped value
/// doesn't fit in the queue, the last value is removed from the queue and LRU cache, and newly popped value is inserted
/// to the queue.
/// Needed to delay deletions when we have forks. In such case, many blocks can share same parent, and we want to keep
/// old nodes in cache for a while to process all new roots. For example, it helps to read old state root.
pub struct TrieCacheInner {
    /// LRU cache keeping mapping from keys to values.
    cache: LruCache<CryptoHash, Arc<[u8]>>,
    /// Queue of items which were popped, which postpones deletion of old nodes.
    deletions: BoundedQueue<CryptoHash>,
    /// Current total size of all values in the cache.
    total_size: u64,
    /// Upper bound for the total size.
    total_size_limit: u64,
    /// Shard id of the nodes being cached.
    shard_id: ShardId,
    /// Whether cache is used for view calls execution.
    is_view: bool,
    // Counters tracking operations happening inside the shard cache.
    // Stored here to avoid overhead of looking them up on hot paths.
    metrics: TrieCacheMetrics,
}

struct TrieCacheMetrics {
    shard_cache_too_large: GenericCounter<prometheus::core::AtomicU64>,
    shard_cache_pop_hits: GenericCounter<prometheus::core::AtomicU64>,
    shard_cache_pop_misses: GenericCounter<prometheus::core::AtomicU64>,
    shard_cache_pop_lru: GenericCounter<prometheus::core::AtomicU64>,
    shard_cache_gc_pop_misses: GenericCounter<prometheus::core::AtomicU64>,
    shard_cache_deletions_size: GenericGauge<prometheus::core::AtomicI64>,
}

impl TrieCacheInner {
    /// Assumed number of bytes used to store an entry in the cache.
    ///
    /// 100 bytes is an approximation based on lru 0.7.5.
    pub(crate) const PER_ENTRY_OVERHEAD: u64 = 100;

    pub(crate) fn new(
        deletions_queue_capacity: usize,
        total_size_limit: u64,
        shard_id: ShardId,
        is_view: bool,
    ) -> Self {
        assert!(total_size_limit > 0);
        // `itoa` is much faster for printing shard_id to a string than trivial alternatives.
        let mut buffer = itoa::Buffer::new();
        let shard_id_str = buffer.format(shard_id.get());

        let metrics_labels: [&str; 2] = [&shard_id_str, if is_view { "1" } else { "0" }];
        let metrics = TrieCacheMetrics {
            shard_cache_too_large: metrics::SHARD_CACHE_TOO_LARGE
                .with_label_values(&metrics_labels),
            shard_cache_pop_hits: metrics::SHARD_CACHE_POP_HITS.with_label_values(&metrics_labels),
            shard_cache_pop_misses: metrics::SHARD_CACHE_POP_MISSES
                .with_label_values(&metrics_labels),
            shard_cache_pop_lru: metrics::SHARD_CACHE_POP_LRU.with_label_values(&metrics_labels),
            shard_cache_gc_pop_misses: metrics::SHARD_CACHE_GC_POP_MISSES
                .with_label_values(&metrics_labels),
            shard_cache_deletions_size: metrics::SHARD_CACHE_DELETIONS_SIZE
                .with_label_values(&metrics_labels),
        };
        // Assuming the values are actual all empty and we store a full hashmap of overhead.
        let max_elements = total_size_limit.div_ceil(Self::PER_ENTRY_OVERHEAD);
        let max_elements = usize::try_from(max_elements).unwrap();
        Self {
            cache: LruCache::new(NonZeroUsize::new(max_elements).unwrap()),
            deletions: BoundedQueue::new(deletions_queue_capacity),
            total_size: 0,
            total_size_limit,
            shard_id,
            is_view,
            metrics,
        }
    }

    pub(crate) fn get(&mut self, key: &CryptoHash) -> Option<Arc<[u8]>> {
        self.cache.get(key).cloned()
    }

    pub(crate) fn clear(&mut self) {
        self.total_size = 0;
        self.deletions.clear();
        self.cache.clear();
    }

    pub(crate) fn put(&mut self, key: CryptoHash, value: Arc<[u8]>) {
        while self.total_size > self.total_size_limit || self.cache.len() == self.cache.cap().get()
        {
            // First, try to evict value using the key from deletions queue.
            match self.deletions.pop() {
                Some(key) => match self.cache.pop(&key) {
                    Some(value) => {
                        self.metrics.shard_cache_pop_hits.inc();
                        self.remove_value_of_size(value.len());
                        continue;
                    }
                    None => {
                        self.metrics.shard_cache_pop_misses.inc();
                    }
                },
                None => {}
            }

            // Second, pop LRU value.
            self.metrics.shard_cache_pop_lru.inc();
            let (_, value) =
                self.cache.pop_lru().expect("Cannot fail because total size capacity is > 0");
            self.remove_value_of_size(value.len());
        }

        // Add value to the cache.
        self.add_value_of_size(value.len());
        match self.cache.push(key, value) {
            Some((evicted_key, evicted_value)) => {
                log_assert!(key == evicted_key, "LRU cache with shard_id = {}, is_view = {} can't be full before inserting key {}", self.shard_id, self.is_view, key);
                self.remove_value_of_size(evicted_value.len());
            }
            None => {}
        };
    }

    // Adds key to the deletions queue if it is present in cache.
    // Returns key-value pair which are popped if deletions queue is full.
    pub(crate) fn pop(&mut self, key: &CryptoHash) -> Option<(CryptoHash, Arc<[u8]>)> {
        self.metrics.shard_cache_deletions_size.set(self.deletions.len() as i64);
        // Do nothing if key was removed before.
        if self.cache.contains(key) {
            // Put key to the queue of deletions and possibly remove another key we have to delete.
            match self.deletions.put(*key) {
                Some(key_to_delete) => match self.cache.pop(&key_to_delete) {
                    Some(evicted_value) => {
                        self.metrics.shard_cache_pop_hits.inc();
                        self.remove_value_of_size(evicted_value.len());
                        Some((key_to_delete, evicted_value))
                    }
                    None => {
                        self.metrics.shard_cache_pop_misses.inc();
                        None
                    }
                },
                None => None,
            }
        } else {
            self.metrics.shard_cache_gc_pop_misses.inc();
            None
        }
    }

    /// Number of currently cached entries.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Account consumed memory for a new entry in the cache.
    pub(crate) fn add_value_of_size(&mut self, len: usize) {
        self.total_size += Self::entry_size(len);
    }

    /// Remove consumed memory for an entry in the cache.
    pub(crate) fn remove_value_of_size(&mut self, len: usize) {
        self.total_size -= Self::entry_size(len);
    }

    /// Approximate memory consumption of LRU cache.
    pub fn current_total_size(&self) -> u64 {
        self.total_size
    }

    fn entry_size(len: usize) -> u64 {
        len as u64 + Self::PER_ENTRY_OVERHEAD
    }
}

/// Wrapper over LruCache to handle concurrent access.
#[derive(Clone)]
pub struct TrieCache(pub(crate) Arc<Mutex<TrieCacheInner>>);

impl TrieCache {
    pub fn new(config: &TrieConfig, shard_uid: ShardUId, is_view: bool) -> Self {
        let cache_config =
            if is_view { &config.view_shard_cache_config } else { &config.shard_cache_config };
        let total_size_limit = cache_config
            .per_shard_max_bytes
            .get(&shard_uid)
            .copied()
            .unwrap_or(cache_config.default_max_bytes)
            .as_u64();
        let queue_capacity = config.deletions_queue_capacity();
        Self(Arc::new(Mutex::new(TrieCacheInner::new(
            queue_capacity,
            total_size_limit,
            shard_uid.shard_id(),
            is_view,
        ))))
    }

    pub fn get(&self, key: &CryptoHash) -> Option<Arc<[u8]>> {
        self.lock().get(key)
    }

    pub fn clear(&self) {
        self.lock().clear()
    }

    pub fn update_cache(&self, ops: Vec<(&CryptoHash, Option<&[u8]>)>) {
        let mut guard = self.lock();
        for (hash, opt_value) in ops {
            if let Some(value) = opt_value {
                if value.len() < TrieConfig::max_cached_value_size() {
                    guard.put(*hash, value.into());
                } else {
                    guard.pop(&hash);
                    guard.metrics.shard_cache_too_large.inc();
                }
            } else {
                guard.pop(&hash);
            }
        }
    }

    pub(crate) fn lock(&self) -> std::sync::MutexGuard<TrieCacheInner> {
        self.0.lock().expect(POISONED_LOCK_ERR)
    }
}

pub trait TrieStorage: Send + Sync {
    /// Get bytes of a serialized `TrieNode`.
    ///
    /// # Errors
    ///
    /// [`StorageError`] if the storage fails internally or the hash is not present.
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError>;

    /// DEPRECATED.
    /// Returns `TrieCachingStorage` if `TrieStorage` is implemented by it.
    /// TODO (#9004) remove all remaining calls.
    fn as_caching_storage(&self) -> Option<&TrieCachingStorage> {
        None
    }

    fn as_partial_storage(&self) -> Option<&TrieMemoryPartialStorage> {
        None
    }
}

/// Storage for validating recorded partial storage.
/// visited_nodes are to validate that partial storage doesn't contain unnecessary nodes.
#[derive(Default)]
pub struct TrieMemoryPartialStorage {
    pub(crate) recorded_storage: HashMap<CryptoHash, Arc<[u8]>>,
    pub(crate) visited_nodes: std::sync::RwLock<HashSet<CryptoHash>>,
}

impl TrieStorage for TrieMemoryPartialStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
        let result =
            self.recorded_storage.get(hash).cloned().ok_or(StorageError::MissingTrieValue(
                MissingTrieValueContext::TrieMemoryPartialStorage,
                *hash,
            ));
        if result.is_ok() {
            self.visited_nodes.write().expect("write visited_nodes").insert(*hash);
        }
        result
    }

    fn as_partial_storage(&self) -> Option<&TrieMemoryPartialStorage> {
        Some(self)
    }
}

impl TrieMemoryPartialStorage {
    pub fn new(recorded_storage: HashMap<CryptoHash, Arc<[u8]>>) -> Self {
        Self { recorded_storage, visited_nodes: Default::default() }
    }

    pub fn partial_state(&self) -> PartialState {
        let touched_nodes = self.visited_nodes.read().expect("read visited_nodes");
        let mut nodes: Vec<_> =
            self.recorded_storage
                .iter()
                .filter_map(|(node_hash, value)| {
                    if touched_nodes.contains(node_hash) {
                        Some(value.clone())
                    } else {
                        None
                    }
                })
                .collect();

        nodes.sort();
        PartialState::TrieValues(nodes)
    }
}

/// Storage for reading State nodes and values from DB which caches reads.
///
/// Important: The TrieCachingStorage contains the shard cache, which is
/// different from the accounting cache. The former is a best-effort
/// optimization to speed up execution, whereas the latter is a deterministic
/// cache used for gas accounting during contract execution.
pub struct TrieCachingStorage {
    pub(crate) store: TrieStoreAdapter,
    pub(crate) shard_uid: ShardUId,
    pub(crate) is_view: bool,

    /// Caches ever requested items for the shard `shard_uid`. Used to speed up DB operations, presence of any item is
    /// not guaranteed.
    pub(crate) shard_cache: TrieCache,

    /// The entry point for the runtime to submit prefetch requests.
    pub(crate) prefetch_api: Option<PrefetchApi>,

    // Counters tracking operations happening inside the shard cache.
    // Stored here to avoid overhead of looking them up on hot paths.
    metrics: TrieCacheInnerMetrics,
}

struct TrieCacheInnerMetrics {
    shard_cache_hits: GenericCounter<prometheus::core::AtomicU64>,
    shard_cache_misses: GenericCounter<prometheus::core::AtomicU64>,
    shard_cache_too_large: GenericCounter<prometheus::core::AtomicU64>,
    shard_cache_size: GenericGauge<prometheus::core::AtomicI64>,
    shard_cache_current_total_size: GenericGauge<prometheus::core::AtomicI64>,
    prefetch_hits: GenericCounter<prometheus::core::AtomicU64>,
    prefetch_pending: GenericCounter<prometheus::core::AtomicU64>,
    prefetch_not_requested: GenericCounter<prometheus::core::AtomicU64>,
    prefetch_memory_limit_reached: GenericCounter<prometheus::core::AtomicU64>,
    prefetch_retry: GenericCounter<prometheus::core::AtomicU64>,
    prefetch_conflict: GenericCounter<prometheus::core::AtomicU64>,
}

impl TrieCachingStorage {
    pub fn new(
        store: TrieStoreAdapter,
        shard_cache: TrieCache,
        shard_uid: ShardUId,
        is_view: bool,
        prefetch_api: Option<PrefetchApi>,
    ) -> TrieCachingStorage {
        // `itoa` is much faster for printing shard_id to a string than trivial alternatives.
        let mut buffer = itoa::Buffer::new();
        let shard_id = buffer.format(shard_uid.shard_id);

        let metrics_labels: [&str; 2] = [&shard_id, if is_view { "1" } else { "0" }];
        let metrics = TrieCacheInnerMetrics {
            shard_cache_hits: metrics::SHARD_CACHE_HITS.with_label_values(&metrics_labels),
            shard_cache_misses: metrics::SHARD_CACHE_MISSES.with_label_values(&metrics_labels),
            shard_cache_too_large: metrics::SHARD_CACHE_TOO_LARGE
                .with_label_values(&metrics_labels),
            shard_cache_size: metrics::SHARD_CACHE_SIZE.with_label_values(&metrics_labels),
            shard_cache_current_total_size: metrics::SHARD_CACHE_CURRENT_TOTAL_SIZE
                .with_label_values(&metrics_labels),
            prefetch_hits: metrics::PREFETCH_HITS.with_label_values(&metrics_labels[..1]),
            prefetch_pending: metrics::PREFETCH_PENDING.with_label_values(&metrics_labels[..1]),
            prefetch_not_requested: metrics::PREFETCH_NOT_REQUESTED
                .with_label_values(&metrics_labels[..1]),
            prefetch_memory_limit_reached: metrics::PREFETCH_MEMORY_LIMIT_REACHED
                .with_label_values(&metrics_labels[..1]),
            prefetch_retry: metrics::PREFETCH_RETRY.with_label_values(&metrics_labels[..1]),
            prefetch_conflict: metrics::PREFETCH_CONFLICT.with_label_values(&metrics_labels[..1]),
        };
        TrieCachingStorage { store, shard_uid, is_view, shard_cache, prefetch_api, metrics }
    }

    /// Reads value if it is not in shard cache. Handles dropping the cache
    /// lock. Either waits for prefetcher to fetch it or reads it from DB.
    /// It is responsibility of caller to release the prefetch slot later.
    fn read_for_shard_cache_miss(
        &self,
        guard: std::sync::MutexGuard<TrieCacheInner>,
        hash: &CryptoHash,
    ) -> Result<Arc<[u8]>, StorageError> {
        let Some(prefetcher) = &self.prefetch_api else {
            std::mem::drop(guard);
            return self.read_from_db(hash);
        };

        let prefetch_state = prefetcher.prefetching.get_or_set_fetching(*hash);
        // Drop lock only here to avoid race condition between shard cache
        // lookup and reserving prefetch slot.
        std::mem::drop(guard);

        let val = match prefetch_state {
            // Slot reserved for us, the main thread.
            // `SlotReserved` for the main thread means, either we have not submitted a prefetch request for
            // this value, or maybe it is just still queued up. Either way, prefetching is not going to help
            // so the main thread should fetch data from DB on its own.
            PrefetcherResult::SlotReserved => {
                self.metrics.prefetch_not_requested.inc();
                self.read_from_db(hash)?
            }
            // `MemoryLimitReached` is not really relevant for the main thread,
            // we always have to go to DB even if we could not stage a new prefetch.
            // It only means we were not able to mark it as already being fetched, which in turn could lead to
            // a prefetcher trying to fetch the same value before we can put it in the shard cache.
            PrefetcherResult::MemoryLimitReached => {
                self.metrics.prefetch_memory_limit_reached.inc();
                self.read_from_db(hash)?
            }
            PrefetcherResult::Prefetched(value) => {
                near_o11y::io_trace!(count: "prefetch_hit");
                self.metrics.prefetch_hits.inc();
                value
            }
            PrefetcherResult::Pending => {
                near_o11y::io_trace!(count: "prefetch_pending");
                self.metrics.prefetch_pending.inc();
                std::thread::yield_now();
                // If data is already being prefetched, wait for that instead of sending a new request.
                match prefetcher.prefetching.blocking_get(*hash) {
                    Some(value) => value,
                    // Only main thread (this one) removes values from staging area,
                    // therefore blocking read will usually not return empty unless there
                    // was a storage error. Or in the case of forks and parallel chunk
                    // processing where one chunk cleans up prefetched data from the other.
                    // So first we need to check if the data was inserted to shard_cache
                    // by the main thread from another fork and only if that fails then
                    // fetch the data from the DB.
                    None => {
                        if let Some(value) = self.shard_cache.get(hash) {
                            self.metrics.prefetch_conflict.inc();
                            value
                        } else {
                            self.metrics.prefetch_retry.inc();
                            self.read_from_db(hash)?
                        }
                    }
                }
            }
        };
        Ok(val)
    }
}

impl TrieStorage for TrieCachingStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
        // Try to get value from shard cache containing most recently touched nodes.
        let mut guard = self.shard_cache.lock();
        self.metrics.shard_cache_size.set(guard.len() as i64);
        self.metrics.shard_cache_current_total_size.set(guard.current_total_size() as i64);
        if let Some(val) = guard.get(hash) {
            self.metrics.shard_cache_hits.inc();
            near_o11y::io_trace!(count: "shard_cache_hit");
            return Ok(val);
        }

        self.metrics.shard_cache_misses.inc();
        near_o11y::io_trace!(count: "shard_cache_miss");
        let val = match self.read_for_shard_cache_miss(guard, hash) {
            Ok(val) => val,
            Err(e) => {
                // Only release after attempt to read the value. See comment on fn release.
                if let Some(prefetcher) = &self.prefetch_api {
                    prefetcher.prefetching.release(hash);
                }
                return Err(e);
            }
        };

        // Insert value to shard cache, if its size is small enough.
        // It is fine to have a size limit for shard cache and **not** have a limit for accounting cache, because key
        // is always a value hash, so for each key there could be only one value, and it is impossible to have
        // **different** values for the given key in shard and accounting caches.
        if val.len() < TrieConfig::max_cached_value_size() {
            let mut guard = self.shard_cache.lock();
            guard.put(*hash, val.clone());
        } else {
            self.metrics.shard_cache_too_large.inc();
            near_o11y::io_trace!(count: "shard_cache_too_large");
        }

        if let Some(prefetcher) = &self.prefetch_api {
            // Only release after insertion in shard cache. See comment on fn release.
            prefetcher.prefetching.release(hash);
        }

        Ok(val)
    }

    fn as_caching_storage(&self) -> Option<&TrieCachingStorage> {
        Some(self)
    }
}

impl TrieCachingStorage {
    fn read_from_db(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
        self.store.get(self.shard_uid, hash)
    }

    pub fn prefetch_api(&self) -> &Option<PrefetchApi> {
        &self.prefetch_api
    }
}

/// Storage for reading State nodes and values directly from DB.
///
/// This `TrieStorage` implementation has no caches, it just goes to DB.
/// It is useful for background tasks that should not affect chunk processing and block each other.
pub struct TrieDBStorage {
    pub(crate) store: TrieStoreAdapter,
    pub(crate) shard_uid: ShardUId,
}

impl TrieDBStorage {
    pub fn new(store: TrieStoreAdapter, shard_uid: ShardUId) -> Self {
        Self { store, shard_uid }
    }
}

impl TrieStorage for TrieDBStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
        self.store.get(self.shard_uid, hash)
    }
}

#[cfg(test)]
mod bounded_queue_tests {
    use crate::trie::trie_storage::BoundedQueue;

    #[test]
    fn test_put_pop() {
        let mut queue = BoundedQueue::new(2);
        assert_eq!(queue.put(1), None);
        assert_eq!(queue.put(2), None);
        assert_eq!(queue.put(3), Some(1));

        assert_eq!(queue.pop(), Some(2));
        assert_eq!(queue.pop(), Some(3));
        assert_eq!(queue.pop(), None);
    }

    #[test]
    fn test_clear() {
        let mut queue = BoundedQueue::new(2);
        queue.put(1);
        assert_eq!(queue.queue.get(0), Some(&1));
        queue.clear();
        assert!(queue.queue.is_empty());
    }

    #[test]
    fn test_zero_capacity() {
        let mut queue = BoundedQueue::new(0);
        assert_eq!(queue.put(1), Some(1));
        assert!(queue.queue.is_empty());
    }
}

#[cfg(test)]
mod trie_cache_tests {
    use crate::trie::trie_storage::TrieCacheInner;
    use crate::{StoreConfig, TrieCache, TrieConfig};
    use near_primitives::hash::hash;
    use near_primitives::shard_layout::ShardUId;
    use near_primitives::types::ShardId;

    fn put_value(cache: &mut TrieCacheInner, value: &[u8]) {
        cache.put(hash(value), value.into());
    }

    #[test]
    fn test_size_limit() {
        let value_size_sum = 5;
        let memory_overhead = 2 * TrieCacheInner::PER_ENTRY_OVERHEAD;
        let mut cache = TrieCacheInner::new(100, value_size_sum + memory_overhead, 0, false);
        // Add three values. Before each put, condition on total size should not be triggered.
        put_value(&mut cache, &[1, 1]);
        assert_eq!(cache.current_total_size(), 2 + TrieCacheInner::PER_ENTRY_OVERHEAD);
        put_value(&mut cache, &[1, 1, 1]);
        assert_eq!(cache.current_total_size(), 5 + 2 * TrieCacheInner::PER_ENTRY_OVERHEAD);
        put_value(&mut cache, &[1]);
        assert_eq!(cache.current_total_size(), 6 + 3 * TrieCacheInner::PER_ENTRY_OVERHEAD);

        // Add one of previous values. 2 LRU values should be evicted.
        put_value(&mut cache, &[1, 1, 1]);
        assert_eq!(cache.current_total_size(), 4 + 2 * TrieCacheInner::PER_ENTRY_OVERHEAD);
        assert_eq!(cache.cache.pop_lru(), Some((hash(&[1]), vec![1].into())));
        assert_eq!(cache.cache.pop_lru(), Some((hash(&[1, 1, 1]), vec![1, 1, 1].into())));
    }

    #[test]
    fn test_deletions_queue() {
        let mut cache = TrieCacheInner::new(2, 1000, 0, false);
        // Add two values to the cache.
        put_value(&mut cache, &[1]);
        put_value(&mut cache, &[1, 1]);

        // Call pop for inserted values. Because deletions queue is not full, no elements should be deleted.
        assert_eq!(cache.pop(&hash(&[1, 1])), None);
        assert_eq!(cache.pop(&hash(&[1])), None);

        // Call pop two times for a value existing in cache. Because queue is full, both elements should be deleted.
        assert_eq!(cache.pop(&hash(&[1])), Some((hash(&[1, 1]), vec![1, 1].into())));
        assert_eq!(cache.pop(&hash(&[1])), Some((hash(&[1]), vec![1].into())));
    }

    /// test implicit capacity limit imposed by memory limit
    #[test]
    fn test_cache_capacity() {
        let capacity = 2;
        let total_size_limit = TrieCacheInner::PER_ENTRY_OVERHEAD * capacity;
        let mut cache = TrieCacheInner::new(100, total_size_limit, 0, false);
        put_value(&mut cache, &[1]);
        put_value(&mut cache, &[2]);
        put_value(&mut cache, &[3]);

        assert!(!cache.cache.contains(&hash(&[1])));
        assert!(cache.cache.contains(&hash(&[2])));
        assert!(cache.cache.contains(&hash(&[3])));
    }

    #[test]
    fn test_small_memory_limit() {
        let total_size_limit = 1;
        let mut cache = TrieCacheInner::new(100, total_size_limit, 0, false);
        put_value(&mut cache, &[1, 2, 3]);
        put_value(&mut cache, &[2, 3, 4]);
        put_value(&mut cache, &[3, 4, 5]);

        assert!(!cache.cache.contains(&hash(&[1, 2, 3])));
        assert!(!cache.cache.contains(&hash(&[2, 3, 4])));
        assert!(cache.cache.contains(&hash(&[3, 4, 5])));
    }

    /// Check that setting from `StoreConfig` are applied.
    #[test]
    fn test_trie_config() {
        let mut store_config = StoreConfig::default();

        const DEFAULT_SIZE: bytesize::ByteSize = bytesize::ByteSize(1);
        const S0_SIZE: bytesize::ByteSize = bytesize::ByteSize(2);
        const DEFAULT_VIEW_SIZE: bytesize::ByteSize = bytesize::ByteSize(3);
        const S0_VIEW_SIZE: bytesize::ByteSize = bytesize::ByteSize(4);

        let s0 = ShardUId::single_shard();
        store_config.trie_cache.default_max_bytes = DEFAULT_SIZE;
        store_config.trie_cache.per_shard_max_bytes.insert(s0, S0_SIZE);
        store_config.view_trie_cache.default_max_bytes = DEFAULT_VIEW_SIZE;
        store_config.view_trie_cache.per_shard_max_bytes.insert(s0, S0_VIEW_SIZE);
        let trie_config = TrieConfig::from_store_config(&store_config);

        check_cache_size(&trie_config, 1, false, DEFAULT_SIZE);
        check_cache_size(&trie_config, 0, false, S0_SIZE);
        check_cache_size(&trie_config, 1, true, DEFAULT_VIEW_SIZE);
        check_cache_size(&trie_config, 0, true, S0_VIEW_SIZE);
    }

    #[track_caller]
    fn check_cache_size(
        trie_config: &TrieConfig,
        shard_id: ShardId,
        is_view: bool,
        expected_size: bytesize::ByteSize,
    ) {
        let shard_uid = ShardUId { version: 0, shard_id: shard_id as u32 };
        let trie_cache = TrieCache::new(&trie_config, shard_uid, is_view);
        assert_eq!(expected_size.as_u64(), trie_cache.lock().total_size_limit);
        assert_eq!(is_view, trie_cache.lock().is_view);
    }
}

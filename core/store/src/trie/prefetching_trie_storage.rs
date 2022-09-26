use crate::trie::POISONED_LOCK_ERR;
use crate::{
    metrics, DBCol, StorageError, Store, Trie, TrieCache, TrieCachingStorage, TrieConfig,
    TrieStorage,
};
use near_o11y::metrics::prometheus;
use near_o11y::metrics::prometheus::core::GenericGauge;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{AccountId, ShardId, StateRoot, TrieNodesCount};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::error;

const MAX_QUEUED_WORK_ITEMS: usize = 16 * 1024;
const MAX_PREFETCH_STAGING_MEMORY: usize = 200 * 1024 * 1024;
/// How much memory capacity is reserved for each prefetch request before
/// sending it. Once the value is fetched, the actual size is used instead.
/// Set to 4MiB, the same as `max_length_storage_value`.
const PREFETCH_RESERVED_BYTES_PER_SLOT: usize = 4 * 1024 * 1024;
/// How many threads will be prefetching data, without the scheduler thread.
/// Because the storage driver is blocking, there is only one request per thread
/// at a time.
const NUM_IO_THREADS: usize = 8;

/// Storage used by I/O threads to prefetch data.
///
/// This implements `TrieStorage` and therefore can be used inside a `Trie`.
/// Prefetching runs through the normal trie lookup code and only the backing
/// trie storage behaves differently.
///
/// `TriePrefetchingStorage` instances are always linked to a parent
/// `TrieCachingStorage`. They share a shard cache, to avoid reading anything
/// from the DB that is already cached.
/// They communicate through `PrefetchStagingArea` exclusively.
///
/// Each I/O threads will have its own copy of `TriePrefetchingStorage`, so
/// this should remain a cheap object.
#[derive(Clone)]
struct TriePrefetchingStorage {
    /// Store is shared with parent `TrieCachingStorage`.
    store: Store,
    shard_uid: ShardUId,
    /// Shard cache is shared with parent `TrieCachingStorage`. But the
    /// pre-fetcher uses this in read-only mode to avoid premature evictions.
    shard_cache: TrieCache,
    /// Shared with parent `TrieCachingStorage`.
    prefetching: PrefetchStagingArea,
}

/// This type is shared between runtime crate and store crate.
///
/// The former puts requests in, the latter serves requests.
/// With this API, the store does not know about receipts etc, and the runtime
/// does not know about the trie structure. The only thing they share is this object.
#[derive(Clone)]
pub struct PrefetchApi {
    /// Bounded, shared queue for all IO threads to take work from.
    ///
    /// Work items are defined as `TrieKey` because currently the only
    /// work is to prefetch a trie key. If other IO work is added, consider
    /// changing the queue to an enum.
    /// The state root is also included because multiple chunks could be applied
    /// at the same time.
    work_queue: WorkQueue,
    /// Prefetching IO threads will insert fetched data here. This is also used
    /// to mark what is already being fetched, to avoid fetching the same data
    /// multiple times.
    pub(crate) prefetching: PrefetchStagingArea,

    pub enable_receipt_prefetching: bool,
    /// Configured accounts will be prefetched as SWEAT token account, if predecessor is listed as receiver.
    pub sweat_prefetch_receivers: Vec<AccountId>,
    /// List of allowed predecessor accounts for SWEAT prefetching.
    pub sweat_prefetch_senders: Vec<AccountId>,

    pub shard_uid: ShardUId,
}

/// Staging area for in-flight prefetch requests and a buffer for prefetched data.
///
/// Before starting a pre-fetch, a slot is reserved for it. Once the data is
/// here, it will be put in that slot. The parent `TrieCachingStorage` needs
/// to take it out and move it to the shard cache.
///
/// A shared staging area is the interface between `TrieCachingStorage` and
/// `TriePrefetchingStorage`. The parent simply checks the staging area before
/// going to the DB. Otherwise, no communication between the two is necessary.
///
/// This design also ensures the shard cache works exactly the same with or
/// without the prefetcher, because the order in which it sees accesses is
/// independent of the prefetcher.
#[derive(Clone)]
pub(crate) struct PrefetchStagingArea(Arc<Mutex<InnerPrefetchStagingArea>>);

struct InnerPrefetchStagingArea {
    slots: SizeTrackedHashMap,
}

/// Result when atomically accessing the prefetch staging area.
pub(crate) enum PrefetcherResult {
    SlotReserved,
    Pending,
    Prefetched(Arc<[u8]>),
    MemoryLimitReached,
}

struct StagedMetrics {
    prefetch_staged_bytes: GenericGauge<prometheus::core::AtomicI64>,
    prefetch_staged_items: GenericGauge<prometheus::core::AtomicI64>,
}

impl StagedMetrics {
    fn new(shard_id: ShardId) -> Self {
        Self {
            prefetch_staged_bytes: metrics::PREFETCH_STAGED_BYTES
                .with_label_values(&[&shard_id.to_string()]),
            prefetch_staged_items: metrics::PREFETCH_STAGED_SLOTS
                .with_label_values(&[&shard_id.to_string()]),
        }
    }
}

/// Type used internally in the staging area to keep track of requests.
#[derive(Clone, Debug)]
enum PrefetchSlot {
    PendingPrefetch,
    PendingFetch,
    Done(Arc<[u8]>),
}

struct SizeTrackedHashMap {
    map: HashMap<CryptoHash, PrefetchSlot>,
    size_bytes: usize,
    metrics: StagedMetrics,
}

impl SizeTrackedHashMap {
    fn insert(&mut self, k: CryptoHash, v: PrefetchSlot) -> Option<PrefetchSlot> {
        self.size_bytes += Self::reserved_memory(&v);
        let dropped = self.map.insert(k, v);
        if let Some(dropped) = &dropped {
            self.size_bytes -= Self::reserved_memory(dropped);
        }
        self.update_metrics();
        dropped
    }

    fn remove(&mut self, k: &CryptoHash) -> Option<PrefetchSlot> {
        let dropped = self.map.remove(k);
        if let Some(dropped) = &dropped {
            self.size_bytes -= Self::reserved_memory(dropped);
        }
        self.update_metrics();
        dropped
    }

    fn clear(&mut self) {
        self.map.clear();
        self.size_bytes = 0;
        self.update_metrics();
    }

    fn update_metrics(&self) {
        self.metrics.prefetch_staged_bytes.set(self.size_bytes as i64);
        self.metrics.prefetch_staged_items.set(self.map.len() as i64);
    }

    /// Reserved memory capacity for a value from the prefetching area.
    fn reserved_memory(slot: &PrefetchSlot) -> usize {
        match slot {
            PrefetchSlot::Done(value) => value.len(),
            PrefetchSlot::PendingFetch | PrefetchSlot::PendingPrefetch => {
                PREFETCH_RESERVED_BYTES_PER_SLOT
            }
        }
    }

    fn get(&self, key: &CryptoHash) -> Option<&PrefetchSlot> {
        self.map.get(key)
    }
}

impl TrieStorage for TriePrefetchingStorage {
    // Note: This is the tricky bit of the implementation.
    // We have to retrieve data only once in many threads, so all IO threads
    // have to go though the staging area and check for inflight requests.
    // The shard cache mutex plus the prefetch staging area mutex are used for
    // that in combination. Let's call the first lock S and the second P.
    // The rules for S and P are:
    // 1. To avoid deadlocks, S must always be requested before P, if they are
    //    held at the same time.
    // 2. When looking up if something is already in the shard cache, S must not
    //    be released until the staging area is updated by the current thread.
    //    Otherwise, there will be race conditions that could lead to multiple
    //    threads looking up the same value from DB.
    // 3. IO threads should release S and P as soon as possible, as they can
    //    block the main thread otherwise.
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
        // Try to get value from shard cache containing most recently touched nodes.
        let mut shard_cache_guard = self.shard_cache.0.lock().expect(POISONED_LOCK_ERR);
        if let Some(val) = shard_cache_guard.get(hash) {
            return Ok(val);
        }

        // If data is already being prefetched, wait for that instead of sending a new request.
        let prefetch_state =
            self.prefetching.get_and_set_if_empty(hash.clone(), PrefetchSlot::PendingPrefetch);
        // Keep lock until here to avoid race condition between shard cache insertion and reserving prefetch slot.
        std::mem::drop(shard_cache_guard);

        match prefetch_state {
            // Slot reserved for us, this thread should fetch it from DB.
            PrefetcherResult::SlotReserved => {
                let key = TrieCachingStorage::get_key_from_shard_uid_and_hash(self.shard_uid, hash);
                match self.store.get(DBCol::State, key.as_ref()) {
                    Ok(Some(value)) => {
                        let value: Arc<[u8]> = value.into();
                        self.prefetching.insert_fetched(hash.clone(), value.clone());
                        Ok(value)
                    }
                    Ok(None) => {
                        // This is an unrecoverable error, a hash found in the trie had no node.
                        // Releasing the lock here to unstuck main thread if it
                        // was blocking on this value, but it will also fail on its read.
                        self.prefetching.release(hash);
                        Err(StorageError::TrieNodeMissing)
                    }
                    Err(e) => {
                        // This is an unrecoverable IO error.
                        // Releasing the lock here to unstuck main thread if it
                        // was blocking on this value, but it will also fail on its read.
                        self.prefetching.release(hash);
                        Err(StorageError::StorageInconsistentState(e.to_string()))
                    }
                }
            }
            PrefetcherResult::Prefetched(value) => Ok(value),
            PrefetcherResult::Pending => {
                // yield once before calling `block_get` that will check for data to be present again.
                std::thread::yield_now();
                self.prefetching
                    .blocking_get(hash.clone())
                    .or_else(|| {
                        // `blocking_get` will return None if the prefetch slot has been removed
                        // by the main thread and the value inserted into the shard cache.
                        let mut guard = self.shard_cache.0.lock().expect(POISONED_LOCK_ERR);
                        guard.get(hash)
                    })
                    .ok_or_else(|| {
                        // This could only happen if this thread started prefetching a value
                        // while also another thread was already prefetching it. When the
                        // other thread finishes, the main thread takes it out, and moves it to
                        // the shard cache. And then this current thread gets delayed for long
                        // enough that the value gets evicted from the shard cache again before
                        // this thread has a chance to read it.
                        // In this rare occasion, we shall abort the current prefetch request and
                        // move on to the next.
                        StorageError::StorageInconsistentState(format!(
                            "Prefetcher failed on hash {hash}"
                        ))
                    })
            }
            PrefetcherResult::MemoryLimitReached => Err(StorageError::StorageInconsistentState(
                format!("Prefetcher failed due to memory limit hash {hash}"),
            )),
        }
    }

    fn get_trie_nodes_count(&self) -> TrieNodesCount {
        unimplemented!()
    }
}

impl TriePrefetchingStorage {
    pub(crate) fn new(
        store: Store,
        shard_uid: ShardUId,
        shard_cache: TrieCache,
        prefetching: PrefetchStagingArea,
    ) -> Self {
        Self { store, shard_uid, shard_cache, prefetching }
    }
}

impl PrefetchStagingArea {
    fn new(shard_id: ShardId) -> Self {
        let inner = InnerPrefetchStagingArea {
            slots: SizeTrackedHashMap {
                map: Default::default(),
                size_bytes: 0,
                metrics: StagedMetrics::new(shard_id),
            },
        };
        inner.slots.update_metrics();
        Self(Arc::new(Mutex::new(inner)))
    }

    /// Release a slot in the prefetcher staging area.
    ///
    /// This must only be called after inserting the value to the shard cache.
    /// Otherwise, the following scenario becomes possible:
    /// 1: Main thread removes a value from the prefetch staging area.
    /// 2: IO thread misses in the shard cache on the same key and starts fetching it again.
    /// 3: Main thread value is inserted in shard cache.
    pub(crate) fn release(&self, key: &CryptoHash) {
        let mut guard = self.0.lock().expect(POISONED_LOCK_ERR);
        let dropped = guard.slots.remove(key);
        // `Done` is the result after a successful prefetch.
        // `PendingFetch` means the value has been read without a prefetch.
        // `None` means prefetching was stopped due to memory limits.
        debug_assert!(
            dropped.is_none()
                || prefetch_state_matches(
                    PrefetchSlot::Done(Arc::new([])),
                    dropped.as_ref().unwrap()
                )
                || prefetch_state_matches(PrefetchSlot::PendingFetch, dropped.as_ref().unwrap()),
        );
    }

    /// Block until value is prefetched and then return it.
    ///
    /// Note: This function could return a future and become async.
    /// DB requests are all blocking, unfortunately, so the benefit seems small.
    /// The main benefit would be if many IO threads end up prefetching the
    /// same data and thus are waiting on each other rather than the DB.
    /// Of course, that would require prefetching to be moved into an async environment,
    pub(crate) fn blocking_get(&self, key: CryptoHash) -> Option<Arc<[u8]>> {
        loop {
            match self.0.lock().expect(POISONED_LOCK_ERR).slots.get(&key) {
                Some(PrefetchSlot::Done(value)) => return Some(value.clone()),
                Some(_) => (),
                None => return None,
            }
            std::thread::sleep(std::time::Duration::from_micros(1));
        }
    }

    /// Get prefetched value if available and otherwise atomically set
    /// prefetcher state to being fetched by main thread.
    pub(crate) fn get_or_set_fetching(&self, key: CryptoHash) -> PrefetcherResult {
        self.get_and_set_if_empty(key, PrefetchSlot::PendingFetch)
    }

    fn insert_fetched(&self, key: CryptoHash, value: Arc<[u8]>) {
        self.0.lock().expect(POISONED_LOCK_ERR).slots.insert(key, PrefetchSlot::Done(value));
    }

    /// Get prefetched value if available and otherwise atomically insert the
    /// given `PrefetchSlot` if no request is pending yet.
    fn get_and_set_if_empty(
        &self,
        key: CryptoHash,
        set_if_empty: PrefetchSlot,
    ) -> PrefetcherResult {
        let mut guard = self.0.lock().expect(POISONED_LOCK_ERR);
        let full =
            guard.slots.size_bytes > MAX_PREFETCH_STAGING_MEMORY - PREFETCH_RESERVED_BYTES_PER_SLOT;
        match guard.slots.map.get(&key) {
            Some(value) => match value {
                PrefetchSlot::Done(value) => PrefetcherResult::Prefetched(value.clone()),
                PrefetchSlot::PendingPrefetch | PrefetchSlot::PendingFetch => {
                    PrefetcherResult::Pending
                }
            },
            None => {
                if full {
                    return PrefetcherResult::MemoryLimitReached;
                }
                guard.slots.insert(key, set_if_empty);
                PrefetcherResult::SlotReserved
            }
        }
    }
}

impl PrefetchApi {
    pub fn new(
        store: Store,
        shard_cache: TrieCache,
        shard_uid: ShardUId,
        trie_config: &TrieConfig,
    ) -> Self {
        let (tx, rx) = crossbeam::channel::bounded(MAX_QUEUED_WORK_ITEMS);
        let sweat_prefetch_receivers = trie_config.sweat_prefetch_receivers.clone();
        let sweat_prefetch_senders = trie_config.sweat_prefetch_senders.clone();
        let enable_receipt_prefetching = trie_config.enable_receipt_prefetching;
        let prefetching = PrefetchStagingArea::new(shard_uid.shard_id());

        let handles = (0..NUM_IO_THREADS)
            .map(|_| {
                Self::start_io_thread(
                    rx.clone(),
                    prefetching.clone(),
                    store.clone(),
                    shard_cache.clone(),
                    shard_uid.clone(),
                )
            })
            .collect();
        // Do not clone tx before this point, or `WorkQueue` invariant is broken.
        let work_queue = WorkQueue { rx, tx, _handles: Arc::new(JoinGuard(handles)) };
        Self {
            work_queue,
            prefetching,
            enable_receipt_prefetching,
            sweat_prefetch_receivers,
            sweat_prefetch_senders,
            shard_uid,
        }
    }

    /// Returns the argument back if queue is full.
    pub fn prefetch_trie_key(
        &self,
        root: StateRoot,
        trie_key: TrieKey,
    ) -> Result<(), (StateRoot, TrieKey)> {
        self.work_queue.tx.send((root, trie_key)).map_err(|e| e.0)
    }

    fn start_io_thread(
        work_queue: crossbeam::channel::Receiver<(StateRoot, TrieKey)>,
        prefetching: PrefetchStagingArea,
        store: Store,
        shard_cache: TrieCache,
        shard_uid: ShardUId,
    ) -> std::thread::JoinHandle<()> {
        let prefetcher_storage =
            TriePrefetchingStorage::new(store, shard_uid, shard_cache, prefetching);
        let metric_prefetch_sent =
            metrics::PREFETCH_SENT.with_label_values(&[&shard_uid.shard_id.to_string()]);
        let metric_prefetch_fail =
            metrics::PREFETCH_FAIL.with_label_values(&[&shard_uid.shard_id.to_string()]);
        std::thread::spawn(move || {
            while let Ok((trie_root, trie_key)) = work_queue.recv() {
                // Since the trie root can change,and since the root is not known at the time when the IO threads starts,
                // we need to redefine the trie before each request.
                // Note that the constructor of `Trie` is trivial, and the clone only clones a few `Arc`s, so the performance hit is small.
                let prefetcher_trie =
                    Trie::new(Box::new(prefetcher_storage.clone()), trie_root, None);
                let storage_key = trie_key.to_vec();
                metric_prefetch_sent.inc();
                if let Ok(_maybe_value) = prefetcher_trie.get(&storage_key) {
                    near_o11y::io_trace!(count: "prefetch");
                } else {
                    // This may happen in rare occasions and can be ignored safely.
                    // See comments in `TriePrefetchingStorage::retrieve_raw_bytes`.
                    near_o11y::io_trace!(count: "prefetch_failure");
                    metric_prefetch_fail.inc();
                }
            }
        })
    }

    /// Remove queued up requests so IO threads will be paused after they finish their current task.
    ///
    /// Queued up work will not be finished. But trie keys that are already
    /// being fetched will finish.
    pub fn clear_queue(&self) {
        while let Ok(_dropped) = self.work_queue.rx.try_recv() {}
    }

    /// Clear prefetched staging area from data that has not been picked up by the main thread.
    pub fn clear_data(&self) {
        self.prefetching.0.lock().expect(POISONED_LOCK_ERR).slots.clear();
    }
}

/// Bounded, shared queue for all IO threads to take work from.
///
/// Work items are defined as `TrieKey` because currently the only
/// work is to prefetch a trie key. If other IO work is added, consider
/// changing the queue to an enum.
/// The state root is also included because multiple chunks could be applied
/// at the same time.
#[derive(Clone)]
struct WorkQueue {
    /// The channel to the IO prefetch work queue.
    rx: crossbeam::channel::Receiver<(StateRoot, TrieKey)>,
    tx: crossbeam::channel::Sender<(StateRoot, TrieKey)>,
    /// Thread handles for threads sitting behind channel.
    ///
    /// Invariant: The number of existing clones of `tx` is equal to
    /// the reference count of join handles.
    ///
    /// The invariant holds because when `WorkQueue` is created there is no
    /// clone of it, yet. And afterwards the only clones are through
    /// `WorkQueue.clone()` which also increases the handles reference count.
    ///
    /// When the last reference to `handles` is dropped, the handles
    /// are joined, which will terminate because the last `tx` has
    /// already been dropped (field order matters!) and therefore the crossbeam
    /// channel has been closed.
    _handles: Arc<JoinGuard>,
}

/// Only exists to implement `Drop`.
struct JoinGuard(Vec<std::thread::JoinHandle<()>>);

impl Drop for JoinGuard {
    fn drop(&mut self) {
        for handle in self.0.drain(..) {
            if let Err(e) = handle.join() {
                error!("Failed to join background thread: {e:?}")
            }
        }
    }
}

fn prefetch_state_matches(expected: PrefetchSlot, actual: &PrefetchSlot) -> bool {
    match (expected, actual) {
        (PrefetchSlot::PendingPrefetch, PrefetchSlot::PendingPrefetch)
        | (PrefetchSlot::PendingFetch, PrefetchSlot::PendingFetch)
        | (PrefetchSlot::Done(_), PrefetchSlot::Done(_)) => true,
        _ => false,
    }
}

/// Implementation to make testing from runtime possible.
///
/// Prefetching by design has no visible side-effects.
/// To nevertheless test the functionality on the API level,
/// a minimal set of functions is required to check the inner
/// state of the prefetcher.
#[cfg(feature = "test_features")]
mod tests {
    use super::{PrefetchApi, PrefetchSlot};
    use crate::TrieCachingStorage;

    impl PrefetchApi {
        /// Returns the number of prefetched values currently staged.
        pub fn num_prefetched_and_staged(&self) -> usize {
            self.prefetching
                .0
                .lock()
                .unwrap()
                .slots
                .map
                .iter()
                .filter(|(_key, slot)| match slot {
                    PrefetchSlot::PendingPrefetch | PrefetchSlot::PendingFetch => false,
                    PrefetchSlot::Done(_) => true,
                })
                .count()
        }

        pub fn work_queued(&self) -> bool {
            !self.work_queue.rx.is_empty()
        }
    }

    impl TrieCachingStorage {
        pub fn clear_cache(&self) {
            self.shard_cache.clear();
        }
    }
}

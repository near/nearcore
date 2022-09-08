use crate::trie::POISONED_LOCK_ERR;
use crate::{DBCol, StorageError, Store, TrieCache, TrieCachingStorage, TrieStorage};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::TrieNodesCount;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::error;

pub type IoRequestQueue = Arc<Mutex<VecDeque<IoThreadCmd>>>;

const MAX_PREFETCH_STAGING_MEMORY: usize = 200 * 1024 * 1024;
/// How much memory capacity is reserved for each prefetch request.
/// Set to 4MiB, the same as `max_length_storage_value`.
const PREFETCH_RESERVED_BYTES_PER_SLOT: usize = 4 * 1024 * 1024;

/// Storage used by I/O threads to prefetch data.
///
/// Is always linked to a parent `TrieCachingStorage`. One  caching storage can
/// produce many I/O threads and each will have its own prefetching storage.
/// However, the underlying shard cache and prefetching map is shared among all
/// instances, including the parent.
#[derive(Clone)]
pub struct TriePrefetchingStorage {
    /// Store is shared with parent `TrieCachingStorage`.
    store: Store,
    shard_uid: ShardUId,
    /// Shard cache is shared with parent `TrieCachingStorage`. But the
    /// pre-fetcher uses this in read-only mode to avoid premature evictions.
    shard_cache: TrieCache,
    /// Shared with parent `TrieCachingStorage`.
    prefetching: Arc<Mutex<PrefetchStagingArea>>,
}

/// Staging area for in-flight prefetch requests and a buffer for prefetched data,
///
/// Before starting a pre-fetch, a slot is reserved for it. Once the data is
/// here, it will be put in that slot. The parent `TrieCachingStorage` needs
/// to take it out and move it to the shard cache.
///
/// This design is to avoid prematurely filling up the shard cache.
#[derive(Default)]
pub(crate) struct PrefetchStagingArea {
    slots: HashMap<CryptoHash, PrefetchSlot>,
    size_bytes: usize,
}

#[derive(Clone, Debug)]
enum PrefetchSlot {
    PendingPrefetch,
    PendingFetch,
    Done(Arc<[u8]>),
}

pub enum IoThreadCmd {
    PrefetchTrieNode(TrieKey),
    StopSelf,
}

pub(crate) enum PrefetcherResult {
    SlotReserved,
    Pending,
    Prefetched(Arc<[u8]>),
    MemoryLimitReached,
}

impl TrieStorage for TriePrefetchingStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
        // Try to get value from shard cache containing most recently touched nodes.
        let mut shard_cache_guard = self.shard_cache.0.lock().expect(POISONED_LOCK_ERR);
        let maybe_val = { shard_cache_guard.get(hash) };

        match maybe_val {
            Some(val) => Ok(val),
            None => {
                // If data is already being prefetched, wait for that instead of sending a new request.
                let prefetch_state = PrefetchStagingArea::get_and_set_if_empty(
                    &self.prefetching,
                    hash.clone(),
                    PrefetchSlot::PendingPrefetch,
                );
                // Keep lock until here to avoid race condition between shard cache insertion and reserving prefetch slot.
                std::mem::drop(shard_cache_guard);

                match prefetch_state {
                    PrefetcherResult::SlotReserved => {
                        let key = TrieCachingStorage::get_key_from_shard_uid_and_hash(
                            self.shard_uid,
                            hash,
                        );
                        let value: Arc<[u8]> = self
                            .store
                            .get(DBCol::State, key.as_ref())
                            .map_err(|_| StorageError::StorageInternalError)?
                            .ok_or_else(|| {
                                StorageError::StorageInconsistentState(
                                    "Trie node missing".to_string(),
                                )
                            })?
                            .into();

                        self.prefetching
                            .lock()
                            .expect(POISONED_LOCK_ERR)
                            .insert_fetched(hash.clone(), value.clone());
                        Ok(value)
                    }
                    PrefetcherResult::Prefetched(value) => Ok(value),
                    PrefetcherResult::Pending => {
                        std::thread::sleep(Duration::from_micros(1));
                        // Unwrap: Only main thread  (this one) removes values from staging area,
                        // therefore blocking read will not return empty.
                        PrefetchStagingArea::blocking_get(&self.prefetching, hash.clone())
                            .or_else(|| {
                                // `blocking_get` will return None if the prefetch slot has been removed
                                // by the main thread and the value inserted into the shard cache.
                                let mut guard = self.shard_cache.0.lock().expect(POISONED_LOCK_ERR);
                                guard.get(hash)
                            })
                            .ok_or_else(|| {
                                // This could only happen if this thread started prefetching a value
                                // while also another thread was already prefetching it. Then the other
                                // other thread finished, the main thread takes it out, and moves it to
                                // the shard cache. And then this current thread gets delayed for long
                                // enough that the value gets evicted from the shard cache again before
                                // this thread has a change to read it.
                                // In this rare occasion, we shall abort the current prefetch request and
                                // move on to the next.
                                StorageError::StorageInconsistentState(
                                    "Prefetcher failed".to_owned(),
                                )
                            })
                    }
                    PrefetcherResult::MemoryLimitReached => {
                        Err(StorageError::StorageInconsistentState("Prefetcher failed".to_owned()))
                    }
                }
            }
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
        prefetching: Arc<Mutex<PrefetchStagingArea>>,
    ) -> Self {
        Self { store, shard_uid, shard_cache, prefetching }
    }
}

impl PrefetchStagingArea {
    /// Release a slot in the prefetcher staging area.
    ///
    /// This must only be called after inserting the value to the shard cache.
    /// Otherwise, the following scenario becomes possible:
    /// 1: Main thread removes a value from the prefetch staging area
    /// 2: IO thread misses in the shard cache on the same key and starts fetching it again.
    /// 3: Main thread value is inserted in shard cache.
    pub(crate) fn release(&mut self, key: &CryptoHash) {
        let dropped = self.slots.remove(key);
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
        match dropped {
            Some(PrefetchSlot::Done(value)) => self.size_bytes -= value.len(),
            Some(PrefetchSlot::PendingFetch) => self.size_bytes -= PREFETCH_RESERVED_BYTES_PER_SLOT,
            None => { /* NOP */ }
            _ => {
                error!(target: "prefetcher", "prefetcher bug detected, trying to release {dropped:?}");
            }
        }
    }

    /// Block until value is prefetched and then return it.
    pub(crate) fn blocking_get(this: &Arc<Mutex<Self>>, key: CryptoHash) -> Option<Arc<[u8]>> {
        loop {
            match this.lock().expect(POISONED_LOCK_ERR).slots.get(&key) {
                Some(PrefetchSlot::Done(value)) => return Some(value.clone()),
                Some(_) => { /* NOP */ }
                None => return None,
            }
            std::thread::sleep(std::time::Duration::from_micros(1));
        }
    }

    /// Get prefetched value if available and otherwise atomically set
    /// prefetcher state to being fetched by main thread.
    pub(crate) fn get_or_set_fetching(
        this: &Arc<Mutex<Self>>,
        key: CryptoHash,
    ) -> PrefetcherResult {
        Self::get_and_set_if_empty(this, key, PrefetchSlot::PendingFetch)
    }

    fn insert_fetched(&mut self, key: CryptoHash, value: Arc<[u8]>) {
        self.size_bytes -= PREFETCH_RESERVED_BYTES_PER_SLOT;
        self.size_bytes += value.len();
        let pending = self.slots.insert(key, PrefetchSlot::Done(value));
        assert!(prefetch_state_matches(PrefetchSlot::PendingPrefetch, &pending.unwrap()));
    }

    /// Get prefetched value if available and otherwise atomically insert the
    /// given `PrefetchSlot` if no request is pending yet.
    fn get_and_set_if_empty(
        this: &Arc<Mutex<Self>>,
        key: CryptoHash,
        set_if_empty: PrefetchSlot,
    ) -> PrefetcherResult {
        let mut guard = this.lock().expect(POISONED_LOCK_ERR);
        let full = match guard.size_bytes.checked_add(PREFETCH_RESERVED_BYTES_PER_SLOT) {
            Some(size_after) => size_after > MAX_PREFETCH_STAGING_MEMORY,
            None => true,
        };
        if full {
            return PrefetcherResult::MemoryLimitReached;
        }
        match guard.slots.entry(key) {
            Entry::Occupied(entry) => match entry.get() {
                PrefetchSlot::Done(value) => {
                    near_o11y::io_trace!(count: "prefetch_hit");
                    PrefetcherResult::Prefetched(value.clone())
                }
                PrefetchSlot::PendingPrefetch | PrefetchSlot::PendingFetch => {
                    std::mem::drop(guard);
                    near_o11y::io_trace!(count: "prefetch_pending");
                    PrefetcherResult::Pending
                }
            },
            Entry::Vacant(entry) => {
                entry.insert(set_if_empty);
                guard.size_bytes += PREFETCH_RESERVED_BYTES_PER_SLOT;
                PrefetcherResult::SlotReserved
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

// #[cfg(test)]
mod tests {
    use crate::TriePrefetchingStorage;

    impl TriePrefetchingStorage {
        pub fn prefetched_staging_area_size(&self) -> usize {
            self.prefetching.lock().unwrap().slots.len()
        }
    }
}

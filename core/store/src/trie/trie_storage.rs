use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use near_primitives::hash::CryptoHash;

use crate::db::refcount::decode_value_with_rc;
use crate::trie::POISONED_LOCK_ERR;
use crate::{ColState, StorageError, Store};
use lru::LruCache;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::AccountId;
use std::cell::RefCell;
use std::io::ErrorKind;

#[derive(Clone)]
pub struct SyncTrieCache(Arc<Mutex<TrieCache>>);

// Maps account (contract) id to the last block hash for which some key was touched
pub struct KeyTouchRecords(HashMap<AccountId, CryptoHash>);

// Active trie worker - pair (block_hash, account (contract) id)
pub struct ActiveWorker {
    pub block_hash: CryptoHash,
    pub account_id: AccountId,
}

struct TrieCache {
    inner: LruCache<CryptoHash, Vec<u8>>,
    cap: usize,
    active_worker: Option<ActiveWorker>,
    // Key hash -> Last recorded block hash
    block_touches: HashMap<CryptoHash, CryptoHash>,
    // Key hash -> Vector of accounts which took this key
    account_touches: HashMap<CryptoHash, HashSet<AccountId>>,
}

impl TrieCache {
    pub fn get(&mut self, hash: &CryptoHash) -> Option<&Vec<u8>> {
        self.inner.get(hash)
    }

    pub fn chargeable_get(&mut self, hash: &CryptoHash) -> (Option<&Vec<u8>>, bool) {
        match self.inner.get(hash) {
            Some(value) => {
                // value was already moved to head in LRU
                let mut need_charge = false;
                if let Some(ActiveWorker { block_hash, account_id }) = &self.active_worker {
                    if let Some(recorded_block_hash) = self.block_touches.get(hash) {
                        if block_hash != recorded_block_hash {
                            self.block_touches.insert(hash.clone(), block_hash.clone());
                            self.account_touches.insert(hash.clone(), Default::default());
                        }
                        if let Some(accounts) = self.account_touches.get_mut(hash) {
                            need_charge = accounts.contains(account_id);
                            accounts.insert(account_id.clone());
                        }
                    }
                }
                (Some(value), need_charge)
            }
            None => (None, false),
        }
    }

    fn put(&mut self, hash: CryptoHash, value: Vec<u8>) {
        self.inner.put(hash, value);
        // Eviction
        while self.inner.len() > self.cap {
            // If number of touched nodes among all contracts in chunk is < 2 * cap, we do it no more than 1 time per entry
            match self.inner.pop_lru() {
                Some((hash, value)) => {
                    if let Some(ActiveWorker { block_hash, .. }) = self.active_worker {
                        if let Some(recorded_block_hash) = self.block_touches.get(&hash) {
                            // if active block hash matches recorded block hash, we can't evict the element
                            if block_hash == *recorded_block_hash {
                                self.inner.put(hash, value);
                            }
                        }
                    }
                }
                None => unreachable!(""),
            }
        }
    }

    fn pop(&mut self, hash: &CryptoHash) -> Option<Vec<u8>> {
        // eviction?
        self.block_touches.clear();
        self.account_touches.clear();
        self.inner.pop(hash)
    }
}

impl SyncTrieCache {
    pub fn new() -> Self {
        // Self(Arc::new(Mutex::new(LruCache::new(TRIE_MAX_CACHE_SIZE))))
        Self(Arc::new(Mutex::new(TrieCache {
            inner: LruCache::unbounded(),
            cap: TRIE_MAX_CACHE_SIZE,
            active_worker: None,
            block_touches: Default::default(),
            account_touches: Default::default(),
        })))
    }

    pub fn clear(&self) {
        let mut guard = self.0.lock().expect(POISONED_LOCK_ERR);
        guard.inner.clear();
        guard.active_worker = None;
        guard.block_touches.clear();
        guard.account_touches.clear();
    }

    pub fn update_active_worker(&self, active_worker: Option<ActiveWorker>) {
        let mut guard = self.0.lock().expect(POISONED_LOCK_ERR);
        guard.active_worker = active_worker;
    }

    pub fn chargeable_get(&self, hash: &CryptoHash) -> (Option<&Vec<u8>>, bool) {
        let mut guard = self.0.lock().expect(POISONED_LOCK_ERR);
        let (value, is_charge) = guard.chargeable_get(hash);
        (value, is_charge)
    }

    // TODO: Can we remove some key in the middle of the block, which breaks node touch charge logic?
    pub fn update_cache(&self, ops: Vec<(CryptoHash, Option<&Vec<u8>>)>) {
        let mut guard = self.0.lock().expect(POISONED_LOCK_ERR);
        for (hash, opt_value_rc) in ops {
            if let Some(value_rc) = opt_value_rc {
                if let (Some(value), _rc) = decode_value_with_rc(&value_rc) {
                    // TODO: put TRIE_LIMIT_CACHED_VALUE_SIZE to runtime config
                    if value.len() < TRIE_LIMIT_CACHED_VALUE_SIZE {
                        guard.put(hash, value.to_vec());
                    }
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
}

impl TrieStorage for TrieCachingStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Vec<u8>, StorageError> {
        let mut guard = self.cache.0.lock().expect(POISONED_LOCK_ERR);
        if let Some(val) = guard.pop(hash) {
            Ok(val.clone())
        } else {
            let key = Self::get_key_from_shard_uid_and_hash(self.shard_uid, hash);
            let val = self
                .store
                .get(ColState, key.as_ref())
                .map_err(|_| StorageError::StorageInternalError)?;
            if let Some(val) = val {
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

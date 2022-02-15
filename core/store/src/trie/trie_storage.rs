use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use near_primitives::hash::CryptoHash;

use crate::db::refcount::decode_value_with_rc;
use crate::trie::POISONED_LOCK_ERR;
use crate::{ColState, StorageError, Store};
use lru::LruCache;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::TrieCacheState;
use std::cell::RefCell;
use std::io::ErrorKind;

#[derive(Clone)]
pub struct SyncTrieCache(Arc<Mutex<TrieCache>>);

/// Cache for Trie nodes.
/// Maps hash of the encoded node to the encoded node bytes.
/// Note that "node" here means both the trie node and the value stored in trie.
pub(crate) struct TrieCache {
    pub(crate) cache_state: TrieCacheState,
    shard_cache: LruCache<CryptoHash, Vec<u8>>,
    chunk_cache: HashMap<CryptoHash, Vec<u8>>,
}

/// Position of the value in cache.
#[derive(Debug)]
pub(crate) enum CachePosition {
    /// Value is not presented.
    None,
    /// Value is presented in the shard cache.
    ShardCache(Vec<u8>),
    /// Value is presented in the chunk cache.
    ChunkCache(Vec<u8>),
}

#[derive(Debug, Eq, PartialEq)]
pub enum TrieNodeRetrievalCost {
    Free,
    Full,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct RawBytesWithCost {
    /// Bytes of the retrieved node. None if no value was found in cache.
    pub(crate) value: Option<Vec<u8>>,
    /// Cost of node retrieval.
    pub(crate) cost: TrieNodeRetrievalCost,
}

impl TrieCache {
    pub fn new(shard_cache_size: usize) -> Self {
        TrieCache {
            cache_state: TrieCacheState::CachingShard,
            shard_cache: LruCache::new(shard_cache_size),
            chunk_cache: Default::default(),
        }
    }

    pub(crate) fn get_cache_position(&mut self, key: &CryptoHash) -> CachePosition {
        match self.chunk_cache.get(key) {
            Some(value) => CachePosition::ChunkCache(value.clone()),
            None => match self.shard_cache.get(key) {
                Some(value) => CachePosition::ShardCache(value.clone()),
                None => CachePosition::None,
            },
        }
    }

    pub fn get_with_cost(&mut self, key: &CryptoHash) -> RawBytesWithCost {
        match self.get_cache_position(key) {
            CachePosition::None => {
                RawBytesWithCost { value: None, cost: TrieNodeRetrievalCost::Full }
            }
            CachePosition::ShardCache(value) => {
                if let TrieCacheState::CachingChunk = self.cache_state {
                    let value = self
                        .shard_cache
                        .pop(key)
                        .expect("If position is ShardCache then value must be presented");
                    self.chunk_cache.insert(key.clone(), value);
                };
                RawBytesWithCost { value: Some(value), cost: TrieNodeRetrievalCost::Full }
            }
            CachePosition::ChunkCache(value) => RawBytesWithCost {
                value: Some(value),
                cost: match self.cache_state {
                    TrieCacheState::CachingShard => TrieNodeRetrievalCost::Full,
                    TrieCacheState::CachingChunk => TrieNodeRetrievalCost::Free,
                },
            },
        }
    }

    pub fn put(&mut self, key: CryptoHash, value: &[u8]) {
        if value.len() >= TRIE_LIMIT_CACHED_VALUE_SIZE {
            return;
        }
        let value = value.to_vec();

        if let TrieCacheState::CachingChunk = &self.cache_state {
            self.shard_cache.pop(&key);
            self.chunk_cache.insert(key, value);
        } else {
            if self.chunk_cache.contains_key(&key) {
                self.chunk_cache.insert(key, value);
            } else {
                self.shard_cache.put(key, value);
            }
        }
    }

    pub fn pop(&mut self, hash: &CryptoHash) -> Option<Vec<u8>> {
        match self.chunk_cache.remove(hash) {
            Some(value) => Some(value),
            None => self.shard_cache.pop(hash),
        }
    }
}

impl SyncTrieCache {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(TrieCache::new(TRIE_MAX_SHARD_CACHE_SIZE))))
    }

    pub fn clear(&self) {
        let mut guard = self.0.lock().expect(POISONED_LOCK_ERR);
        guard.cache_state = TrieCacheState::CachingShard;
        guard.shard_cache.clear();
        guard.chunk_cache.clear();
    }

    pub fn set_state(&self, state: TrieCacheState) {
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
/// 50_000 * 4 (number of shards) * TRIE_LIMIT_CACHED_VALUE_SIZE * 2 (number of caches - for regular and view client) = 1.6 GB.
/// In our tests on a single shard, it barely occupied 40 MB, which is dominated by state cache size
/// with 512 MB limit. The total RAM usage for a single shard was 1 GB.
#[cfg(not(feature = "no_cache"))]
const TRIE_MAX_SHARD_CACHE_SIZE: usize = 50000;

#[cfg(feature = "no_cache")]
const TRIE_MAX_SHARD_CACHE_SIZE: usize = 1;

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
        self.retrieve_raw_bytes_with_cost(hash).map(|(val, _)| val)
    }

    fn retrieve_raw_bytes_with_cost(
        &self,
        hash: &CryptoHash,
    ) -> Result<(Vec<u8>, TrieNodeRetrievalCost), StorageError> {
        let mut guard = self.cache.0.lock().expect(POISONED_LOCK_ERR);
        if let RawBytesWithCost { value: Some(val), cost } = guard.get_with_cost(hash) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use near_primitives::hash::hash;

    #[test]
    fn test_put() {
        let mut trie_cache = TrieCache::new(5);
        let key = hash(&[0, 1, 2]);
        let value = vec![1u8];

        trie_cache.put(key, &value);
        assert_eq!(
            trie_cache.get_with_cost(&key),
            RawBytesWithCost { value: Some(value), cost: TrieNodeRetrievalCost::Full }
        );

        let value = vec![2u8];
        trie_cache.put(key, &value);
        assert_eq!(
            trie_cache.get_with_cost(&key),
            RawBytesWithCost { value: Some(value), cost: TrieNodeRetrievalCost::Full }
        );
    }

    #[test]
    fn test_pop() {
        let mut trie_cache = TrieCache::new(5);
        let key = hash(&[0, 1, 2]);
        let value = vec![1u8];

        trie_cache.put(key, &value);
        assert_eq!(
            trie_cache.get_with_cost(&key),
            RawBytesWithCost { value: Some(value), cost: TrieNodeRetrievalCost::Full }
        );

        trie_cache.pop(&key);
        assert_matches!(trie_cache.get_with_cost(&key), RawBytesWithCost { value: None, .. });
    }

    #[test]
    fn test_shard_cache_size() {
        let shard_cache_size = 5;
        let mut trie_cache = TrieCache::new(shard_cache_size);

        (0..shard_cache_size as u8 + 1).for_each(|i| trie_cache.put(hash(&[i]), &[i]));

        assert_matches!(
            trie_cache.get_with_cost(&hash(&[0u8])),
            RawBytesWithCost { value: None, .. }
        );
        (1..shard_cache_size as u8 + 1).for_each(|i| {
            let value = vec![i];
            assert_eq!(
                trie_cache.get_with_cost(&hash(&value)),
                RawBytesWithCost { value: Some(value), cost: TrieNodeRetrievalCost::Full }
            )
        });
    }

    #[test]
    fn test_positions_and_costs() {
        let mut trie_cache = TrieCache::new(5);
        let value = vec![1u8];
        let key = hash(&value);

        assert_matches!(trie_cache.cache_state, TrieCacheState::CachingShard);
        assert_matches!(trie_cache.get_cache_position(&key), CachePosition::None);

        trie_cache.put(key, &value);
        assert_matches!(trie_cache.get_cache_position(&key), CachePosition::ShardCache(_));

        trie_cache.cache_state = TrieCacheState::CachingChunk;
        assert_matches!(trie_cache.get_cache_position(&key), CachePosition::ShardCache(_));
        assert_eq!(
            trie_cache.get_with_cost(&key),
            RawBytesWithCost { value: Some(value.clone()), cost: TrieNodeRetrievalCost::Full }
        );
        assert_matches!(trie_cache.get_cache_position(&key), CachePosition::ChunkCache(_));
        assert_eq!(
            trie_cache.get_with_cost(&key),
            RawBytesWithCost { value: Some(value), cost: TrieNodeRetrievalCost::Free }
        );

        trie_cache.pop(&key);
        assert_matches!(trie_cache.get_cache_position(&key), CachePosition::None);
    }

    #[test]
    fn test_replacement() {
        let mut trie_cache = TrieCache::new(5);
        let value = vec![1u8];
        let key = hash(&value);

        trie_cache.put(key, &value);
        assert_eq!(trie_cache.shard_cache.get(&key), Some(&value));
        assert!(!trie_cache.chunk_cache.contains_key(&key));

        trie_cache.cache_state = TrieCacheState::CachingChunk;
        let value = vec![2u8];
        trie_cache.put(key, &value);
        assert!(!trie_cache.shard_cache.contains(&key));
        assert_eq!(trie_cache.chunk_cache.get(&key), Some(&value));
    }
}

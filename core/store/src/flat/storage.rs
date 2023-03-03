use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use lru::LruCache;
use near_o11y::metrics::IntGauge;
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::state::ValueRef;
use near_primitives::types::{BlockHeight, ShardId};
#[cfg(feature = "protocol_feature_flat_state")]
use tracing::info;

use crate::{metrics, Store, StoreUpdate};

use super::delta::FlatStateDelta;
use super::store_helper;
use super::types::{BlockInfo, ChainAccessForFlatStorage, FlatStorageError};

/// FlatStorage stores information on which blocks flat storage current supports key lookups on.
/// Note that this struct is shared by multiple threads, the chain thread, threads that apply chunks,
/// and view client, so the implementation here must be thread safe and must have interior mutability,
/// thus all methods in this class are with &self instead of &mut self.
#[derive(Clone)]
pub struct FlatStorage(pub(crate) Arc<RwLock<FlatStorageInner>>);

// FlatStorage need to support concurrent access and be consistent if node crashes or restarts,
// so we make sure to keep the following invariants in our implementation.
// - `flat_head` is stored on disk. The value of flat_head in memory and on disk should always
//   be consistent with the flat state stored in `DbCol::FlatState` on disk. This means, updates to
//   these values much be atomic from the outside.
// - `blocks` and `deltas` store the same set of blocks, except that `flat_head` is in `blocks`,
//     but not in `deltas`. For any block in `blocks`, `flat_head`
//    must be on the same chain as the block and all blocks between `flat_head` and the block must
//    also be in `blocks`.
// - All deltas in `deltas` are stored on disk. And if a block is accepted by chain, its deltas
//   must be stored on disk as well, if the block is children of `flat_head`.
//   This makes sure that when a node restarts, FlatStorage can load deltas for all blocks
//   after the `flat_head` block successfully.
pub(crate) struct FlatStorageInner {
    #[allow(unused)]
    store: Store,
    /// Id of the shard which state is accessed by this flat storage.
    #[allow(unused)]
    shard_id: ShardId,
    /// The block for which we store the key value pairs of the state after it is applied.
    /// For non catchup mode, it should be the last final block.
    #[allow(unused)]
    flat_head: CryptoHash,
    /// Stores some information for all blocks supported by flat storage, this is used for finding
    /// paths between the root block and a target block
    #[allow(unused)]
    blocks: HashMap<CryptoHash, BlockInfo>,
    /// State deltas for all blocks supported by this flat storage.
    /// All these deltas here are stored on disk too.
    #[allow(unused)]
    deltas: HashMap<CryptoHash, Arc<FlatStateDelta>>,
    /// Cache for the mapping from trie storage keys to value refs for `flat_head`.
    /// Must be equivalent to the mapping stored on disk only for `flat_head`. For
    /// other blocks, deltas have to be applied as usual.
    // TODO (#8649): consider using RocksDB RowCache.
    #[allow(unused)]
    value_ref_cache: LruCache<Vec<u8>, Option<ValueRef>>,
    #[allow(unused)]
    metrics: FlatStorageMetrics,
}

struct FlatStorageMetrics {
    flat_head_height: IntGauge,
    cached_blocks: IntGauge,
    cached_deltas: IntGauge,
    cached_deltas_num_items: IntGauge,
    cached_deltas_size: IntGauge,
    #[allow(unused)]
    distance_to_head: IntGauge,
    #[allow(unused)]
    value_ref_cache_len: IntGauge,
    #[allow(unused)]
    value_ref_cache_total_key_size: IntGauge,
    #[allow(unused)]
    value_ref_cache_total_value_size: IntGauge,
}

#[cfg(feature = "protocol_feature_flat_state")]
impl FlatStorageInner {
    /// Creates `BlockNotSupported` error for the given block.
    fn create_block_not_supported_error(&self, block_hash: &CryptoHash) -> FlatStorageError {
        FlatStorageError::BlockNotSupported((self.flat_head, *block_hash))
    }

    /// Gets delta for the given block and shard `self.shard_id`.
    fn get_delta(&self, block_hash: &CryptoHash) -> Result<Arc<FlatStateDelta>, FlatStorageError> {
        // TODO (#7327): add limitation on cached deltas number to limit RAM usage
        // and read single `ValueRef` from delta if it is not cached.
        Ok(self
            .deltas
            .get(block_hash)
            .ok_or(self.create_block_not_supported_error(block_hash))?
            .clone())
    }

    /// Get sequence of blocks `target_block_hash` (inclusive) to flat head (exclusive)
    /// in backwards chain order. Returns an error if there is no path between them.
    fn get_blocks_to_head(
        &self,
        target_block_hash: &CryptoHash,
    ) -> Result<Vec<CryptoHash>, FlatStorageError> {
        let shard_id = &self.shard_id;
        let flat_head = &self.flat_head;
        let flat_head_info = self
            .blocks
            .get(flat_head)
            .expect(&format!("Inconsistent flat storage state for shard {shard_id}: head {flat_head} not found in cached blocks"));

        let mut block_hash = target_block_hash.clone();
        let mut blocks = vec![];
        while block_hash != *flat_head {
            let block_info = self
                .blocks
                .get(&block_hash)
                .ok_or(self.create_block_not_supported_error(target_block_hash))?;

            if block_info.height < flat_head_info.height {
                return Err(self.create_block_not_supported_error(target_block_hash));
            }

            blocks.push(block_hash);
            block_hash = block_info.prev_hash;
        }
        self.metrics.distance_to_head.set(blocks.len() as i64);

        Ok(blocks)
    }

    #[allow(unused)]
    fn put_value_ref_to_cache(&mut self, key: Vec<u8>, value: Option<ValueRef>) {
        let value_size = value.as_ref().map_or(1, |_| 37);
        if let Some((key, old_value)) = self.value_ref_cache.push(key.to_vec(), value) {
            self.metrics.value_ref_cache_total_key_size.sub(key.len() as i64);
            self.metrics.value_ref_cache_total_value_size.sub(old_value.map_or(1, |_| 37));
        }
        if self.value_ref_cache.cap() > 0 {
            self.metrics.value_ref_cache_total_key_size.add(key.len() as i64);
            self.metrics.value_ref_cache_total_value_size.add(value_size);
        }
        self.metrics.value_ref_cache_len.set(self.value_ref_cache.len() as i64);
    }

    /// Get cached `ValueRef` for flat storage head. Possible results:
    /// - None: no entry in cache;
    /// - Some(None): entry None found in cache, meaning that there is no such key in state;
    /// - Some(Some(value_ref)): entry found in cache.
    #[cfg(feature = "protocol_feature_flat_state")]
    pub(crate) fn get_cached_ref(&mut self, key: &[u8]) -> Option<Option<ValueRef>> {
        self.value_ref_cache.get(key).cloned()
    }
}

impl FlatStorage {
    /// Create a new FlatStorage for `shard_id` using flat head if it is stored on storage.
    /// We also load all blocks with height between flat head to `latest_block_height`
    /// including those on forks into the returned FlatStorage.
    pub fn new(
        store: Store,
        shard_id: ShardId,
        latest_block_height: BlockHeight,
        // Unfortunately we don't have access to ChainStore inside this file because of package
        // dependencies, so we pass these functions in to access chain info
        chain_access: &dyn ChainAccessForFlatStorage,
        cache_capacity: usize,
    ) -> Self {
        let flat_head = store_helper::get_flat_head(&store, shard_id)
            .unwrap_or_else(|| panic!("Cannot read flat head for shard {} from storage", shard_id));
        let flat_head_info = chain_access.get_block_info(&flat_head);
        let flat_head_height = flat_head_info.height;
        let mut blocks = HashMap::from([(
            flat_head,
            BlockInfo {
                hash: flat_head,
                height: flat_head_height,
                prev_hash: flat_head_info.prev_hash,
            },
        )]);
        let mut deltas = HashMap::new();

        // `itoa` is much faster for printing shard_id to a string than trivial alternatives.
        let mut buffer = itoa::Buffer::new();
        let shard_id_label = buffer.format(shard_id);
        let metrics = FlatStorageMetrics {
            flat_head_height: metrics::FLAT_STORAGE_HEAD_HEIGHT
                .with_label_values(&[shard_id_label]),
            cached_blocks: metrics::FLAT_STORAGE_CACHED_BLOCKS.with_label_values(&[shard_id_label]),
            cached_deltas: metrics::FLAT_STORAGE_CACHED_DELTAS.with_label_values(&[shard_id_label]),
            cached_deltas_num_items: metrics::FLAT_STORAGE_CACHED_DELTAS_NUM_ITEMS
                .with_label_values(&[shard_id_label]),
            cached_deltas_size: metrics::FLAT_STORAGE_CACHED_DELTAS_SIZE
                .with_label_values(&[shard_id_label]),
            distance_to_head: metrics::FLAT_STORAGE_DISTANCE_TO_HEAD
                .with_label_values(&[shard_id_label]),
            value_ref_cache_len: metrics::FLAT_STORAGE_VALUE_REF_CACHE_LEN
                .with_label_values(&[shard_id_label]),
            value_ref_cache_total_key_size: metrics::FLAT_STORAGE_VALUE_REF_CACHE_TOTAL_KEY_SIZE
                .with_label_values(&[shard_id_label]),
            value_ref_cache_total_value_size:
                metrics::FLAT_STORAGE_VALUE_REF_CACHE_TOTAL_VALUE_SIZE
                    .with_label_values(&[shard_id_label]),
        };
        metrics.flat_head_height.set(flat_head_height as i64);

        for height in flat_head_height + 1..=latest_block_height {
            for hash in chain_access.get_block_hashes_at_height(height) {
                let block_info = chain_access.get_block_info(&hash);
                assert!(
                    blocks.contains_key(&block_info.prev_hash),
                    "Can't find a path from the current flat head {:?}@{} to block {:?}@{}",
                    flat_head,
                    flat_head_height,
                    hash,
                    block_info.height
                );
                blocks.insert(hash, block_info);
                metrics.cached_blocks.inc();
                let delta = store_helper::get_delta(&store, shard_id, hash)
                    .expect("Borsh cannot fail")
                    .unwrap_or_else(|| {
                        panic!("Cannot find block delta for block {:?} shard {}", hash, shard_id)
                    });
                metrics.cached_deltas.inc();
                metrics.cached_deltas_num_items.add(delta.len() as i64);
                metrics.cached_deltas_size.add(delta.total_size() as i64);
                deltas.insert(hash, delta);
            }
        }

        Self(Arc::new(RwLock::new(FlatStorageInner {
            store,
            shard_id,
            flat_head,
            blocks,
            deltas,
            value_ref_cache: LruCache::new(cache_capacity),
            metrics,
        })))
    }

    /// Get sequence of blocks `target_block_hash` (inclusive) to flat head (exclusive)
    /// in backwards chain order. Returns an error if there is no path between them.
    #[cfg(feature = "protocol_feature_flat_state")]
    #[cfg(test)]
    pub(crate) fn get_blocks_to_head(
        &self,
        target_block_hash: &CryptoHash,
    ) -> Result<Vec<CryptoHash>, FlatStorageError> {
        let guard = self.0.write().expect(super::POISONED_LOCK_ERR);
        guard.get_blocks_to_head(target_block_hash)
    }

    #[cfg(not(feature = "protocol_feature_flat_state"))]
    #[allow(unused)]
    pub(crate) fn get_blocks_to_head(
        &self,
        _target_block_hash: &CryptoHash,
    ) -> Result<Vec<CryptoHash>, FlatStorageError> {
        Err(FlatStorageError::StorageInternalError)
    }

    #[cfg(feature = "protocol_feature_flat_state")]
    pub fn get_ref(
        &self,
        block_hash: &CryptoHash,
        key: &[u8],
    ) -> Result<Option<ValueRef>, crate::StorageError> {
        let mut guard = self.0.write().expect(super::POISONED_LOCK_ERR);
        let blocks_to_head =
            guard.get_blocks_to_head(block_hash).map_err(|e| StorageError::from(e))?;
        for block_hash in blocks_to_head.iter() {
            // If we found a key in delta, we can return a value because it is the most recent key update.
            let delta = guard.get_delta(block_hash)?;
            match delta.get(key) {
                Some(value_ref) => {
                    return Ok(value_ref);
                }
                None => {}
            };
        }

        if let Some(value_ref) = guard.get_cached_ref(key) {
            return Ok(value_ref);
        }

        let value_ref = store_helper::get_ref(&guard.store, key)?;
        guard.put_value_ref_to_cache(key.to_vec(), value_ref.clone());
        Ok(value_ref)
    }

    #[cfg(not(feature = "protocol_feature_flat_state"))]
    #[allow(unused)]
    fn get_ref(
        &self,
        _block_hash: &CryptoHash,
        _key: &[u8],
    ) -> Result<Option<ValueRef>, crate::StorageError> {
        Err(StorageError::StorageInternalError)
    }

    /// Update the head of the flat storage, including updating the flat state in memory and on disk
    /// and updating the flat state to reflect the state at the new head. If updating to given head is not possible,
    /// returns an error.
    #[cfg(feature = "protocol_feature_flat_state")]
    pub fn update_flat_head(&self, new_head: &CryptoHash) -> Result<(), FlatStorageError> {
        let mut guard = self.0.write().expect(crate::flat::POISONED_LOCK_ERR);
        let blocks = guard.get_blocks_to_head(new_head)?;
        for block in blocks.into_iter().rev() {
            let mut store_update = StoreUpdate::new(guard.store.storage.clone());
            let delta = guard.get_delta(&block)?.as_ref().clone();
            for (key, value) in delta.0.iter() {
                guard.put_value_ref_to_cache(key.clone(), value.clone());
            }
            delta.apply_to_flat_state(&mut store_update);
            store_helper::set_flat_head(&mut store_update, guard.shard_id, &block);

            // Remove old blocks and deltas from disk and memory.
            // Do it for each head update separately to ensure that old data is removed properly if node was
            // interrupted in the middle.
            // TODO (#7327): in case of long forks it can take a while and delay processing of some chunk.
            // Consider avoid iterating over all blocks and make removals lazy.
            let gc_height = guard
                .blocks
                .get(&block)
                .ok_or(guard.create_block_not_supported_error(&block))?
                .height;
            let hashes_to_remove: Vec<_> = guard
                .blocks
                .iter()
                .filter(|(_, block_info)| block_info.height <= gc_height)
                .map(|(block_hash, _)| block_hash)
                .cloned()
                .collect();
            for hash in hashes_to_remove {
                // It is fine to remove all deltas in single store update, because memory overhead of `DeleteRange`
                // operation is low.
                store_helper::remove_delta(&mut store_update, guard.shard_id, hash);
                match guard.deltas.remove(&hash) {
                    Some(delta) => {
                        guard.metrics.cached_deltas.dec();
                        guard.metrics.cached_deltas_num_items.sub(delta.len() as i64);
                        guard.metrics.cached_deltas_size.sub(delta.total_size() as i64);
                    }
                    None => {}
                }

                // Note that we need to keep block info for new flat storage head to know its height.
                if &hash != new_head {
                    match guard.blocks.remove(&hash) {
                        Some(_) => {
                            guard.metrics.cached_blocks.dec();
                        }
                        None => {}
                    }
                }
            }

            store_update.commit().unwrap();
        }

        let shard_id = guard.shard_id;
        guard.flat_head = *new_head;
        let flat_head_height = guard
            .blocks
            .get(&new_head)
            .ok_or(guard.create_block_not_supported_error(&new_head))?
            .height;
        guard.metrics.flat_head_height.set(flat_head_height as i64);
        info!(target: "chain", %shard_id, %new_head, %flat_head_height, "Moved flat storage head");

        Ok(())
    }

    #[cfg(not(feature = "protocol_feature_flat_state"))]
    pub fn update_flat_head(&self, _new_head: &CryptoHash) -> Result<(), FlatStorageError> {
        Ok(())
    }

    /// Adds a block (including the block delta and block info) to flat storage,
    /// returns a StoreUpdate to store the delta on disk. Node that this StoreUpdate should be
    /// committed to disk in one db transaction together with the rest of changes caused by block,
    /// in case the node stopped or crashed in between and a block is on chain but its delta is not
    /// stored or vice versa.
    #[cfg(feature = "protocol_feature_flat_state")]
    pub fn add_block(
        &self,
        block_hash: &CryptoHash,
        delta: FlatStateDelta,
        block: BlockInfo,
    ) -> Result<StoreUpdate, FlatStorageError> {
        let mut guard = self.0.write().expect(super::POISONED_LOCK_ERR);
        let shard_id = guard.shard_id;
        let block_height = block.height;
        info!(target: "chain", %shard_id, %block_hash, %block_height, "Adding block to flat storage");
        if !guard.blocks.contains_key(&block.prev_hash) {
            return Err(guard.create_block_not_supported_error(block_hash));
        }
        let mut store_update = StoreUpdate::new(guard.store.storage.clone());
        store_helper::set_delta(&mut store_update, guard.shard_id, block_hash.clone(), &delta)?;
        guard.metrics.cached_deltas.inc();
        guard.metrics.cached_deltas_num_items.add(delta.len() as i64);
        guard.metrics.cached_deltas_size.add(delta.total_size() as i64);
        guard.deltas.insert(*block_hash, Arc::new(delta));
        guard.blocks.insert(*block_hash, block);
        guard.metrics.cached_blocks.inc();
        Ok(store_update)
    }

    #[cfg(not(feature = "protocol_feature_flat_state"))]
    pub fn add_block(
        &self,
        _block_hash: &CryptoHash,
        _delta: FlatStateDelta,
        _block_info: BlockInfo,
    ) -> Result<StoreUpdate, FlatStorageError> {
        panic!("not implemented")
    }

    /// Clears all State key-value pairs from flat storage.
    #[cfg(feature = "protocol_feature_flat_state")]
    pub fn clear_state(&self, shard_layout: ShardLayout) -> Result<(), StorageError> {
        let guard = self.0.write().expect(super::POISONED_LOCK_ERR);
        let shard_id = guard.shard_id;

        // Removes all items belonging to the shard one by one.
        // Note that it does not work for resharding.
        // TODO (#7327): call it just after we stopped tracking a shard.
        // TODO (#7327): remove FlatStateDeltas. Consider custom serialization of keys to remove them by
        // prefix.
        // TODO (#7327): support range deletions which are much faster than naive deletions. For that, we
        // can delete ranges of keys like
        // [ [0]+boundary_accounts(shard_id) .. [0]+boundary_accounts(shard_id+1) ), etc.
        // We should also take fixed accounts into account.
        let mut store_update = guard.store.store_update();
        let mut removed_items = 0;
        for item in guard.store.iter(crate::DBCol::FlatState) {
            let (key, _) =
                item.map_err(|e| StorageError::StorageInconsistentState(e.to_string()))?;

            if store_helper::key_belongs_to_shard(&key, &shard_layout, shard_id)? {
                removed_items += 1;
                store_update.delete(crate::DBCol::FlatState, &key);
            }
        }
        info!(target: "chain", %shard_id, %removed_items, "Removing old items from flat storage");

        store_helper::remove_flat_head(&mut store_update, shard_id);
        store_update.commit().map_err(|_| StorageError::StorageInternalError)?;
        Ok(())
    }

    #[cfg(not(feature = "protocol_feature_flat_state"))]
    pub fn clear_state(&self, _shard_layout: ShardLayout) {}
}

#[cfg(feature = "protocol_feature_flat_state")]
#[cfg(test)]
mod tests {
    use crate::flat::delta::FlatStateDelta;
    use crate::flat::manager::FlatStorageManager;
    use crate::flat::storage::FlatStorage;
    use crate::flat::store_helper;
    use crate::flat::types::{BlockInfo, ChainAccessForFlatStorage, FlatStorageError};
    use crate::test_utils::create_test_store;
    use crate::StorageError;
    use borsh::BorshSerialize;
    use near_primitives::borsh::maybestd::collections::HashSet;
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::state::ValueRef;
    use near_primitives::trie_key::TrieKey;
    use near_primitives::types::{
        BlockHeight, RawStateChange, RawStateChangesWithTrieKey, StateChangeCause,
    };

    use assert_matches::assert_matches;
    use std::collections::HashMap;

    struct MockChain {
        height_to_hashes: HashMap<BlockHeight, CryptoHash>,
        blocks: HashMap<CryptoHash, BlockInfo>,
        head_height: BlockHeight,
    }

    impl ChainAccessForFlatStorage for MockChain {
        fn get_block_info(&self, block_hash: &CryptoHash) -> BlockInfo {
            self.blocks.get(block_hash).unwrap().clone()
        }

        fn get_block_hashes_at_height(&self, block_height: BlockHeight) -> HashSet<CryptoHash> {
            self.height_to_hashes.get(&block_height).cloned().iter().cloned().collect()
        }
    }

    impl MockChain {
        fn block_hash(height: BlockHeight) -> CryptoHash {
            hash(&height.try_to_vec().unwrap())
        }

        /// Build a chain with given set of heights and a function mapping block heights to heights of their parents.
        fn build(
            heights: Vec<BlockHeight>,
            get_parent: fn(BlockHeight) -> Option<BlockHeight>,
        ) -> MockChain {
            let height_to_hashes: HashMap<_, _> = heights
                .iter()
                .cloned()
                .map(|height| (height, MockChain::block_hash(height)))
                .collect();
            let blocks = heights
                .iter()
                .cloned()
                .map(|height| {
                    let hash = height_to_hashes.get(&height).unwrap().clone();
                    let prev_hash = match get_parent(height) {
                        None => CryptoHash::default(),
                        Some(parent_height) => *height_to_hashes.get(&parent_height).unwrap(),
                    };
                    (hash, BlockInfo { hash, height, prev_hash })
                })
                .collect();
            MockChain { height_to_hashes, blocks, head_height: heights.last().unwrap().clone() }
        }

        // Create a chain with no forks with length n.
        fn linear_chain(n: usize) -> MockChain {
            Self::build(
                (0..n as BlockHeight).collect(),
                |i| if i == 0 { None } else { Some(i - 1) },
            )
        }

        // Create a linear chain of length n where blocks with odd numbers are skipped:
        // 0 -> 2 -> 4 -> ...
        fn linear_chain_with_skips(n: usize) -> MockChain {
            Self::build((0..n as BlockHeight).map(|i| i * 2).collect(), |i| {
                if i == 0 {
                    None
                } else {
                    Some(i - 2)
                }
            })
        }

        // Create a chain with two forks, where blocks 1 and 2 have a parent block 0, and each next block H
        // has a parent block H-2:
        // 0 |-> 1 -> 3 -> 5 -> ...
        //   --> 2 -> 4 -> 6 -> ...
        fn chain_with_two_forks(n: usize) -> MockChain {
            Self::build((0..n as BlockHeight).collect(), |i| {
                if i == 0 {
                    None
                } else {
                    Some(i.max(2) - 2)
                }
            })
        }

        fn get_block_hash(&self, height: BlockHeight) -> CryptoHash {
            *self.height_to_hashes.get(&height).unwrap()
        }

        /// create a new block on top the current chain head, return the new block hash
        fn create_block(&mut self) -> CryptoHash {
            let hash = MockChain::block_hash(self.head_height + 1);
            self.height_to_hashes.insert(self.head_height + 1, hash);
            self.blocks.insert(
                hash,
                BlockInfo {
                    hash,
                    height: self.head_height + 1,
                    prev_hash: self.get_block_hash(self.head_height),
                },
            );
            self.head_height += 1;
            hash
        }
    }

    /// Check correctness of creating `FlatStateDelta` from state changes.
    #[test]
    fn flat_state_delta_creation() {
        let alice_trie_key = TrieKey::ContractCode { account_id: "alice".parse().unwrap() };
        let bob_trie_key = TrieKey::ContractCode { account_id: "bob".parse().unwrap() };
        let carol_trie_key = TrieKey::ContractCode { account_id: "carol".parse().unwrap() };

        let state_changes = vec![
            RawStateChangesWithTrieKey {
                trie_key: alice_trie_key.clone(),
                changes: vec![
                    RawStateChange {
                        cause: StateChangeCause::InitialState,
                        data: Some(vec![1, 2]),
                    },
                    RawStateChange {
                        cause: StateChangeCause::ReceiptProcessing {
                            receipt_hash: Default::default(),
                        },
                        data: Some(vec![3, 4]),
                    },
                ],
            },
            RawStateChangesWithTrieKey {
                trie_key: bob_trie_key.clone(),
                changes: vec![
                    RawStateChange {
                        cause: StateChangeCause::InitialState,
                        data: Some(vec![5, 6]),
                    },
                    RawStateChange {
                        cause: StateChangeCause::ReceiptProcessing {
                            receipt_hash: Default::default(),
                        },
                        data: None,
                    },
                ],
            },
        ];

        let flat_state_delta = FlatStateDelta::from_state_changes(&state_changes);
        assert_eq!(
            flat_state_delta.get(&alice_trie_key.to_vec()),
            Some(Some(ValueRef::new(&[3, 4])))
        );
        assert_eq!(flat_state_delta.get(&bob_trie_key.to_vec()), Some(None));
        assert_eq!(flat_state_delta.get(&carol_trie_key.to_vec()), None);
    }

    /// Check that keys related to delayed receipts are not included to `FlatStateDelta`.
    #[test]
    fn flat_state_delta_delayed_keys() {
        let delayed_trie_key = TrieKey::DelayedReceiptIndices;
        let delayed_receipt_trie_key = TrieKey::DelayedReceipt { index: 1 };

        let state_changes = vec![
            RawStateChangesWithTrieKey {
                trie_key: delayed_trie_key.clone(),
                changes: vec![RawStateChange {
                    cause: StateChangeCause::InitialState,
                    data: Some(vec![1]),
                }],
            },
            RawStateChangesWithTrieKey {
                trie_key: delayed_receipt_trie_key.clone(),
                changes: vec![RawStateChange {
                    cause: StateChangeCause::InitialState,
                    data: Some(vec![2]),
                }],
            },
        ];

        let flat_state_delta = FlatStateDelta::from_state_changes(&state_changes);
        assert!(flat_state_delta.get(&delayed_trie_key.to_vec()).is_none());
        assert!(flat_state_delta.get(&delayed_receipt_trie_key.to_vec()).is_none());
    }

    /// Check that merge of `FlatStateDelta`s overrides the old changes for the same keys and doesn't conflict with
    /// different keys.
    #[test]
    fn flat_state_delta_merge() {
        let mut delta = FlatStateDelta::from([
            (vec![1], Some(ValueRef::new(&[4]))),
            (vec![2], Some(ValueRef::new(&[5]))),
            (vec![3], None),
            (vec![4], Some(ValueRef::new(&[6]))),
        ]);
        let delta_new = FlatStateDelta::from([
            (vec![2], Some(ValueRef::new(&[7]))),
            (vec![3], Some(ValueRef::new(&[8]))),
            (vec![4], None),
            (vec![5], Some(ValueRef::new(&[9]))),
        ]);
        delta.merge(&delta_new);

        assert_eq!(delta.get(&[1]), Some(Some(ValueRef::new(&[4]))));
        assert_eq!(delta.get(&[2]), Some(Some(ValueRef::new(&[7]))));
        assert_eq!(delta.get(&[3]), Some(Some(ValueRef::new(&[8]))));
        assert_eq!(delta.get(&[4]), Some(None));
        assert_eq!(delta.get(&[5]), Some(Some(ValueRef::new(&[9]))));
    }

    #[test]
    fn block_not_supported_errors() {
        // Create a chain with two forks. Set flat head to be at block 0.
        let chain = MockChain::chain_with_two_forks(5);
        let store = create_test_store();
        let mut store_update = store.store_update();
        store_helper::set_flat_head(&mut store_update, 0, &chain.get_block_hash(0));
        for i in 1..5 {
            store_helper::set_delta(
                &mut store_update,
                0,
                chain.get_block_hash(i),
                &FlatStateDelta::default(),
            )
            .unwrap();
        }
        store_update.commit().unwrap();

        let flat_storage = FlatStorage::new(store.clone(), 0, 4, &chain, 0);
        let flat_storage_manager = FlatStorageManager::new(store.clone());
        flat_storage_manager.add_flat_storage_for_shard(0, flat_storage);
        let flat_storage = flat_storage_manager.get_flat_storage_for_shard(0).unwrap();

        // Check that flat head can be moved to block 1.
        let flat_head_hash = chain.get_block_hash(1);
        assert_eq!(flat_storage.update_flat_head(&flat_head_hash), Ok(()));
        // Check that attempt to move flat head to block 2 results in error because it lays in unreachable fork.
        let fork_block_hash = chain.get_block_hash(2);
        assert_eq!(
            flat_storage.update_flat_head(&fork_block_hash),
            Err(FlatStorageError::BlockNotSupported((flat_head_hash, fork_block_hash)))
        );
        // Check that attempt to move flat head to block 0 results in error because it is an unreachable parent.
        let parent_block_hash = chain.get_block_hash(0);
        assert_eq!(
            flat_storage.update_flat_head(&parent_block_hash),
            Err(FlatStorageError::BlockNotSupported((flat_head_hash, parent_block_hash)))
        );
        // Check that attempt to move flat head to non-existent block results in the same error.
        let not_existing_hash = hash(&[1, 2, 3]);
        assert_eq!(
            flat_storage.update_flat_head(&not_existing_hash),
            Err(FlatStorageError::BlockNotSupported((flat_head_hash, not_existing_hash)))
        );
    }

    #[test]
    fn skipped_heights() {
        // Create a linear chain where some heights are skipped.
        let chain = MockChain::linear_chain_with_skips(5);
        let store = create_test_store();
        let mut store_update = store.store_update();
        store_helper::set_flat_head(&mut store_update, 0, &chain.get_block_hash(0));
        for i in 1..5 {
            store_helper::set_delta(
                &mut store_update,
                0,
                chain.get_block_hash(i * 2),
                &FlatStateDelta::default(),
            )
            .unwrap();
        }
        store_update.commit().unwrap();

        // Check that flat storage state is created correctly for chain which has skipped heights.
        let flat_storage = FlatStorage::new(store.clone(), 0, 8, &chain, 0);
        let flat_storage_manager = FlatStorageManager::new(store.clone());
        flat_storage_manager.add_flat_storage_for_shard(0, flat_storage);
        let flat_storage = flat_storage_manager.get_flat_storage_for_shard(0).unwrap();

        // Check that flat head can be moved to block 8.
        let flat_head_hash = chain.get_block_hash(8);
        assert_eq!(flat_storage.update_flat_head(&flat_head_hash), Ok(()));
    }

    // This setup tests basic use cases for FlatStorageChunkView and FlatStorage.
    // We created a linear chain with no forks, start with flat head at the genesis block, then
    // moves the flat head forward, which checking that chunk_view.get_ref() still returns the correct
    // values and the state is being updated in store.
    fn flat_storage_sanity(cache_capacity: usize) {
        // 1. Create a chain with 10 blocks with no forks. Set flat head to be at block 0.
        //    Block i sets value for key &[1] to &[i].
        let mut chain = MockChain::linear_chain(10);
        let store = create_test_store();
        let mut store_update = store.store_update();
        store_helper::set_flat_head(&mut store_update, 0, &chain.get_block_hash(0));
        store_helper::set_ref(&mut store_update, vec![1], Some(ValueRef::new(&[0]))).unwrap();
        for i in 1..10 {
            store_helper::set_delta(
                &mut store_update,
                0,
                chain.get_block_hash(i),
                &FlatStateDelta::from([(vec![1], Some(ValueRef::new(&[i as u8])))]),
            )
            .unwrap();
        }
        store_update.commit().unwrap();

        let flat_storage = FlatStorage::new(store.clone(), 0, 9, &chain, cache_capacity);
        let flat_storage_manager = FlatStorageManager::new(store.clone());
        flat_storage_manager.add_flat_storage_for_shard(0, flat_storage);
        let flat_storage = flat_storage_manager.get_flat_storage_for_shard(0).unwrap();

        // 2. Check that the chunk_view at block i reads the value of key &[1] as &[i]
        for i in 0..10 {
            let block_hash = chain.get_block_hash(i);
            let blocks = flat_storage.get_blocks_to_head(&block_hash).unwrap();
            assert_eq!(blocks.len(), i as usize);
            let chunk_view = flat_storage_manager.chunk_view(0, Some(block_hash), false).unwrap();
            assert_eq!(chunk_view.get_ref(&[1]).unwrap(), Some(ValueRef::new(&[i as u8])));
        }

        // 3. Create a new block that deletes &[1] and add a new value &[2]
        //    Add the block to flat storage.
        let hash = chain.create_block();
        let store_update = flat_storage
            .add_block(
                &hash,
                FlatStateDelta::from([(vec![1], None), (vec![2], Some(ValueRef::new(&[1])))]),
                chain.get_block_info(&hash),
            )
            .unwrap();
        store_update.commit().unwrap();

        // 4. Create a flat_state0 at block 10 and flat_state1 at block 4
        //    Verify that they return the correct values
        let blocks = flat_storage.get_blocks_to_head(&chain.get_block_hash(10)).unwrap();
        assert_eq!(blocks.len(), 10);
        let chunk_view0 =
            flat_storage_manager.chunk_view(0, Some(chain.get_block_hash(10)), false).unwrap();
        let chunk_view1 =
            flat_storage_manager.chunk_view(0, Some(chain.get_block_hash(4)), false).unwrap();
        assert_eq!(chunk_view0.get_ref(&[1]).unwrap(), None);
        assert_eq!(chunk_view0.get_ref(&[2]).unwrap(), Some(ValueRef::new(&[1])));
        assert_eq!(chunk_view1.get_ref(&[1]).unwrap(), Some(ValueRef::new(&[4])));
        assert_eq!(chunk_view1.get_ref(&[2]).unwrap(), None);
        assert_matches!(
            store_helper::get_delta(&store, 0, chain.get_block_hash(5)).unwrap(),
            Some(_)
        );
        assert_matches!(
            store_helper::get_delta(&store, 0, chain.get_block_hash(10)).unwrap(),
            Some(_)
        );

        // 5. Move the flat head to block 5, verify that chunk_view0 still returns the same values
        // and chunk_view1 returns an error. Also check that DBCol::FlatState is updated correctly
        flat_storage.update_flat_head(&chain.get_block_hash(5)).unwrap();
        assert_eq!(store_helper::get_ref(&store, &[1]).unwrap(), Some(ValueRef::new(&[5])));
        let blocks = flat_storage.get_blocks_to_head(&chain.get_block_hash(10)).unwrap();
        assert_eq!(blocks.len(), 5);
        assert_eq!(chunk_view0.get_ref(&[1]).unwrap(), None);
        assert_eq!(chunk_view0.get_ref(&[2]).unwrap(), Some(ValueRef::new(&[1])));
        assert_matches!(chunk_view1.get_ref(&[1]), Err(StorageError::FlatStorageError(_)));
        assert_matches!(store_helper::get_delta(&store, 0, chain.get_block_hash(5)).unwrap(), None);
        assert_matches!(
            store_helper::get_delta(&store, 0, chain.get_block_hash(10)).unwrap(),
            Some(_)
        );

        // 6. Move the flat head to block 10, verify that chunk_view0 still returns the same values
        //    Also checks that DBCol::FlatState is updated correctly.
        flat_storage.update_flat_head(&chain.get_block_hash(10)).unwrap();
        let blocks = flat_storage.get_blocks_to_head(&chain.get_block_hash(10)).unwrap();
        assert_eq!(blocks.len(), 0);
        assert_eq!(store_helper::get_ref(&store, &[1]).unwrap(), None);
        assert_eq!(store_helper::get_ref(&store, &[2]).unwrap(), Some(ValueRef::new(&[1])));
        assert_eq!(chunk_view0.get_ref(&[1]).unwrap(), None);
        assert_eq!(chunk_view0.get_ref(&[2]).unwrap(), Some(ValueRef::new(&[1])));
        assert_matches!(
            store_helper::get_delta(&store, 0, chain.get_block_hash(10)).unwrap(),
            None
        );
    }

    #[test]
    fn flat_storage_sanity_cache() {
        flat_storage_sanity(100);
    }

    #[test]
    fn flat_storage_sanity_no_cache() {
        flat_storage_sanity(0);
    }

    #[test]
    fn flat_storage_cache_eviction() {
        // 1. Create a simple chain and add single key-value deltas for 3 consecutive blocks.
        let chain = MockChain::linear_chain(4);
        let store = create_test_store();
        let mut store_update = store.store_update();
        store_helper::set_flat_head(&mut store_update, 0, &chain.get_block_hash(0));

        let mut deltas: Vec<(BlockHeight, Vec<u8>, Option<ValueRef>)> = vec![
            (1, vec![1], Some(ValueRef::new(&[1 as u8]))),
            (2, vec![2], None),
            (3, vec![3], Some(ValueRef::new(&[3 as u8]))),
        ];
        for (height, key, value) in deltas.drain(..) {
            store_helper::set_delta(
                &mut store_update,
                0,
                chain.get_block_hash(height),
                &FlatStateDelta::from([(key, value)]),
            )
            .unwrap();
        }
        store_update.commit().unwrap();

        // 2. Create flat storage and apply 3 blocks to it.
        let flat_storage = FlatStorage::new(store.clone(), 0, 3, &chain, 2);
        let flat_storage_manager = FlatStorageManager::new(store.clone());
        flat_storage_manager.add_flat_storage_for_shard(0, flat_storage);
        let flat_storage = flat_storage_manager.get_flat_storage_for_shard(0).unwrap();
        flat_storage.update_flat_head(&chain.get_block_hash(3)).unwrap();

        {
            let mut guard = flat_storage.0.write().unwrap();
            // 1st key should be kicked out.
            assert_eq!(guard.get_cached_ref(&[1]), None);
            // For 2nd key, None should be cached.
            assert_eq!(guard.get_cached_ref(&[2]), Some(None));
            // For 3rd key, value should be cached.
            assert_eq!(guard.get_cached_ref(&[3]), Some(Some(ValueRef::new(&[3 as u8]))));
        }

        // Check that value for 1st key is correct, even though it is not in cache.
        assert_eq!(
            flat_storage.get_ref(&chain.get_block_hash(3), &[1]),
            Ok(Some(ValueRef::new(&[1 as u8])))
        );

        // After that, 1st key should be added back to LRU cache.
        {
            let mut guard = flat_storage.0.write().unwrap();
            assert_eq!(guard.get_cached_ref(&[1]), Some(Some(ValueRef::new(&[1 as u8]))));
        }
    }
}

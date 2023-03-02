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

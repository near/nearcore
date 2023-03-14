use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use near_o11y::metrics::IntGauge;
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::state::ValueRef;
use tracing::info;

use crate::flat::delta::CachedFlatStateChanges;
use crate::flat::store_helper::FlatStateColumn;
use crate::flat::{FlatStorageReadyStatus, FlatStorageStatus};
use crate::{metrics, Store, StoreUpdate};

use super::delta::{CachedFlatStateDelta, FlatStateDelta};
use super::types::FlatStorageError;
use super::{store_helper, BlockInfo};

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
// - All changes and metadata in `deltas` are stored on disk. And if a block is accepted by chain
//   then its changes must be stored on disk as well, if the block is a child of `flat_head`.
//   This makes sure that when a node restarts, FlatStorage can load changes for all blocks
//   after the `flat_head` block successfully.
pub(crate) struct FlatStorageInner {
    store: Store,
    /// UId of the shard which state is accessed by this flat storage.
    shard_uid: ShardUId,
    /// The block for which we store the key value pairs of the state after it is applied.
    /// For non catchup mode, it should be the last final block.
    flat_head: BlockInfo,
    /// Cached deltas for all blocks supported by this flat storage.
    deltas: HashMap<CryptoHash, CachedFlatStateDelta>,
    metrics: FlatStorageMetrics,
}

struct FlatStorageMetrics {
    flat_head_height: IntGauge,
    cached_deltas: IntGauge,
    cached_changes_num_items: IntGauge,
    cached_changes_size: IntGauge,
    distance_to_head: IntGauge,
}

impl FlatStorageInner {
    /// Creates `BlockNotSupported` error for the given block.
    fn create_block_not_supported_error(&self, block_hash: &CryptoHash) -> FlatStorageError {
        FlatStorageError::BlockNotSupported((self.flat_head.hash, *block_hash))
    }

    /// Gets changes for the given block and shard `self.shard_id`.
    fn get_block_changes(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Arc<CachedFlatStateChanges>, FlatStorageError> {
        // TODO (#7327): add limitation on cached changes number to limit RAM usage
        // and read single `ValueRef` from delta if it is not cached.
        self.deltas
            .get(block_hash)
            .ok_or_else(|| self.create_block_not_supported_error(block_hash))
            .map(|delta| delta.changes.clone())
    }

    /// Get sequence of blocks `target_block_hash` (inclusive) to flat head (exclusive)
    /// in backwards chain order. Returns an error if there is no path between them.
    fn get_blocks_to_head(
        &self,
        target_block_hash: &CryptoHash,
    ) -> Result<Vec<CryptoHash>, FlatStorageError> {
        let flat_head = &self.flat_head;
        let mut block_hash = *target_block_hash;
        let mut blocks = vec![];
        while block_hash != flat_head.hash {
            blocks.push(block_hash);
            block_hash = self
                .deltas
                .get(&block_hash)
                .ok_or_else(|| self.create_block_not_supported_error(target_block_hash))?
                .metadata
                .block
                .prev_hash;
        }
        self.metrics.distance_to_head.set(blocks.len() as i64);

        Ok(blocks)
    }
}

impl FlatStorage {
    /// Create a new FlatStorage for `shard_id` using flat head if it is stored on storage.
    /// We also load all blocks with height between flat head to `latest_block_height`
    /// including those on forks into the returned FlatStorage.
    pub fn new(store: Store, shard_uid: ShardUId) -> Self {
        let shard_id = shard_uid.shard_id();
        let flat_head = match store_helper::get_flat_storage_status(&store, shard_uid) {
            FlatStorageStatus::Ready(ready_status) => ready_status.flat_head,
            status => {
                panic!("cannot create flat storage for shard {shard_id} with status {status:?}")
            }
        };

        // `itoa` is much faster for printing shard_id to a string than trivial alternatives.
        let mut buffer = itoa::Buffer::new();
        let shard_id_label = buffer.format(shard_id);
        let metrics = FlatStorageMetrics {
            flat_head_height: metrics::FLAT_STORAGE_HEAD_HEIGHT
                .with_label_values(&[shard_id_label]),
            cached_deltas: metrics::FLAT_STORAGE_CACHED_DELTAS.with_label_values(&[shard_id_label]),
            cached_changes_num_items: metrics::FLAT_STORAGE_CACHED_CHANGES_NUM_ITEMS
                .with_label_values(&[shard_id_label]),
            cached_changes_size: metrics::FLAT_STORAGE_CACHED_CHANGES_SIZE
                .with_label_values(&[shard_id_label]),
            distance_to_head: metrics::FLAT_STORAGE_DISTANCE_TO_HEAD
                .with_label_values(&[shard_id_label]),
        };
        metrics.flat_head_height.set(flat_head.height as i64);

        let deltas_metadata = store_helper::get_all_deltas_metadata(&store, shard_uid)
            .unwrap_or_else(|_| {
                panic!("Cannot read flat state deltas metadata for shard {shard_id} from storage")
            });
        let mut deltas = HashMap::new();
        for delta_metadata in deltas_metadata {
            let block_hash = delta_metadata.block.hash;
            let changes: CachedFlatStateChanges =
                store_helper::get_delta_changes(&store, shard_uid, block_hash)
                    .expect("Borsh cannot fail")
                    .unwrap_or_else(|| {
                        panic!("Cannot find block delta for block {block_hash:?} shard {shard_id}")
                    })
                    .into();
            metrics.cached_deltas.inc();
            metrics.cached_changes_num_items.add(changes.len() as i64);
            metrics.cached_changes_size.add(changes.total_size() as i64);
            deltas.insert(
                block_hash,
                CachedFlatStateDelta { metadata: delta_metadata, changes: Arc::new(changes) },
            );
        }

        Self(Arc::new(RwLock::new(FlatStorageInner {
            store,
            shard_uid,
            flat_head,
            deltas,
            metrics,
        })))
    }

    /// Get sequence of blocks `target_block_hash` (inclusive) to flat head (exclusive)
    /// in backwards chain order. Returns an error if there is no path between them.
    #[cfg(test)]
    #[allow(unused)]
    pub(crate) fn get_blocks_to_head(
        &self,
        target_block_hash: &CryptoHash,
    ) -> Result<Vec<CryptoHash>, FlatStorageError> {
        let guard = self.0.read().expect(super::POISONED_LOCK_ERR);
        guard.get_blocks_to_head(target_block_hash)
    }

    pub fn get_ref(
        &self,
        block_hash: &CryptoHash,
        key: &[u8],
    ) -> Result<Option<ValueRef>, crate::StorageError> {
        let guard = self.0.read().expect(super::POISONED_LOCK_ERR);
        let blocks_to_head =
            guard.get_blocks_to_head(block_hash).map_err(|e| StorageError::from(e))?;
        for block_hash in blocks_to_head.iter() {
            // If we found a key in changes, we can return a value because it is the most recent key update.
            let changes = guard.get_block_changes(block_hash)?;
            match changes.get(key) {
                Some(value_ref) => {
                    return Ok(value_ref);
                }
                None => {}
            };
        }

        let value_ref = store_helper::get_ref(&guard.store, guard.shard_uid, key)?;
        Ok(value_ref)
    }

    /// Update the head of the flat storage, including updating the flat state in memory and on disk
    /// and updating the flat state to reflect the state at the new head. If updating to given head is not possible,
    /// returns an error.
    pub fn update_flat_head(&self, new_head: &CryptoHash) -> Result<(), FlatStorageError> {
        let mut guard = self.0.write().expect(crate::flat::POISONED_LOCK_ERR);
        let shard_uid = guard.shard_uid;
        let shard_id = shard_uid.shard_id();
        let blocks = guard.get_blocks_to_head(new_head)?;
        if blocks.is_empty() {
            // This effectively means that new flat head is the same as the current one,
            // so we are not updating it
            return Err(guard.create_block_not_supported_error(new_head));
        }
        for block_hash in blocks.into_iter().rev() {
            let mut store_update = StoreUpdate::new(guard.store.storage.clone());
            // We unwrap here because flat storage is locked and we could retrieve path from old to new head,
            // so delta must exist.
            let changes =
                store_helper::get_delta_changes(&guard.store, shard_uid, block_hash)?.unwrap();
            changes.apply_to_flat_state(&mut store_update, guard.shard_uid);
            let block = &guard.deltas[&block_hash].metadata.block;
            store_helper::set_flat_storage_status(
                &mut store_update,
                shard_uid,
                FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head: block.clone() }),
            );
            guard.metrics.flat_head_height.set(block.height as i64);
            guard.flat_head = block.clone();

            // Remove old deltas from disk and memory.
            // Do it for each head update separately to ensure that old data is removed properly if node was
            // interrupted in the middle.
            // TODO (#7327): in case of long forks it can take a while and delay processing of some chunk.
            // Consider avoid iterating over all blocks and make removals lazy.
            let gc_height = guard
                .deltas
                .get(&block_hash)
                .ok_or(guard.create_block_not_supported_error(&block_hash))?
                .metadata
                .block
                .height;
            let hashes_to_remove: Vec<_> = guard
                .deltas
                .iter()
                .filter(|(_, delta)| delta.metadata.block.height <= gc_height)
                .map(|(block_hash, _)| block_hash)
                .cloned()
                .collect();
            for hash in hashes_to_remove {
                store_helper::remove_delta(&mut store_update, shard_uid, hash);
                match guard.deltas.remove(&hash) {
                    Some(delta) => {
                        guard.metrics.cached_deltas.dec();
                        guard.metrics.cached_changes_num_items.sub(delta.changes.len() as i64);
                        guard.metrics.cached_changes_size.sub(delta.changes.total_size() as i64);
                    }
                    None => {}
                }
            }

            store_update.commit().unwrap();
        }

        let new_head_height = guard.flat_head.height;
        info!(target: "chain", %shard_id, %new_head, %new_head_height, "Moved flat storage head");

        Ok(())
    }

    /// Adds a delta (including the changes and block info) to flat storage,
    /// returns a StoreUpdate to store the delta on disk. Node that this StoreUpdate should be
    /// committed to disk in one db transaction together with the rest of changes caused by block,
    /// in case the node stopped or crashed in between and a block is on chain but its delta is not
    /// stored or vice versa.
    pub fn add_delta(&self, delta: FlatStateDelta) -> Result<StoreUpdate, FlatStorageError> {
        let mut guard = self.0.write().expect(super::POISONED_LOCK_ERR);
        let shard_uid = guard.shard_uid;
        let shard_id = shard_uid.shard_id();
        let block = &delta.metadata.block;
        let block_hash = block.hash;
        let block_height = block.height;
        info!(target: "chain", %shard_id, %block_hash, %block_height, "Adding block to flat storage");
        if block.prev_hash != guard.flat_head.hash && !guard.deltas.contains_key(&block.prev_hash) {
            return Err(guard.create_block_not_supported_error(&block_hash));
        }
        let mut store_update = StoreUpdate::new(guard.store.storage.clone());
        store_helper::set_delta(&mut store_update, shard_uid, &delta)?;
        let cached_changes: CachedFlatStateChanges = delta.changes.into();
        guard.metrics.cached_deltas.inc();
        guard.metrics.cached_changes_num_items.add(cached_changes.len() as i64);
        guard.metrics.cached_changes_size.add(cached_changes.total_size() as i64);
        guard.deltas.insert(
            block_hash,
            CachedFlatStateDelta { metadata: delta.metadata, changes: Arc::new(cached_changes) },
        );
        Ok(store_update)
    }

    /// Clears all State key-value pairs from flat storage.
    pub fn clear_state(&self, shard_layout: ShardLayout) -> Result<(), StorageError> {
        let guard = self.0.write().expect(super::POISONED_LOCK_ERR);
        let shard_id = guard.shard_uid.shard_id();

        // Removes all items belonging to the shard one by one.
        // Note that it does not work for resharding.
        // TODO (#7327): call it just after we stopped tracking a shard.
        // TODO (#7327): remove FlatStateChanges. Consider custom serialization of keys to remove them by
        // prefix.
        // TODO (#7327): support range deletions which are much faster than naive deletions. For that, we
        // can delete ranges of keys like
        // [ [0]+boundary_accounts(shard_id) .. [0]+boundary_accounts(shard_id+1) ), etc.
        // We should also take fixed accounts into account.
        let mut store_update = guard.store.store_update();
        let mut removed_items = 0;
        for item in guard.store.iter(FlatStateColumn::State.to_db_col()) {
            let (key, _) =
                item.map_err(|e| StorageError::StorageInconsistentState(e.to_string()))?;

            if store_helper::key_belongs_to_shard(&key, &shard_layout, shard_id)? {
                removed_items += 1;
                store_update.delete(FlatStateColumn::State.to_db_col(), &key);
            }
        }
        info!(target: "chain", %shard_id, %removed_items, "Removing old items from flat storage");

        store_helper::set_flat_storage_status(
            &mut store_update,
            guard.shard_uid,
            FlatStorageStatus::Empty,
        );
        store_update.commit().map_err(|_| StorageError::StorageInternalError)?;
        Ok(())
    }
}

#[cfg(feature = "protocol_feature_flat_state")]
#[cfg(test)]
mod tests {
    use crate::flat::delta::{FlatStateChanges, FlatStateDelta, FlatStateDeltaMetadata};
    use crate::flat::manager::FlatStorageManager;
    use crate::flat::storage::FlatStorage;
    use crate::flat::types::{BlockInfo, FlatStorageError};
    use crate::flat::{store_helper, FlatStorageReadyStatus, FlatStorageStatus};
    use crate::test_utils::create_test_store;
    use crate::StorageError;
    use borsh::BorshSerialize;
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::state::ValueRef;
    use near_primitives::types::BlockHeight;

    use assert_matches::assert_matches;
    use near_primitives::shard_layout::ShardUId;
    use std::collections::HashMap;

    struct MockChain {
        height_to_hashes: HashMap<BlockHeight, CryptoHash>,
        blocks: HashMap<CryptoHash, BlockInfo>,
        head_height: BlockHeight,
    }

    impl MockChain {
        fn get_block_info(&self, block_hash: &CryptoHash) -> BlockInfo {
            self.blocks.get(block_hash).unwrap().clone()
        }

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

        fn get_block(&self, height: BlockHeight) -> BlockInfo {
            self.blocks[&self.height_to_hashes[&height]].clone()
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

    #[test]
    fn block_not_supported_errors() {
        // Create a chain with two forks. Set flat head to be at block 0.
        let chain = MockChain::chain_with_two_forks(5);
        let shard_uid = ShardUId::single_shard();
        let shard_id = shard_uid.shard_id();
        let store = create_test_store();
        let mut store_update = store.store_update();
        store_helper::set_flat_storage_status(
            &mut store_update,
            shard_uid,
            FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head: chain.get_block(0) }),
        );
        for i in 1..5 {
            let delta = FlatStateDelta {
                changes: FlatStateChanges::default(),
                metadata: FlatStateDeltaMetadata { block: chain.get_block(i) },
            };
            store_helper::set_delta(&mut store_update, shard_uid, &delta).unwrap();
        }
        store_update.commit().unwrap();

        let flat_storage = FlatStorage::new(store.clone(), shard_uid);
        let flat_storage_manager = FlatStorageManager::new(store.clone());
        flat_storage_manager.add_flat_storage_for_shard(shard_id, flat_storage);
        let flat_storage = flat_storage_manager.get_flat_storage_for_shard(shard_id).unwrap();

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
        let shard_uid = ShardUId::single_shard();
        let shard_id = shard_uid.shard_id();
        let store = create_test_store();
        let mut store_update = store.store_update();
        store_helper::set_flat_storage_status(
            &mut store_update,
            shard_uid,
            FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head: chain.get_block(0) }),
        );
        for i in 1..5 {
            let delta = FlatStateDelta {
                changes: FlatStateChanges::default(),
                metadata: FlatStateDeltaMetadata { block: chain.get_block(i * 2) },
            };
            store_helper::set_delta(&mut store_update, shard_uid, &delta).unwrap();
        }
        store_update.commit().unwrap();

        // Check that flat storage state is created correctly for chain which has skipped heights.
        let flat_storage = FlatStorage::new(store.clone(), shard_uid);
        let flat_storage_manager = FlatStorageManager::new(store.clone());
        flat_storage_manager.add_flat_storage_for_shard(shard_id, flat_storage);
        let flat_storage = flat_storage_manager.get_flat_storage_for_shard(shard_id).unwrap();

        // Check that flat head can be moved to block 8.
        let flat_head_hash = chain.get_block_hash(8);
        assert_eq!(flat_storage.update_flat_head(&flat_head_hash), Ok(()));
    }

    // This tests basic use cases for FlatStorageChunkView and FlatStorage.
    // We created a linear chain with no forks, start with flat head at the genesis block, then
    // moves the flat head forward, which checking that chunk_view.get_ref() still returns the correct
    // values and the state is being updated in store.
    #[test]
    fn flat_storage_sanity() {
        // 1. Create a chain with 10 blocks with no forks. Set flat head to be at block 0.
        //    Block i sets value for key &[1] to &[i].
        let mut chain = MockChain::linear_chain(10);
        let shard_uid = ShardUId::single_shard();
        let shard_id = shard_uid.shard_id();
        let store = create_test_store();
        let mut store_update = store.store_update();
        store_helper::set_flat_storage_status(
            &mut store_update,
            shard_uid,
            FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head: chain.get_block(0) }),
        );
        store_helper::set_ref(&mut store_update, shard_uid, vec![1], Some(ValueRef::new(&[0])))
            .unwrap();
        for i in 1..10 {
            let delta = FlatStateDelta {
                changes: FlatStateChanges::from([(vec![1], Some(ValueRef::new(&[i as u8])))]),
                metadata: FlatStateDeltaMetadata { block: chain.get_block(i) },
            };
            store_helper::set_delta(&mut store_update, shard_uid, &delta).unwrap();
        }
        store_update.commit().unwrap();

        let flat_storage = FlatStorage::new(store.clone(), shard_uid);
        let flat_storage_manager = FlatStorageManager::new(store.clone());
        flat_storage_manager.add_flat_storage_for_shard(shard_id, flat_storage);
        let flat_storage = flat_storage_manager.get_flat_storage_for_shard(0).unwrap();

        // 2. Check that the chunk_view at block i reads the value of key &[1] as &[i]
        for i in 0..10 {
            let block_hash = chain.get_block_hash(i);
            let blocks = flat_storage.get_blocks_to_head(&block_hash).unwrap();
            assert_eq!(blocks.len(), i as usize);
            let chunk_view =
                flat_storage_manager.chunk_view(shard_id, Some(block_hash), false).unwrap();
            assert_eq!(chunk_view.get_ref(&[1]).unwrap(), Some(ValueRef::new(&[i as u8])));
        }

        // 3. Create a new block that deletes &[1] and add a new value &[2]
        //    Add the block to flat storage.
        let hash = chain.create_block();
        let store_update = flat_storage
            .add_delta(FlatStateDelta {
                changes: FlatStateChanges::from([
                    (vec![1], None),
                    (vec![2], Some(ValueRef::new(&[1]))),
                ]),
                metadata: FlatStateDeltaMetadata { block: chain.get_block_info(&hash) },
            })
            .unwrap();
        store_update.commit().unwrap();

        // 4. Create a flat_state0 at block 10 and flat_state1 at block 4
        //    Verify that they return the correct values
        let blocks = flat_storage.get_blocks_to_head(&chain.get_block_hash(10)).unwrap();
        assert_eq!(blocks.len(), 10);
        let chunk_view0 = flat_storage_manager
            .chunk_view(shard_id, Some(chain.get_block_hash(10)), false)
            .unwrap();
        let chunk_view1 = flat_storage_manager
            .chunk_view(shard_id, Some(chain.get_block_hash(4)), false)
            .unwrap();
        assert_eq!(chunk_view0.get_ref(&[1]).unwrap(), None);
        assert_eq!(chunk_view0.get_ref(&[2]).unwrap(), Some(ValueRef::new(&[1])));
        assert_eq!(chunk_view1.get_ref(&[1]).unwrap(), Some(ValueRef::new(&[4])));
        assert_eq!(chunk_view1.get_ref(&[2]).unwrap(), None);
        assert_matches!(
            store_helper::get_delta_changes(&store, shard_uid, chain.get_block_hash(5)).unwrap(),
            Some(_)
        );
        assert_matches!(
            store_helper::get_delta_changes(&store, shard_uid, chain.get_block_hash(10)).unwrap(),
            Some(_)
        );

        // 5. Move the flat head to block 5, verify that chunk_view0 still returns the same values
        // and chunk_view1 returns an error. Also check that DBCol::FlatState is updated correctly
        flat_storage.update_flat_head(&chain.get_block_hash(5)).unwrap();
        assert_eq!(
            store_helper::get_ref(&store, shard_uid, &[1]).unwrap(),
            Some(ValueRef::new(&[5]))
        );
        let blocks = flat_storage.get_blocks_to_head(&chain.get_block_hash(10)).unwrap();
        assert_eq!(blocks.len(), 5);
        assert_eq!(chunk_view0.get_ref(&[1]).unwrap(), None);
        assert_eq!(chunk_view0.get_ref(&[2]).unwrap(), Some(ValueRef::new(&[1])));
        assert_matches!(chunk_view1.get_ref(&[1]), Err(StorageError::FlatStorageError(_)));
        assert_matches!(
            store_helper::get_delta_changes(&store, shard_uid, chain.get_block_hash(5)).unwrap(),
            None
        );
        assert_matches!(
            store_helper::get_delta_changes(&store, shard_uid, chain.get_block_hash(10)).unwrap(),
            Some(_)
        );

        // 6. Move the flat head to block 10, verify that chunk_view0 still returns the same values
        //    Also checks that DBCol::FlatState is updated correctly.
        flat_storage.update_flat_head(&chain.get_block_hash(10)).unwrap();
        let blocks = flat_storage.get_blocks_to_head(&chain.get_block_hash(10)).unwrap();
        assert_eq!(blocks.len(), 0);
        assert_eq!(store_helper::get_ref(&store, shard_uid, &[1]).unwrap(), None);
        assert_eq!(
            store_helper::get_ref(&store, shard_uid, &[2]).unwrap(),
            Some(ValueRef::new(&[1]))
        );
        assert_eq!(chunk_view0.get_ref(&[1]).unwrap(), None);
        assert_eq!(chunk_view0.get_ref(&[2]).unwrap(), Some(ValueRef::new(&[1])));
        assert_matches!(
            store_helper::get_delta_changes(&store, shard_uid, chain.get_block_hash(10)).unwrap(),
            None
        );
    }
}

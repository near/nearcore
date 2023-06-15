use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state::FlatStateValue;
use tracing::{debug, warn};

use crate::flat::delta::CachedFlatStateChanges;
use crate::flat::{FlatStorageReadyStatus, FlatStorageStatus};
use crate::{Store, StoreUpdate};

use super::delta::{CachedFlatStateDelta, FlatStateDelta};
use super::metrics::FlatStorageMetrics;
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
    /// This flag enables skipping flat head moves, needed temporarily for FlatState
    /// values inlining migration.
    /// The flag has a numerical value and not a bool, to let us detect attempts
    /// to disable move head multiple times.
    move_head_enabled: bool,
    metrics: FlatStorageMetrics,
}

impl FlatStorageInner {
    /// Expected limits for in-memory stored changes, under which flat storage must keep working.
    /// If they are exceeded, warnings are displayed. Flat storage still will work, but its
    /// performance will slow down, and eventually it can cause OOM error.
    /// Limit for number of changes. When over 100, introduces 89 us overhead:
    /// https://github.com/near/nearcore/issues/8006#issuecomment-1473621334
    const CACHED_CHANGES_LIMIT: usize = 100;
    /// Limit for total size of cached changes. We allocate 600 MiB for cached deltas, which
    /// means 150 MiB per shards.
    const CACHED_CHANGES_SIZE_LIMIT: bytesize::ByteSize = bytesize::ByteSize(150 * bytesize::MIB);

    /// Creates `BlockNotSupported` error for the given block.
    fn create_block_not_supported_error(&self, block_hash: &CryptoHash) -> FlatStorageError {
        FlatStorageError::BlockNotSupported((self.flat_head.hash, *block_hash))
    }

    /// Gets changes for the given block and shard `self.shard_uid`, assuming that they must exist.
    fn get_block_changes(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Arc<CachedFlatStateChanges>, FlatStorageError> {
        // TODO (#7327): add limitation on cached changes number to limit RAM usage
        // and read single `ValueRef` from delta if it is not cached.
        self.deltas
            .get(block_hash)
            .ok_or_else(|| missing_delta_error(block_hash))
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
        self.metrics.set_distance_to_head(blocks.len());

        Ok(blocks)
    }

    /// Updates metrics related to deltas, displays a warning if they are off.
    fn update_delta_metrics(&self) {
        let cached_deltas = self.deltas.len();
        let mut cached_changes_num_items = 0;
        let mut cached_changes_size = 0;
        for changes in self.deltas.values() {
            cached_changes_num_items += changes.changes.len();
            cached_changes_size += changes.changes.total_size();
        }

        self.metrics.set_cached_deltas(
            cached_deltas,
            cached_changes_num_items,
            cached_changes_size,
        );

        let cached_changes_size_bytes = bytesize::ByteSize(cached_changes_size);
        if cached_deltas >= Self::CACHED_CHANGES_LIMIT
            || cached_changes_size_bytes >= Self::CACHED_CHANGES_SIZE_LIMIT
        {
            let shard_id = self.shard_uid.shard_id();
            let flat_head_height = self.flat_head.height;
            warn!(target: "chain", %shard_id, %flat_head_height, %cached_deltas, %cached_changes_size_bytes, "Flat storage cached deltas exceeded expected limits");
        }
    }
}

impl FlatStorage {
    /// Create a new FlatStorage for `shard_uid` using flat head if it is stored on storage.
    /// We also load all blocks with height between flat head to `latest_block_height`
    /// including those on forks into the returned FlatStorage.
    pub fn new(store: Store, shard_uid: ShardUId) -> Result<Self, StorageError> {
        let shard_id = shard_uid.shard_id();
        let flat_head = match store_helper::get_flat_storage_status(&store, shard_uid) {
            Ok(FlatStorageStatus::Ready(ready_status)) => ready_status.flat_head,
            status => {
                return Err(StorageError::StorageInconsistentState(format!(
                    "Cannot create flat storage for shard {shard_id} with status {status:?}"
                )));
            }
        };
        let metrics = FlatStorageMetrics::new(shard_id);
        metrics.set_flat_head_height(flat_head.height);

        let deltas_metadata = store_helper::get_all_deltas_metadata(&store, shard_uid)
            .unwrap_or_else(|_| {
                panic!("Cannot read flat state deltas metadata for shard {shard_id} from storage")
            });
        let mut deltas = HashMap::new();
        for delta_metadata in deltas_metadata {
            let block_hash = delta_metadata.block.hash;
            let changes: CachedFlatStateChanges =
                store_helper::get_delta_changes(&store, shard_uid, block_hash)
                    .expect("failed to read flat state delta changes")
                    .unwrap_or_else(|| {
                        panic!("cannot find block delta for block {block_hash:?} shard {shard_id}")
                    })
                    .into();
            deltas.insert(
                block_hash,
                CachedFlatStateDelta { metadata: delta_metadata, changes: Arc::new(changes) },
            );
        }

        let inner = FlatStorageInner {
            store,
            shard_uid,
            flat_head,
            deltas,
            move_head_enabled: true,
            metrics,
        };
        inner.update_delta_metrics();
        Ok(Self(Arc::new(RwLock::new(inner))))
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

    pub fn get_value(
        &self,
        block_hash: &CryptoHash,
        key: &[u8],
    ) -> Result<Option<FlatStateValue>, crate::StorageError> {
        let guard = self.0.read().expect(super::POISONED_LOCK_ERR);
        let blocks_to_head =
            guard.get_blocks_to_head(block_hash).map_err(|e| StorageError::from(e))?;
        for block_hash in blocks_to_head.iter() {
            // If we found a key in changes, we can return a value because it is the most recent key update.
            let changes = guard.get_block_changes(block_hash)?;
            match changes.get(key) {
                Some(value_ref) => {
                    return Ok(value_ref.map(|value_ref| FlatStateValue::Ref(value_ref)));
                }
                None => {}
            };
        }

        let value = store_helper::get_flat_state_value(&guard.store, guard.shard_uid, key)?;
        Ok(value)
    }

    /// Update the head of the flat storage, including updating the flat state in memory and on disk
    /// and updating the flat state to reflect the state at the new head. If updating to given head is not possible,
    /// returns an error.
    pub fn update_flat_head(&self, new_head: &CryptoHash) -> Result<(), FlatStorageError> {
        let mut guard = self.0.write().expect(crate::flat::POISONED_LOCK_ERR);
        if !guard.move_head_enabled {
            return Ok(());
        }
        let shard_uid = guard.shard_uid;
        let shard_id = shard_uid.shard_id();
        let blocks = guard.get_blocks_to_head(new_head)?;
        for block_hash in blocks.into_iter().rev() {
            let mut store_update = StoreUpdate::new(guard.store.storage.clone());
            // Delta must exist because flat storage is locked and we could retrieve
            // path from old to new head. Otherwise we return internal error.
            let changes = store_helper::get_delta_changes(&guard.store, shard_uid, block_hash)?
                .ok_or_else(|| missing_delta_error(&block_hash))?;
            changes.apply_to_flat_state(&mut store_update, guard.shard_uid);
            let block = &guard.deltas[&block_hash].metadata.block;
            let block_height = block.height;
            store_helper::set_flat_storage_status(
                &mut store_update,
                shard_uid,
                FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head: *block }),
            );

            guard.metrics.set_flat_head_height(block.height);
            guard.flat_head = *block;

            // Remove old deltas from disk and memory.
            // Do it for each head update separately to ensure that old data is removed properly if node was
            // interrupted in the middle.
            // TODO (#7327): in case of long forks it can take a while and delay processing of some chunk.
            // Consider avoid iterating over all blocks and make removals lazy.
            let gc_height = guard
                .deltas
                .get(&block_hash)
                .ok_or_else(|| missing_delta_error(&block_hash))?
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
                guard.deltas.remove(&hash);
            }

            store_update.commit().unwrap();
            debug!(target: "store", %shard_id, %block_hash, %block_height, "Moved flat storage head");
        }
        guard.update_delta_metrics();

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
        debug!(target: "store", %shard_id, %block_hash, %block_height, "Adding block to flat storage");
        if block.prev_hash != guard.flat_head.hash && !guard.deltas.contains_key(&block.prev_hash) {
            return Err(guard.create_block_not_supported_error(&block_hash));
        }
        let mut store_update = StoreUpdate::new(guard.store.storage.clone());
        store_helper::set_delta(&mut store_update, shard_uid, &delta);
        let cached_changes: CachedFlatStateChanges = delta.changes.into();
        guard.deltas.insert(
            block_hash,
            CachedFlatStateDelta { metadata: delta.metadata, changes: Arc::new(cached_changes) },
        );
        guard.update_delta_metrics();

        Ok(store_update)
    }

    /// Clears all State key-value pairs from flat storage.
    pub fn clear_state(&self) -> Result<(), StorageError> {
        let guard = self.0.write().expect(super::POISONED_LOCK_ERR);

        let mut store_update = guard.store.store_update();
        store_helper::remove_all_flat_state_values(&mut store_update, guard.shard_uid);
        store_helper::remove_all_deltas(&mut store_update, guard.shard_uid);

        store_helper::set_flat_storage_status(
            &mut store_update,
            guard.shard_uid,
            FlatStorageStatus::Empty,
        );
        store_update.commit().map_err(|_| StorageError::StorageInternalError)?;
        guard.update_delta_metrics();

        Ok(())
    }

    pub(crate) fn get_head_hash(&self) -> CryptoHash {
        let guard = self.0.read().expect(super::POISONED_LOCK_ERR);
        guard.flat_head.hash
    }

    pub(crate) fn shard_uid(&self) -> ShardUId {
        let guard = self.0.read().expect(super::POISONED_LOCK_ERR);
        guard.shard_uid
    }

    /// Updates `move_head_enabled` and returns whether the change was done.
    pub(crate) fn set_flat_head_update_mode(&self, enabled: bool) -> bool {
        let mut guard = self.0.write().expect(crate::flat::POISONED_LOCK_ERR);
        if enabled != guard.move_head_enabled {
            guard.move_head_enabled = enabled;
            true
        } else {
            false
        }
    }
}

fn missing_delta_error(block_hash: &CryptoHash) -> FlatStorageError {
    FlatStorageError::StorageInternalError(format!("delta does not exist for block {block_hash}"))
}

#[cfg(test)]
mod tests {
    use crate::flat::delta::{FlatStateChanges, FlatStateDelta, FlatStateDeltaMetadata};
    use crate::flat::manager::FlatStorageManager;
    use crate::flat::types::{BlockInfo, FlatStorageError};
    use crate::flat::{store_helper, FlatStorageReadyStatus, FlatStorageStatus};
    use crate::test_utils::create_test_store;
    use crate::StorageError;
    use borsh::BorshSerialize;
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::types::BlockHeight;

    use assert_matches::assert_matches;
    use near_primitives::shard_layout::ShardUId;
    use near_primitives::state::FlatStateValue;
    use std::collections::HashMap;

    struct MockChain {
        height_to_hashes: HashMap<BlockHeight, CryptoHash>,
        blocks: HashMap<CryptoHash, BlockInfo>,
        head_height: BlockHeight,
    }

    impl MockChain {
        fn get_block_info(&self, block_hash: &CryptoHash) -> BlockInfo {
            *self.blocks.get(block_hash).unwrap()
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
                    let hash = *height_to_hashes.get(&height).unwrap();
                    let prev_hash = match get_parent(height) {
                        None => CryptoHash::default(),
                        Some(parent_height) => *height_to_hashes.get(&parent_height).unwrap(),
                    };
                    (hash, BlockInfo { hash, height, prev_hash })
                })
                .collect();
            MockChain { height_to_hashes, blocks, head_height: *heights.last().unwrap() }
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
            self.blocks[&self.height_to_hashes[&height]]
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
    fn flat_storage_errors() {
        // Create a chain with two forks. Set flat head to be at block 0.
        let chain = MockChain::chain_with_two_forks(5);
        let shard_uid = ShardUId::single_shard();
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
            store_helper::set_delta(&mut store_update, shard_uid, &delta);
        }
        store_update.commit().unwrap();

        let flat_storage_manager = FlatStorageManager::new(store.clone());
        flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
        let flat_storage = flat_storage_manager.get_flat_storage_for_shard(shard_uid).unwrap();

        // Check `BlockNotSupported` errors which are fine to occur during regular block processing.
        // First, check that flat head can be moved to block 1.
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
        // Corrupt DB state for block 3 and try moving flat head to it.
        // Should result in `StorageInternalError` indicating that flat storage is broken.
        let mut store_update = store.store_update();
        store_helper::remove_delta(&mut store_update, shard_uid, chain.get_block_hash(3));
        store_update.commit().unwrap();
        assert_matches!(
            flat_storage.update_flat_head(&chain.get_block_hash(3)),
            Err(FlatStorageError::StorageInternalError(_))
        );
    }

    #[test]
    fn skipped_heights() {
        // Create a linear chain where some heights are skipped.
        let chain = MockChain::linear_chain_with_skips(5);
        let shard_uid = ShardUId::single_shard();
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
            store_helper::set_delta(&mut store_update, shard_uid, &delta);
        }
        store_update.commit().unwrap();

        // Check that flat storage state is created correctly for chain which has skipped heights.
        let flat_storage_manager = FlatStorageManager::new(store);
        flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
        let flat_storage = flat_storage_manager.get_flat_storage_for_shard(shard_uid).unwrap();

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
        let store = create_test_store();
        let mut store_update = store.store_update();
        store_helper::set_flat_storage_status(
            &mut store_update,
            shard_uid,
            FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head: chain.get_block(0) }),
        );
        store_helper::set_flat_state_value(
            &mut store_update,
            shard_uid,
            vec![1],
            Some(FlatStateValue::value_ref(&[0])),
        );
        for i in 1..10 {
            let delta = FlatStateDelta {
                changes: FlatStateChanges::from([(
                    vec![1],
                    Some(FlatStateValue::value_ref(&[i as u8])),
                )]),
                metadata: FlatStateDeltaMetadata { block: chain.get_block(i) },
            };
            store_helper::set_delta(&mut store_update, shard_uid, &delta);
        }
        store_update.commit().unwrap();

        let flat_storage_manager = FlatStorageManager::new(store.clone());
        flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
        let flat_storage = flat_storage_manager.get_flat_storage_for_shard(shard_uid).unwrap();

        // 2. Check that the chunk_view at block i reads the value of key &[1] as &[i]
        for i in 0..10 {
            let block_hash = chain.get_block_hash(i);
            let blocks = flat_storage.get_blocks_to_head(&block_hash).unwrap();
            assert_eq!(blocks.len(), i as usize);
            let chunk_view = flat_storage_manager.chunk_view(shard_uid, block_hash).unwrap();
            assert_eq!(
                chunk_view.get_value(&[1]).unwrap(),
                Some(FlatStateValue::value_ref(&[i as u8]))
            );
        }

        // 3. Create a new block that deletes &[1] and add a new value &[2]
        //    Add the block to flat storage.
        let hash = chain.create_block();
        let store_update = flat_storage
            .add_delta(FlatStateDelta {
                changes: FlatStateChanges::from([
                    (vec![1], None),
                    (vec![2], Some(FlatStateValue::value_ref(&[1]))),
                ]),
                metadata: FlatStateDeltaMetadata { block: chain.get_block_info(&hash) },
            })
            .unwrap();
        store_update.commit().unwrap();

        // 4. Create a flat_state0 at block 10 and flat_state1 at block 4
        //    Verify that they return the correct values
        let blocks = flat_storage.get_blocks_to_head(&chain.get_block_hash(10)).unwrap();
        assert_eq!(blocks.len(), 10);
        let chunk_view0 =
            flat_storage_manager.chunk_view(shard_uid, chain.get_block_hash(10)).unwrap();
        let chunk_view1 =
            flat_storage_manager.chunk_view(shard_uid, chain.get_block_hash(4)).unwrap();
        assert_eq!(chunk_view0.get_value(&[1]).unwrap(), None);
        assert_eq!(chunk_view0.get_value(&[2]).unwrap(), Some(FlatStateValue::value_ref(&[1])));
        assert_eq!(chunk_view1.get_value(&[1]).unwrap(), Some(FlatStateValue::value_ref(&[4])));
        assert_eq!(chunk_view1.get_value(&[2]).unwrap(), None);
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
            store_helper::get_flat_state_value(&store, shard_uid, &[1]).unwrap(),
            Some(FlatStateValue::value_ref(&[5]))
        );
        let blocks = flat_storage.get_blocks_to_head(&chain.get_block_hash(10)).unwrap();
        assert_eq!(blocks.len(), 5);
        assert_eq!(chunk_view0.get_value(&[1]).unwrap(), None);
        assert_eq!(chunk_view0.get_value(&[2]).unwrap(), Some(FlatStateValue::value_ref(&[1])));
        assert_matches!(
            chunk_view1.get_value(&[1]),
            Err(StorageError::FlatStorageBlockNotSupported(_))
        );
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
        assert_eq!(store_helper::get_flat_state_value(&store, shard_uid, &[1]).unwrap(), None);
        assert_eq!(
            store_helper::get_flat_state_value(&store, shard_uid, &[2]).unwrap(),
            Some(FlatStateValue::value_ref(&[1]))
        );
        assert_eq!(chunk_view0.get_value(&[1]).unwrap(), None);
        assert_eq!(chunk_view0.get_value(&[2]).unwrap(), Some(FlatStateValue::value_ref(&[1])));
        assert_matches!(
            store_helper::get_delta_changes(&store, shard_uid, chain.get_block_hash(10)).unwrap(),
            None
        );
    }
}

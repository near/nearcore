use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state::FlatStateValue;
use near_primitives::types::BlockHeight;
use tracing::{debug, warn};

use crate::adapter::flat_store::{FlatStoreAdapter, FlatStoreUpdateAdapter};
use crate::adapter::StoreUpdateAdapter;
use crate::flat::delta::{BlockWithChangesInfo, CachedFlatStateChanges};
use crate::flat::BlockInfo;
use crate::flat::{FlatStorageReadyStatus, FlatStorageStatus};

use super::delta::{CachedFlatStateDelta, FlatStateDelta};
use super::metrics::FlatStorageMetrics;
use super::types::FlatStorageError;

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
    store: FlatStoreAdapter,
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
    /// Limit for number of blocks needed to read. When over 100, introduces significant overhead.
    /// https://github.com/near/nearcore/issues/8006#issuecomment-1473621334
    const HOPS_LIMIT: usize = 100;
    /// Limit for total size of cached changes. We allocate 600 MiB for cached deltas, which
    /// means 150 MiB per shards.
    const CACHED_CHANGES_SIZE_LIMIT: bytesize::ByteSize = bytesize::ByteSize(150 * bytesize::MIB);

    const BLOCKS_WITH_CHANGES_FLAT_HEAD_GAP: BlockHeight = 2;

    /// Creates `BlockNotSupported` error for the given block.
    /// In the context of updating the flat head, the error is handled gracefully.
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
        let mut first_height = None;
        while block_hash != flat_head.hash {
            let metadata = self
                .deltas
                .get(&block_hash)
                .ok_or_else(|| self.create_block_not_supported_error(target_block_hash))?
                .metadata;
            if first_height.is_none() {
                // Keep track of the height of the initial block.
                first_height = Some(metadata.block.height);
            }

            // Maybe skip to the previous block with changes.
            block_hash = match metadata.prev_block_with_changes {
                None => {
                    blocks.push(block_hash);
                    metadata.block.prev_hash
                }
                Some(prev_block_with_changes) => {
                    // Don't include blocks with no changes in the result,
                    // unless it's the target block.
                    if &block_hash == target_block_hash {
                        blocks.push(block_hash);
                    }
                    if prev_block_with_changes.height > flat_head.height {
                        prev_block_with_changes.hash
                    } else {
                        flat_head.hash
                    }
                }
            };
        }
        self.metrics.set_distance_to_head(
            blocks.len(),
            first_height.map(|height| height - flat_head.height),
        );
        if blocks.len() >= Self::HOPS_LIMIT {
            warn!(
                target: "chain",
                shard_id = self.shard_uid.shard_id(),
                flat_head_height = flat_head.height,
                cached_deltas = self.deltas.len(),
                num_hops = blocks.len(),
                "Flat storage needs too many hops to access a block");
        }

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
        if cached_changes_size_bytes >= Self::CACHED_CHANGES_SIZE_LIMIT {
            warn!(
                target: "chain",
                shard_id = self.shard_uid.shard_id(),
                flat_head_height = self.flat_head.height,
                cached_deltas,
                %cached_changes_size_bytes,
                "Flat storage total size of cached deltas exceeded expected limits");
        }
    }

    // Determine a block to make the new flat head.
    // If `strict`, uses `block_hash` as the new flat head.
    // If not `strict`, uses the second most recent block with flat state
    // changes from `block_hash`, if it exists.
    fn get_new_flat_head(
        &self,
        block_hash: CryptoHash,
        strict: bool,
    ) -> Result<CryptoHash, FlatStorageError> {
        if strict {
            return Ok(block_hash);
        }

        let current_flat_head_hash = self.flat_head.hash;
        let current_flat_head_height = self.flat_head.height;

        let mut new_head = block_hash;
        let mut blocks_with_changes = 0;
        // Delays updating flat head, keeps this many blocks with non-empty flat
        // state changes between the requested flat head and the chosen head to
        // make flat state snapshots function properly.
        while blocks_with_changes < Self::BLOCKS_WITH_CHANGES_FLAT_HEAD_GAP {
            if new_head == current_flat_head_hash {
                return Ok(current_flat_head_hash);
            }
            let metadata = self
                .deltas
                .get(&new_head)
                // BlockNotSupported error kind will be handled gracefully.
                .ok_or_else(|| self.create_block_not_supported_error(&new_head))?
                .metadata;
            new_head = match metadata.prev_block_with_changes {
                None => {
                    // The block has flat state changes.
                    blocks_with_changes += 1;
                    if blocks_with_changes == Self::BLOCKS_WITH_CHANGES_FLAT_HEAD_GAP {
                        break;
                    }
                    metadata.block.prev_hash
                }
                Some(BlockWithChangesInfo { hash, height, .. }) => {
                    // The block has no flat state changes.
                    if height <= current_flat_head_height {
                        return Ok(current_flat_head_hash);
                    }
                    hash
                }
            };
        }
        Ok(new_head)
    }

    #[cfg(test)]
    pub fn test_get_new_flat_head(
        &self,
        block_hash: CryptoHash,
        strict: bool,
    ) -> Result<CryptoHash, FlatStorageError> {
        self.get_new_flat_head(block_hash, strict)
    }
}

impl FlatStorage {
    /// Create a new FlatStorage for `shard_uid` using flat head if it is stored on storage.
    /// We also load all blocks with height between flat head to `latest_block_height`
    /// including those on forks into the returned FlatStorage.
    pub fn new(store: FlatStoreAdapter, shard_uid: ShardUId) -> Result<Self, StorageError> {
        let shard_id = shard_uid.shard_id();
        let flat_head = match store.get_flat_storage_status(shard_uid) {
            Ok(FlatStorageStatus::Ready(ready_status)) => ready_status.flat_head,
            status => {
                return Err(StorageError::StorageInconsistentState(format!(
                    "Cannot create flat storage for shard {shard_id} with status {status:?}"
                )));
            }
        };
        let metrics = FlatStorageMetrics::new(shard_uid);
        metrics.set_flat_head_height(flat_head.height);

        let deltas_metadata = store.get_all_deltas_metadata(shard_uid).unwrap_or_else(|_| {
            panic!("Cannot read flat state deltas metadata for shard {shard_id} from storage")
        });
        let mut deltas = HashMap::new();
        for delta_metadata in deltas_metadata {
            let block_hash = delta_metadata.block.hash;
            let changes: CachedFlatStateChanges = if delta_metadata.has_changes() {
                store
                    .get_delta(shard_uid, block_hash)
                    .expect("failed to read flat state delta changes")
                    .unwrap_or_else(|| {
                        panic!("cannot find block delta for block {block_hash:?} shard {shard_id}")
                    })
                    .into()
            } else {
                // Don't read delta if we know that it is empty.
                Default::default()
            };
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
        let blocks_to_head = guard.get_blocks_to_head(block_hash)?;
        for block_hash in blocks_to_head.iter() {
            // If we found a key in changes, we can return a value because it is the most recent key update.
            let changes = guard.get_block_changes(block_hash)?;
            match changes.get(key) {
                Some(value_ref) => {
                    return Ok(value_ref.clone().map(|value_ref| FlatStateValue::Ref(value_ref)));
                }
                None => {}
            };
        }

        let value = guard.store.get(guard.shard_uid, key)?;
        Ok(value)
    }

    /// Same as `get_value()?.is_some()`, but avoids reading out the value.
    pub fn contains_key(
        &self,
        block_hash: &CryptoHash,
        key: &[u8],
    ) -> Result<bool, crate::StorageError> {
        let guard = self.0.read().expect(super::POISONED_LOCK_ERR);
        let blocks_to_head =
            guard.get_blocks_to_head(block_hash).map_err(|e| StorageError::from(e))?;
        for block_hash in blocks_to_head.iter() {
            // If we found a key in changes, we can return a value because it is the most recent key update.
            let changes = guard.get_block_changes(block_hash)?;
            match changes.get(key) {
                Some(value_ref) => return Ok(value_ref.is_some()),
                None => {}
            };
        }

        Ok(guard.store.exists(guard.shard_uid, key)?)
    }

    // TODO(#11601): Direct call is DEPRECATED, consider removing non-strict mode.
    /// Update the head of the flat storage, including updating the flat state
    /// in memory and on disk and updating the flat state to reflect the state
    /// at the new head. If updating to given head is not possible, returns an
    /// error.
    /// If `strict`, then unconditionally sets flat head to the given block.
    /// If not `strict`, then it updates the flat head to the latest block X,
    /// such that [X, block_hash] contains 2 blocks with flat state changes. If possible.
    ///
    /// The function respects the current flat head and will never try to
    /// set flat head to a block older than the current flat head.
    //
    // Let's denote blocks with flat state changes as X, and blocks without
    // flat state changes as O.
    //
    //                        block_hash
    //                             |
    //                             v
    // ...-O-O-X-O-O-O-X-O-O-O-X-O-O-O-X-....->future
    //                 ^
    //                 |
    //              new_head
    //
    // The segment [new_head, block_hash] contains two blocks with flat state changes.
    fn update_flat_head_impl(
        &self,
        block_hash: &CryptoHash,
        strict: bool,
    ) -> Result<(), FlatStorageError> {
        let mut guard = self.0.write().expect(crate::flat::POISONED_LOCK_ERR);
        if !guard.move_head_enabled {
            return Ok(());
        }

        let new_head = guard.get_new_flat_head(*block_hash, strict)?;
        if new_head == guard.flat_head.hash {
            tracing::debug!(target: "store", "update_flat_head, shard id {}, flat head already at block {}", guard.shard_uid.shard_id(), guard.flat_head.height);
            return Ok(());
        }

        let shard_uid = guard.shard_uid;
        let shard_id = shard_uid.shard_id();

        tracing::debug!(target: "store", flat_head = ?guard.flat_head.hash, ?new_head, shard_id, "Moving flat head");
        let blocks = guard.get_blocks_to_head(&new_head)?;

        for block_hash in blocks.into_iter().rev() {
            let mut store_update = guard.store.store_update();
            // Delta must exist because flat storage is locked and we could retrieve
            // path from old to new head. Otherwise we return internal error.
            let changes = guard
                .store
                .get_delta(shard_uid, block_hash)?
                .ok_or_else(|| missing_delta_error(&block_hash))?;
            changes.apply_to_flat_state(&mut store_update, guard.shard_uid);
            let metadata = guard
                .deltas
                .get(&block_hash)
                .ok_or_else(|| missing_delta_error(&block_hash))?
                .metadata;
            let block = metadata.block;
            let block_height = block.height;
            store_update.set_flat_storage_status(
                shard_uid,
                FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head: block }),
            );

            guard.metrics.set_flat_head_height(block.height);
            guard.flat_head = block;

            // Remove old deltas from disk and memory.
            // Do it for each head update separately to ensure that old data is removed properly if node was
            // interrupted in the middle.
            // TODO (#7327): in case of long forks it can take a while and delay processing of some chunk.
            // Consider avoid iterating over all blocks and make removals lazy.
            let gc_height = metadata.block.height;
            let hashes_to_remove: Vec<_> = guard
                .deltas
                .iter()
                .filter(|(_, delta)| delta.metadata.block.height <= gc_height)
                .map(|(block_hash, _)| block_hash)
                .cloned()
                .collect();
            for hash in hashes_to_remove {
                store_update.remove_delta(shard_uid, hash);
                guard.deltas.remove(&hash);
            }

            store_update.commit().unwrap();
            debug!(target: "store", %shard_id, %block_hash, %block_height, "Moved flat storage head");
        }
        guard.update_delta_metrics();

        Ok(())
    }

    /// Update the head of the flat storage, including updating the flat state
    /// in memory and on disk and updating the flat state to reflect the state
    /// at the new head. If updating to given head is not possible, returns an
    /// error.
    pub fn update_flat_head(&self, block_hash: &CryptoHash) -> Result<(), FlatStorageError> {
        self.update_flat_head_impl(block_hash, true)
    }

    /// Adds a delta (including the changes and block info) to flat storage,
    /// returns a StoreUpdate to store the delta on disk. Node that this StoreUpdate should be
    /// committed to disk in one db transaction together with the rest of changes caused by block,
    /// in case the node stopped or crashed in between and a block is on chain but its delta is not
    /// stored or vice versa.
    pub fn add_delta(
        &self,
        delta: FlatStateDelta,
    ) -> Result<FlatStoreUpdateAdapter, FlatStorageError> {
        let mut guard = self.0.write().expect(super::POISONED_LOCK_ERR);
        let shard_uid = guard.shard_uid;
        let block = &delta.metadata.block;
        let block_hash = block.hash;
        let block_height = block.height;
        debug!(target: "store", %shard_uid, %block_hash, %block_height, "Adding block to flat storage");
        if block.prev_hash != guard.flat_head.hash && !guard.deltas.contains_key(&block.prev_hash) {
            return Err(guard.create_block_not_supported_error(&block_hash));
        }
        let mut store_update = guard.store.store_update();
        store_update.set_delta(shard_uid, &delta);
        let cached_changes: CachedFlatStateChanges = delta.changes.into();
        guard.deltas.insert(
            block_hash,
            CachedFlatStateDelta { metadata: delta.metadata, changes: Arc::new(cached_changes) },
        );
        guard.update_delta_metrics();

        Ok(store_update)
    }

    /// Clears all State key-value pairs from flat storage.
    pub fn clear_state(
        &self,
        store_update: &mut FlatStoreUpdateAdapter,
    ) -> Result<(), StorageError> {
        let guard = self.0.write().expect(super::POISONED_LOCK_ERR);
        let shard_uid = guard.shard_uid;
        store_update.remove_all(shard_uid);
        store_update.remove_all_deltas(shard_uid);
        store_update.set_flat_storage_status(shard_uid, FlatStorageStatus::Empty);
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
    use crate::adapter::{StoreAdapter, StoreUpdateAdapter};
    use crate::flat::delta::{
        BlockWithChangesInfo, FlatStateChanges, FlatStateDelta, FlatStateDeltaMetadata,
    };
    use crate::flat::manager::FlatStorageManager;
    use crate::flat::storage::FlatStorageInner;
    use crate::flat::test_utils::MockChain;
    use crate::flat::types::FlatStorageError;
    use crate::flat::{FlatStorageReadyStatus, FlatStorageStatus};
    use crate::test_utils::create_test_store;
    use crate::StorageError;
    use assert_matches::assert_matches;

    use near_o11y::testonly::init_test_logger;
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::shard_layout::ShardUId;
    use near_primitives::state::FlatStateValue;
    use near_primitives::types::BlockHeight;
    use rand::{thread_rng, Rng};
    use std::collections::HashMap;

    #[test]
    fn flat_storage_errors() {
        // Create a chain with two forks. Set flat head to be at block 0.
        let chain = MockChain::chain_with_two_forks(5);
        let shard_uid = ShardUId::single_shard();
        let store = create_test_store().flat_store();
        let mut store_update = store.store_update();
        store_update.set_flat_storage_status(
            shard_uid,
            FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head: chain.get_block(0) }),
        );
        for i in 1..5 {
            let delta = FlatStateDelta {
                changes: FlatStateChanges::default(),
                metadata: FlatStateDeltaMetadata {
                    block: chain.get_block(i),
                    prev_block_with_changes: None,
                },
            };
            store_update.set_delta(shard_uid, &delta);
        }
        store_update.commit().unwrap();

        let flat_storage_manager = FlatStorageManager::new(store.clone());
        flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
        let flat_storage = flat_storage_manager.get_flat_storage_for_shard(shard_uid).unwrap();

        // Check `BlockNotSupported` errors which are fine to occur during regular block processing.
        // First, check that flat head can be moved to block 1.
        let flat_head_hash = chain.get_block_hash(1);
        assert_eq!(flat_storage.update_flat_head_impl(&flat_head_hash, true), Ok(()));
        // Check that attempt to move flat head to block 2 results in error because it lays in unreachable fork.
        let fork_block_hash = chain.get_block_hash(2);
        assert_eq!(
            flat_storage.update_flat_head_impl(&fork_block_hash, true),
            Err(FlatStorageError::BlockNotSupported((flat_head_hash, fork_block_hash)))
        );
        // Check that attempt to move flat head to block 0 results in error because it is an unreachable parent.
        let parent_block_hash = chain.get_block_hash(0);
        assert_eq!(
            flat_storage.update_flat_head_impl(&parent_block_hash, true),
            Err(FlatStorageError::BlockNotSupported((flat_head_hash, parent_block_hash)))
        );
        // Check that attempt to move flat head to non-existent block results in the same error.
        let not_existing_hash = hash(&[1, 2, 3]);
        assert_eq!(
            flat_storage.update_flat_head_impl(&not_existing_hash, true),
            Err(FlatStorageError::BlockNotSupported((flat_head_hash, not_existing_hash)))
        );
        // Corrupt DB state for block 3 and try moving flat head to it.
        // Should result in `StorageInternalError` indicating that flat storage is broken.
        let mut store_update = store.store_update();
        store_update.remove_delta(shard_uid, chain.get_block_hash(3));
        store_update.commit().unwrap();
        assert_matches!(
            flat_storage.update_flat_head_impl(&chain.get_block_hash(3), true),
            Err(FlatStorageError::StorageInternalError(_))
        );
    }

    #[test]
    /// Builds a chain with occasional forks.
    /// Checks that request to update flat head fail and are handled gracefully.
    fn flat_storage_fork() {
        init_test_logger();
        // Create a chain with two forks. Set flat head to be at block 0.
        let num_blocks = 10 as BlockHeight;

        // Build a chain that looks lke this:
        //     2     5     8
        //    /     /     /
        // 0-1---3-4---6-7---9
        // Note that forks [0,1,2], [3,4,5] and [6,7,8] form triples of consecutive blocks.
        let chain = MockChain::build((0..num_blocks).collect(), |i| {
            if i == 0 {
                None
            } else if i % 3 == 0 {
                Some(i - 2)
            } else {
                Some(i - 1)
            }
        });
        let shard_uid = ShardUId::single_shard();
        let store = create_test_store().flat_store();
        let mut store_update = store.store_update();
        store_update.set_flat_storage_status(
            shard_uid,
            FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head: chain.get_block(0) }),
        );
        for i in 1..num_blocks {
            let delta = FlatStateDelta {
                changes: FlatStateChanges::default(),
                metadata: FlatStateDeltaMetadata {
                    block: chain.get_block(i),
                    prev_block_with_changes: None,
                },
            };
            store_update.set_delta(shard_uid, &delta);
        }
        store_update.commit().unwrap();

        let flat_storage_manager = FlatStorageManager::new(store);
        flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
        let flat_storage = flat_storage_manager.get_flat_storage_for_shard(shard_uid).unwrap();

        // Simulates an actual sequence of calls to `update_flat_head()` in the
        // presence of forks.
        assert_eq!(flat_storage.update_flat_head_impl(&chain.get_block_hash(0), false), Ok(()));
        assert_eq!(flat_storage.update_flat_head_impl(&chain.get_block_hash(3), false), Ok(()));
        assert_matches!(
            flat_storage.update_flat_head_impl(&chain.get_block_hash(0), false),
            Err(FlatStorageError::BlockNotSupported(_))
        );
        assert_eq!(flat_storage.update_flat_head_impl(&chain.get_block_hash(6), false), Ok(()));
        assert_matches!(
            flat_storage.update_flat_head_impl(&chain.get_block_hash(0), false),
            Err(FlatStorageError::BlockNotSupported(_))
        );
    }

    #[test]
    fn skipped_heights() {
        // Create a linear chain where some heights are skipped.
        let chain = MockChain::linear_chain_with_skips(5);
        let shard_uid = ShardUId::single_shard();
        let store = create_test_store().flat_store();
        let mut store_update = store.store_update();
        store_update.set_flat_storage_status(
            shard_uid,
            FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head: chain.get_block(0) }),
        );
        for i in 1..5 {
            let delta = FlatStateDelta {
                changes: FlatStateChanges::default(),
                metadata: FlatStateDeltaMetadata {
                    block: chain.get_block(i * 2),
                    prev_block_with_changes: None,
                },
            };
            store_update.set_delta(shard_uid, &delta);
        }
        store_update.commit().unwrap();

        // Check that flat storage state is created correctly for chain which has skipped heights.
        let flat_storage_manager = FlatStorageManager::new(store);
        flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
        let flat_storage = flat_storage_manager.get_flat_storage_for_shard(shard_uid).unwrap();

        // Check that flat head can be moved to block 8.
        let flat_head_hash = chain.get_block_hash(8);
        assert_eq!(flat_storage.update_flat_head_impl(&flat_head_hash, false), Ok(()));
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
        let store = create_test_store().flat_store();
        let flat_store_adapter = store.flat_store();
        let mut store_update = store.store_update();
        store_update.set_flat_storage_status(
            shard_uid,
            FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head: chain.get_block(0) }),
        );
        store_update.set(shard_uid, vec![1], Some(FlatStateValue::value_ref(&[0])));
        for i in 1..10 {
            let delta = FlatStateDelta {
                changes: FlatStateChanges::from([(
                    vec![1],
                    Some(FlatStateValue::value_ref(&[i as u8])),
                )]),
                metadata: FlatStateDeltaMetadata {
                    block: chain.get_block(i),
                    prev_block_with_changes: None,
                },
            };
            store_update.set_delta(shard_uid, &delta);
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
                metadata: FlatStateDeltaMetadata {
                    block: chain.get_block_info(&hash),
                    prev_block_with_changes: None,
                },
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
        assert_matches!(store.get_delta(shard_uid, chain.get_block_hash(5)).unwrap(), Some(_));
        assert_matches!(store.get_delta(shard_uid, chain.get_block_hash(10)).unwrap(), Some(_));

        // 5. Move the flat head to block 5, verify that chunk_view0 still returns the same values
        // and chunk_view1 returns an error. Also check that DBCol::FlatState is updated correctly
        flat_storage.update_flat_head_impl(&chain.get_block_hash(5), true).unwrap();
        assert_eq!(
            flat_store_adapter.get(shard_uid, &[1]).unwrap(),
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
        assert_matches!(store.get_delta(shard_uid, chain.get_block_hash(5)).unwrap(), None);
        assert_matches!(store.get_delta(shard_uid, chain.get_block_hash(10)).unwrap(), Some(_));

        // 6. Move the flat head to block 10, verify that chunk_view0 still returns the same values
        //    Also checks that DBCol::FlatState is updated correctly.
        flat_storage.update_flat_head_impl(&chain.get_block_hash(10), true).unwrap();
        let blocks = flat_storage.get_blocks_to_head(&chain.get_block_hash(10)).unwrap();
        assert_eq!(blocks.len(), 0);
        assert_eq!(flat_store_adapter.get(shard_uid, &[1]).unwrap(), None);
        assert_eq!(
            flat_store_adapter.get(shard_uid, &[2]).unwrap(),
            Some(FlatStateValue::value_ref(&[1]))
        );
        assert_eq!(chunk_view0.get_value(&[1]).unwrap(), None);
        assert_eq!(chunk_view0.get_value(&[2]).unwrap(), Some(FlatStateValue::value_ref(&[1])));
        assert_matches!(store.get_delta(shard_uid, chain.get_block_hash(10)).unwrap(), None);
    }

    #[test]
    fn flat_storage_with_hops() {
        init_test_logger();
        // 1. Create a chain with no forks. Set flat head to be at block 0.
        let num_blocks = 15;
        let chain = MockChain::linear_chain(num_blocks);
        let shard_uid = ShardUId::single_shard();
        let store = create_test_store().flat_store();
        let mut store_update = store.store_update();
        store_update.set_flat_storage_status(
            shard_uid,
            FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head: chain.get_block(0) }),
        );
        store_update.set(shard_uid, vec![1], Some(FlatStateValue::value_ref(&[0])));
        store_update.commit().unwrap();

        for i in 1..num_blocks as BlockHeight {
            let mut store_update = store.store_update();
            let changes = if i % 3 == 0 {
                // Add a change.
                FlatStateChanges::from([(vec![1], Some(FlatStateValue::value_ref(&[i as u8])))])
            } else {
                // No changes.
                FlatStateChanges::from([])
            };

            // Simulates `Chain::save_flat_state_changes()`.
            let prev_block_with_changes = if changes.0.is_empty() {
                store
                    .get_prev_block_with_changes(
                        shard_uid,
                        chain.get_block(i).hash,
                        chain.get_block(i).prev_hash,
                    )
                    .unwrap()
            } else {
                None
            };
            let delta = FlatStateDelta {
                changes,
                metadata: FlatStateDeltaMetadata {
                    block: chain.get_block(i),
                    prev_block_with_changes,
                },
            };
            tracing::info!(?i, ?delta);
            store_update.set_delta(shard_uid, &delta);
            store_update.commit().unwrap();
        }

        let flat_storage_manager = FlatStorageManager::new(store.clone());
        flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
        let flat_storage = flat_storage_manager.get_flat_storage_for_shard(shard_uid).unwrap();

        // 2. Check that the chunk_view at block i reads the value of key &[1] as &[round_down_to_a_multiple_of_3(i)]
        for i in 0..num_blocks as BlockHeight {
            let block_hash = chain.get_block_hash(i);
            let blocks = flat_storage.get_blocks_to_head(&block_hash).unwrap();
            let chunk_view = flat_storage_manager.chunk_view(shard_uid, block_hash).unwrap();
            let value = chunk_view.get_value(&[1]).unwrap();
            tracing::info!(?i, ?block_hash, ?value, blocks_to_head = ?blocks);
            assert_eq!(value, Some(FlatStateValue::value_ref(&[((i / 3) * 3) as u8])));

            // Don't check the first block because it may be a block with no changes.
            for i in 1..blocks.len() {
                let block_hash = blocks[i];
                let delta = store.get_delta(shard_uid, block_hash).unwrap().unwrap();
                assert!(
                    !delta.0.is_empty(),
                    "i: {i}, block_hash: {block_hash:?}, delta: {delta:?}"
                );
            }
        }

        // 3. Simulate moving head forward with a delay of two from the tip.
        // flat head is chosen to keep 2 blocks between the suggested head and the chosen head,
        // resulting in <=4 blocks on the way from the tip to the chosen head.
        for i in 2..num_blocks as BlockHeight {
            let final_block_hash = chain.get_block_hash(i - 2);
            flat_storage.update_flat_head_impl(&final_block_hash, false).unwrap();

            let block_hash = chain.get_block_hash(i);
            let blocks = flat_storage.get_blocks_to_head(&block_hash).unwrap();

            assert!(
                blocks.len() <= 2 + FlatStorageInner::BLOCKS_WITH_CHANGES_FLAT_HEAD_GAP as usize
            );
        }
    }

    #[test]
    /// Move flat storage to an exact height when flat storage has no changes.
    fn flat_storage_with_no_changes() {
        init_test_logger();
        let num_blocks = 10;
        let chain = MockChain::linear_chain(num_blocks);
        let shard_uid = ShardUId::single_shard();
        let store = create_test_store().flat_store();
        let mut store_update = store.store_update();
        store_update.set_flat_storage_status(
            shard_uid,
            FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head: chain.get_block(0) }),
        );
        store_update.set(shard_uid, vec![1], Some(FlatStateValue::value_ref(&[0])));
        store_update.commit().unwrap();

        for i in 1..num_blocks as BlockHeight {
            let mut store_update = store.store_update();
            // No changes.
            let changes = FlatStateChanges::default();
            // Simulates `Chain::save_flat_state_changes()`.
            let prev_block_with_changes = store
                .get_prev_block_with_changes(
                    shard_uid,
                    chain.get_block(i).hash,
                    chain.get_block(i).prev_hash,
                )
                .unwrap();
            let delta = FlatStateDelta {
                changes,
                metadata: FlatStateDeltaMetadata {
                    block: chain.get_block(i),
                    prev_block_with_changes,
                },
            };
            tracing::info!(?i, ?delta);
            store_update.set_delta(shard_uid, &delta);
            store_update.commit().unwrap();
        }

        let flat_storage_manager = FlatStorageManager::new(store);
        flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
        let flat_storage = flat_storage_manager.get_flat_storage_for_shard(shard_uid).unwrap();

        let hashes = (0..num_blocks as BlockHeight)
            .map(|height| (chain.get_block_hash(height), height))
            .collect::<HashMap<CryptoHash, BlockHeight>>();

        let block_hash = chain.get_block_hash((num_blocks - 1) as BlockHeight);
        flat_storage.update_flat_head_impl(&block_hash, true).unwrap();

        let flat_head_hash = flat_storage.get_head_hash();
        let flat_head_height = hashes.get(&flat_head_hash).unwrap();

        assert_eq!(*flat_head_height, (num_blocks - 1) as BlockHeight);
    }

    #[test]
    fn flat_storage_with_hops_random() {
        init_test_logger();
        // 1. Create a long chain with no forks. Set flat head to be at block 0.
        let num_blocks = 1000;
        let mut rng = thread_rng();
        let chain = MockChain::linear_chain(num_blocks);
        let shard_uid = ShardUId::single_shard();
        let store = create_test_store().flat_store();
        let mut store_update = store.store_update();
        store_update.set_flat_storage_status(
            shard_uid,
            FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head: chain.get_block(0) }),
        );
        store_update.set(shard_uid, vec![1], Some(FlatStateValue::value_ref(&[0])));
        store_update.commit().unwrap();

        for i in 1..num_blocks as BlockHeight {
            let mut store_update = store.store_update();
            let changes = if rng.gen_bool(0.3) {
                // Add a change.
                FlatStateChanges::from([(vec![1], Some(FlatStateValue::value_ref(&[i as u8])))])
            } else {
                // No changes.
                FlatStateChanges::default()
            };

            // Simulates `Chain::save_flat_state_changes()`.
            let prev_block_with_changes = if changes.0.is_empty() {
                store
                    .get_prev_block_with_changes(
                        shard_uid,
                        chain.get_block(i).hash,
                        chain.get_block(i).prev_hash,
                    )
                    .unwrap()
            } else {
                None
            };
            let delta = FlatStateDelta {
                changes,
                metadata: FlatStateDeltaMetadata {
                    block: chain.get_block(i),
                    prev_block_with_changes,
                },
            };
            tracing::info!(?i, ?delta);
            store_update.set_delta(shard_uid, &delta);
            store_update.commit().unwrap();
        }

        let flat_storage_manager = FlatStorageManager::new(store.clone());
        flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
        let flat_storage = flat_storage_manager.get_flat_storage_for_shard(shard_uid).unwrap();

        let hashes = (0..num_blocks as BlockHeight)
            .map(|height| (chain.get_block_hash(height), height))
            .collect::<HashMap<CryptoHash, BlockHeight>>();

        // 2. Simulate moving head, with the suggested head lagging by 2 blocks behind the tip.
        let mut max_lag = None;
        for i in 2..num_blocks as BlockHeight {
            let final_block_hash = chain.get_block_hash(i - 2);
            flat_storage.update_flat_head_impl(&final_block_hash, false).unwrap();

            let block_hash = chain.get_block_hash(i);
            let blocks = flat_storage.get_blocks_to_head(&block_hash).unwrap();
            assert!(
                blocks.len() <= 2 + FlatStorageInner::BLOCKS_WITH_CHANGES_FLAT_HEAD_GAP as usize
            );
            // Don't check the first block because it may be a block with no changes.
            for i in 1..blocks.len() {
                let block_hash = blocks[i];
                let delta = store.get_delta(shard_uid, block_hash).unwrap().unwrap();
                assert!(
                    !delta.0.is_empty(),
                    "i: {i}, block_hash: {block_hash:?}, delta: {delta:?}"
                );
            }

            let flat_head_hash = flat_storage.get_head_hash();
            let flat_head_height = hashes.get(&flat_head_hash).unwrap();

            let flat_head_lag = i - flat_head_height;
            let delta = store.get_delta(shard_uid, block_hash).unwrap().unwrap();
            let has_changes = !delta.0.is_empty();
            tracing::info!(?i, has_changes, ?flat_head_lag);
            max_lag = max_lag.max(Some(flat_head_lag));
        }
        tracing::info!(?max_lag);
    }

    #[test]
    fn test_new_flat_head() {
        init_test_logger();

        let shard_uid = ShardUId::single_shard();

        // Case 1. Each block has flat state changes.
        {
            tracing::info!("Case 1");
            let num_blocks = 10;
            let chain = MockChain::linear_chain(num_blocks);
            let store = create_test_store().flat_store();
            let mut store_update = store.store_update();
            store_update.set_flat_storage_status(
                shard_uid,
                FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head: chain.get_block(0) }),
            );
            store_update.set(shard_uid, vec![1], Some(FlatStateValue::value_ref(&[0])));
            store_update.commit().unwrap();

            for i in 1..num_blocks as BlockHeight {
                let mut store_update = store.store_update();
                // Add a change.
                let changes = FlatStateChanges::from([(
                    vec![1],
                    Some(FlatStateValue::value_ref(&[i as u8])),
                )]);
                let delta = FlatStateDelta {
                    changes,
                    metadata: FlatStateDeltaMetadata {
                        block: chain.get_block(i),
                        prev_block_with_changes: None,
                    },
                };
                tracing::info!(?i, ?delta);
                store_update.set_delta(shard_uid, &delta);
                store_update.commit().unwrap();
            }

            let flat_storage_manager = FlatStorageManager::new(store);
            flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
            let flat_storage = flat_storage_manager.get_flat_storage_for_shard(shard_uid).unwrap();
            let guard = flat_storage.0.write().expect(crate::flat::POISONED_LOCK_ERR);

            for i in 0..num_blocks as BlockHeight {
                let block_hash = chain.get_block_hash(i);
                let new_head = guard.test_get_new_flat_head(block_hash, false).unwrap();
                tracing::info!(i, ?block_hash, ?new_head);
                if i == 0 {
                    assert_eq!(new_head, chain.get_block_hash(0));
                } else {
                    assert_eq!(new_head, chain.get_block_hash(i - 1));
                }
            }
        }

        // Case 2. Even-numbered blocks have flat changes
        {
            tracing::info!("Case 2");
            let num_blocks = 20;
            let chain = MockChain::linear_chain(num_blocks);
            let store = create_test_store().flat_store();
            let mut store_update = store.store_update();
            store_update.set_flat_storage_status(
                shard_uid,
                FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head: chain.get_block(0) }),
            );
            store_update.set(shard_uid, vec![1], Some(FlatStateValue::value_ref(&[0])));
            store_update.commit().unwrap();

            for i in 1..num_blocks as BlockHeight {
                let mut store_update = store.store_update();
                let (changes, prev_block_with_changes) = if i % 2 == 0 {
                    // Add a change.
                    (
                        FlatStateChanges::from([(
                            vec![1],
                            Some(FlatStateValue::value_ref(&[i as u8])),
                        )]),
                        None,
                    )
                } else {
                    // No changes.
                    (
                        FlatStateChanges::default(),
                        Some(BlockWithChangesInfo {
                            hash: chain.get_block_hash(i - 1),
                            height: i - 1,
                        }),
                    )
                };

                let delta = FlatStateDelta {
                    changes,
                    metadata: FlatStateDeltaMetadata {
                        block: chain.get_block(i),
                        prev_block_with_changes,
                    },
                };
                tracing::info!(?i, ?delta);
                store_update.set_delta(shard_uid, &delta);
                store_update.commit().unwrap();
            }

            let flat_storage_manager = FlatStorageManager::new(store);
            flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
            let flat_storage = flat_storage_manager.get_flat_storage_for_shard(shard_uid).unwrap();
            let guard = flat_storage.0.write().expect(crate::flat::POISONED_LOCK_ERR);

            // A chain looks like this:
            // X-O-X-O-X-O-...
            // where X is a block with flat state changes and O is a block without flat state changes.
            for i in 0..num_blocks as BlockHeight {
                let new_head = guard.get_new_flat_head(chain.get_block_hash(i), false).unwrap();
                if i <= 3 {
                    assert_eq!(new_head, chain.get_block_hash(0));
                } else {
                    // if i is odd, then it's pointing at an O, and the head is expected to be 3 blocks ago.
                    //               i
                    //               |
                    //               v
                    // X-O-X-O-X-O-X-O
                    //         ^
                    //         |
                    //        new_head
                    //
                    // if i is even, then it's pointing at an X, and the head is expected to be the previous X:
                    //             i
                    //             |
                    //             v
                    // X-O-X-O-X-O-X-O
                    //         ^
                    //         |
                    //        new_head

                    // Both of these cases can be computed as rounding (i-2) down to a multiple of 2.
                    assert_eq!(new_head, chain.get_block_hash(((i - 2) / 2) * 2));
                }
            }
        }

        // Case 3. Triplets of blocks: HasChanges, NoChanges, HasChanges.
        {
            tracing::info!("Case 3");
            let num_blocks = 20;
            let chain = MockChain::linear_chain(num_blocks);
            let store = create_test_store().flat_store();
            let mut store_update = store.store_update();
            store_update.set_flat_storage_status(
                shard_uid,
                FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head: chain.get_block(0) }),
            );
            store_update.set(shard_uid, vec![1], Some(FlatStateValue::value_ref(&[0])));
            store_update.commit().unwrap();

            for i in 1..num_blocks as BlockHeight {
                let mut store_update = store.store_update();
                let (changes, prev_block_with_changes) = if i % 3 == 1 {
                    // No changes.
                    (
                        FlatStateChanges::default(),
                        Some(BlockWithChangesInfo {
                            hash: chain.get_block_hash(i - 1),
                            height: i - 1,
                        }),
                    )
                } else {
                    // Add a change.
                    (
                        FlatStateChanges::from([(
                            vec![1],
                            Some(FlatStateValue::value_ref(&[i as u8])),
                        )]),
                        None,
                    )
                };

                let delta = FlatStateDelta {
                    changes,
                    metadata: FlatStateDeltaMetadata {
                        block: chain.get_block(i),
                        prev_block_with_changes,
                    },
                };
                tracing::info!(?i, ?delta);
                store_update.set_delta(shard_uid, &delta);
                store_update.commit().unwrap();
            }

            let flat_storage_manager = FlatStorageManager::new(store);
            flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
            let flat_storage = flat_storage_manager.get_flat_storage_for_shard(shard_uid).unwrap();
            let guard = flat_storage.0.write().expect(crate::flat::POISONED_LOCK_ERR);

            for i in 0..num_blocks as BlockHeight {
                let new_head = guard.get_new_flat_head(chain.get_block_hash(i), false).unwrap();
                // if i%3 == 0
                //             i
                //             |
                //             v
                // X-O-X-X-O-X-X-O-X
                //           ^
                //           |
                //        new_head
                //
                // if i%3 == 1
                //               i
                //               |
                //               v
                // X-O-X-X-O-X-X-O-X
                //           ^
                //           |
                //        new_head
                //
                // if i%3 == 2
                //                 i
                //                 |
                //                 v
                // X-O-X-X-O-X-X-O-X
                //             ^
                //             |
                //          new_head
                if i <= 2 {
                    assert_eq!(new_head, chain.get_block_hash(0));
                } else if i % 3 == 0 {
                    assert_eq!(new_head, chain.get_block_hash(i - 1));
                } else {
                    assert_eq!(new_head, chain.get_block_hash(i - 2));
                }
            }
        }
    }
}

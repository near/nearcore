use crate::Mode;
use crate::ShardTries;
use crate::StoreConfig;
use crate::adapter::StoreAdapter;
use crate::adapter::trie_store::TrieStoreAdapter;
use crate::flat::{FlatStorageManager, FlatStorageStatus};
use crate::{DBCol, NodeStorage, checkpoint_hot_storage_and_cleanup_columns, metrics};
use near_primitives::block::Block;
use near_primitives::errors::EpochError;
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state::PartialState;
use near_primitives::state_part::PartId;
use near_primitives::types::ShardIndex;
use near_primitives::types::StateRoot;
use std::error::Error;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::Trie;
use super::TrieCachingStorage;

#[derive(Debug, PartialEq, Eq)]
pub enum SnapshotError {
    // The requested hash and the snapshot hash don't match.
    IncorrectSnapshotRequested(CryptoHash, CryptoHash),
    // Snapshot doesn't exist at all.
    SnapshotNotFound(CryptoHash),
    // Lock for snapshot acquired by another process.
    // Most likely the StateSnapshotActor is creating a snapshot or doing compaction.
    LockWouldBlock,
    // Any other unexpected error
    Other(String),
}

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotError::IncorrectSnapshotRequested(requested, available) => write!(
                f,
                "Wrong snapshot hash. Requested: {:?}, Available: {:?}",
                requested, available
            ),
            SnapshotError::SnapshotNotFound(hash) => {
                write!(f, "No state snapshot available. Requested: {:?}", hash)
            }
            SnapshotError::LockWouldBlock => {
                write!(f, "Accessing state snapshot would block. Retry in a few seconds.")
            }
            SnapshotError::Other(err_msg) => write!(f, "{}", err_msg),
        }
    }
}

impl Error for SnapshotError {}

impl From<SnapshotError> for StorageError {
    fn from(err: SnapshotError) -> Self {
        StorageError::StorageInconsistentState(err.to_string())
    }
}

/// Snapshot of the state at the epoch boundary.
pub struct StateSnapshot {
    /// The state snapshot represents the state including changes of the next block of this block.
    prev_block_hash: CryptoHash,
    /// Read-only store.
    store: TrieStoreAdapter,
    /// Access to flat storage in that store.
    flat_storage_manager: FlatStorageManager,
    /// Shards which were successfully included in the snapshot.
    included_shard_uids: Vec<ShardUId>,
}

impl StateSnapshot {
    /// Creates an object and also creates flat storage for the given shards.
    pub fn new(
        store: TrieStoreAdapter,
        prev_block_hash: CryptoHash,
        flat_storage_manager: FlatStorageManager,
        shard_indexes_and_uids: &[(ShardIndex, ShardUId)],
        block: Option<&Block>,
    ) -> Self {
        tracing::debug!(target: "state_snapshot", ?shard_indexes_and_uids, ?prev_block_hash, "new StateSnapshot");
        let mut included_shard_uids = vec![];
        for &(shard_index, shard_uid) in shard_indexes_and_uids {
            if let Err(err) = flat_storage_manager.mark_ready_and_create_flat_storage(shard_uid) {
                tracing::warn!(target: "state_snapshot", ?err, ?shard_uid, "Failed to create a flat storage for snapshot shard");
                continue;
            }
            if let Some(block) = block {
                let flat_storage =
                    flat_storage_manager.get_flat_storage_for_shard(shard_uid).unwrap();
                let current_flat_head = flat_storage.get_head_hash();
                tracing::debug!(target: "state_snapshot", ?shard_uid, ?current_flat_head, block_hash = ?block.header().hash(), block_height = block.header().height(), "Moving FlatStorage head of the snapshot");
                let _timer = metrics::MOVE_STATE_SNAPSHOT_FLAT_HEAD_ELAPSED
                    .with_label_values(&[&shard_uid.shard_id.to_string()])
                    .start_timer();
                if let Some(chunk) = block.chunks().get(shard_index) {
                    // Flat state snapshot needs to be at a height that lets it
                    // replay the last chunk of the shard.
                    let desired_flat_head = chunk.prev_block_hash();
                    match flat_storage.update_flat_head(desired_flat_head) {
                        Ok(_) => {
                            tracing::debug!(target: "state_snapshot", ?shard_uid, ?current_flat_head, ?desired_flat_head, "Successfully moved FlatStorage head of the snapshot");
                            included_shard_uids.push(shard_uid);
                        }
                        Err(err) => {
                            tracing::error!(target: "state_snapshot", ?shard_uid, ?err, ?current_flat_head, ?desired_flat_head, "Failed to move FlatStorage head of the snapshot");
                        }
                    }
                } else {
                    tracing::error!(target: "state_snapshot", ?shard_uid, current_flat_head = ?flat_storage.get_head_hash(), ?prev_block_hash, "Failed to move FlatStorage head of the snapshot, no chunk");
                }
            }
        }
        Self { prev_block_hash, store, flat_storage_manager, included_shard_uids }
    }

    /// Returns the UIds for the shards included in the snapshot.
    pub fn get_included_shard_uids(&self) -> Vec<ShardUId> {
        self.included_shard_uids.clone()
    }

    /// Returns status of a shard of a flat storage in the state snapshot.
    pub fn get_flat_storage_status(&self, shard_uid: ShardUId) -> FlatStorageStatus {
        self.flat_storage_manager.get_flat_storage_status(shard_uid)
    }
}

/// Information needed to make a state snapshot.
#[derive(Debug)]
pub enum StateSnapshotConfig {
    Disabled,
    Enabled { state_snapshots_dir: PathBuf },
}

pub fn state_snapshots_dir(
    home_dir: impl AsRef<Path>,
    hot_store_path: impl AsRef<Path>,
    state_snapshots_subdir: impl AsRef<Path>,
) -> PathBuf {
    home_dir.as_ref().join(hot_store_path).join(state_snapshots_subdir)
}

impl StateSnapshotConfig {
    pub fn enabled(
        home_dir: impl AsRef<Path>,
        hot_store_path: impl AsRef<Path>,
        state_snapshots_subdir: impl AsRef<Path>,
    ) -> Self {
        // Assumptions:
        // * RocksDB checkpoints are taken instantly and for free, because the filesystem supports hard links.
        // * The best place for checkpoints is within the `hot_store_path`, because that directory is often a separate disk.
        Self::Enabled {
            state_snapshots_dir: state_snapshots_dir(
                home_dir,
                hot_store_path,
                state_snapshots_subdir,
            ),
        }
    }

    pub fn state_snapshots_dir(&self) -> Option<&Path> {
        match self {
            StateSnapshotConfig::Disabled => None,
            StateSnapshotConfig::Enabled { state_snapshots_dir } => Some(state_snapshots_dir),
        }
    }
}

pub const STATE_SNAPSHOT_COLUMNS: &[DBCol] = &[
    // Keep DbVersion and BlockMisc, otherwise you'll not be able to open the state snapshot as a Store.
    DBCol::DbVersion,
    DBCol::BlockMisc,
    // Flat storage columns.
    DBCol::FlatState,
    DBCol::FlatStateChanges,
    DBCol::FlatStateDeltaMetadata,
    DBCol::FlatStorageStatus,
];

type ShardIndexesAndUIds = Vec<(ShardIndex, ShardUId)>;

impl ShardTries {
    pub fn get_trie_nodes_for_part_from_snapshot(
        &self,
        shard_uid: ShardUId,
        state_root: &StateRoot,
        block_hash: &CryptoHash,
        part_id: PartId,
        path_boundary_nodes: PartialState,
        nibbles_begin: Vec<u8>,
        nibbles_end: Vec<u8>,
        state_trie: Trie,
    ) -> Result<PartialState, StorageError> {
        let guard = self.state_snapshot().try_read().ok_or(SnapshotError::LockWouldBlock)?;
        let data = guard.as_ref().ok_or(SnapshotError::SnapshotNotFound(*block_hash))?;
        if &data.prev_block_hash != block_hash {
            return Err(SnapshotError::IncorrectSnapshotRequested(
                *block_hash,
                data.prev_block_hash,
            )
            .into());
        };
        let cache = self
            .get_trie_cache_for(shard_uid, true)
            .expect("trie cache should be enabled for view calls");
        let storage =
            Arc::new(TrieCachingStorage::new(data.store.clone(), cache, shard_uid, true, None));
        let flat_storage_chunk_view = data.flat_storage_manager.chunk_view(shard_uid, *block_hash);

        let snapshot_trie = Trie::new(storage, *state_root, flat_storage_chunk_view);
        snapshot_trie.get_trie_nodes_for_part_with_flat_storage(
            part_id,
            path_boundary_nodes,
            nibbles_begin,
            nibbles_end,
            &state_trie,
        )
    }

    /// Makes a snapshot of the current state of the DB, if one is not already available.
    /// If a new snapshot is created, returns the ids of the included shards.
    pub fn create_state_snapshot(
        &self,
        prev_block_hash: CryptoHash,
        shard_indexes_and_uids: &[(ShardIndex, ShardUId)],
        block: &Block,
    ) -> Result<Option<Vec<ShardUId>>, anyhow::Error> {
        metrics::HAS_STATE_SNAPSHOT.set(0);
        // The function returns an `anyhow::Error`, because no special handling of errors is done yet. The errors are logged and ignored.
        let _span =
            tracing::info_span!(target: "state_snapshot", "create_state_snapshot", ?prev_block_hash)
                .entered();
        let _timer = metrics::CREATE_STATE_SNAPSHOT_ELAPSED.start_timer();

        // Checking if state snapshots are enabled is already done on actor level, hence the warning
        let Some(state_snapshots_dir) = self.state_snapshots_dir() else {
            tracing::warn!(target: "state_snapshot", "State snapshots are disabled");
            return Ok(None);
        };

        // `write()` lock is held for the whole duration of this function.
        let mut state_snapshot_lock = self.state_snapshot().write();
        let db_snapshot_hash = self.store().get_state_snapshot_hash();
        if let Some(state_snapshot) = &*state_snapshot_lock {
            // only return Ok() when the hash stored in STATE_SNAPSHOT_KEY and in state_snapshot_lock and prev_block_hash are the same
            if db_snapshot_hash.is_ok_and(|hash| hash == prev_block_hash)
                && state_snapshot.prev_block_hash == prev_block_hash
            {
                tracing::warn!(target: "state_snapshot", ?prev_block_hash, "Requested a state snapshot but that is already available");
                return Ok(None);
            }
            tracing::error!(target: "state_snapshot", ?prev_block_hash, ?state_snapshot.prev_block_hash, "Requested a state snapshot but that is already available with a different hash");
        }

        let storage = checkpoint_hot_storage_and_cleanup_columns(
            &self.store().store(),
            &Self::get_state_snapshot_base_dir(&prev_block_hash, state_snapshots_dir),
            // TODO: Cleanup Changes and DeltaMetadata to avoid extra memory usage.
            // Can't be cleaned up now because these columns are needed to `update_flat_head()`.
            Some(STATE_SNAPSHOT_COLUMNS),
        )?;
        let store = storage.get_hot_store().trie_store();
        // It is fine to create a separate FlatStorageManager, because
        // it is used only for reading flat storage in the snapshot a
        // doesn't introduce memory overhead.
        let flat_storage_manager = FlatStorageManager::new(store.flat_store());
        *state_snapshot_lock = Some(StateSnapshot::new(
            store,
            prev_block_hash,
            flat_storage_manager,
            shard_indexes_and_uids,
            Some(block),
        ));

        // this will set the new hash for state snapshot in rocksdb. will retry until success.
        for _ in 0..3 {
            let mut store_update = self.store_update();
            store_update.set_state_snapshot_hash(Some(prev_block_hash));
            match store_update.commit() {
                Ok(_) => {}
                Err(err) => {
                    tracing::error!(target: "state_snapshot", ?err, "Failed to set the new state snapshot for BlockMisc::STATE_SNAPSHOT_KEY in rocksdb");
                }
            }
        }

        metrics::HAS_STATE_SNAPSHOT.set(1);
        tracing::info!(target: "state_snapshot", ?prev_block_hash, "Made a checkpoint");
        Ok(Some(state_snapshot_lock.as_ref().unwrap().get_included_shard_uids()))
    }

    /// Deletes all snapshots and unset the STATE_SNAPSHOT_KEY.
    pub fn delete_state_snapshot(&self) {
        let _span =
            tracing::info_span!(target: "state_snapshot", "delete_state_snapshot").entered();
        let _timer = metrics::DELETE_STATE_SNAPSHOT_ELAPSED.start_timer();

        // Checking if state snapshots are enabled is already done on actor level, hence the warning
        let Some(state_snapshots_dir) = self.state_snapshots_dir() else {
            tracing::warn!(target: "state_snapshot", "State snapshots are disabled");
            return;
        };

        // get snapshot_hash after acquiring write lock
        let mut state_snapshot_lock = self.state_snapshot().write();
        if state_snapshot_lock.is_some() {
            // Drop Store before deleting the underlying data.
            *state_snapshot_lock = None;
        }

        // This will delete all existing snapshots from file system. Will retry 3 times
        for _ in 0..3 {
            match self.delete_all_state_snapshots(state_snapshots_dir) {
                Ok(_) => break,
                Err(err) => {
                    tracing::error!(target: "state_snapshot", ?err, "Failed to delete the old state snapshot from file system or from rocksdb")
                }
            }
        }

        // this will delete the STATE_SNAPSHOT_KEY-value pair from db. Will retry 3 times
        for _ in 0..3 {
            let mut store_update = self.store_update();
            store_update.set_state_snapshot_hash(None);
            match store_update.commit() {
                Ok(_) => break,
                Err(err) => {
                    tracing::error!(target: "state_snapshot", ?err, "Failed to delete the old state snapshot for BlockMisc::STATE_SNAPSHOT_KEY in rocksdb")
                }
            }
        }

        metrics::HAS_STATE_SNAPSHOT.set(0);
    }

    /// Deletes all existing state snapshots in the parent directory
    fn delete_all_state_snapshots(&self, state_snapshots_dir: &Path) -> Result<(), io::Error> {
        let _span =
            tracing::info_span!(target: "state_snapshot", "delete_all_state_snapshots").entered();
        if state_snapshots_dir.exists() {
            std::fs::remove_dir_all(state_snapshots_dir)?
        }
        Ok(())
    }

    pub fn get_state_snapshot_base_dir(
        prev_block_hash: &CryptoHash,
        state_snapshots_dir: &Path,
    ) -> PathBuf {
        state_snapshots_dir.join(format!("{prev_block_hash}"))
    }

    /// Read RocksDB for the latest available snapshot hash, if available, open base_path+snapshot_hash for the state snapshot
    /// we don't deal with multiple snapshots here because we will deal with it whenever a new snapshot is created and saved to file system
    pub fn maybe_open_state_snapshot(
        &self,
        get_shard_indexes_and_uids_fn: impl FnOnce(
            CryptoHash,
        ) -> Result<ShardIndexesAndUIds, EpochError>,
    ) -> Result<(), anyhow::Error> {
        let _span =
            tracing::info_span!(target: "state_snapshot", "maybe_open_state_snapshot").entered();
        metrics::HAS_STATE_SNAPSHOT.set(0);
        let state_snapshots_dir = self
            .state_snapshots_dir()
            .ok_or_else(|| anyhow::anyhow!("State snapshots disabled"))?;

        // directly return error if no snapshot is found
        let snapshot_hash = self.store().get_state_snapshot_hash()?;

        let snapshot_path = Self::get_state_snapshot_base_dir(&snapshot_hash, &state_snapshots_dir);
        let parent_path = snapshot_path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("{snapshot_path:?} needs to have a parent dir"))?;
        tracing::debug!(target: "state_snapshot", ?snapshot_path, ?parent_path);

        let store_config = StoreConfig::default();

        let opener = NodeStorage::opener(&snapshot_path, &store_config, None);
        let storage = opener.open_in_mode(Mode::ReadOnly)?;
        let store = storage.get_hot_store().trie_store();
        let flat_storage_manager = FlatStorageManager::new(store.flat_store());

        let shard_indexes_and_uids = get_shard_indexes_and_uids_fn(snapshot_hash)?;
        let mut guard = self.state_snapshot().write();
        *guard = Some(StateSnapshot::new(
            store,
            snapshot_hash,
            flat_storage_manager,
            &shard_indexes_and_uids,
            None,
        ));
        metrics::HAS_STATE_SNAPSHOT.set(1);
        tracing::info!(target: "runtime", ?snapshot_hash, ?snapshot_path, "Detected and opened a state snapshot.");
        Ok(())
    }
}

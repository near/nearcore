use crate::config::StateSnapshotType;
use crate::db::STATE_SNAPSHOT_KEY;
use crate::flat::{FlatStorageManager, FlatStorageStatus};
use crate::Mode;
use crate::{checkpoint_hot_storage_and_cleanup_columns, metrics, DBCol, NodeStorage};
use crate::{option_to_not_found, ShardTries};
use crate::{Store, StoreConfig};
use near_primitives::block::Block;
use near_primitives::errors::EpochError;
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use std::error::Error;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::TryLockError;

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

impl<T> From<TryLockError<T>> for SnapshotError {
    fn from(err: TryLockError<T>) -> Self {
        match err {
            TryLockError::Poisoned(_) => SnapshotError::Other("Poisoned lock".to_string()),
            TryLockError::WouldBlock => SnapshotError::LockWouldBlock,
        }
    }
}

/// Snapshot of the state at the epoch boundary.
pub struct StateSnapshot {
    /// The state snapshot represents the state including changes of the next block of this block.
    prev_block_hash: CryptoHash,
    /// Read-only store.
    store: Store,
    /// Access to flat storage in that store.
    flat_storage_manager: FlatStorageManager,
}

impl StateSnapshot {
    /// Creates an object and also creates flat storage for the given shards.
    pub fn new(
        store: Store,
        prev_block_hash: CryptoHash,
        flat_storage_manager: FlatStorageManager,
        shard_uids: &[ShardUId],
        block: Option<&Block>,
    ) -> Self {
        tracing::debug!(target: "state_snapshot", ?shard_uids, ?prev_block_hash, "new StateSnapshot");
        for shard_uid in shard_uids {
            if let Err(err) = flat_storage_manager.create_flat_storage_for_shard(*shard_uid) {
                tracing::warn!(target: "state_snapshot", ?err, ?shard_uid, "Failed to create a flat storage for snapshot shard");
                continue;
            }
            if let Some(block) = block {
                let flat_storage =
                    flat_storage_manager.get_flat_storage_for_shard(*shard_uid).unwrap();
                let current_flat_head = flat_storage.get_head_hash();
                tracing::debug!(target: "state_snapshot", ?shard_uid, ?current_flat_head, block_hash = ?block.header().hash(), block_height = block.header().height(), "Moving FlatStorage head of the snapshot");
                let _timer = metrics::MOVE_STATE_SNAPSHOT_FLAT_HEAD_ELAPSED
                    .with_label_values(&[&shard_uid.shard_id.to_string()])
                    .start_timer();
                if let Some(chunk) = block.chunks().get(shard_uid.shard_id as usize) {
                    // Flat state snapshot needs to be at a height that lets it
                    // replay the last chunk of the shard.
                    let desired_flat_head = chunk.prev_block_hash();
                    match flat_storage.update_flat_head(desired_flat_head, true) {
                        Ok(_) => {
                            tracing::debug!(target: "state_snapshot", ?shard_uid, ?current_flat_head, ?desired_flat_head, "Successfully moved FlatStorage head of the snapshot");
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
        Self { prev_block_hash, store, flat_storage_manager }
    }

    /// Returns the UIds for the shards included in the snapshot.
    pub fn get_shard_uids(&self) -> Vec<ShardUId> {
        self.flat_storage_manager.get_shard_uids()
    }

    /// Returns status of a shard of a flat storage in the state snapshot.
    pub fn get_flat_storage_status(&self, shard_uid: ShardUId) -> FlatStorageStatus {
        self.flat_storage_manager.get_flat_storage_status(shard_uid)
    }
}

/// Information needed to make a state snapshot.
/// Note that it's possible to override the `enabled` config and force create snapshot for resharding.
#[derive(Default)]
pub struct StateSnapshotConfig {
    /// It's possible to override the `enabled` config and force create snapshot for resharding.
    pub state_snapshot_type: StateSnapshotType,
    pub home_dir: PathBuf,
    pub hot_store_path: PathBuf,
    pub state_snapshot_subdir: PathBuf,
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

impl ShardTries {
    pub fn get_state_snapshot(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<(Store, FlatStorageManager), SnapshotError> {
        // Taking this lock can last up to 10 seconds, if the snapshot happens to be re-created.
        let guard = self.state_snapshot().try_read()?;
        let data = guard.as_ref().ok_or(SnapshotError::SnapshotNotFound(*block_hash))?;
        if &data.prev_block_hash != block_hash {
            return Err(SnapshotError::IncorrectSnapshotRequested(
                *block_hash,
                data.prev_block_hash,
            ));
        }
        Ok((data.store.clone(), data.flat_storage_manager.clone()))
    }

    /// Makes a snapshot of the current state of the DB, if one is not already available.
    /// If a new snapshot is created, returns the ids of the included shards.
    pub fn create_state_snapshot(
        &self,
        prev_block_hash: CryptoHash,
        shard_uids: &[ShardUId],
        block: &Block,
    ) -> Result<Option<Vec<ShardUId>>, anyhow::Error> {
        metrics::HAS_STATE_SNAPSHOT.set(0);
        // The function returns an `anyhow::Error`, because no special handling of errors is done yet. The errors are logged and ignored.
        let _span =
            tracing::info_span!(target: "state_snapshot", "create_state_snapshot", ?prev_block_hash)
                .entered();
        let _timer = metrics::CREATE_STATE_SNAPSHOT_ELAPSED.start_timer();

        // `write()` lock is held for the whole duration of this function.
        let mut state_snapshot_lock = self.state_snapshot().write().unwrap();
        let db_snapshot_hash = self.get_state_snapshot_hash();
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

        let StateSnapshotConfig { home_dir, hot_store_path, state_snapshot_subdir, .. } =
            self.state_snapshot_config();
        let storage = checkpoint_hot_storage_and_cleanup_columns(
            &self.get_store(),
            &Self::get_state_snapshot_base_dir(
                &prev_block_hash,
                home_dir,
                hot_store_path,
                state_snapshot_subdir,
            ),
            // TODO: Cleanup Changes and DeltaMetadata to avoid extra memory usage.
            // Can't be cleaned up now because these columns are needed to `update_flat_head()`.
            Some(STATE_SNAPSHOT_COLUMNS),
        )?;
        let store = storage.get_hot_store();
        // It is fine to create a separate FlatStorageManager, because
        // it is used only for reading flat storage in the snapshot a
        // doesn't introduce memory overhead.
        let flat_storage_manager = FlatStorageManager::new(store.clone());
        *state_snapshot_lock = Some(StateSnapshot::new(
            store,
            prev_block_hash,
            flat_storage_manager,
            shard_uids,
            Some(block),
        ));

        // this will set the new hash for state snapshot in rocksdb. will retry until success.
        let mut set_state_snapshot_in_db = false;
        while !set_state_snapshot_in_db {
            set_state_snapshot_in_db = match self.set_state_snapshot_hash(Some(prev_block_hash)) {
                Ok(_) => true,
                Err(err) => {
                    // This will be retried.
                    tracing::debug!(target: "state_snapshot", ?err, "Failed to set the new state snapshot for BlockMisc::STATE_SNAPSHOT_KEY in rocksdb");
                    false
                }
            }
        }

        metrics::HAS_STATE_SNAPSHOT.set(1);
        tracing::info!(target: "state_snapshot", ?prev_block_hash, "Made a checkpoint");
        Ok(Some(state_snapshot_lock.as_ref().unwrap().get_shard_uids()))
    }

    /// Deletes all snapshots and unsets the STATE_SNAPSHOT_KEY.
    pub fn delete_state_snapshot(&self) {
        let _span =
            tracing::info_span!(target: "state_snapshot", "delete_state_snapshot").entered();
        let _timer = metrics::DELETE_STATE_SNAPSHOT_ELAPSED.start_timer();

        // get snapshot_hash after acquiring write lock
        let mut state_snapshot_lock = self.state_snapshot().write().unwrap();
        if state_snapshot_lock.is_some() {
            // Drop Store before deleting the underlying data.
            *state_snapshot_lock = None;
        }
        let StateSnapshotConfig { home_dir, hot_store_path, state_snapshot_subdir, .. } =
            self.state_snapshot_config();

        // This will delete all existing snapshots from file system. Will retry 3 times
        for _ in 0..3 {
            match self.delete_all_state_snapshots(home_dir, hot_store_path, state_snapshot_subdir) {
                Ok(_) => break,
                Err(err) => {
                    tracing::error!(target: "state_snapshot", ?err, "Failed to delete the old state snapshot from file system or from rocksdb")
                }
            }
        }

        // this will delete the STATE_SNAPSHOT_KEY-value pair from db. Will retry 3 times
        for _ in 0..3 {
            match self.set_state_snapshot_hash(None) {
                Ok(_) => break,
                Err(err) => {
                    tracing::error!(target: "state_snapshot", ?err, "Failed to delete the old state snapshot for BlockMisc::STATE_SNAPSHOT_KEY in rocksdb")
                }
            }
        }

        metrics::HAS_STATE_SNAPSHOT.set(0);
    }

    /// Deletes all existing state snapshots in the parent directory
    fn delete_all_state_snapshots(
        &self,
        home_dir: &Path,
        hot_store_path: &Path,
        state_snapshot_subdir: &Path,
    ) -> Result<(), io::Error> {
        let _span =
            tracing::info_span!(target: "state_snapshot", "delete_all_state_snapshots").entered();
        let path = home_dir.join(hot_store_path).join(state_snapshot_subdir);
        if path.exists() {
            std::fs::remove_dir_all(&path)?
        }
        Ok(())
    }

    pub fn get_state_snapshot_base_dir(
        prev_block_hash: &CryptoHash,
        home_dir: &Path,
        hot_store_path: &Path,
        state_snapshot_subdir: &Path,
    ) -> PathBuf {
        // Assumptions:
        // * RocksDB checkpoints are taken instantly and for free, because the filesystem supports hard links.
        // * The best place for checkpoints is within the `hot_store_path`, because that directory is often a separate disk.
        home_dir.join(hot_store_path).join(state_snapshot_subdir).join(format!("{prev_block_hash}"))
    }

    /// Retrieves STATE_SNAPSHOT_KEY
    pub fn get_state_snapshot_hash(&self) -> Result<CryptoHash, io::Error> {
        option_to_not_found(
            self.get_store().get_ser(DBCol::BlockMisc, STATE_SNAPSHOT_KEY),
            "STATE_SNAPSHOT_KEY",
        )
    }

    /// Updates STATE_SNAPSHOT_KEY.
    pub fn set_state_snapshot_hash(&self, value: Option<CryptoHash>) -> Result<(), io::Error> {
        let mut store_update = self.store_update();
        let key = STATE_SNAPSHOT_KEY;
        match value {
            None => store_update.delete(DBCol::BlockMisc, key),
            Some(value) => store_update.set_ser(DBCol::BlockMisc, key, &value)?,
        }
        store_update.commit().into()
    }

    /// Read RocksDB for the latest available snapshot hash, if available, open base_path+snapshot_hash for the state snapshot
    /// we don't deal with multiple snapshots here because we will deal with it whenever a new snapshot is created and saved to file system
    pub fn maybe_open_state_snapshot(
        &self,
        get_shard_uids_fn: impl FnOnce(CryptoHash) -> Result<Vec<ShardUId>, EpochError>,
    ) -> Result<(), anyhow::Error> {
        let _span =
            tracing::info_span!(target: "state_snapshot", "maybe_open_state_snapshot").entered();
        metrics::HAS_STATE_SNAPSHOT.set(0);
        let StateSnapshotConfig { home_dir, hot_store_path, state_snapshot_subdir, .. } =
            self.state_snapshot_config();

        // directly return error if no snapshot is found
        let snapshot_hash = self.get_state_snapshot_hash()?;

        let snapshot_path = Self::get_state_snapshot_base_dir(
            &snapshot_hash,
            &home_dir,
            &hot_store_path,
            &state_snapshot_subdir,
        );
        let parent_path = snapshot_path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("{snapshot_path:?} needs to have a parent dir"))?;
        tracing::debug!(target: "state_snapshot", ?snapshot_path, ?parent_path);

        let store_config = StoreConfig::default();

        tracing::debug!(target: "state_snapshot", "1");
        let opener = NodeStorage::opener(&snapshot_path, false, &store_config, None);
        tracing::debug!(target: "state_snapshot", "2");
        let storage = opener.open_in_mode(Mode::ReadOnly)?;
        tracing::debug!(target: "state_snapshot", "3");
        let store = storage.get_hot_store();
        tracing::debug!(target: "state_snapshot", "4");
        let flat_storage_manager = FlatStorageManager::new(store.clone());

        tracing::debug!(target: "state_snapshot", "5");
        let shard_uids = get_shard_uids_fn(snapshot_hash)?;
        tracing::debug!(target: "state_snapshot", "6");
        let mut guard = self.state_snapshot().write().unwrap();
        *guard =
            Some(StateSnapshot::new(store, snapshot_hash, flat_storage_manager, &shard_uids, None));
        metrics::HAS_STATE_SNAPSHOT.set(1);
        tracing::info!(target: "runtime", ?snapshot_hash, ?snapshot_path, "Detected and opened a state snapshot.");
        Ok(())
    }
}

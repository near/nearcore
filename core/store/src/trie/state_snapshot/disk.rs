use super::{STATE_SNAPSHOT_COLUMNS, StateSnapshot};
use crate::adapter::StoreAdapter;
use crate::flat::FlatStorageManager;
use crate::{
    Mode, NodeStorage, ShardTries, StoreConfig, checkpoint_hot_storage_and_cleanup_columns, metrics,
};
use near_primitives::block::Block;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::ShardIndex;

type ShardIndexesAndUIds = Vec<(ShardIndex, ShardUId)>;

impl ShardTries {
    /// Makes a snapshot of the current state of the DB, if one is not already available.
    /// If a new snapshot is created, returns the ids of the included shards.
    pub fn create_state_snapshot(
        &self,
        snapshot_hash: CryptoHash,
        shard_indexes_and_uids: &[(ShardIndex, ShardUId)],
        block: &Block,
    ) -> Result<Option<Vec<ShardUId>>, anyhow::Error> {
        metrics::HAS_STATE_SNAPSHOT.set(0);
        // The function returns an `anyhow::Error`, because no special handling of errors is done yet. The errors are logged and ignored.
        let _span =
            tracing::info_span!(target: "state_snapshot", "create_state_snapshot", ?snapshot_hash)
                .entered();
        let _timer = metrics::CREATE_STATE_SNAPSHOT_ELAPSED.start_timer();

        // Checking if state snapshots are enabled is already done on actor level, hence the warning
        let Some(state_snapshots_dir) = self.state_snapshots_dir() else {
            tracing::warn!(target: "state_snapshot", "state snapshots are disabled");
            return Ok(None);
        };

        // `write()` lock is held for the whole duration of this function.
        let mut state_snapshot_lock = self.state_snapshot().write();
        let db_snapshot_hash = self.store().get_state_snapshot_hash();
        if let Some(state_snapshot) = &*state_snapshot_lock {
            // only return Ok() when the hash stored in STATE_SNAPSHOT_KEY and in state_snapshot_lock and snapshot_hash are the same
            if db_snapshot_hash.is_ok_and(|hash| hash == snapshot_hash)
                && state_snapshot.snapshot_hash == snapshot_hash
            {
                tracing::warn!(target: "state_snapshot", ?snapshot_hash, "requested a state snapshot but that is already available");
                return Ok(None);
            }
            tracing::error!(target: "state_snapshot", ?snapshot_hash, ?state_snapshot.snapshot_hash, "requested a state snapshot but that is already available with a different hash");
        }

        let storage = checkpoint_hot_storage_and_cleanup_columns(
            &self.store().store(),
            &Self::get_state_snapshot_base_dir(&snapshot_hash, state_snapshots_dir),
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
            snapshot_hash,
            flat_storage_manager,
            shard_indexes_and_uids,
            Some(block),
        ));

        let mut store_update = self.store_update();
        store_update.set_state_snapshot_hash(Some(snapshot_hash));
        store_update.commit();

        metrics::HAS_STATE_SNAPSHOT.set(1);
        tracing::info!(target: "state_snapshot", ?snapshot_hash, "made a checkpoint");
        Ok(Some(state_snapshot_lock.as_ref().unwrap().get_included_shard_uids()))
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

        let store_config = StoreConfig::state_snapshot_store_config();

        let opener = NodeStorage::opener(&snapshot_path, &store_config, None, None);
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
        tracing::info!(target: "runtime", ?snapshot_hash, ?snapshot_path, "detected and opened a state snapshot");
        Ok(())
    }
}

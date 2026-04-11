use crate::replay_chunk::{ChunkReplayResult, replay_chunk};
use anyhow::{Context, Result};
use near_chain::runtime::NightshadeRuntime;
use near_chain::types::{RuntimeAdapter, StorageDataSource};
use near_chain::{BlockHeader, ChainStore, ChainStoreAccess};
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_primitives::borsh::BorshDeserialize;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{RawStateChangesWithTrieKey, ShardId};
use near_store::adapter::StoreAdapter;
use near_store::flat::{self, FlatStateChanges, FlatStorageStatus};
use near_store::trie::KeyForStateChanges;
use near_store::trie::mem::memtrie_update::TrackingMode;
use near_store::{DBCol, ShardTries, ShardUId};
use std::collections::HashMap;
use std::sync::Arc;

/// Result of replaying a single block across all shards.
pub struct BlockReplayResult {
    pub block_header: BlockHeader,
    pub chunk_results: Vec<ChunkReplayResult>,
}

/// How the controller reads state during replay and advances between blocks.
pub enum ReplayStorageMode {
    /// Uses memtries for state reads. On advance, updates memtries using
    /// state changes from `DBCol::StateChanges`.
    Memtries,
    /// Uses flat storage for state reads. On advance, adds deltas to flat
    /// storage in memory and moves the flat head forward.
    FlatState,
}

/// Controller for replaying chunks block by block.
///
/// Tracks the current block hash and advances state between blocks.
/// Replay and advance are decoupled: `replay_current_block()` replays
/// without mutating controller state, while `advance()` moves forward
/// and updates the internal state (memtries or flat storage) to match
/// the expected database state.
pub struct SequentialChunksReplayController {
    chain_store: ChainStore,
    runtime: Arc<NightshadeRuntime>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    current_block_hash: CryptoHash,
    storage_mode: ReplayStorageMode,
}

impl SequentialChunksReplayController {
    /// Creates a controller with the given storage mode.
    ///
    /// The runtime should be backed by a `CombinedDatabase` store that
    /// reads flat-state columns from an earlier snapshot and everything
    /// else from the main store.
    pub fn new(
        chain_store: ChainStore,
        runtime: Arc<NightshadeRuntime>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        storage_mode: ReplayStorageMode,
    ) -> Result<Self> {
        let flat_head_hash =
            init_storage(&chain_store, &runtime, epoch_manager.as_ref(), &storage_mode)?;

        let current_block_hash = chain_store
            .get_next_block_hash(&flat_head_hash)
            .context("no block after flat head to start replay from")?;

        Ok(Self { chain_store, runtime, epoch_manager, current_block_hash, storage_mode })
    }

    /// Returns the block hash the controller is currently at.
    pub fn current_block_hash(&self) -> &CryptoHash {
        &self.current_block_hash
    }

    /// Returns the block hash of the next block to replay, or `None` if
    /// there is no next block in the chain.
    pub fn next_block_hash(&self) -> Option<CryptoHash> {
        self.chain_store.get_next_block_hash(&self.current_block_hash).ok()
    }

    /// Replays the current block without changing the controller state.
    pub fn replay_current_block(&self) -> Result<BlockReplayResult> {
        let block = self.chain_store.get_block(&self.current_block_hash)?;
        let block_header = block.header().clone();
        let epoch_id = block_header.epoch_id();
        let shard_ids = self.epoch_manager.shard_ids(epoch_id)?;
        let storage_data_source = match self.storage_mode {
            ReplayStorageMode::Memtries | ReplayStorageMode::FlatState => StorageDataSource::Db,
        };

        let mut chunk_results = Vec::with_capacity(shard_ids.len());
        for shard_id in &shard_ids {
            let shard_uid = shard_id_to_uid(self.epoch_manager.as_ref(), *shard_id, epoch_id)?;
            let result = replay_chunk(
                &self.chain_store,
                self.runtime.as_ref(),
                self.epoch_manager.as_ref(),
                &self.current_block_hash,
                shard_uid,
                storage_data_source.clone(),
            )?;
            chunk_results.push(result);
        }

        Ok(BlockReplayResult { block_header, chunk_results })
    }

    /// Advances the controller to the next block, updating internal state
    /// (memtries or flat storage) using stored state changes.
    /// Returns `false` when there is no next block in the chain.
    pub fn advance(&mut self) -> Result<bool> {
        let block_hash = match self.next_block_hash() {
            Some(hash) => hash,
            None => return Ok(false),
        };

        let block = self.chain_store.get_block(&block_hash)?;
        let height = block.header().height();
        let prev_block_hash = *block.header().prev_hash();
        let epoch_id = block.header().epoch_id();
        let shard_layout = self.epoch_manager.get_shard_layout(epoch_id)?;
        let shard_ids = self.epoch_manager.shard_ids(epoch_id)?;

        let changes_by_shard = self.partition_state_changes(&block_hash, &shard_layout)?;

        for shard_id in &shard_ids {
            let shard_uid = shard_id_to_uid(self.epoch_manager.as_ref(), *shard_id, epoch_id)?;
            let shard_changes = changes_by_shard.get(shard_id).cloned().unwrap_or_default();
            let flat_changes = FlatStateChanges::from_state_changes(&shard_changes);

            match self.storage_mode {
                ReplayStorageMode::Memtries => {
                    self.advance_memtrie(shard_uid, height, &flat_changes)?;
                }
                ReplayStorageMode::FlatState => {
                    self.advance_flat_storage(
                        shard_uid,
                        &block_hash,
                        height,
                        &prev_block_hash,
                        flat_changes,
                    )?;
                }
            }
        }

        self.current_block_hash = block_hash;
        Ok(true)
    }

    /// Reads state changes for a block and partitions them by shard id.
    fn partition_state_changes(
        &self,
        block_hash: &CryptoHash,
        shard_layout: &near_primitives::shard_layout::ShardLayout,
    ) -> Result<HashMap<ShardId, Vec<RawStateChangesWithTrieKey>>> {
        let mut changes_by_shard: HashMap<ShardId, Vec<RawStateChangesWithTrieKey>> =
            HashMap::new();
        for (row_key, change) in
            KeyForStateChanges::for_block(block_hash).find_rows_iter(&self.chain_store.store())
        {
            let shard_id = match change.trie_key.get_account_id() {
                Some(account_id) => shard_layout.account_id_to_shard_id(&account_id),
                None => KeyForStateChanges::delayed_receipt_key_decode_shard_uid(
                    &row_key,
                    block_hash,
                    &change.trie_key,
                )
                .context("failed to decode shard uid from state change row key")?
                .shard_id(),
            };
            changes_by_shard.entry(shard_id).or_default().push(change);
        }
        Ok(changes_by_shard)
    }

    /// Advances memtries for a shard using the given flat state changes.
    fn advance_memtrie(
        &self,
        shard_uid: ShardUId,
        block_height: u64,
        flat_changes: &FlatStateChanges,
    ) -> Result<()> {
        let prev_chunk_extra = self
            .chain_store
            .get_chunk_extra(&self.current_block_hash, &shard_uid)
            .context("failed to get prev chunk extra")?;
        let old_state_root = *prev_chunk_extra.state_root();

        let tries = self.runtime.get_tries();
        let memtries = tries.get_memtries(shard_uid).context("memtries not loaded for shard")?;
        let mut memtries_guard = memtries.write();

        let mut trie_update = memtries_guard
            .update(old_state_root, TrackingMode::None)
            .map_err(|e| anyhow::anyhow!("failed to create memtrie update: {}", e))?;
        for (key, value) in &flat_changes.0 {
            match value {
                Some(value) => {
                    trie_update
                        .insert_memtrie_only(key, value.clone())
                        .map_err(|e| anyhow::anyhow!("failed to insert into memtrie: {}", e))?;
                }
                None => {
                    trie_update
                        .delete_memtrie_only(key)
                        .map_err(|e| anyhow::anyhow!("failed to delete from memtrie: {}", e))?;
                }
            };
        }

        let memtrie_changes = trie_update.to_memtrie_changes_only();
        memtries_guard.apply_memtrie_changes(block_height, &memtrie_changes);
        Ok(())
    }

    /// Advances flat storage for a shard by adding a delta and moving the
    /// flat head. The delta is kept in memory; the returned store update
    /// is intentionally not committed to disk.
    fn advance_flat_storage(
        &self,
        shard_uid: ShardUId,
        block_hash: &CryptoHash,
        block_height: u64,
        prev_block_hash: &CryptoHash,
        flat_changes: FlatStateChanges,
    ) -> Result<()> {
        let delta = flat::FlatStateDelta {
            metadata: flat::FlatStateDeltaMetadata {
                block: flat::BlockInfo {
                    hash: *block_hash,
                    height: block_height,
                    prev_hash: *prev_block_hash,
                },
                prev_block_with_changes: None,
            },
            changes: flat_changes,
        };

        let flat_storage_manager = self.runtime.get_flat_storage_manager();
        let flat_storage = flat_storage_manager
            .get_flat_storage_for_shard(shard_uid)
            .context("flat storage not initialized for shard")?;

        // add_delta inserts the delta into the in-memory cache. Flat storage
        // serves reads for subsequent blocks by applying cached deltas on
        // top of the flat head. We drop the store update since both stores
        // are read-only — no disk writes needed.
        let _store_update = flat_storage
            .add_delta(delta)
            .map_err(|e| anyhow::anyhow!("failed to add flat storage delta: {}", e))?;

        Ok(())
    }
}

/// Reads the flat head hash (verifying consistency across shards) and
/// initializes storage for each shard based on the storage mode.
fn init_storage(
    chain_store: &ChainStore,
    runtime: &NightshadeRuntime,
    epoch_manager: &dyn EpochManagerAdapter,
    storage_mode: &ReplayStorageMode,
) -> Result<CryptoHash> {
    let tries = runtime.get_tries();
    let store = tries.store();

    // Read all flat storage statuses, collecting the flat head hash from
    // ready shards and tracking non-ready ones for later validation.
    let mut flat_head_hash: Option<CryptoHash> = None;
    let mut non_ready: HashMap<ShardUId, FlatStorageStatus> = HashMap::new();

    for (shard_uid_bytes, status_bytes) in store.store_ref().iter(DBCol::FlatStorageStatus) {
        let status = FlatStorageStatus::try_from_slice(&status_bytes)
            .context("failed to deserialize flat storage status")?;
        match &status {
            FlatStorageStatus::Ready(ready) => {
                let hash = ready.flat_head.hash;
                match flat_head_hash {
                    None => flat_head_hash = Some(hash),
                    Some(expected) => anyhow::ensure!(
                        hash == expected,
                        "flat head mismatch: shard {} has flat head {} but expected {}",
                        ShardUId::try_from(shard_uid_bytes.as_ref())
                            .map_or_else(|_| "unknown".to_string(), |uid| uid.to_string()),
                        hash,
                        expected,
                    ),
                }
            }
            _ => {
                if let Ok(shard_uid) = ShardUId::try_from(shard_uid_bytes.as_ref()) {
                    non_ready.insert(shard_uid, status);
                }
            }
        }
    }

    let flat_head_hash = flat_head_hash.context("no flat storage status found in store")?;

    // Verify all shards in the flat head's layout are ready, then
    // initialize each shard according to the storage mode.
    let block =
        chain_store.get_block(&flat_head_hash).context("failed to get block at flat head")?;
    let epoch_id = block.header().epoch_id();
    let shard_ids = epoch_manager.shard_ids(epoch_id)?;

    let flat_storage_manager = runtime.get_flat_storage_manager();
    for shard_id in &shard_ids {
        let shard_uid = shard_id_to_uid(epoch_manager, *shard_id, epoch_id)
            .context("failed to get shard uid")?;
        if let Some(status) = non_ready.get(&shard_uid) {
            anyhow::bail!(
                "flat storage not ready for shard {} in current layout: {:?}",
                shard_uid,
                status,
            );
        }
        match storage_mode {
            ReplayStorageMode::Memtries => {
                tries.load_memtrie(&shard_uid, None, true).map_err(|e| {
                    anyhow::anyhow!("failed to load memtrie for shard {}: {}", shard_uid, e)
                })?;
            }
            ReplayStorageMode::FlatState => {
                flat_storage_manager.create_flat_storage_for_shard(shard_uid).map_err(|e| {
                    anyhow::anyhow!("failed to create flat storage for shard {}: {}", shard_uid, e)
                })?;
            }
        }
    }

    Ok(flat_head_hash)
}

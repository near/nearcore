use crate::replay_chunk::{ChunkReplayResult, replay_chunk};
use anyhow::{Context, Result};
use near_chain::runtime::NightshadeRuntime;
use near_chain::types::{RuntimeAdapter, StorageDataSource};
use near_chain::{BlockHeader, ChainStore, ChainStoreAccess};
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{RawStateChangesWithTrieKey, ShardId};
use near_store::adapter::StoreAdapter;
use near_store::flat::{self, FlatStateChanges, FlatStorageStatus};
use near_store::trie::KeyForStateChanges;
use near_store::trie::mem::memtrie_update::TrackingMode;
use near_store::{DBCol, ShardUId};
use std::collections::HashMap;
use std::sync::Arc;

/// Result of replaying a single block across all shards.
pub struct BlockReplayResult {
    pub block_header: BlockHeader,
    pub chunk_results: Vec<ChunkReplayResult>,
}

/// How the controller reads state during replay and advances between blocks.
#[derive(Clone, Copy, Debug)]
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
/// Tracks the current block header and advances state between blocks.
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
            init_storage(&chain_store, &runtime, epoch_manager.as_ref(), &storage_mode)
                .context("failed to initialize storage")?;

        let current_block_hash = chain_store
            .get_next_block_hash(&flat_head_hash)
            .context("no block after flat head to start replay from")?;

        Ok(Self { chain_store, runtime, epoch_manager, current_block_hash, storage_mode })
    }

    /// Returns the block hash the controller is currently at.
    pub fn current_block_hash(&self) -> &CryptoHash {
        &self.current_block_hash
    }

    /// Replays the current block without changing the controller state.
    #[tracing::instrument(
        target = "replay",
        level = "debug",
        skip_all,
        fields(block_hash = %self.current_block_hash),
    )]
    pub fn replay_current_block(&self) -> Result<BlockReplayResult> {
        let block_header = self
            .chain_store
            .get_block(&self.current_block_hash)
            .context("failed to get current block for replay")?
            .header()
            .clone();
        let shard_layout = get_shard_layout(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            &self.current_block_hash,
        )
        .context("failed to get shard layout for replay")?;

        let mut chunk_results = Vec::with_capacity(shard_layout.num_shards() as usize);
        for shard_uid in shard_layout.shard_uids() {
            let result = replay_chunk(
                &self.chain_store,
                self.runtime.as_ref(),
                self.epoch_manager.as_ref(),
                &self.current_block_hash,
                shard_uid,
                StorageDataSource::Db,
            )
            .with_context(|| format!("failed to replay chunk for shard {}", shard_uid))?;
            chunk_results.push(result);
        }

        Ok(BlockReplayResult { block_header, chunk_results })
    }

    /// Advances the controller to the next block, updating internal state
    /// (memtries or flat storage) using stored state changes.
    /// Returns `false` when there is no next block in the chain.
    #[tracing::instrument(
        target = "replay",
        level = "debug",
        skip_all,
        fields(block_hash = %self.current_block_hash),
    )]
    pub fn advance(&mut self) -> Result<bool> {
        let next_block_hash = match self.chain_store.get_next_block_hash(&self.current_block_hash) {
            Ok(hash) => hash,
            Err(_) => return Ok(false),
        };

        ensure_same_shard_layout(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            &self.current_block_hash,
            &next_block_hash,
        )
        .context("shard layout check failed during advance")?;

        let mut changes_by_shard = get_flat_state_changes_by_shard(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            &next_block_hash,
        )
        .context("failed to get flat state changes")?;

        let shard_layout = get_shard_layout(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            &self.current_block_hash,
        )
        .context("failed to get shard layout for advance")?;
        for shard_uid in shard_layout.shard_uids() {
            let flat_changes = changes_by_shard.remove(&shard_uid.shard_id()).unwrap_or_default();
            advance_shard(
                &self.chain_store,
                self.runtime.as_ref(),
                shard_uid,
                &self.current_block_hash,
                &next_block_hash,
                flat_changes,
                self.storage_mode,
            )
            .with_context(|| {
                format!("failed to advance shard {} to block {}", shard_uid, next_block_hash)
            })?;
        }

        self.current_block_hash = next_block_hash;
        Ok(true)
    }
}

/// Advances a single shard's state (memtries or flat storage) by one block.
fn advance_shard(
    chain_store: &ChainStore,
    runtime: &NightshadeRuntime,
    shard_uid: ShardUId,
    prev_hash: &CryptoHash,
    next_hash: &CryptoHash,
    flat_changes: FlatStateChanges,
    storage_mode: ReplayStorageMode,
) -> Result<()> {
    let next_height = chain_store
        .get_block(next_hash)
        .context("failed to get next block in advance_shard")?
        .header()
        .height();
    match storage_mode {
        ReplayStorageMode::Memtries => {
            let prev_chunk_extra = chain_store
                .get_chunk_extra(prev_hash, &shard_uid)
                .context("failed to get prev chunk extra")?;
            let old_state_root = *prev_chunk_extra.state_root();

            let tries = runtime.get_tries();
            let memtries =
                tries.get_memtries(shard_uid).context("memtries not loaded for shard")?;
            let mut memtries_guard = memtries.write();

            let mut trie_update = memtries_guard
                .update(old_state_root, TrackingMode::None)
                .map_err(|e| anyhow::anyhow!("failed to create memtrie update: {}", e))?;
            for (key, value) in flat_changes.0 {
                match value {
                    Some(value) => {
                        trie_update
                            .insert_memtrie_only(&key, value)
                            .map_err(|e| anyhow::anyhow!("failed to insert into memtrie: {}", e))?;
                    }
                    None => {
                        trie_update
                            .delete_memtrie_only(&key)
                            .map_err(|e| anyhow::anyhow!("failed to delete from memtrie: {}", e))?;
                    }
                };
            }
            let memtrie_changes = trie_update.to_memtrie_changes_only();
            memtries_guard.apply_memtrie_changes(next_height, &memtrie_changes);
        }
        ReplayStorageMode::FlatState => {
            let delta = flat::FlatStateDelta {
                metadata: flat::FlatStateDeltaMetadata {
                    block: flat::BlockInfo {
                        hash: *next_hash,
                        height: next_height,
                        prev_hash: *prev_hash,
                    },
                    prev_block_with_changes: None,
                },
                changes: flat_changes,
            };
            let flat_storage = runtime
                .get_flat_storage_manager()
                .get_flat_storage_for_shard(shard_uid)
                .context("flat storage not initialized for shard")?;
            let _store_update = flat_storage
                .add_delta(delta)
                .map_err(|e| anyhow::anyhow!("failed to add flat storage delta: {}", e))?;
        }
    }
    Ok(())
}

/// Initializes storage for each shard based on the storage mode.
/// Different shards may have their flat heads at different heights; lagging
/// shards are advanced to the maximum flat head. Returns the maximum flat
/// head block hash.
#[tracing::instrument(
    target = "replay",
    level = "debug",
    skip_all,
    fields(?storage_mode),
)]
fn init_storage(
    chain_store: &ChainStore,
    runtime: &NightshadeRuntime,
    epoch_manager: &dyn EpochManagerAdapter,
    storage_mode: &ReplayStorageMode,
) -> Result<CryptoHash> {
    let tries = runtime.get_tries();
    let flat_storage_manager = runtime.get_flat_storage_manager();

    // Read all flat storage statuses into a map.
    let mut statuses: HashMap<ShardUId, FlatStorageStatus> = HashMap::new();
    for (shard_uid_bytes, _) in tries.store().store_ref().iter(DBCol::FlatStorageStatus) {
        let shard_uid = match ShardUId::try_from(shard_uid_bytes.as_ref()) {
            Ok(uid) => uid,
            Err(e) => {
                tracing::warn!(target: "replay", ?e, "failed to parse shard uid from flat storage status key");
                continue;
            }
        };
        statuses.insert(shard_uid, flat_storage_manager.get_flat_storage_status(shard_uid));
    }

    // Find the block hash at the maximum flat head height.
    let max_flat_head = statuses
        .values()
        .filter_map(|s| match s {
            FlatStorageStatus::Ready(r) => Some(r.flat_head.clone()),
            _ => None,
        })
        .max_by_key(|info| info.height)
        .context("no ready flat storage status found")?;
    let max_flat_head_hash = max_flat_head.hash;

    // Iterate through all shards of the block at max height.
    let block = chain_store
        .get_block(&max_flat_head_hash)
        .context("failed to get block at max flat head")?;
    let epoch_id = block.header().epoch_id();
    let shard_layout = epoch_manager
        .get_shard_layout(epoch_id)
        .context("failed to get shard layout at max flat head")?;

    for shard_uid in shard_layout.shard_uids() {
        let flat_head = match statuses.get(&shard_uid) {
            Some(FlatStorageStatus::Ready(r)) => &r.flat_head,
            Some(other) => {
                anyhow::bail!("flat storage not ready for shard {}: {:?}", shard_uid, other)
            }
            None => anyhow::bail!("no flat storage status for shard {}", shard_uid),
        };

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

        // Advance from this shard's flat head to the max flat head.
        let mut current_hash = flat_head.hash;
        while current_hash != max_flat_head_hash {
            let next_hash = chain_store
                .get_next_block_hash(&current_hash)
                .context("failed to get next block hash during catch-up")?;
            tracing::debug!(
                target: "replay",
                %shard_uid,
                %current_hash,
                %next_hash,
                "catching up shard",
            );
            ensure_same_shard_layout(chain_store, epoch_manager, &current_hash, &next_hash)
                .context("shard layout check failed during catch-up")?;
            let mut changes_by_shard =
                get_flat_state_changes_by_shard(chain_store, epoch_manager, &next_hash)
                    .context("failed to get flat state changes during catch-up")?;
            let flat_changes = changes_by_shard.remove(&shard_uid.shard_id()).unwrap_or_default();
            advance_shard(
                chain_store,
                runtime,
                shard_uid,
                &current_hash,
                &next_hash,
                flat_changes,
                *storage_mode,
            )
            .with_context(|| {
                format!(
                    "failed to advance shard {} during catch-up to block {}",
                    shard_uid, next_hash
                )
            })?;
            current_hash = next_hash;
        }
    }

    Ok(max_flat_head_hash)
}

/// Reads state changes for a block from the chain store, partitions them
/// by shard id, and converts each shard's changes to `FlatStateChanges`.
fn get_flat_state_changes_by_shard(
    chain_store: &ChainStore,
    epoch_manager: &dyn EpochManagerAdapter,
    block_hash: &CryptoHash,
) -> Result<HashMap<ShardId, FlatStateChanges>> {
    let shard_layout = get_shard_layout(chain_store, epoch_manager, block_hash)
        .context("failed to get shard layout for state changes")?;
    let mut raw_by_shard: HashMap<ShardId, Vec<RawStateChangesWithTrieKey>> = HashMap::new();
    for (row_key, change) in
        KeyForStateChanges::for_block(block_hash).find_rows_iter(&chain_store.store())
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
        raw_by_shard.entry(shard_id).or_default().push(change);
    }
    Ok(raw_by_shard
        .into_iter()
        .map(|(shard_id, changes)| (shard_id, FlatStateChanges::from_state_changes(&changes)))
        .collect())
}


/// Returns an error if the shard layout differs between the two blocks.
fn ensure_same_shard_layout(
    chain_store: &ChainStore,
    epoch_manager: &dyn EpochManagerAdapter,
    block_hash_a: &CryptoHash,
    block_hash_b: &CryptoHash,
) -> Result<()> {
    let layout_a = get_shard_layout(chain_store, epoch_manager, block_hash_a)?;
    let layout_b = get_shard_layout(chain_store, epoch_manager, block_hash_b)?;
    anyhow::ensure!(
        layout_a == layout_b,
        "shard layout changed between blocks {} and {} — resharding replay is not supported",
        block_hash_a,
        block_hash_b,
    );
    Ok(())
}

/// Returns the shard layout for the epoch of the given block.
fn get_shard_layout(
    chain_store: &ChainStore,
    epoch_manager: &dyn EpochManagerAdapter,
    block_hash: &CryptoHash,
) -> Result<ShardLayout> {
    let block = chain_store
        .get_block(block_hash)
        .with_context(|| format!("failed to get block {}", block_hash))?;
    epoch_manager
        .get_shard_layout(block.header().epoch_id())
        .with_context(|| format!("failed to get shard layout for block {}", block_hash))
}
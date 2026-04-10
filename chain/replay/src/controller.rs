use crate::replay_chunk::{ChunkReplayResult, replay_chunk};
use anyhow::{Context, Result};
use near_chain::runtime::NightshadeRuntime;
use near_chain::types::{RuntimeAdapter, StorageDataSource};
use near_chain::{ChainStore, ChainStoreAccess};
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_primitives::borsh::BorshDeserialize;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{RawStateChangesWithTrieKey, ShardId};
use near_store::flat::{FlatStateChanges, FlatStorageStatus};
use near_store::trie::KeyForStateChanges;
use near_store::trie::mem::memtrie_update::TrackingMode;
use near_store::{DBCol, ShardTries, ShardUId, Store};
use std::collections::HashMap;
use std::sync::Arc;

/// Result of replaying a single block across all shards.
pub struct BlockReplayResult {
    pub height: u64,
    pub block_hash: CryptoHash,
    pub chunk_results: Vec<ChunkReplayResult>,
}

/// Controller for replaying chunks block by block.
///
/// Tracks the current block hash and advances memtries using state changes
/// from the chain store. Memtries are loaded from the store inside the
/// runtime's ShardTries, which may be a different (earlier) snapshot than
/// the one backing ChainStore and EpochManager.
///
/// Replay and advance are decoupled: `replay_next_block` replays chunks
/// without changing the controller state, while `advance` moves the
/// controller forward and updates memtries to the expected database state.
pub struct MemtriesChunksReplayController {
    chain_store: ChainStore,
    runtime: Arc<NightshadeRuntime>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    current_block_hash: CryptoHash,
}

impl MemtriesChunksReplayController {
    /// Loads memtries from the given snapshot store and creates a controller
    /// ready to replay from the block after the snapshot's flat head.
    ///
    /// `memtrie_source_store` should have flat storage at or before the
    /// start of the replay range. Memtries are loaded from it, and the
    /// starting block is determined from its flat head.
    ///
    /// The runtime's ShardTries should be backed by the main store (same
    /// as `chain_store`) so that trie data reads during replay have access
    /// to the full state.
    pub fn load_memtries(
        chain_store: ChainStore,
        runtime: Arc<NightshadeRuntime>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        memtrie_source_store: &Store,
    ) -> Result<Self> {
        let memtrie_block_hash = load_memtries_from_store(
            &chain_store,
            &runtime.get_tries(),
            epoch_manager.as_ref(),
            memtrie_source_store,
        )?;

        // The memtrie has state at memtrie_block_hash. The first replayable
        // block is the one after it, because replay needs the prev block's
        // state root in the memtrie.
        let current_block_hash = chain_store
            .get_next_block_hash(&memtrie_block_hash)
            .context("no block after memtrie block to start replay from")?;

        Ok(Self { chain_store, runtime, epoch_manager, current_block_hash })
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
        let block_hash = self.current_block_hash;
        let block = self.chain_store.get_block(&block_hash)?;
        let height = block.header().height();
        let epoch_id = block.header().epoch_id();
        let shard_ids = self.epoch_manager.shard_ids(epoch_id)?;

        let mut chunk_results = Vec::with_capacity(shard_ids.len());
        for shard_id in &shard_ids {
            let shard_uid = shard_id_to_uid(self.epoch_manager.as_ref(), *shard_id, epoch_id)?;
            let result = replay_chunk(
                &self.chain_store,
                self.runtime.as_ref(),
                self.epoch_manager.as_ref(),
                &block_hash,
                shard_uid,
                StorageDataSource::Db,
            )?;
            chunk_results.push(result);
        }

        Ok(BlockReplayResult { height, block_hash, chunk_results })
    }

    /// Advances the controller to the next block, updating memtries to
    /// the expected state using stored state changes from the chain store.
    /// Returns `false` when there is no next block in the chain.
    pub fn advance(&mut self) -> Result<bool> {
        let block_hash = match self.next_block_hash() {
            Some(hash) => hash,
            None => return Ok(false),
        };

        let block = self.chain_store.get_block(&block_hash)?;
        let height = block.header().height();
        let epoch_id = block.header().epoch_id();
        let shard_layout = self.epoch_manager.get_shard_layout(epoch_id)?;
        let shard_ids = self.epoch_manager.shard_ids(epoch_id)?;

        // Read all state changes for this block and partition by shard id.
        let mut changes_by_shard: HashMap<ShardId, Vec<RawStateChangesWithTrieKey>> =
            HashMap::new();
        for (row_key, change) in
            KeyForStateChanges::for_block(&block_hash).find_rows_iter(&self.chain_store.store())
        {
            let shard_id = match change.trie_key.get_account_id() {
                Some(account_id) => shard_layout.account_id_to_shard_id(&account_id),
                // Keys without an account (e.g. delayed receipts) have the
                // shard UID encoded in the row key.
                None => KeyForStateChanges::delayed_receipt_key_decode_shard_uid(
                    &row_key,
                    &block_hash,
                    &change.trie_key,
                )
                .context("failed to decode shard uid from state change row key")?
                .shard_id(),
            };
            changes_by_shard.entry(shard_id).or_default().push(change);
        }

        let tries = self.runtime.get_tries();
        for shard_id in &shard_ids {
            let shard_uid = shard_id_to_uid(self.epoch_manager.as_ref(), *shard_id, epoch_id)?;

            let prev_chunk_extra = self
                .chain_store
                .get_chunk_extra(&self.current_block_hash, &shard_uid)
                .context("failed to get prev chunk extra for memtrie advance")?;
            let expected_chunk_extra = self
                .chain_store
                .get_chunk_extra(&block_hash, &shard_uid)
                .context("failed to get expected chunk extra for memtrie advance")?;
            let old_state_root = *prev_chunk_extra.state_root();
            let new_state_root = *expected_chunk_extra.state_root();

            let shard_changes = changes_by_shard.remove(shard_id).unwrap_or_default();
            let flat_changes = FlatStateChanges::from_state_changes(&shard_changes);

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
            let applied_root = memtries_guard.apply_memtrie_changes(height, &memtrie_changes);
            assert_eq!(
                applied_root, new_state_root,
                "memtrie root mismatch after applying stored state changes for shard {}",
                shard_uid,
            );
        }

        self.current_block_hash = block_hash;
        Ok(true)
    }
}

/// Loads memtries from a snapshot store into the given ShardTries
/// and returns the flat head block hash.
fn load_memtries_from_store(
    chain_store: &ChainStore,
    tries: &ShardTries,
    epoch_manager: &dyn EpochManagerAdapter,
    snapshot_store: &Store,
) -> Result<CryptoHash> {
    let (_, status_bytes) = snapshot_store
        .iter(DBCol::FlatStorageStatus)
        .next()
        .context("no flat storage status found in snapshot store")?;
    let status = FlatStorageStatus::try_from_slice(&status_bytes)
        .context("failed to deserialize flat storage status")?;
    let flat_head = match &status {
        FlatStorageStatus::Ready(ready) => &ready.flat_head,
        other => anyhow::bail!(
            "flat storage not ready for shard {}: {:?}",
            ShardUId::try_from(status_bytes.as_ref())
                .map_or_else(|_| "unknown".to_string(), |uid| uid.to_string()),
            other,
        ),
    };
    let flat_head_hash = flat_head.hash;

    let block =
        chain_store.get_block(&flat_head_hash).context("failed to get block at flat head")?;
    let epoch_id = block.header().epoch_id();
    let shard_ids = epoch_manager.shard_ids(epoch_id)?;

    for shard_id in &shard_ids {
        let shard_uid = shard_id_to_uid(epoch_manager, *shard_id, epoch_id)
            .context("failed to get shard uid")?;
        tries.load_memtrie_from_store(snapshot_store, &shard_uid, None, true).map_err(|e| {
            anyhow::anyhow!("failed to load memtrie for shard {}: {}", shard_uid, e)
        })?;
    }

    Ok(flat_head_hash)
}

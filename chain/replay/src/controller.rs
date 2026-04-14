use crate::replay_chunk::{ChunkReplayResult, replay_chunk};
use anyhow::{Context, Result, anyhow, ensure};
use near_chain::types::{RuntimeAdapter, StorageDataSource};
use near_chain::{ChainStore, ChainStoreAccess};
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::state::FlatStateValue;
use near_primitives::trie_key::trie_key_parsers::parse_account_id_from_raw_key;
use near_primitives::types::ShardId;
use near_store::trie::mem::memtrie_update::TrackingMode;
use near_store::trie::trie_storage::TrieDBStorage;
use near_store::trie::{AccessOptions, KeyForStateChanges};
use near_store::{KeyLookupMode, ShardUId, Trie};
use std::sync::Arc;

/// Controller for replaying chunks backwards, block by block, for a single shard.
///
/// Starts at the chain head (where the memtrie is loaded from flat state)
/// and moves backwards. Only requires a single database.
///
/// Replay and advance are decoupled: `replay_current_chunk()` replays
/// without mutating controller state, while `advance()` moves to the
/// previous block by reversing memtrie changes.
pub struct SequentialChunksReplayController {
    chain_store: ChainStore,
    runtime: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    current_block_hash: CryptoHash,
    shard_uid: ShardUId,
}

impl SequentialChunksReplayController {
    /// Creates a controller that replays backwards from the chain head for
    /// the given shard.
    ///
    /// Resolves the shard's `ShardUId` from the chain head's shard layout,
    /// loads the memtrie from the store's flat state, then reverses the
    /// head block's changes so the memtrie is at prev_state_root(head),
    /// ready for replay_current_chunk().
    pub fn load_memtrie(
        chain_store: ChainStore,
        runtime: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_id: ShardId,
    ) -> Result<Self> {
        let head_hash = chain_store.head().context("failed to get chain head")?.last_block_hash;
        let shard_layout = get_shard_layout(&chain_store, epoch_manager.as_ref(), &head_hash)
            .context("failed to get shard layout at head")?;
        let shard_uid = shard_layout
            .shard_uids()
            .find(|uid| uid.shard_id() == shard_id)
            .with_context(|| format!("shard id {shard_id} not in current shard layout"))?;

        runtime
            .get_tries()
            .load_memtrie(&shard_uid, None, true)
            .with_context(|| format!("failed to load memtries for shard id {shard_id}"))?;

        let controller =
            Self { chain_store, runtime, epoch_manager, current_block_hash: head_hash, shard_uid };

        // Reverse the head block's changes so the memtrie is at
        // prev_state_root(head), ready for replay_current_chunk().
        controller.reverse_current_block()?;
        Ok(controller)
    }

    /// Returns the block hash the controller is currently at.
    pub fn current_block_hash(&self) -> &CryptoHash {
        &self.current_block_hash
    }

    /// Replays the chunk at the current block without changing the
    /// controller state.
    #[tracing::instrument(
        target = "replay",
        level = "debug",
        skip_all,
        fields(block_hash = %self.current_block_hash, shard_uid = %self.shard_uid),
    )]
    pub fn replay_current_chunk(&self) -> Result<ChunkReplayResult> {
        replay_chunk(
            &self.chain_store,
            self.runtime.as_ref(),
            self.epoch_manager.as_ref(),
            &self.current_block_hash,
            self.shard_uid,
            StorageDataSource::Db,
        )
        .with_context(|| format!("failed to replay chunk for shard {}", self.shard_uid))
    }

    /// Advances the controller to the previous block by reversing memtrie
    /// changes. After advancing, `replay_current_chunk()` is ready to use.
    /// Returns `false` when the current block is genesis or when the
    /// previous block is no longer available in the store (e.g. it has
    /// been garbage collected).
    #[tracing::instrument(
        target = "replay",
        level = "debug",
        skip_all,
        fields(block_hash = %self.current_block_hash, shard_uid = %self.shard_uid),
    )]
    pub fn advance(&mut self) -> Result<bool> {
        let block = self
            .chain_store
            .get_block(&self.current_block_hash)
            .context("failed to get current block for advance")?;
        if block.header().is_genesis() {
            return Ok(false);
        }
        let prev_hash = *block.header().prev_hash();

        if !self.chain_store.block_exists(&prev_hash) {
            tracing::debug!(
                target: "replay",
                %prev_hash,
                "prev block not available (likely garbage collected), stopping advance",
            );
            return Ok(false);
        }

        ensure_same_shard_layout(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            &self.current_block_hash,
            &prev_hash,
        )
        .context("shard layout check failed during advance")?;

        self.current_block_hash = prev_hash;
        self.reverse_current_block()?;
        Ok(true)
    }

    /// Reverses the current block's memtrie changes so the memtrie is at
    /// prev_state_root(current), ready for replay.
    ///
    /// Iterates the shard's changed trie keys from `DBCol::StateChanges`,
    /// reads the previous value for each key from the on-disk trie at
    /// prev_state_root using `TrieDBStorage` (bypassing memtries and flat
    /// storage), and applies the reverse delta to the memtrie.
    fn reverse_current_block(&self) -> Result<()> {
        let shard_uid = self.shard_uid;
        let block_hash = &self.current_block_hash;

        let block = self.chain_store.get_block(block_hash).context("failed to get block")?;
        let prev_hash = block.header().prev_hash();
        let height = block.header().height();

        let shard_layout =
            get_shard_layout(&self.chain_store, self.epoch_manager.as_ref(), block_hash)
                .context("failed to get shard layout for reverse")?;

        let chunk_extra = self
            .chain_store
            .get_chunk_extra(block_hash, &shard_uid)
            .context("failed to get chunk extra")?;
        let current_state_root = *chunk_extra.state_root();

        let prev_chunk_extra = self
            .chain_store
            .get_chunk_extra(prev_hash, &shard_uid)
            .context("failed to get prev chunk extra")?;
        let prev_state_root = *prev_chunk_extra.state_root();

        // Create a trie that reads directly from DBCol::State, bypassing
        // memtries and flat storage, to look up previous values.
        let tries = self.runtime.get_tries();
        let trie = Trie::new(
            Arc::new(TrieDBStorage::new(tries.store(), shard_uid)),
            prev_state_root,
            None,
        );

        let memtries = tries.get_memtries(shard_uid).context("memtries not loaded for shard")?;
        let mut memtries_guard = memtries.write();
        let mut trie_update = memtries_guard
            .update(current_state_root, TrackingMode::None)
            .map_err(|e| anyhow!("failed to create memtrie update: {}", e))?;

        // Iterate changed trie keys for this shard, looking up the previous
        // value and applying the reverse change directly to the memtrie.
        let target_shard_id = shard_uid.shard_id();
        let store = self.chain_store.store();
        for (row_key, _change) in KeyForStateChanges::for_block(block_hash).find_rows_iter(&store) {
            let full_trie_key = &row_key[CryptoHash::LENGTH..];

            let (key_shard_id, actual_trie_key) =
                match parse_account_id_from_raw_key(full_trie_key)? {
                    Some(account_id) => {
                        (shard_layout.account_id_to_shard_id(&account_id), full_trie_key)
                    }
                    None => {
                        ensure!(
                            full_trie_key.len() >= 8,
                            "trie key too short for shard uid suffix"
                        );
                        let (trie_key, shard_uid_raw) =
                            full_trie_key.split_at(full_trie_key.len() - 8);
                        let key_shard_uid = ShardUId::try_from(shard_uid_raw)
                            .map_err(|e| anyhow!("failed to decode shard uid: {}", e))?;
                        (key_shard_uid.shard_id(), trie_key)
                    }
                };
            if key_shard_id != target_shard_id {
                continue;
            }

            let prev_value_ref = trie
                .get_optimized_ref(
                    actual_trie_key,
                    KeyLookupMode::MemOrTrie,
                    AccessOptions::DEFAULT,
                )?
                .map(|v| v.into_value_ref());

            match prev_value_ref {
                None => {
                    trie_update
                        .delete_memtrie_only(actual_trie_key)
                        .map_err(|e| anyhow!("failed to delete from memtrie: {}", e))?;
                }
                Some(value_ref) => {
                    let prev_value = if FlatStateValue::should_inline(value_ref.len()) {
                        let value = trie.retrieve_value(&value_ref.hash, AccessOptions::DEFAULT)?;
                        FlatStateValue::Inlined(value)
                    } else {
                        FlatStateValue::Ref(value_ref)
                    };
                    trie_update
                        .insert_memtrie_only(actual_trie_key, prev_value)
                        .map_err(|e| anyhow!("failed to insert into memtrie: {}", e))?;
                }
            }
        }

        let memtrie_changes = trie_update.to_memtrie_changes_only();
        let applied_root = memtries_guard.apply_memtrie_changes(height - 1, &memtrie_changes);
        ensure!(
            applied_root == prev_state_root,
            "memtrie root mismatch after reverse: expected {} but got {}",
            prev_state_root,
            applied_root,
        );

        // Clean up the old root we reversed away from.
        memtries_guard.delete_root(&current_state_root);

        Ok(())
    }
}

impl Drop for SequentialChunksReplayController {
    fn drop(&mut self) {
        self.runtime.get_tries().unload_memtrie(&self.shard_uid);
    }
}

fn ensure_same_shard_layout(
    chain_store: &ChainStore,
    epoch_manager: &dyn EpochManagerAdapter,
    block_hash_a: &CryptoHash,
    block_hash_b: &CryptoHash,
) -> Result<()> {
    let layout_a = get_shard_layout(chain_store, epoch_manager, block_hash_a)?;
    let layout_b = get_shard_layout(chain_store, epoch_manager, block_hash_b)?;
    ensure!(
        layout_a == layout_b,
        "shard layout changed between blocks {} and {} — resharding replay is not supported",
        block_hash_a,
        block_hash_b,
    );
    Ok(())
}

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

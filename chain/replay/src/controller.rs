use crate::replay_chunk::{ChunkReplayResult, replay_chunk};
use near_chain::types::{RuntimeAdapter, StorageDataSource};
use near_chain::{ChainStore, ChainStoreAccess, Error};
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
pub struct MemtrieShardReplayController {
    chain_store: ChainStore,
    runtime: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    current_block_hash: CryptoHash,
    shard_uid: ShardUId,
}

impl MemtrieShardReplayController {
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
    ) -> Result<Self, Error> {
        let head_hash = chain_store.head()?.last_block_hash;
        let shard_layout = get_shard_layout(&chain_store, epoch_manager.as_ref(), &head_hash)?;
        let shard_uid =
            shard_layout.shard_uids().find(|uid| uid.shard_id() == shard_id).ok_or_else(|| {
                Error::Other(format!("shard id {shard_id} not in current shard layout"))
            })?;

        runtime.get_tries().load_memtrie(&shard_uid, None, true)?;

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
    pub fn replay_current_chunk(&self) -> Result<ChunkReplayResult, Error> {
        replay_chunk(
            &self.chain_store,
            self.runtime.as_ref(),
            self.epoch_manager.as_ref(),
            &self.current_block_hash,
            self.shard_uid,
            StorageDataSource::Db,
        )
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
    pub fn advance(&mut self) -> Result<bool, Error> {
        let block = self.chain_store.get_block(&self.current_block_hash)?;
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
        )?;

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
    fn reverse_current_block(&self) -> Result<(), Error> {
        let shard_uid = self.shard_uid;
        let block_hash = &self.current_block_hash;

        let block = self.chain_store.get_block(block_hash)?;
        let prev_hash = block.header().prev_hash();
        let height = block.header().height();

        let shard_layout =
            get_shard_layout(&self.chain_store, self.epoch_manager.as_ref(), block_hash)?;

        let chunk_extra = self.chain_store.get_chunk_extra(block_hash, &shard_uid)?;
        let current_state_root = *chunk_extra.state_root();

        let prev_chunk_extra = self.chain_store.get_chunk_extra(prev_hash, &shard_uid)?;
        let prev_state_root = *prev_chunk_extra.state_root();

        // Create a trie that reads directly from DBCol::State, bypassing
        // memtries and flat storage, to look up previous values.
        let tries = self.runtime.get_tries();
        let trie = Trie::new(
            Arc::new(TrieDBStorage::new(tries.store(), shard_uid)),
            prev_state_root,
            None,
        );

        let memtries = tries
            .get_memtries(shard_uid)
            .ok_or_else(|| Error::Other(format!("memtries not loaded for shard {shard_uid}")))?;
        let mut memtries_guard = memtries.write();
        let mut trie_update = memtries_guard.update(current_state_root, TrackingMode::None)?;

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
                        if full_trie_key.len() < 8 {
                            return Err(Error::Other(
                                "trie key too short for shard uid suffix".into(),
                            ));
                        }
                        let (trie_key, shard_uid_raw) =
                            full_trie_key.split_at(full_trie_key.len() - 8);
                        let key_shard_uid = ShardUId::try_from(shard_uid_raw).map_err(|e| {
                            Error::Other(format!("failed to decode shard uid: {e}"))
                        })?;
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
                    trie_update.delete_memtrie_only(actual_trie_key)?;
                }
                Some(value_ref) => {
                    let prev_value = if FlatStateValue::should_inline(value_ref.len()) {
                        let value = trie.retrieve_value(&value_ref.hash, AccessOptions::DEFAULT)?;
                        FlatStateValue::Inlined(value)
                    } else {
                        FlatStateValue::Ref(value_ref)
                    };
                    trie_update.insert_memtrie_only(actual_trie_key, prev_value)?;
                }
            }
        }

        let memtrie_changes = trie_update.to_memtrie_changes_only();
        let applied_root = memtries_guard.apply_memtrie_changes(height - 1, &memtrie_changes);
        if applied_root != prev_state_root {
            return Err(Error::Other(format!(
                "memtrie root mismatch after reverse: expected {prev_state_root} but got {applied_root}",
            )));
        }

        // Clean up the old root we reversed away from.
        memtries_guard.delete_root(&current_state_root);

        Ok(())
    }
}

impl Drop for MemtrieShardReplayController {
    fn drop(&mut self) {
        self.runtime.get_tries().unload_memtrie(&self.shard_uid);
    }
}

fn ensure_same_shard_layout(
    chain_store: &ChainStore,
    epoch_manager: &dyn EpochManagerAdapter,
    block_hash_a: &CryptoHash,
    block_hash_b: &CryptoHash,
) -> Result<(), Error> {
    let layout_a = get_shard_layout(chain_store, epoch_manager, block_hash_a)?;
    let layout_b = get_shard_layout(chain_store, epoch_manager, block_hash_b)?;
    if layout_a != layout_b {
        return Err(Error::Other(format!(
            "shard layout changed between blocks {block_hash_a} and {block_hash_b} — resharding replay is not supported",
        )));
    }
    Ok(())
}

fn get_shard_layout(
    chain_store: &ChainStore,
    epoch_manager: &dyn EpochManagerAdapter,
    block_hash: &CryptoHash,
) -> Result<ShardLayout, Error> {
    let block = chain_store.get_block(block_hash)?;
    Ok(epoch_manager.get_shard_layout(block.header().epoch_id())?)
}

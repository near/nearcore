use near_chain::chain::collect_receipts_from_response;
use near_chain::types::{
    ApplyChunkResult, MaybePinnedMemtrieRoot, RuntimeAdapter, StorageDataSource,
};
use near_chain::update_shard::{
    NewChunkData, NewChunkResult, OldChunkData, OldChunkResult, ShardContext, ShardUpdateReason,
    ShardUpdateResult, StorageContext, process_shard_update,
};
use near_chain::{
    Chain, ChainStore, ChainStoreAccess, Error, ReceiptFilter, get_incoming_receipts_for_shard,
};
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_index;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::state::FlatStateValue;
use near_primitives::trie_key::trie_key_parsers::parse_account_id_from_raw_key;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, ShardId};
use near_store::trie::mem::memtrie_update::TrackingMode;
use near_store::trie::trie_storage::TrieDBStorage;
use near_store::trie::{AccessOptions, KeyForStateChanges};
use near_store::{KeyLookupMode, ShardUId, Trie};
use node_runtime::SignedValidPeriodTransactions;
use std::sync::Arc;

/// Result of replaying a single chunk. Contains both the expected ChunkExtra
/// (read from the database) and the actual ChunkExtra (produced by replay).
/// The caller decides how to interpret differences.
pub struct ChunkReplayResult {
    pub block_hash: CryptoHash,
    pub block_height: BlockHeight,
    pub expected: ChunkExtra,
    pub actual: ChunkExtra,
    pub apply_result: ApplyChunkResult,
}

impl ChunkReplayResult {
    /// Returns `Ok(())` if the actual ChunkExtra matches the expected one,
    /// or an error describing the mismatch otherwise.
    pub fn verify(&self) -> Result<(), Error> {
        if self.expected != self.actual {
            return Err(Error::Other(format!(
                "chunk extra mismatch:\nexpected: {:#?}\nactual:   {:#?}",
                self.expected, self.actual,
            )));
        }
        Ok(())
    }
}

/// A chunk replay that has been prepared but not yet executed. Owns all the
/// data needed to apply the chunk via the runtime, so [`Self::replay`] does
/// not access the chain store and is unaffected by garbage collection of the
/// underlying blocks/receipts.
///
/// Borrows from the controller, so the controller cannot prepare another
/// replay until this handle has been consumed by [`Self::replay`].
pub struct PreparedReplay<'a> {
    runtime: &'a dyn RuntimeAdapter,
    shard_uid: ShardUId,
    block_hash: CryptoHash,
    block_height: BlockHeight,
    update_reason: ShardUpdateReason,
    memtrie_pin: MaybePinnedMemtrieRoot,
    /// Cached so the resulting ChunkExtra can be constructed in the
    /// `OldChunk` case (where it copies validator proposals, gas limit etc.
    /// from the previous chunk extra). `None` for the `NewChunk` case.
    prev_chunk_extra: Option<ChunkExtra>,
    expected_chunk_extra: ChunkExtra,
}

impl PreparedReplay<'_> {
    #[tracing::instrument(
        target = "replay",
        level = "debug",
        skip_all,
        fields(block_hash = %self.block_hash, shard_uid = %self.shard_uid),
    )]
    pub fn replay(self) -> Result<ChunkReplayResult, Error> {
        let span = tracing::Span::current();
        let PreparedReplay {
            runtime,
            shard_uid,
            block_hash,
            block_height,
            update_reason,
            memtrie_pin,
            prev_chunk_extra,
            expected_chunk_extra,
        } = self;

        let shard_context = ShardContext { shard_uid, should_apply_chunk: true };
        let shard_update_result =
            process_shard_update(&span, runtime, update_reason, shard_context, memtrie_pin, None)?;

        let (actual_chunk_extra, apply_result) = match shard_update_result {
            ShardUpdateResult::NewChunk(NewChunkResult { apply_result, gas_limit, .. }) => {
                let chunk_extra = apply_result.to_chunk_extra(gas_limit);
                (chunk_extra, apply_result)
            }
            ShardUpdateResult::OldChunk(OldChunkResult { apply_result, .. }) => {
                let mut chunk_extra = prev_chunk_extra.ok_or_else(|| {
                    Error::Other("old chunk replay missing prev_chunk_extra".into())
                })?;
                *chunk_extra.state_root_mut() = apply_result.new_root;
                (chunk_extra, apply_result)
            }
        };

        Ok(ChunkReplayResult {
            block_hash,
            block_height,
            expected: expected_chunk_extra,
            actual: actual_chunk_extra,
            apply_result,
        })
    }
}

/// Controller for replaying chunks backwards, block by block, for a single
/// shard. Starts at the chain head (where the memtrie is loaded from flat
/// state) and moves backwards. Only requires a single database.
///
/// Replay proceeds via repeated calls to [`Self::prepare_next_replay`]; each
/// call captures the data needed to replay the current block, reverses the
/// memtrie to the block's pre-state, and advances internally to the previous
/// block. The returned [`PreparedReplay`] handle owns all the data required
/// to apply the chunk.
pub struct MemtrieShardReplayController {
    chain_store: ChainStore,
    runtime: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    /// The block whose chunk will be replayed by the next call to
    /// [`Self::prepare_next_replay`]. Initially the chain head; advances to
    /// its predecessor at the end of each call.
    current_block_hash: CryptoHash,
    shard_uid: ShardUId,
}

impl MemtrieShardReplayController {
    /// Creates a controller that replays backwards from the chain head for
    /// the given shard.
    ///
    /// Resolves the shard's `ShardUId` from the chain head's shard layout
    /// and loads the memtrie from the store's flat state. The memtrie is
    /// left at `state_root(head)`; each call to
    /// [`Self::prepare_next_replay`] reverses one block's changes.
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

        Ok(Self { chain_store, runtime, epoch_manager, current_block_hash: head_hash, shard_uid })
    }

    /// Prepares the next chunk replay. The first call returns a handle for
    /// the chain head; subsequent calls return handles for successive
    /// predecessors. Each call reverses the current block's memtrie changes,
    /// advances `current_block_hash` to its predecessor, then builds a
    /// handle that owns all the data needed to apply the (now reversed)
    /// block's chunk.
    ///
    /// Returns an error if any required data cannot be read (e.g. the
    /// current block is genesis or the previous block has been garbage
    /// collected). On error the controller's state is unspecified and it
    /// should not be reused.
    #[tracing::instrument(
        target = "replay",
        level = "debug",
        skip_all,
        fields(block_hash = %self.current_block_hash, shard_uid = %self.shard_uid),
    )]
    pub fn prepare_next_replay(&mut self) -> Result<PreparedReplay<'_>, Error> {
        let block_hash = self.current_block_hash;
        // Reversing now to set the memtrie at the previous block so that
        // it can be used to replay the chunk in the current block.
        self.reverse_current_block()?;
        self.build_prepared_replay(block_hash)
    }

    /// Reads all the chain-store/epoch-manager data needed to replay the
    /// chunk at `block_hash` and packages it into a [`PreparedReplay`]
    /// handle that borrows the runtime from `self`.
    fn build_prepared_replay(&self, block_hash: CryptoHash) -> Result<PreparedReplay<'_>, Error> {
        let shard_uid = self.shard_uid;
        let chain_store = &self.chain_store;
        let epoch_manager = self.epoch_manager.as_ref();

        let block = chain_store.get_block(&block_hash)?;
        let block_header = block.header();
        let block_height = block_header.height();
        let prev_hash = *block_header.prev_hash();

        let prev_block = chain_store.get_block(&prev_hash)?;

        let shard_id = shard_uid.shard_id();
        let epoch_id = block_header.epoch_id();
        let shard_index = shard_id_to_index(epoch_manager, shard_id, epoch_id)?;

        let chunk_header = block.chunks()[shard_index].clone();
        let prev_chunk_header = epoch_manager.get_prev_chunk_header(&prev_block, shard_id)?;

        let is_new_chunk = chunk_header.is_new_chunk(block_height);

        let prev_chunk_extra =
            ChunkExtra::clone(chain_store.get_chunk_extra(&prev_hash, &shard_uid)?.as_ref());

        let storage_context = StorageContext {
            storage_data_source: StorageDataSource::Db,
            state_patch: Default::default(),
        };
        let block_context =
            Chain::get_apply_chunk_block_context(&block, prev_block.header(), is_new_chunk);

        let (update_reason, prev_state_root, prev_chunk_extra_for_result) = if is_new_chunk {
            let chunk = chain_store.get_chunk(&chunk_header.chunk_hash())?;

            let shard_layout = epoch_manager.get_shard_layout_from_prev_block(&prev_hash)?;
            let receipt_response = get_incoming_receipts_for_shard(
                chain_store,
                epoch_manager,
                shard_id,
                &shard_layout,
                *block_header.hash(),
                prev_chunk_header.height_included(),
                ReceiptFilter::TargetShard,
            )?;
            let receipts = collect_receipts_from_response(&receipt_response);

            let transactions = SignedValidPeriodTransactions::new(
                chunk.to_transactions().to_vec(),
                vec![true; chunk.to_transactions().len()],
            );

            let prev_state_root = chunk_header.prev_state_root();
            let reason = ShardUpdateReason::NewChunk(NewChunkData {
                gas_limit: chunk_header.gas_limit(),
                prev_state_root,
                prev_validator_proposals: chunk_header.prev_validator_proposals().collect(),
                chunk_hash: Some(chunk_header.chunk_hash().clone()),
                transactions,
                receipts,
                block: block_context,
                storage_context,
            });
            (reason, prev_state_root, None)
        } else {
            let prev_state_root = *prev_chunk_extra.state_root();
            let reason = ShardUpdateReason::OldChunk(OldChunkData {
                block: block_context,
                prev_chunk_extra: prev_chunk_extra.clone(),
                storage_context,
            });
            (reason, prev_state_root, Some(prev_chunk_extra))
        };

        let memtrie_pin =
            self.runtime.get_tries().maybe_pin_memtrie_root(shard_uid, prev_state_root)?;

        let expected_chunk_extra =
            ChunkExtra::clone(chain_store.get_chunk_extra(&block_hash, &shard_uid)?.as_ref());

        Ok(PreparedReplay {
            runtime: self.runtime.as_ref(),
            shard_uid,
            block_hash,
            block_height,
            update_reason,
            memtrie_pin,
            prev_chunk_extra: prev_chunk_extra_for_result,
            expected_chunk_extra,
        })
    }

    /// Reverses the current block's memtrie changes so the memtrie is at
    /// prev_state_root(current), ready for replay.
    ///
    /// Iterates the shard's changed trie keys from `DBCol::StateChanges`,
    /// reads the previous value for each key from the on-disk trie at
    /// prev_state_root using `TrieDBStorage` (bypassing memtries and flat
    /// storage), and applies the reverse delta to the memtrie.
    fn reverse_current_block(&mut self) -> Result<(), Error> {
        let shard_uid = self.shard_uid;
        let block_hash = self.current_block_hash;

        let block = self.chain_store.get_block(&block_hash)?;
        let prev_hash = *block.header().prev_hash();
        let prev_height = self.chain_store.get_block_header(&prev_hash)?.height();

        let shard_layout =
            get_shard_layout(&self.chain_store, self.epoch_manager.as_ref(), &block_hash)?;
        let prev_block_shard_layout =
            get_shard_layout(&self.chain_store, self.epoch_manager.as_ref(), &prev_hash)?;
        if shard_layout != prev_block_shard_layout {
            return Err(Error::Other(format!(
                "shard layout changed between blocks {prev_hash} and {block_hash} - resharding replay is not supported",
            )));
        }

        let chunk_extra = self.chain_store.get_chunk_extra(&block_hash, &shard_uid)?;
        let current_state_root = *chunk_extra.state_root();

        let prev_chunk_extra = self.chain_store.get_chunk_extra(&prev_hash, &shard_uid)?;
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
        for (row_key, _change) in KeyForStateChanges::for_block(&block_hash).find_rows_iter(&store)
        {
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
        let applied_root = memtries_guard.apply_memtrie_changes(prev_height, &memtrie_changes);
        if applied_root != prev_state_root {
            return Err(Error::Other(format!(
                "memtrie root mismatch after reverse: expected {prev_state_root} but got {applied_root}",
            )));
        }

        // Clean up the old root we reversed away from.
        memtries_guard.delete_root(&current_state_root);

        self.current_block_hash = prev_hash;

        Ok(())
    }
}

impl Drop for MemtrieShardReplayController {
    fn drop(&mut self) {
        self.runtime.get_tries().unload_memtrie(&self.shard_uid);
    }
}

fn get_shard_layout(
    chain_store: &ChainStore,
    epoch_manager: &dyn EpochManagerAdapter,
    block_hash: &CryptoHash,
) -> Result<ShardLayout, Error> {
    let block = chain_store.get_block(block_hash)?;
    Ok(epoch_manager.get_shard_layout(block.header().epoch_id())?)
}

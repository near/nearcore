use std::collections::HashMap;
use std::sync::Arc;
use std::{fmt, io};

use near_chain_configs::GCConfig;
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::get_block_shard_uid;
use near_primitives::state_sync::{StateHeaderKey, StatePartKey};
use near_primitives::types::{BlockHeight, BlockHeightDelta, EpochId, NumBlocks, ShardId};
use near_primitives::utils::{get_block_shard_id, get_outcome_id_block_hash, index_to_bytes};
use near_store::adapter::StoreUpdateAdapter;
use near_store::{DBCol, KeyForStateChanges, ShardTries, ShardUId};

use crate::types::RuntimeAdapter;
use crate::{metrics, Chain, ChainStore, ChainStoreAccess, ChainStoreUpdate};

#[derive(Clone)]
pub enum GCMode {
    Fork(ShardTries),
    Canonical(ShardTries),
    StateSync { clear_block_info: bool },
}

impl fmt::Debug for GCMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GCMode::Fork(_) => write!(f, "GCMode::Fork"),
            GCMode::Canonical(_) => write!(f, "GCMode::Canonical"),
            GCMode::StateSync { .. } => write!(f, "GCMode::StateSync"),
        }
    }
}

/// Both functions here are only used for testing as they create convenient
/// wrappers that allow us to do correctness integration testing without having
/// to fully spin up GCActor
///
/// TODO - the reset_data_pre_state_sync function seems to also be used in
/// production code. It's used in update_sync_status <- handle_sync_needed <- run_sync_step
impl Chain {
    pub fn clear_data(&mut self, gc_config: &GCConfig) -> Result<(), Error> {
        let runtime_adapter = self.runtime_adapter.clone();
        let epoch_manager = self.epoch_manager.clone();
        self.mut_chain_store().clear_data(gc_config, runtime_adapter, epoch_manager)
    }

    pub fn reset_data_pre_state_sync(&mut self, sync_hash: CryptoHash) -> Result<(), Error> {
        let runtime_adapter = self.runtime_adapter.clone();
        let epoch_manager = self.epoch_manager.clone();
        self.mut_chain_store().reset_data_pre_state_sync(sync_hash, runtime_adapter, epoch_manager)
    }
}

impl ChainStore {
    // GC CONTRACT
    // ===
    //
    // Prerequisites, guaranteed by the System:
    // 1. Genesis block is available and should not be removed by GC.
    // 2. No block in storage except Genesis has height lower or equal to `genesis_height`.
    // 3. There is known lowest block height (Tail) came from Genesis or State Sync.
    //    a. Tail is always on the Canonical Chain.
    //    b. Only one Tail exists.
    //    c. Tail's height is higher than or equal to `genesis_height`,
    // 4. There is a known highest block height (Head).
    //    a. Head is always on the Canonical Chain.
    // 5. All blocks in the storage have heights in range [Tail; Head].
    //    a. All forks end up on height of Head or lower.
    // 6. If block A is ancestor of block B, height of A is strictly less then height of B.
    // 7. (Property 1). A block with the lowest height among all the blocks at which the fork has started,
    //    i.e. all the blocks with the outgoing degree 2 or more,
    //    has the least height among all blocks on the fork.
    // 8. (Property 2). The oldest block where the fork happened is never affected
    //    by Canonical Chain Switching and always stays on Canonical Chain.
    //
    // Overall:
    // 1. GC procedure is handled by `clear_data()` function.
    // 2. `clear_data()` runs GC process for all blocks from the Tail to GC Stop Height provided by Epoch Manager.
    // 3. `clear_data()` executes separately:
    //    a. Forks Clearing runs for each height from Tail up to GC Stop Height.
    //    b. Canonical Chain Clearing from (Tail + 1) up to GC Stop Height.
    // 4. Before actual clearing is started, Block Reference Map should be built.
    // 5. `clear_data()` executes every time when block at new height is added.
    // 6. In case of State Sync, State Sync Clearing happens.
    //
    // Forks Clearing:
    // 1. Any fork which ends up on height `height` INCLUSIVELY and earlier will be completely deleted
    //    from the Store with all its ancestors up to the ancestor block where fork is happened
    //    EXCLUDING the ancestor block where fork is happened.
    // 2. The oldest ancestor block always remains on the Canonical Chain by property 2.
    // 3. All forks which end up on height `height + 1` and further are protected from deletion and
    //    no their ancestor will be deleted (even with lowest heights).
    // 4. `clear_forks_data()` handles forks clearing for fixed height `height`.
    //
    // Canonical Chain Clearing:
    // 1. Blocks on the Canonical Chain with the only descendant (if no forks started from them)
    //    are unlocked for Canonical Chain Clearing.
    // 2. If Forks Clearing ended up on the Canonical Chain, the block may be unlocked
    //    for the Canonical Chain Clearing. There is no other reason to unlock the block exists.
    // 3. All the unlocked blocks will be completely deleted
    //    from the Tail up to GC Stop Height EXCLUSIVELY.
    // 4. (Property 3, GC invariant). Tail can be shifted safely to the height of the
    //    earliest existing block. There is always only one Tail (based on property 1)
    //    and it's always on the Canonical Chain (based on property 2).
    //
    // Example:
    //
    // height: 101   102   103   104
    // --------[A]---[B]---[C]---[D]
    //          \     \
    //           \     \---[E]
    //            \
    //             \-[F]---[G]
    //
    // 1. Let's define clearing height = 102. It this case fork A-F-G is protected from deletion
    //    because of G which is on height 103. Nothing will be deleted.
    // 2. Let's define clearing height = 103. It this case Fork Clearing will be executed for A
    //    to delete blocks G and F, then Fork Clearing will be executed for B to delete block E.
    //    Then Canonical Chain Clearing will delete blocks A and B as unlocked.
    //    Block C is the only block of height 103 remains on the Canonical Chain (invariant).
    //
    // State Sync Clearing:
    // 1. Executing State Sync means that no data in the storage is useful for block processing
    //    and should be removed completely.
    // 2. The Tail should be set to the block preceding Sync Block if there are
    //    no missing chunks or to a block before that such that all shards have
    //    at least one new chunk in the blocks leading to the Sync Block.
    // 3. All the data preceding new Tail is deleted in State Sync Clearing
    //    and the Trie is updated with having only Genesis data.
    // 4. State Sync Clearing happens in `reset_data_pre_state_sync()`.
    //
    pub fn clear_data(
        &mut self,
        gc_config: &GCConfig,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
    ) -> Result<(), Error> {
        let _span = tracing::debug_span!(target: "garbage_collection", "clear_data").entered();
        let tries = runtime_adapter.get_tries();
        let head = self.head()?;
        let tail = self.tail()?;
        let gc_stop_height = runtime_adapter.get_gc_stop_height(&head.last_block_hash);
        if gc_stop_height > head.height {
            return Err(Error::GCError("gc_stop_height cannot be larger than head.height".into()));
        }
        let prev_epoch_id = *self.get_block_header(&head.prev_block_hash)?.epoch_id();
        let epoch_change = prev_epoch_id != head.epoch_id;
        let mut fork_tail = self.fork_tail()?;
        metrics::TAIL_HEIGHT.set(tail as i64);
        metrics::FORK_TAIL_HEIGHT.set(fork_tail as i64);
        metrics::CHUNK_TAIL_HEIGHT.set(self.chain_store().chunk_tail()? as i64);
        metrics::GC_STOP_HEIGHT.set(gc_stop_height as i64);
        if epoch_change && fork_tail < gc_stop_height {
            // if head doesn't change on the epoch boundary, we may update fork tail several times
            // but that is fine since it doesn't affect correctness and also we limit the number of
            // heights that fork cleaning goes through so it doesn't slow down client either.
            let mut chain_store_update = self.store_update();
            chain_store_update.update_fork_tail(gc_stop_height);
            chain_store_update.commit()?;
            fork_tail = gc_stop_height;
        }
        let mut gc_blocks_remaining = gc_config.gc_blocks_limit;

        // Forks Cleaning
        let gc_fork_clean_step = gc_config.gc_fork_clean_step;
        let stop_height = tail.max(fork_tail.saturating_sub(gc_fork_clean_step));
        for height in (stop_height..fork_tail).rev() {
            self.clear_forks_data(
                tries.clone(),
                height,
                &mut gc_blocks_remaining,
                epoch_manager.clone(),
            )?;
            if gc_blocks_remaining == 0 {
                return Ok(());
            }
            let mut chain_store_update = self.store_update();
            chain_store_update.update_fork_tail(height);
            chain_store_update.commit()?;
        }

        // Canonical Chain Clearing
        for height in tail + 1..gc_stop_height {
            if gc_blocks_remaining == 0 {
                return Ok(());
            }
            let blocks_current_height = self
                .chain_store()
                .get_all_block_hashes_by_height(height)?
                .values()
                .flatten()
                .cloned()
                .collect::<Vec<_>>();
            let epoch_manager = epoch_manager.clone();
            let mut chain_store_update = self.store_update();
            if let Some(block_hash) = blocks_current_height.first() {
                let prev_hash = *chain_store_update.get_block_header(block_hash)?.prev_hash();
                let prev_block_refcount = chain_store_update.get_block_refcount(&prev_hash)?;
                if prev_block_refcount > 1 {
                    // Block of `prev_hash` starts a Fork, stopping
                    break;
                } else if prev_block_refcount == 1 {
                    debug_assert_eq!(blocks_current_height.len(), 1);
                    chain_store_update.clear_block_data(
                        epoch_manager.as_ref(),
                        *block_hash,
                        GCMode::Canonical(tries.clone()),
                    )?;
                    gc_blocks_remaining -= 1;
                } else {
                    return Err(Error::GCError(
                        "block on canonical chain shouldn't have refcount 0".into(),
                    ));
                }
            }
            chain_store_update.update_tail(height)?;
            chain_store_update.commit()?;
        }
        Ok(())
    }

    /// Garbage collect data which archival node doesn’t need to keep.
    ///
    /// Normally, archival nodes keep all the data from the genesis block and
    /// don’t run garbage collection.  On the other hand, for better performance
    /// the storage contains some data duplication, i.e. values in some of the
    /// columns can be recomputed from data in different columns.  To save on
    /// storage, archival nodes do garbage collect that data.
    ///
    /// `gc_height_limit` limits how many heights will the function process.
    pub fn clear_archive_data(
        &mut self,
        gc_height_limit: BlockHeightDelta,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
    ) -> Result<(), Error> {
        let _span = tracing::debug_span!(target: "chain", "clear_archive_data").entered();

        let head = self.head()?;
        let gc_stop_height = runtime_adapter.get_gc_stop_height(&head.last_block_hash);
        if gc_stop_height > head.height {
            return Err(Error::GCError("gc_stop_height cannot be larger than head.height".into()));
        }

        let mut chain_store_update = self.store_update();
        chain_store_update.clear_redundant_chunk_data(gc_stop_height, gc_height_limit)?;
        metrics::CHUNK_TAIL_HEIGHT.set(chain_store_update.chunk_tail()? as i64);
        metrics::GC_STOP_HEIGHT.set(gc_stop_height as i64);
        chain_store_update.commit()
    }

    fn clear_forks_data(
        &mut self,
        tries: ShardTries,
        height: BlockHeight,
        gc_blocks_remaining: &mut NumBlocks,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
    ) -> Result<(), Error> {
        let blocks_current_height = self
            .chain_store()
            .get_all_block_hashes_by_height(height)?
            .values()
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        for block_hash in blocks_current_height.iter() {
            let mut current_hash = *block_hash;
            loop {
                if *gc_blocks_remaining == 0 {
                    return Ok(());
                }
                // Block `block_hash` is not on the Canonical Chain
                // because shorter chain cannot be Canonical one
                // and it may be safely deleted
                // and all its ancestors while there are no other sibling blocks rely on it.
                let epoch_manager = epoch_manager.clone();
                let mut chain_store_update = self.store_update();
                if chain_store_update.get_block_refcount(&current_hash)? == 0 {
                    let prev_hash =
                        *chain_store_update.get_block_header(&current_hash)?.prev_hash();

                    // It's safe to call `clear_block_data` for prev data because it clears fork only here
                    chain_store_update.clear_block_data(
                        epoch_manager.as_ref(),
                        current_hash,
                        GCMode::Fork(tries.clone()),
                    )?;
                    chain_store_update.commit()?;
                    *gc_blocks_remaining -= 1;

                    current_hash = prev_hash;
                } else {
                    // Block of `current_hash` is an ancestor for some other blocks, stopping
                    break;
                }
            }
        }

        Ok(())
    }

    pub fn reset_data_pre_state_sync(
        &mut self,
        sync_hash: CryptoHash,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
    ) -> Result<(), Error> {
        let _span = tracing::debug_span!(target: "sync", "reset_data_pre_state_sync").entered();
        let head = self.head()?;
        if head.prev_block_hash == CryptoHash::default() {
            // This is genesis. It means we are state syncing right after epoch sync. Don't clear
            // anything at genesis, or else the node will never boot up again.
            return Ok(());
        }
        // Get header we were syncing into.
        let header = self.get_block_header(&sync_hash)?;
        let prev_hash = *header.prev_hash();
        let sync_height = header.height();
        // The congestion control added a dependency on the prev block when
        // applying chunks in a block. This means that we need to keep the
        // blocks at sync hash, prev hash and prev prev hash. The heigh of that
        // block is sync_height - 2.
        let mut gc_height = std::cmp::min(head.height + 1, sync_height - 2);

        // In case there are missing chunks we need to keep more than just the
        // sync hash block. The logic below adjusts the gc_height so that every
        // shard is guaranteed to have at least one new chunk in the blocks
        // leading to the sync hash block.
        let prev_block = self.get_block(&prev_hash);
        if let Ok(prev_block) = prev_block {
            let min_height_included =
                prev_block.chunks().iter().map(|chunk| chunk.height_included()).min();
            if let Some(min_height_included) = min_height_included {
                tracing::debug!(target: "sync", ?min_height_included, ?gc_height, "adjusting gc_height for missing chunks");
                gc_height = std::cmp::min(gc_height, min_height_included - 1);
            };
        }

        // GC all the data from current tail up to `gc_height`. In case tail points to a height where
        // there is no block, we need to make sure that the last block before tail is cleaned.
        let tail = self.chain_store().tail()?;
        let mut tail_prev_block_cleaned = false;
        for height in tail..gc_height {
            let blocks_current_height = self
                .chain_store()
                .get_all_block_hashes_by_height(height)?
                .values()
                .flatten()
                .cloned()
                .collect::<Vec<_>>();
            for block_hash in blocks_current_height {
                let epoch_manager = epoch_manager.clone();
                let mut chain_store_update = self.store_update();
                if !tail_prev_block_cleaned {
                    let prev_block_hash =
                        *chain_store_update.get_block_header(&block_hash)?.prev_hash();
                    if chain_store_update.get_block(&prev_block_hash).is_ok() {
                        chain_store_update.clear_block_data(
                            epoch_manager.as_ref(),
                            prev_block_hash,
                            GCMode::StateSync { clear_block_info: true },
                        )?;
                    }
                    tail_prev_block_cleaned = true;
                }
                chain_store_update.clear_block_data(
                    epoch_manager.as_ref(),
                    block_hash,
                    GCMode::StateSync { clear_block_info: block_hash != prev_hash },
                )?;
                chain_store_update.commit()?;
            }
        }

        // Clear Chunks data
        let mut chain_store_update = self.store_update();
        // The largest height of chunk we have in storage is head.height + 1
        let chunk_height = std::cmp::min(head.height + 2, sync_height);
        chain_store_update.clear_chunk_data_and_headers(chunk_height)?;
        chain_store_update.commit()?;

        // clear all trie data

        let tries = runtime_adapter.get_tries();
        let mut chain_store_update = self.store_update();
        let mut store_update = tries.store_update();
        store_update.delete_all(DBCol::State);
        chain_store_update.merge(store_update);

        // The reason to reset tail here is not to allow Tail be greater than Head
        chain_store_update.reset_tail();
        chain_store_update.commit()?;
        Ok(())
    }
}

impl<'a> ChainStoreUpdate<'a> {
    fn clear_header_data_for_heights(
        &mut self,
        start: BlockHeight,
        end: BlockHeight,
    ) -> Result<(), Error> {
        for height in start..=end {
            let header_hashes = self.chain_store().get_all_header_hashes_by_height(height)?;
            for header_hash in header_hashes {
                // Delete header_hash-indexed data: block header
                let mut store_update = self.store().store_update();
                let key: &[u8] = header_hash.as_bytes();
                store_update.delete(DBCol::BlockHeader, key);
                self.chain_store().headers.pop(key);
                self.merge(store_update);
            }
            let key = index_to_bytes(height);
            self.gc_col(DBCol::HeaderHashesByHeight, &key);
        }
        Ok(())
    }

    fn clear_chunk_data_and_headers(&mut self, min_chunk_height: BlockHeight) -> Result<(), Error> {
        let chunk_tail = self.chunk_tail()?;
        for height in chunk_tail..min_chunk_height {
            let chunk_hashes = self.chain_store().get_all_chunk_hashes_by_height(height)?;
            for chunk_hash in chunk_hashes {
                // 1. Delete chunk-related data
                let chunk = self.get_chunk(&chunk_hash)?.clone();
                debug_assert_eq!(chunk.cloned_header().height_created(), height);
                for transaction in chunk.transactions() {
                    self.gc_col(DBCol::Transactions, transaction.get_hash().as_bytes());
                }
                for receipt in chunk.prev_outgoing_receipts() {
                    self.gc_col(DBCol::Receipts, receipt.get_hash().as_bytes());
                }

                // 2. Delete chunk_hash-indexed data
                let chunk_hash = chunk_hash.as_bytes();
                self.gc_col(DBCol::Chunks, chunk_hash);
                self.gc_col(DBCol::PartialChunks, chunk_hash);
                self.gc_col(DBCol::InvalidChunks, chunk_hash);
            }

            let header_hashes = self.chain_store().get_all_header_hashes_by_height(height)?;
            for _header_hash in header_hashes {
                // 3. Delete header_hash-indexed data
                // TODO #3488: enable
                //self.gc_col(DBCol::BlockHeader, header_hash.as_bytes());
            }

            // 4. Delete chunks_tail-related data
            let key = index_to_bytes(height);
            self.gc_col(DBCol::ChunkHashesByHeight, &key);
            self.gc_col(DBCol::HeaderHashesByHeight, &key);
        }
        self.update_chunk_tail(min_chunk_height);
        Ok(())
    }

    /// Clears chunk data which can be computed from other data in the storage.
    ///
    /// We are storing PartialEncodedChunk objects in the DBCol::PartialChunks in
    /// the storage.  However, those objects can be computed from data in
    /// DBCol::Chunks and as such are redundant.  For performance reasons we want to
    /// keep that data when operating at head of the chain but the data can be
    /// safely removed from archival storage.
    ///
    /// `gc_stop_height` indicates height starting from which no data should be
    /// garbage collected.  Roughly speaking this represents start of the ‘hot’
    /// data that we want to keep.
    ///
    /// `gt_height_limit` indicates limit of how many non-empty heights to
    /// process.  This limit means that the method may stop garbage collection
    /// before reaching `gc_stop_height`.
    fn clear_redundant_chunk_data(
        &mut self,
        gc_stop_height: BlockHeight,
        gc_height_limit: BlockHeightDelta,
    ) -> Result<(), Error> {
        let mut height = self.chunk_tail()?;
        let mut remaining = gc_height_limit;
        while height < gc_stop_height && remaining > 0 {
            let chunk_hashes = self.chain_store().get_all_chunk_hashes_by_height(height)?;
            height += 1;
            if !chunk_hashes.is_empty() {
                remaining -= 1;
                for chunk_hash in chunk_hashes {
                    let chunk_hash = chunk_hash.as_bytes();
                    self.gc_col(DBCol::PartialChunks, chunk_hash);
                    // Data in DBCol::InvalidChunks isn’t technically redundant (it
                    // cannot be calculated from other data) but it is data we
                    // don’t need for anything so it can be deleted as well.
                    self.gc_col(DBCol::InvalidChunks, chunk_hash);
                }
            }
        }
        self.update_chunk_tail(height);
        Ok(())
    }

    fn get_shard_uids_to_gc(
        &mut self,
        epoch_manager: &dyn EpochManagerAdapter,
        block_hash: &CryptoHash,
    ) -> Vec<ShardUId> {
        let block_header = self.get_block_header(block_hash).expect("block header must exist");
        let shard_layout =
            epoch_manager.get_shard_layout(block_header.epoch_id()).expect("epoch info must exist");
        // gc shards in this epoch
        let mut shard_uids_to_gc: Vec<_> = shard_layout.shard_uids().collect();
        // gc shards in the shard layout in the next epoch if shards will change in the next epoch
        // Suppose shard changes at epoch T, we need to garbage collect the new shard layout
        // from the last block in epoch T-2 to the last block in epoch T-1
        // Because we need to gc the last block in epoch T-2, we can't simply use
        // block_header.epoch_id() as next_epoch_id
        let next_epoch_id = block_header.next_epoch_id();
        let next_shard_layout =
            epoch_manager.get_shard_layout(next_epoch_id).expect("epoch info must exist");
        if shard_layout != next_shard_layout {
            shard_uids_to_gc.extend(next_shard_layout.shard_uids());
        }
        shard_uids_to_gc
    }

    // Clearing block data of `block_hash`, if on a fork.
    // Clearing block data of `block_hash.prev`, if on the Canonical Chain.
    pub fn clear_block_data(
        &mut self,
        epoch_manager: &dyn EpochManagerAdapter,
        mut block_hash: CryptoHash,
        gc_mode: GCMode,
    ) -> Result<(), Error> {
        let mut store_update = self.store().store_update();

        tracing::debug!(target: "garbage_collection", ?gc_mode, ?block_hash, "GC block_hash");

        // 1. Apply revert insertions or deletions from DBCol::TrieChanges for Trie
        {
            let shard_uids_to_gc: Vec<_> = self.get_shard_uids_to_gc(epoch_manager, &block_hash);
            match gc_mode.clone() {
                GCMode::Fork(tries) => {
                    // If the block is on a fork, we delete the state that's the result of applying this block
                    for shard_uid in shard_uids_to_gc {
                        let trie_changes = self.store().get_ser(
                            DBCol::TrieChanges,
                            &get_block_shard_uid(&block_hash, &shard_uid),
                        )?;
                        if let Some(trie_changes) = trie_changes {
                            tries.revert_insertions(&trie_changes, shard_uid, &mut store_update);
                            self.gc_col(
                                DBCol::TrieChanges,
                                &get_block_shard_uid(&block_hash, &shard_uid),
                            );
                        }
                    }
                }
                GCMode::Canonical(tries) => {
                    // If the block is on canonical chain, we delete the state that's before applying this block
                    for shard_uid in shard_uids_to_gc {
                        let trie_changes = self.store().get_ser(
                            DBCol::TrieChanges,
                            &get_block_shard_uid(&block_hash, &shard_uid),
                        )?;
                        if let Some(trie_changes) = trie_changes {
                            tries.apply_deletions(&trie_changes, shard_uid, &mut store_update);
                            self.gc_col(
                                DBCol::TrieChanges,
                                &get_block_shard_uid(&block_hash, &shard_uid),
                            );
                        }
                    }
                    // Set `block_hash` on previous one
                    block_hash = *self.get_block_header(&block_hash)?.prev_hash();
                }
                GCMode::StateSync { .. } => {
                    // Not apply the data from DBCol::TrieChanges
                    for shard_uid in shard_uids_to_gc {
                        self.gc_col(
                            DBCol::TrieChanges,
                            &get_block_shard_uid(&block_hash, &shard_uid),
                        );
                    }
                }
            }
        }

        let block =
            self.get_block(&block_hash).expect("block data is not expected to be already cleaned");
        let height = block.header().height();

        // 2. Delete shard_id-indexed data (Receipts, State Headers and Parts, etc.)
        for shard_id in 0..block.header().chunk_mask().len() as ShardId {
            let block_shard_id = get_block_shard_id(&block_hash, shard_id);
            self.gc_outgoing_receipts(&block_hash, shard_id);
            self.gc_col(DBCol::IncomingReceipts, &block_shard_id);
            self.gc_col(DBCol::StateTransitionData, &block_shard_id);

            // For incoming State Parts it's done in chain.clear_downloaded_parts()
            // The following code is mostly for outgoing State Parts.
            // However, if node crashes while State Syncing, it may never clear
            // downloaded State parts in `clear_downloaded_parts`.
            // We need to make sure all State Parts are removed.
            if let Ok(shard_state_header) =
                self.chain_store().get_state_header(shard_id, block_hash)
            {
                let state_num_parts = shard_state_header.num_state_parts();
                self.gc_col_state_parts(block_hash, shard_id, state_num_parts)?;
                let key = borsh::to_vec(&StateHeaderKey(shard_id, block_hash))?;
                self.gc_col(DBCol::StateHeaders, &key);
            }
        }
        // gc DBCol::ChunkExtra based on shard_uid since it's indexed by shard_uid in the storage
        for shard_uid in self.get_shard_uids_to_gc(epoch_manager, &block_hash) {
            let block_shard_uid = get_block_shard_uid(&block_hash, &shard_uid);
            self.gc_col(DBCol::ChunkExtra, &block_shard_uid);
        }

        // 3. Delete block_hash-indexed data
        self.gc_col(DBCol::Block, block_hash.as_bytes());
        self.gc_col(DBCol::BlockExtra, block_hash.as_bytes());
        self.gc_col(DBCol::NextBlockHashes, block_hash.as_bytes());
        self.gc_col(DBCol::ChallengedBlocks, block_hash.as_bytes());
        self.gc_col(DBCol::BlocksToCatchup, block_hash.as_bytes());
        let storage_key = KeyForStateChanges::for_block(&block_hash);
        let stored_state_changes: Vec<Box<[u8]>> = self
            .store()
            .iter_prefix(DBCol::StateChanges, storage_key.as_ref())
            .map(|item| item.map(|(key, _)| key))
            .collect::<io::Result<Vec<_>>>()?;
        for key in stored_state_changes {
            self.gc_col(DBCol::StateChanges, &key);
        }
        self.gc_col(DBCol::BlockRefCount, block_hash.as_bytes());
        self.gc_outcomes(&block)?;
        match gc_mode {
            GCMode::StateSync { clear_block_info: false } => {}
            _ => self.gc_col(DBCol::BlockInfo, block_hash.as_bytes()),
        }
        self.gc_col(DBCol::StateDlInfos, block_hash.as_bytes());

        // 4. Update or delete block_hash_per_height
        self.gc_col_block_per_height(&block_hash, height, block.header().epoch_id())?;

        match gc_mode {
            GCMode::Fork(_) => {
                // 5. Forks only clearing
                self.dec_block_refcount(block.header().prev_hash())?;
            }
            GCMode::Canonical(_) => {
                // 6. Canonical Chain only clearing
                // Delete chunks, chunk-indexed data and block headers
                let mut min_chunk_height = self.tail()?;
                for chunk_header in block.chunks().iter() {
                    if min_chunk_height > chunk_header.height_created() {
                        min_chunk_height = chunk_header.height_created();
                    }
                }
                self.clear_chunk_data_and_headers(min_chunk_height)?;
            }
            GCMode::StateSync { .. } => {
                // 7. State Sync clearing
                // Chunks deleted separately
            }
        };
        self.merge(store_update);
        Ok(())
    }

    // Delete all data in rocksdb that are partially or wholly indexed and can be looked up by hash of the current head of the chain
    // and that indicates a link between current head and its prev block
    pub fn clear_head_block_data(
        &mut self,
        epoch_manager: &dyn EpochManagerAdapter,
    ) -> Result<(), Error> {
        let header_head = self.header_head().unwrap();
        let header_head_height = header_head.height;
        let block_hash = self.head().unwrap().last_block_hash;

        let block =
            self.get_block(&block_hash).expect("block data is not expected to be already cleaned");

        let epoch_id = block.header().epoch_id();

        let head_height = block.header().height();

        // 1. Delete shard_id-indexed data (TrieChanges, Receipts, ChunkExtra, State Headers and Parts, FlatStorage data)
        for shard_id in 0..block.header().chunk_mask().len() as ShardId {
            let shard_uid = epoch_manager.shard_id_to_uid(shard_id, epoch_id).unwrap();
            let block_shard_id = get_block_shard_uid(&block_hash, &shard_uid);

            // delete TrieChanges
            self.gc_col(DBCol::TrieChanges, &block_shard_id);

            // delete Receipts
            self.gc_outgoing_receipts(&block_hash, shard_id);
            self.gc_col(DBCol::IncomingReceipts, &block_shard_id);

            self.gc_col(DBCol::StateTransitionData, &block_shard_id);

            // delete DBCol::ChunkExtra based on shard_uid since it's indexed by shard_uid in the storage
            self.gc_col(DBCol::ChunkExtra, &block_shard_id);

            // delete state parts and state headers
            if let Ok(shard_state_header) =
                self.chain_store().get_state_header(shard_id, block_hash)
            {
                let state_num_parts = shard_state_header.num_state_parts();
                self.gc_col_state_parts(block_hash, shard_id, state_num_parts)?;
                let state_header_key = borsh::to_vec(&StateHeaderKey(shard_id, block_hash))?;
                self.gc_col(DBCol::StateHeaders, &state_header_key);
            }

            // delete flat storage columns: FlatStateChanges and FlatStateDeltaMetadata
            let mut store_update = self.store().store_update().flat_store_update();
            store_update.remove_delta(shard_uid, block_hash);
            self.merge(store_update.store_update());
        }

        // 2. Delete block_hash-indexed data
        self.gc_col(DBCol::Block, block_hash.as_bytes());
        self.gc_col(DBCol::BlockExtra, block_hash.as_bytes());
        self.gc_col(DBCol::NextBlockHashes, block_hash.as_bytes());
        self.gc_col(DBCol::ChallengedBlocks, block_hash.as_bytes());
        self.gc_col(DBCol::BlocksToCatchup, block_hash.as_bytes());
        let storage_key = KeyForStateChanges::for_block(&block_hash);
        let stored_state_changes: Vec<Box<[u8]>> = self
            .store()
            .iter_prefix(DBCol::StateChanges, storage_key.as_ref())
            .map(|item| item.map(|(key, _)| key))
            .collect::<io::Result<Vec<_>>>()?;
        for key in stored_state_changes {
            self.gc_col(DBCol::StateChanges, &key);
        }
        self.gc_col(DBCol::BlockRefCount, block_hash.as_bytes());
        self.gc_outcomes(&block)?;
        self.gc_col(DBCol::BlockInfo, block_hash.as_bytes());
        self.gc_col(DBCol::StateDlInfos, block_hash.as_bytes());

        // 3. update columns related to prev block (block refcount and NextBlockHashes)
        self.dec_block_refcount(block.header().prev_hash())?;
        self.gc_col(DBCol::NextBlockHashes, block.header().prev_hash().as_bytes());

        // 4. Update or delete block_hash_per_height
        self.gc_col_block_per_height(&block_hash, head_height, block.header().epoch_id())?;

        self.clear_chunk_data_at_height(head_height)?;

        self.clear_header_data_for_heights(head_height, header_head_height)?;

        Ok(())
    }

    fn clear_chunk_data_at_height(&mut self, height: BlockHeight) -> Result<(), Error> {
        let chunk_hashes = self.chain_store().get_all_chunk_hashes_by_height(height)?;
        for chunk_hash in chunk_hashes {
            // 1. Delete chunk-related data
            let chunk = self.get_chunk(&chunk_hash)?.clone();
            debug_assert_eq!(chunk.cloned_header().height_created(), height);
            for transaction in chunk.transactions() {
                self.gc_col(DBCol::Transactions, transaction.get_hash().as_bytes());
            }
            for receipt in chunk.prev_outgoing_receipts() {
                self.gc_col(DBCol::Receipts, receipt.get_hash().as_bytes());
            }

            // 2. Delete chunk_hash-indexed data
            let chunk_hash = chunk_hash.as_bytes();
            self.gc_col(DBCol::Chunks, chunk_hash);
            self.gc_col(DBCol::PartialChunks, chunk_hash);
            self.gc_col(DBCol::InvalidChunks, chunk_hash);
        }

        // 4. Delete chunk hashes per height
        let key = index_to_bytes(height);
        self.gc_col(DBCol::ChunkHashesByHeight, &key);

        Ok(())
    }

    fn gc_col_block_per_height(
        &mut self,
        block_hash: &CryptoHash,
        height: BlockHeight,
        epoch_id: &EpochId,
    ) -> Result<(), Error> {
        let mut store_update = self.store().store_update();
        let mut epoch_to_hashes =
            HashMap::clone(self.chain_store().get_all_block_hashes_by_height(height)?.as_ref());
        let hashes = epoch_to_hashes.get_mut(epoch_id).ok_or_else(|| {
            near_chain_primitives::Error::Other("current epoch id should exist".into())
        })?;
        hashes.remove(block_hash);
        if hashes.is_empty() {
            epoch_to_hashes.remove(epoch_id);
        }
        let key = &index_to_bytes(height)[..];
        if epoch_to_hashes.is_empty() {
            store_update.delete(DBCol::BlockPerHeight, key);
            self.chain_store().block_hash_per_height.pop(key);
        } else {
            store_update.set_ser(DBCol::BlockPerHeight, key, &epoch_to_hashes)?;
            self.chain_store().block_hash_per_height.put(key.to_vec(), Arc::new(epoch_to_hashes));
        }
        if self.is_height_processed(height)? {
            self.gc_col(DBCol::ProcessedBlockHeights, key);
        }
        self.merge(store_update);
        Ok(())
    }

    pub fn gc_col_state_parts(
        &mut self,
        sync_hash: CryptoHash,
        shard_id: ShardId,
        num_parts: u64,
    ) -> Result<(), Error> {
        for part_id in 0..num_parts {
            let key = borsh::to_vec(&StatePartKey(sync_hash, shard_id, part_id))?;
            self.gc_col(DBCol::StateParts, &key);
        }
        Ok(())
    }

    fn gc_outgoing_receipts(&mut self, block_hash: &CryptoHash, shard_id: ShardId) {
        let mut store_update = self.store().store_update();
        let key = get_block_shard_id(block_hash, shard_id);
        store_update.delete(DBCol::OutgoingReceipts, &key);
        self.chain_store().outgoing_receipts.pop(&key);
        self.merge(store_update);
    }

    fn gc_outcomes(&mut self, block: &Block) -> Result<(), Error> {
        let block_hash = block.hash();
        let store_update = self.store().store_update();
        for chunk_header in
            block.chunks().iter().filter(|h| h.height_included() == block.header().height())
        {
            let shard_id = chunk_header.shard_id();
            let outcome_ids =
                self.chain_store().get_outcomes_by_block_hash_and_shard_id(block_hash, shard_id)?;
            for outcome_id in outcome_ids {
                self.gc_col(
                    DBCol::TransactionResultForBlock,
                    &get_outcome_id_block_hash(&outcome_id, block_hash),
                );
            }
            self.gc_col(DBCol::OutcomeIds, &get_block_shard_id(block_hash, shard_id));
        }
        self.merge(store_update);
        Ok(())
    }

    fn gc_col(&mut self, col: DBCol, key: &[u8]) {
        let mut store_update = self.store().store_update();
        match col {
            DBCol::OutgoingReceipts => {
                panic!("Outgoing receipts must be garbage collected by calling gc_outgoing_receipts");
            }
            DBCol::IncomingReceipts => {
                store_update.delete(col, key);
                self.chain_store().incoming_receipts.pop(key);
            }
            DBCol::StateHeaders => {
                store_update.delete(col, key);
            }
            DBCol::BlockHeader => {
                // TODO(#3488) At the moment header sync needs block headers.
                // However, we want to eventually garbage collect headers.
                // When that happens we should make sure that block headers is
                // copied to the cold storage.
                store_update.delete(col, key);
                self.chain_store().headers.pop(key);
                unreachable!();
            }
            DBCol::Block => {
                store_update.delete(col, key);
                self.chain_store().blocks.pop(key);
            }
            DBCol::BlockExtra => {
                store_update.delete(col, key);
                self.chain_store().block_extras.pop(key);
            }
            DBCol::NextBlockHashes => {
                store_update.delete(col, key);
                self.chain_store().next_block_hashes.pop(key);
            }
            DBCol::ChallengedBlocks => {
                store_update.delete(col, key);
            }
            DBCol::BlocksToCatchup => {
                store_update.delete(col, key);
            }
            DBCol::StateChanges => {
                store_update.delete(col, key);
            }
            DBCol::BlockRefCount => {
                store_update.delete(col, key);
                self.chain_store().block_refcounts.pop(key);
            }
            DBCol::Transactions => {
                store_update.decrement_refcount(col, key);
                self.chain_store().transactions.pop(key);
            }
            DBCol::Receipts => {
                store_update.decrement_refcount(col, key);
                self.chain_store().receipts.pop(key);
            }
            DBCol::Chunks => {
                store_update.delete(col, key);
                self.chain_store().chunks.pop(key);
            }
            DBCol::ChunkExtra => {
                store_update.delete(col, key);
                self.chain_store().chunk_extras.pop(key);
            }
            DBCol::PartialChunks => {
                store_update.delete(col, key);
                self.chain_store().partial_chunks.pop(key);
            }
            DBCol::InvalidChunks => {
                store_update.delete(col, key);
                self.chain_store().invalid_chunks.pop(key);
            }
            DBCol::ChunkHashesByHeight => {
                store_update.delete(col, key);
            }
            DBCol::StateParts => {
                store_update.delete(col, key);
            }
            DBCol::State => {
                panic!("Actual gc happens elsewhere, call inc_gc_col_state to increase gc count");
            }
            DBCol::TrieChanges => {
                store_update.delete(col, key);
            }
            DBCol::BlockPerHeight => {
                panic!("Must use gc_col_glock_per_height method to gc DBCol::BlockPerHeight");
            }
            DBCol::TransactionResultForBlock => {
                store_update.delete(col, key);
            }
            DBCol::OutcomeIds => {
                store_update.delete(col, key);
            }
            DBCol::StateDlInfos => {
                store_update.delete(col, key);
            }
            DBCol::BlockInfo => {
                store_update.delete(col, key);
            }
            DBCol::ProcessedBlockHeights => {
                store_update.delete(col, key);
                self.chain_store().processed_block_heights.pop(key);
            }
            DBCol::HeaderHashesByHeight => {
                store_update.delete(col, key);
            }
            DBCol::StateTransitionData => {
                store_update.delete(col, key);
            }
            DBCol::LatestChunkStateWitnesses => {
                store_update.delete(col, key);
            }
            DBCol::LatestWitnessesByIndex => {
                store_update.delete(col, key);
            }
            DBCol::DbVersion
            | DBCol::BlockMisc
            | DBCol::_GCCount
            | DBCol::BlockHeight  // block sync needs it + genesis should be accessible
            | DBCol::_Peers
            | DBCol::RecentOutboundConnections
            | DBCol::BlockMerkleTree
            | DBCol::AccountAnnouncements
            | DBCol::EpochLightClientBlocks
            | DBCol::PeerComponent
            | DBCol::LastComponentNonce
            | DBCol::ComponentEdges
            // https://github.com/nearprotocol/nearcore/pull/2952
            | DBCol::EpochInfo
            | DBCol::EpochStart
            | DBCol::EpochValidatorInfo
            | DBCol::BlockOrdinal
            | DBCol::_ChunkPerHeightShard
            | DBCol::_NextBlockWithNewChunk
            | DBCol::_LastBlockWithNewChunk
            | DBCol::_TransactionRefCount
            | DBCol::_TransactionResult
            | DBCol::StateChangesForSplitStates
            | DBCol::CachedContractCode
            | DBCol::FlatState
            | DBCol::FlatStateChanges
            | DBCol::FlatStateDeltaMetadata
            | DBCol::FlatStorageStatus
            | DBCol::Misc
            | DBCol::_ReceiptIdToShardId
            => unreachable!(),
        }
        self.merge(store_update);
    }
}

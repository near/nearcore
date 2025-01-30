use std::cell::RefCell;
use std::io;
use std::sync::Arc;

use super::event_type::{ReshardingEventType, ReshardingSplitShardParams};
use super::types::ReshardingSender;
use crate::flat_storage_resharder::{FlatStorageResharder, FlatStorageResharderController};
use crate::types::RuntimeAdapter;
use crate::ChainStoreUpdate;
use itertools::Itertools;
use near_chain_configs::{MutableConfigValue, ReshardingConfig, ReshardingHandle};
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::Block;
use near_primitives::congestion_info::CongestionInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{get_block_shard_uid, ShardLayout};
use near_primitives::state::PartialState;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_store::adapter::trie_store::get_shard_uid_mapping;
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use near_store::flat::BlockInfo;
use near_store::trie::mem::memtrie_update::TrackingMode;
use near_store::trie::ops::resharding::RetainMode;
use near_store::trie::outgoing_metadata::ReceiptGroupsQueue;
use near_store::trie::TrieRecorder;
use near_store::{DBCol, ShardTries, ShardUId, Store, TrieAccess};

pub struct ReshardingManager {
    store: Store,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    /// Configuration for resharding.
    pub resharding_config: MutableConfigValue<ReshardingConfig>,
    /// A handle that allows the main process to interrupt resharding if needed.
    /// This typically happens when the main process is interrupted.
    pub resharding_handle: ReshardingHandle,
    /// Takes care of performing resharding on the flat storage.
    pub flat_storage_resharder: FlatStorageResharder,
}

impl ReshardingManager {
    pub fn new(
        store: Store,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        resharding_config: MutableConfigValue<ReshardingConfig>,
        resharding_sender: ReshardingSender,
    ) -> Self {
        let resharding_handle = ReshardingHandle::new();
        let flat_storage_resharder = FlatStorageResharder::new(
            runtime_adapter,
            resharding_sender,
            FlatStorageResharderController::from_resharding_handle(resharding_handle.clone()),
            resharding_config.clone(),
        );
        Self { store, epoch_manager, resharding_config, flat_storage_resharder, resharding_handle }
    }

    /// Trigger resharding if shard layout changes after the given block.
    pub fn start_resharding(
        &mut self,
        chain_store_update: ChainStoreUpdate,
        block: &Block,
        shard_uid: ShardUId,
        tries: ShardTries,
    ) -> Result<(), Error> {
        let block_hash = block.hash();
        let block_height = block.header().height();
        let _span = tracing::debug_span!(
            target: "resharding", "start_resharding",
            ?block_hash, block_height, ?shard_uid)
        .entered();

        let prev_hash = block.header().prev_hash();
        let shard_layout = self.epoch_manager.get_shard_layout(&block.header().epoch_id())?;
        let next_epoch_id = self.epoch_manager.get_next_epoch_id_from_prev_block(prev_hash)?;
        let next_shard_layout = self.epoch_manager.get_shard_layout(&next_epoch_id)?;

        let is_next_block_epoch_start = self.epoch_manager.is_next_block_epoch_start(block_hash)?;
        if !is_next_block_epoch_start {
            return Ok(());
        }

        let will_shard_layout_change = shard_layout != next_shard_layout;
        if !will_shard_layout_change {
            tracing::debug!(target: "resharding", ?prev_hash, "prev block has the same shard layout, skipping");
            return Ok(());
        }

        if !matches!(next_shard_layout, ShardLayout::V2(_)) {
            tracing::debug!(target: "resharding", ?next_shard_layout, "next shard layout is not v2, skipping");
            return Ok(());
        }

        let block_info = BlockInfo {
            hash: *block.hash(),
            height: block.header().height(),
            prev_hash: *block.header().prev_hash(),
        };
        let resharding_event_type =
            ReshardingEventType::from_shard_layout(&next_shard_layout, block_info)?;
        match resharding_event_type {
            Some(ReshardingEventType::SplitShard(split_shard_event)) => {
                self.split_shard(
                    chain_store_update,
                    block,
                    shard_uid,
                    tries,
                    split_shard_event,
                    next_shard_layout,
                )?;
            }
            None => {
                tracing::warn!(target: "resharding", ?resharding_event_type, "unsupported resharding event type, skipping");
            }
        };
        Ok(())
    }

    fn split_shard(
        &mut self,
        chain_store_update: ChainStoreUpdate,
        block: &Block,
        shard_uid: ShardUId,
        tries: ShardTries,
        split_shard_event: ReshardingSplitShardParams,
        next_shard_layout: ShardLayout,
    ) -> Result<(), Error> {
        if split_shard_event.parent_shard != shard_uid {
            let parent_shard = split_shard_event.parent_shard;
            tracing::debug!(target: "resharding", ?parent_shard, "ShardUId does not match event parent shard, skipping");
            return Ok(());
        }

        // Reshard the State column by setting ShardUId mapping from children to ancestor.
        self.set_state_shard_uid_mapping(&split_shard_event)?;

        // Create temporary children memtries by freezing parent memtrie and referencing it.
        self.process_memtrie_resharding_storage_update(
            chain_store_update,
            block,
            shard_uid,
            tries,
            split_shard_event.clone(),
        )?;

        // Trigger resharding of flat storage.
        self.flat_storage_resharder.start_resharding(
            ReshardingEventType::SplitShard(split_shard_event),
            &next_shard_layout,
        )?;

        Ok(())
    }

    /// Store in the database the mapping of ShardUId from children to the parent shard,
    /// so that subsequent accesses to the State will use the ancestor's ShardUId prefix
    /// as a prefix for the database key.
    // TODO(resharding) add testloop where grandparent ShardUId is used
    fn set_state_shard_uid_mapping(
        &mut self,
        split_shard_event: &ReshardingSplitShardParams,
    ) -> io::Result<()> {
        let mut store_update = self.store.trie_store().store_update();
        let parent_shard_uid = split_shard_event.parent_shard;
        let parent_shard_uid_prefix = get_shard_uid_mapping(&self.store, parent_shard_uid);
        // TODO(resharding) No need to set the mapping for children shards that we won't track just after resharding?
        for child_shard_uid in split_shard_event.children_shards() {
            store_update.set_shard_uid_mapping(child_shard_uid, parent_shard_uid_prefix);
        }
        store_update.commit()
    }

    /// Creates temporary memtries for new shards to be able to process them in the next epoch.
    /// Note this doesn't complete memtries resharding, proper memtries are to be created later.
    fn process_memtrie_resharding_storage_update(
        &mut self,
        mut chain_store_update: ChainStoreUpdate,
        block: &Block,
        parent_shard_uid: ShardUId,
        tries: ShardTries,
        split_shard_event: ReshardingSplitShardParams,
    ) -> Result<(), Error> {
        let block_hash = block.hash();
        let block_height = block.header().height();
        let _span = tracing::debug_span!(
            target: "resharding", "process_memtrie_resharding_storage_update",
            ?block_hash, block_height, ?parent_shard_uid)
        .entered();

        tries.freeze_parent_memtrie(parent_shard_uid, split_shard_event.children_shards())?;

        let parent_chunk_extra = self.get_chunk_extra(block_hash, &parent_shard_uid)?;
        let boundary_account = split_shard_event.boundary_account;

        let mut trie_store_update = self.store.store_update();

        // TODO(resharding): leave only tracked shards.
        for (new_shard_uid, retain_mode) in [
            (split_shard_event.left_child_shard, RetainMode::Left),
            (split_shard_event.right_child_shard, RetainMode::Right),
        ] {
            let Some(memtries) = tries.get_memtries(new_shard_uid) else {
                tracing::error!(
                    "Memtrie not loaded. Cannot process memtrie resharding storage
                     update for block {:?}, shard {:?}",
                    block_hash,
                    parent_shard_uid,
                );
                return Err(Error::Other("Memtrie not loaded".to_string()));
            };

            tracing::info!(
                target: "resharding", ?new_shard_uid, ?retain_mode,
                "Creating child memtrie by retaining nodes in parent memtrie..."
            );
            let mut memtries = memtries.write().unwrap();
            let mut trie_recorder = TrieRecorder::new(None);
            let mode = TrackingMode::RefcountsAndAccesses(&mut trie_recorder);
            let memtrie_update = memtries.update(*parent_chunk_extra.state_root(), mode)?;

            let trie_changes = memtrie_update.retain_split_shard(&boundary_account, retain_mode);
            let memtrie_changes = trie_changes.memtrie_changes.as_ref().unwrap();
            let new_state_root = memtries.apply_memtrie_changes(block_height, memtrie_changes);
            drop(memtries);

            // Get the congestion info for the child.
            let parent_epoch_id = block.header().epoch_id();
            let parent_shard_layout = self.epoch_manager.get_shard_layout(&parent_epoch_id)?;
            let parent_state_root = *parent_chunk_extra.state_root();
            let parent_trie = tries.get_trie_for_shard(parent_shard_uid, parent_state_root);
            let parent_congestion_info =
                parent_chunk_extra.congestion_info().expect("The congestion info must exist!");

            let trie_recorder = RefCell::new(trie_recorder);
            let parent_trie = parent_trie.recording_reads_with_recorder(trie_recorder);

            let child_epoch_id = self.epoch_manager.get_next_epoch_id(block.hash())?;
            let child_shard_layout = self.epoch_manager.get_shard_layout(&child_epoch_id)?;
            let child_congestion_info = Self::get_child_congestion_info(
                &parent_trie,
                &parent_shard_layout,
                parent_congestion_info,
                &child_shard_layout,
                new_shard_uid,
                retain_mode,
            )?;

            let trie_recorder = parent_trie.take_recorder().unwrap();
            let partial_storage = trie_recorder.borrow_mut().recorded_storage();
            let partial_state_len = match &partial_storage.nodes {
                PartialState::TrieValues(values) => values.len(),
            };

            // TODO(resharding): set all fields of `ChunkExtra`. Consider stronger
            // typing. Clarify where it should happen when `State` and
            // `FlatState` update is implemented.
            let mut child_chunk_extra = ChunkExtra::clone(&parent_chunk_extra);
            *child_chunk_extra.state_root_mut() = new_state_root;
            *child_chunk_extra.congestion_info_mut().expect("The congestion info must exist!") =
                child_congestion_info;

            chain_store_update.save_chunk_extra(block_hash, &new_shard_uid, child_chunk_extra);
            chain_store_update.save_state_transition_data(
                *block_hash,
                new_shard_uid.shard_id(),
                Some(partial_storage),
                CryptoHash::default(),
                // No contract code is accessed or deployed during resharding.
                // TODO(#11099): Confirm if sending no contracts is ok here.
                Default::default(),
            );

            // Commit `TrieChanges` directly. They are needed to serve reads of
            // new nodes from `DBCol::State` while memtrie is properly created
            // from flat storage.
            tries.apply_insertions(
                &trie_changes,
                new_shard_uid,
                &mut trie_store_update.trie_store_update(),
            );
            tracing::info!(
                target: "resharding", ?new_shard_uid, ?new_state_root, ?partial_state_len,
                "Child memtrie created"
            );
        }

        chain_store_update.merge(trie_store_update);
        chain_store_update.commit()?;

        Ok(())
    }

    pub fn get_child_congestion_info(
        parent_trie: &dyn TrieAccess,
        parent_shard_layout: &ShardLayout,
        parent_congestion_info: CongestionInfo,
        child_shard_layout: &ShardLayout,
        child_shard_uid: ShardUId,
        retain_mode: RetainMode,
    ) -> Result<CongestionInfo, Error> {
        // Get the congestion info based on the parent shard.
        let mut child_congestion_info = Self::get_child_congestion_info_not_finalized(
            parent_trie,
            &parent_shard_layout,
            parent_congestion_info,
            retain_mode,
        )?;

        // Set the allowed shard based on the child shard.
        Self::finalize_allowed_shard(
            &child_shard_layout,
            child_shard_uid,
            &mut child_congestion_info,
        )?;

        Ok(child_congestion_info)
    }

    // Get the congestion info for the child shard. The congestion info can be
    // inferred efficiently from the combination of the parent shard's
    // congestion info and the receipt group metadata, that is available in the
    // parent shard's trie.
    fn get_child_congestion_info_not_finalized(
        parent_trie: &dyn TrieAccess,
        parent_shard_layout: &ShardLayout,
        parent_congestion_info: CongestionInfo,
        retain_mode: RetainMode,
    ) -> Result<CongestionInfo, Error> {
        // The left child contains all the delayed and buffered receipts from the
        // parent so it should have identical congestion info.
        if retain_mode == RetainMode::Left {
            return Ok(parent_congestion_info);
        }

        // The right child contains all the delayed receipts from the parent but it
        // has no buffered receipts. It's info needs to be computed by subtracting
        // the parent's buffered receipts from the parent's congestion info.
        let mut congestion_info = parent_congestion_info;
        for shard_id in parent_shard_layout.shard_ids() {
            let receipt_groups = ReceiptGroupsQueue::load(parent_trie, shard_id)?;
            let Some(receipt_groups) = receipt_groups else {
                continue;
            };

            let bytes = receipt_groups.total_size();
            let gas = receipt_groups.total_gas();

            congestion_info
                .remove_buffered_receipt_gas(gas)
                .expect("Buffered gas must not exceed congestion info buffered gas");
            congestion_info
                .remove_receipt_bytes(bytes)
                .expect("Buffered size must not exceed congestion info buffered size");
        }

        // The right child does not inherit any buffered receipts. The
        // congestion info must match this invariant.
        assert_eq!(congestion_info.buffered_receipts_gas(), 0);

        Ok(congestion_info)
    }

    fn finalize_allowed_shard(
        child_shard_layout: &ShardLayout,
        child_shard_uid: ShardUId,
        congestion_info: &mut CongestionInfo,
    ) -> Result<(), Error> {
        let all_shards = child_shard_layout.shard_ids().collect_vec();
        let own_shard = child_shard_uid.shard_id();
        let own_shard_index = child_shard_layout
            .get_shard_index(own_shard)?
            .try_into()
            .expect("ShardIndex must fit in u64");
        // Please note that the congestion seed used during resharding is
        // different than the one used during normal operation. In runtime the
        // seed is set to the sum of shard index and block height. The block
        // height isn't easily available on all call sites which is why the
        // simplified seed is used. This is valid because it's deterministic and
        // resharding is a very rare event. However in a perfect world it should
        // be the same.
        // TODO - Use proper congestion control seed during resharding.
        let congestion_seed = own_shard_index;
        congestion_info.finalize_allowed_shard(own_shard, &all_shards, congestion_seed);
        Ok(())
    }

    // TODO(store): Use proper store interface
    fn get_chunk_extra(
        &self,
        block_hash: &CryptoHash,
        shard_uid: &ShardUId,
    ) -> Result<Arc<ChunkExtra>, Error> {
        let key = get_block_shard_uid(block_hash, shard_uid);
        let value = self
            .store
            .get_ser(DBCol::ChunkExtra, &key)
            .map_err(|e| Error::DBNotFoundErr(e.to_string()))?;
        value.ok_or_else(|| {
            Error::DBNotFoundErr(
                format_args!("CHUNK EXTRA: {}:{:?}", block_hash, shard_uid).to_string(),
            )
        })
    }
}

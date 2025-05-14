use super::event_type::{ReshardingEventType, ReshardingSplitShardParams};
use super::types::ReshardingSender;
use crate::ChainStoreUpdate;
use crate::flat_storage_resharder::{FlatStorageResharder, FlatStorageResharderController};
use crate::types::RuntimeAdapter;
use itertools::Itertools;
use near_chain_configs::{MutableConfigValue, ReshardingConfig, ReshardingHandle};
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::Block;
use near_primitives::congestion_info::CongestionInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_store::adapter::StoreAdapter;
use near_store::adapter::trie_store::{TrieStoreUpdateAdapter, get_shard_uid_mapping};
use near_store::flat::BlockInfo;
use near_store::trie::ops::resharding::RetainMode;
use near_store::trie::outgoing_metadata::ReceiptGroupsQueue;
use near_store::{ShardTries, ShardUId, Store, TrieAccess};
use std::io;
use std::num::NonZero;
use std::sync::Arc;

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
        &self,
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
            tries,
            &split_shard_event,
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
    fn set_state_shard_uid_mapping(
        &self,
        split_shard_event: &ReshardingSplitShardParams,
    ) -> io::Result<()> {
        let mut store_update = self.store.trie_store().store_update();
        let parent_shard_uid = split_shard_event.parent_shard;
        let parent_shard_uid_prefix = get_shard_uid_mapping(&self.store, parent_shard_uid);
        for child_shard_uid in split_shard_event.children_shards() {
            store_update.set_shard_uid_mapping(child_shard_uid, parent_shard_uid_prefix);
        }
        store_update.commit()
    }

    /// TODO(resharding): Remove once proper solution for negative refcounts is implemented.
    fn duplicate_nodes_at_split_boundary<'a>(
        trie_store_update: &mut TrieStoreUpdateAdapter,
        trie_nodes: impl Iterator<Item = (&'a CryptoHash, &'a Arc<[u8]>)>,
        shard_uid_prefix: ShardUId,
    ) {
        let refcount_increment = NonZero::new(1).unwrap();
        for (node_hash, node_value) in trie_nodes {
            trie_store_update.increment_refcount_by(
                shard_uid_prefix,
                &node_hash,
                &node_value,
                refcount_increment,
            );
        }
    }

    /// Creates temporary memtries for new shards to be able to process them in the next epoch.
    /// Note this doesn't complete memtries resharding, proper memtries are to be created later.
    fn process_memtrie_resharding_storage_update(
        &self,
        mut chain_store_update: ChainStoreUpdate,
        block: &Block,
        tries: ShardTries,
        split_shard_event: &ReshardingSplitShardParams,
    ) -> Result<(), Error> {
        let block_hash = block.hash();
        let block_height = block.header().height();
        let ReshardingSplitShardParams {
            left_child_shard,
            right_child_shard,
            parent_shard: parent_shard_uid,
            boundary_account,
            ..
        } = split_shard_event;
        let _span = tracing::debug_span!(
            target: "resharding", "process_memtrie_resharding_storage_update",
            ?block_hash, block_height, ?parent_shard_uid)
        .entered();

        let parent_chunk_extra =
            self.store.chain_store().get_chunk_extra(block_hash, parent_shard_uid)?;
        let mut store_update = self.store.trie_store().store_update();

        // TODO(resharding): leave only tracked shards.
        for (new_shard_uid, retain_mode) in
            [(left_child_shard, RetainMode::Left), (right_child_shard, RetainMode::Right)]
        {
            let parent_trie = tries
                .get_trie_for_shard(*parent_shard_uid, *parent_chunk_extra.state_root())
                .recording_reads_new_recorder();

            if !parent_trie.has_memtries() {
                tracing::error!(
                    "Memtrie not loaded. Cannot process memtrie resharding storage
                     update for block {:?}, shard {:?}",
                    block_hash,
                    parent_shard_uid,
                );
                return Err(Error::Other("Memtrie not loaded".to_string()));
            }

            tracing::info!(
                target: "resharding", ?new_shard_uid, ?retain_mode,
                "Creating child trie by retaining nodes in parent memtrie..."
            );

            // Get the congestion info for the child.
            // We need to record this as this is used later in ImplicitTransitionParams::Resharding chunk validation.
            let parent_epoch_id = block.header().epoch_id();
            let parent_shard_layout = self.epoch_manager.get_shard_layout(&parent_epoch_id)?;
            let parent_congestion_info = parent_chunk_extra.congestion_info();

            let child_epoch_id = self.epoch_manager.get_next_epoch_id(&block_hash)?;
            let child_shard_layout = self.epoch_manager.get_shard_layout(&child_epoch_id)?;
            let child_congestion_info = Self::get_child_congestion_info(
                &parent_trie,
                &parent_shard_layout,
                parent_congestion_info,
                &child_shard_layout,
                new_shard_uid,
                retain_mode,
            )?;

            // Split the parent trie and create a new child trie.
            let trie_changes = parent_trie.retain_split_shard(boundary_account, retain_mode)?;
            let new_root = tries.apply_all(&trie_changes, *parent_shard_uid, &mut store_update);
            tries.apply_memtrie_changes(&trie_changes, *parent_shard_uid, block_height);

            // TODO(resharding): remove duplicate_nodes_at_split_boundary method after proper fix for refcount issue
            let trie_recorder = parent_trie.take_recorder().unwrap();
            let mut trie_recorder = trie_recorder.write();
            Self::duplicate_nodes_at_split_boundary(
                &mut store_update,
                trie_recorder.recorded_iter(),
                *parent_shard_uid,
            );

            // TODO(resharding): set all fields of `ChunkExtra`. Consider stronger
            // typing. Clarify where it should happen when `State` and
            // `FlatState` update is implemented.
            let mut child_chunk_extra = ChunkExtra::clone(&parent_chunk_extra);
            *child_chunk_extra.state_root_mut() = new_root;
            *child_chunk_extra.congestion_info_mut() = child_congestion_info;

            chain_store_update.save_chunk_extra(block_hash, &new_shard_uid, child_chunk_extra);
            chain_store_update.save_state_transition_data(
                *block_hash,
                new_shard_uid.shard_id(),
                Some(trie_recorder.recorded_storage()),
                CryptoHash::default(),
                // No contract code is accessed or deployed during resharding.
                // TODO(#11099): Confirm if sending no contracts is ok here.
                Default::default(),
            );

            tracing::info!(target: "resharding", ?new_shard_uid, ?new_root, "Child trie created");
        }

        // After committing the split changes, the parent trie has the state root of both the children.
        // Now we can freeze the parent memtrie and copy it to the children.
        tries.freeze_parent_memtrie(*parent_shard_uid, split_shard_event.children_shards())?;

        chain_store_update.merge(store_update.into());
        chain_store_update.commit()?;

        Ok(())
    }

    pub fn get_child_congestion_info(
        parent_trie: &dyn TrieAccess,
        parent_shard_layout: &ShardLayout,
        parent_congestion_info: CongestionInfo,
        child_shard_layout: &ShardLayout,
        child_shard_uid: &ShardUId,
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
        child_shard_uid: &ShardUId,
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
}

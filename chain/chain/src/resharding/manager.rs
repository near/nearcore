use std::io;
use std::sync::Arc;

use super::event_type::{ReshardingEventType, ReshardingSplitShardParams};
use super::types::ReshardingSender;
use crate::flat_storage_resharder::{FlatStorageResharder, FlatStorageResharderController};
use crate::types::RuntimeAdapter;
use crate::ChainStoreUpdate;
use near_chain_configs::{MutableConfigValue, ReshardingConfig, ReshardingHandle};
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::Block;
use near_primitives::challenge::PartialState;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{get_block_shard_uid, ShardLayout};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use near_store::trie::mem::mem_trie_update::TrackingMode;
use near_store::trie::ops::resharding::RetainMode;
use near_store::trie::TrieRecorder;
use near_store::{DBCol, ShardTries, ShardUId, Store};

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

        let next_block_has_new_shard_layout =
            self.epoch_manager.is_next_block_epoch_start(block_hash)?
                && shard_layout != next_shard_layout;
        if !next_block_has_new_shard_layout {
            tracing::debug!(target: "resharding", ?prev_hash, "prev block has the same shard layout, skipping");
            return Ok(());
        }

        if !matches!(next_shard_layout, ShardLayout::V2(_)) {
            tracing::debug!(target: "resharding", ?next_shard_layout, "next shard layout is not v2, skipping");
            return Ok(());
        }

        let resharding_event_type =
            ReshardingEventType::from_shard_layout(&next_shard_layout, *block_hash)?;
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

        // Reshard the State column by setting ShardUId mapping from children to parent.
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
    /// so that subsequent accesses to the State will use the parent shard's UId as a prefix for the database key.
    fn set_state_shard_uid_mapping(
        &mut self,
        split_shard_event: &ReshardingSplitShardParams,
    ) -> io::Result<()> {
        let mut store_update = self.store.trie_store().store_update();
        let parent_shard_uid = split_shard_event.parent_shard;
        // TODO(resharding) No need to set the mapping for children shards that we won't track just after resharding?
        for child_shard_uid in split_shard_event.children_shards() {
            store_update.set_shard_uid_mapping(child_shard_uid, parent_shard_uid);
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

        // TODO(resharding): what if node doesn't have memtrie? just pause
        // processing?
        // TODO(resharding): fork handling. if epoch is finalized on different
        // blocks, the second finalization will crash.
        tries.freeze_mem_tries(parent_shard_uid, split_shard_event.children_shards())?;

        let chunk_extra = self.get_chunk_extra(block_hash, &parent_shard_uid)?;
        let boundary_account = split_shard_event.boundary_account;

        let mut trie_store_update = self.store.store_update();

        // TODO(resharding): leave only tracked shards.
        for (new_shard_uid, retain_mode) in [
            (split_shard_event.left_child_shard, RetainMode::Left),
            (split_shard_event.right_child_shard, RetainMode::Right),
        ] {
            let Some(mem_tries) = tries.get_mem_tries(new_shard_uid) else {
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
            let mut mem_tries = mem_tries.write().unwrap();
            let mut trie_recorder = TrieRecorder::new();
            let mode = TrackingMode::RefcountsAndAccesses(&mut trie_recorder);
            let mem_trie_update = mem_tries.update(*chunk_extra.state_root(), mode)?;

            let trie_changes = mem_trie_update.retain_split_shard(&boundary_account, retain_mode);
            let partial_storage = trie_recorder.recorded_storage();
            let partial_state_len = match &partial_storage.nodes {
                PartialState::TrieValues(values) => values.len(),
            };
            let mem_changes = trie_changes.mem_trie_changes.as_ref().unwrap();
            let new_state_root = mem_tries.apply_memtrie_changes(block_height, mem_changes);
            // TODO(resharding): set all fields of `ChunkExtra`. Consider stronger
            // typing. Clarify where it should happen when `State` and
            // `FlatState` update is implemented.
            let mut child_chunk_extra = ChunkExtra::clone(&chunk_extra);
            *child_chunk_extra.state_root_mut() = new_state_root;

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

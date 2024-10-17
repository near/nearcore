use std::sync::Arc;

use super::event_type::ReshardingEventType;
use super::types::ReshardingSender;
use crate::flat_storage_resharder::{FlatStorageResharder, FlatStorageResharderController};
use crate::types::RuntimeAdapter;
use near_async::messaging::IntoSender;
use near_chain_configs::{MutableConfigValue, ReshardingConfig, ReshardingHandle};
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::Block;
use near_primitives::challenge::PartialState;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{get_block_shard_uid, ShardLayout};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_store::adapter::StoreUpdateAdapter;
use near_store::trie::mem::resharding::RetainMode;
use near_store::{DBCol, PartialStorage, ShardTries, ShardUId, Store};

use crate::ChainStoreUpdate;

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
            resharding_sender.into_sender(),
            FlatStorageResharderController::from_resharding_handle(resharding_handle.clone()),
        );
        Self { store, epoch_manager, resharding_config, flat_storage_resharder, resharding_handle }
    }

    /// If shard layout changes after the given block, creates temporary
    /// memtries for new shards to be able to process them in the next epoch.
    /// Note this doesn't complete resharding, proper memtries are to be
    /// created later.
    pub fn process_memtrie_resharding_storage_update(
        &mut self,
        mut chain_store_update: ChainStoreUpdate,
        block: &Block,
        shard_uid: ShardUId,
        tries: ShardTries,
    ) -> Result<(), Error> {
        let block_hash = block.hash();
        let block_height = block.header().height();
        let _span = tracing::debug_span!(
            target: "resharding", "process_memtrie_resharding_storage_update", 
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
            ReshardingEventType::from_shard_layout(&next_shard_layout, *block_hash, *prev_hash)?;
        let Some(ReshardingEventType::SplitShard(split_shard_event)) = resharding_event_type else {
            tracing::debug!(target: "resharding", ?resharding_event_type, "resharding event type is not split shard, skipping");
            return Ok(());
        };
        if split_shard_event.parent_shard != shard_uid {
            let parent_shard = split_shard_event.parent_shard;
            tracing::debug!(target: "resharding", ?parent_shard, "shard uid does not match event parent shard, skipping");
            return Ok(());
        }

        // TODO(resharding): what if node doesn't have memtrie? just pause
        // processing?
        // TODO(resharding): fork handling. if epoch is finalized on different
        // blocks, the second finalization will crash.
        tries.freeze_mem_tries(
            shard_uid,
            vec![split_shard_event.left_child_shard, split_shard_event.right_child_shard],
        )?;

        // Trigger resharding of flat storage.
        self.flat_storage_resharder.start_resharding(
            ReshardingEventType::SplitShard(split_shard_event.clone()),
            &next_shard_layout,
        )?;

        let chunk_extra = self.get_chunk_extra(block_hash, &shard_uid)?;
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
                    shard_uid
                );
                return Err(Error::Other("Memtrie not loaded".to_string()));
            };

            tracing::info!(
                target: "resharding", ?new_shard_uid, ?retain_mode,
                "Creating child memtrie by retaining nodes in parent memtrie..."
            );
            let mut mem_tries = mem_tries.write().unwrap();
            let mem_trie_update = mem_tries.update(*chunk_extra.state_root(), true)?;

            let (trie_changes, _) =
                mem_trie_update.retain_split_shard(&boundary_account, retain_mode);
            // TODO(#12019): proof generation
            let partial_state = PartialState::default();
            let partial_state_len = match &partial_state {
                PartialState::TrieValues(values) => values.len(),
            };
            let partial_storage = PartialStorage { nodes: partial_state };
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
                // No contract code is accessed during resharding.
                // TODO(#11099): Confirm if sending no contracts is ok here.
                vec![],
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
        value.ok_or(Error::DBNotFoundErr(
            format_args!("CHUNK EXTRA: {}:{:?}", block_hash, shard_uid).to_string(),
        ))
    }
}

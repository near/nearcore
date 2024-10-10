use std::sync::Arc;

use near_chain_configs::{MutableConfigValue, ReshardingConfig, ReshardingHandle};
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::Block;
use near_primitives::challenge::PartialState;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::get_block_shard_uid;
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
}

impl ReshardingManager {
    pub fn new(
        store: Store,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        resharding_config: MutableConfigValue<ReshardingConfig>,
    ) -> Self {
        Self { store, epoch_manager, resharding_config, resharding_handle: ReshardingHandle::new() }
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
        let prev_hash = block.header().prev_hash();
        let shard_layout = self.epoch_manager.get_shard_layout(&block.header().epoch_id())?;
        let next_epoch_id = self.epoch_manager.get_next_epoch_id_from_prev_block(prev_hash)?;
        let next_shard_layout = self.epoch_manager.get_shard_layout(&next_epoch_id)?;

        let next_block_has_new_shard_layout =
            self.epoch_manager.is_next_block_epoch_start(block.hash())?
                && shard_layout != next_shard_layout;
        if !next_block_has_new_shard_layout {
            return Ok(());
        }

        let children_shard_uids =
            next_shard_layout.get_children_shards_uids(shard_uid.shard_id()).unwrap();

        // Hack to ensure this logic is not applied before ReshardingV3.
        // TODO(#12019): proper logic.
        if next_shard_layout.version() < 3 || children_shard_uids.len() == 1 {
            return Ok(());
        }
        assert_eq!(children_shard_uids.len(), 2);
        let old_shard_index = shard_layout
            .shard_uids()
            .enumerate()
            .find_map(
                |(index, old_shard_uid)| {
                    if old_shard_uid == shard_uid {
                        Some(index)
                    } else {
                        None
                    }
                },
            )
            .unwrap();
        let boundary_accounts = next_shard_layout.boundary_accounts();
        let boundary_account = &boundary_accounts[old_shard_index];

        // TODO(#12019): what if node doesn't have memtrie? just pause
        // processing?
        tries.freeze(shard_uid, children_shard_uids.clone())?;

        let chunk_extra = self.get_chunk_extra(block_hash, &shard_uid)?;

        // TODO(#12019): leave only tracked shards.
        for (new_shard_uid, retain_mode) in [
            (children_shard_uids[0], RetainMode::Left),
            (children_shard_uids[1], RetainMode::Right),
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

            let mut mem_tries = mem_tries.write().unwrap();
            let mem_trie_update = mem_tries.update(*chunk_extra.state_root(), true)?;

            let (trie_changes, _) =
                mem_trie_update.retain_split_shard(&boundary_account, retain_mode);
            let partial_state = PartialState::default();
            let partial_storage = PartialStorage { nodes: partial_state };
            let mem_changes = trie_changes.mem_trie_changes.as_ref().unwrap();
            let new_state_root = mem_tries.apply_memtrie_changes(block_height, mem_changes);
            // TODO(#12019): set all fields of `ChunkExtra`. Consider stronger
            // typing. Clarify where it should happen when `State` and
            // `FlatState` update is implemented.
            let mut child_chunk_extra = ChunkExtra::clone(&chunk_extra);
            *child_chunk_extra.state_root_mut() = new_state_root;

            // TODO(store): Use proper store interface
            println!(
                "save chunk extra: block hash: {:?}, shard uid: {:?}, state root: {:?}",
                block_hash, new_shard_uid, new_state_root
            );
            chain_store_update.save_chunk_extra(block_hash, &new_shard_uid, child_chunk_extra);
            chain_store_update.save_state_transition_data(
                *block_hash,
                new_shard_uid.shard_id(),
                Some(partial_storage),
                CryptoHash::default(),
            );

            // Commit `TrieChanges` directly. They are needed to serve reads of
            // new nodes from `DBCol::State` while memtrie is properly created
            // from flat storage.
            let mut store_update = self.store.store_update();
            tries.apply_insertions(
                &trie_changes,
                new_shard_uid,
                &mut store_update.trie_store_update(),
            );
            store_update.commit()?;
        }

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

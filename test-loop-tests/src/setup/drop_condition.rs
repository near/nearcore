use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::{Arc, Mutex};

use near_async::test_loop::data::TestLoopData;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::types::{AccountId, BlockHeight, ShardId, ShardIndex};
use near_vm_runner::logic::ProtocolVersion;

use crate::utils::network::{
    block_dropper_by_height, chunk_endorsement_dropper, chunk_endorsement_dropper_by_hash,
};

use super::env::TestLoopChunksStorage;
use super::state::TestData;

pub enum DropCondition {
    /// Whether test loop should drop all chunks validated by the given account.
    /// Works if number of nodes is significant enough (at least three?).
    ChunksValidatedBy(AccountId),
    /// Whether test loop should drop all endorsements from the given account.
    EndorsementsFrom(AccountId),
    /// Whether test loop should drop all chunks in the given range of heights
    /// relative to first block height where protocol version changes.
    ProtocolUpgradeChunkRange(ProtocolVersion, HashMap<ShardIndex, Range<i64>>),
    /// Specifies the chunks that should be produced by their appearance in the
    /// chain with respect to the start of an epoch. That is, a given chunk at height
    /// `height_created` for shard `shard_id` will be produced if
    /// self.0[`shard_id`][`height_created` - `epoch_start`] is true, or if
    /// `height_created` - `epoch_start` > self.0[`shard_id`].len()
    ChunksProducedByHeight(HashMap<ShardId, Vec<bool>>),
    // Drops Block broadcast messages with height in `self.0`
    BlocksByHeight(HashSet<BlockHeight>),
}

impl TestData {
    pub fn register_drop_condition(
        &mut self,
        test_loop_data: &mut TestLoopData,
        chunks_storage: Arc<Mutex<TestLoopChunksStorage>>,
        drop_condition: &DropCondition,
    ) {
        match drop_condition {
            DropCondition::ChunksValidatedBy(account_id) => {
                self.register_drop_chunks_validated_by(test_loop_data, chunks_storage, account_id)
            }
            DropCondition::EndorsementsFrom(account_id) => {
                self.register_drop_endorsements_from(test_loop_data, account_id);
            }
            DropCondition::ProtocolUpgradeChunkRange(protocol_version, chunk_ranges) => {
                self.register_drop_protocol_upgrade_chunks(
                    test_loop_data,
                    chunks_storage,
                    *protocol_version,
                    chunk_ranges.clone(),
                );
            }
            DropCondition::ChunksProducedByHeight(chunks_produced) => {
                self.register_drop_chunks_by_height(
                    test_loop_data,
                    chunks_storage,
                    chunks_produced.clone(),
                );
            }
            DropCondition::BlocksByHeight(heights) => {
                self.register_drop_blocks_by_height(test_loop_data, heights);
            }
        }
    }

    fn register_drop_chunks_validated_by(
        &mut self,
        test_loop_data: &mut TestLoopData,
        chunks_storage: Arc<Mutex<TestLoopChunksStorage>>,
        account_id: &AccountId,
    ) {
        let client_actor = test_loop_data.get(&self.client_sender.actor_handle());
        let epoch_manager = client_actor.client.chain.epoch_manager.clone();

        let inner_epoch_manager = epoch_manager.clone();
        let account_id = account_id.clone();
        let drop_chunks_condition = Box::new(move |chunk: ShardChunkHeader| -> bool {
            is_chunk_validated_by(inner_epoch_manager.as_ref(), chunk, account_id.clone())
        });

        let peer_actor = test_loop_data.get_mut(&self.peer_manager_sender.actor_handle());
        peer_actor.register_override_handler(chunk_endorsement_dropper_by_hash(
            chunks_storage,
            epoch_manager,
            drop_chunks_condition,
        ));
    }

    fn register_drop_endorsements_from(
        &mut self,
        test_loop_data: &mut TestLoopData,
        account_id: &AccountId,
    ) {
        let peer_actor = test_loop_data.get_mut(&self.peer_manager_sender.actor_handle());
        peer_actor.register_override_handler(chunk_endorsement_dropper(account_id.clone()));
    }

    fn register_drop_protocol_upgrade_chunks(
        &mut self,
        test_loop_data: &mut TestLoopData,
        chunks_storage: Arc<Mutex<TestLoopChunksStorage>>,
        protocol_version: ProtocolVersion,
        chunk_ranges: HashMap<ShardIndex, Range<i64>>,
    ) {
        let client_actor = test_loop_data.get(&self.client_sender.actor_handle());
        let epoch_manager = client_actor.client.chain.epoch_manager.clone();

        let inner_epoch_manager = epoch_manager.clone();
        let drop_chunks_condition = Box::new(move |chunk: ShardChunkHeader| -> bool {
            should_drop_chunk_for_protocol_upgrade(
                inner_epoch_manager.as_ref(),
                chunk,
                protocol_version,
                chunk_ranges.clone(),
            )
        });

        let peer_actor = test_loop_data.get_mut(&self.peer_manager_sender.actor_handle());
        peer_actor.register_override_handler(chunk_endorsement_dropper_by_hash(
            chunks_storage,
            epoch_manager,
            drop_chunks_condition,
        ));
    }

    fn register_drop_chunks_by_height(
        &mut self,
        test_loop_data: &mut TestLoopData,
        chunks_storage: Arc<Mutex<TestLoopChunksStorage>>,
        chunks_produced: HashMap<ShardId, Vec<bool>>,
    ) {
        let client_actor = test_loop_data.get(&self.client_sender.actor_handle());
        let epoch_manager = client_actor.client.chain.epoch_manager.clone();

        let inner_epoch_manager = epoch_manager.clone();
        let drop_chunks_condition = Box::new(move |chunk: ShardChunkHeader| -> bool {
            should_drop_chunk_by_height(
                inner_epoch_manager.as_ref(),
                chunk,
                chunks_produced.clone(),
            )
        });

        let peer_actor = test_loop_data.get_mut(&self.peer_manager_sender.actor_handle());
        peer_actor.register_override_handler(chunk_endorsement_dropper_by_hash(
            chunks_storage,
            epoch_manager.clone(),
            drop_chunks_condition.clone(),
        ));
    }

    fn register_drop_blocks_by_height(
        &mut self,
        test_loop_data: &mut TestLoopData,
        heights: &HashSet<BlockHeight>,
    ) {
        let peer_actor = test_loop_data.get_mut(&self.peer_manager_sender.actor_handle());
        peer_actor.register_override_handler(block_dropper_by_height(heights.clone()));
    }
}

/// Checks whether chunk is validated by the given account.
fn is_chunk_validated_by(
    epoch_manager_adapter: &dyn EpochManagerAdapter,
    chunk: ShardChunkHeader,
    account_id: AccountId,
) -> bool {
    let prev_block_hash = chunk.prev_block_hash();
    let shard_id = chunk.shard_id();
    let height_created = chunk.height_created();
    let epoch_id = epoch_manager_adapter.get_epoch_id_from_prev_block(prev_block_hash).unwrap();

    let chunk_validators = epoch_manager_adapter
        .get_chunk_validator_assignments(&epoch_id, shard_id, height_created)
        .unwrap();
    return chunk_validators.contains(&account_id);
}

/// returns !chunks_produced[shard_id][height_created - epoch_start].
fn should_drop_chunk_by_height(
    epoch_manager_adapter: &dyn EpochManagerAdapter,
    chunk: ShardChunkHeader,
    chunks_produced: HashMap<ShardId, Vec<bool>>,
) -> bool {
    let prev_block_hash = chunk.prev_block_hash();
    let shard_id = chunk.shard_id();
    let height_created = chunk.height_created();

    let height_in_epoch =
        if epoch_manager_adapter.is_next_block_epoch_start(prev_block_hash).unwrap() {
            0
        } else {
            let epoch_start =
                epoch_manager_adapter.get_epoch_start_height(prev_block_hash).unwrap();
            height_created - epoch_start
        };
    let Some(chunks_produced) = chunks_produced.get(&shard_id) else {
        return false;
    };
    let Some(should_produce) = chunks_produced.get(height_in_epoch as usize) else {
        return false;
    };
    !*should_produce
}

/// Returns true if the chunk should be dropped based on the
/// `DropCondition::ProtocolUpgradeChunkRange`.
fn should_drop_chunk_for_protocol_upgrade(
    epoch_manager_adapter: &dyn EpochManagerAdapter,
    chunk: ShardChunkHeader,
    version_of_protocol_upgrade: ProtocolVersion,
    chunk_ranges: HashMap<ShardIndex, Range<i64>>,
) -> bool {
    let prev_block_hash = chunk.prev_block_hash();
    let shard_id = chunk.shard_id();
    let height_created = chunk.height_created();
    let epoch_id = epoch_manager_adapter.get_epoch_id_from_prev_block(prev_block_hash).unwrap();
    let shard_layout = epoch_manager_adapter.get_shard_layout(&epoch_id).unwrap();
    let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
    // If there is no condition for the shard, all chunks
    // pass through.
    let Some(range) = chunk_ranges.get(&shard_index) else {
        return false;
    };

    let epoch_protocol_version =
        epoch_manager_adapter.get_epoch_protocol_version(&epoch_id).unwrap();
    // Drop condition for the first epoch with new protocol version.
    if epoch_protocol_version >= version_of_protocol_upgrade {
        let prev_epoch_id =
            epoch_manager_adapter.get_prev_epoch_id_from_prev_block(prev_block_hash).unwrap();
        let prev_epoch_protocol_version =
            epoch_manager_adapter.get_epoch_protocol_version(&prev_epoch_id).unwrap();
        // If this is not the first epoch with new protocol version,
        // all chunks go through.
        if prev_epoch_protocol_version >= version_of_protocol_upgrade {
            return false;
        }

        // Check the first block height in the epoch separately,
        // because the block itself is not created yet.
        // Its relative height is 0.
        if epoch_manager_adapter.is_next_block_epoch_start(prev_block_hash).unwrap() {
            return range.contains(&0);
        }

        // Otherwise we can get start height of the epoch by
        // the previous hash.
        let epoch_start_height =
            epoch_manager_adapter.get_epoch_start_height(&prev_block_hash).unwrap();
        range.contains(&(height_created as i64 - epoch_start_height as i64))
    } else if epoch_protocol_version < version_of_protocol_upgrade {
        // Drop condition for the last epoch with old protocol version.
        let maybe_upgrade_height = epoch_manager_adapter
            .get_estimated_protocol_upgrade_block_height(*prev_block_hash)
            .unwrap();

        // The protocol upgrade height is known if and only if
        // protocol upgrade happens in the next epoch.
        let Some(upgrade_height) = maybe_upgrade_height else {
            return false;
        };
        let next_epoch_id =
            epoch_manager_adapter.get_next_epoch_id_from_prev_block(prev_block_hash).unwrap();
        let next_epoch_protocol_version =
            epoch_manager_adapter.get_epoch_protocol_version(&next_epoch_id).unwrap();
        assert!(epoch_protocol_version < next_epoch_protocol_version);
        range.contains(&(height_created as i64 - upgrade_height as i64))
    } else {
        false
    }
}

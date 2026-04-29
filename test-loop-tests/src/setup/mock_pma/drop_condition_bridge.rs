//! Mock-side bridge that wires `DropCondition` into the
//! `TestLoopPeerManagerActor` via `register_override_handler`.
//!
//! Tests call `node_data.register_drop_condition(...)` /
//! `node_data.register_override_handler(...)`; both go through this
//! impl block, which reaches into the mock PMA's actor handle.

use super::peer_manager_actor::NetworkRequestHandler;
use crate::setup::drop_condition::{
    DropCondition, TestLoopChunksStorage, is_chunk_validated_by, should_drop_chunk_by_height,
    should_drop_chunk_for_protocol_upgrade,
};
use crate::setup::state::NodeExecutionData;
use crate::utils::network::{
    block_dropper_by_height, chunk_endorsement_dropper, chunk_endorsement_dropper_by_hash,
};
use near_async::test_loop::data::TestLoopData;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::types::{AccountId, BlockHeight, ShardId, ShardIndex};
use near_vm_runner::logic::ProtocolVersion;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;

impl NodeExecutionData {
    pub fn register_drop_condition(
        &self,
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
        &self,
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

        self.register_override_handler(
            test_loop_data,
            chunk_endorsement_dropper_by_hash(chunks_storage, epoch_manager, drop_chunks_condition),
        );
    }

    fn register_drop_endorsements_from(
        &self,
        test_loop_data: &mut TestLoopData,
        account_id: &AccountId,
    ) {
        self.register_override_handler(
            test_loop_data,
            chunk_endorsement_dropper(account_id.clone()),
        );
    }

    fn register_drop_protocol_upgrade_chunks(
        &self,
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

        self.register_override_handler(
            test_loop_data,
            chunk_endorsement_dropper_by_hash(chunks_storage, epoch_manager, drop_chunks_condition),
        );
    }

    fn register_drop_chunks_by_height(
        &self,
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

        self.register_override_handler(
            test_loop_data,
            chunk_endorsement_dropper_by_hash(
                chunks_storage,
                epoch_manager.clone(),
                drop_chunks_condition.clone(),
            ),
        );
    }

    fn register_drop_blocks_by_height(
        &self,
        test_loop_data: &mut TestLoopData,
        heights: &HashSet<BlockHeight>,
    ) {
        self.register_override_handler(test_loop_data, block_dropper_by_height(heights.clone()));
    }

    pub fn register_override_handler(
        &self,
        test_loop_data: &mut TestLoopData,
        handler: NetworkRequestHandler,
    ) {
        let peer_actor = test_loop_data.get_mut(
            &self
                .legacy_mock_pma_sender
                .as_ref()
                .expect(
                    "register_override_handler requires the legacy mock PMA — \
                     test must call .use_legacy_mock_pma() on the builder",
                )
                .actor_handle(),
        );
        peer_actor.register_override_handler(handler);
    }
}

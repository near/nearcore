//! `shard_rotation`: chunk producer assignments shuffled every epoch.

use super::super::scenario::{Fault, Timeline};
use super::render;
use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use near_primitives::types::{AccountId, EpochId, ShardId};
use std::collections::HashSet;
use std::fmt::{self, Debug, Formatter};

/// The shuffle rotates producers into shards at the boundary, so a producer that kept
/// applying its new shard's chunks synthesizes and endorses that shard's boundary result
/// itself, and one that only caught up through state sync skips the boundary step.
///
/// The shuffle permutes whole producer groups between shards, seeded by the epoch's random
/// value. That value is a chain of VRF outputs from a constant genesis seed, so it depends
/// only on which validator produces each height, never on the chain's content: whether
/// the groups swap at the boundary is a fixed property of the profile, and on profile A
/// they do not. The fault then cannot fire there, which its fired check reports, so no
/// preset names it until the shuffle can be steered or a profile whose chain swaps at the
/// boundary exists.
// Constructed by no preset until it can fire, see above.
#[allow(dead_code)]
#[derive(Default)]
pub struct ShardRotation;

#[allow(dead_code)]
impl ShardRotation {
    pub fn new() -> Self {
        Self
    }
}

impl Debug for ShardRotation {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        render(f, self.tag(), &[])
    }
}

impl Fault for ShardRotation {
    fn tag(&self) -> &'static str {
        "shard_rotation"
    }

    fn setup(&self, builder: TestLoopBuilder) -> TestLoopBuilder {
        builder.shuffle_shard_assignment_for_chunk_producers(true)
    }

    fn assert_fired(&self, env: &TestLoopEnv, timeline: &Timeline) {
        let last_pre_spice = timeline.last_pre_spice.as_ref().unwrap();
        let node = env.rpc_node();
        let epoch_manager = node.client().epoch_manager.clone();
        let pre_spice_epoch_id = last_pre_spice.header().epoch_id();
        let spice_epoch_id =
            epoch_manager.get_epoch_id_from_prev_block(last_pre_spice.hash()).unwrap();
        let producers = |epoch_id: &EpochId, shard_id: ShardId| -> HashSet<AccountId> {
            epoch_manager
                .get_epoch_chunk_producers_for_shard(epoch_id, shard_id)
                .unwrap()
                .into_iter()
                .collect()
        };
        let shard_layout = epoch_manager.get_shard_layout(pre_spice_epoch_id).unwrap();
        let rotated = shard_layout.shard_ids().any(|shard_id| {
            !producers(&spice_epoch_id, shard_id)
                .is_subset(&producers(pre_spice_epoch_id, shard_id))
        });
        assert!(
            rotated,
            "the shuffle left every shard's chunk-producer set unchanged at the boundary"
        );
    }
}

//! `shard_skip`: a shard silenced from warmup through the last pre-spice block.

use super::super::loads::CONTRACT_SHARD_INDEX;
use super::super::scenario::{
    Fault, FaultEvent, Narrowing, OracleId, Progress, Relaxation, Timeline,
};
use super::{render, shard_layout_at_head};
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use near_primitives::types::{BlockHeight, ShardId};
use std::collections::HashSet;
use std::fmt::{self, Debug, Formatter};
use std::ops::Range;

/// Which epochs the shard is silent in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
// The other two variants are rejected until their milestones land.
#[allow(dead_code)]
pub enum Epochs {
    /// From the first chunk the dropper may touch through the last pre-spice block. Both
    /// pre-spice epochs: they share the genesis epoch info and so its epoch height.
    PreSpice,
    /// The last pre-spice epoch only; needs `setup:vote=epoch(N)`, milestone 6.
    LastPreSpice,
    /// The whole first spice epoch, through the adversarial chunk mode, milestone 3.
    FirstSpice,
}

/// A shard silenced through `DropCondition::ChunksForShardsInEpochs`, keyed on the
/// pre-spice epoch height, so that the undelivered-receipt carriers span an epoch boundary
/// and the receipts pending for the shard reach back to warmup.
pub struct ShardSkip {
    shard: usize,
    epochs: Epochs,
    shard_id: Option<ShardId>,
    /// From the height after the shard's last new chunk through the last pre-spice block,
    /// read off the chain at `last_pre_spice`.
    silenced: Option<Range<BlockHeight>>,
    /// The first height of the last pre-spice epoch.
    last_pre_spice_epoch_start: Option<BlockHeight>,
}

impl Default for ShardSkip {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardSkip {
    /// Shard index 0 silent through the last pre-spice block.
    pub fn new() -> Self {
        Self {
            shard: 0,
            epochs: Epochs::PreSpice,
            shard_id: None,
            silenced: None,
            last_pre_spice_epoch_start: None,
        }
    }

    /// Surface for the string form of milestone 4: on profile A, index 0 is the default
    /// and index 1, the contract's shard, is rejected.
    #[allow(dead_code)]
    pub fn shard(mut self, shard_index: usize) -> Self {
        self.shard = shard_index;
        self
    }

    /// Only `Epochs::PreSpice` is runnable until milestones 3 and 6 land.
    #[allow(dead_code)]
    pub fn epochs(mut self, epochs: Epochs) -> Self {
        self.epochs = epochs;
        self
    }

    fn shard_id(&self) -> ShardId {
        self.shard_id.expect("the shard id is read off the layout at built")
    }
}

impl Debug for ShardSkip {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let epochs = match self.epochs {
            Epochs::PreSpice => None,
            Epochs::LastPreSpice => Some("last_pre_spice".to_string()),
            Epochs::FirstSpice => Some("first_spice".to_string()),
        };
        render(
            f,
            self.tag(),
            &[("shard", (self.shard != 0).then(|| self.shard.to_string())), ("epochs", epochs)],
        )
    }
}

impl Fault for ShardSkip {
    fn tag(&self) -> &'static str {
        "shard_skip"
    }

    fn validate(&self) -> Result<(), String> {
        if self.shard == CONTRACT_SHARD_INDEX {
            return Err(format!(
                "shard {} holds the contract: silencing it keeps the deploy and the call backlog \
                 in the pool, and the shard's first spice chunks congest until the pool's \
                 transactions expire",
                self.shard,
            ));
        }
        match self.epochs {
            Epochs::PreSpice => Ok(()),
            Epochs::LastPreSpice => {
                Err("epochs=last_pre_spice needs setup:vote=epoch(N), which lands in milestone 6"
                    .to_string())
            }
            Epochs::FirstSpice => {
                Err("epochs=first_spice lands in milestone 3 with the adversarial chunk mode"
                    .to_string())
            }
        }
    }

    fn on_progress(
        &mut self,
        env: &mut TestLoopEnv,
        timeline: &Timeline,
        progress: &Progress,
    ) -> Vec<FaultEvent> {
        match progress {
            Progress::Built => {
                let shard_layout = shard_layout_at_head(env);
                let shard_id = shard_layout
                    .get_shard_id(self.shard)
                    .unwrap_or_else(|_| panic!("shard index {} is out of range", self.shard));
                self.shard_id = Some(shard_id);
                let node = env.rpc_node();
                let epoch_manager = node.client().epoch_manager.as_ref();
                let pre_spice_epoch_height =
                    epoch_manager.get_epoch_info(&node.head().epoch_id).unwrap().epoch_height();
                env.add_drop_condition(DropCondition::ChunksForShardsInEpochs {
                    shards: HashSet::from([shard_id]),
                    epoch_heights: HashSet::from([pre_spice_epoch_height]),
                });
            }
            Progress::LastPreSpice => {
                let last_pre_spice = timeline.last_pre_spice.as_ref().unwrap();
                let last_included = last_pre_spice.chunks()[self.shard].height_included();
                self.silenced = Some(last_included + 1..last_pre_spice.header().height() + 1);
                let epoch_manager = env.rpc_node().client().epoch_manager.clone();
                self.last_pre_spice_epoch_start =
                    Some(epoch_manager.get_epoch_start_height(last_pre_spice.hash()).unwrap());
            }
            _ => {}
        }
        Vec::new()
    }

    fn relaxes(&self) -> Vec<Relaxation> {
        let shard_id = self.shard_id();
        let silenced: HashSet<(ShardId, BlockHeight)> = self
            .silenced
            .clone()
            .expect("the silenced range is read off the chain at last_pre_spice")
            .map(|height| (shard_id, height))
            .collect();
        vec![
            Relaxation {
                oracle: OracleId::ChunkCompleteness,
                narrowing: Narrowing::ShardHeights(silenced),
            },
            Relaxation {
                oracle: OracleId::LoadInFlight,
                narrowing: Narrowing::Shards(HashSet::from([shard_id])),
            },
        ]
    }

    fn assert_fired(&self, _env: &TestLoopEnv, _timeline: &Timeline) {
        let silenced = self.silenced.clone().expect("the silenced range is read at last_pre_spice");
        let last_included = silenced.start - 1;
        let epoch_start = self.last_pre_spice_epoch_start.unwrap();
        assert!(
            last_included < epoch_start,
            "shard {}'s chunk at the last pre-spice block was included at height \
             {last_included}, inside the last pre-spice epoch starting at {epoch_start}: the \
             shard was not silent through it",
            self.shard,
        );
    }
}

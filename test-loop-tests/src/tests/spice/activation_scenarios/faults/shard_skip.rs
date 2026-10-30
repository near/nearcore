//! `shard_skip`: a shard silenced from warmup through the last pre-spice block, or for the
//! whole first spice epoch.

use super::super::loads::CONTRACT_SHARD_INDEX;
use super::super::scenario::{
    Fault, FaultEvent, Narrowing, OracleId, Progress, Relaxation, Timeline,
};
use super::{render, shard_layout_at_head};
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use near_client::NetworkAdversarialMessage;
use near_client::client_actor::AdvProduceChunksMode;
use near_primitives::types::{BlockHeight, ShardId};
use std::any::Any;
use std::collections::HashSet;
use std::fmt::{self, Debug, Formatter};
use std::ops::RangeInclusive;

/// Which epochs the shard is silent in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
// `LastPreSpice` is rejected until milestone 6.
#[allow(dead_code)]
pub enum Epochs {
    /// From the first chunk the dropper may touch through the last pre-spice block. Both
    /// pre-spice epochs: they share the genesis epoch info and so its epoch height.
    PreSpice,
    /// The last pre-spice epoch only; needs `setup:vote=epoch(N)`, milestone 6.
    LastPreSpice,
    /// The whole first spice epoch, through the adversarial chunk mode on the shard's
    /// producers: the endorsement drop has no effect under spice, and a chunk that is
    /// never produced is applied the same way as one whose parts never arrive.
    FirstSpice,
}

/// A shard silenced through `DropCondition::ChunksForShardsInEpochs` keyed on the
/// pre-spice epoch height, so that the undelivered-receipt carriers span an epoch boundary
/// and the receipts pending for the shard reach back to warmup; or, for the first spice
/// epoch, through `AdvProduceChunks(StopProduce)` on every chunk producer the epoch
/// assigns to the shard, sent when the head is the last pre-spice block and lifted when
/// the epoch is left. The mode lands at `H` or `H + 1`, since the producer of `H`'s chunk
/// may have processed the last pre-spice block before the message; the fault records which.
pub struct ShardSkip {
    shard: usize,
    epochs: Epochs,
    shard_id: Option<ShardId>,
    /// The heights the shard had no new chunk at, read off the chain: for `pre_spice`
    /// from the height after its last new chunk through the last pre-spice block, for
    /// `first_spice` from the height the silence started through the epoch's last block.
    silenced: Option<RangeInclusive<BlockHeight>>,
    /// The first height of the last pre-spice epoch, for the `pre_spice` fired check.
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

    /// On profile A, index 0 is the default
    /// and index 1, the contract's shard, is rejected.
    #[allow(dead_code)]
    pub fn shard(mut self, shard_index: usize) -> Self {
        self.shard = shard_index;
        self
    }

    pub fn epochs(mut self, epochs: Epochs) -> Self {
        self.epochs = epochs;
        self
    }

    fn shard_id(&self) -> ShardId {
        self.shard_id.expect("the shard id is read off the layout at built")
    }

    /// Sends `mode` to every chunk producer the first spice epoch assigns to the shard.
    fn set_producers_mode(&self, env: &mut TestLoopEnv, timeline: &Timeline, stop: bool) {
        let last_pre_spice = timeline.last_pre_spice.as_ref().unwrap();
        let producers = {
            let node = env.rpc_node();
            let epoch_manager = node.client().epoch_manager.as_ref();
            let spice_epoch_id =
                epoch_manager.get_epoch_id_from_prev_block(last_pre_spice.hash()).unwrap();
            epoch_manager
                .get_epoch_chunk_producers_for_shard(&spice_epoch_id, self.shard_id())
                .unwrap()
        };
        for account_id in producers {
            env.runner_for_account(&account_id).send_adversarial_message(
                NetworkAdversarialMessage::AdvProduceChunks(if stop {
                    AdvProduceChunksMode::StopProduce
                } else {
                    AdvProduceChunksMode::Valid
                }),
            );
        }
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

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn validate(&self) -> Result<(), String> {
        if self.shard == CONTRACT_SHARD_INDEX && self.epochs == Epochs::PreSpice {
            return Err(format!(
                "shard {} holds the contract: silencing it keeps the deploy and the call backlog \
                 in the pool, and the shard's first spice chunks congest until the pool's \
                 transactions expire",
                self.shard,
            ));
        }
        match self.epochs {
            Epochs::PreSpice | Epochs::FirstSpice => Ok(()),
            Epochs::LastPreSpice => {
                Err("epochs=last_pre_spice needs setup:vote=epoch(N), which lands in milestone 6"
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
                if self.epochs == Epochs::PreSpice {
                    let node = env.rpc_node();
                    let epoch_manager = node.client().epoch_manager.as_ref();
                    let pre_spice_epoch_height =
                        epoch_manager.get_epoch_info(&node.head().epoch_id).unwrap().epoch_height();
                    env.add_drop_condition(DropCondition::ChunksForShardsInEpochs {
                        shards: HashSet::from([shard_id]),
                        epoch_heights: HashSet::from([pre_spice_epoch_height]),
                    });
                }
            }
            Progress::LastPreSpice => match self.epochs {
                Epochs::PreSpice => {
                    let last_pre_spice = timeline.last_pre_spice.as_ref().unwrap();
                    let last_included = last_pre_spice.chunks()[self.shard].height_included();
                    self.silenced = Some(last_included + 1..=last_pre_spice.header().height());
                    let epoch_manager = env.rpc_node().client().epoch_manager.clone();
                    self.last_pre_spice_epoch_start =
                        Some(epoch_manager.get_epoch_start_height(last_pre_spice.hash()).unwrap());
                }
                Epochs::FirstSpice => self.set_producers_mode(env, timeline, true),
                Epochs::LastPreSpice => {}
            },
            Progress::EpochAfter(1) if self.epochs == Epochs::FirstSpice => {
                self.set_producers_mode(env, timeline, false);
            }
            Progress::Done if self.epochs == Epochs::FirstSpice => {
                // The silence runs from the first spice height with no new chunk to the
                // height before the shard's next new chunk, which comes a block or two
                // after the mode was lifted on leaving the epoch: the producers had already
                // skipped the chunks of the heights they were processing.
                let first_spice_height = timeline.first_spice_height().unwrap();
                let node = env.rpc_node();
                let chain = &node.client().chain;
                let head = node.head().height;
                let is_new = |height: BlockHeight| {
                    chain
                        .get_block_by_height(height)
                        .is_ok_and(|block| block.chunks()[self.shard].is_new_chunk(height))
                };
                let silence_start = (first_spice_height..=head).find(|height| !is_new(*height));
                let silence_start = silence_start.unwrap_or_else(|| {
                    panic!("shard {} had a new chunk at every spice height", self.shard)
                });
                let silence_end =
                    (silence_start..=head).find(|height| is_new(*height)).map_or(head, |h| h - 1);
                self.silenced = Some(silence_start..=silence_end);
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
            .expect("the silenced range is read off the chain")
            .map(|height| (shard_id, height))
            .collect();
        let mut relaxations = vec![Relaxation {
            oracle: OracleId::ChunkCompleteness,
            narrowing: Narrowing::ShardHeights(silenced),
        }];
        match self.epochs {
            Epochs::PreSpice => relaxations.push(Relaxation {
                oracle: OracleId::LoadInFlight,
                narrowing: Narrowing::Shards(HashSet::from([shard_id])),
            }),
            // The producers' pools diverge while nothing is produced, so once production
            // resumes a transaction of the silenced shard's senders can be included after a
            // later nonce of its account and fail with a stale nonce.
            Epochs::FirstSpice => relaxations.push(Relaxation {
                oracle: OracleId::ExactlyOnce,
                narrowing: Narrowing::Shards(HashSet::from([shard_id])),
            }),
            Epochs::LastPreSpice => {}
        }
        relaxations
    }

    fn assert_fired(&self, env: &TestLoopEnv, timeline: &Timeline) {
        let silenced = self.silenced.clone().expect("the silenced range is read off the chain");
        match self.epochs {
            Epochs::PreSpice => {
                let last_included = silenced.start() - 1;
                let epoch_start = self.last_pre_spice_epoch_start.unwrap();
                assert!(
                    last_included < epoch_start,
                    "shard {}'s chunk at the last pre-spice block was included at height \
                     {last_included}, inside the last pre-spice epoch starting at {epoch_start}: \
                     the shard was not silent through it",
                    self.shard,
                );
            }
            Epochs::FirstSpice => {
                let first_spice_height = timeline.first_spice_height().unwrap();
                assert!(
                    *silenced.start() <= first_spice_height + 1,
                    "shard {}'s silence started at height {}, more than one block after the \
                     first spice block at {first_spice_height}",
                    self.shard,
                    silenced.start(),
                );
                let chain = &env.rpc_node().client().chain;
                for height in silenced {
                    let Ok(block) = chain.get_block_by_height(height) else {
                        continue;
                    };
                    assert!(
                        !block.chunks()[self.shard].is_new_chunk(height),
                        "shard {} has a new chunk at height {height}, inside its silence",
                        self.shard,
                    );
                }
            }
            Epochs::LastPreSpice => unreachable!("rejected by validate"),
        }
    }
}

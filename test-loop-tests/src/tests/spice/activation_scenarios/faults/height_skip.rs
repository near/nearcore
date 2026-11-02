//! `height_skip`: block heights skipped around the boundary.

use super::super::resolve::Resolved;
use super::super::scenario::{
    Fault, FaultEvent, Narrowing, OracleId, Progress, Relaxation, Timeline,
};
use super::render;
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use crate::utils::gaps::block_producer_at;
use near_chain::ChainStoreAccess;
use near_primitives::types::BlockHeight;
use std::any::Any;
use std::collections::HashSet;
use std::fmt::{self, Debug, Formatter};
use std::ops::Range;

const DEFAULT_AT: Range<i64> = -1..0;

/// `DropCondition::BlocksByHeight` installed at `boundary_known` for the heights at the
/// offsets, measured against the estimate: the skip is what moves the boundary, so it
/// cannot be keyed on the boundary. Only the dropped block's producer has it as head, so
/// the other producers time out, send skip approvals, and the next block builds on the
/// one below the gap; the network rules swallow the requests for the dropped block that
/// the producer's own child would otherwise trigger. The default skip at `-1` moves the
/// real boundary three heights up, see the design's trace.
pub struct HeightSkip {
    at: Range<i64>,
    /// The absolute heights, from `boundary_known`.
    heights: Vec<BlockHeight>,
    /// The injected heights and, above each, the run of heights the block producer
    /// assignment gives to the same producer, which the others skip as well since they
    /// never see the parent; read off the chain at `done`.
    relaxed: HashSet<BlockHeight>,
}

impl Default for HeightSkip {
    fn default() -> Self {
        Self::new()
    }
}

impl HeightSkip {
    /// The height right below the estimate skipped.
    pub fn new() -> Self {
        Self { at: DEFAULT_AT, heights: Vec::new(), relaxed: HashSet::new() }
    }

    /// The offsets to skip, against the estimate, half-open.
    pub fn at(mut self, at: Range<i64>) -> Self {
        self.at = at;
        self
    }
}

impl Debug for HeightSkip {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        render(
            f,
            self.tag(),
            &[(
                "at",
                (self.at != DEFAULT_AT).then(|| format!("{}..{}", self.at.start, self.at.end)),
            )],
        )
    }
}

impl Fault for HeightSkip {
    fn tag(&self) -> &'static str {
        "height_skip"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn validate(&self, _resolved: Resolved) -> Result<(), String> {
        if self.at.is_empty() {
            return Err(format!("the range {}..{} is empty", self.at.start, self.at.end));
        }
        // Each dropped block's producer keeps it as its head and approves its own fork
        // from then on; two consecutive skips by different producers leave two of the four
        // producers there, half the producer stake, and Doomslug cannot approve the block
        // above the gap (measured: the chain stalled below the first skipped height).
        if self.at.end - self.at.start > 1 {
            return Err(format!(
                "the range {}..{} skips more than one height: two consecutive skipped heights \
                 by different producers leave half the producer stake on their own forks and \
                 stall the chain",
                self.at.start, self.at.end,
            ));
        }
        Ok(())
    }

    /// A pre-spice skip delays the end of the last pre-spice epoch; a skip at or above the
    /// estimate leaves the last pre-spice block where it is and moves the first spice one.
    fn moves_boundary(&self, _resolved: Resolved) -> bool {
        self.at.start < 0
    }

    fn on_progress(
        &mut self,
        env: &mut TestLoopEnv,
        _timeline: &Timeline,
        progress: &Progress,
    ) -> Vec<FaultEvent> {
        match progress {
            Progress::BoundaryKnown { estimate } => {
                self.heights = self
                    .at
                    .clone()
                    .map(|offset| (*estimate as i64 + offset) as BlockHeight)
                    .collect();
                env.add_drop_condition(DropCondition::BlocksByHeight(
                    self.heights.iter().copied().collect(),
                ));
            }
            Progress::Done => {
                let node = env.rpc_node();
                let chain = &node.client().chain;
                let exists = |height: BlockHeight| {
                    chain.chain_store.get_block_hash_by_height(height).is_ok()
                };
                for &height in &self.heights {
                    self.relaxed.insert(height);
                    let producer = block_producer_at(&node, height);
                    let mut above = height + 1;
                    while !exists(above) && block_producer_at(&node, above) == producer {
                        self.relaxed.insert(above);
                        above += 1;
                    }
                }
            }
            _ => {}
        }
        Vec::new()
    }

    fn relaxes(&self) -> Vec<Relaxation> {
        vec![Relaxation {
            oracle: OracleId::HeightContiguity,
            narrowing: Narrowing::Heights(self.relaxed.clone()),
        }]
    }

    /// No block exists at the injected heights on the driver node; the requests the rules
    /// swallowed are reported.
    fn assert_fired(&self, env: &TestLoopEnv, _timeline: &Timeline) {
        assert!(!self.heights.is_empty(), "the offsets were never resolved");
        let chain = &env.rpc_node().client().chain;
        for &height in &self.heights {
            assert!(
                chain.chain_store.get_block_hash_by_height(height).is_err(),
                "a block exists at height {height}, which the fault skipped",
            );
        }
        let kept = env.shared_state.network_rules.kept_blocks();
        tracing::info!(
            target: "test",
            heights = ?self.heights,
            dropped = kept.hashes.len(),
            requests_swallowed = kept.requests_swallowed,
            header_requests_swallowed = kept.header_requests_swallowed,
            "height_skip dropped the blocks at the injected heights",
        );
    }
}

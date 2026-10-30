//! `endorsement_delay`: endorsements held back during a window of heights.

use super::super::resolve::Profile;
use super::super::scenario::{
    Fault, FaultEvent, Narrowing, OracleId, Progress, Relaxation, Timeline,
};
use super::render;
use crate::setup::env::TestLoopEnv;
use near_chain::spice::all_stake_fallback::SPICE_FALLBACK_CERTIFICATION_DELAY;
use near_primitives::types::{BlockHeight, SpiceChunkId};
use std::any::Any;
use std::fmt::{self, Debug, Formatter};

/// Where the window opens.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowStart {
    WarmedUp,
    /// A height offset, resolved as `height(k)` is.
    Offset(i64),
}

/// Where the window closes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowEnd {
    Done,
    /// A height offset, resolved as `height(k)` is.
    Offset(i64),
}

const DEFAULT_BLOCKS: u64 = 3;
const DEFAULT_START: WindowStart = WindowStart::Offset(-2);
const DEFAULT_END: WindowEnd = WindowEnd::Offset(1);

/// Endorsements held back for `blocks` blocks between `start` and `end`, through
/// `delay_endorsements_propagation`. A start earlier than the height at which the estimate
/// becomes known opens at `boundary_known` instead; after `end`, new endorsements pass
/// straight through and the queued ones drain on the next block.
pub struct EndorsementDelay {
    blocks: u64,
    start: WindowStart,
    end: WindowEnd,
    /// The height the window opened at and the delayed count then.
    opened: Option<(BlockHeight, u64)>,
    /// The height the window closed at and the delayed count then.
    closed: Option<(BlockHeight, u64)>,
    opened_at_boundary_known: bool,
}

impl Default for EndorsementDelay {
    fn default() -> Self {
        Self::new()
    }
}

impl EndorsementDelay {
    /// Three blocks of delay from two heights below the boundary to one above it.
    pub fn new() -> Self {
        Self {
            blocks: DEFAULT_BLOCKS,
            start: DEFAULT_START,
            end: DEFAULT_END,
            opened: None,
            closed: None,
            opened_at_boundary_known: false,
        }
    }

    pub fn blocks(mut self, blocks: u64) -> Self {
        self.blocks = blocks;
        self
    }

    pub fn start(mut self, start: WindowStart) -> Self {
        self.start = start;
        self
    }

    pub fn end(mut self, end: WindowEnd) -> Self {
        self.end = end;
        self
    }

    fn is_open(&self) -> bool {
        self.opened.is_some() && self.closed.is_none()
    }

    fn open(&mut self, env: &TestLoopEnv) {
        assert!(self.opened.is_none(), "the window opens once");
        env.delay_endorsements_propagation(self.blocks);
        self.opened = Some((env.rpc_node().head().height, env.delayed_endorsements()));
    }

    fn close(&mut self, env: &TestLoopEnv) {
        assert!(self.is_open(), "the window closes once, after it opened");
        env.delay_endorsements_propagation(0);
        self.closed = Some((env.rpc_node().head().height, env.delayed_endorsements()));
    }

    /// Whether the window is open when the endorsements of the boundary chunks are sent,
    /// which happens as the last pre-spice block is processed, at offset `-1`: the boundary
    /// bootstrap runs in that block's postprocessing and the validators endorse from its
    /// witnesses right away, so they can land in the first spice block. A window closing at
    /// `-1` closes once the driver has seen that block, which may be after some of them went
    /// out, so it does not count as covering.
    fn covers_boundary(&self) -> bool {
        let starts_in_time = match self.start {
            WindowStart::WarmedUp => true,
            WindowStart::Offset(start) => start <= -1,
        };
        let ends_in_time = match self.end {
            WindowEnd::Done => true,
            WindowEnd::Offset(end) => end >= 0,
        };
        starts_in_time && ends_in_time
    }
}

impl Debug for EndorsementDelay {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let start = match self.start {
            WindowStart::WarmedUp => Some("warmed_up".to_string()),
            WindowStart::Offset(start) => (start != -2).then(|| start.to_string()),
        };
        let end = match self.end {
            WindowEnd::Done => Some("done".to_string()),
            WindowEnd::Offset(end) => (end != 1).then(|| end.to_string()),
        };
        render(
            f,
            self.tag(),
            &[
                ("blocks", (self.blocks != DEFAULT_BLOCKS).then(|| self.blocks.to_string())),
                ("start", start),
                ("end", end),
            ],
        )
    }
}

impl Fault for EndorsementDelay {
    fn tag(&self) -> &'static str {
        "endorsement_delay"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn validate(&self, _profile: Profile) -> Result<(), String> {
        if self.blocks == 0 {
            return Err("blocks must be at least 1".to_string());
        }
        if let (WindowStart::Offset(start), WindowEnd::Offset(end)) = (self.start, self.end) {
            if start >= end {
                return Err(format!("the window start {start} must be below its end {end}"));
            }
        }
        Ok(())
    }

    fn on_progress(
        &mut self,
        env: &mut TestLoopEnv,
        _timeline: &Timeline,
        progress: &Progress,
    ) -> Vec<FaultEvent> {
        match progress {
            Progress::WarmedUp if self.start == WindowStart::WarmedUp => self.open(env),
            Progress::BoundaryKnown { estimate } => {
                // A start below the offset the estimate becomes known at cannot fire as a
                // height; it opens here instead, and the run's output says so.
                let offset_now = env.rpc_node().head().height as i64 - *estimate as i64;
                if let WindowStart::Offset(start) = self.start {
                    if start < offset_now && self.opened.is_none() {
                        tracing::info!(
                            target: "test",
                            start,
                            offset_now,
                            "endorsement_delay window start is before the estimate was known, opening at boundary_known",
                        );
                        self.opened_at_boundary_known = true;
                        self.open(env);
                    }
                }
            }
            Progress::Height { offset, .. } => {
                if self.start == WindowStart::Offset(*offset) && self.opened.is_none() {
                    self.open(env);
                }
                if self.end == WindowEnd::Offset(*offset) && self.is_open() {
                    self.close(env);
                }
            }
            _ => {}
        }
        Vec::new()
    }

    fn relaxes(&self) -> Vec<Relaxation> {
        // The rule releases an endorsement of a block at height `h` once the node has seen a
        // block above `h + blocks`, and the released endorsement lands on chain in the block
        // after that, so certification trails the block by `blocks + 2` heights.
        const RELEASE_AND_INCLUSION: u64 = 2;
        if self.blocks + RELEASE_AND_INCLUSION >= SPICE_FALLBACK_CERTIFICATION_DELAY
            && self.covers_boundary()
        {
            return vec![Relaxation {
                oracle: OracleId::WithinWindow,
                narrowing: Narrowing::Whole,
            }];
        }
        Vec::new()
    }

    fn assert_fired(&self, env: &TestLoopEnv, timeline: &Timeline) {
        let (open_height, delayed_at_open) = self.opened.expect("the window never opened");
        let delayed_at_close = match self.closed {
            Some((_, delayed)) => delayed,
            None => env.delayed_endorsements(),
        };
        assert!(
            delayed_at_close > delayed_at_open,
            "no endorsement was queued inside the window opened at height {open_height}",
        );
        if !self.covers_boundary() {
            return;
        }
        // The boundary chunks' endorsements were held, so they certified late.
        let node = env.rpc_node();
        let last_pre_spice = timeline.last_pre_spice.as_ref().unwrap();
        let boundary_height = last_pre_spice.header().height();
        let shard_layout = node
            .client()
            .epoch_manager
            .get_shard_layout(last_pre_spice.header().epoch_id())
            .unwrap();
        for shard_id in shard_layout.shard_ids() {
            let chunk_id = SpiceChunkId { block_hash: *last_pre_spice.hash(), shard_id };
            let (certifying_height, _) =
                node.certifying_height_and_endorsers(&chunk_id).unwrap_or_else(|| {
                    panic!("the boundary chunk of shard {shard_id} never certified")
                });
            assert!(
                certifying_height > boundary_height + 1,
                "the boundary chunk of shard {shard_id} certified at height {certifying_height}, \
                 right after the last pre-spice block at {boundary_height}: the delay did not \
                 hold its endorsements",
            );
        }
    }
}

//! `missing_chunk`: chunks dropped at heights around the boundary.

use super::super::oracles::observed_chunk_pattern;
use super::super::scenario::{
    Fault, FaultEvent, Narrowing, OracleId, Progress, Relaxation, Timeline,
};
use super::{render, shard_layout_at_head};
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use near_primitives::types::{BlockHeight, ShardId};
use near_primitives::version::ProtocolFeature;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug, Formatter};
use std::ops::Range;

/// Which shards, by shard index, a range of offsets applies to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Shards {
    All,
    Index(usize),
    /// Surface for the string form of milestone 4; no preset needs it.
    #[allow(dead_code)]
    Indices(Vec<usize>),
}

impl Shards {
    fn indices(&self, num_shards: usize) -> Vec<usize> {
        match self {
            Shards::All => (0..num_shards).collect(),
            Shards::Index(index) => vec![*index],
            Shards::Indices(indices) => indices.clone(),
        }
    }

    fn render(&self) -> Option<String> {
        match self {
            Shards::All => None,
            Shards::Index(index) => Some(index.to_string()),
            Shards::Indices(indices) => {
                Some(indices.iter().map(usize::to_string).collect::<Vec<_>>().join("+"))
            }
        }
    }
}

/// How a chunk is kept out of its block: by dropping its pre-spice endorsements, or by
/// dropping its parts so the header never reaches the producer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum How {
    Endorsements,
    Parts,
}

/// One shard's requested chunk presence over a run of heights, resolved to absolute
/// heights at `boundary_known`: `x` missing, `.` present, oldest first.
struct ResolvedPattern {
    shard_index: usize,
    shard_id: ShardId,
    heights: Range<BlockHeight>,
    pattern: String,
}

/// Chunks dropped at heights around the boundary: a range of offsets for a set of shards,
/// or a per-shard pattern over the last pre-spice heights as in the mixed-pattern test.
///
/// Pre-spice offsets go through `DropCondition::ProtocolUpgradeChunkRange`, which
/// measures from the estimate; nothing moves the boundary until milestone 6, so that is
/// exact at `-1` too. Offsets at or after `0` and `how=parts` need the chunk-part rules of
/// milestone 7 and are rejected until then.
pub struct MissingChunk {
    shards: Shards,
    at: Range<i64>,
    /// Per shard index, the chunk presence over the last `len` pre-spice heights. When
    /// non-empty it replaces `shards` and `at`.
    patterns: Vec<(usize, String)>,
    how: How,
    /// Shard index to shard id, read off the layout at `built`.
    shard_ids: Vec<ShardId>,
    resolved: Vec<ResolvedPattern>,
}

const DEFAULT_AT: Range<i64> = -1..0;

impl Default for MissingChunk {
    fn default() -> Self {
        Self::new()
    }
}

impl MissingChunk {
    /// Both shards' chunks missing at the last pre-spice block.
    pub fn new() -> Self {
        Self {
            shards: Shards::All,
            at: DEFAULT_AT,
            patterns: Vec::new(),
            how: How::Endorsements,
            shard_ids: Vec::new(),
            resolved: Vec::new(),
        }
    }

    pub fn shards(mut self, shards: Shards) -> Self {
        self.shards = shards;
        self
    }

    /// The offsets to drop at, relative to the estimated first spice height, half-open.
    pub fn at(mut self, at: Range<i64>) -> Self {
        self.at = at;
        self
    }

    /// Adds a per-shard pattern: the presence of the shard's chunks over the last
    /// `pattern.len()` pre-spice heights, oldest first, `x` missing and `.` present.
    pub fn pattern(mut self, shard_index: usize, pattern: &str) -> Self {
        self.patterns.push((shard_index, pattern.to_string()));
        self
    }

    /// Rejected until the chunk-part rules of milestone 7 land.
    #[allow(dead_code)]
    pub fn how(mut self, how: How) -> Self {
        self.how = how;
        self
    }

    /// Per shard index, the offsets to drop.
    fn requested(&self, num_shards: usize) -> Vec<(usize, Vec<i64>)> {
        if self.patterns.is_empty() {
            let offsets: Vec<i64> = self.at.clone().collect();
            return self
                .shards
                .indices(num_shards)
                .into_iter()
                .map(|index| (index, offsets.clone()))
                .collect();
        }
        self.patterns
            .iter()
            .map(|(index, pattern)| {
                let first_offset = -(pattern.len() as i64);
                let offsets = pattern
                    .chars()
                    .enumerate()
                    .filter(|(_, presence)| *presence == 'x')
                    .map(|(position, _)| first_offset + position as i64)
                    .collect();
                (*index, offsets)
            })
            .collect()
    }

    /// Per shard index, the heights the drops land at and the pattern they make.
    fn resolve(&self, estimate: BlockHeight) -> Vec<ResolvedPattern> {
        let mut resolved = Vec::new();
        if self.patterns.is_empty() {
            for index in self.shards.indices(self.shard_ids.len()) {
                let heights =
                    offset_height(estimate, self.at.start)..offset_height(estimate, self.at.end);
                let pattern = "x".repeat((heights.end - heights.start) as usize);
                resolved.push(ResolvedPattern {
                    shard_index: index,
                    shard_id: self.shard_ids[index],
                    heights,
                    pattern,
                });
            }
        } else {
            for (index, pattern) in &self.patterns {
                let first_height = estimate - pattern.len() as BlockHeight;
                resolved.push(ResolvedPattern {
                    shard_index: *index,
                    shard_id: self.shard_ids[*index],
                    heights: first_height..estimate,
                    pattern: pattern.clone(),
                });
            }
        }
        resolved
    }
}

fn offset_height(estimate: BlockHeight, offset: i64) -> BlockHeight {
    (estimate as i64 + offset) as BlockHeight
}

impl Debug for MissingChunk {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let patterns = (!self.patterns.is_empty()).then(|| {
            self.patterns
                .iter()
                .map(|(index, pattern)| format!("{index}:{pattern}"))
                .collect::<Vec<_>>()
                .join("+")
        });
        render(
            f,
            self.tag(),
            &[
                ("shards", self.shards.render()),
                (
                    "at",
                    (self.at != DEFAULT_AT).then(|| format!("{}..{}", self.at.start, self.at.end)),
                ),
                ("pattern", patterns),
                ("how", (self.how == How::Parts).then(|| "parts".to_string())),
            ],
        )
    }
}

impl Fault for MissingChunk {
    fn tag(&self) -> &'static str {
        "missing_chunk"
    }

    fn validate(&self) -> Result<(), String> {
        if self.how == How::Parts {
            return Err("how=parts needs the chunk-part rules of milestone 7".to_string());
        }
        if self.patterns.is_empty() && self.at.is_empty() {
            return Err(format!("the range {}..{} is empty", self.at.start, self.at.end));
        }
        if self.patterns.is_empty() && self.at.end > 0 {
            return Err(format!(
                "the range {}..{} reaches the first spice block; offsets at or after 0 need the \
                 chunk-part rules of milestone 7",
                self.at.start, self.at.end,
            ));
        }
        for (index, pattern) in &self.patterns {
            if pattern.is_empty()
                || pattern.chars().any(|presence| presence != '.' && presence != 'x')
            {
                return Err(format!(
                    "pattern {pattern:?} of shard {index} must be non-empty and made of '.' and 'x'"
                ));
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
            Progress::Built => {
                let shard_layout = shard_layout_at_head(env);
                let num_shards = shard_layout.num_shards() as usize;
                self.shard_ids = (0..num_shards)
                    .map(|index| shard_layout.get_shard_id(index).unwrap())
                    .collect();
                for (index, offsets) in self.requested(num_shards) {
                    assert!(
                        index < num_shards,
                        "shard index {index} is out of range: {num_shards} shards"
                    );
                    for offset in offsets {
                        env.add_drop_condition(DropCondition::ProtocolUpgradeChunkRange(
                            ProtocolFeature::Spice.protocol_version(),
                            HashMap::from([(index, offset..offset + 1)]),
                        ));
                    }
                }
            }
            Progress::BoundaryKnown { estimate } => self.resolved = self.resolve(*estimate),
            _ => {}
        }
        Vec::new()
    }

    fn relaxes(&self) -> Vec<Relaxation> {
        let pairs: HashSet<(ShardId, BlockHeight)> = self
            .resolved
            .iter()
            .flat_map(|resolved| {
                resolved.pattern.chars().enumerate().filter(|(_, presence)| *presence == 'x').map(
                    move |(position, _)| {
                        (resolved.shard_id, resolved.heights.start + position as BlockHeight)
                    },
                )
            })
            .collect();
        vec![Relaxation {
            oracle: OracleId::ChunkCompleteness,
            narrowing: Narrowing::ShardHeights(pairs),
        }]
    }

    fn assert_fired(&self, env: &TestLoopEnv, _timeline: &Timeline) {
        assert!(
            !self.resolved.is_empty(),
            "the offsets were never resolved: the estimate never became known"
        );
        let chain = &env.rpc_node().client().chain;
        for resolved in &self.resolved {
            let observed =
                observed_chunk_pattern(chain, resolved.shard_index, resolved.heights.clone());
            assert_eq!(
                observed,
                resolved.pattern,
                "shard {} has chunk pattern {observed} over heights {}..{}, the fault asked for {}",
                resolved.shard_index,
                resolved.heights.start,
                resolved.heights.end,
                resolved.pattern,
            );
        }
    }
}

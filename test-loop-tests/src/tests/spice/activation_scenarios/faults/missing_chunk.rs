//! `missing_chunk`: chunks dropped at heights around the boundary.

use super::super::resolve::Resolved;
use super::super::scenario::{
    Fault, FaultEvent, Narrowing, OracleId, Progress, Relaxation, Timeline,
};
use super::{render, shard_layout_at_head};
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use near_primitives::types::{BlockHeight, ShardId};
use near_primitives::version::ProtocolFeature;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug, Formatter};
use std::ops::Range;

/// Which shards, by shard index, a range of offsets applies to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Shards {
    All,
    Index(usize),
    /// No preset needs it yet.
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

/// One requested chunk presence, resolved to an absolute height at `last_pre_spice`: the
/// offset `-1` to the last pre-spice block wherever it landed, the others against the
/// estimate.
struct ResolvedChunk {
    shard_index: usize,
    shard_id: ShardId,
    offset: i64,
    height: BlockHeight,
    missing: bool,
}

/// Chunks dropped at heights around the boundary: a range of offsets for a set of shards,
/// or a per-shard pattern over the last pre-spice heights as in the mixed-pattern test.
///
/// Offset `-1` goes through `DropCondition::LastPreSpiceChunks`, which drops the chunks of
/// whichever block turns out to be the last pre-spice block, decided per chunk from its
/// parent; the offsets below it go through `DropCondition::ProtocolUpgradeChunkRange`,
/// which measures from the estimate, so under a fault that moves the boundary they land
/// earlier, relative to the real boundary, than asked, and the fault proves exactly the
/// heights it resolved. Offsets at or after `0` and `how=parts` need the chunk-part rules
/// of milestone 7 and are rejected until then.
pub struct MissingChunk {
    shards: Shards,
    at: Range<i64>,
    /// Per shard index, the chunk presence over the last `len` pre-spice heights. When
    /// non-empty it replaces `shards` and `at`.
    patterns: Vec<(usize, String)>,
    how: How,
    /// Shard index to shard id, read off the layout at `built`.
    shard_ids: Vec<ShardId>,
    resolved: Vec<ResolvedChunk>,
    /// The chunks the boundary predicate dropped at the estimate's boundary heights when
    /// the boundary moved past them, which a block at those heights then lacks.
    predicate_misses: HashSet<(ShardId, BlockHeight)>,
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
            predicate_misses: HashSet::new(),
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

    /// Per shard index, every offset the fault speaks for and whether the chunk is to be
    /// missing there: the dropped offsets of a range, every offset of a pattern.
    fn requested(&self, num_shards: usize) -> Vec<(usize, Vec<(i64, bool)>)> {
        if self.patterns.is_empty() {
            let offsets: Vec<(i64, bool)> = self.at.clone().map(|offset| (offset, true)).collect();
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
                    .map(|(position, presence)| (first_offset + position as i64, presence == 'x'))
                    .collect();
                (*index, offsets)
            })
            .collect()
    }
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

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn validate(&self, _resolved: Resolved) -> Result<(), String> {
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
        timeline: &Timeline,
        progress: &Progress,
    ) -> Vec<FaultEvent> {
        match progress {
            Progress::Built => {
                let shard_layout = shard_layout_at_head(env);
                let num_shards = shard_layout.num_shards() as usize;
                self.shard_ids = (0..num_shards)
                    .map(|index| shard_layout.get_shard_id(index).unwrap())
                    .collect();
                let mut boundary_shards = HashSet::new();
                for (index, offsets) in self.requested(num_shards) {
                    assert!(
                        index < num_shards,
                        "shard index {index} is out of range: {num_shards} shards"
                    );
                    for (offset, missing) in offsets {
                        if !missing {
                            continue;
                        }
                        if offset == -1 {
                            boundary_shards.insert(self.shard_ids[index]);
                        } else {
                            env.add_drop_condition(DropCondition::ProtocolUpgradeChunkRange(
                                ProtocolFeature::Spice.protocol_version(),
                                HashMap::from([(index, offset..offset + 1)]),
                            ));
                        }
                    }
                }
                if !boundary_shards.is_empty() {
                    env.add_drop_condition(DropCondition::LastPreSpiceChunks {
                        shards: boundary_shards,
                    });
                }
            }
            Progress::LastPreSpice => {
                let estimate = timeline.estimate.expect("the estimate is known by the boundary");
                let boundary_height = timeline.last_pre_spice_height().unwrap();
                for (index, offsets) in self.requested(self.shard_ids.len()) {
                    for (offset, missing) in offsets {
                        let height = if offset == -1 {
                            boundary_height
                        } else {
                            (estimate as i64 + offset) as BlockHeight
                        };
                        self.resolved.push(ResolvedChunk {
                            shard_index: index,
                            shard_id: self.shard_ids[index],
                            offset,
                            height,
                            missing,
                        });
                        // The predicate takes every block that would end the epoch if it
                        // existed, so when a skip moves the boundary up it also hit the
                        // chunks of the skipped heights, which the block above the gap
                        // then lacks.
                        if offset == -1 && missing {
                            for height in estimate - 1..boundary_height {
                                self.predicate_misses.insert((self.shard_ids[index], height));
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        Vec::new()
    }

    fn relaxes(&self) -> Vec<Relaxation> {
        let mut pairs: HashSet<(ShardId, BlockHeight)> = self
            .resolved
            .iter()
            .filter(|resolved| resolved.missing)
            .map(|resolved| (resolved.shard_id, resolved.height))
            .collect();
        pairs.extend(self.predicate_misses.iter().copied());
        vec![Relaxation {
            oracle: OracleId::ChunkCompleteness,
            narrowing: Narrowing::ShardHeights(pairs),
        }]
    }

    /// The chunk mask at every resolved height says what the fault asked for: missing where
    /// it dropped, present where a pattern says so. A height with no block, under a fault
    /// that skips heights, has nothing to check.
    fn assert_fired(&self, env: &TestLoopEnv, _timeline: &Timeline) {
        assert!(!self.resolved.is_empty(), "the offsets were never resolved");
        let chain = &env.rpc_node().client().chain;
        for resolved in &self.resolved {
            let Ok(block) = chain.get_block_by_height(resolved.height) else {
                assert!(
                    !resolved.missing,
                    "no block at height {} for shard {}'s chunk at offset {} to be missing from",
                    resolved.height, resolved.shard_index, resolved.offset,
                );
                continue;
            };
            let is_new = block.chunks()[resolved.shard_index].is_new_chunk(resolved.height);
            assert_eq!(
                is_new,
                !resolved.missing,
                "shard {}'s chunk at offset {} (height {}) is {}, the fault asked for it to be {}",
                resolved.shard_index,
                resolved.offset,
                resolved.height,
                if is_new { "present" } else { "missing" },
                if resolved.missing { "missing" } else { "present" },
            );
        }
    }
}

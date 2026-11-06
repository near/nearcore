//! `missing_chunk`: chunks dropped at heights around the boundary.

use super::super::resolve::Resolved;
use super::super::scenario::{
    Fault, FaultEvent, Narrowing, OracleId, Progress, Relaxation, Timeline,
};
use super::{
    chunk_at_offset_selector, chunk_produced_by_block_producer, render, shard_layout_at_head,
};
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use crate::setup::network_rules::{Action, RuleHandle};
use near_chain::ChainStoreAccess;
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
/// dropping its parts so the header never reaches the block producer. Endorsements keep a
/// chunk out before spice only, where inclusion needs them; parts work on either side of
/// the boundary, with a different footprint: the chunk's body is then absent from every
/// node but its producer's rather than present and unendorsed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum How {
    Endorsements,
    Parts,
}

/// One requested chunk presence, resolved to an absolute height once the boundary blocks
/// are known: the offset `-1` to the last pre-spice block and `0` to the first spice block
/// wherever they landed, the others against the estimate.
struct ResolvedChunk {
    shard_index: usize,
    shard_id: ShardId,
    offset: i64,
    height: BlockHeight,
    missing: bool,
    /// Whether the chunk's parts were dropped rather than its endorsements.
    parts: bool,
    /// Whether the chunk's producer is also the block producer of its height, so that
    /// dropping its parts could not keep it out and the fault asked for nothing there.
    coincides: bool,
}

/// Chunks dropped at heights around the boundary: a range of offsets for a set of shards,
/// or a per-shard pattern over the last pre-spice heights as in the mixed-pattern test.
///
/// Before the boundary a chunk is kept out by its endorsements unless `how=parts`: offset
/// `-1` goes through `DropCondition::LastPreSpiceChunks`, which drops the chunks of
/// whichever block turns out to be the last pre-spice block, decided per chunk from its
/// parent, and the offsets below it through `DropCondition::ProtocolUpgradeChunkRange`,
/// which measures from the estimate. At and after the boundary, and under `how=parts`
/// anywhere, the chunk's parts are dropped by a chunk-part rule on every node, keyed the
/// same way, see `chunk_at_offset_selector`: the parts never reach the block producer, so
/// neither does the header, which is what keeps a chunk out under spice, where inclusion
/// checks no endorsements. The rules are keyed when the estimate is known, before any of
/// the chunks exists, since a chunk is produced as soon as its parent is processed and a
/// rule keyed later could miss it.
///
/// Under a fault that moves the boundary the offsets keyed against the estimate land
/// earlier, relative to the real boundary, than asked, and the fault proves exactly the
/// heights it resolved. A chunk whose producer is also the block producer at its height
/// cannot be kept out by its parts, since the producer has it without the network; the
/// fault reports it at `fired`, and a preset avoids such a pair.
pub struct MissingChunk {
    shards: Shards,
    at: Range<i64>,
    /// Per shard index, the chunk presence over the last `len` pre-spice heights. When
    /// non-empty it replaces `shards` and `at`.
    patterns: Vec<(usize, String)>,
    how: How,
    /// Shard index to shard id, read off the layout at `built`.
    shard_ids: Vec<ShardId>,
    /// The `(shard index, offset)` pairs whose chunk parts are dropped, keyed at
    /// `boundary_known`.
    by_parts: Vec<(usize, i64)>,
    rules: Vec<RuleHandle>,
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
            by_parts: Vec::new(),
            rules: Vec::new(),
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

    /// How the pre-spice chunks are kept out; the spice-side ones go by parts whatever this
    /// says.
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

    /// Whether the chunk at `offset` is kept out by its parts.
    fn drops_parts(&self, offset: i64) -> bool {
        self.how == How::Parts || offset >= 0
    }

    /// The `(shard index, offset)` pairs a pattern asks to be present, for the resolver's
    /// overlap checks.
    pub fn present_offsets(&self, num_shards: usize) -> Vec<(usize, i64)> {
        self.requested(num_shards)
            .into_iter()
            .flat_map(|(index, offsets)| {
                offsets
                    .into_iter()
                    .filter(|(_, missing)| !*missing)
                    .map(move |(offset, _)| (index, offset))
            })
            .collect()
    }

    /// Whether the fault names pre-spice heights by a pattern.
    pub fn has_patterns(&self) -> bool {
        !self.patterns.is_empty()
    }

    /// The `(shard index, offset)` pairs this fault drops, for the resolver's overlap checks.
    pub fn dropped_offsets(&self, num_shards: usize) -> Vec<(usize, i64)> {
        self.requested(num_shards)
            .into_iter()
            .flat_map(|(index, offsets)| {
                offsets
                    .into_iter()
                    .filter(|(_, missing)| *missing)
                    .map(move |(offset, _)| (index, offset))
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

    fn validate(&self, resolved: Resolved) -> Result<(), String> {
        if self.patterns.is_empty() && self.at.is_empty() {
            return Err(format!("the range {}..{} is empty", self.at.start, self.at.end));
        }
        let profile = resolved.profile;
        let by_parts: Vec<(usize, i64)> = self
            .dropped_offsets(profile.num_shards())
            .into_iter()
            .filter(|(_, offset)| self.drops_parts(*offset))
            .collect();
        if !by_parts.is_empty()
            && by_parts.iter().all(|(shard, offset)| {
                profile.chunk_is_block_producers_own(*shard, *offset) == Some(true)
            })
        {
            return Err(format!(
                "every chunk to drop by its parts, {by_parts:?} as (shard, offset), is its block \
                 producer's own on profile {profile:?}, which the producer includes without the \
                 network"
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
                        if self.drops_parts(offset) {
                            self.by_parts.push((index, offset));
                        } else if offset == -1 {
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
            Progress::BoundaryKnown { estimate } => {
                for &(index, offset) in &self.by_parts {
                    let shard_id = self.shard_ids[index];
                    for node in 0..env.node_datas.len() {
                        let epoch_manager = env.node(node).client().epoch_manager.clone();
                        let selects =
                            chunk_at_offset_selector(epoch_manager, *estimate, shard_id, offset);
                        self.rules.push(env.chunk_parts_rule(node, selects, Action::Drop));
                    }
                }
            }
            Progress::FirstSpice => {
                let estimate = timeline.estimate.expect("the estimate is known by the boundary");
                let boundary_height = timeline.last_pre_spice_height().unwrap();
                let first_spice_height = timeline.first_spice_height().unwrap();
                for (index, offsets) in self.requested(self.shard_ids.len()) {
                    for (offset, missing) in offsets {
                        let height = match offset {
                            -1 => boundary_height,
                            0 => first_spice_height,
                            _ => (estimate as i64 + offset) as BlockHeight,
                        };
                        self.resolved.push(ResolvedChunk {
                            shard_index: index,
                            shard_id: self.shard_ids[index],
                            offset,
                            height,
                            missing,
                            parts: self.drops_parts(offset),
                            coincides: false,
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
            Progress::Done => {
                // The blocks above the first spice one exist by now, so the chunks the
                // block producers had without the network can be told apart.
                let node = env.rpc_node();
                for resolved in &mut self.resolved {
                    if !(resolved.parts && resolved.missing) {
                        continue;
                    }
                    resolved.coincides =
                        chunk_produced_by_block_producer(&node, resolved.height, resolved.shard_id);
                    if resolved.coincides {
                        resolved.missing = false;
                        tracing::info!(
                            target: "test",
                            shard = resolved.shard_index,
                            offset = resolved.offset,
                            height = resolved.height,
                            "missing_chunk: the chunk's producer is the block producer, so its \
                             parts could not keep it out and it is expected present",
                        );
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
    /// that skips heights, has nothing to check. A chunk dropped by its parts is also absent
    /// from every live node's chunk store but its producer's.
    fn assert_fired(&self, env: &TestLoopEnv, _timeline: &Timeline) {
        assert!(!self.resolved.is_empty(), "the offsets were never resolved");
        assert!(
            self.resolved.iter().any(|resolved| resolved.missing),
            "every chunk the fault asked to be missing is its block producer's own, which \
             dropping parts cannot keep out: {:?}",
            self.resolved
                .iter()
                .filter(|resolved| resolved.coincides)
                .map(|resolved| (resolved.shard_index, resolved.offset))
                .collect::<Vec<_>>(),
        );
        let node = env.rpc_node();
        let chain = &node.client().chain;
        let epoch_manager = node.client().epoch_manager.as_ref();
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
            let producer = epoch_manager
                .get_chunk_producer_info_from_prev_block(
                    block.header().prev_hash(),
                    resolved.shard_id,
                )
                .unwrap()
                .take_account_id();
            assert_eq!(
                is_new,
                !resolved.missing,
                "shard {}'s chunk at offset {} (height {}) is {}, the fault asked for it to be {}{}",
                resolved.shard_index,
                resolved.offset,
                resolved.height,
                if is_new { "present" } else { "missing" },
                if resolved.missing { "missing" } else { "present" },
                if resolved.coincides {
                    format!(
                        "; its producer {producer} is the block producer, so dropping the parts \
                         could not keep it out"
                    )
                } else {
                    String::new()
                },
            );
            if !(resolved.missing && resolved.parts) {
                continue;
            }
            let chunks = env.shared_state.chunks_storage.lock().find(|header| {
                header.shard_id() == resolved.shard_id
                    && header.prev_block_hash() == block.header().prev_hash()
            });
            assert!(
                !chunks.is_empty(),
                "shard {}'s chunk on the block below height {} was never produced, so nothing \
                 was dropped",
                resolved.shard_index,
                resolved.height,
            );
            for chunk in chunks {
                for index in env.live_node_indices() {
                    let other = env.node(index);
                    if other.node_data.account_id == producer {
                        continue;
                    }
                    assert!(
                        !other.client().chain.chain_store.chunk_exists(chunk.chunk_hash()),
                        "node {} has the chunk of shard {} at height {}, whose parts were dropped",
                        other.node_data.account_id,
                        resolved.shard_index,
                        resolved.height,
                    );
                }
            }
        }
    }
}

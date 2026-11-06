//! `late_chunk`: a chunk's parts delayed, not dropped, so the chunk arrives after the block
//! that should have carried it.

use super::super::driver::RunRecord;
use super::super::oracles::{Oracle, Relaxations};
use super::super::resolve::Resolved;
use super::super::scenario::{
    Fault, FaultEvent, Narrowing, OracleId, Progress, Relaxation, Timeline,
};
use super::{
    chunk_at_offset_selector, chunk_produced_by_block_producer, render, shard_layout_at_head,
};
use crate::setup::builder::MIN_BLOCK_PROD_TIME;
use crate::setup::env::TestLoopEnv;
use crate::setup::network_rules::{Action, RuleHandle};
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ChunkHash, ShardChunkHeader};
use near_primitives::types::{BlockHeight, ShardId};
use parking_lot::Mutex;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

/// Whether the delay is long enough to keep the chunk out of its block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The block is produced without the chunk, which arrives afterwards.
    Missing,
    /// The chunk arrives late but before the block is produced, which carries it.
    Included,
}

/// Shard 1: shard 0's chunk of the last pre-spice block is its block producer's own on
/// profile A, see `Profile::chunk_is_block_producers_own`.
const DEFAULT_SHARD: usize = 1;
const DEFAULT_AT: i64 = -1;
/// Past the block producer's wait for chunks: two block times, which is what a block takes
/// on a healthy chain and more than the producer waits after the previous block.
const MISSING_DELAY_MS: u64 = 2 * MIN_BLOCK_PROD_TIME;
/// Inside the wait: half a block time, which the producer sits through anyway before it may
/// produce, so the chunk is there when it does.
const INCLUDED_DELAY_MS: u64 = MIN_BLOCK_PROD_TIME / 2;

/// The parts of `shard`'s chunk at offset `at` delayed by `delay_ms` on every node, through
/// a chunk-part rule keyed as `missing_chunk:how=parts` keys its drops: the pushes, the
/// forwards and the request and response pair all go out `delay_ms` later. The block
/// producer waits for chunks a third of a block time past the approvals, which arrive right
/// after the parent, then produces without the chunk: under `outcome=missing` the chunk is
/// orphaned and exists everywhere afterwards, which the shards manager and garbage
/// collection must cope with, and its transactions go back to the pool and into a later
/// chunk. Under `outcome=included` the delay sits inside the block time the producer waits
/// out anyway, so the block carries the chunk on time: the delay itself is what is
/// exercised, and no late block is observable, since a chunk is produced the moment its
/// parent is processed and the block follows a block time later.
pub struct LateChunk {
    shard: usize,
    at: i64,
    delay_ms: Option<u64>,
    outcome: Outcome,
    shard_id: Option<ShardId>,
    rules: Vec<RuleHandle>,
    /// The hashes of the chunks the rules delayed, per node.
    delayed: Arc<Mutex<HashSet<ChunkHash>>>,
    /// The height the chunk was created for, from `first_spice`.
    height: Option<BlockHeight>,
    /// The block the delayed chunk was built on, which is where a block lacking it carries
    /// its absence, from `first_spice`.
    prev_block_hash: Option<CryptoHash>,
    /// The chunks the boundary predicate delayed at the estimate's boundary heights when
    /// the boundary moved past them, which a block at those heights then lacks.
    predicate_misses: HashSet<(ShardId, BlockHeight)>,
    /// Whether the chunk's producer is also the block producer of its height, so that the
    /// delay could not touch it, see `chunk_at_offset_selector`.
    coincides: bool,
}

impl Default for LateChunk {
    fn default() -> Self {
        Self::new()
    }
}

impl LateChunk {
    /// Shard 1's chunk of the last pre-spice block, late by two block times, so the block
    /// is produced without it.
    pub fn new() -> Self {
        Self {
            shard: DEFAULT_SHARD,
            at: DEFAULT_AT,
            delay_ms: None,
            outcome: Outcome::Missing,
            shard_id: None,
            rules: Vec::new(),
            delayed: Arc::default(),
            height: None,
            prev_block_hash: None,
            predicate_misses: HashSet::new(),
            coincides: false,
        }
    }

    pub fn shard(mut self, shard: usize) -> Self {
        self.shard = shard;
        self
    }

    /// The offset, relative to the estimated first spice height, resolved as
    /// `missing_chunk` resolves its offsets.
    pub fn at(mut self, at: i64) -> Self {
        self.at = at;
        self
    }

    /// The delay; the outcome's default otherwise. No preset needs it yet.
    #[allow(dead_code)]
    pub fn delay_ms(mut self, delay_ms: u64) -> Self {
        self.delay_ms = Some(delay_ms);
        self
    }

    pub fn outcome(mut self, outcome: Outcome) -> Self {
        self.outcome = outcome;
        self
    }

    /// Whether the chunk arrives in time to be included, only late.
    pub fn arrives_included(&self) -> bool {
        self.outcome == Outcome::Included
    }

    /// The `(shard index, offset)` pair this fault delays, for the resolver's overlap checks.
    pub fn delayed_offset(&self) -> (usize, i64) {
        (self.shard, self.at)
    }

    fn delay(&self) -> Duration {
        let default = match self.outcome {
            Outcome::Missing => MISSING_DELAY_MS,
            Outcome::Included => INCLUDED_DELAY_MS,
        };
        Duration::milliseconds(self.delay_ms.unwrap_or(default) as i64)
    }

    fn matched(&self) -> u64 {
        self.rules.iter().map(RuleHandle::matched).sum()
    }
}

impl Debug for LateChunk {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        render(
            f,
            self.tag(),
            &[
                ("shard", (self.shard != DEFAULT_SHARD).then(|| self.shard.to_string())),
                ("at", (self.at != DEFAULT_AT).then(|| self.at.to_string())),
                ("delay_ms", self.delay_ms.map(|delay_ms| delay_ms.to_string())),
                ("outcome", (self.outcome == Outcome::Included).then(|| "included".to_string())),
            ],
        )
    }
}

impl Fault for LateChunk {
    fn tag(&self) -> &'static str {
        "late_chunk"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn validate(&self, resolved: Resolved) -> Result<(), String> {
        if self.delay_ms == Some(0) {
            return Err("delay_ms must be positive".to_string());
        }
        let profile = resolved.profile;
        if profile.chunk_is_block_producers_own(self.shard, self.at) == Some(true) {
            return Err(format!(
                "shard {}'s chunk at offset {} is its block producer's own on profile {profile:?}, \
                 which the producer includes without the network, so delaying its parts delays \
                 nothing",
                self.shard, self.at,
            ));
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
                assert!(
                    self.shard < num_shards,
                    "shard index {} is out of range: {num_shards} shards",
                    self.shard
                );
                self.shard_id = Some(shard_layout.get_shard_id(self.shard).unwrap());
            }
            Progress::BoundaryKnown { estimate } => {
                let shard_id = self.shard_id.unwrap();
                let delay = self.delay();
                for node in 0..env.node_datas.len() {
                    let epoch_manager = env.node(node).client().epoch_manager.clone();
                    let selects =
                        chunk_at_offset_selector(epoch_manager, *estimate, shard_id, self.at);
                    let delayed = self.delayed.clone();
                    let selects = move |header: &ShardChunkHeader| {
                        let selected = selects(header);
                        if selected {
                            delayed.lock().insert(header.chunk_hash().clone());
                        }
                        selected
                    };
                    self.rules.push(env.chunk_parts_rule(node, selects, Action::Delay(delay)));
                }
            }
            Progress::FirstSpice => {
                let estimate = timeline.estimate.expect("the estimate is known by the boundary");
                let boundary_height = timeline.last_pre_spice_height().unwrap();
                let height = match self.at {
                    -1 => boundary_height,
                    0 => timeline.first_spice_height().unwrap(),
                    offset => (estimate as i64 + offset) as BlockHeight,
                };
                self.height = Some(height);
                if self.at == -1 {
                    for height in estimate - 1..boundary_height {
                        self.predicate_misses.insert((self.shard_id.unwrap(), height));
                    }
                }
            }
            Progress::Done => {
                // The block at the resolved height exists by now, whatever the offset.
                let node = env.rpc_node();
                let height = self.height.expect("the offset was resolved at first_spice");
                let chain = &node.client().chain;
                self.prev_block_hash =
                    chain.get_block_by_height(height).ok().map(|block| *block.header().prev_hash());
                self.coincides =
                    chunk_produced_by_block_producer(&node, height, self.shard_id.unwrap());
            }
            _ => {}
        }
        Vec::new()
    }

    fn relaxes(&self) -> Vec<Relaxation> {
        if self.outcome == Outcome::Included {
            return Vec::new();
        }
        let mut pairs = self.predicate_misses.clone();
        if let Some(height) = self.height {
            pairs.insert((self.shard_id.unwrap(), height));
        }
        vec![Relaxation {
            oracle: OracleId::ChunkCompleteness,
            narrowing: Narrowing::ShardHeights(pairs),
        }]
    }

    fn extra_oracles(&self) -> Vec<Box<dyn Oracle>> {
        if self.outcome == Outcome::Included {
            return Vec::new();
        }
        vec![Box::new(LateChunkTransactionsReincluded {
            shard: self.shard,
            shard_id: self.shard_id.unwrap(),
            height: self.height.unwrap(),
            prev_block_hash: self.prev_block_hash,
        })]
    }

    /// Parts were delayed, and the chunk at the resolved height is missing or present as the
    /// outcome says; a missing chunk is in the chunk store of every live node tracking its
    /// shard afterwards, as an orphan.
    fn assert_fired(&self, env: &TestLoopEnv, _timeline: &Timeline) {
        let height = self.height.expect("the offset was never resolved");
        assert!(
            !self.coincides,
            "shard {}'s chunk at height {height} is its block producer's own, which the producer \
             includes without the network, so delaying its parts delays nothing; name another \
             shard or offset",
            self.shard,
        );
        assert!(self.matched() > 0, "no part of shard {}'s chunk was delayed", self.shard);
        let node = env.rpc_node();
        let chain = &node.client().chain;
        let block = chain.get_block_by_height(height).unwrap_or_else(|_| {
            panic!("no block at height {height} for shard {}'s chunk to be late for", self.shard)
        });
        let is_new = block.chunks()[self.shard].is_new_chunk(height);
        match self.outcome {
            Outcome::Included => assert!(
                is_new,
                "shard {}'s chunk at height {height} is missing: the delay was not inside the \
                 block producer's wait",
                self.shard,
            ),
            Outcome::Missing => {
                assert!(
                    !is_new,
                    "shard {}'s chunk at height {height} is present: the delay did not keep it \
                     out of its block",
                    self.shard,
                );
                let chunks = env.shared_state.chunks_storage.lock().find(|header| {
                    header.shard_id() == self.shard_id.unwrap()
                        && header.prev_block_hash() == block.header().prev_hash()
                });
                assert!(!chunks.is_empty(), "the late chunk was never produced");
                let mut holders = Vec::new();
                for chunk in &chunks {
                    for index in env.live_node_indices() {
                        let other = env.node(index);
                        if other.client().chain.chain_store.chunk_exists(chunk.chunk_hash()) {
                            holders.push(other.node_data.account_id.clone());
                        }
                    }
                }
                tracing::info!(
                    target: "test",
                    height,
                    shard = self.shard,
                    chunks = chunks.len(),
                    ?holders,
                    "late_chunk: the orphaned chunk's holders at done",
                );
            }
        }
    }
}

/// Every transaction the late chunk carried is included exactly once in a later chunk of
/// its shard and none twice: the chunk was produced, so its transactions left the pool,
/// and the block producer's skipping it has to put them back.
struct LateChunkTransactionsReincluded {
    shard: usize,
    shard_id: ShardId,
    height: BlockHeight,
    prev_block_hash: Option<CryptoHash>,
}

impl Oracle for LateChunkTransactionsReincluded {
    fn id(&self) -> OracleId {
        OracleId::LateChunkTransactionsReincluded
    }

    fn check(&self, env: &TestLoopEnv, run: &RunRecord, _: &Relaxations) -> Result<(), String> {
        let Some(prev_block_hash) = self.prev_block_hash else {
            return Err(format!(
                "no block at height {}, so the late chunk has no block",
                self.height
            ));
        };
        let chunks = env.shared_state.chunks_storage.lock().find(|header| {
            header.shard_id() == self.shard_id && header.prev_block_hash() == &prev_block_hash
        });
        let [header] = &chunks[..] else {
            return Err(format!(
                "{} chunks of shard {} were produced on the block below height {}, expected one",
                chunks.len(),
                self.shard,
                self.height,
            ));
        };
        // The producer has the chunk's body whatever happened to its parts.
        let node = env.rpc_node();
        let producer = node
            .client()
            .epoch_manager
            .get_chunk_producer_info_from_prev_block(&prev_block_hash, self.shard_id)
            .map_err(|error| format!("the late chunk's producer is unknown: {error}"))?
            .take_account_id();
        let producer_node = env.node_for_account(&producer);
        let chunk =
            producer_node.client().chain.chain_store.get_chunk(header.chunk_hash()).map_err(
                |error| format!("the producer {producer} has no body of the late chunk: {error}"),
            )?;
        let carried: Vec<CryptoHash> =
            chunk.to_transactions().iter().map(|tx| tx.get_hash()).collect();
        let mut inclusions: HashMap<CryptoHash, Vec<BlockHeight>> = HashMap::new();
        for height in self.height..=run.terminal_height {
            let Ok(block) = node.client().chain.get_block_by_height(height) else {
                continue;
            };
            for chunk in node.block_chunks(&block) {
                if !block.chunks()[self.shard].is_new_chunk(height)
                    || chunk.shard_id() != self.shard_id
                {
                    continue;
                }
                for tx in chunk.to_transactions() {
                    inclusions.entry(tx.get_hash()).or_default().push(height);
                }
            }
        }
        if carried.is_empty() {
            return Err(format!(
                "the late chunk at height {} carried no transactions, so there is nothing to \
                 include again",
                self.height,
            ));
        }
        let mut errors = Vec::new();
        for tx in &carried {
            match inclusions.get(tx).map(Vec::as_slice) {
                Some([_]) => {}
                Some(heights) => {
                    errors.push(format!("transaction {tx} was included at {heights:?}"))
                }
                None => errors.push(format!("transaction {tx} was never included again")),
            }
        }
        if !errors.is_empty() {
            return Err(format!(
                "of the {} transactions of the late chunk at height {}: {}",
                carried.len(),
                self.height,
                errors.join("; "),
            ));
        }
        tracing::info!(
            target: "test",
            height = self.height,
            transactions = carried.len(),
            "late_chunk: every transaction of the late chunk was included once later",
        );
        Ok(())
    }
}

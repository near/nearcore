//! `bad_first_spice_block`: a boundary block is rewritten in flight into one honest nodes
//! reject.

use super::super::driver::RunRecord;
use super::super::oracles::{Oracle, Relaxations};
use super::super::resolve::Resolved;
use super::super::scenario::{
    Fault, FaultEvent, Narrowing, OracleId, Progress, Relaxation, Timeline,
};
use super::{producer_outage_relaxations, render};
use crate::setup::drop_condition::is_last_pre_spice_header;
use crate::setup::env::TestLoopEnv;
use crate::setup::network_rules::{Action, RuleHandle};
use crate::utils::gaps::block_producer_at;
use crate::utils::headers::convert_v6_header_to_v7;
use near_chain::ChainStoreAccess;
use near_chain::spice::boundary::is_last_pre_spice_block;
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::NetworkRequests;
use near_primitives::block::Block;
use near_primitives::block_body::{BlockBody, SpiceCoreStatement, SpiceCoreStatements};
use near_primitives::block_header::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{
    AccountId, BlockHeight, ChunkExecutionResult, SpiceChunkId, compute_chunk_execution_root,
};
use parking_lot::Mutex;
use std::any::Any;
use std::collections::HashSet;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

/// What the rewritten block claims.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// The first spice block carries an execution result for a boundary chunk that no
    /// endorsement backs.
    FakeBoundaryResult,
    /// The last pre-spice block carries a spice header: the V7 header honest nodes reject
    /// before spice.
    SpiceHeaderPreSpice,
}

const DEFAULT_MODE: Mode = Mode::FakeBoundaryResult;

/// What the rewrite did, shared with the rules.
#[derive(Default)]
struct Rewrite {
    /// The height, the original hash and the rewritten hash, once a block was rewritten.
    done: Option<(BlockHeight, CryptoHash, CryptoHash)>,
}

/// A rule on every node from `boundary_known` rewrites the first block that is the one the
/// mode names, in flight and re-signed with its producer's test signer, as the V7 header
/// test does; the producer keeps its own valid block, and the rules swallow the requests
/// for it, so it cannot arrive by request when the producer builds the next height on it.
/// Honest nodes reject the rewritten block, so its height is skipped, and the run above it
/// the assignment gives to the same producer with it.
pub struct BadFirstSpiceBlock {
    mode: Mode,
    rules: Vec<RuleHandle>,
    rewrite: Arc<Mutex<Rewrite>>,
    /// The driver height at which the producer's ban was lifted.
    unbanned_at: Option<BlockHeight>,
    /// The rewritten height and the run above it, read off the chain at `done`.
    relaxed: HashSet<BlockHeight>,
    /// What the producer left out while cut off, read off the chain at `done`.
    outage: Vec<Relaxation>,
}

impl Default for BadFirstSpiceBlock {
    fn default() -> Self {
        Self::new()
    }
}

impl BadFirstSpiceBlock {
    /// The first spice block with a fake boundary result.
    pub fn new() -> Self {
        Self {
            mode: DEFAULT_MODE,
            rules: Vec::new(),
            rewrite: Arc::default(),
            unbanned_at: None,
            relaxed: HashSet::new(),
            outage: Vec::new(),
        }
    }

    pub fn mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self
    }

    fn done(&self) -> Option<(BlockHeight, CryptoHash, CryptoHash)> {
        self.rewrite.lock().done
    }
}

impl Debug for BadFirstSpiceBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        render(
            f,
            self.tag(),
            &[("mode", (self.mode != DEFAULT_MODE).then(|| "spice_header_pre_spice".to_string()))],
        )
    }
}

/// Whether `block`, broadcast by `me`, is the block the mode names and `me` produced it:
/// the rules are on every node, since the producer is not known in advance, and a
/// rebroadcast by another node would be re-signed with the wrong key.
fn is_target(
    mode: Mode,
    epoch_manager: &dyn EpochManagerAdapter,
    me: &AccountId,
    block: &Block,
) -> bool {
    let header = block.header();
    let named = match mode {
        Mode::FakeBoundaryResult => {
            block.is_spice_block()
                && is_last_pre_spice_block(epoch_manager, header.prev_hash()).unwrap_or(false)
        }
        Mode::SpiceHeaderPreSpice => {
            !block.is_spice_block() && is_last_pre_spice_header(epoch_manager, header)
        }
    };
    named
        && epoch_manager
            .get_block_producer(header.epoch_id(), header.height())
            .is_ok_and(|producer| &producer == me)
}

/// `block` with an execution result for shard 0's chunk of its parent pushed into its core
/// statements, the header's body hash and execution root recomputed to match, and the
/// header re-signed.
fn with_fake_boundary_result(block: &Block, signer_account: &AccountId) -> Block {
    let mut rewritten = block.clone();
    let shard_id = block.chunks()[0].shard_id();
    let fake = SpiceCoreStatement::ChunkExecutionResult {
        chunk_id: SpiceChunkId { block_hash: *block.header().prev_hash(), shard_id },
        execution_result: ChunkExecutionResult {
            chunk_extra: ChunkExtra::new_with_only_state_root(&CryptoHash::hash_bytes(
                b"fake boundary result",
            )),
            outgoing_receipts_root: CryptoHash::default(),
        },
    };
    let Block::BlockV4(v4) = &mut rewritten else {
        panic!("a spice block is a BlockV4");
    };
    let BlockBody::V3(body) = &mut v4.body else {
        panic!("a spice block has a V3 body");
    };
    let mut statements: Vec<SpiceCoreStatement> = body.core_statements.iter().cloned().collect();
    statements.push(fake);
    body.core_statements = SpiceCoreStatements::new(statements);
    let execution_root =
        compute_chunk_execution_root(body.core_statements.iter_execution_results());
    let body_hash = v4.body.compute_hash();
    let BlockHeader::BlockHeaderV8(header) = &mut v4.header else {
        panic!("a spice block has a V8 header");
    };
    header.inner_rest.block_body_hash = body_hash;
    header.inner_lite.chunk_execution_root = execution_root;
    rewritten.mut_header().resign(&create_test_signer(signer_account.as_str()));
    rewritten
}

/// `block` with its V6 header converted to V7 and re-signed.
fn with_spice_header(block: &Block, signer_account: &AccountId) -> Block {
    let mut rewritten = block.clone();
    *rewritten.mut_header() = convert_v6_header_to_v7(block.header());
    rewritten.mut_header().resign(&create_test_signer(signer_account.as_str()));
    rewritten
}

impl Fault for BadFirstSpiceBlock {
    fn tag(&self) -> &'static str {
        "bad_first_spice_block"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn validate(&self, _resolved: Resolved) -> Result<(), String> {
        Ok(())
    }

    /// Rewriting the last pre-spice block skips its height, which delays the end of the
    /// epoch; rewriting the first spice block leaves the last pre-spice block where it is.
    fn moves_boundary(&self, _resolved: Resolved) -> bool {
        self.mode == Mode::SpiceHeaderPreSpice
    }

    fn on_progress(
        &mut self,
        env: &mut TestLoopEnv,
        _timeline: &Timeline,
        progress: &Progress,
    ) -> Vec<FaultEvent> {
        match progress {
            Progress::BoundaryKnown { .. } => {
                for index in 0..env.node_datas.len() {
                    let me = env.node_datas[index].account_id.clone();
                    let epoch_manager = env.node(index).client().epoch_manager.clone();
                    let mode = self.mode;
                    let rewrite = self.rewrite.clone();
                    let matches = {
                        let me = me.clone();
                        let rewrite = rewrite.clone();
                        move |request: &NetworkRequests| {
                            let NetworkRequests::Block { block } = request else {
                                return false;
                            };
                            rewrite.lock().done.is_none()
                                && is_target(mode, epoch_manager.as_ref(), &me, block)
                        }
                    };
                    let replace = move |request: NetworkRequests| {
                        let NetworkRequests::Block { block } = request else {
                            unreachable!("the rule matches blocks only");
                        };
                        let rewritten = match mode {
                            Mode::FakeBoundaryResult => with_fake_boundary_result(&block, &me),
                            Mode::SpiceHeaderPreSpice => with_spice_header(&block, &me),
                        };
                        rewrite.lock().done =
                            Some((block.header().height(), *block.hash(), *rewritten.hash()));
                        vec![NetworkRequests::Block { block: Arc::new(rewritten) }]
                    };
                    self.rules.push(env.network_rule(
                        index,
                        matches,
                        Action::Replace(Box::new(replace)),
                    ));
                }
            }
            Progress::Height { height, .. } => {
                // Honest nodes ban the peer that sent them an invalid block, which cuts the
                // producer off from everyone for good in the test loop, where it has no
                // other peers to turn to (measured: the producer of a rewritten last
                // pre-spice block received nothing after it). Lifting the ban at the next
                // height stands in for the reconnection a real node would make, and the
                // producer catches up from its fork.
                if self.unbanned_at.is_none() && self.done().is_some() {
                    env.shared_state.network_shared_state.allow_all_requests();
                    self.unbanned_at = Some(*height);
                }
            }
            Progress::Done => {
                let Some((height, _, _)) = self.done() else {
                    return Vec::new();
                };
                let node = env.rpc_node();
                let chain = &node.client().chain;
                let exists = |height: BlockHeight| {
                    chain.chain_store.get_block_hash_by_height(height).is_ok()
                };
                self.relaxed.insert(height);
                let producer = block_producer_at(&node, height);
                let mut above = height + 1;
                while !exists(above) && block_producer_at(&node, above) == producer {
                    self.relaxed.insert(above);
                    above += 1;
                }
                // Banned and on its own fork, the producer produces neither blocks nor
                // chunks for the others until the ban is lifted and it has caught up: the
                // outage rule of `node_crash` names what it left out (measured: the
                // producer of a rewritten last pre-spice block, cut off for one height,
                // left a chunk missing two heights above the boundary).
                self.outage =
                    producer_outage_relaxations(env, &producer, height + 1, self.unbanned_at);
            }
            _ => {}
        }
        Vec::new()
    }

    fn relaxes(&self) -> Vec<Relaxation> {
        let mut relaxations = vec![Relaxation {
            oracle: OracleId::HeightContiguity,
            narrowing: Narrowing::Heights(self.relaxed.clone()),
        }];
        relaxations.extend(self.outage.iter().cloned());
        relaxations
    }

    fn extra_oracles(&self) -> Vec<Box<dyn Oracle>> {
        vec![Box::new(InvalidBlockRejected { rewrite: self.rewrite.clone() })]
    }

    /// A block was rewritten, its height is skipped on the driver node, and the valid
    /// original stayed with its producer.
    fn assert_fired(&self, env: &TestLoopEnv, _timeline: &Timeline) {
        let (height, original, rewritten) =
            self.done().expect("no block the mode names was broadcast");
        let node = env.rpc_node();
        let chain = &node.client().chain;
        assert!(
            chain.chain_store.get_block_hash_by_height(height).is_err(),
            "a block exists at height {height} on the driver node, whose block was rewritten",
        );
        let producer = block_producer_at(&node, height);
        for index in env.live_node_indices() {
            let other = env.node(index);
            let has_original = other.client().chain.get_block(&original).is_ok();
            assert_eq!(
                has_original,
                other.node_data.account_id == producer,
                "node {} {} the valid block at height {height}, which only its producer \
                 {producer} keeps",
                other.node_data.account_id,
                if has_original { "has" } else { "lacks" },
            );
        }
        let kept = env.shared_state.network_rules.kept_blocks();
        tracing::info!(
            target: "test",
            height,
            %original,
            %rewritten,
            unbanned_at = ?self.unbanned_at,
            requests_swallowed = kept.requests_swallowed,
            "bad_first_spice_block rewrote the block and its height was skipped",
        );
    }
}

/// No node holds the rewritten block: every honest node rejected it, and the producer
/// never saw it.
struct InvalidBlockRejected {
    rewrite: Arc<Mutex<Rewrite>>,
}

impl Oracle for InvalidBlockRejected {
    fn id(&self) -> OracleId {
        OracleId::InvalidBlockRejected
    }

    fn check(&self, env: &TestLoopEnv, _: &RunRecord, _: &Relaxations) -> Result<(), String> {
        let Some((height, _, rewritten)) = self.rewrite.lock().done else {
            return Err("no block was rewritten".to_string());
        };
        for index in env.live_node_indices() {
            let node = env.node(index);
            if node.client().chain.get_block(&rewritten).is_ok() {
                return Err(format!(
                    "node {} holds the rewritten block {rewritten} at height {height}",
                    node.node_data.account_id,
                ));
            }
            let final_head = node.final_head();
            let mut hash = final_head.last_block_hash;
            let mut header = node.client().chain.get_block_header(&hash).unwrap();
            while header.height() > height {
                hash = *header.prev_hash();
                header = node.client().chain.get_block_header(&hash).unwrap();
            }
            if hash == rewritten {
                return Err(format!(
                    "the final head of node {} descends from the rewritten block {rewritten}",
                    node.node_data.account_id,
                ));
            }
        }
        Ok(())
    }
}

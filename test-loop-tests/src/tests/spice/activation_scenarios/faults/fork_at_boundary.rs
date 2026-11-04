//! `fork_at_boundary`: two last pre-spice blocks on different forks, or two first spice
//! blocks that are siblings.

use super::super::driver::RunRecord;
use super::super::oracles::{Oracle, Relaxations};
use super::super::resolve::Resolved;
use super::super::scenario::{
    Fault, FaultEvent, Narrowing, OracleId, Progress, Relaxation, Timeline,
};
use super::render;
use crate::setup::drop_condition::is_last_pre_spice_header;
use crate::setup::env::TestLoopEnv;
use crate::setup::network_rules::{Action, RuleHandle};
use crate::utils::gaps::block_producer_at;
use near_chain::ChainStoreAccess;
use near_chain::spice::boundary::is_last_pre_spice_block;
use near_network::types::NetworkRequests;
use near_primitives::hash::CryptoHash;
use near_primitives::types::BlockHeight;
use parking_lot::Mutex;
use std::any::Any;
use std::collections::HashSet;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

/// Which boundary block is held.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    LastPreSpice,
    FirstSpice,
}

const DEFAULT_KIND: Kind = Kind::LastPreSpice;

/// A hold rule on every node from `boundary_known` holds the first block the kind names,
/// decided from the block alone when its producer broadcasts it, and releases it once the
/// producer has approved a block two above it: the others time out on the held height,
/// send skip approvals, and the next producer builds on the held block's parent, so when
/// the held block goes out every node processes it as a losing fork that is also a last
/// pre-spice block, or a first spice block. With `last_pre_spice` the skip delays finality
/// and the epoch ends three heights later, so the canonical last pre-spice block sits above
/// the held one; with `first_spice` the parent is the last pre-spice block, so the held
/// block and the skipping block are siblings. The release comes before the canonical
/// chain's final head passes the held height, which is when block processing would reject
/// it. The rules swallow the requests for the held block meanwhile, so it cannot arrive by
/// request when the producer builds the next height on it; that height is skipped too and
/// relaxed with the held one.
pub struct ForkAtBoundary {
    kind: Kind,
    rules: Vec<RuleHandle>,
    /// The held block's height and hash, once held.
    held: Arc<Mutex<Option<(BlockHeight, CryptoHash)>>>,
    /// The held height and the run above it, read off the chain at `done`.
    relaxed: HashSet<BlockHeight>,
}

impl Default for ForkAtBoundary {
    fn default() -> Self {
        Self::new()
    }
}

impl ForkAtBoundary {
    /// Two last pre-spice blocks.
    pub fn new() -> Self {
        Self {
            kind: DEFAULT_KIND,
            rules: Vec::new(),
            held: Arc::default(),
            relaxed: HashSet::new(),
        }
    }

    pub fn kind(mut self, kind: Kind) -> Self {
        self.kind = kind;
        self
    }

    /// Whether the held block is a last pre-spice block, which ends up on a losing fork.
    pub fn holds_last_pre_spice(&self) -> bool {
        self.kind == Kind::LastPreSpice
    }

    fn held(&self) -> Option<(BlockHeight, CryptoHash)> {
        *self.held.lock()
    }
}

impl Debug for ForkAtBoundary {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        render(
            f,
            self.tag(),
            &[("kind", (self.kind != DEFAULT_KIND).then(|| "first_spice".to_string()))],
        )
    }
}

impl Fault for ForkAtBoundary {
    fn tag(&self) -> &'static str {
        "fork_at_boundary"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Holding the last pre-spice block skips its height, which delays the end of the
    /// epoch; holding the first spice block leaves the last pre-spice block where it is.
    fn moves_boundary(&self, _resolved: Resolved) -> bool {
        self.kind == Kind::LastPreSpice
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
                    let epoch_manager = env.node(index).client().epoch_manager.clone();
                    let kind = self.kind;
                    let held = self.held.clone();
                    let matches = move |request: &NetworkRequests| {
                        let NetworkRequests::Block { block } = request else {
                            return false;
                        };
                        let mut held = held.lock();
                        match *held {
                            Some((_, hash)) => hash == *block.hash(),
                            None => {
                                let header = block.header();
                                let named = match kind {
                                    Kind::LastPreSpice => {
                                        is_last_pre_spice_header(epoch_manager.as_ref(), header)
                                    }
                                    Kind::FirstSpice => is_last_pre_spice_block(
                                        epoch_manager.as_ref(),
                                        header.prev_hash(),
                                    )
                                    .unwrap_or(false),
                                };
                                if named {
                                    *held = Some((header.height(), *block.hash()));
                                }
                                named
                            }
                        }
                    };
                    // The producer shows the height right above its held block as soon as
                    // it times out on it and sends a skip approval, which released the
                    // block to nodes that had already sent their own skip approvals and so
                    // could approve nothing on it (measured: the held block became canonical
                    // and the height above it was skipped instead). Two above is a block
                    // the others built on the held block's parent and the one after it,
                    // which the producer approved, and comes before the final head passes
                    // the held height.
                    let release = |held: &NetworkRequests, seen_height: BlockHeight| {
                        let NetworkRequests::Block { block } = held else {
                            return true;
                        };
                        seen_height > block.header().height() + 1
                    };
                    self.rules.push(env.network_rule(
                        index,
                        matches,
                        Action::Hold(Box::new(release)),
                    ));
                }
            }
            Progress::Done => {
                let Some((height, _)) = self.held() else {
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

    fn extra_oracles(&self) -> Vec<Box<dyn Oracle>> {
        vec![Box::new(BothForksBootstrapped { kind: self.kind, held: self.held.clone() })]
    }

    /// A block was held, every live node has it and the canonical block at its height is
    /// not it: the held height is skipped on the canonical chain.
    fn assert_fired(&self, env: &TestLoopEnv, _timeline: &Timeline) {
        let (height, hash) = self.held().expect("no block the kind names was broadcast");
        let node = env.rpc_node();
        assert!(
            node.client().chain.chain_store.get_block_hash_by_height(height).is_err(),
            "a block exists at height {height} on the driver node, which the held block was \
             to leave skipped",
        );
        for index in env.live_node_indices() {
            let other = env.node(index);
            assert!(
                other.client().chain.get_block(&hash).is_ok(),
                "node {} never got the held block {hash} at height {height}",
                other.node_data.account_id,
            );
        }
        let kept = env.shared_state.network_rules.kept_blocks();
        tracing::info!(
            target: "test",
            height,
            %hash,
            held = self.rules.iter().map(RuleHandle::matched).sum::<u64>(),
            requests_swallowed = kept.requests_swallowed,
            "fork_at_boundary held the block and every node got it later",
        );
    }
}

/// Both boundary blocks were bootstrapped on every live node: with `last_pre_spice`, both
/// last pre-spice blocks have their seeded uncertified-chunks row; with `first_spice`, both
/// first spice blocks are on disk and the execution head went forward only, past the
/// boundary.
struct BothForksBootstrapped {
    kind: Kind,
    held: Arc<Mutex<Option<(BlockHeight, CryptoHash)>>>,
}

impl Oracle for BothForksBootstrapped {
    fn id(&self) -> OracleId {
        OracleId::BothForksBootstrapped
    }

    fn check(&self, env: &TestLoopEnv, run: &RunRecord, _: &Relaxations) -> Result<(), String> {
        let Some((height, held)) = *self.held.lock() else {
            return Err("no block was held".to_string());
        };
        let canonical = match self.kind {
            Kind::LastPreSpice => *run.last_pre_spice().hash(),
            Kind::FirstSpice => *run.first_spice().hash(),
        };
        if canonical == held {
            return Err(format!("the held block {held} at height {height} is the canonical one"));
        }
        for index in env.live_node_indices() {
            let node = env.node(index);
            let chain = &node.client().chain;
            for (which, hash) in [("held", held), ("canonical", canonical)] {
                if chain.get_block(&hash).is_err() {
                    return Err(format!(
                        "node {} lacks the {which} block {hash}",
                        node.node_data.account_id
                    ));
                }
                if self.kind == Kind::LastPreSpice {
                    let seeded =
                        chain.spice_core_reader.get_uncertified_chunks(&hash).map_err(|error| {
                            format!(
                                "node {} has no uncertified-chunks row for the {which} last \
                                 pre-spice block {hash}: {error}",
                                node.node_data.account_id
                            )
                        })?;
                    if seeded.is_empty() {
                        return Err(format!(
                            "node {} seeded no uncertified chunks for the {which} last pre-spice \
                             block {hash}",
                            node.node_data.account_id
                        ));
                    }
                }
            }
            if self.kind == Kind::FirstSpice {
                let executed = node.last_executed().height;
                let boundary = run.last_pre_spice().header().height();
                if executed <= boundary {
                    return Err(format!(
                        "the execution head of node {} is at height {executed}, not past the \
                         boundary at {boundary}",
                        node.node_data.account_id
                    ));
                }
            }
        }
        Ok(())
    }
}

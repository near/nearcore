//! `data_before_block`: boundary data reaches a validator before the block it is about.

use super::super::driver::{GateCounts, RunRecord, gate_counts};
use super::super::oracles::{Oracle, Relaxations};
use super::super::resolve::Resolved;
use super::super::scenario::{Fault, FaultEvent, OracleId, Progress, Timeline};
use super::render;
use crate::setup::env::TestLoopEnv;
use crate::setup::network_rules::{Action, RuleHandle};
use near_network::types::NetworkRequests;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockHeight};
use parking_lot::Mutex;
use std::any::Any;
use std::collections::HashSet;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Which boundary block is withheld.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum What {
    LastPreSpice,
    FirstSpice,
}

const DEFAULT_NODE: usize = 4;
const DEFAULT_WHAT: What = What::LastPreSpice;
const DEFAULT_HOLD: u64 = 3;

/// The broadcast of a boundary block is withheld from `node` for `hold` blocks, through
/// `withhold_block_delivery` on the network's shared state, while partial data and
/// endorsements about it get through; the node's own requests for the block, and its
/// header requests while it is behind, are swallowed by a rule on its account, since the
/// test-loop peer manager answers both by handing the response straight to the requester's
/// client. The held block is keyed by height against the estimate, so the fault needs the
/// boundary where the estimate puts it. Exercises the spice gate's fall-back-to-head
/// branch and the pending-endorsement and parked-block paths live.
pub struct DataBeforeBlock {
    node: usize,
    what: What,
    hold: u64,
    account_id: Option<AccountId>,
    held_height: Option<BlockHeight>,
    /// Rules on the node's requests, lifted at the release.
    request_rules: Vec<RuleHandle>,
    /// Rules on the other nodes reporting data about the held block sent to the node.
    observers: Vec<RuleHandle>,
    /// Data and endorsements about the held block the node was sent before it had the
    /// block.
    data_before_block: Arc<AtomicU64>,
    /// Endorsements of the held block the node sent after the release.
    endorsed_after_release: Arc<AtomicU64>,
    /// The hashes of the blocks withheld from the node, read at the release, which is
    /// when the network's shared state forgets them.
    held_hashes: Arc<Mutex<HashSet<CryptoHash>>>,
    released: Arc<AtomicU64>,
    released_at: Option<(BlockHeight, usize)>,
    gates_at_hold: Option<GateCounts>,
}

impl Default for DataBeforeBlock {
    fn default() -> Self {
        Self::new()
    }
}

impl DataBeforeBlock {
    /// The first chunk-only validator of profile A, which the last pre-spice block reaches
    /// three blocks late.
    pub fn new() -> Self {
        Self {
            node: DEFAULT_NODE,
            what: DEFAULT_WHAT,
            hold: DEFAULT_HOLD,
            account_id: None,
            held_height: None,
            request_rules: Vec::new(),
            observers: Vec::new(),
            data_before_block: Arc::default(),
            endorsed_after_release: Arc::default(),
            held_hashes: Arc::default(),
            released: Arc::default(),
            released_at: None,
            gates_at_hold: None,
        }
    }

    /// The index among the validators; the driver is never held back.
    pub fn node(mut self, node: usize) -> Self {
        self.node = node;
        self
    }

    pub fn what(mut self, what: What) -> Self {
        self.what = what;
        self
    }

    pub fn hold(mut self, hold: u64) -> Self {
        self.hold = hold;
        self
    }

    pub fn target(&self) -> usize {
        self.node
    }

    fn account_id(&self) -> &AccountId {
        self.account_id.as_ref().expect("the target is looked up at built")
    }
}

impl Debug for DataBeforeBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        render(
            f,
            self.tag(),
            &[
                ("node", (self.node != DEFAULT_NODE).then(|| self.node.to_string())),
                ("what", (self.what != DEFAULT_WHAT).then(|| "first_spice".to_string())),
                ("hold", (self.hold != DEFAULT_HOLD).then(|| self.hold.to_string())),
            ],
        )
    }
}

impl Fault for DataBeforeBlock {
    fn tag(&self) -> &'static str {
        "data_before_block"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn validate(&self, resolved: Resolved) -> Result<(), String> {
        if self.hold == 0 {
            return Err("hold must be at least 1 block".to_string());
        }
        if self.node >= resolved.profile.num_validators() {
            return Err(format!(
                "node {} is not a validator; the driver and the clients the faults add are \
                 never held back",
                self.node
            ));
        }
        if resolved.profile.is_producer(self.node) {
            return Err(format!(
                "node {} is a block producer, which held back skips its slots; hold back a \
                 chunk-only validator, one of {:?}",
                self.node,
                (0..resolved.profile.num_validators())
                    .filter(|index| resolved.profile.is_chunk_only_validator(*index))
                    .collect::<Vec<_>>(),
            ));
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
                self.account_id = Some(env.node_datas[self.node].account_id.clone());
            }
            Progress::BoundaryKnown { estimate } => {
                let held_height = match self.what {
                    What::LastPreSpice => estimate - 1,
                    What::FirstSpice => *estimate,
                };
                self.held_height = Some(held_height);
                self.gates_at_hold = Some(gate_counts(env, self.node));
                let network = env.shared_state.network_shared_state.clone();
                let account_id = self.account_id().clone();
                network.withhold_block_delivery(&account_id, held_height..held_height + 1);

                // The node asks for the block once it sees a child of it, and for headers
                // once it is far enough behind; both are answered directly, so both are
                // swallowed while the block is withheld.
                let withheld = {
                    let account_id = account_id.clone();
                    move || network.withheld_block_hashes(&account_id)
                };
                let requests = {
                    let withheld = withheld.clone();
                    move |request: &NetworkRequests| match request {
                        NetworkRequests::BlockRequest { hash, .. } => withheld().contains(hash),
                        NetworkRequests::BlockHeadersRequest { .. } => !withheld().is_empty(),
                        _ => false,
                    }
                };
                self.request_rules.push(env.network_rule(self.node, requests, Action::Drop));

                // What the others send the node about the withheld block, while it is.
                for index in 0..env.node_datas.len() {
                    if index == self.node {
                        continue;
                    }
                    let withheld = withheld.clone();
                    let account_id = account_id.clone();
                    let counter = self.data_before_block.clone();
                    let about_held_block = move |request: &NetworkRequests| match request {
                        NetworkRequests::SpicePartialData { partial_data, recipients } => {
                            recipients.contains(&account_id)
                                && withheld().contains(partial_data.id().block_hash())
                        }
                        NetworkRequests::SpiceChunkEndorsement(target, endorsement) => {
                            *target == account_id && withheld().contains(endorsement.block_hash())
                        }
                        _ => false,
                    };
                    let observe = move |_: &NetworkRequests| {
                        counter.fetch_add(1, Ordering::Relaxed);
                    };
                    self.observers.push(env.network_rule(
                        index,
                        about_held_block,
                        Action::Observe(Box::new(observe)),
                    ));
                }

                // The node's endorsements of the held block, counted once it is released.
                let released = self.released.clone();
                let endorsed = self.endorsed_after_release.clone();
                let held_hashes = self.held_hashes.clone();
                let matches = move |request: &NetworkRequests| {
                    let NetworkRequests::SpiceChunkEndorsement(_, endorsement) = request else {
                        return false;
                    };
                    held_hashes.lock().contains(endorsement.block_hash())
                };
                let observe = move |_: &NetworkRequests| {
                    if released.load(Ordering::Relaxed) > 0 {
                        endorsed.fetch_add(1, Ordering::Relaxed);
                    }
                };
                // Observed first, so an endorsement another rule holds or delays still
                // counts: the node sent it.
                self.observers.push(env.observe_messages_from(self.node, matches, observe));
            }
            Progress::Height { height, .. } => {
                if let Some(held_height) = self.held_height {
                    if self.released_at.is_none() && *height >= held_height + self.hold {
                        let network = &env.shared_state.network_shared_state;
                        self.held_hashes
                            .lock()
                            .extend(network.withheld_block_hashes(self.account_id()));
                        let delivered = network.deliver_withheld_blocks(self.account_id());
                        self.released.store(1, Ordering::Relaxed);
                        self.released_at = Some((*height, delivered));
                        for rule in &self.request_rules {
                            rule.lift();
                        }
                    }
                }
            }
            _ => {}
        }
        Vec::new()
    }

    fn extra_oracles(&self) -> Vec<Box<dyn Oracle>> {
        vec![Box::new(HeldNodeEndorsesAfterRelease {
            node: self.node,
            endorsed_after_release: self.endorsed_after_release.clone(),
            gates_at_hold: self.gates_at_hold.clone().expect("the hold was installed"),
        })]
    }

    /// The block was withheld and delivered late, and the node was sent data or
    /// endorsements about it before it had it.
    fn assert_fired(&self, _env: &TestLoopEnv, _timeline: &Timeline) {
        let held_height = self.held_height.expect("the hold was never installed");
        let (released_at, delivered) = self.released_at.expect("the block was never released");
        assert!(
            delivered > 0,
            "no block at height {held_height} was withheld from node {} by the release at {released_at}",
            self.node,
        );
        assert!(
            self.data_before_block.load(Ordering::Relaxed) > 0,
            "node {} was sent nothing about the withheld block at height {held_height} before \
             it had the block",
            self.node,
        );
        tracing::info!(
            target: "test",
            node = self.node,
            held_height,
            released_at,
            delivered,
            data_before_block = self.data_before_block.load(Ordering::Relaxed),
            swallowed = self.request_rules.iter().map(RuleHandle::matched).sum::<u64>(),
            "data_before_block held the block and released it",
        );
    }
}

/// The held node endorsed the held block once it had it, and its spice gate dropped
/// nothing from the hold on: what arrived before the block was parked, not dropped.
struct HeldNodeEndorsesAfterRelease {
    node: usize,
    endorsed_after_release: Arc<AtomicU64>,
    gates_at_hold: GateCounts,
}

impl Oracle for HeldNodeEndorsesAfterRelease {
    fn id(&self) -> OracleId {
        OracleId::HeldNodeEndorsesAfterRelease
    }

    fn check(&self, env: &TestLoopEnv, _: &RunRecord, _: &Relaxations) -> Result<(), String> {
        let endorsed = self.endorsed_after_release.load(Ordering::Relaxed);
        if endorsed == 0 {
            return Err(format!(
                "node {} sent no endorsement of the withheld block after it was delivered",
                self.node
            ));
        }
        let now = gate_counts(env, self.node);
        let mut dropped = Vec::new();
        for ((actor, kind, before), (_, _, after)) in self.gates_at_hold.iter().zip(&now) {
            if before != after {
                dropped.push(format!(
                    "the {actor} dropped {} {} messages",
                    after - before,
                    kind.as_str()
                ));
            }
        }
        if !dropped.is_empty() {
            return Err(format!(
                "node {} did not park what arrived before the withheld block: {}",
                self.node,
                dropped.join(", "),
            ));
        }
        Ok(())
    }
}

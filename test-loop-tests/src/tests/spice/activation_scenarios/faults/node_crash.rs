//! `node_crash`: a node is killed and later restarted.

use super::super::driver::RunRecord;
use super::super::oracles::{Oracle, Relaxations};
use super::super::resolve::Resolved;
use super::super::scenario::{
    Fault, FaultEvent, Narrowing, OracleId, Progress, Relaxation, Timeline,
};
use super::{producer_outage_relaxations, render};
use crate::setup::env::TestLoopEnv;
use crate::setup::state::NodeSetupState;
use crate::utils::gaps::forwarding_dead_ends;
use near_chain::ChainStoreAccess;
use near_chain_configs::TrackedShardsConfig;
use near_primitives::types::{AccountId, BlockHeight};
use std::any::Any;
use std::fmt::{self, Debug, Formatter};

/// When the node is killed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum At {
    /// The target's head is the last pre-spice block.
    LastPreSpice,
    /// The target's head is the first spice block.
    FirstSpice,
    /// The target's head is a spice block but its spice execution head is still pre-spice:
    /// the window the existing restart test covers.
    BoundaryPending,
    /// The driver's head reaches this offset, resolved as `height(k)` is. Pre-spice
    /// offsets move the boundary and land in milestone 6.
    Height(i64),
}

/// What the node tracks when it comes back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Retrack {
    Same,
    /// `TrackedShardsConfig::NoShards`: the shards it validates for and nothing more. A
    /// validator of profile A already runs that, so this changes nothing for it today;
    /// coming back tracking more would need state sync under spice.
    None,
}

const DEFAULT_DOWN: u64 = 5;

/// A node killed at a point of the timeline and restarted `down` driver blocks later,
/// catching up from where it stopped. The killed node's entry stays, and the env reports it
/// dead; the restarted node is a new entry with fresh actors, which the network rules and
/// task delays follow by account, and which a fault with handlers of its own instruments
/// again through `on_node_restarted`. The target is first run to the driver's head, so its
/// own head satisfies the point it is killed at.
pub struct NodeCrash {
    node: usize,
    at: At,
    down: u64,
    restart: bool,
    retrack: Retrack,
    identifier: Option<String>,
    account_id: Option<AccountId>,
    killed_at: Option<BlockHeight>,
    /// Whether the target's head satisfied `at` when it was killed.
    anchor_satisfied: bool,
    state: Option<NodeSetupState>,
    restarted: Option<usize>,
    /// The driver height at the restart.
    restarted_at: Option<BlockHeight>,
    /// The first driver height the restarted node had as its own head when the driver saw
    /// it: until then the node is fetching the blocks it missed and takes no transactions.
    caught_up_at: Option<BlockHeight>,
    /// Whether the restarted node executed past the boundary by `done`.
    crossed: bool,
    relaxations: Vec<Relaxation>,
}

impl Default for NodeCrash {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeCrash {
    /// Validator 0, a block producer on profile A, killed at the last pre-spice block and
    /// restarted five blocks later tracking what it tracked.
    pub fn new() -> Self {
        Self {
            node: 0,
            at: At::LastPreSpice,
            down: DEFAULT_DOWN,
            restart: true,
            retrack: Retrack::Same,
            identifier: None,
            account_id: None,
            killed_at: None,
            anchor_satisfied: false,
            state: None,
            restarted: None,
            restarted_at: None,
            caught_up_at: None,
            crossed: false,
            relaxations: Vec::new(),
        }
    }

    /// The index among the validators; the rpc node is never a target.
    pub fn node(mut self, node: usize) -> Self {
        self.node = node;
        self
    }

    pub fn at(mut self, at: At) -> Self {
        self.at = at;
        self
    }

    pub fn down(mut self, down: u64) -> Self {
        self.down = down;
        self
    }

    pub fn restart(mut self, restart: bool) -> Self {
        self.restart = restart;
        self
    }

    /// No preset needs it yet, see [`Retrack`].
    #[allow(dead_code)]
    pub fn retrack(mut self, retrack: Retrack) -> Self {
        self.retrack = retrack;
        self
    }

    pub fn target(&self) -> usize {
        self.node
    }

    fn account_id(&self) -> &AccountId {
        self.account_id.as_ref().expect("the target is looked up at built")
    }

    fn kill(&mut self, env: &mut TestLoopEnv, timeline: &Timeline) {
        let driver_height = env.rpc_node().head().height;
        if self.at != At::BoundaryPending {
            env.node_runner(self.node).run_until_head_height(driver_height);
        }
        let node = env.node(self.node);
        self.anchor_satisfied = match self.at {
            At::LastPreSpice => timeline
                .last_pre_spice
                .as_ref()
                .is_some_and(|block| node.head().last_block_hash == *block.hash()),
            At::FirstSpice => node.head_block().is_spice_block(),
            At::BoundaryPending => boundary_pending(env, self.node),
            At::Height(_) => node.head().height == driver_height,
        };
        let identifier = self.identifier.clone().expect("the target is looked up at built");
        self.state = Some(env.kill_node(&identifier));
        self.killed_at = Some(driver_height);
    }

    fn restart_node(&mut self, env: &mut TestLoopEnv) -> Vec<FaultEvent> {
        let mut state = self.state.take().expect("the node was killed");
        if self.retrack == Retrack::None {
            state.client_config.tracked_shards_config = TrackedShardsConfig::NoShards;
        }
        let identifier = self.identifier.as_ref().unwrap();
        self.restarted_at = Some(env.rpc_node().head().height);
        let index = env.restart_node(&format!("{identifier}-restart"), state);
        self.restarted = Some(index);
        vec![FaultEvent::NodeRestarted(index)]
    }

    /// The outage window, read off the chain: from the kill through the last height the
    /// crashed account skipped, per thing it produces, see the framework's
    /// `producer_outage`. A node that never came back is relaxed through the head. The
    /// loads' transactions are relaxed too where they were lost, see the framework's
    /// `forwarding_dead_ends`.
    fn resolve_relaxations(&mut self, env: &TestLoopEnv) {
        let killed_at = self.killed_at.expect("the node was killed");
        self.relaxations =
            producer_outage_relaxations(env, self.account_id(), killed_at + 1, self.restarted_at);
        let node = env.rpc_node();
        let unavailable = killed_at..self.caught_up_at.unwrap_or(node.head().height + 1);
        let lost = forwarding_dead_ends(&node, self.account_id(), unavailable);
        tracing::info!(
            target: "test",
            killed_at,
            restarted_at = ?self.restarted_at,
            caught_up_at = ?self.caught_up_at,
            lost = ?lost,
            "node_crash relaxed the transactions lost with the crashed node",
        );
        self.relaxations.push(Relaxation {
            oracle: OracleId::ExactlyOnce,
            narrowing: Narrowing::ShardHeights(lost),
        });
    }
}

/// Whether node `index`'s head is a spice block while its spice execution head is still
/// pre-spice.
fn boundary_pending(env: &TestLoopEnv, index: usize) -> bool {
    let node = env.node(index);
    if !node.head_block().is_spice_block() {
        return false;
    }
    let chain_store = &node.client().chain.chain_store;
    let Ok(execution_head) = chain_store.spice_execution_head() else {
        return false;
    };
    chain_store
        .get_block_header(&execution_head.last_block_hash)
        .is_ok_and(|header| !header.is_spice())
}

impl Debug for NodeCrash {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let at = match self.at {
            At::LastPreSpice => None,
            At::FirstSpice => Some("first_spice".to_string()),
            At::BoundaryPending => Some("boundary_pending".to_string()),
            At::Height(offset) => Some(format!("height({offset})")),
        };
        render(
            f,
            self.tag(),
            &[
                ("node", (self.node != 0).then(|| self.node.to_string())),
                ("at", at),
                ("down", (self.down != DEFAULT_DOWN).then(|| self.down.to_string())),
                ("restart", (!self.restart).then(|| "no".to_string())),
                ("retrack", (self.retrack == Retrack::None).then(|| "none".to_string())),
            ],
        )
    }
}

impl Fault for NodeCrash {
    fn tag(&self) -> &'static str {
        "node_crash"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn validate(&self, _resolved: Resolved) -> Result<(), String> {
        if self.down == 0 {
            return Err("down must be at least 1 block".to_string());
        }
        Ok(())
    }

    /// A producer crashed at a pre-spice point skips its slots before the boundary, which
    /// delays the end of the last pre-spice epoch.
    fn moves_boundary(&self, resolved: Resolved) -> bool {
        matches!(self.at, At::Height(offset) if offset <= -2)
            && resolved.profile.is_producer(self.node)
    }

    fn on_progress(
        &mut self,
        env: &mut TestLoopEnv,
        timeline: &Timeline,
        progress: &Progress,
    ) -> Vec<FaultEvent> {
        match progress {
            Progress::Built => {
                let data = env
                    .node_datas
                    .get(self.node)
                    .unwrap_or_else(|| panic!("node {} does not exist", self.node));
                self.identifier = Some(data.identifier.clone());
                self.account_id = Some(data.account_id.clone());
                let node = env.rpc_node();
                let validators = node
                    .client()
                    .epoch_manager
                    .get_epoch_all_validators(&node.head().epoch_id)
                    .unwrap();
                assert!(
                    validators.iter().any(|validator| validator.account_id() == self.account_id()),
                    "node {} is not a validator; only validators are crashed",
                    self.node,
                );
            }
            Progress::LastPreSpice if self.at == At::LastPreSpice => self.kill(env, timeline),
            Progress::FirstSpice if self.at == At::FirstSpice => self.kill(env, timeline),
            Progress::Height { offset, height } => match self.killed_at {
                None => match self.at {
                    At::Height(at) if at == *offset => self.kill(env, timeline),
                    At::BoundaryPending if *offset >= 0 && boundary_pending(env, self.node) => {
                        self.kill(env, timeline)
                    }
                    _ => {}
                },
                Some(killed_at) => {
                    if self.restart && self.restarted.is_none() && *height >= killed_at + self.down
                    {
                        return self.restart_node(env);
                    }
                    if let Some(index) = self.restarted {
                        if self.caught_up_at.is_none() && env.node(index).head().height >= *height {
                            self.caught_up_at = Some(*height);
                        }
                    }
                }
            },
            Progress::Done => {
                self.resolve_relaxations(env);
                if let Some(index) = self.restarted {
                    // The re-bootstrapped executor must execute across the boundary, not
                    // just follow headers; give it the time the existing restart test does.
                    let boundary_height = timeline.last_pre_spice_height().unwrap();
                    self.crossed = env
                        .node_runner(index)
                        .try_run_until_executed_height(boundary_height + 1)
                        .is_ok();
                }
            }
            _ => {}
        }
        Vec::new()
    }

    fn relaxes(&self) -> Vec<Relaxation> {
        self.relaxations.clone()
    }

    fn extra_oracles(&self) -> Vec<Box<dyn Oracle>> {
        match self.restarted {
            Some(index) => vec![Box::new(RestartedNodeCrosses { index, crossed: self.crossed })],
            None => Vec::new(),
        }
    }

    fn assert_fired(&self, _env: &TestLoopEnv, _timeline: &Timeline) {
        let killed_at = self.killed_at.unwrap_or_else(|| {
            panic!("node {} was never killed: {:?} never came", self.node, self.at)
        });
        assert!(
            self.anchor_satisfied,
            "node {} was killed at driver height {killed_at} with a head that does not satisfy \
             {:?}",
            self.node, self.at,
        );
        if self.restart {
            assert!(self.restarted.is_some(), "node {} was never restarted", self.node);
        }
    }
}

/// The restarted node's head is spice and its spice execution head is past the boundary.
struct RestartedNodeCrosses {
    index: usize,
    crossed: bool,
}

impl Oracle for RestartedNodeCrosses {
    fn id(&self) -> OracleId {
        OracleId::RestartedNodeCrosses
    }

    fn check(
        &self,
        env: &TestLoopEnv,
        run: &RunRecord,
        _relaxations: &Relaxations,
    ) -> Result<(), String> {
        let node = env.node(self.index);
        let boundary_height = run.last_pre_spice().header().height();
        if !node.head_block().is_spice_block() {
            return Err(format!(
                "the restarted node's head at height {} is not a spice block",
                node.head().height
            ));
        }
        let execution_head = node
            .client()
            .chain
            .chain_store
            .spice_execution_head()
            .map_err(|err| format!("the restarted node has no spice execution head: {err}"))?;
        if !self.crossed || execution_head.height <= boundary_height {
            return Err(format!(
                "the restarted node's spice execution head is at height {}, not past the last \
                 pre-spice block at {boundary_height}",
                execution_head.height,
            ));
        }
        Ok(())
    }
}

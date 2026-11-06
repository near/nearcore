//! Drives a scenario: builds the environment from the profile and the faults, runs the
//! chain one block at a time across the activation boundary firing the progress points, and
//! checks everything the oracles and the preconditions say at the end.

use super::loads::{self, Loads};
use super::oracles::{BoundaryInputs, Relaxations, always_on_oracles, read_boundary_inputs};
use super::resolve::{ACTIVATION_EPOCH_HEIGHT, Profile, Resolved, resolve};
use super::scenario::{Fault, FaultEvent, Progress, Scenario, Timeline};
use crate::setup::env::TestLoopEnv;
use crate::setup::network_rules::Action;
use crate::utils::node::TestLoopNode;
use near_async::time::Duration;
use near_chain::spice::activation::SpiceMessageKind;
use near_chain::spice::boundary::is_last_pre_spice_block;
use near_chain::{Block, ChainStoreAccess};
use near_network::types::NetworkRequests;
use near_primitives::types::{BlockHeight, EpochId, SpiceChunkId};
use parking_lot::Mutex;
use std::any::Any;
use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use strum::IntoEnumIterator as _;

/// The spice epochs the chain must leave before the terminal condition. An epoch cannot end
/// before its first block certifies, so leaving the first proves the boundary certified,
/// and the second leaves blocks past the certifying one for the supply walk and lets the
/// loads drain.
const SPICE_EPOCHS_TO_RUN: u32 = 2;

/// The time budget of a run, in epochs of ticks: a plain crossing takes about four, and
/// every fault is allowed two more for the delay it causes.
const BASE_EPOCH_BUDGET: u64 = 6;
const EPOCH_BUDGET_PER_FAULT: u64 = 2;

/// How long one tick may take, in test-loop time. A block normally takes about a second,
/// but a run of heights assigned to a dead producer costs a Doomslug skip delay each, so
/// a tick may legitimately span several of those.
const TICK_TIMEOUT: Duration = Duration::seconds(120);
/// How long certification may take to catch up with the head at `done`, in test-loop
/// time; a slow catch-up the chain waits on can put it far behind.
const CERTIFICATION_TIMEOUT: Duration = Duration::seconds(600);
/// How long the driver's node may take to execute up to the terminal height, in test-loop
/// time. Execution trails the head under spice, and the view queries read the executed state.
const EXECUTION_TIMEOUT: Duration = Duration::seconds(600);
/// How long the first full epoch may take, in test-loop time, before the run's own
/// `warmed_up` at the start of the second.
const WARMUP_TIMEOUT: Duration = Duration::seconds(120);

/// The per-actor, per-kind drop counts of one node's spice message gates.
pub type GateCounts = Vec<(&'static str, SpiceMessageKind, u64)>;

/// What the driver observed during a run: everything the oracles need besides the chain.
pub struct RunRecord {
    pub timeline: Timeline,
    pub loads: Loads,
    /// The driver node's head height when the terminal condition was reached, before
    /// certification caught up to it. The loads ended well before, so every transaction was
    /// included by then, and it is the upper bound of the view queries.
    pub terminal_height: BlockHeight,
    /// The height every node was driven to before the oracles ran, so they compare the
    /// same chain.
    pub agreement_height: BlockHeight,
    /// The chunks every contract code request observed on any node was for.
    pub code_requests: Vec<SpiceChunkId>,
    /// Per node index, the gates' drop counts when that node's head first reached the last
    /// pre-spice block.
    pub gate_snapshots: BTreeMap<usize, GateCounts>,
    /// What the oracles read of the pre-spice chain, read when the last pre-spice block was
    /// found, before garbage collection could take it.
    pub boundary_inputs: BoundaryInputs,
}

impl RunRecord {
    pub fn last_pre_spice(&self) -> &Arc<Block> {
        self.timeline.last_pre_spice.as_ref().expect("the run found the last pre-spice block")
    }

    pub fn first_spice(&self) -> &Arc<Block> {
        self.timeline.first_spice.as_ref().expect("the run found the first spice block")
    }
}

/// Runs `scenario` and panics unless every fault fired, every precondition held and every
/// oracle passed. The panic names the scenario and, for a failed check, the resolved setup
/// and each failure; any other panic on the way, such as a timeout of the test loop, is
/// reported under the scenario too.
pub fn run_scenario(scenario: Scenario) {
    let description = format!("{scenario:?}");
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drive(scenario))) {
        panic!("scenario {description} failed:\n{}", panic_message(payload.as_ref()));
    }
}

/// Builds the environment, runs the chain across the boundary and checks the run. The
/// environment is dropped here, so its shutdown finishes before the caller goes on.
fn drive(scenario: Scenario) {
    let resolved = resolve(&scenario);
    let (faults, load_rate) = scenario.into_parts();

    let mut builder = resolved.profile.builder();
    for fault in &faults {
        builder = fault.setup(resolved, builder);
    }
    let mut env = builder.build();
    let mut driver = Driver::new(resolved, faults, load_rate);

    driver.check_producers(&env);
    driver.fire(&mut env, Progress::Built);
    driver.observe_code_requests_on_every_node(&env);
    let mut env = env.warmup();
    // Every scenario votes for spice from a few blocks into the second full epoch, so the
    // run's `warmed_up` is that epoch's first block: the vote, the stake changes a fault
    // submits there and the loads' deploy all fall in the epoch whose votes and stake
    // decide the first spice epoch, the epoch after next.
    let first_epoch_id = env.rpc_node().head().epoch_id;
    env.rpc_runner().run_until(|node| node.head().epoch_id != first_epoch_id, WARMUP_TIMEOUT);
    driver.timeline.head_height = env.rpc_node().head().height;
    driver.fire(&mut env, Progress::WarmedUp);

    let extra_epochs: u64 = driver.faults.iter().map(|fault| fault.extra_epochs()).sum();
    let budget =
        (BASE_EPOCH_BUDGET + EPOCH_BUDGET_PER_FAULT * driver.faults.len() as u64 + extra_epochs)
            * resolved.profile.epoch_length();
    for _ in 0..budget {
        let head_before = env.rpc_node().head().height;
        let advanced =
            env.rpc_runner().try_run_until(|node| node.head().height > head_before, TICK_TIMEOUT);
        if advanced.is_err() {
            panic!("the chain stalled at height {head_before}\n{}", driver.describe_setup());
        }
        driver.observe(&mut env);
        if driver.reached_terminal_condition(&env) {
            driver.finish(&mut env);
            return;
        }
    }
    panic!(
        "the terminal condition was not reached within {budget} blocks: the head is at height \
         {} with {} spice epochs still to leave\n{}",
        env.rpc_node().head().height,
        driver.timeline.spice_epochs_left,
        driver.describe_setup(),
    );
}

struct Driver {
    resolved: Resolved,
    /// Whether a fault may move the last pre-spice block away from the estimate, in which
    /// case the driver reports where it landed instead of asserting the estimate.
    boundary_may_move: bool,
    faults: Vec<Box<dyn Fault>>,
    timeline: Timeline,
    loads: Loads,
    /// Every contract code request any node's peer manager was asked to send.
    code_requests: Arc<Mutex<Vec<SpiceChunkId>>>,
    /// The epoch the driver's last observed spice block is in.
    spice_epoch_id: Option<EpochId>,
    gate_snapshots: BTreeMap<usize, GateCounts>,
    /// Read when the last pre-spice block is found, see [`RunRecord::boundary_inputs`].
    boundary_inputs: Option<BoundaryInputs>,
}

impl Driver {
    fn new(resolved: Resolved, faults: Vec<Box<dyn Fault>>, load_rate: usize) -> Self {
        Self {
            resolved,
            boundary_may_move: faults.iter().any(|fault| fault.moves_boundary(resolved)),
            faults,
            timeline: Timeline::new(SPICE_EPOCHS_TO_RUN),
            loads: Loads::new(resolved.profile, load_rate),
            code_requests: Arc::default(),
            spice_epoch_id: None,
            gate_snapshots: BTreeMap::new(),
            boundary_inputs: None,
        }
    }

    /// The genesis epoch's block producers are the nodes the profile says they are, so the
    /// conflict list's producer checks hold on the chain.
    fn check_producers(&self, env: &TestLoopEnv) {
        let node = env.rpc_node();
        let mut producers: Vec<_> = node
            .client()
            .epoch_manager
            .get_epoch_block_producers_ordered(&node.head().epoch_id)
            .unwrap()
            .into_iter()
            .map(|stake| stake.take_account_id())
            .collect();
        producers.sort();
        let mut expected: Vec<_> = self
            .resolved
            .profile
            .producer_indices()
            .into_iter()
            .map(|index| env.node_datas[index].account_id.clone())
            .collect();
        expected.sort();
        assert_eq!(
            producers, expected,
            "the genesis epoch's block producers are not the nodes the profile assumes, so the \
             conflict list would check the wrong nodes",
        );
    }

    /// Fires `progress` at the loads and at every fault, then hands the faults' events to
    /// every fault.
    fn fire(&mut self, env: &mut TestLoopEnv, progress: Progress) {
        self.loads.on_progress(env, &progress);
        let mut events = Vec::new();
        for fault in &mut self.faults {
            events.extend(fault.on_progress(env, &self.timeline, &progress));
        }
        for FaultEvent::NodeRestarted(index) in events {
            self.on_node_restarted(env, index);
        }
    }

    /// The restarted node is a new entry with fresh actors; the network rules and task delays
    /// are keyed by account and follow it, and a fault with handlers of its own re-registers
    /// them here.
    fn on_node_restarted(&mut self, env: &mut TestLoopEnv, index: usize) {
        for fault in &mut self.faults {
            fault.on_node_restarted(env, index);
        }
    }

    /// Records the chunk of every contract code request any node sends, through a rule on
    /// each node that lets the request go through.
    fn observe_code_requests_on_every_node(&self, env: &TestLoopEnv) {
        for index in 0..env.node_datas.len() {
            let code_requests = self.code_requests.clone();
            env.network_rule(
                index,
                |request| matches!(request, NetworkRequests::SpiceContractCodeRequest(..)),
                Action::Observe(Box::new(move |request| {
                    if let NetworkRequests::SpiceContractCodeRequest(_, code_request) = request {
                        code_requests.lock().push(code_request.chunk_id().clone());
                    }
                })),
            );
        }
    }

    /// Processes what the chain did since the last call
    fn observe(&mut self, env: &mut TestLoopEnv) {
        let head_height = env.rpc_node().head().height;
        if self.timeline.estimate.is_none() {
            if let Some(estimate) = estimated_first_spice_height(&env.rpc_node()) {
                self.timeline.estimate = Some(estimate);
                self.fire(env, Progress::BoundaryKnown { estimate });
            }
        }
        for height in self.timeline.head_height + 1..=head_height {
            let block_hash =
                env.rpc_node().client().chain.chain_store.get_block_hash_by_height(height);
            let Ok(block_hash) = block_hash else {
                continue;
            };
            let block = env.rpc_node().block(block_hash);
            self.observe_block(env, &block);
        }
        self.timeline.head_height = head_height;
        self.snapshot_gates(env);
    }

    fn observe_block(&mut self, env: &mut TestLoopEnv, block: &Arc<Block>) {
        let height = block.header().height();
        let is_last_pre_spice = {
            let node = env.rpc_node();
            is_last_pre_spice_block(node.client().epoch_manager.as_ref(), block.hash()).unwrap()
        };
        let found_last_pre_spice = is_last_pre_spice && self.timeline.last_pre_spice.is_none();
        if found_last_pre_spice {
            self.check_boundary(env, block);
            self.boundary_inputs = Some(read_boundary_inputs(env, block));
            self.timeline.last_pre_spice = Some(block.clone());
        }
        let found_first_spice = block.is_spice_block() && self.timeline.first_spice.is_none();
        if found_first_spice {
            let last_pre_spice = self
                .timeline
                .last_pre_spice
                .as_ref()
                .expect("the first spice block came before the last pre-spice block");
            assert_eq!(
                block.header().prev_hash(),
                last_pre_spice.hash(),
                "the first spice block at height {height} is not built on the last pre-spice \
                 block at height {}",
                last_pre_spice.header().height(),
            );
            self.timeline.first_spice = Some(block.clone());
            self.spice_epoch_id = Some(*block.header().epoch_id());
        }

        // Offsets at or above `1` are measured from the actual first spice height, `-1`
        // and `0` are the boundary blocks themselves, whatever moved them, and only offsets
        // at or below `-2` are measured against the estimate: the blocks at the estimate's
        // `-1` and `0` fire nothing when the boundary moved past them.
        let offset = match (self.timeline.first_spice_height(), self.timeline.estimate) {
            (Some(first_spice_height), _) => Some(height as i64 - first_spice_height as i64),
            (None, Some(estimate)) => Some(height as i64 - estimate as i64).filter(|o| *o <= -2),
            (None, None) => None,
        };
        if let Some(offset) = offset {
            self.fire(env, Progress::Height { offset, height });
        }
        if found_last_pre_spice {
            self.fire(env, Progress::Height { offset: -1, height });
            self.fire(env, Progress::LastPreSpice);
        }
        if found_first_spice {
            self.fire(env, Progress::FirstSpice);
        } else if let Some(spice_epoch_id) = self.spice_epoch_id {
            if block.header().epoch_id() != &spice_epoch_id {
                self.spice_epoch_id = Some(*block.header().epoch_id());
                self.timeline.spice_epochs_left -= 1;
                let epochs_left_behind = SPICE_EPOCHS_TO_RUN - self.timeline.spice_epochs_left;
                self.fire(env, Progress::EpochAfter(epochs_left_behind));
            }
        }

        if self.timeline.boundary_certified_height.is_none() {
            if let Some(last_pre_spice) = &self.timeline.last_pre_spice {
                let certified = env
                    .rpc_node()
                    .client()
                    .chain
                    .spice_core_reader
                    .all_execution_results_exist(last_pre_spice.header())
                    .unwrap();
                if certified {
                    self.timeline.boundary_certified_height = Some(height);
                    self.fire(env, Progress::BoundaryCertified);
                }
            }
        }

        self.loads.tick(env);
    }

    /// Unless a fault moves the boundary, the last pre-spice block must sit right below the
    /// estimate; with one, the driver reports where it landed. Either way spice must
    /// activate at the epoch height the vote puts it at, whatever height that epoch starts
    /// at.
    fn check_boundary(&self, env: &TestLoopEnv, last_pre_spice: &Block) {
        let height = last_pre_spice.header().height();
        let estimate = self
            .timeline
            .estimate
            .expect("the last pre-spice block came before the estimate was known");
        if self.boundary_may_move {
            tracing::info!(
                target: "test",
                height,
                estimate,
                moved_by = height as i64 + 1 - estimate as i64,
                "the last pre-spice block landed relative to the estimate",
            );
        } else {
            assert!(
                height + 1 == estimate,
                "the last pre-spice block is at height {height}, but the estimate {estimate} \
                 puts it at {}: the boundary moved",
                estimate - 1,
            );
        }
        let epoch_height = env
            .rpc_node()
            .client()
            .epoch_manager
            .get_epoch_height_from_prev_block(last_pre_spice.hash())
            .unwrap();
        assert_eq!(
            epoch_height, ACTIVATION_EPOCH_HEIGHT,
            "spice activates at epoch height {epoch_height}, the vote cast at warmup puts it \
             at {ACTIVATION_EPOCH_HEIGHT}",
        );
    }

    /// The head is spice and the chain has left every spice epoch the run needs.
    fn reached_terminal_condition(&self, env: &TestLoopEnv) -> bool {
        self.timeline.spice_epochs_left == 0 && env.rpc_node().head_block().is_spice_block()
    }

    /// Snapshots the gate drop counts of every node whose head reached the last pre-spice
    /// height and has no snapshot yet.
    fn snapshot_gates(&mut self, env: &TestLoopEnv) {
        let Some(last_pre_spice_height) = self.timeline.last_pre_spice_height() else {
            return;
        };
        for index in 0..env.node_datas.len() {
            if self.gate_snapshots.contains_key(&index) || !env.is_alive(index) {
                continue;
            }
            if env.node(index).head().height >= last_pre_spice_height {
                self.gate_snapshots.insert(index, gate_counts(env, index));
            }
        }
    }

    fn describe_setup(&self) -> String {
        let height = |height: Option<BlockHeight>| {
            height.map_or_else(|| "unknown".to_string(), |height| height.to_string())
        };
        format!(
            "setup: {}; estimate {}, last pre-spice height {}, first spice height {}",
            self.resolved.describe(),
            height(self.timeline.estimate),
            height(self.timeline.last_pre_spice_height()),
            height(self.timeline.first_spice_height()),
        )
    }

    /// The terminal condition holds: lets certification catch up to the head, fires
    /// `Done`, drives every node to the same height and runs the checks. Panics with every
    /// failure.
    fn finish(mut self, env: &mut TestLoopEnv) {
        let terminal_height = env.rpc_node().head().height;
        // Certification can trail the head by a lot under a slow catch-up, so this waits
        // longer than the runner's own helper would.
        let certified = env.rpc_runner().try_run_until(
            |node| node.last_certified_block_header().height() >= terminal_height,
            CERTIFICATION_TIMEOUT,
        );
        if certified.is_err() {
            panic!(
                "certification did not catch up with the head at height {terminal_height} \
                 within {CERTIFICATION_TIMEOUT} of test-loop time\n{}",
                self.describe_setup(),
            );
        }
        // The driver's node executes asynchronously, and its executed head can trail the
        // height it certified by a few epochs' worth of blocks; the queries the oracles make
        // at a height need that block executed.
        let executed = env.rpc_runner().try_run_until(
            |node| {
                node.client()
                    .chain
                    .chain_store
                    .spice_final_execution_head()
                    .map_or(false, |tip| tip.height >= terminal_height)
            },
            EXECUTION_TIMEOUT,
        );
        if executed.is_err() {
            panic!(
                "the driver's node did not execute up to the terminal height {terminal_height} \
                 within {EXECUTION_TIMEOUT} of test-loop time\n{}",
                self.describe_setup(),
            );
        }
        self.fire(env, Progress::Done);

        self.loads.scan_inclusions(env, terminal_height);
        // The loop stops the instant the driver's predicate holds and the other nodes may
        // not have processed that block yet, so the node agreement oracle starts from
        // every node at the same height.
        let agreement_height = env.rpc_node().head().height;
        for index in env.live_node_indices() {
            let caught_up = env.node_runner(index).try_run_until_head_height(agreement_height);
            if let Err(timed_out) = caught_up {
                panic!(
                    "{} did not reach the agreement height {agreement_height}: {timed_out}\n{}",
                    env.node_datas[index].account_id,
                    self.describe_setup(),
                );
            }
        }

        let setup = self.describe_setup();
        let record = RunRecord {
            timeline: self.timeline,
            loads: self.loads,
            terminal_height,
            agreement_height,
            code_requests: self.code_requests.lock().clone(),
            gate_snapshots: self.gate_snapshots,
            boundary_inputs: self.boundary_inputs.expect("the run found the last pre-spice block"),
        };
        let mut failures = collect_failures(env, self.resolved.profile, &self.faults, &record);
        // A fault whose check needs the chain to run on past `done` runs it now; only its
        // own check runs after that.
        for fault in &mut self.faults {
            if let Err(message) = fault.post_done(env, &record.timeline) {
                failures.push((format!("{} post-done", fault.tag()), message));
            }
        }
        if !failures.is_empty() {
            let failures: Vec<String> =
                failures.iter().map(|(name, message)| format!("  {name}: {message}")).collect();
            panic!("{setup}\nfailures:\n{}", failures.join("\n"));
        }
    }
}

/// Runs every fault's fired check, the gas price precondition, and every oracle with the
/// faults' relaxations. Returns each failure with the name of what failed.
fn collect_failures(
    env: &TestLoopEnv,
    profile: Profile,
    faults: &[Box<dyn Fault>],
    record: &RunRecord,
) -> Vec<(String, String)> {
    let mut failures = Vec::new();
    for fault in faults {
        let result = catch_unwind(AssertUnwindSafe(|| fault.assert_fired(env, &record.timeline)));
        if let Err(payload) = result {
            failures.push((
                format!("fault {} did not fire", fault.tag()),
                panic_message(payload.as_ref()),
            ));
        }
    }
    if let Err(error) = loads::check_gas_price_moves(env, profile, record.last_pre_spice()) {
        failures.push(("gas_price_moves".to_string(), error));
    }

    let relaxations = Relaxations::from_faults(faults);
    let mut oracles = always_on_oracles();
    for fault in faults {
        oracles.extend(fault.extra_oracles());
    }
    for oracle in &oracles {
        if let Err(message) = oracle.check(env, record, &relaxations) {
            failures.push((oracle.name().to_string(), message));
        }
    }
    failures
}

/// The text of a panic payload.
pub fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&str>() {
        message.to_string()
    } else {
        "a panic with a payload that is not text".to_string()
    }
}

/// The estimated height of the first spice block as `node` knows it from its head.
fn estimated_first_spice_height(node: &TestLoopNode<'_>) -> Option<BlockHeight> {
    node.client()
        .epoch_manager
        .get_estimated_protocol_upgrade_block_height(node.head().last_block_hash)
        .unwrap()
}

/// The drop counts of every spice message gate of node `index`: the core writer, the data
/// distributor and the chunk validator, for every kind of message.
pub fn gate_counts(env: &TestLoopEnv, index: usize) -> GateCounts {
    let node_data = &env.node_datas[index];
    let data = &env.test_loop.data;
    let core_writer = data.get(&node_data.spice_core_writer_sender.actor_handle());
    let distributor = data.get(&node_data.spice_data_distributor_sender.actor_handle());
    let validator = data.get(&node_data.spice_chunk_validator_sender.actor_handle());
    let mut counts = Vec::new();
    for kind in SpiceMessageKind::iter() {
        counts.push(("core writer", kind, core_writer.spice_dropped_count(kind)));
        counts.push(("data distributor", kind, distributor.spice_dropped_count(kind)));
        counts.push(("chunk validator", kind, validator.spice_dropped_count(kind)));
    }
    counts
}

/// Runs until the rpc node knows the height of the first spice block, and returns it.
pub fn run_until_estimated_first_spice_height(env: &mut TestLoopEnv) -> BlockHeight {
    env.rpc_runner()
        .run_until(|node| estimated_first_spice_height(node).is_some(), Duration::seconds(100));
    estimated_first_spice_height(&env.rpc_node()).unwrap()
}

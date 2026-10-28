//! Drives a scenario: builds the environment from the profile and the faults, runs the
//! chain one block at a time across the activation boundary firing the progress points, and
//! checks everything the oracles and the preconditions say at the end.

use super::loads::{self, Loads};
use super::oracles::{Relaxations, always_on_oracles};
use super::resolve::{Profile, resolve};
use super::scenario::{Fault, FaultEvent, Progress, Scenario, Timeline};
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use crate::setup::peer_manager_actor::HandlerResult;
use crate::utils::node::TestLoopNode;
use near_async::time::Duration;
use near_chain::spice::boundary::is_last_pre_spice_block;
use near_chain::{Block, ChainStoreAccess};
use near_network::types::NetworkRequests;
use near_primitives::types::{BlockHeight, EpochId, SpiceChunkId};
use near_primitives::version::ProtocolFeature;
use parking_lot::Mutex;
use std::any::Any;
use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use near_chain::spice::activation::SpiceMessageKind;
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
    pub gate_snapshots: HashMap<usize, GateCounts>,
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
    let profile = resolve(&scenario).profile;
    let (faults, load_rate) = scenario.into_parts();

    let mut builder = profile.builder();
    for fault in &faults {
        builder = fault.setup(builder);
    }
    let mut env = builder.build();
    let mut driver = Driver::new(profile, faults, load_rate);

    driver.fire(&mut env, Progress::Built);
    driver.observe_code_requests_on_every_node(&mut env);
    let mut env = env.warmup();
    driver.timeline.head_height = env.rpc_node().head().height;
    driver.fire(&mut env, Progress::WarmedUp);

    let budget = (BASE_EPOCH_BUDGET + EPOCH_BUDGET_PER_FAULT * driver.faults.len() as u64)
        * profile.epoch_length();
    for _ in 0..budget {
        env.rpc_runner().run_for_number_of_blocks(1);
        driver.observe(&mut env);
        if driver.reached_terminal_condition(&env) {
            driver.finish(&mut env);
            return;
        }
    }
    panic!(
        "the terminal condition was not reached within {budget} blocks\n{}",
        driver.describe_setup(),
    );
}

struct Driver {
    profile: Profile,
    faults: Vec<Box<dyn Fault>>,
    timeline: Timeline,
    loads: Loads,
    /// Every contract code request any node's peer manager was asked to send.
    code_requests: Arc<Mutex<Vec<SpiceChunkId>>>,
    /// The epoch the driver's last observed spice block is in.
    spice_epoch_id: Option<EpochId>,
    gate_snapshots: HashMap<usize, GateCounts>,
}

impl Driver {
    fn new(profile: Profile, faults: Vec<Box<dyn Fault>>, load_rate: usize) -> Self {
        Self {
            profile,
            faults,
            timeline: Timeline::new(SPICE_EPOCHS_TO_RUN),
            loads: Loads::new(load_rate),
            code_requests: Arc::default(),
            spice_epoch_id: None,
            gate_snapshots: HashMap::new(),
        }
    }

    /// Fires `progress` at the loads and at every fault, then hands the faults' events to the
    /// driver and to every fault.
    fn fire(&mut self, env: &mut TestLoopEnv, progress: Progress) {
        self.loads.on_progress(env, &progress);
        let mut events = Vec::new();
        for fault in &mut self.faults {
            events.extend(fault.on_progress(env, &self.timeline, &progress));
        }
        for event in events {
            match event {
                FaultEvent::NodeRestarted(index) => self.on_node_restarted(env, index),
            }
        }
    }

    fn on_node_restarted(&mut self, env: &mut TestLoopEnv, index: usize) {
        // The restarted node has a fresh peer manager and fresh gates.
        self.observe_code_requests_on(env, index);
        self.gate_snapshots.remove(&index);
        for fault in &mut self.faults {
            fault.on_node_restarted(env, index);
        }
    }

    /// Registers a handler on every node that records the chunk of each contract code
    /// request and lets the request go through.
    fn observe_code_requests_on_every_node(&self, env: &mut TestLoopEnv) {
        for index in 0..env.node_datas.len() {
            self.observe_code_requests_on(env, index);
        }
    }

    fn observe_code_requests_on(&self, env: &mut TestLoopEnv, index: usize) {
        let code_requests = self.code_requests.clone();
        env.node_datas[index].register_override_handler(&mut env.test_loop.data, move |request| {
            if let NetworkRequests::SpiceContractCodeRequest(_, code_request) = &request {
                code_requests.lock().push(code_request.chunk_id().clone());
            }
            HandlerResult::Unhandled(request)
        });
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

        // Offsets are against the actual first spice height once it is known, and
        // against the estimate before, so `0` is the first spice block whatever moved it.
        let reference = self.timeline.first_spice_height().or(self.timeline.estimate);
        if let Some(reference) = reference {
            self.fire(env, Progress::Height { offset: height as i64 - reference as i64, height });
        }
        if found_last_pre_spice {
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

    /// Nothing moves the boundary until the faults of milestone 6 arrive, so the last
    /// pre-spice block must sit right below the estimate; and spice must activate at the
    /// epoch height the vote puts it at, whatever height that epoch starts at.
    fn check_boundary(&self, env: &TestLoopEnv, last_pre_spice: &Block) {
        let height = last_pre_spice.header().height();
        let estimate = self
            .timeline
            .estimate
            .expect("the last pre-spice block came before the estimate was known");
        assert!(
            height + 1 == estimate,
            "the last pre-spice block is at height {height}, but the estimate {estimate} puts \
             it at {}: the boundary moved",
            estimate - 1,
        );
        let epoch_height = env
            .rpc_node()
            .client()
            .epoch_manager
            .get_epoch_height_from_prev_block(last_pre_spice.hash())
            .unwrap();
        assert_eq!(
            epoch_height,
            self.profile.activation_epoch_height(),
            "spice activates at epoch height {epoch_height}, the profile expects {}",
            self.profile.activation_epoch_height(),
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
            if self.gate_snapshots.contains_key(&index) {
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
            self.profile.describe(),
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
        env.rpc_runner().run_until_certified(terminal_height);
        self.fire(env, Progress::Done);

        self.loads.scan_inclusions(env, terminal_height);
        // The loop stops the instant the driver's predicate holds and the other nodes may
        // not have processed that block yet, so the node agreement oracle starts from
        // every node at the same height.
        let agreement_height = env.rpc_node().head().height;
        for index in 0..env.node_datas.len() {
            env.node_runner(index).run_until_head_height(agreement_height);
        }

        let setup = self.describe_setup();
        let record = RunRecord {
            timeline: self.timeline,
            loads: self.loads,
            terminal_height,
            agreement_height,
            code_requests: self.code_requests.lock().clone(),
            gate_snapshots: self.gate_snapshots,
        };
        let failures = collect_failures(env, &self.faults, &record);
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
    if let Err(error) = loads::check_gas_price_moves(env, record.last_pre_spice()) {
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

/// Drops `shard_index`'s chunk at every height `pattern` marks 'x'. The pattern covers
/// the last `pattern.len()` pre-spice heights, oldest first.
pub fn drop_chunks_marked_missing(
    env: TestLoopEnv,
    shard_index: usize,
    pattern: &str,
) -> TestLoopEnv {
    let first_height_offset = -(pattern.len() as i64);
    pattern.chars().enumerate().filter(|(_, presence)| *presence == 'x').fold(
        env,
        |env, (position, _)| {
            let offset = first_height_offset + position as i64;
            env.drop(DropCondition::ProtocolUpgradeChunkRange(
                ProtocolFeature::Spice.protocol_version(),
                HashMap::from([(shard_index, offset..offset + 1)]),
            ))
        },
    )
}

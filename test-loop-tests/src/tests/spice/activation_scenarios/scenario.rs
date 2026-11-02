//! The value types of a scenario: the scenario itself, the faults it is made of, the
//! progress points faults attach to and the relaxations they declare against the oracles.

use super::oracles::Oracle;
use super::resolve::{Profile, Resolved};
use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use near_chain::Block;
use near_primitives::types::{BlockHeight, ShardId};
use std::any::Any;
use std::collections::HashSet;
use std::fmt::{self, Debug};
use std::sync::Arc;

/// A list of faults plus the knobs that are not faults. The empty scenario is the plain
/// upgrade with every oracle on.
///
/// Its `Debug` form is the string form of the design: the faults' `Debug` forms joined by
/// `,`, then the knobs that differ from their defaults, in quotes, so a failure prints what
/// to re-run.
pub struct Scenario {
    faults: Vec<Box<dyn Fault>>,
    load_rate: usize,
    /// The profile the scenario insists on; without it the resolver picks one from the
    /// faults.
    profile: Option<Profile>,
}

impl Default for Scenario {
    fn default() -> Self {
        Self::empty()
    }
}

// The builder methods below are the surface the faults of milestone 2 on use.
#[allow(dead_code)]
impl Scenario {
    /// The plain crossing: no fault, the default load rate, the profile the faults pick.
    pub fn empty() -> Self {
        Self { faults: Vec::new(), load_rate: 1, profile: None }
    }

    /// Same as [`Self::empty`], the starting point of a chain of [`Self::fault`] calls.
    pub fn new() -> Self {
        Self::empty()
    }

    pub fn fault(mut self, fault: impl Fault + 'static) -> Self {
        self.faults.push(Box::new(fault));
        self
    }

    /// Same as [`Self::fault`] for a fault already boxed, as the sampler and the pairwise
    /// group build them.
    pub fn fault_boxed(mut self, fault: Box<dyn Fault>) -> Self {
        self.faults.push(fault);
        self
    }

    /// Sets how many transactions each load submits per block. The `load:rate=N` of the
    /// string form.
    pub fn with_load_rate(mut self, load_rate: usize) -> Self {
        assert!(load_rate >= 1, "the load rate must be at least one transaction per block");
        self.load_rate = load_rate;
        self
    }

    /// Forces the profile, the `setup:profile=` of the string form. Without it `fallback`
    /// selects profile B and every other scenario runs on A; a scenario with `fallback`
    /// cannot be forced onto A.
    pub fn on_profile(mut self, profile: Profile) -> Self {
        self.profile = Some(profile);
        self
    }

    pub fn faults(&self) -> &[Box<dyn Fault>] {
        &self.faults
    }

    pub fn forced_profile(&self) -> Option<Profile> {
        self.profile
    }

    /// The faults and the load rate, for the driver, which mutates the faults as it runs.
    pub fn into_parts(self) -> (Vec<Box<dyn Fault>>, usize) {
        (self.faults, self.load_rate)
    }
}

impl Debug for Scenario {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts: Vec<String> = self.faults.iter().map(|fault| format!("{fault:?}")).collect();
        if self.load_rate != 1 {
            parts.push(format!("load:rate={}", self.load_rate));
        }
        if let Some(profile) = self.profile {
            parts.push(format!("setup:profile={}", profile.letter()));
        }
        write!(f, "\"{}\"", parts.join(","))
    }
}

/// A point on the timeline of a run. The driver detects them as it runs the chain one block
/// at a time and every fault, and the loads, may act at them.
#[derive(Clone, Debug, PartialEq, Eq)]
// The progress points no fault listens to yet are still fired.
#[allow(dead_code)]
pub enum Progress {
    /// The builder has produced the env, before warmup.
    Built,
    /// After warmup, when the chain is producing blocks in the genesis epoch.
    WarmedUp,
    /// The driver node knows the estimated height of the first spice block.
    BoundaryKnown { estimate: BlockHeight },
    /// The driver node's head reached `height`, `offset` away from the first spice height.
    /// Offsets at or below `-2` are measured against the estimate, so under a fault that
    /// moves the boundary they land up to three heights earlier, relative to the real
    /// boundary, than asked; `-1` is the last pre-spice block and `0` the first spice one,
    /// whatever moved them; offsets at or above `1` are measured from the first spice
    /// block's actual height.
    Height { offset: i64, height: BlockHeight },
    /// The driver node's head is the last pre-spice block.
    LastPreSpice,
    /// The driver node's head is a spice block for the first time.
    FirstSpice,
    /// The last pre-spice block's results are certified on the driver node.
    BoundaryCertified,
    /// The driver node has left the `n`-th spice epoch.
    EpochAfter(u32),
    /// The terminal condition is reached.
    Done,
}

/// What a fault tells the driver happened while it handled a progress point.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FaultEvent {
    /// The node was restarted; the payload is its index in `node_datas`. The driver tells
    /// every fault through [`Fault::on_node_restarted`].
    NodeRestarted(usize),
}

/// What the driver knows about the run so far.
#[derive(Debug)]
pub struct Timeline {
    /// The estimated height of the first spice block, from `BoundaryKnown` on.
    pub estimate: Option<BlockHeight>,
    /// The last pre-spice block, from `LastPreSpice` on.
    pub last_pre_spice: Option<Arc<Block>>,
    /// The first spice block, from `FirstSpice` on.
    pub first_spice: Option<Arc<Block>>,
    /// The driver node's head height when it first saw the last pre-spice block certified.
    pub boundary_certified_height: Option<BlockHeight>,
    /// Spice epochs the driver still has to leave before the terminal condition.
    pub spice_epochs_left: u32,
    /// The highest height the driver processed.
    pub head_height: BlockHeight,
}

impl Timeline {
    pub fn new(spice_epochs_to_run: u32) -> Self {
        Self {
            estimate: None,
            last_pre_spice: None,
            first_spice: None,
            boundary_certified_height: None,
            spice_epochs_left: spice_epochs_to_run,
            head_height: 0,
        }
    }

    pub fn last_pre_spice_height(&self) -> Option<BlockHeight> {
        self.last_pre_spice.as_ref().map(|block| block.header().height())
    }

    pub fn first_spice_height(&self) -> Option<BlockHeight> {
        self.first_spice.as_ref().map(|block| block.header().height())
    }
}

/// A disturbance the boundary must survive. Every implementation renders itself in its own
/// `Debug` as `tag` or `tag:key=value:...`, so a [`Scenario`] prints what to re-run.
// The hooks are called by the driver; the defaults do nothing, so a fault implements only
// what it needs.
#[allow(unused_variables)]
pub trait Fault: Debug {
    /// The tag of the design's catalog.
    fn tag(&self) -> &'static str;

    /// The fault as `Any`, so the resolver can look at the parameters of a pair of faults
    /// the design lists as a conflict.
    fn as_any(&self) -> &dyn Any;

    /// `Err` with why the fault's parameters are not runnable on `resolved`: a combination
    /// the design rejects, or a parameter whose milestone has not landed yet.
    fn validate(&self, resolved: Resolved) -> Result<(), String> {
        Ok(())
    }

    /// Build-time work: accounts, clients and client config, through the builder of
    /// `resolved`.
    fn setup(&self, resolved: Resolved, builder: TestLoopBuilder) -> TestLoopBuilder {
        builder
    }

    /// Whether the fault can skip a pre-spice height near the boundary on `resolved` and
    /// so move the last pre-spice block away from the estimate. The driver then reports
    /// where the boundary landed instead of asserting it sits at the estimate.
    fn moves_boundary(&self, resolved: Resolved) -> bool {
        false
    }

    /// Acts at `progress`. Returns what happened that other faults must know about.
    fn on_progress(
        &mut self,
        env: &mut TestLoopEnv,
        timeline: &Timeline,
        progress: &Progress,
    ) -> Vec<FaultEvent> {
        Vec::new()
    }

    /// Re-registers the fault's handlers on the restarted node `index`.
    fn on_node_restarted(&mut self, env: &mut TestLoopEnv, index: usize) {}

    /// The oracles this fault legitimately violates, with how much of each.
    fn relaxes(&self) -> Vec<Relaxation> {
        Vec::new()
    }

    /// Oracles only this fault needs.
    fn extra_oracles(&self) -> Vec<Box<dyn Oracle>> {
        Vec::new()
    }

    /// Panics unless the fault had an observable effect, so a mis-resolved height or a
    /// handler that failed to install cannot pass as a success.
    fn assert_fired(&self, env: &TestLoopEnv, timeline: &Timeline) {}

    /// Epochs of ticks the fault adds to the run's time budget, for a fault that stretches
    /// the epochs, such as a slow catch-up the chain waits on.
    fn extra_epochs(&self) -> u64 {
        0
    }

    /// Runs after the oracles, for a fault whose check needs the chain to run on past
    /// `done`. `Err` with what was violated.
    fn post_done(&mut self, env: &mut TestLoopEnv, timeline: &Timeline) -> Result<(), String> {
        Ok(())
    }
}

/// The oracles, by the id a fault names in [`Relaxation`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OracleId {
    Crossed,
    HeightContiguity,
    ChunkCompleteness,
    SupplyIdentity,
    BoundaryResults,
    StatelessEndorser,
    WithinWindow,
    LoadInFlight,
    ExactlyOnce,
    ViewQueries,
    CodeRequested,
    UndeliveredReceipts,
    NodeAgreement,
    GateQuiet,
    NoPanics,
    ArchivalCrosses,
    RestartedNodeCrosses,
    EndorsedBeforeCatchup,
    CertifiedThroughDesignated,
    JoinerSkipsBoundaryStep,
    CertifiedViaFallback,
}

impl OracleId {
    /// The id as the design's oracle table spells it.
    pub fn name(self) -> &'static str {
        match self {
            OracleId::Crossed => "crossed",
            OracleId::HeightContiguity => "height_contiguity",
            OracleId::ChunkCompleteness => "chunk_completeness",
            OracleId::SupplyIdentity => "supply_identity",
            OracleId::BoundaryResults => "boundary_results",
            OracleId::StatelessEndorser => "stateless_endorser",
            OracleId::WithinWindow => "within_window",
            OracleId::LoadInFlight => "load_in_flight",
            OracleId::ExactlyOnce => "exactly_once",
            OracleId::ViewQueries => "view_queries",
            OracleId::CodeRequested => "code_requested",
            OracleId::UndeliveredReceipts => "undelivered_receipts",
            OracleId::NodeAgreement => "node_agreement",
            OracleId::GateQuiet => "gate_quiet",
            OracleId::NoPanics => "no_panics",
            OracleId::ArchivalCrosses => "archival_crosses",
            OracleId::RestartedNodeCrosses => "restarted_node_crosses",
            OracleId::EndorsedBeforeCatchup => "endorsed_before_catchup",
            OracleId::CertifiedThroughDesignated => "certified_through_designated",
            OracleId::JoinerSkipsBoundaryStep => "joiner_skips_boundary_step",
            OracleId::CertifiedViaFallback => "certified_via_fallback",
        }
    }

    /// Whether the design's oracle table lets any fault relax this oracle. The others are
    /// "never relaxed", and a fault naming one is a bug in the fault.
    pub fn is_relaxable(self) -> bool {
        match self {
            OracleId::HeightContiguity
            | OracleId::ChunkCompleteness
            | OracleId::StatelessEndorser
            | OracleId::WithinWindow
            | OracleId::LoadInFlight
            | OracleId::ExactlyOnce
            | OracleId::CodeRequested => true,
            OracleId::Crossed
            | OracleId::SupplyIdentity
            | OracleId::BoundaryResults
            | OracleId::ViewQueries
            | OracleId::UndeliveredReceipts
            | OracleId::NodeAgreement
            | OracleId::GateQuiet
            | OracleId::NoPanics
            | OracleId::ArchivalCrosses
            | OracleId::RestartedNodeCrosses
            | OracleId::EndorsedBeforeCatchup
            | OracleId::CertifiedThroughDesignated
            | OracleId::JoinerSkipsBoundaryStep
            | OracleId::CertifiedViaFallback => false,
        }
    }
}

/// How much of an oracle a fault relaxes. Always narrowed to what the fault injected, never
/// a blanket skip.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Relaxation {
    pub oracle: OracleId,
    pub narrowing: Narrowing,
}

/// The part of an oracle a [`Relaxation`] switches off.
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub enum Narrowing {
    /// The whole oracle, or the half of it the oracle documents as relaxable.
    Whole,
    /// Every check at these heights.
    Heights(HashSet<BlockHeight>),
    /// Every check of these shards.
    Shards(HashSet<ShardId>),
    /// The checks of these shards at these heights.
    ShardHeights(HashSet<(ShardId, BlockHeight)>),
    /// The checks of this node, by index in `node_datas`.
    Node(usize),
}

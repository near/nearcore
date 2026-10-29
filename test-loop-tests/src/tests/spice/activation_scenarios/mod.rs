//! One test that drives a pre-spice chain across the spice activation boundary under an
//! arbitrary combination of faults, checked by one fixed set of oracles. The design is in
//! `docs/architecture/next/spice_activation_scenarios.md`.
//!
//! A [`Scenario`] is a list of typed faults. [`run_scenarios`] runs each through the
//! driver, which builds the environment from the profile and the faults, runs the chain one
//! block at a time firing the progress points the faults attach to, and at the end runs the
//! oracles with the relaxations the faults declare.

use driver::run_scenario;
use faults::{
    Archival, At, ChurnKind, DropContractAccesses, EndorsementDelay, Epochs, GcTight, Kind,
    LateTracker, MissingChunk, NodeCrash, Only, PartialDataFault, Producers, ShardSkip, Shards,
    SlowNode, TrackAll, ValidatorChurn, WindowEnd, WindowStart,
};
use near_o11y::testonly::init_test_logger;
use scenario::Scenario;
use std::panic::{AssertUnwindSafe, catch_unwind};

mod driver;
mod faults;
pub(crate) mod loads;
pub(crate) mod oracles;
mod resolve;
mod scenario;

pub(crate) use driver::{drop_chunks_marked_missing, run_until_estimated_first_spice_height};

/// Runs each scenario in turn and keeps going after a failure, so one run shows the whole
/// picture. Panics at the end listing every failing scenario with its message.
///
/// Each scenario runs under `catch_unwind` with `AssertUnwindSafe`, since the env's
/// captures are not unwind-safe, and the env is dropped inside it so its 30-second
/// shutdown finishes before the next scenario builds; a scenario whose shutdown panics is
/// reported as failed too.
pub fn run_scenarios(scenarios: Vec<Scenario>) {
    init_test_logger();
    let mut failures = Vec::new();
    for scenario in scenarios {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| run_scenario(scenario))) {
            failures.push(driver::panic_message(payload.as_ref()));
        }
    }
    assert!(
        failures.is_empty(),
        "{} scenarios failed:\n\n{}",
        failures.len(),
        failures.join("\n\n"),
    );
}

/// The plain crossing through the scenario driver, run in CI. Nextest's default filter
/// skips `slow_test_` names locally; run it with `--ignore-default-filter`.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn slow_test_spice_activation_scenario_empty() {
    run_scenarios(vec![Scenario::empty()]);
}

/// The cheapest fault through the scenario driver, run in CI, so the fault machinery is
/// exercised and not just type-checked: both shards' chunks missing at the last pre-spice
/// block.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn slow_test_spice_activation_scenario_missing_chunk() {
    run_scenarios(vec![Scenario::new().fault(MissingChunk::new())]);
}

/// The chunk patterns of the hand-written mixed-missing-chunks test, per shard index: the
/// presence of the shard's chunks over the last four pre-spice heights, `x` missing.
const MIXED_PATTERNS: [(&str, &str); 8] = [
    ("....", "...x"),
    ("...x", "...."),
    ("....", ".x.x"),
    ("x.x.", ".x.x"),
    ("..x.", "x..x"),
    (".xx.", "x.xx"),
    ("xx..", "..xx"),
    ("...x", ".xxx"),
];

/// The plain crossing, every fault on existing hooks on its own, and the pairs the design
/// names.
fn core_presets() -> Vec<Scenario> {
    let mut presets = vec![
        Scenario::empty(),
        Scenario::new().fault(MissingChunk::new()),
        Scenario::new().fault(MissingChunk::new().shards(Shards::Index(1))),
        Scenario::new().fault(MissingChunk::new().at(-2..0)).fault(EndorsementDelay::new()),
        Scenario::new().fault(ShardSkip::new()),
        Scenario::new().fault(ShardSkip::new()).fault(EndorsementDelay::new().blocks(4)),
        Scenario::new().fault(EndorsementDelay::new()),
        Scenario::new()
            .fault(EndorsementDelay::new().start(WindowStart::WarmedUp).end(WindowEnd::Done)),
        Scenario::new().fault(PartialDataFault::new()),
        Scenario::new().fault(PartialDataFault::new().kind(Kind::Drop)),
        Scenario::new().fault(PartialDataFault::new().kind(Kind::Corrupt)),
        Scenario::new().fault(PartialDataFault::new().kind(Kind::Equivocate)),
        Scenario::new().fault(PartialDataFault::new().kind(Kind::Drop).only(Only::Witness)),
        Scenario::new().fault(PartialDataFault::new().kind(Kind::Drop).only(Only::Receipts)),
        Scenario::new()
            .fault(PartialDataFault::new().kind(Kind::Corrupt).from(Producers::Producer(1))),
        Scenario::new().fault(DropContractAccesses::new()),
        // `shard_rotation` is not here: on profile A the shuffle does not swap the producer
        // groups at the boundary, see the fault.
        Scenario::new().fault(TrackAll::new()),
        // Pairs and a triple across the mechanisms.
        Scenario::new().fault(ShardSkip::new()).fault(MissingChunk::new().shards(Shards::Index(1))),
        Scenario::new()
            .fault(MissingChunk::new().at(-2..0))
            .fault(PartialDataFault::new().kind(Kind::Drop)),
        Scenario::new().fault(TrackAll::new()).fault(MissingChunk::new()),
        Scenario::new()
            .fault(
                EndorsementDelay::new().start(WindowStart::Offset(-5)).end(WindowEnd::Offset(-1)),
            )
            .fault(
                EndorsementDelay::new()
                    .blocks(6)
                    .start(WindowStart::Offset(0))
                    .end(WindowEnd::Offset(3)),
            ),
        Scenario::new()
            .fault(MissingChunk::new().pattern(0, "..x.").pattern(1, "x..x"))
            .fault(EndorsementDelay::new())
            .fault(PartialDataFault::new()),
    ];
    presets.extend(MIXED_PATTERNS.iter().map(|(low, upper)| {
        Scenario::new().fault(MissingChunk::new().pattern(0, low).pattern(1, upper))
    }));
    presets
}

/// Preset group, run on request.
#[test]
#[ignore]
fn ultra_slow_test_spice_activation_matrix_core() {
    run_scenarios(core_presets());
}

/// The node lifecycle faults: crashes, late trackers, churn, a slowed node, a shard silent
/// for the first spice epoch, and the pairs the design names.
fn nodes_presets() -> Vec<Scenario> {
    vec![
        Scenario::new().fault(NodeCrash::new()),
        Scenario::new().fault(NodeCrash::new().at(At::FirstSpice)),
        Scenario::new().fault(NodeCrash::new().at(At::BoundaryPending)),
        Scenario::new().fault(NodeCrash::new().restart(false)),
        Scenario::new().fault(NodeCrash::new().node(4)),
        Scenario::new().fault(NodeCrash::new().at(At::Height(2)).down(8)),
        Scenario::new()
            .fault(SlowNode::new().node(5).start(WindowStart::Offset(0)).end(WindowEnd::Offset(6))),
        // One producer and one chunk-only validator down together.
        Scenario::new().fault(NodeCrash::new()).fault(NodeCrash::new().node(4).at(At::FirstSpice)),
        Scenario::new().fault(LateTracker::new()),
        Scenario::new().fault(ValidatorChurn::new()),
        Scenario::new().fault(ValidatorChurn::new().kind(ChurnKind::Join)),
        Scenario::new().fault(ValidatorChurn::new().kind(ChurnKind::Leave)),
        Scenario::new().fault(SlowNode::new()),
        Scenario::new().fault(ShardSkip::new().epochs(Epochs::FirstSpice)),
        Scenario::new().fault(NodeCrash::new()).fault(MissingChunk::new().shards(Shards::Index(1))),
        Scenario::new().fault(NodeCrash::new()).fault(ShardSkip::new()),
        Scenario::new().fault(LateTracker::new()).fault(EndorsementDelay::new()),
        Scenario::new().fault(ValidatorChurn::new()).fault(MissingChunk::new()),
        Scenario::new().fault(SlowNode::new()).fault(ShardSkip::new().epochs(Epochs::FirstSpice)),
    ]
}

/// Preset group, run on request.
#[test]
#[ignore]
fn ultra_slow_test_spice_activation_matrix_nodes() {
    run_scenarios(nodes_presets());
}

/// Preset group, run on request.
#[test]
#[ignore]
fn ultra_slow_test_spice_activation_matrix_storage() {
    run_scenarios(vec![
        Scenario::new().fault(Archival::new()),
        Scenario::new().fault(Archival::new()).fault(ShardSkip::new()),
        Scenario::new().fault(GcTight::new()),
        Scenario::new().fault(GcTight::new()).fault(MissingChunk::new()),
    ]);
}

/// SCENARIOS UNDER INVESTIGATION: each of these fails today on what looks like a product
/// issue. This group is expected to fail
/// until the issue is fixed; a scenario leaves it for the group it belongs to once it
/// passes.
#[test]
#[ignore]
fn ultra_slow_test_spice_activation_matrix_under_investigation() {
    run_scenarios(vec![
        Scenario::new().fault(EndorsementDelay::new().blocks(24).end(WindowEnd::Done)),
        // The same one block later: heights 68 and 69 are both rejected before the chain resumes.
        Scenario::new().fault(EndorsementDelay::new().blocks(25).end(WindowEnd::Done)),
    ]);
}

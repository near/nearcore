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
    Archival, At, ChurnKind, DropContractAccesses, EndorsementDelay, Epochs, Fallback, GcTight,
    Kind, LateTracker, MissingChunk, NodeCrash, Only, PartialDataFault, Producers, ShardSkip,
    Shards, SlowNode, TrackAll, ValidatorChurn, WindowEnd, WindowStart,
};
use near_o11y::testonly::init_test_logger;
use random::RandomSpec;
use resolve::{Profile, conflicts_of};
use scenario::{Fault, Scenario};
use std::panic::{AssertUnwindSafe, catch_unwind};

mod driver;
mod faults;
pub(crate) mod loads;
pub(crate) mod oracles;
mod random;
pub(crate) mod resolve;
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

/// The storage faults: an archival node and a tight garbage collector.
fn storage_presets() -> Vec<Scenario> {
    vec![
        Scenario::new().fault(Archival::new()),
        Scenario::new().fault(Archival::new()).fault(ShardSkip::new()),
        Scenario::new().fault(GcTight::new()),
        Scenario::new().fault(GcTight::new()).fault(MissingChunk::new()),
    ]
}

/// Preset group, run on request.
#[test]
#[ignore]
fn ultra_slow_test_spice_activation_matrix_storage() {
    run_scenarios(storage_presets());
}

/// Profile B, which `fallback` selects: the all-stake fallback carries every chunk's
/// certification, the boundary's included, under the faults that leave it something to
/// certify from; and the plain crossing and the cheapest fault forced onto B without the
/// fallback, so B's size and shard count are crossed on their own.
fn fallback_presets() -> Vec<Scenario> {
    vec![
        Scenario::new().fault(Fallback::new()),
        Scenario::new().fault(Fallback::new()).fault(MissingChunk::new()),
        Scenario::new().fault(Fallback::new()).fault(TrackAll::new()),
        Scenario::new().fault(Fallback::new()).fault(DropContractAccesses::new()),
        // A chunk-only validator: every shard has one chunk producer on B, so a crashed
        // producer would lose the loads' transactions with its pool, see the conflicts.
        Scenario::new().fault(Fallback::new()).fault(NodeCrash::new().node(4)),
        // A named producer, for the same reason: all but one of one is nobody.
        Scenario::new()
            .fault(Fallback::new())
            .fault(PartialDataFault::new().from(Producers::Producer(1))),
        Scenario::empty().on_profile(Profile::B),
        Scenario::new().fault(MissingChunk::new()).on_profile(Profile::B),
    ]
}

/// Preset group, run on request.
#[test]
#[ignore]
fn ultra_slow_test_spice_activation_matrix_fallback() {
    run_scenarios(fallback_presets());
}

/// Every fault of the catalog at its defaults, in the catalog's order, for the group built
/// from pairs. `shard_rotation` is left out: it cannot fire on profile A, see the fault.
const DEFAULT_FAULTS: &[fn() -> Box<dyn Fault>] = &[
    || Box::new(MissingChunk::new()),
    || Box::new(ShardSkip::new()),
    || Box::new(EndorsementDelay::new()),
    || Box::new(PartialDataFault::new()),
    || Box::new(DropContractAccesses::new()),
    || Box::new(SlowNode::new()),
    || Box::new(NodeCrash::new()),
    || Box::new(LateTracker::new()),
    || Box::new(ValidatorChurn::new()),
    || Box::new(TrackAll::new()),
    || Box::new(GcTight::new()),
    || Box::new(Archival::new()),
];

/// Every pair of distinct default faults that the resolver accepts, in the catalog's
/// order.
fn pairwise_presets() -> Vec<Scenario> {
    let mut presets = Vec::new();
    for (position, first) in DEFAULT_FAULTS.iter().enumerate() {
        for second in &DEFAULT_FAULTS[position + 1..] {
            let scenario = Scenario::new().fault_boxed(first()).fault_boxed(second());
            if conflicts_of(&scenario).is_empty() {
                presets.push(scenario);
            }
        }
    }
    presets
}

/// The pairwise group is too long for nextest's hour, so it runs as three interleaved
/// thirds: `part` 0, 1 or 2 takes every third pair.
fn pairwise_part(part: usize) -> Vec<Scenario> {
    pairwise_presets().into_iter().skip(part).step_by(3).collect()
}

/// Preset group, run on request; the first third of the pairs.
#[test]
#[ignore]
fn ultra_slow_test_spice_activation_matrix_pairwise_a() {
    run_scenarios(pairwise_part(0));
}

/// Preset group, run on request; the second third of the pairs.
#[test]
#[ignore]
fn ultra_slow_test_spice_activation_matrix_pairwise_b() {
    run_scenarios(pairwise_part(1));
}

/// Preset group, run on request; the last third of the pairs.
#[test]
#[ignore]
fn ultra_slow_test_spice_activation_matrix_pairwise_c() {
    run_scenarios(pairwise_part(2));
}

/// Sampled scenarios from a fixed seed, run on request; the scenarios are logged before
/// the first run and a failing one is printed, so a failure is reproduced as a preset of
/// the same faults rather than by re-sampling.
#[test]
#[ignore]
fn ultra_slow_test_spice_activation_matrix_random() {
    init_test_logger();
    run_scenarios(RandomSpec::new(1).count(8).max_faults(3).sample());
}

/// SCENARIOS UNDER INVESTIGATION: each of these fails today on what looks like a product
/// issue, recorded under *Findings* in the design. This group is expected to fail until the
/// issue is fixed; a scenario leaves it for the group it belongs to once it passes.
fn under_investigation_presets() -> Vec<Scenario> {
    vec![
        Scenario::new().fault(EndorsementDelay::new().blocks(24).end(WindowEnd::Done)),
        // The same one block later: heights 68 and 69 are both rejected before the chain resumes.
        Scenario::new().fault(EndorsementDelay::new().blocks(25).end(WindowEnd::Done)),
    ]
}

/// Preset group, run on request; expected to fail, see [`under_investigation_presets`].
#[test]
#[ignore]
fn ultra_slow_test_spice_activation_matrix_under_investigation() {
    run_scenarios(under_investigation_presets());
}

/// No preset is one the resolver rejects, so a group never fails before it runs.
#[test]
fn test_presets_are_accepted_by_the_resolver() {
    let mut presets = core_presets();
    presets.extend(nodes_presets());
    presets.extend(storage_presets());
    presets.extend(fallback_presets());
    presets.extend(pairwise_presets());
    presets.extend(under_investigation_presets());
    for scenario in presets {
        assert_eq!(conflicts_of(&scenario), Vec::<String>::new(), "{scenario:?}");
    }
}

/// The same spec samples the same scenarios, none of which the resolver rejects.
#[test]
fn test_random_spec_samples_deterministically() {
    let printed = |spec: RandomSpec| -> Vec<String> {
        spec.sample().iter().map(|scenario| format!("{scenario:?}")).collect()
    };
    let first = printed(RandomSpec::new(7).count(12).max_faults(3));
    assert_eq!(first, printed(RandomSpec::new(7).count(12).max_faults(3)));
    assert_ne!(first, printed(RandomSpec::new(8).count(12).max_faults(3)));
    assert_eq!(first.len(), 12);
    for scenario in RandomSpec::new(7).count(12).max_faults(3).sample() {
        assert_eq!(conflicts_of(&scenario), Vec::<String>::new(), "{scenario:?}");
        assert!((1..=3).contains(&scenario.faults().len()), "{scenario:?}");
    }
}

/// The pairwise group covers every allowed pair once and leaves out the conflicts the
/// design lists.
#[test]
fn test_pairwise_presets_cover_the_allowed_pairs() {
    let presets = pairwise_presets();
    let printed: Vec<String> = presets.iter().map(|scenario| format!("{scenario:?}")).collect();
    let tags = DEFAULT_FAULTS.len();
    let all_pairs = tags * (tags - 1) / 2;
    assert_eq!(printed.len(), all_pairs - 2, "{printed:?}");
    assert!(!printed.contains(&"\"node_crash,gc_tight\"".to_string()));
    assert!(!printed.contains(&"\"late_tracker,track_all\"".to_string()));
    assert!(printed.contains(&"\"missing_chunk,shard_skip\"".to_string()));
    let parts: usize = (0..3).map(|part| pairwise_part(part).len()).sum();
    assert_eq!(parts, printed.len());
}

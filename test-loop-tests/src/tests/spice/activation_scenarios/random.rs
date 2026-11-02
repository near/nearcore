//! Scenarios sampled from the catalog with default-or-sampled parameters, minus the ones
//! the resolver rejects. The sampled scenarios are logged before the first run, so a
//! failure is reproduced by writing the printed scenario down as a preset, not by
//! re-sampling; the seed is part of the spec, so a nightly entry later is a fresh seed per
//! run.

use super::faults::{
    Archival, At, BadBlockMode, BadBoundaryEndorsement, BadFirstSpiceBlock, ChurnKind,
    DataBeforeBlock, DropContractAccesses, EndorsementDelay, Epochs, ForkAtBoundary, ForkKind,
    GcTight, How, Kind, LateChunk, LateTracker, MissingChunk, NodeCrash, Only, Outcome,
    PartialDataFault, Producers, ShardSkip, Shards, SlowNode, TrackAll, ValidatorChurn, What,
    WindowEnd, WindowStart,
};
use super::resolve::{Profile, conflicts_of};
use super::scenario::{Fault, Scenario};
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

const DEFAULT_COUNT: usize = 10;
const DEFAULT_MAX_FAULTS: usize = 3;
/// Attempts at a conflict-free scenario before giving up on the spec.
const MAX_ATTEMPTS: usize = 1000;

/// A sampler of one fault with its parameters at their defaults or drawn from a small set
/// of values the fault accepts on the profile.
type Sampler = fn(&mut ChaCha8Rng) -> Box<dyn Fault>;

/// One sampler per tag whose default fires on the profile, in the catalog's order;
/// `shard_rotation` cannot fire on profile A, see the fault.
const SAMPLERS: &[Sampler] = &[
    missing_chunk,
    shard_skip,
    endorsement_delay,
    partial_data_fault,
    drop_contract_accesses,
    slow_node,
    node_crash,
    late_tracker,
    validator_churn,
    track_all,
    gc_tight,
    archival,
    late_chunk,
    data_before_block,
    bad_boundary_endorsement,
    bad_first_spice_block,
    fork_at_boundary,
];

/// How many scenarios to sample from which seed, and how many faults each may have.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RandomSpec {
    seed: u64,
    count: usize,
    max_faults: usize,
}

impl RandomSpec {
    /// Ten scenarios of up to three faults from `seed`.
    pub fn new(seed: u64) -> Self {
        Self { seed, count: DEFAULT_COUNT, max_faults: DEFAULT_MAX_FAULTS }
    }

    pub fn count(mut self, count: usize) -> Self {
        self.count = count;
        self
    }

    pub fn max_faults(mut self, max_faults: usize) -> Self {
        assert!(
            (1..=SAMPLERS.len()).contains(&max_faults),
            "max_faults must be between 1 and {}, the tags that fire on the profile",
            SAMPLERS.len(),
        );
        self.max_faults = max_faults;
        self
    }

    /// The sampled scenarios, the same for the same spec on every platform; logged first.
    pub fn sample(&self) -> Vec<Scenario> {
        let mut rng = ChaCha8Rng::seed_from_u64(self.seed);
        let scenarios: Vec<Scenario> =
            (0..self.count).map(|_| sample_scenario(&mut rng, self.max_faults)).collect();
        let printed: Vec<String> =
            scenarios.iter().map(|scenario| format!("{scenario:?}")).collect();
        tracing::info!(
            target: "test",
            seed = self.seed,
            scenarios = ?printed,
            "sampled scenarios; a failing one is reproduced as a preset of the same faults",
        );
        scenarios
    }
}

/// One to `max_faults` distinct tags with sampled parameters, re-drawn until the resolver
/// accepts the combination.
fn sample_scenario(rng: &mut ChaCha8Rng, max_faults: usize) -> Scenario {
    for _ in 0..MAX_ATTEMPTS {
        let count = rng.gen_range(1..=max_faults);
        let mut samplers = SAMPLERS.to_vec();
        samplers.shuffle(rng);
        let mut scenario = Scenario::empty();
        for sampler in &samplers[..count] {
            scenario = scenario.fault_boxed(sampler(rng));
        }
        if conflicts_of(&scenario).is_empty() {
            return scenario;
        }
    }
    panic!("no conflict-free scenario of up to {max_faults} faults in {MAX_ATTEMPTS} attempts");
}

fn missing_chunk(rng: &mut ChaCha8Rng) -> Box<dyn Fault> {
    let mut fault = MissingChunk::new();
    if rng.gen_bool(0.3) {
        for shard_index in 0..Profile::A.num_shards() {
            fault = fault.pattern(shard_index, &sample_pattern(rng));
        }
    } else {
        fault = match rng.gen_range(0..3) {
            0 => fault.shards(Shards::All),
            _ => fault.shards(Shards::Index(rng.gen_range(0..Profile::A.num_shards()))),
        };
        // Up to two heights on either side of the boundary; the spice side goes by parts.
        let start = rng.gen_range(-4..2);
        let end = rng.gen_range(start + 1..=2);
        fault = fault.at(start..end);
        if rng.gen_bool(0.3) {
            fault = fault.how(How::Parts);
        }
    }
    Box::new(fault)
}

fn late_chunk(rng: &mut ChaCha8Rng) -> Box<dyn Fault> {
    let outcome = *[Outcome::Missing, Outcome::Included].choose(rng).unwrap();
    Box::new(
        LateChunk::new()
            .shard(rng.gen_range(0..Profile::A.num_shards()))
            .at(rng.gen_range(-2..2))
            .outcome(outcome),
    )
}

fn data_before_block(rng: &mut ChaCha8Rng) -> Box<dyn Fault> {
    let profile = Profile::A;
    let chunk_validators =
        profile.num_producers()..profile.num_producers() + profile.num_chunk_validators();
    let what = *[What::LastPreSpice, What::FirstSpice].choose(rng).unwrap();
    Box::new(
        DataBeforeBlock::new()
            .node(rng.gen_range(chunk_validators))
            .what(what)
            .hold(rng.gen_range(1..=4)),
    )
}

fn bad_boundary_endorsement(rng: &mut ChaCha8Rng) -> Box<dyn Fault> {
    Box::new(
        BadBoundaryEndorsement::new()
            .validator(rng.gen_range(0..Profile::A.num_validators()))
            .shard(rng.gen_range(0..Profile::A.num_shards())),
    )
}

fn bad_first_spice_block(rng: &mut ChaCha8Rng) -> Box<dyn Fault> {
    let mode =
        *[BadBlockMode::FakeBoundaryResult, BadBlockMode::SpiceHeaderPreSpice].choose(rng).unwrap();
    Box::new(BadFirstSpiceBlock::new().mode(mode))
}

fn fork_at_boundary(rng: &mut ChaCha8Rng) -> Box<dyn Fault> {
    let kind = *[ForkKind::LastPreSpice, ForkKind::FirstSpice].choose(rng).unwrap();
    Box::new(ForkAtBoundary::new().kind(kind))
}

/// Four pre-spice heights with at least one chunk missing.
fn sample_pattern(rng: &mut ChaCha8Rng) -> String {
    loop {
        let pattern: String = (0..4).map(|_| if rng.gen_bool(0.4) { 'x' } else { '.' }).collect();
        if pattern.contains('x') {
            return pattern;
        }
    }
}

fn shard_skip(rng: &mut ChaCha8Rng) -> Box<dyn Fault> {
    let epochs = *[Epochs::PreSpice, Epochs::FirstSpice].choose(rng).unwrap();
    let shard = match epochs {
        Epochs::FirstSpice => rng.gen_range(0..Profile::A.num_shards()),
        // The contract's shard is rejected for the pre-spice epochs, see the fault.
        Epochs::PreSpice | Epochs::LastPreSpice => 0,
    };
    Box::new(ShardSkip::new().shard(shard).epochs(epochs))
}

/// A window opening at warmup or between `earliest_start` and `-1`, closing at `done` or
/// between `0` and `latest_end`.
fn sample_window(
    rng: &mut ChaCha8Rng,
    earliest_start: i64,
    latest_end: i64,
) -> (WindowStart, WindowEnd) {
    let start = if rng.gen_bool(0.2) {
        WindowStart::WarmedUp
    } else {
        WindowStart::Offset(rng.gen_range(earliest_start..=-1))
    };
    let end = if rng.gen_bool(0.2) {
        WindowEnd::Done
    } else {
        WindowEnd::Offset(rng.gen_range(0..=latest_end))
    };
    (start, end)
}

fn endorsement_delay(rng: &mut ChaCha8Rng) -> Box<dyn Fault> {
    let (start, end) = sample_window(rng, -5, 4);
    Box::new(EndorsementDelay::new().blocks(rng.gen_range(1..=8)).start(start).end(end))
}

fn partial_data_fault(rng: &mut ChaCha8Rng) -> Box<dyn Fault> {
    let kind = *[Kind::Drop, Kind::Delay, Kind::Corrupt, Kind::Equivocate].choose(rng).unwrap();
    let from = match rng.gen_range(0..3) {
        0 => Producers::AllButOne,
        // Dropping or corrupting every copy leaves nothing to certify from.
        1 if matches!(kind, Kind::Delay | Kind::Equivocate) => Producers::All,
        _ => Producers::Producer(rng.gen_range(0..Profile::A.num_producers())),
    };
    let only = *[Only::Any, Only::Witness, Only::Receipts].choose(rng).unwrap();
    Box::new(PartialDataFault::new().kind(kind).from(from).only(only))
}

fn drop_contract_accesses(_rng: &mut ChaCha8Rng) -> Box<dyn Fault> {
    Box::new(DropContractAccesses::new())
}

fn slow_node(rng: &mut ChaCha8Rng) -> Box<dyn Fault> {
    let profile = Profile::A;
    let chunk_validators =
        profile.num_producers()..profile.num_producers() + profile.num_chunk_validators();
    let (start, end) = sample_window(rng, -4, 6);
    Box::new(
        SlowNode::new()
            .node(rng.gen_range(chunk_validators))
            .delay_ms(*[1000, 2000, 4000].choose(rng).unwrap())
            .start(start)
            .end(end),
    )
}

fn node_crash(rng: &mut ChaCha8Rng) -> Box<dyn Fault> {
    let profile = Profile::A;
    let validators = 0..profile.num_producers() + profile.num_chunk_validators();
    let at = match rng.gen_range(0..4) {
        0 => At::LastPreSpice,
        1 => At::FirstSpice,
        2 => At::BoundaryPending,
        _ => At::Height(rng.gen_range(0..=3)),
    };
    Box::new(
        NodeCrash::new()
            .node(rng.gen_range(validators))
            .at(at)
            .down(rng.gen_range(2..=8))
            .restart(rng.gen_bool(0.8)),
    )
}

fn late_tracker(_rng: &mut ChaCha8Rng) -> Box<dyn Fault> {
    Box::new(LateTracker::new())
}

fn validator_churn(rng: &mut ChaCha8Rng) -> Box<dyn Fault> {
    let kind = *[ChurnKind::Both, ChurnKind::Join, ChurnKind::Leave].choose(rng).unwrap();
    Box::new(ValidatorChurn::new().kind(kind))
}

fn track_all(_rng: &mut ChaCha8Rng) -> Box<dyn Fault> {
    Box::new(TrackAll::new())
}

fn gc_tight(rng: &mut ChaCha8Rng) -> Box<dyn Fault> {
    Box::new(GcTight::new().keep(rng.gen_range(4..=6)))
}

fn archival(_rng: &mut ChaCha8Rng) -> Box<dyn Fault> {
    Box::new(Archival::new())
}

//! Profile choice and the conflict list: turns a scenario into the setup it runs on.

use super::faults::{
    BadBoundaryWitness, BadFirstSpiceBlock, DataBeforeBlock, ForkAtBoundary, LateChunk,
    MissingChunk, NodeCrash, ShardSkip, SlowNode, ValidatorChurn,
};
use super::loads;
use super::scenario::{Fault as _, Scenario};
use crate::setup::builder::{MIN_BLOCK_PROD_TIME, TestLoopBuilder};
use crate::utils::account::create_validator_id;
use chrono::{DateTime, Utc};
use near_async::time::FakeClock;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_primitives::gas::Gas;
use near_primitives::test_utils::{create_test_signer, pre_spice_protocol_version};
use near_primitives::types::{
    AccountId, AccountInfo, Balance, BlockHeightDelta, EpochHeight, NumSeats,
};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::ProtocolFeature;
use near_primitives_core::num_rational::Rational32;

/// Block producers of profile A; they are also chunk producers and validators.
const A_NUM_PRODUCERS: usize = 4;
/// Chunk-only validators of profile A. With three of them the producers hold 59.5% of the
/// stake, so a boundary chunk cannot certify without the chunk-only validators.
const A_NUM_CHUNK_VALIDATORS: usize = 3;
const A_NUM_SHARDS: usize = 2;
/// Chosen against the fallback-only slots, which sit at the multiples of
/// `epoch_length / num_shards`, 10: the boundary height 64 is not one, nor are the heights
/// a skip near the boundary can move it to, while the first spice epoch has slots at 70
/// and 80, see the design's discussion of profile A.
const A_EPOCH_LENGTH: BlockHeightDelta = 21;

/// Producer seats of profile B, among [`B_NUM_VALIDATORS`] validators of equal stake, so
/// the producers hold 28.6% of it. Fewer seats than shards, so every shard has exactly one
/// chunk producer, which the conflicts below account for.
const B_NUM_PRODUCERS: usize = 4;
const B_NUM_VALIDATORS: usize = 14;
/// Six shards with two mandates each keep every chunk's designated set under a third of
/// the stake, so the non-designated remainder alone can certify it through the all-stake
/// fallback.
const B_NUM_SHARDS: usize = 6;
const B_MANDATES_PER_SHARD: NumSeats = 2;
/// Longer than the fallback window, so certification can trail by a full window without
/// the boundary chunks leaving the current epoch's uncertified set, and chosen against the
/// fallback-only slots at the multiples of `27 / 6 = 4`: the boundary height 82 is not
/// one, nor is 83.
const B_EPOCH_LENGTH: BlockHeightDelta = 27;

const GC_NUM_EPOCHS_TO_KEEP: u64 = 20;

/// The `(shard index, offset)` pairs on profile A whose chunk is its block producer's own,
/// see [`Profile::chunk_is_block_producers_own`].
const A_BLOCK_PRODUCERS_OWN_CHUNKS: &[(usize, i64)] =
    &[(0, -3), (0, -1), (1, 0), (0, 1), (1, 2), (0, 3)];

/// The epoch height spice activates at on every profile. The last pre-spice epoch is the
/// one below it and has an epoch height of its own: the epoch before that is the one
/// sharing the genesis epoch's.
pub const ACTIVATION_EPOCH_HEIGHT: EpochHeight = 3;

/// The chunk gas limit of every profile, sized for the loads. One block's load, a transfer
/// per shard and a contract call, burns about 2.9 Tgas as measured: 0.67 Tgas on the shard
/// of the low accounts and 2.21 Tgas on the shard of the upper accounts, of which the call
/// is 1.3 Tgas, mostly the loading of the contract. So the limit must be
///
/// - below 2.9 Tgas, so the load uses more than half of the block's summed chunk gas limit
///   and the gas price moves at the boundary, which the replay-pricing check relies on, and
/// - above 2.0 Tgas, what the upper shard burns before the last receipt of a block, so no
///   incoming receipt is deferred to the delayed receipts queue. A deferred deposit shows
///   up blocks late, which breaks every check of when a deposit is visible.
///
/// [`loads::check_gas_price_moves`] checks the first instead of trusting the number, and the
/// constant is adjusted by hand when it fails. The contract deploy at warmup burns about 7
/// Tgas, and is included because a chunk takes a transaction while its gas burnt so far is
/// below the limit, and applies the receipt even if it overshoots; its price spike has
/// decayed long before the boundary. On profile B the first bound cannot hold: six shards
/// sum to far more than the loads burn, so the price stays at its floor and the check does
/// not apply there.
pub const GAS_LIMIT: Gas = Gas::from_gigagas(2400);

/// The fixed setups a scenario runs on. Nothing is sized per fault: a profile is large
/// enough for every tag it admits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Profile {
    /// The default: four producers, three chunk-only validators, two shards.
    A,
    /// The all-stake fallback's: four producer seats among fourteen equal-stake
    /// validators, six shards with two mandates each. Selected by `fallback`.
    B,
}

impl Profile {
    pub fn epoch_length(self) -> BlockHeightDelta {
        match self {
            Profile::A => A_EPOCH_LENGTH,
            Profile::B => B_EPOCH_LENGTH,
        }
    }

    pub fn num_shards(self) -> usize {
        match self {
            Profile::A => A_NUM_SHARDS,
            Profile::B => B_NUM_SHARDS,
        }
    }

    pub fn num_producers(self) -> usize {
        match self {
            Profile::A => A_NUM_PRODUCERS,
            Profile::B => B_NUM_PRODUCERS,
        }
    }

    pub fn num_chunk_validators(self) -> usize {
        match self {
            Profile::A => A_NUM_CHUNK_VALIDATORS,
            Profile::B => B_NUM_VALIDATORS - B_NUM_PRODUCERS,
        }
    }

    /// The validators, which are the first entries of `node_datas`; the rpc driver comes
    /// right after them.
    pub fn num_validators(self) -> usize {
        self.num_producers() + self.num_chunk_validators()
    }

    /// The most the epoch manager allows: a backlog of transactions a silenced shard
    /// accumulates must not expire on the way, and the loads start close enough to the
    /// boundary that it does not.
    fn transaction_validity_period(self) -> BlockHeightDelta {
        2 * self.epoch_length()
    }

    /// The indices in `node_datas` of the block producers. On A the builder's default split
    /// gives the first four the larger stakes. On B every validator has the same stake and
    /// the seat assignment breaks ties by account id, so the producers are the four with
    /// the lexicographically smallest ids: `validator0`, `validator1`, `validator10` and
    /// `validator11`. The driver checks this against the genesis epoch at `built`.
    pub fn producer_indices(self) -> Vec<usize> {
        match self {
            Profile::A => (0..A_NUM_PRODUCERS).collect(),
            Profile::B => {
                let mut by_id: Vec<(AccountId, usize)> = (0..B_NUM_VALIDATORS)
                    .map(|index| (create_validator_id(index), index))
                    .collect();
                by_id.sort();
                by_id.into_iter().take(B_NUM_PRODUCERS).map(|(_, index)| index).collect()
            }
        }
    }

    pub fn is_producer(self, index: usize) -> bool {
        self.producer_indices().contains(&index)
    }

    pub fn is_chunk_only_validator(self, index: usize) -> bool {
        index < self.num_validators() && !self.is_producer(index)
    }

    /// Whether every shard has a single chunk producer, so a producer's loss, of its node
    /// or of its data, leaves nobody else to produce or to serve the shard.
    pub fn one_producer_per_shard(self) -> bool {
        self.num_producers() < self.num_shards() * 2
    }

    /// The shard index of the loads' upper accounts, where the contract lives: the last
    /// one, since the layout's boundary accounts `test1..` sort below `upper_`.
    pub fn contract_shard_index(self) -> usize {
        self.num_shards() - 1
    }

    /// Whether the chunk of `shard_index` at `offset` against the boundary is produced by
    /// the block producer of its height, who has it without the network, so that keeping
    /// its parts from the others cannot keep it out of its block and delaying them delays
    /// nothing. `None` where it was not measured: the table is the seeded assignment of
    /// profile A over offsets `-3..=3` with the boundary where the estimate puts it,
    /// read off a run that dropped the parts of every chunk there.
    pub fn chunk_is_block_producers_own(self, shard_index: usize, offset: i64) -> Option<bool> {
        match self {
            Profile::A => (-3..=3)
                .contains(&offset)
                .then(|| A_BLOCK_PRODUCERS_OWN_CHUNKS.contains(&(shard_index, offset))),
            Profile::B => None,
        }
    }

    /// The letter of the profile, as `setup:profile=` names it.
    pub fn letter(self) -> &'static str {
        match self {
            Profile::A => "a",
            Profile::B => "b",
        }
    }

    fn upgrade_schedule(self) -> ProtocolUpgradeVotingSchedule {
        let spice = ProtocolFeature::Spice.protocol_version();
        let genesis_nanos = FakeClock::default().clock().now_utc().unix_timestamp_nanos();
        let block_nanos = i128::from(MIN_BLOCK_PROD_TIME) * 1_000_000;
        let blocks_in = i128::from(self.epoch_length()) + 3;
        let vote_nanos = genesis_nanos + blocks_in * block_nanos;
        let datetime = DateTime::<Utc>::from_timestamp(
            (vote_nanos / 1_000_000_000) as i64,
            (vote_nanos % 1_000_000_000) as u32,
        )
        .expect("the vote datetime is in range");
        ProtocolUpgradeVotingSchedule::new_from_env_or_schedule(
            pre_spice_protocol_version(),
            spice,
            vec![(datetime, spice)],
        )
        .expect("a schedule of one upgrade to the client version is valid")
    }

    /// The builder of the profile, with the accounts of the loads. Returned before
    /// warmup, so the faults can add their setup and the driver can fire `Built` first.
    /// Both profiles use zero inflation, so the supply identity across the boundary is
    /// exact with no minting term, a real gas price, so transactions burn tokens and the
    /// identity subtracts a real burn, a fixed gas limit sized for the loads, the compiled
    /// contract cache disabled, delayed warmup, and the vote of [`Self::upgrade_schedule`]
    /// from one version below spice.
    pub fn builder(self) -> TestLoopBuilder {
        let builder = match self {
            Profile::A => {
                TestLoopBuilder::new().validators(A_NUM_PRODUCERS, A_NUM_CHUNK_VALIDATORS)
            }
            Profile::B => TestLoopBuilder::new()
                .validators_spec(fallback_validators_spec())
                .target_validator_mandates_per_shard(B_MANDATES_PER_SHARD),
        };
        let builder = builder
            .enable_rpc()
            .num_shards(self.num_shards())
            .epoch_length(self.epoch_length())
            .protocol_version(pre_spice_protocol_version())
            .protocol_upgrade_schedule(self.upgrade_schedule())
            .max_inflation_rate(Rational32::new(0, 1))
            .gas_prices(
                Balance::from_yoctonear(100_000_000),
                Balance::from_yoctonear(10_000_000_000),
            )
            .gas_price_adjustment_rate(Rational32::new(1, 10))
            .gas_limit(GAS_LIMIT)
            .transaction_validity_period(self.transaction_validity_period())
            .gc_num_epochs_to_keep(GC_NUM_EPOCHS_TO_KEEP)
            .disable_compiled_contract_cache()
            .delay_warmup();
        loads::add_accounts(builder)
    }
}

/// The validators of profile B as a raw spec: fourteen of equal stake, four producer seats.
/// Shared with the all-stake fallback tests, which run on the same validators.
pub(crate) fn fallback_validators_spec() -> ValidatorsSpec {
    let validators: Vec<AccountInfo> = (0..B_NUM_VALIDATORS)
        .map(|index| {
            let account_id = create_validator_id(index);
            AccountInfo {
                public_key: create_test_signer(account_id.as_str()).public_key(),
                account_id,
                amount: Balance::from_near(100),
            }
        })
        .collect();
    ValidatorsSpec::raw(
        validators,
        B_NUM_PRODUCERS as NumSeats,
        B_NUM_PRODUCERS as NumSeats,
        B_NUM_VALIDATORS as NumSeats,
    )
}

/// What a scenario resolved to: the profile it runs on, and whether an archival node is
/// added to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Resolved {
    pub profile: Profile,
    /// The builder puts the archival node right after the validators, which shifts the
    /// index of every client added after them by one.
    pub archival_node: bool,
}

impl Resolved {
    /// The setup as a failure message prints it.
    pub fn describe(self) -> String {
        format!(
            "profile {:?}: {} block producers and {} chunk-only validators, {} shards, \
             epoch length {}",
            self.profile,
            self.profile.num_producers(),
            self.profile.num_chunk_validators(),
            self.profile.num_shards(),
            self.profile.epoch_length(),
        )
    }
}

/// Picks the profile of `scenario` and rejects the combinations the design lists as
/// conflicts, naming the tags involved. Panics with every conflict found.
pub fn resolve(scenario: &Scenario) -> Resolved {
    let resolved = resolved_of(scenario);
    let errors = conflicts(scenario, resolved);
    assert!(errors.is_empty(), "the scenario is rejected:\n{}", errors.join("\n"));
    resolved
}

/// What [`resolve`] would reject `scenario` for, without panicking, for the sampler and
/// the pairwise group to leave out what cannot run.
pub fn conflicts_of(scenario: &Scenario) -> Vec<String> {
    conflicts(scenario, resolved_of(scenario))
}

/// The profile the scenario insists on, else B when it contains `fallback` and A
/// otherwise.
fn resolved_of(scenario: &Scenario) -> Resolved {
    let profile = scenario.forced_profile().unwrap_or_else(|| {
        if scenario.faults().iter().any(|fault| fault.tag() == "fallback") {
            Profile::B
        } else {
            Profile::A
        }
    });
    let archival_node = scenario.faults().iter().any(|fault| fault.tag() == "archival");
    Resolved { profile, archival_node }
}

/// The faults that skip a height at the boundary, by holding, dropping or rewriting a block
/// there.
const BOUNDARY_SKIPPERS: [&str; 3] = ["height_skip", "fork_at_boundary", "bad_first_spice_block"];

/// The conflicts and unmet requirements of `scenario` on `resolved`, one message each,
/// naming the tags involved: what each fault rejects on its own, then the pairs the
/// design lists.
fn conflicts(scenario: &Scenario, resolved: Resolved) -> Vec<String> {
    let profile = resolved.profile;
    let faults = scenario.faults();
    let mut errors: Vec<String> = faults
        .iter()
        .filter_map(|fault| {
            fault.validate(resolved).err().map(|error| format!("{}: {error}", fault.tag()))
        })
        .collect();
    let has = |tag: &str| faults.iter().any(|fault| fault.tag() == tag);
    if has("fallback") && profile != Profile::B {
        errors.push(
            "fallback: needs profile B, whose designated sets the fallback can outweigh; \
             setup:profile=a cannot hold it"
                .to_string(),
        );
    }
    if has("fallback") && has("endorsement_delay") {
        errors.push(
            "fallback with endorsement_delay: the boundary chunks' designated endorsements \
             are dropped and nothing else is endorsed until they certify through the \
             fallback window, so a delay window around the boundary holds nothing"
                .to_string(),
        );
    }

    let crashes: Vec<&NodeCrash> =
        faults.iter().filter_map(|fault| fault.as_any().downcast_ref::<NodeCrash>()).collect();
    for crash in &crashes {
        if crash.target() >= profile.num_validators() {
            errors.push(format!(
                "node_crash: node {} is not a validator; the driver and the clients the faults \
                 add are never crashed",
                crash.target(),
            ));
        }
        if has("late_tracker") && !profile.is_producer(crash.target()) {
            errors.push(format!(
                "node_crash with late_tracker: node {} is a late tracker",
                crash.target()
            ));
        }
        if profile.one_producer_per_shard() && profile.is_producer(crash.target()) {
            errors.push(format!(
                "node_crash on profile {profile:?}: node {} is the only chunk producer of its \
                 shards, so the loads' transactions forwarded to it during the outage are \
                 lost with its pool; crash a chunk-only validator, one of {:?}",
                crash.target(),
                (0..profile.num_validators())
                    .filter(|index| profile.is_chunk_only_validator(*index))
                    .collect::<Vec<_>>(),
            ));
        }
    }
    // Only producers approve blocks, and with four producer seats two down leaves half the
    // stake. The crash points of a run all sit near the boundary, so two producer crashes
    // always overlap.
    if crashes.iter().filter(|crash| profile.is_producer(crash.target())).count() >= 2 {
        errors.push(
            "node_crash with node_crash: two block producers down at once stall the chain"
                .to_string(),
        );
    }
    for slow in faults.iter().filter_map(|fault| fault.as_any().downcast_ref::<SlowNode>()) {
        if slow.target() >= profile.num_validators() {
            errors.push(format!(
                "slow_node: node {} is not a validator; the driver and the clients the faults \
                 add are never slowed",
                slow.target(),
            ));
        }
        if slow.slows_witness_validation() && has("track_all") {
            errors.push(
                "slow_node with track_all: nobody validates from a witness, so the slowed \
                 witness validation never runs"
                    .to_string(),
            );
        }
    }
    if has("gc_tight") && has("node_crash") {
        errors.push(
            "gc_tight with node_crash: catch-up needs the blocks GC would remove".to_string(),
        );
    }
    // The held last pre-spice block is meant to arrive as a losing fork, but with a delayed
    // chunk shifting the timing it reaches the driver's node first and is canonical until the
    // chain built on its parent overtakes it.
    if has("late_chunk")
        && faults
            .iter()
            .filter_map(|fault| fault.as_any().downcast_ref::<ForkAtBoundary>())
            .any(ForkAtBoundary::holds_last_pre_spice)
    {
        errors.push(
            "late_chunk with fork_at_boundary holding the last pre-spice block: the held block \
             can be canonical for a while, and the loads' transactions signed against it are \
             rejected once it loses, since their base block is no ancestor of the chain"
                .to_string(),
        );
    }
    if has("track_all") && has("late_tracker") {
        errors.push("track_all with late_tracker: nothing is tracked late".to_string());
    }

    // Two skips at the boundary leave two of the four producers on their own forks and
    // stall Doomslug, as two consecutive height skips do.
    let skippers: Vec<&str> = BOUNDARY_SKIPPERS.into_iter().filter(|tag| has(tag)).collect();
    if skippers.len() >= 2 {
        errors.push(format!(
            "{}: each skips a height at the boundary, and two skips there stall the chain",
            skippers.join(" with "),
        ));
    }
    // A crashed producer approves nothing while it is down, and a producer left on its own
    // fork by a skip approves nothing the others build on, so a crash and a skip at once
    // leave half the producer stake and stall the chain. A skip that moves the boundary is
    // over before a crash that does not begins, at the canonical last pre-spice block or
    // later; any other pair overlaps. `height_skip` is not told apart by where it falls.
    for crash in crashes.iter().filter(|crash| profile.is_producer(crash.target())) {
        for skip in faults.iter().filter(|fault| BOUNDARY_SKIPPERS.contains(&fault.tag())) {
            let apart = skip.tag() != "height_skip"
                && skip.moves_boundary(resolved)
                && !crash.moves_boundary(resolved);
            if !apart {
                errors.push(format!(
                    "node_crash with {}: producer {} is down while the skip leaves another on \
                     its own fork, which is half the producer stake and stalls the chain",
                    skip.tag(),
                    crash.target(),
                ));
            }
        }
    }
    // The withheld block is keyed by height against the estimate, so the boundary must sit
    // where the estimate puts it; the faults that hold or rewrite a boundary block also
    // compete for the same block.
    for held in faults.iter().filter_map(|fault| fault.as_any().downcast_ref::<DataBeforeBlock>()) {
        let movers: Vec<&str> = faults
            .iter()
            .filter(|fault| fault.moves_boundary(resolved))
            .map(|fault| fault.tag())
            .collect();
        if !movers.is_empty() {
            errors.push(format!(
                "data_before_block with {}: the withheld block is keyed by height against \
                 the estimate, which the other fault moves the boundary away from",
                movers.join(" and "),
            ));
        }
        for tag in ["fork_at_boundary", "bad_first_spice_block"] {
            if has(tag) {
                errors.push(format!(
                    "data_before_block with {tag}: both act on the same boundary block, one \
                     holding it back from node {} and the other keeping or rewriting it for \
                     everyone",
                    held.target(),
                ));
            }
        }
    }
    let dropped: Vec<(usize, i64)> = faults
        .iter()
        .filter_map(|fault| fault.as_any().downcast_ref::<MissingChunk>())
        .flat_map(|fault| fault.dropped_offsets(profile.num_shards()))
        .collect();
    // A chunk of a silenced shard is absent whatever else happens to it, so a delay of it
    // shows nothing and the delayed chunk is not there to arrive late.
    let epoch_length = profile.epoch_length() as i64;
    for skip in faults.iter().filter_map(|fault| fault.as_any().downcast_ref::<ShardSkip>()) {
        for late in faults.iter().filter_map(|fault| fault.as_any().downcast_ref::<LateChunk>()) {
            let (shard, offset) = late.delayed_offset();
            if skip.silences(shard, offset, epoch_length) {
                errors.push(format!(
                    "late_chunk with shard_skip: shard {shard}'s chunk at offset {offset} is \
                     silenced, so it cannot arrive late"
                ));
            }
        }
    }
    // The leaver's producer slot goes to a chunk-only validator when nobody joins, and the
    // chunk-only validators are the late trackers, whose catch-up the fault slows: as a
    // producer one of them skips its slots while it catches up.
    if has("late_tracker")
        && faults
            .iter()
            .filter_map(|fault| fault.as_any().downcast_ref::<ValidatorChurn>())
            .any(ValidatorChurn::promotes_a_chunk_only_validator)
    {
        errors.push(
            "late_tracker with validator_churn leaving and nobody joining: a late tracker \
             becomes a block producer and skips its slots while it catches up"
                .to_string(),
        );
    }
    // The late trackers stretch the first spice epoch, since an epoch cannot end before its
    // first block certifies and the certification waits on their endorsements, and a silence
    // that lasts the whole epoch then outlasts the transactions' validity period: the
    // silenced shard's senders' transactions expire unincluded.
    if has("late_tracker")
        && faults
            .iter()
            .filter_map(|fault| fault.as_any().downcast_ref::<ShardSkip>())
            .any(ShardSkip::silences_first_spice_epoch)
    {
        errors.push(
            "late_tracker with shard_skip over the first spice epoch: the late trackers \
             stretch that epoch far past the transactions' validity period, so the silenced \
             shard's senders' transactions expire"
                .to_string(),
        );
    }
    // A spice header on the last pre-spice block moves the first spice block, and the offsets
    // from `1` up, measured against the estimate, then land on the heights the first spice
    // block's offset `0` lands on, where the chunk producer can be the block producer, whose
    // chunk dropping parts cannot keep out.
    if faults
        .iter()
        .filter_map(|fault| fault.as_any().downcast_ref::<BadFirstSpiceBlock>())
        .any(BadFirstSpiceBlock::is_spice_header_pre_spice)
        && (dropped.iter().any(|(_, offset)| *offset >= 1)
            || faults
                .iter()
                .filter_map(|fault| fault.as_any().downcast_ref::<MissingChunk>())
                .any(MissingChunk::has_patterns))
    {
        errors.push(
            "missing_chunk with bad_first_spice_block in spice_header_pre_spice mode: the \
             boundary moves, so a pattern over the pre-spice heights lands on the wrong \
             blocks and the chunks dropped from offset 1 up can be their block producer's \
             own, which dropping parts cannot keep out"
                .to_string(),
        );
    }
    // The delayed chunk, still included, shifts the timing so that the crashed node is a block
    // past the last pre-spice block when the driver finds it, and cannot be killed there.
    if faults
        .iter()
        .filter_map(|fault| fault.as_any().downcast_ref::<LateChunk>())
        .any(LateChunk::arrives_included)
        && faults
            .iter()
            .filter_map(|fault| fault.as_any().downcast_ref::<NodeCrash>())
            .any(NodeCrash::kills_at_last_pre_spice)
    {
        errors.push(
            "late_chunk arriving included with node_crash at the last pre-spice block: the \
             crashed node is already past that block when the driver finds it"
                .to_string(),
        );
    }
    // The post state and implicit transition modes change a boundary witness only when
    // it carries an implicit transition, which it does when the boundary block lacks the
    // shard's chunk.
    let boundary_chunk_missing = dropped.iter().any(|(_, offset)| *offset == -1);
    for witness in
        faults.iter().filter_map(|fault| fault.as_any().downcast_ref::<BadBoundaryWitness>())
    {
        if witness.needs_missing_boundary_chunk() && !boundary_chunk_missing {
            errors.push(format!(
                "{witness:?}: the mode changes a boundary witness only when the boundary block \
                 lacks the shard's chunk; add missing_chunk at offset -1"
            ));
        }
    }
    // A delayed chunk that does not arrive in time is missing, so it cannot be one a pattern
    // asks to be present.
    let present: Vec<(usize, i64)> = faults
        .iter()
        .filter_map(|fault| fault.as_any().downcast_ref::<MissingChunk>())
        .flat_map(|fault| fault.present_offsets(profile.num_shards()))
        .collect();
    for late in faults.iter().filter_map(|fault| fault.as_any().downcast_ref::<LateChunk>()) {
        let (shard, offset) = late.delayed_offset();
        if !late.arrives_included() && present.contains(&(shard, offset)) {
            errors.push(format!(
                "late_chunk with missing_chunk: shard {shard}'s chunk at offset {offset} is \
                 delayed past the block producer's wait, but a pattern asks for it to be present"
            ));
        }
    }
    // A chunk cannot be both dropped and delayed.
    let delayed: Vec<(usize, i64)> = faults
        .iter()
        .filter_map(|fault| fault.as_any().downcast_ref::<LateChunk>())
        .map(LateChunk::delayed_offset)
        .collect();
    for (shard, offset) in &delayed {
        if dropped.contains(&(*shard, *offset)) {
            errors.push(format!(
                "late_chunk with missing_chunk: both name shard {shard}'s chunk at offset \
                 {offset}, which cannot be dropped and delayed"
            ));
        }
    }
    errors
}

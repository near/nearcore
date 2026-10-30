//! Profile choice and the conflict list: turns a scenario into the setup it runs on.

use super::faults::{NodeCrash, SlowNode};
use super::loads;
use super::scenario::Scenario;
use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_validator_id;
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
/// Odd on purpose, so the boundary height is never a fallback-only slot of the all-stake
/// fallback, see the design's discussion of profile A.
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
/// the boundary chunks leaving the current epoch's uncertified set; the boundary height 51
/// is not a fallback-only slot either, see the design.
const B_EPOCH_LENGTH: BlockHeightDelta = 25;

const GC_NUM_EPOCHS_TO_KEEP: u64 = 20;

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

    /// The epoch height spice activates at. Under an immediate vote from one version below
    /// spice the votes of the genesis epoch decide the epoch after next, and the first two
    /// epochs share the genesis epoch info and so its epoch height, so that is 2. The
    /// height that epoch starts at is the chain's business: the test genesis sits at height
    /// 1 and the first full epoch starts right above it, so it is `2 * epoch_length + 2`,
    /// 44 on profile A and 52 on B, but the driver reads it from the chain rather than
    /// computing it.
    pub fn activation_epoch_height(self) -> EpochHeight {
        2
    }

    /// The letter of the profile, as `setup:profile=` names it.
    pub fn letter(self) -> &'static str {
        match self {
            Profile::A => "a",
            Profile::B => "b",
        }
    }

    /// The builder of the profile, with the accounts of the loads. Returned before
    /// warmup, so the faults can add their setup and the driver can fire `Built` first.
    /// Both profiles use zero inflation, so the supply identity across the boundary is
    /// exact with no minting term, a real gas price, so transactions burn tokens and the
    /// identity subtracts a real burn, a fixed gas limit sized for the loads, the compiled
    /// contract cache disabled, delayed warmup, and an immediate vote from one version
    /// below spice.
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
            .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(
                ProtocolFeature::Spice.protocol_version(),
            ))
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

    /// The setup as a failure message prints it.
    pub fn describe(self) -> String {
        format!(
            "profile {self:?}: {} block producers and {} chunk-only validators, {} shards, \
             epoch length {}",
            self.num_producers(),
            self.num_chunk_validators(),
            self.num_shards(),
            self.epoch_length(),
        )
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

/// What a scenario resolved to.
#[derive(Debug)]
pub struct Resolved {
    pub profile: Profile,
}

/// Picks the profile of `scenario` and rejects the combinations the design lists as
/// conflicts, naming the tags involved. Panics with every conflict found.
pub fn resolve(scenario: &Scenario) -> Resolved {
    let profile = profile_of(scenario);
    let errors = conflicts(scenario, profile);
    assert!(errors.is_empty(), "the scenario is rejected:\n{}", errors.join("\n"));
    Resolved { profile }
}

/// What [`resolve`] would reject `scenario` for, without panicking, for the sampler and
/// the pairwise group to leave out what cannot run.
pub fn conflicts_of(scenario: &Scenario) -> Vec<String> {
    conflicts(scenario, profile_of(scenario))
}

/// The profile the scenario insists on, else B when it contains `fallback` and A otherwise.
fn profile_of(scenario: &Scenario) -> Profile {
    scenario.forced_profile().unwrap_or_else(|| {
        if scenario.faults().iter().any(|fault| fault.tag() == "fallback") {
            Profile::B
        } else {
            Profile::A
        }
    })
}

/// The conflicts and unmet requirements of `scenario` on `profile`, one message each,
/// naming the tags involved: what each fault rejects on its own, then the pairs the
/// design lists.
fn conflicts(scenario: &Scenario, profile: Profile) -> Vec<String> {
    let faults = scenario.faults();
    let mut errors: Vec<String> = faults
        .iter()
        .filter_map(|fault| {
            fault.validate(profile).err().map(|error| format!("{}: {error}", fault.tag()))
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
    // stake. Every point a crash can be killed at before milestone 6 sits at the boundary,
    // so two producer crashes always overlap.
    if crashes.iter().filter(|crash| profile.is_producer(crash.target())).count() >= 2 {
        errors.push(
            "node_crash with node_crash: two block producers down at once stall the chain"
                .to_string(),
        );
    }
    for slow in faults.iter().filter_map(|fault| fault.as_any().downcast_ref::<SlowNode>()) {
        if !profile.is_chunk_only_validator(slow.target()) {
            errors.push(format!(
                "slow_node: node {} must be a chunk-only validator until milestone 6, one of \
                 {:?} on profile {profile:?}; a slowed producer skips its pre-spice heights \
                 and moves the boundary",
                slow.target(),
                (0..profile.num_validators())
                    .filter(|index| profile.is_chunk_only_validator(*index))
                    .collect::<Vec<_>>(),
            ));
        }
    }
    if has("gc_tight") && has("node_crash") {
        errors.push(
            "gc_tight with node_crash: catch-up needs the blocks GC would remove".to_string(),
        );
    }
    if has("track_all") && has("late_tracker") {
        errors.push("track_all with late_tracker: nothing is tracked late".to_string());
    }
    errors
}

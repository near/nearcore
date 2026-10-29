//! Profile choice and the conflict list: turns a scenario into the setup it runs on.

use super::faults::{NodeCrash, SlowNode};
use super::loads;
use super::scenario::Scenario;
use crate::setup::builder::TestLoopBuilder;
use near_primitives::gas::Gas;
use near_primitives::test_utils::pre_spice_protocol_version;
use near_primitives::types::{Balance, BlockHeightDelta, EpochHeight};
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
/// The most the epoch manager allows, which is also the test default: a backlog of
/// transactions a silenced shard accumulates must not expire on the way, and the loads start
/// close enough to the boundary that it does not.
const A_TRANSACTION_VALIDITY_PERIOD: BlockHeightDelta = 2 * A_EPOCH_LENGTH;
const A_GC_NUM_EPOCHS_TO_KEEP: u64 = 20;

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
/// decayed long before the boundary.
pub const GAS_LIMIT: Gas = Gas::from_gigagas(2400);

/// The fixed setups a scenario runs on. Nothing is sized per fault: a profile is large
/// enough for every tag it admits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Profile {
    /// The default: four producers, three chunk-only validators, two shards.
    A,
}

impl Profile {
    pub fn epoch_length(self) -> BlockHeightDelta {
        match self {
            Profile::A => A_EPOCH_LENGTH,
        }
    }

    pub fn num_shards(self) -> usize {
        match self {
            Profile::A => A_NUM_SHARDS,
        }
    }

    pub fn num_producers(self) -> usize {
        match self {
            Profile::A => A_NUM_PRODUCERS,
        }
    }

    pub fn num_chunk_validators(self) -> usize {
        match self {
            Profile::A => A_NUM_CHUNK_VALIDATORS,
        }
    }

    /// The epoch height spice activates at. Under an immediate vote from one version below
    /// spice the votes of the genesis epoch decide the epoch after next, and the first two
    /// epochs share the genesis epoch info and so its epoch height, so that is 2. The
    /// height that epoch starts at is the chain's business: the test genesis sits at height
    /// 1 and the first full epoch starts right above it, so it is `2 * epoch_length + 2`,
    /// 44 on profile A, but the driver reads it from the chain rather than computing it.
    pub fn activation_epoch_height(self) -> EpochHeight {
        2
    }

    /// The builder of the profile, with the accounts of the loads. Returned before
    /// warmup, so the faults can add their setup and the driver can fire `Built` first.
    pub fn builder(self) -> TestLoopBuilder {
        match self {
            Profile::A => {
                let builder = TestLoopBuilder::new()
                    .validators(A_NUM_PRODUCERS, A_NUM_CHUNK_VALIDATORS)
                    .enable_rpc()
                    .num_shards(A_NUM_SHARDS)
                    .epoch_length(A_EPOCH_LENGTH)
                    .protocol_version(pre_spice_protocol_version())
                    .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(
                        ProtocolFeature::Spice.protocol_version(),
                    ))
                    // Zero inflation, so the supply identity across the boundary is exact
                    // with no minting term. A real gas price, so transactions burn tokens
                    // and the identity subtracts a real burn.
                    .max_inflation_rate(Rational32::new(0, 1))
                    .gas_prices(
                        Balance::from_yoctonear(100_000_000),
                        Balance::from_yoctonear(10_000_000_000),
                    )
                    .gas_price_adjustment_rate(Rational32::new(1, 10))
                    .gas_limit(GAS_LIMIT)
                    .transaction_validity_period(A_TRANSACTION_VALIDITY_PERIOD)
                    .gc_num_epochs_to_keep(A_GC_NUM_EPOCHS_TO_KEEP)
                    .disable_compiled_contract_cache()
                    .delay_warmup();
                loads::add_accounts(builder)
            }
        }
    }

    /// The setup as a failure message prints it, with `estimate` the height the chain
    /// reported, if it did.
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

/// What a scenario resolved to.
#[derive(Debug)]
pub struct Resolved {
    pub profile: Profile,
}

/// Picks the profile of `scenario` and rejects the combinations the design lists as
/// conflicts, naming the tags involved. Panics with every conflict found.
pub fn resolve(scenario: &Scenario) -> Resolved {
    // Profile B arrives with the `fallback` fault, which selects it; until then every
    // scenario runs on A.
    let profile = Profile::A;
    let errors = conflicts(scenario, profile);
    assert!(errors.is_empty(), "the scenario is rejected:\n{}", errors.join("\n"));
    Resolved { profile }
}

/// The conflicts and unmet requirements of `scenario` on `profile`, one message each,
/// naming the tags involved: what each fault rejects on its own, then the pairs the
/// design lists.
fn conflicts(scenario: &Scenario, profile: Profile) -> Vec<String> {
    let faults = scenario.faults();
    let mut errors: Vec<String> = faults
        .iter()
        .filter_map(|fault| fault.validate().err().map(|error| format!("{}: {error}", fault.tag())))
        .collect();
    let has = |tag: &str| faults.iter().any(|fault| fault.tag() == tag);
    let num_validators = profile.num_producers() + profile.num_chunk_validators();

    let crashes: Vec<&NodeCrash> =
        faults.iter().filter_map(|fault| fault.as_any().downcast_ref::<NodeCrash>()).collect();
    for crash in &crashes {
        if crash.target() >= num_validators {
            errors.push(format!(
                "node_crash: node {} is not a validator; the driver and the clients the faults \
                 add are never crashed",
                crash.target(),
            ));
        }
        if has("late_tracker") && crash.target() >= profile.num_producers() {
            errors.push(format!(
                "node_crash with late_tracker: node {} is a late tracker",
                crash.target()
            ));
        }
    }
    // Only producers approve blocks, and with four producer seats two down leaves half the
    // stake. Every point a crash can be killed at before milestone 6 sits at the boundary,
    // so two producer crashes always overlap.
    if crashes.iter().filter(|crash| crash.target() < profile.num_producers()).count() >= 2 {
        errors.push(
            "node_crash with node_crash: two block producers down at once stall the chain"
                .to_string(),
        );
    }
    for slow in faults.iter().filter_map(|fault| fault.as_any().downcast_ref::<SlowNode>()) {
        if slow.target() < profile.num_producers() || slow.target() >= num_validators {
            errors.push(format!(
                "slow_node: node {} must be a chunk-only validator until milestone 6; a slowed \
                 producer skips its pre-spice heights and moves the boundary",
                slow.target(),
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

use lazy_static::lazy_static;
use near_primitives::epoch_manager::{EpochConfig, RngSeed};
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::errors::EpochError;
use near_primitives::rand::WeightedIndex;
use near_primitives::types::{
    AccountId, Balance, ProtocolVersion, ValidatorKickoutReason,
};
use near_primitives::types::validator_stake::ValidatorStake;
use num_rational::Ratio;
use std::cmp::{self, Ordering};
use std::collections::{BTreeMap, BinaryHeap, HashMap};

const MAX_NUM_BP: usize = 100;
const MAX_NUM_CP: usize = 300;
// Derived from PROBABILITY_NEVER_SELECTED = 0.001 and
// epoch_length = 43200
// TODO setup like neard/res/genesis_config.json:  "minimum_stake_divisor": 10
lazy_static! {
    static ref MIN_STAKE_RATIO: Ratio<u128> = Ratio::new(160, 1_000_000);
}

pub fn proposals_to_epoch_info(
    epoch_config: &EpochConfig,
    rng_seed: RngSeed,
    prev_epoch_info: &EpochInfo,
    block_producer_proposals: Vec<ValidatorStake>,
    chunk_producer_proposals: Vec<ValidatorStake>,
    mut validator_kickout: HashMap<AccountId, ValidatorKickoutReason>,
    validator_reward: HashMap<AccountId, Balance>,
    minted_amount: Balance,
    next_version: ProtocolVersion,
) -> Result<EpochInfo, EpochError> {
    // TODO: how to roll over block/chunk producers from the previous epoch?
    Err(EpochError::IOErr("TODO".to_string()))
}

fn order_proposals<I: IntoIterator<Item = ValidatorStake>>(
    proposals: I,
    mut stake_change: BTreeMap<AccountId, Balance>,
    validator_kickout: &HashMap<AccountId, ValidatorKickoutReason>,
) -> BinaryHeap<OrderedValidatorStake> {
    let mut ordered_proposals = BinaryHeap::new();
    for p in proposals {
        if validator_kickout.contains_key(p.account_id()) {
            stake_change.insert(p.take_account_id(), 0);
        } else {
            stake_change.insert(p.account_id().clone(), p.stake());
            ordered_proposals.push(OrderedValidatorStake(p));
        }
    }
    ordered_proposals
}

fn select_block_producers(
    block_producer_proposals: &mut BinaryHeap<OrderedValidatorStake>,
) -> (Vec<ValidatorStake>, WeightedIndex) {
    let block_producers = select_validators(block_producer_proposals, MAX_NUM_BP, *MIN_STAKE_RATIO);
    let weights = block_producers.iter().map(|bp| bp.stake()).collect();
    let block_producer_sampler = WeightedIndex::new(weights);
    (block_producers, block_producer_sampler)
}

fn select_chunk_producers(
    all_proposals: &mut BinaryHeap<OrderedValidatorStake>,
    num_shards: u64,
) -> Vec<ValidatorStake> {
    select_validators(
        all_proposals,
        MAX_NUM_BP + MAX_NUM_CP,
        *MIN_STAKE_RATIO * Ratio::new(1, num_shards as u128),
    )
}

fn select_validators(
    proposals: &mut BinaryHeap<OrderedValidatorStake>,
    max_number_selected: usize,
    min_stake_ratio: Ratio<u128>,
) -> Vec<ValidatorStake> {
    let mut total_stake = 0;
    let n = cmp::min(max_number_selected, proposals.len());
    let mut validators = Vec::with_capacity(n);
    for _ in 0..n {
        let p = proposals.pop().unwrap().0;
        let p_stake = p.stake();
        let total_stake_with_p = total_stake + p_stake;
        if Ratio::new(p_stake, total_stake_with_p) > min_stake_ratio {
            validators.push(p);
            total_stake = total_stake_with_p;
        } else {
            break;
        }
    }
    validators
}

#[derive(Eq, PartialEq)]
struct OrderedValidatorStake(ValidatorStake);
impl PartialOrd for OrderedValidatorStake {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let stake_order = self.0.stake().partial_cmp(&other.0.stake())?;
        match stake_order {
            Ordering::Equal => self.0.account_id().partial_cmp(other.0.account_id()),
            Ordering::Less | Ordering::Greater => Some(stake_order),
        }
    }
}
impl Ord for OrderedValidatorStake {
    fn cmp(&self, other: &Self) -> Ordering {
        let stake_order = self.0.stake().cmp(&other.0.stake());
        match stake_order {
            Ordering::Equal => self.0.account_id().cmp(other.0.account_id()),
            Ordering::Less | Ordering::Greater => stake_order,
        }
    }
}

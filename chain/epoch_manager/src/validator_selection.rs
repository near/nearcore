use lazy_static::lazy_static;
use near_primitives::epoch_manager::{EpochConfig, RngSeed};
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::errors::EpochError;
use near_primitives::rand::WeightedIndex;
use near_primitives::types::{AccountId, Balance, ProtocolVersion, ValidatorKickoutReason, ValidatorId};
use near_primitives::types::validator_stake::ValidatorStake;
use num_rational::Ratio;
use std::cmp::{self, Ordering};
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet, hash_map};
use crate::shard_assignment::assign_shards;

const MAX_NUM_BP: usize = 100;
const MAX_NUM_CP: usize = 300;
const MIN_VALIDATORS_PER_SHARD: usize = 1; // TODO: what should this value be?
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
    proposals: Vec<ValidatorStake>,
    mut validator_kickout: HashMap<AccountId, ValidatorKickoutReason>,
    validator_reward: HashMap<AccountId, Balance>,
    minted_amount: Balance,
    next_version: ProtocolVersion,
    num_shards: u64,
) -> Result<EpochInfo, EpochError> {
    debug_assert!(
        proposals.iter().map(|stake| stake.account_id()).collect::<HashSet<_>>().len()
            == proposals.len(),
        "Proposals should not have duplicates"
    );

    let mut stake_change = BTreeMap::new();
    let mut fishermen = vec![];
    let proposals = proposals_with_rollover(
        proposals,
        prev_epoch_info,
        &validator_reward,
        &validator_kickout,
        &mut stake_change,
        &mut fishermen,
    );
    let mut block_producer_proposals = order_proposals(
        proposals
            .values()
            .filter(|p| !p.is_chunk_only())
            .cloned()
    );
    let (block_producers, bp_sampler, bp_stake_threshold) = select_block_producers(&mut block_producer_proposals);
    let mut chunk_producer_proposals = order_proposals(
        proposals.into_iter().map(|(_, p)| p)
    );
    let (chunk_producers, cp_stake_threshold) = select_chunk_producers(&mut chunk_producer_proposals, num_shards);

    // since block producer proposals could become chunk producers, their actual stake threshold
    // is the smaller of the two thresholds
    let bp_stake_threshold = bp_stake_threshold.and_then(|x| cp_stake_threshold.map(|y| cmp::min(x, y)));

    // proposals remaining chunk_producer_proposals were not selected for either role
    for OrderedValidatorStake(p) in chunk_producer_proposals {
        let stake = p.stake();
        let account_id = p.account_id();
        if stake > epoch_config.fishermen_threshold {
            fishermen.push(p);
        } else {
            *stake_change.get_mut(account_id).unwrap() = 0;
            if prev_epoch_info.account_is_validator(account_id)
                || prev_epoch_info.account_is_fisherman(account_id)
            {
                let threshold = if p.is_chunk_only() {
                    // the stake threshold must be some value since
                    // a proposal was not chosen
                    debug_assert!(cp_stake_threshold.is_some());
                    cp_stake_threshold.unwrap_or_default()
                } else {
                    debug_assert!(bp_stake_threshold.is_some());
                    bp_stake_threshold.unwrap_or_default()
                };
                debug_assert!(stake < threshold);
                let account_id = p.take_account_id();
                validator_kickout.insert(
                    account_id,
                    ValidatorKickoutReason::NotEnoughStake { stake, threshold },
                );
            }
        }
    }

    let num_chunk_producers = chunk_producers.len();
    let mut all_validators: Vec<ValidatorStake> = Vec::with_capacity(num_chunk_producers);
    let mut validator_to_index = HashMap::new();
    let mut block_producers_settlement = Vec::with_capacity(block_producers.len());

    for (i, bp) in block_producers.into_iter().enumerate() {
        let id = i as ValidatorId;
        validator_to_index.insert(bp.account_id().clone(), id);
        block_producers_settlement.push(id);
        all_validators.push(bp);
    }

    let shard_assignment = assign_shards(
        chunk_producers,
        num_shards as usize,
        MIN_VALIDATORS_PER_SHARD
    ).map_err(|_| EpochError::NotEnoughValidators { num_validators: num_chunk_producers as u64, num_shards})?;

    let mut chunk_producers_settlement: Vec<Vec<ValidatorId>> =
        shard_assignment.iter().map(|vs| Vec::with_capacity(vs.len())).collect();
    let mut i = all_validators.len();
    for (shard_validators, shard_validator_ids) in shard_assignment.into_iter().zip(chunk_producers_settlement.iter_mut()) {
        for validator in shard_validators {
            debug_assert_eq!(i, all_validators.len());
            match validator_to_index.entry(validator.account_id().clone()) {
                hash_map::Entry::Vacant(entry) => {
                    let validator_id = i as ValidatorId;
                    entry.insert(validator_id);
                    shard_validator_ids.push(validator_id);
                    all_validators.push(validator);
                    i += 1;
                }
                // Validators which have an entry in the validator_to_index map
                // have already been inserted into `all_validators`.
                hash_map::Entry::Occupied(entry) => {
                    let validator_id = *entry.get();
                    shard_validator_ids.push(validator_id);
                }
            }
        }
    }

    let fishermen_to_index = fishermen
        .iter()
        .enumerate()
        .map(|(index, s)| (s.account_id().clone(), index as ValidatorId))
        .collect::<HashMap<_, _>>();

    let threshold = match (bp_stake_threshold, cp_stake_threshold) {
        (Some(x), Some(y)) => cmp::max(x, y),
        (None, Some(y)) => y,
        (Some(x), None) => x,
        (None, None) => 0,
    };

    Ok(EpochInfo::new(
        prev_epoch_info.epoch_height() + 1,
        all_validators,
        validator_to_index,
        block_producers_settlement,
        chunk_producers_settlement,
        vec![],
        fishermen,
        fishermen_to_index,
        stake_change,
        validator_reward,
        validator_kickout,
        minted_amount,
        threshold,
        next_version,
    ))
}

fn proposals_with_rollover(
    proposals: Vec<ValidatorStake>,
    prev_epoch_info: &EpochInfo,
    validator_reward: &HashMap<AccountId, Balance>,
    validator_kickout: &HashMap<AccountId, ValidatorKickoutReason>,
    stake_change: &mut BTreeMap<AccountId, Balance>,
    fishermen: &mut Vec<ValidatorStake>
) -> HashMap<AccountId, ValidatorStake> {
    let mut proposals_by_account = HashMap::new();
    for p in proposals {
        let account_id = p.account_id();
        if validator_kickout.contains_key(account_id) {
            let account_id = p.take_account_id();
            stake_change.insert(account_id, 0);
        } else {
            stake_change.insert(account_id.clone(), p.stake());
            proposals_by_account.insert(account_id.clone(), p);
        }
    }

    for r in prev_epoch_info.validators_iter() {
        let account_id = r.account_id().clone();
        if validator_kickout.contains_key(&account_id) {
            stake_change.insert(account_id, 0);
            continue;
        }
        let p = proposals_by_account.entry(account_id).or_insert(r);
        if let Some(reward) = validator_reward.get(p.account_id()) {
            *p.stake_mut() += *reward;
        }
        stake_change.insert(p.account_id().clone(), p.stake());
    }

    for r in prev_epoch_info.fishermen_iter() {
        let account_id = r.account_id();
        if validator_kickout.contains_key(account_id) {
            stake_change.insert(account_id.clone(), 0);
            continue;
        }
        if !proposals_by_account.contains_key(account_id) {
            // safe to do this here because fishermen from previous epoch is guaranteed to have no
            // duplicates.
            stake_change.insert(account_id.clone(), r.stake());
            fishermen.push(r);
        }
    }

    proposals_by_account
}

fn order_proposals<I: IntoIterator<Item = ValidatorStake>>(
    proposals: I,
) -> BinaryHeap<OrderedValidatorStake> {
    let mut ordered_proposals = BinaryHeap::new();
    for p in proposals {
        ordered_proposals.push(OrderedValidatorStake(p));
    }
    ordered_proposals
}

fn select_block_producers(
    block_producer_proposals: &mut BinaryHeap<OrderedValidatorStake>,
) -> (Vec<ValidatorStake>, WeightedIndex, Option<Balance>) {
    let (block_producers, stake_threshold) = select_validators(block_producer_proposals, MAX_NUM_BP, *MIN_STAKE_RATIO);
    let weights = block_producers.iter().map(|bp| bp.stake()).collect();
    let block_producer_sampler = WeightedIndex::new(weights);
    (block_producers, block_producer_sampler, stake_threshold)
}

fn select_chunk_producers(
    all_proposals: &mut BinaryHeap<OrderedValidatorStake>,
    num_shards: u64,
) -> (Vec<ValidatorStake>, Option<Balance>) {
    select_validators(
        all_proposals,
        MAX_NUM_BP + MAX_NUM_CP,
        *MIN_STAKE_RATIO * Ratio::new(1, num_shards as u128),
    )
}

// Takes the top N proposals (by stake), or fewer if there are not enough or the
// next proposals is too small relative to the others. In the case where all N
// slots are filled, or the stake ratio falls too low, the threshold stake to be included
// is also returned.
fn select_validators(
    proposals: &mut BinaryHeap<OrderedValidatorStake>,
    max_number_selected: usize,
    min_stake_ratio: Ratio<u128>,
) -> (Vec<ValidatorStake>, Option<Balance>) {
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
    if n < max_number_selected {
        // there were fewer proposals than the maximum allowed, so there is no threshold stake
        (validators, None)
    } else if validators.len() == n {
        // all n slots were filled, so the threshold stake is 1 more than the current
        // smallest stake
        let threshold = validators.last().unwrap().stake() + 1;
        (validators, Some(threshold))
    } else {
        // the stake ratio condition prevented all slots from being filled,
        // so the threshold stake is whatever amount would have passed this check
        let threshold = (min_stake_ratio * Ratio::new(total_stake, 1)).ceil().to_integer();
        (validators, Some(threshold))
    }
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
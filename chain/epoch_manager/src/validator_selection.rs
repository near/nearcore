use crate::shard_assignment::assign_shards;
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::epoch_manager::{EpochConfig, RngSeed};
use near_primitives::errors::EpochError;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, Balance, NumShards, ProtocolVersion, ValidatorId, ValidatorKickoutReason,
};
use num_rational::Ratio;
use std::cmp::{self, Ordering};
use std::collections::{hash_map, BTreeMap, BinaryHeap, HashMap, HashSet};

pub fn proposals_to_epoch_info(
    epoch_config: &EpochConfig,
    rng_seed: RngSeed,
    prev_epoch_info: &EpochInfo,
    proposals: Vec<ValidatorStake>,
    mut validator_kickout: HashMap<AccountId, ValidatorKickoutReason>,
    validator_reward: HashMap<AccountId, Balance>,
    minted_amount: Balance,
    next_version: ProtocolVersion,
) -> Result<EpochInfo, EpochError> {
    debug_assert!(
        proposals.iter().map(|stake| stake.account_id()).collect::<HashSet<_>>().len()
            == proposals.len(),
        "Proposals should not have duplicates"
    );

    let num_shards = epoch_config.num_block_producer_seats_per_shard.len() as NumShards;
    let min_stake_ratio = {
        let rational = epoch_config.validator_selection_config.minimum_stake_ratio;
        Ratio::new(*rational.numer() as u128, *rational.denom() as u128)
    };
    let max_bp_selected = epoch_config.num_block_producer_seats as usize;
    let max_cp_selected = max_bp_selected
        + (epoch_config.validator_selection_config.num_chunk_only_producer_seats as usize);
    let minimum_validators_per_shard =
        epoch_config.validator_selection_config.minimum_validators_per_shard as usize;
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
    let mut block_producer_proposals =
        order_proposals(proposals.values().filter(|p| !p.is_chunk_only()).cloned());
    let (block_producers, bp_stake_threshold) =
        select_block_producers(&mut block_producer_proposals, max_bp_selected, min_stake_ratio);
    let mut chunk_producer_proposals = order_proposals(proposals.into_iter().map(|(_, p)| p));
    let (chunk_producers, cp_stake_threshold) = select_chunk_producers(
        &mut chunk_producer_proposals,
        max_cp_selected,
        min_stake_ratio,
        num_shards,
    );

    // since block producer proposals could become chunk producers, their actual stake threshold
    // is the smaller of the two thresholds
    let bp_stake_threshold = cmp::min(bp_stake_threshold, cp_stake_threshold);

    // process remaining chunk_producer_proposals that were not selected for either role
    for OrderedValidatorStake(p) in chunk_producer_proposals {
        let stake = p.stake();
        let account_id = p.account_id();
        if stake >= epoch_config.fishermen_threshold {
            fishermen.push(p);
        } else {
            *stake_change.get_mut(account_id).unwrap() = 0;
            if prev_epoch_info.account_is_validator(account_id)
                || prev_epoch_info.account_is_fisherman(account_id)
            {
                let threshold =
                    if p.is_chunk_only() { cp_stake_threshold } else { bp_stake_threshold };
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

    let shard_assignment =
        assign_shards(chunk_producers, num_shards as usize, minimum_validators_per_shard).map_err(
            |_| EpochError::NotEnoughValidators {
                num_validators: num_chunk_producers as u64,
                num_shards,
            },
        )?;

    let mut chunk_producers_settlement: Vec<Vec<ValidatorId>> =
        shard_assignment.iter().map(|vs| Vec::with_capacity(vs.len())).collect();
    let mut i = all_validators.len();
    for (shard_validators, shard_validator_ids) in
        shard_assignment.into_iter().zip(chunk_producers_settlement.iter_mut())
    {
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
        bp_stake_threshold,
        next_version,
        rng_seed,
    ))
}

fn proposals_with_rollover(
    proposals: Vec<ValidatorStake>,
    prev_epoch_info: &EpochInfo,
    validator_reward: &HashMap<AccountId, Balance>,
    validator_kickout: &HashMap<AccountId, ValidatorKickoutReason>,
    stake_change: &mut BTreeMap<AccountId, Balance>,
    fishermen: &mut Vec<ValidatorStake>,
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
    max_num_selected: usize,
    min_stake_ratio: Ratio<u128>,
) -> (Vec<ValidatorStake>, Balance) {
    select_validators(block_producer_proposals, max_num_selected, min_stake_ratio)
}

fn select_chunk_producers(
    all_proposals: &mut BinaryHeap<OrderedValidatorStake>,
    max_num_selected: usize,
    min_stake_ratio: Ratio<u128>,
    num_shards: u64,
) -> (Vec<ValidatorStake>, Balance) {
    select_validators(
        all_proposals,
        max_num_selected,
        min_stake_ratio * Ratio::new(1, num_shards as u128),
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
) -> (Vec<ValidatorStake>, Balance) {
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
            // p was not included, return it to the list of proposals
            proposals.push(OrderedValidatorStake(p));
            break;
        }
    }
    if validators.len() == max_number_selected {
        // all slots were filled, so the threshold stake is 1 more than the current
        // smallest stake
        let threshold = validators.last().unwrap().stake() + 1;
        (validators, threshold)
    } else {
        // the stake ratio condition prevented all slots from being filled,
        // or there were fewer proposals than available slots,
        // so the threshold stake is whatever amount pass the stake ratio condition
        let threshold = (min_stake_ratio * Ratio::new(total_stake, 1)).ceil().to_integer();
        (validators, threshold)
    }
}

#[derive(Eq, PartialEq)]
struct OrderedValidatorStake(ValidatorStake);
impl PartialOrd for OrderedValidatorStake {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let stake_order = self.0.stake().partial_cmp(&other.0.stake())?;
        match stake_order {
            Ordering::Equal => other.0.account_id().partial_cmp(self.0.account_id()),
            Ordering::Less | Ordering::Greater => Some(stake_order),
        }
    }
}
impl Ord for OrderedValidatorStake {
    fn cmp(&self, other: &Self) -> Ordering {
        let stake_order = self.0.stake().cmp(&other.0.stake());
        match stake_order {
            Ordering::Equal => other.0.account_id().cmp(self.0.account_id()),
            Ordering::Less | Ordering::Greater => stake_order,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_crypto::{KeyType, PublicKey};
    use near_primitives::epoch_manager::epoch_info::{EpochInfo, EpochInfoV3};
    use near_primitives::epoch_manager::ValidatorSelectionConfig;
    use near_primitives::shard_layout::ShardLayout;
    use near_primitives::types::validator_stake::ValidatorStake;
    use near_primitives::version::PROTOCOL_VERSION;
    use num_rational::Rational;

    #[test]
    fn test_validator_assignment_all_block_producers() {
        // A simple sanity test. Given fewer proposals than the number of seats,
        // none of which has too little stake, they all get assigned as block and
        // chunk producers.
        let epoch_config = create_epoch_config(2, 100, 0, Default::default());
        let prev_epoch_height = 7;
        let prev_epoch_info = create_prev_epoch_info(prev_epoch_height, &["test1", "test2"], &[]);
        let proposals = create_proposals(&[("test1", 1000), ("test2", 2000), ("test3", 300)]);
        let epoch_info = proposals_to_epoch_info(
            &epoch_config,
            [0; 32],
            &prev_epoch_info,
            proposals.clone(),
            Default::default(),
            Default::default(),
            0,
            PROTOCOL_VERSION,
        )
        .unwrap();

        // increment height
        assert_eq!(epoch_info.epoch_height(), prev_epoch_height + 1);

        // assign block producers in decreasing order of stake
        let mut sorted_proposals = proposals;
        sorted_proposals.sort_unstable_by(|a, b| b.stake().cmp(&a.stake()));
        assert_eq!(sorted_proposals, epoch_info.validators_iter().collect::<Vec<_>>());

        // All proposals become block producers
        assert_eq!(epoch_info.block_producers_settlement(), &[0, 1, 2]);
        assert_eq!(epoch_info.fishermen_iter().len(), 0);

        // Validators are split between chunks to make roughly equal stakes
        // (in this case shard 0 has 2000, while shard 1 has 1300).
        assert_eq!(epoch_info.chunk_producers_settlement(), &[vec![0], vec![1, 2]]);
    }

    #[test]
    fn test_validator_assignment_with_chunk_only_producers() {
        // A more complex test. Here there are more BP proposals than spots, so some will
        // become chunk-only producers, along side the other chunk-only proposals.
        let num_bp_seats = 10;
        let num_cp_seats = 30;
        let epoch_config = create_epoch_config(
            2,
            num_bp_seats,
            // purposely set the fishermen threshold high so that none become fishermen
            10_000,
            ValidatorSelectionConfig {
                num_chunk_only_producer_seats: num_cp_seats,
                minimum_validators_per_shard: 1,
                minimum_stake_ratio: Rational::new(160, 1_000_000),
            },
        );
        let prev_epoch_height = 3;
        let test1_stake = 1000;
        let prev_epoch_info = create_prev_epoch_info(
            prev_epoch_height,
            &[
                // test1 is not included in proposals below, and will get kicked out because
                // their stake is too low.
                ("test1", test1_stake, Proposal::BlockProducer),
                // test2 submitted a new proposal, so their stake will come from there, but it
                // too will be kicked out
                ("test2", 1234, Proposal::ChunkOnlyProducer),
            ],
            &[],
        );
        let proposals = create_proposals((2..(2 * num_bp_seats + num_cp_seats)).map(|i| {
            (
                format!("test{}", i),
                2000u128 + (i as u128),
                if i <= num_cp_seats {
                    Proposal::ChunkOnlyProducer
                } else {
                    Proposal::BlockProducer
                },
            )
        }));
        let epoch_info = proposals_to_epoch_info(
            &epoch_config,
            [0; 32],
            &prev_epoch_info,
            proposals.clone(),
            Default::default(),
            Default::default(),
            0,
            PROTOCOL_VERSION,
        )
        .unwrap();

        assert_eq!(epoch_info.epoch_height(), prev_epoch_height + 1);

        // no block producers are chunk-only
        epoch_info
            .block_producers_settlement()
            .into_iter()
            .map(|id| epoch_info.get_validator(*id))
            .for_each(|v| assert!(!v.is_chunk_only()));

        // the top stakes are the chosen block producers
        let mut sorted_proposals = proposals;
        sorted_proposals.sort_unstable_by(|a, b| b.stake().cmp(&a.stake()));
        assert!(sorted_proposals.iter().take(num_bp_seats as usize).cloned().eq(epoch_info
            .block_producers_settlement()
            .into_iter()
            .map(|id| epoch_info.get_validator(*id))));

        // stakes are evenly distributed between shards
        assert_eq!(
            stake_sum(&epoch_info, epoch_info.chunk_producers_settlement()[0].iter()),
            stake_sum(&epoch_info, epoch_info.chunk_producers_settlement()[1].iter()),
        );

        // The top proposals are all chunk producers
        let mut chosen_chunk_producers: Vec<ValidatorStake> = epoch_info
            .chunk_producers_settlement()
            .into_iter()
            .flatten()
            .map(|id| epoch_info.get_validator(*id))
            .collect();
        chosen_chunk_producers.sort_unstable_by(|a, b| b.stake().cmp(&a.stake()));
        assert!(sorted_proposals
            .into_iter()
            .take((num_bp_seats + num_cp_seats) as usize)
            .eq(chosen_chunk_producers));

        // the old, low-stake proposals were not accepted
        let kickout = epoch_info.validator_kickout();
        assert_eq!(kickout.len(), 2);
        assert_eq!(
            kickout.get("test1").unwrap(),
            &ValidatorKickoutReason::NotEnoughStake { stake: test1_stake, threshold: 2011 },
        );
        assert_eq!(
            kickout.get("test2").unwrap(),
            &ValidatorKickoutReason::NotEnoughStake { stake: 2002, threshold: 2011 },
        );
    }

    #[test]
    fn test_block_producer_sampling() {
        let epoch_config = create_epoch_config(
            1,
            2,
            0,
            ValidatorSelectionConfig {
                num_chunk_only_producer_seats: 0,
                minimum_validators_per_shard: 1,
                minimum_stake_ratio: Rational::new(160, 1_000_000),
            },
        );
        let prev_epoch_height = 7;
        let prev_epoch_info = create_prev_epoch_info(prev_epoch_height, &["test1", "test2"], &[]);
        let proposals = create_proposals(&[("test1", 1000), ("test2", 2000)]);

        let epoch_info = proposals_to_epoch_info(
            &epoch_config,
            [0; 32],
            &prev_epoch_info,
            proposals,
            Default::default(),
            Default::default(),
            0,
            PROTOCOL_VERSION,
        )
        .unwrap();

        // test2 is chosen as the bp 2x more often than test1
        let mut counts: [i32; 2] = [0, 0];
        for h in 0..100_000 {
            let bp = epoch_info.sample_block_producer(h);
            counts[bp as usize] += 1;
        }
        let diff = (2 * counts[1] - counts[0]).abs();
        assert!(diff < 100);
    }

    #[test]
    fn test_chunk_producer_sampling() {
        // When there is 1 CP per shard, they are chosen 100% of the time.
        let num_shards = 4;
        let epoch_config = create_epoch_config(
            num_shards,
            2 * num_shards,
            0,
            ValidatorSelectionConfig {
                num_chunk_only_producer_seats: 0,
                minimum_validators_per_shard: 1,
                minimum_stake_ratio: Rational::new(160, 1_000_000),
            },
        );
        let prev_epoch_height = 7;
        let prev_epoch_info = create_prev_epoch_info(prev_epoch_height, &["test1", "test2"], &[]);
        let proposals =
            create_proposals(&[("test1", 1000), ("test2", 1000), ("test3", 1000), ("test4", 1000)]);

        let epoch_info = proposals_to_epoch_info(
            &epoch_config,
            [0; 32],
            &prev_epoch_info,
            proposals,
            Default::default(),
            Default::default(),
            0,
            PROTOCOL_VERSION,
        )
        .unwrap();

        for shard_id in 0..num_shards {
            for h in 0..100_000 {
                let cp = epoch_info.sample_chunk_producer(h, shard_id);
                // Don't read too much into this. The reason the ValidatorId always
                // equals the ShardId is because the validators are assigned to shards in order.
                assert_eq!(cp, shard_id)
            }
        }

        // When there are multiple CPs they are chosen in proportion to stake.
        let proposals = create_proposals((1..=num_shards).flat_map(|i| {
            // Each shard gets a pair of validators, one with twice as
            // much stake as the other.
            vec![(format!("test{}", i), 1000), (format!("test{}", 100 * i), 2000)].into_iter()
        }));
        let epoch_info = proposals_to_epoch_info(
            &epoch_config,
            [0; 32],
            &prev_epoch_info,
            proposals,
            Default::default(),
            Default::default(),
            0,
            PROTOCOL_VERSION,
        )
        .unwrap();

        for shard_id in 0..num_shards {
            let mut counts: [i32; 2] = [0, 0];
            for h in 0..100_000 {
                let cp = epoch_info.sample_chunk_producer(h, shard_id);
                // if ValidatorId is in the second half then it is the lower
                // stake validator (because they are sorted by decreasing stake).
                let index = if cp >= num_shards { 1 } else { 0 };
                counts[index] += 1;
            }
            let diff = (2 * counts[1] - counts[0]).abs();
            assert!(diff < 500);
        }
    }

    #[test]
    fn test_validator_assignment_ratio_condition() {
        // There are more seats than proposals, however the
        // lower proposals are too small relative to the total
        // (the reason we can't choose them is because the the probability of them actually
        // being selected to make a block would be too low since it is done in
        // proportion to stake).
        let epoch_config = create_epoch_config(
            1,
            100,
            150,
            ValidatorSelectionConfig {
                num_chunk_only_producer_seats: 300,
                minimum_validators_per_shard: 1,
                // for example purposes, we choose a higher ratio than in production
                minimum_stake_ratio: Rational::new(1, 10),
            },
        );
        let prev_epoch_height = 7;
        // test5 and test6 are going to get kicked out for not enough stake.
        let prev_epoch_info = create_prev_epoch_info(prev_epoch_height, &["test5", "test6"], &[]);
        let proposals = create_proposals(&[
            ("test1", 1000),
            ("test2", 1000),
            ("test3", 1000), // the total up to this point is 3000
            ("test4", 200),  // 200 is < 1/10 of 3000, so not validator, but can be fisherman
            ("test5", 100),  // 100 is even too small to be a fisherman, cannot get any role
            ("test6", 50),
        ]);
        let epoch_info = proposals_to_epoch_info(
            &epoch_config,
            [0; 32],
            &prev_epoch_info,
            proposals,
            Default::default(),
            Default::default(),
            0,
            PROTOCOL_VERSION,
        )
        .unwrap();

        // stake below validator threshold, but above fishermen threshold become fishermen
        let fishermen: Vec<_> = epoch_info.fishermen_iter().map(|v| v.take_account_id()).collect();
        assert_eq!(fishermen, vec!["test4".parse().unwrap()]);

        // too low stakes are kicked out
        let kickout = epoch_info.validator_kickout();
        assert_eq!(kickout.len(), 2);
        assert_eq!(
            kickout.get("test5").unwrap(),
            &ValidatorKickoutReason::NotEnoughStake { stake: 100, threshold: 300 },
        );
        assert_eq!(
            kickout.get("test6").unwrap(),
            &ValidatorKickoutReason::NotEnoughStake { stake: 50, threshold: 300 },
        );
    }

    #[test]
    fn test_validator_assignment_with_kickout() {
        // kicked out validators are not selected
        let epoch_config = create_epoch_config(1, 100, 0, Default::default());
        let prev_epoch_height = 7;
        let prev_epoch_info = create_prev_epoch_info(
            prev_epoch_height,
            &[("test1", 10_000), ("test2", 2000), ("test3", 3000)],
            &[],
        );
        let mut kick_out = HashMap::new();
        // test1 is kicked out
        kick_out.insert("test1".parse().unwrap(), ValidatorKickoutReason::Unstaked);
        let epoch_info = proposals_to_epoch_info(
            &epoch_config,
            [0; 32],
            &prev_epoch_info,
            Default::default(),
            kick_out,
            Default::default(),
            0,
            PROTOCOL_VERSION,
        )
        .unwrap();

        // test1 is not selected
        assert_eq!(epoch_info.get_validator_id(&"test1".parse().unwrap()), None);
    }

    #[test]
    fn test_validator_assignment_with_rewards() {
        // validator balances are updated based on their rewards
        let validators = [("test1", 3000), ("test2", 2000), ("test3", 1000)];
        let rewards: [u128; 3] = [7, 8, 9];
        let epoch_config = create_epoch_config(1, 100, 0, Default::default());
        let prev_epoch_height = 7;
        let prev_epoch_info = create_prev_epoch_info(prev_epoch_height, &validators, &[]);
        let rewards_map = validators
            .iter()
            .zip(rewards.iter())
            .map(|((name, _), reward)| (name.parse().unwrap(), *reward))
            .collect();
        let epoch_info = proposals_to_epoch_info(
            &epoch_config,
            [0; 32],
            &prev_epoch_info,
            Default::default(),
            Default::default(),
            rewards_map,
            0,
            PROTOCOL_VERSION,
        )
        .unwrap();

        for (v, ((_, stake), reward)) in
            epoch_info.validators_iter().zip(validators.iter().zip(rewards.iter()))
        {
            assert_eq!(v.stake(), stake + reward);
        }
    }

    fn stake_sum<'a, I: IntoIterator<Item = &'a u64>>(
        epoch_info: &EpochInfo,
        validator_ids: I,
    ) -> u128 {
        validator_ids.into_iter().map(|id| epoch_info.get_validator(*id).stake()).sum()
    }

    /// Create EpochConfig, only filling in the fields important for validator selection.
    fn create_epoch_config(
        num_shards: u64,
        num_block_producer_seats: u64,
        fishermen_threshold: Balance,
        validator_selection_config: ValidatorSelectionConfig,
    ) -> EpochConfig {
        EpochConfig {
            epoch_length: 10,
            num_block_producer_seats,
            num_block_producer_seats_per_shard: vec![0; num_shards as usize],
            avg_hidden_validator_seats_per_shard: vec![0; num_shards as usize],
            block_producer_kickout_threshold: 0,
            chunk_producer_kickout_threshold: 0,
            online_min_threshold: 0.into(),
            online_max_threshold: 0.into(),
            fishermen_threshold,
            minimum_stake_divisor: 0,
            protocol_upgrade_stake_threshold: 0.into(),
            protocol_upgrade_num_epochs: 0,
            shard_layout: ShardLayout::default(),
            validator_selection_config,
        }
    }

    fn create_prev_epoch_info<T: IntoValidatorStake + Copy>(
        epoch_height: u64,
        prev_validators: &[T],
        prev_fishermen: &[T],
    ) -> EpochInfo {
        let mut result: EpochInfoV3 = Default::default();

        result.epoch_height = epoch_height;
        result.validators = create_proposals(prev_validators);
        result.fishermen = create_proposals(prev_fishermen);

        result.validator_to_index = to_map(&result.validators);
        result.fishermen_to_index = to_map(&result.fishermen);

        EpochInfo::V3(result)
    }

    fn to_map(vs: &[ValidatorStake]) -> HashMap<AccountId, ValidatorId> {
        vs.iter().enumerate().map(|(i, v)| (v.account_id().clone(), i as u64)).collect()
    }

    fn create_proposals<I, T>(ps: I) -> Vec<ValidatorStake>
    where
        T: IntoValidatorStake,
        I: IntoIterator<Item = T>,
    {
        ps.into_iter().map(IntoValidatorStake::into_validator_stake).collect()
    }

    #[derive(Debug, PartialEq, Eq, Copy, Clone)]
    enum Proposal {
        BlockProducer,
        ChunkOnlyProducer,
    }

    trait IntoValidatorStake {
        fn into_validator_stake(self) -> ValidatorStake;
    }

    impl IntoValidatorStake for &str {
        fn into_validator_stake(self) -> ValidatorStake {
            ValidatorStake::new(
                self.parse().unwrap(),
                PublicKey::empty(KeyType::ED25519),
                100,
                false,
            )
        }
    }

    impl IntoValidatorStake for (&str, Balance) {
        fn into_validator_stake(self) -> ValidatorStake {
            ValidatorStake::new(
                self.0.parse().unwrap(),
                PublicKey::empty(KeyType::ED25519),
                self.1,
                false,
            )
        }
    }

    impl IntoValidatorStake for (String, Balance) {
        fn into_validator_stake(self) -> ValidatorStake {
            ValidatorStake::new(
                self.0.parse().unwrap(),
                PublicKey::empty(KeyType::ED25519),
                self.1,
                false,
            )
        }
    }

    impl IntoValidatorStake for (&str, Balance, Proposal) {
        fn into_validator_stake(self) -> ValidatorStake {
            let is_chunk_only = match self.2 {
                Proposal::BlockProducer => false,
                Proposal::ChunkOnlyProducer => true,
            };
            ValidatorStake::new(
                self.0.parse().unwrap(),
                PublicKey::empty(KeyType::ED25519),
                self.1,
                is_chunk_only,
            )
        }
    }

    impl IntoValidatorStake for (String, Balance, Proposal) {
        fn into_validator_stake(self) -> ValidatorStake {
            let is_chunk_only = match self.2 {
                Proposal::BlockProducer => false,
                Proposal::ChunkOnlyProducer => true,
            };
            ValidatorStake::new(
                self.0.parse().unwrap(),
                PublicKey::empty(KeyType::ED25519),
                self.1,
                is_chunk_only,
            )
        }
    }

    impl<T: IntoValidatorStake + Copy> IntoValidatorStake for &T {
        fn into_validator_stake(self) -> ValidatorStake {
            (*self).into_validator_stake()
        }
    }
}

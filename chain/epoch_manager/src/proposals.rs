use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::iter;

use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::epoch_manager::EpochConfig;
use near_primitives::errors::EpochError;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{AccountId, Balance, NumSeats, ValidatorId, ValidatorKickoutReason};
use near_primitives::version::ProtocolVersion;

use crate::types::RngSeed;

/// Find threshold of stake per seat, given provided stakes and required number of seats.
pub(crate) fn find_threshold(
    stakes: &[Balance],
    num_seats: NumSeats,
) -> Result<Balance, EpochError> {
    let stake_sum: Balance = stakes.iter().sum();
    if stake_sum < num_seats.into() {
        return Err(EpochError::ThresholdError { stake_sum, num_seats });
    }
    let (mut left, mut right): (Balance, Balance) = (1, stake_sum + 1);
    'outer: loop {
        if left == right - 1 {
            break Ok(left);
        }
        let mid = (left + right) / 2;
        let mut current_sum: Balance = 0;
        for item in stakes.iter() {
            current_sum += item / mid;
            if current_sum >= u128::from(num_seats) {
                left = mid;
                continue 'outer;
            }
        }
        right = mid;
    }
}

/// Calculates new seat assignments based on current seat assignments and proposals.
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
    // Combine proposals with rollovers.
    let mut ordered_proposals = BTreeMap::new();
    // Account -> new_stake
    let mut stake_change = BTreeMap::new();
    let mut fishermen = vec![];
    debug_assert!(
        proposals.iter().map(|stake| stake.account_id()).collect::<HashSet<_>>().len()
            == proposals.len(),
        "Proposals should not have duplicates"
    );

    for p in proposals {
        let account_id = p.account_id();
        if validator_kickout.contains_key(account_id) {
            let account_id = p.take_account_id();
            stake_change.insert(account_id, 0);
        } else {
            stake_change.insert(account_id.clone(), p.stake());
            ordered_proposals.insert(account_id.clone(), p);
        }
    }
    for r in prev_epoch_info.validators_iter() {
        let account_id = r.account_id().clone();
        if validator_kickout.contains_key(&account_id) {
            stake_change.insert(account_id, 0);
            continue;
        }
        let p = ordered_proposals.entry(account_id.clone()).or_insert(r);
        *p.stake_mut() += *validator_reward.get(&account_id).unwrap_or(&0);
        stake_change.insert(account_id, p.stake());
    }

    for r in prev_epoch_info.fishermen_iter() {
        let account_id = r.account_id();
        if validator_kickout.contains_key(account_id) {
            stake_change.insert(account_id.clone(), 0);
            continue;
        }
        if !ordered_proposals.contains_key(account_id) {
            // safe to do this here because fishermen from previous epoch is guaranteed to have no
            // duplicates.
            stake_change.insert(account_id.clone(), r.stake());
            fishermen.push(r);
        }
    }

    // Get the threshold given current number of seats and stakes.
    let num_hidden_validator_seats: NumSeats =
        epoch_config.avg_hidden_validator_seats_per_shard.iter().sum();
    let num_total_seats = epoch_config.num_block_producer_seats + num_hidden_validator_seats;
    let stakes = ordered_proposals.iter().map(|(_, p)| p.stake()).collect::<Vec<_>>();
    let threshold = find_threshold(&stakes, num_total_seats)?;
    // Remove proposals under threshold.
    let mut final_proposals = vec![];

    for (account_id, p) in ordered_proposals {
        let stake = p.stake();
        if stake >= threshold {
            final_proposals.push(p);
        } else if stake >= epoch_config.fishermen_threshold {
            // Do not return stake back since they will become fishermen
            fishermen.push(p);
        } else {
            *stake_change.get_mut(&account_id).unwrap() = 0;
            if prev_epoch_info.account_is_validator(&account_id)
                || prev_epoch_info.account_is_fisherman(&account_id)
            {
                validator_kickout.insert(
                    account_id,
                    ValidatorKickoutReason::NotEnoughStake { stake, threshold },
                );
            }
        }
    }

    // Duplicate each proposal for number of seats it has.
    let mut dup_proposals = final_proposals
        .iter()
        .enumerate()
        .flat_map(|(i, p)| iter::repeat(i as u64).take((p.stake() / threshold) as usize))
        .collect::<Vec<_>>();

    assert!(dup_proposals.len() >= num_total_seats as usize, "bug in find_threshold");
    {
        use protocol_defining_rand::seq::SliceRandom;
        use protocol_defining_rand::{rngs::StdRng, SeedableRng};
        // Shuffle duplicate proposals.
        let mut rng: StdRng = SeedableRng::from_seed(rng_seed);
        dup_proposals.shuffle(&mut rng);
    }

    // Block producers are first `num_block_producer_seats` proposals.
    let mut block_producers_settlement =
        dup_proposals[..epoch_config.num_block_producer_seats as usize].to_vec();
    // remove proposals that are not selected
    let indices_to_keep = block_producers_settlement.iter().copied().collect::<BTreeSet<_>>();
    let (final_proposals, proposals_to_remove) = final_proposals.into_iter().enumerate().fold(
        (vec![], vec![]),
        |(mut proposals, mut to_remove), (i, p)| {
            if indices_to_keep.contains(&(i as u64)) {
                proposals.push(p);
            } else {
                to_remove.push(p);
            }
            (proposals, to_remove)
        },
    );
    for p in proposals_to_remove {
        debug_assert!(p.stake() >= threshold);
        if p.stake() >= epoch_config.fishermen_threshold {
            fishermen.push(p);
        } else {
            let account_id = p.take_account_id();
            stake_change.insert(account_id.clone(), 0);
            if prev_epoch_info.account_is_validator(&account_id)
                || prev_epoch_info.account_is_fisherman(&account_id)
            {
                validator_kickout.insert(account_id, ValidatorKickoutReason::DidNotGetASeat);
            }
        }
    }

    // reset indices
    for index in block_producers_settlement.iter_mut() {
        *index = indices_to_keep.range(..*index).count() as u64;
    }

    // Collect proposals into block producer assignments.
    let mut chunk_producers_settlement: Vec<Vec<ValidatorId>> = vec![];
    let mut last_index: u64 = 0;
    for num_seats_in_shard in epoch_config.num_block_producer_seats_per_shard.iter() {
        let mut shard_settlement: Vec<ValidatorId> = vec![];
        for _ in 0..*num_seats_in_shard {
            let proposal_index = block_producers_settlement[last_index as usize];
            shard_settlement.push(proposal_index);
            last_index = (last_index + 1) % epoch_config.num_block_producer_seats;
        }
        chunk_producers_settlement.push(shard_settlement);
    }

    let fishermen_to_index = fishermen
        .iter()
        .enumerate()
        .map(|(index, s)| (s.account_id().clone(), index as ValidatorId))
        .collect::<HashMap<_, _>>();

    let validator_to_index = final_proposals
        .iter()
        .enumerate()
        .map(|(index, s)| (s.account_id().clone(), index as ValidatorId))
        .collect::<HashMap<_, _>>();

    Ok(EpochInfo::new(
        prev_epoch_info.epoch_height() + 1,
        final_proposals,
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

#[cfg(test)]
mod tests {
    use num_rational::Rational;

    use near_primitives::version::PROTOCOL_VERSION;

    use crate::test_utils::{
        change_stake, epoch_config, epoch_info, epoch_info_with_num_seats, stake,
    };

    use super::*;

    #[test]
    fn test_find_threshold() {
        assert_eq!(find_threshold(&[1_000_000, 1_000_000, 10], 10).unwrap(), 200_000);
        assert_eq!(find_threshold(&[1_000_000_000, 10], 10).unwrap(), 100_000_000);
        assert_eq!(find_threshold(&[1_000_000_000], 1_000_000_000).unwrap(), 1);
        assert_eq!(find_threshold(&[1_000, 1, 1, 1, 1, 1, 1, 1, 1, 1], 1).unwrap(), 1_000);
        assert!(find_threshold(&[1, 1, 2], 100).is_err());
    }

    #[test]
    fn test_proposals_to_assignments() {
        assert_eq!(
            proposals_to_epoch_info(
                &epoch_config(2, 2, 1, 1, 90, 60, 0),
                [0; 32],
                &EpochInfo::default(),
                vec![stake("test1", 1_000_000)],
                HashMap::default(),
                HashMap::default(),
                0,
                PROTOCOL_VERSION,
            )
            .unwrap(),
            epoch_info_with_num_seats(
                1,
                vec![("test1", 1_000_000)],
                vec![0],
                vec![vec![0], vec![0]],
                vec![],
                vec![],
                change_stake(vec![("test1", 1_000_000)]),
                vec![],
                HashMap::default(),
                0,
                3
            )
        );
        assert_eq!(
            proposals_to_epoch_info(
                &EpochConfig {
                    epoch_length: 2,
                    num_shards: 5,
                    num_block_producer_seats: 6,
                    num_block_producer_seats_per_shard: vec![6, 2, 2, 2, 2],
                    avg_hidden_validator_seats_per_shard: vec![6, 2, 2, 2, 2],
                    block_producer_kickout_threshold: 90,
                    chunk_producer_kickout_threshold: 60,
                    fishermen_threshold: 10,
                    online_min_threshold: Rational::new(90, 100),
                    online_max_threshold: Rational::new(99, 100),
                    minimum_stake_divisor: 1,
                    protocol_upgrade_stake_threshold: Rational::new(80, 100),
                    protocol_upgrade_num_epochs: 2,
                },
                [0; 32],
                &EpochInfo::default(),
                vec![
                    stake("test1", 1_000_000),
                    stake("test2", 1_000_000),
                    stake("test3", 1_000_000),
                    stake("test4", 100),
                ],
                HashMap::default(),
                HashMap::default(),
                0,
                PROTOCOL_VERSION
            )
            .unwrap(),
            epoch_info_with_num_seats(
                1,
                vec![("test1", 1_000_000), ("test2", 1_000_000), ("test3", 1_000_000)],
                vec![0, 1, 0, 0, 1, 2],
                vec![
                    // Shard 0 is block produced / validated by all block producers & fisherman.
                    vec![0, 1, 0, 0, 1, 2],
                    vec![0, 1],
                    vec![0, 0],
                    vec![1, 2],
                    vec![0, 1]
                ],
                vec![],
                vec![("test4", 100)],
                change_stake(vec![
                    ("test1", 1_000_000),
                    ("test2", 1_000_000),
                    ("test3", 1_000_000),
                    ("test4", 100),
                ]),
                vec![],
                HashMap::default(),
                0,
                20
            )
        );
    }

    #[test]
    fn test_fishermen_allocation() {
        // 4 proposals of stake 10, fishermen threshold 10 --> 1 validator and 3 fishermen
        assert_eq!(
            proposals_to_epoch_info(
                &epoch_config(2, 2, 1, 0, 90, 60, 10),
                [0; 32],
                &EpochInfo::default(),
                vec![
                    stake("test1", 10),
                    stake("test2", 10),
                    stake("test3", 10),
                    stake("test4", 10)
                ],
                HashMap::default(),
                HashMap::default(),
                0,
                PROTOCOL_VERSION
            )
            .unwrap(),
            epoch_info(
                1,
                vec![("test1", 10)],
                vec![0],
                vec![vec![0], vec![0]],
                vec![],
                vec![("test2", 10), ("test3", 10), ("test4", 10)],
                change_stake(vec![("test1", 10), ("test2", 10), ("test3", 10), ("test4", 10)]),
                vec![],
                HashMap::default(),
                0
            )
        );

        // 4 proposals of stake 9, fishermen threshold 10 --> 1 validator and 0 fishermen
        let mut epoch_info = epoch_info(
            1,
            vec![("test1", 9)],
            vec![0],
            vec![vec![0], vec![0]],
            vec![],
            vec![],
            change_stake(vec![("test1", 9), ("test2", 0), ("test3", 0), ("test4", 0)]),
            vec![],
            HashMap::default(),
            0,
        );
        #[cfg(feature = "protocol_feature_block_header_v3")]
        match &mut epoch_info {
            EpochInfo::V1(info) => info.validator_kickout = HashMap::default(),
            EpochInfo::V2(info) => info.validator_kickout = HashMap::default(),
        }
        #[cfg(not(feature = "protocol_feature_block_header_v3"))]
        {
            epoch_info.validator_kickout = HashMap::default();
        }
        assert_eq!(
            proposals_to_epoch_info(
                &epoch_config(2, 2, 1, 0, 90, 60, 10),
                [0; 32],
                &EpochInfo::default(),
                vec![stake("test1", 9), stake("test2", 9), stake("test3", 9), stake("test4", 9)],
                HashMap::default(),
                HashMap::default(),
                0,
                PROTOCOL_VERSION
            )
            .unwrap(),
            epoch_info
        );
    }
}

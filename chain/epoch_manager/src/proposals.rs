use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::iter;

use near_primitives::types::{AccountId, Balance, NumSeats, ValidatorId, ValidatorStake};

use crate::types::{EpochConfig, EpochError, EpochInfo, RngSeed};

/// Find threshold of stake per seat, given provided stakes and required number of seats.
fn find_threshold(stakes: &[Balance], num_seats: NumSeats) -> Result<Balance, EpochError> {
    let stakes_sum: Balance = stakes.iter().sum();
    if stakes_sum < num_seats.into() {
        return Err(EpochError::ThresholdError(stakes_sum, num_seats));
    }
    let (mut left, mut right): (Balance, Balance) = (1, stakes_sum + 1);
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
    epoch_info: &EpochInfo,
    proposals: Vec<ValidatorStake>,
    mut validator_kickout: HashSet<AccountId>,
    validator_reward: HashMap<AccountId, Balance>,
    inflation: Balance,
) -> Result<EpochInfo, EpochError> {
    // Combine proposals with rollovers.
    let mut ordered_proposals = BTreeMap::new();
    // Account -> new_stake
    let mut stake_change = BTreeMap::new();
    let mut fishermen = vec![];
    debug_assert!(
        proposals.iter().map(|stake| &stake.account_id).collect::<HashSet<_>>().len()
            == proposals.len(),
        "Proposals should not have duplicates"
    );

    for p in proposals {
        if validator_kickout.contains(&p.account_id) {
            stake_change.insert(p.account_id, 0);
        } else {
            stake_change.insert(p.account_id.clone(), p.stake);
            ordered_proposals.insert(p.account_id.clone(), p);
        }
    }
    for r in epoch_info.validators.iter() {
        if validator_kickout.contains(&r.account_id) {
            stake_change.insert(r.account_id.clone(), 0);
            continue;
        }
        let p = ordered_proposals.entry(r.account_id.clone()).or_insert_with(|| r.clone());
        p.stake += *validator_reward.get(&p.account_id).unwrap_or(&0);
        stake_change.insert(p.account_id.clone(), p.stake);
    }

    for r in epoch_info.fishermen.iter() {
        if !ordered_proposals.contains_key(&r.account_id)
            && !validator_kickout.contains(&r.account_id)
        {
            // safe to do this here because fishermen from previous epoch is guaranteed to have no
            // duplicates.
            fishermen.push(r.clone());
            stake_change.insert(r.account_id.clone(), r.stake);
        }
    }

    // Get the threshold given current number of seats and stakes.
    let num_hidden_validator_seats: NumSeats =
        epoch_config.avg_hidden_validator_seats_per_shard.iter().sum();
    let num_total_seats = epoch_config.num_block_producer_seats + num_hidden_validator_seats;
    let stakes = ordered_proposals.iter().map(|(_, p)| p.stake).collect::<Vec<_>>();
    let threshold = find_threshold(&stakes, num_total_seats)?;
    // Remove proposals under threshold.
    let mut final_proposals = vec![];

    for (account_id, p) in ordered_proposals {
        if p.stake >= threshold {
            final_proposals.push(p);
        } else if p.stake >= epoch_config.fishermen_threshold {
            // Do not return stake back since they will become fishermen
            fishermen.push(p);
        } else {
            *stake_change.get_mut(&account_id).unwrap() = 0;
            if epoch_info.validator_to_index.contains_key(&account_id)
                || epoch_info.fishermen_to_index.contains_key(&account_id)
            {
                validator_kickout.insert(account_id);
            }
        }
    }

    // Duplicate each proposal for number of seats it has.
    let mut dup_proposals = final_proposals
        .iter()
        .enumerate()
        .flat_map(|(i, p)| iter::repeat(i as u64).take((p.stake / threshold) as usize))
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
        debug_assert!(p.stake >= threshold);
        if p.stake >= epoch_config.fishermen_threshold {
            fishermen.push(p);
        } else {
            stake_change.insert(p.account_id.clone(), 0);
            if epoch_info.validator_to_index.contains_key(&p.account_id)
                || epoch_info.fishermen_to_index.contains_key(&p.account_id)
            {
                validator_kickout.insert(p.account_id);
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
        .map(|(index, s)| (s.account_id.clone(), index as ValidatorId))
        .collect::<HashMap<_, _>>();

    let validator_to_index = final_proposals
        .iter()
        .enumerate()
        .map(|(index, s)| (s.account_id.clone(), index as ValidatorId))
        .collect::<HashMap<_, _>>();

    Ok(EpochInfo {
        epoch_height: epoch_info.epoch_height + 1,
        validators: final_proposals,
        fishermen,
        validator_to_index,
        block_producers_settlement,
        chunk_producers_settlement,
        hidden_validators_settlement: vec![],
        stake_change,
        validator_reward,
        inflation,
        validator_kickout,
        fishermen_to_index,
    })
}

#[cfg(test)]
mod tests {
    use crate::test_utils::{change_stake, epoch_config, epoch_info, stake};

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
                HashSet::default(),
                HashMap::default(),
                0
            )
            .unwrap(),
            epoch_info(
                1,
                vec![("test1", 1_000_000)],
                vec![0],
                vec![vec![0], vec![0]],
                vec![],
                vec![],
                change_stake(vec![("test1", 1_000_000)]),
                HashMap::default(),
                0
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
                    fishermen_threshold: 10
                },
                [0; 32],
                &EpochInfo::default(),
                vec![
                    stake("test1", 1_000_000),
                    stake("test2", 1_000_000),
                    stake("test3", 1_000_000),
                    stake("test4", 100),
                ],
                HashSet::default(),
                HashMap::default(),
                0
            )
            .unwrap(),
            epoch_info(
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
                HashMap::default(),
                0
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
                HashSet::default(),
                HashMap::default(),
                0
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
            HashMap::default(),
            0,
        );
        epoch_info.validator_kickout = HashSet::default();
        assert_eq!(
            proposals_to_epoch_info(
                &epoch_config(2, 2, 1, 0, 90, 60, 10),
                [0; 32],
                &EpochInfo::default(),
                vec![stake("test1", 9), stake("test2", 9), stake("test3", 9), stake("test4", 9)],
                HashSet::default(),
                HashMap::default(),
                0
            )
            .unwrap(),
            epoch_info
        );
    }
}

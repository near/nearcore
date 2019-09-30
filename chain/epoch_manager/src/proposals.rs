use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::iter;

use rand::seq::SliceRandom;
use rand::{rngs::StdRng, SeedableRng};

use near_primitives::types::{AccountId, Balance, Gas, ValidatorId, ValidatorStake};

use crate::types::{EpochConfig, EpochError, EpochInfo, RngSeed};

/// Find threshold of stake per seat, given provided stakes and required number of seats.
fn find_threshold(stakes: &[Balance], num_seats: u64) -> Result<Balance, EpochError> {
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
    validator_kickout: HashSet<AccountId>,
    validator_reward: HashMap<AccountId, Balance>,
    total_gas_used: Gas,
    inflation: Balance,
) -> Result<EpochInfo, EpochError> {
    // Combine proposals with rollovers.
    let mut ordered_proposals = BTreeMap::new();
    let mut stake_change = BTreeMap::new();
    for p in proposals {
        if validator_kickout.contains(&p.account_id) {
            stake_change.insert(p.account_id, (0, p.amount));
        } else {
            ordered_proposals.insert(p.account_id.clone(), p);
        }
    }
    for r in epoch_info.validators.iter() {
        match ordered_proposals.entry(r.account_id.clone()) {
            Entry::Occupied(e) => {
                let p = &*e.get();
                let return_stake = if r.amount > p.amount { r.amount - p.amount } else { 0 };
                stake_change.insert(r.account_id.clone(), (p.amount, return_stake));
            }
            Entry::Vacant(e) => {
                if !validator_kickout.contains(&r.account_id) {
                    let mut proposal = r.clone();
                    proposal.amount += *validator_reward.get(&r.account_id).unwrap_or(&0);
                    e.insert(proposal);
                } else {
                    stake_change.insert(r.account_id.clone(), (0, r.amount));
                }
            }
        }
    }

    // Get the threshold given current number of seats and stakes.
    let num_fisherman_seats: usize = epoch_config.avg_fisherman_per_shard.iter().sum();
    let num_seats = epoch_config.num_block_producers + num_fisherman_seats;
    let stakes = ordered_proposals.iter().map(|(_, p)| p.amount).collect::<Vec<_>>();
    let threshold = find_threshold(&stakes, num_seats as u64)?;
    // Remove proposals under threshold.
    let mut final_proposals = vec![];
    for (account_id, p) in ordered_proposals {
        if p.amount >= threshold {
            if !stake_change.contains_key(&account_id) {
                stake_change.insert(account_id, (p.amount, 0));
            }
            final_proposals.push(p);
        } else {
            stake_change
                .entry(account_id)
                .and_modify(|(new_stake, return_stake)| {
                    if *new_stake != 0 {
                        *return_stake += *new_stake;
                        *new_stake = 0;
                    }
                })
                .or_insert((0, p.amount));
        }
    }

    let (final_proposals, validator_to_index) = final_proposals.into_iter().enumerate().fold(
        (vec![], HashMap::new()),
        |(mut proposals, mut validator_to_index), (i, p)| {
            validator_to_index.insert(p.account_id.clone(), i);
            proposals.push(p);
            (proposals, validator_to_index)
        },
    );

    // Duplicate each proposal for number of seats it has.
    let mut dup_proposals = final_proposals
        .iter()
        .enumerate()
        .flat_map(|(i, p)| iter::repeat(i).take((p.amount / threshold) as usize))
        .collect::<Vec<_>>();
    if dup_proposals.len() < num_seats as usize {
        return Err(EpochError::SelectedSeatsMismatch(dup_proposals.len() as u64, num_seats));
    }

    // Shuffle duplicate proposals.
    let mut rng: StdRng = SeedableRng::from_seed(rng_seed);
    dup_proposals.shuffle(&mut rng);

    // Block producers are first `num_block_producers` proposals.
    let block_producers = dup_proposals[..epoch_config.num_block_producers].to_vec();

    // Collect proposals into block producer assignments.
    let mut chunk_producers: Vec<Vec<ValidatorId>> = vec![];
    let mut last_index: usize = 0;
    for num_seats in epoch_config.block_producers_per_shard.iter() {
        let mut cp: Vec<ValidatorId> = vec![];
        for i in 0..*num_seats {
            let proposal_index = dup_proposals[(i + last_index) % epoch_config.num_block_producers];
            cp.push(proposal_index);
        }
        chunk_producers.push(cp);
        last_index = (last_index + num_seats) % epoch_config.num_block_producers;
    }

    // TODO(1050): implement fishermen allocation.

    let final_stake_change = stake_change.into_iter().map(|(k, (v, _))| (k, v)).collect();

    Ok(EpochInfo {
        validators: final_proposals,
        validator_to_index,
        block_producers,
        chunk_producers,
        fishermen: vec![],
        stake_change: final_stake_change,
        total_gas_used,
        validator_reward,
        inflation,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{change_stake, epoch_config, epoch_info, stake};

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
                &epoch_config(2, 2, 1, 1, 90),
                [0; 32],
                &EpochInfo::default(),
                vec![stake("test1", 1_000_000)],
                HashSet::new(),
                HashMap::default(),
                0,
                0
            )
            .unwrap(),
            epoch_info(
                vec![("test1", 1_000_000)],
                vec![0],
                vec![vec![0], vec![0]],
                vec![],
                change_stake(vec![("test1", 1_000_000)]),
                0,
                HashMap::default(),
                0
            )
        );
        assert_eq!(
            proposals_to_epoch_info(
                &EpochConfig {
                    epoch_length: 2,
                    num_shards: 5,
                    num_block_producers: 6,
                    block_producers_per_shard: vec![6, 2, 2, 2, 2],
                    avg_fisherman_per_shard: vec![6, 2, 2, 2, 2],
                    validator_kickout_threshold: 90,
                },
                [0; 32],
                &EpochInfo::default(),
                vec![
                    stake("test1", 1_000_000),
                    stake("test2", 1_000_000),
                    stake("test3", 1_000_000)
                ],
                HashSet::new(),
                HashMap::default(),
                0,
                0
            )
            .unwrap(),
            epoch_info(
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
                change_stake(vec![
                    ("test1", 1_000_000),
                    ("test2", 1_000_000),
                    ("test3", 1_000_000)
                ]),
                0,
                HashMap::default(),
                0
            )
        );
    }

}

use std::collections::HashMap;

use near_primitives::checked_feature;
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::epoch_manager::{EpochConfig, RngSeed};
use near_primitives::errors::EpochError;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, Balance, NumSeats, ProtocolVersion, ValidatorKickoutReason,
};

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
    validator_kickout: HashMap<AccountId, ValidatorKickoutReason>,
    validator_reward: HashMap<AccountId, Balance>,
    minted_amount: Balance,
    next_version: ProtocolVersion,
    last_epoch_version: ProtocolVersion,
) -> Result<EpochInfo, EpochError> {
    if checked_feature!("stable", AliasValidatorSelectionAlgorithm, last_epoch_version) {
        return crate::validator_selection::proposals_to_epoch_info(
            epoch_config,
            rng_seed,
            prev_epoch_info,
            proposals,
            validator_kickout,
            validator_reward,
            minted_amount,
            next_version,
            last_epoch_version,
        );
    } else {
        return old_validator_selection::proposals_to_epoch_info(
            epoch_config,
            rng_seed,
            prev_epoch_info,
            proposals,
            validator_kickout,
            validator_reward,
            minted_amount,
            next_version,
        );
    }
}

mod old_validator_selection {
    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
    use std::iter;

    use near_primitives::epoch_manager::epoch_info::EpochInfo;
    use near_primitives::epoch_manager::EpochConfig;
    use near_primitives::errors::EpochError;
    use near_primitives::types::validator_stake::ValidatorStake;
    use near_primitives::types::{
        AccountId, Balance, NumSeats, ValidatorId, ValidatorKickoutReason,
    };
    use near_primitives::version::ProtocolVersion;
    use rand::seq::SliceRandom;
    use rand::SeedableRng;
    use rand_hc::Hc128Rng;

    use crate::proposals::find_threshold;
    use crate::types::RngSeed;

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
            // Shuffle duplicate proposals.
            let mut rng: Hc128Rng = SeedableRng::from_seed(rng_seed);
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
            rng_seed,
        ))
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use rand_hc::Hc128Rng;

    #[test]
    pub fn proposal_randomness_reproducibility() {
        // Sanity check that the Hc128Rng implementation does not change (it should not).
        // This is important to ensure that receipt ordering stays the same.
        let mut rng: Hc128Rng = rand::SeedableRng::seed_from_u64(123456);
        assert_eq!(rng.gen::<u32>(), 3090581895);
    }
}

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
    use rand::{RngCore, SeedableRng};
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
        shuffle_duplicate_proposals(&mut dup_proposals, rng_seed);

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

    fn shuffle_duplicate_proposals(dup_proposals: &mut Vec<u64>, rng_seed: RngSeed) {
        let mut rng: Hc128Rng = SeedableRng::from_seed(rng_seed);
        for i in (1..dup_proposals.len()).rev() {
            dup_proposals.swap(i, gen_index_old(&mut rng, (i + 1) as u64) as usize);
        }
    }

    fn gen_index_old(rng: &mut Hc128Rng, bound: u64) -> u64 {
        // This is a simplified copy of the rand gen_index implementation to ensure that
        // upgrades to the rand library will not cause a change in the shuffling behavior.
        let zone = (bound << bound.leading_zeros()).wrapping_sub(1);
        loop {
            let v = rng.next_u64();
            let mul = (v as u128) * (bound as u128);
            let (hi, lo) = ((mul >> 64) as u64, mul as u64);
            if lo < zone {
                return hi;
            }
        }
    }
    #[cfg(test)]
    mod tests {
        use near_primitives::hash::CryptoHash;

        use crate::proposals::old_validator_selection::shuffle_duplicate_proposals;

        #[test]
        pub fn proposal_shuffling_sanity_checks() {
            // Since we made our own impl for shuffling, do some sanity checks.
            for i in 0..10 {
                let mut dup_proposals = (0..i).collect::<Vec<_>>();
                shuffle_duplicate_proposals(
                    &mut dup_proposals,
                    *CryptoHash::hash_bytes(&[1, 2, 3, 4, 5]).as_bytes(),
                );
                assert_eq!(dup_proposals.len(), i as usize);
                dup_proposals.sort();
                assert_eq!(dup_proposals, (0..i).collect::<Vec<_>>());
            }
        }

        #[test]
        pub fn proposal_randomness_reproducibility() {
            // Sanity check that the proposal shuffling implementation does not change.
            let mut dup_proposals = (0..100).collect::<Vec<u64>>();
            shuffle_duplicate_proposals(
                &mut dup_proposals,
                *CryptoHash::hash_bytes(&[1, 2, 3, 4, 5]).as_bytes(),
            );
            assert_eq!(
                dup_proposals,
                vec![
                    28, 64, 35, 39, 5, 19, 91, 93, 32, 55, 49, 86, 7, 34, 58, 48, 65, 11, 0, 3, 63,
                    85, 96, 12, 23, 76, 29, 69, 31, 45, 1, 15, 33, 61, 38, 74, 87, 10, 62, 9, 40,
                    56, 98, 8, 52, 75, 99, 13, 57, 44, 6, 79, 89, 84, 68, 36, 94, 53, 80, 70, 42,
                    88, 73, 2, 72, 25, 20, 67, 37, 97, 41, 71, 47, 59, 24, 66, 54, 21, 18, 26, 60,
                    92, 50, 77, 81, 14, 43, 17, 90, 95, 78, 16, 30, 46, 22, 83, 27, 4, 51, 82
                ]
            );
        }
    }
}

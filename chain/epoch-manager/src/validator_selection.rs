use crate::shard_assignment::assign_chunk_producers_to_shards;
use near_primitives::checked_feature;
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::epoch_manager::{EpochConfig, RngSeed};
use near_primitives::errors::EpochError;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, Balance, NumShards, ProtocolVersion, ValidatorId, ValidatorKickoutReason,
};
use near_primitives::validator_mandates::{ValidatorMandates, ValidatorMandatesConfig};
use num_rational::Ratio;
use rand::seq::SliceRandom;
use std::cmp::{self, Ordering};
use std::collections::hash_map;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};

/// Helper struct which is a result of proposals processing.
struct ValidatorRoles {
    /// Proposals which were not given any role.
    unselected_proposals: BinaryHeap<OrderedValidatorStake>,
    /// Validators which are assigned to produce chunks.
    chunk_producers: Vec<ValidatorStake>,
    /// Validators which are assigned to produce blocks.
    block_producers: Vec<ValidatorStake>,
    /// Validators which are assigned to validate chunks.
    chunk_validators: Vec<ValidatorStake>,
    /// Stake threshold to become a validator.
    threshold: Balance,
}

/// Helper struct which is a result of assigning chunk producers to shards.
struct ChunkProducersAssignment {
    /// List of all validators in the epoch.
    /// Note that it doesn't belong here, but in the legacy code it is computed
    /// together with chunk producers assignment.
    all_validators: Vec<ValidatorStake>,
    /// Maps validator account names to local indices throughout the epoch.
    validator_to_index: HashMap<AccountId, ValidatorId>,
    /// Maps chunk producers to shards, where i-th list contains local indices
    /// of validators producing chunks for i-th shard.
    chunk_producers_settlement: Vec<Vec<ValidatorId>>,
}

/// Selects validator roles for the given proposals.
fn select_validators_from_proposals(
    epoch_config: &EpochConfig,
    proposals: HashMap<AccountId, ValidatorStake>,
    protocol_version: ProtocolVersion,
) -> ValidatorRoles {
    let shard_ids: Vec<_> = epoch_config.shard_layout.shard_ids().collect();
    let min_stake_ratio = {
        let rational = epoch_config.validator_selection_config.minimum_stake_ratio;
        Ratio::new(*rational.numer() as u128, *rational.denom() as u128)
    };

    let chunk_producer_proposals = order_proposals(proposals.values().cloned());
    let (chunk_producers, _, cp_stake_threshold) = select_chunk_producers(
        chunk_producer_proposals,
        epoch_config.validator_selection_config.num_chunk_producer_seats as usize,
        min_stake_ratio,
        shard_ids.len() as NumShards,
        protocol_version,
    );

    let block_producer_proposals = order_proposals(proposals.values().cloned());
    let (block_producers, _, bp_stake_threshold) = select_block_producers(
        block_producer_proposals,
        epoch_config.num_block_producer_seats as usize,
        min_stake_ratio,
        protocol_version,
    );

    let chunk_validator_proposals = order_proposals(proposals.values().cloned());
    let (chunk_validators, _, cv_stake_threshold) = select_validators(
        chunk_validator_proposals,
        epoch_config.validator_selection_config.num_chunk_validator_seats as usize,
        min_stake_ratio,
        protocol_version,
    );

    let mut unselected_proposals = BinaryHeap::new();
    for proposal in order_proposals(proposals.into_values()) {
        if chunk_producers.contains(&proposal.0) {
            continue;
        }
        if block_producers.contains(&proposal.0) {
            continue;
        }
        if chunk_validators.contains(&proposal.0) {
            continue;
        }
        unselected_proposals.push(proposal);
    }
    let threshold = bp_stake_threshold.min(cp_stake_threshold).min(cv_stake_threshold);
    ValidatorRoles {
        unselected_proposals,
        chunk_producers,
        block_producers,
        chunk_validators,
        threshold,
    }
}

fn get_chunk_producers_assignment(
    epoch_config: &EpochConfig,
    rng_seed: RngSeed,
    prev_epoch_info: &EpochInfo,
    validator_roles: &ValidatorRoles,
    use_stable_shard_assignment: bool,
) -> Result<ChunkProducersAssignment, EpochError> {
    let ValidatorRoles { chunk_producers, block_producers, chunk_validators, .. } = validator_roles;

    // Construct local validator indices.
    // Note that if there are too few validators and too many shards,
    // assigning chunk producers to shards is more aggressive, so it
    // is not enough to iterate over chunk validators.
    // We assign local indices in the order of roles priority and then
    // in decreasing order of stake.
    let max_validators_for_role =
        cmp::max(chunk_producers.len(), cmp::max(block_producers.len(), chunk_validators.len()));
    let mut all_validators: Vec<ValidatorStake> = Vec::with_capacity(max_validators_for_role);
    let mut validator_to_index = HashMap::new();
    for validator in
        chunk_producers.iter().chain(block_producers.iter()).chain(chunk_validators.iter())
    {
        let account_id = validator.account_id().clone();
        if validator_to_index.contains_key(&account_id) {
            continue;
        }
        let id = all_validators.len() as ValidatorId;
        validator_to_index.insert(account_id, id);
        all_validators.push(validator.clone());
    }

    // Assign chunk producers to shards.
    let num_chunk_producers = chunk_producers.len();
    let minimum_validators_per_shard =
        epoch_config.validator_selection_config.minimum_validators_per_shard as usize;
    let prev_chunk_producers_assignment = if use_stable_shard_assignment {
        let mut assignment = vec![];
        for validator_ids in prev_epoch_info.chunk_producers_settlement().iter() {
            let mut validator_stakes = vec![];
            for validator_id in validator_ids.iter() {
                validator_stakes.push(prev_epoch_info.get_validator(*validator_id));
            }
            assignment.push(validator_stakes);
        }
        Some(assignment)
    } else {
        None
    };

    let shard_ids: Vec<_> = epoch_config.shard_layout.shard_ids().collect();
    let shard_assignment = assign_chunk_producers_to_shards(
        chunk_producers.clone(),
        shard_ids.len() as NumShards,
        minimum_validators_per_shard,
        epoch_config.validator_selection_config.chunk_producer_assignment_changes_limit as usize,
        rng_seed,
        prev_chunk_producers_assignment,
    )
    .map_err(|_| EpochError::NotEnoughValidators {
        num_validators: num_chunk_producers as u64,
        num_shards: shard_ids.len() as NumShards,
    })?;

    let chunk_producers_settlement = shard_assignment
        .into_iter()
        .map(|vs| vs.into_iter().map(|v| validator_to_index[v.account_id()]).collect())
        .collect();

    Ok(ChunkProducersAssignment { all_validators, validator_to_index, chunk_producers_settlement })
}

/// Select validators for next epoch and generate epoch info
pub fn proposals_to_epoch_info(
    epoch_config: &EpochConfig,
    rng_seed: RngSeed,
    prev_epoch_info: &EpochInfo,
    proposals: Vec<ValidatorStake>,
    mut validator_kickout: HashMap<AccountId, ValidatorKickoutReason>,
    validator_reward: HashMap<AccountId, Balance>,
    minted_amount: Balance,
    protocol_version: ProtocolVersion,
    use_stable_shard_assignment: bool,
) -> Result<EpochInfo, EpochError> {
    debug_assert!(
        proposals.iter().map(|stake| stake.account_id()).collect::<HashSet<_>>().len()
            == proposals.len(),
        "Proposals should not have duplicates"
    );

    let shard_ids: Vec<_> = epoch_config.shard_layout.shard_ids().collect();
    let mut stake_change = BTreeMap::new();
    let proposals = apply_epoch_update_to_proposals(
        proposals,
        prev_epoch_info,
        &validator_reward,
        &validator_kickout,
        &mut stake_change,
    );

    // Select validators for the next epoch.
    // Returns unselected proposals, validator lists for all roles and stake
    // threshold to become a validator.
    let validator_roles = if checked_feature!("stable", StatelessValidationV0, protocol_version) {
        select_validators_from_proposals(epoch_config, proposals, protocol_version)
    } else {
        old_validator_selection::select_validators_from_proposals(
            epoch_config,
            proposals,
            protocol_version,
        )
    };

    // Add kickouts for validators which fell out of validator set.
    // Used for querying epoch info by RPC.
    let threshold = validator_roles.threshold;
    for OrderedValidatorStake(p) in validator_roles.unselected_proposals.iter() {
        let stake = p.stake();
        let account_id = p.account_id();
        *stake_change.get_mut(account_id).unwrap() = 0;
        if prev_epoch_info.account_is_validator(account_id) {
            debug_assert!(stake < threshold);
            validator_kickout.insert(
                account_id.clone(),
                ValidatorKickoutReason::NotEnoughStake { stake, threshold },
            );
        }
    }

    // Constructing `validator_to_index` and `all_validators` mapping validator
    // account names to local indices throughout the epoch and vice versa, for
    // convenience of epoch manager.
    // Assign chunk producers to shards using local validator indices.
    // TODO: this happens together because assigment logic is more subtle for
    // older protocol versions, consider decoupling it.
    let ChunkProducersAssignment {
        all_validators,
        validator_to_index,
        mut chunk_producers_settlement,
    } = if checked_feature!("stable", StatelessValidationV0, protocol_version) {
        get_chunk_producers_assignment(
            epoch_config,
            rng_seed,
            prev_epoch_info,
            &validator_roles,
            use_stable_shard_assignment,
        )?
    } else if checked_feature!("stable", ChunkOnlyProducers, protocol_version) {
        old_validator_selection::assign_chunk_producers_to_shards_chunk_only(
            epoch_config,
            validator_roles.chunk_producers,
            &validator_roles.block_producers,
        )?
    } else {
        old_validator_selection::assign_chunk_producers_to_shards(
            epoch_config,
            validator_roles.chunk_producers,
            &validator_roles.block_producers,
        )?
    };

    if epoch_config.validator_selection_config.shuffle_shard_assignment_for_chunk_producers {
        chunk_producers_settlement.shuffle(&mut EpochInfo::shard_assignment_rng(&rng_seed));
    }

    // Get local indices for block producers.
    let block_producers_settlement = validator_roles
        .block_producers
        .into_iter()
        .map(|bp| validator_to_index[bp.account_id()])
        .collect();

    // Assign chunk validators to shards using validator mandates abstraction.
    let validator_mandates = if checked_feature!("stable", StatelessValidationV0, protocol_version)
    {
        // Value chosen based on calculations for the security of the protocol.
        // With this number of mandates per shard and 6 shards, the theory calculations predict the
        // protocol is secure for 40 years (at 90% confidence).
        let target_mandates_per_shard = 68;
        let num_shards = shard_ids.len();
        let validator_mandates_config =
            ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);
        // We can use `all_validators` to construct mandates Since a validator's position in
        // `all_validators` corresponds to its `ValidatorId`
        ValidatorMandates::new(validator_mandates_config, &all_validators)
    } else {
        ValidatorMandates::default()
    };

    Ok(EpochInfo::new(
        prev_epoch_info.epoch_height() + 1,
        all_validators,
        validator_to_index,
        block_producers_settlement,
        chunk_producers_settlement,
        stake_change,
        validator_reward,
        validator_kickout,
        minted_amount,
        threshold,
        protocol_version,
        rng_seed,
        validator_mandates,
    ))
}

/// Generates proposals based on proposals generated throughout last epoch,
/// last epoch validators and validator kickouts.
/// For each account that was validator in last epoch or made stake action last epoch
/// we apply the following in the order of priority:
/// 1. If account is in validator_kickout it cannot be validator for the next epoch,
///        we will not include it in proposals
/// 2. If account made staking action last epoch, it will be included in proposals with stake
///        adjusted by rewards from last epoch, if any
/// 3. If account was validator last epoch, it will be included in proposals with the same stake
///        as last epoch, adjusted by rewards from last epoch, if any
fn apply_epoch_update_to_proposals(
    proposals: Vec<ValidatorStake>,
    prev_epoch_info: &EpochInfo,
    validator_reward: &HashMap<AccountId, Balance>,
    validator_kickout: &HashMap<AccountId, ValidatorKickoutReason>,
    stake_change: &mut BTreeMap<AccountId, Balance>,
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

    proposals_by_account
}

fn order_proposals<I: IntoIterator<Item = ValidatorStake>>(
    proposals: I,
) -> BinaryHeap<OrderedValidatorStake> {
    proposals.into_iter().map(OrderedValidatorStake).collect()
}

fn select_block_producers(
    block_producer_proposals: BinaryHeap<OrderedValidatorStake>,
    max_num_selected: usize,
    min_stake_ratio: Ratio<u128>,
    protocol_version: ProtocolVersion,
) -> (Vec<ValidatorStake>, BinaryHeap<OrderedValidatorStake>, Balance) {
    select_validators(block_producer_proposals, max_num_selected, min_stake_ratio, protocol_version)
}

fn select_chunk_producers(
    all_proposals: BinaryHeap<OrderedValidatorStake>,
    max_num_selected: usize,
    min_stake_ratio: Ratio<u128>,
    num_shards: u64,
    protocol_version: ProtocolVersion,
) -> (Vec<ValidatorStake>, BinaryHeap<OrderedValidatorStake>, Balance) {
    select_validators(
        all_proposals,
        max_num_selected,
        min_stake_ratio * Ratio::new(1, num_shards as u128),
        protocol_version,
    )
}

// Takes the top N proposals (by stake), or fewer if there are not enough or the
// next proposals is too small relative to the others. In the case where all N
// slots are filled, or the stake ratio falls too low, the threshold stake to be included
// is also returned.
fn select_validators(
    mut proposals: BinaryHeap<OrderedValidatorStake>,
    max_number_selected: usize,
    min_stake_ratio: Ratio<u128>,
    protocol_version: ProtocolVersion,
) -> (Vec<ValidatorStake>, BinaryHeap<OrderedValidatorStake>, Balance) {
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
        (validators, proposals, threshold)
    } else {
        // the stake ratio condition prevented all slots from being filled,
        // or there were fewer proposals than available slots,
        // so the threshold stake is whatever amount pass the stake ratio condition
        let threshold = if checked_feature!(
            "protocol_feature_fix_staking_threshold",
            FixStakingThreshold,
            protocol_version
        ) {
            (min_stake_ratio * Ratio::from_integer(total_stake)
                / (Ratio::from_integer(1u128) - min_stake_ratio))
                .ceil()
                .to_integer()
        } else {
            (min_stake_ratio * Ratio::new(total_stake, 1)).ceil().to_integer()
        };
        (validators, proposals, threshold)
    }
}

/// We store stakes in max heap and want to order them such that the validator
/// with the largest state and (in case of a tie) lexicographically smallest
/// AccountId comes at the top.
#[derive(Eq, PartialEq)]
struct OrderedValidatorStake(ValidatorStake);
impl OrderedValidatorStake {
    fn key(&self) -> impl Ord + '_ {
        (self.0.stake(), std::cmp::Reverse(self.0.account_id()))
    }
}
impl PartialOrd for OrderedValidatorStake {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrderedValidatorStake {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key().cmp(&other.key())
    }
}

/// Helpers to generate new epoch info for older protocol versions.
mod old_validator_selection {
    use super::*;
    use crate::shard_assignment;

    /// Selects validator roles for the given proposals.
    pub(crate) fn select_validators_from_proposals(
        epoch_config: &EpochConfig,
        proposals: HashMap<AccountId, ValidatorStake>,
        protocol_version: ProtocolVersion,
    ) -> ValidatorRoles {
        let max_bp_selected = epoch_config.num_block_producer_seats as usize;
        let min_stake_ratio = {
            let rational = epoch_config.validator_selection_config.minimum_stake_ratio;
            Ratio::new(*rational.numer() as u128, *rational.denom() as u128)
        };

        let block_producer_proposals = order_proposals(proposals.values().cloned());
        let (block_producers, not_block_producers, bp_stake_threshold) = select_block_producers(
            block_producer_proposals,
            max_bp_selected,
            min_stake_ratio,
            protocol_version,
        );
        let (chunk_producer_proposals, chunk_producers, cp_stake_threshold) =
            if checked_feature!("stable", ChunkOnlyProducers, protocol_version) {
                let chunk_producer_proposals = order_proposals(proposals.into_values());
                let max_cp_selected = max_bp_selected
                    + (epoch_config.validator_selection_config.num_chunk_only_producer_seats
                        as usize);
                let num_shards = epoch_config.shard_layout.shard_ids().count() as NumShards;
                let (chunk_producers, not_chunk_producers, cp_stake_threshold) =
                    select_chunk_producers(
                        chunk_producer_proposals,
                        max_cp_selected,
                        min_stake_ratio,
                        num_shards,
                        protocol_version,
                    );
                (not_chunk_producers, chunk_producers, cp_stake_threshold)
            } else {
                (not_block_producers, block_producers.clone(), bp_stake_threshold)
            };

        // since block producer proposals could become chunk producers, their actual stake threshold
        // is the smaller of the two thresholds
        let threshold = cmp::min(bp_stake_threshold, cp_stake_threshold);

        ValidatorRoles {
            unselected_proposals: chunk_producer_proposals,
            chunk_producers,
            block_producers,
            chunk_validators: vec![], // chunk validators are not used for older protocol versions
            threshold,
        }
    }

    /// Assigns chunk producers to shards for the given proposals when chunk
    /// only producers were enabled, but before stateless validation.
    pub(crate) fn assign_chunk_producers_to_shards_chunk_only(
        epoch_config: &EpochConfig,
        chunk_producers: Vec<ValidatorStake>,
        block_producers: &[ValidatorStake],
    ) -> Result<ChunkProducersAssignment, EpochError> {
        let num_chunk_producers = chunk_producers.len();
        let mut all_validators: Vec<ValidatorStake> = Vec::with_capacity(num_chunk_producers);
        let mut validator_to_index = HashMap::new();
        for (i, bp) in block_producers.iter().enumerate() {
            let id = i as ValidatorId;
            validator_to_index.insert(bp.account_id().clone(), id);
            all_validators.push(bp.clone());
        }

        let shard_ids: Vec<_> = epoch_config.shard_layout.shard_ids().collect();
        let minimum_validators_per_shard =
            epoch_config.validator_selection_config.minimum_validators_per_shard as usize;
        let shard_assignment = shard_assignment::old_validator_selection::assign_shards(
            chunk_producers,
            shard_ids.len() as NumShards,
            minimum_validators_per_shard,
        )
        .map_err(|_| EpochError::NotEnoughValidators {
            num_validators: num_chunk_producers as u64,
            num_shards: shard_ids.len() as NumShards,
        })?;

        let mut chunk_producers_settlement: Vec<Vec<ValidatorId>> =
            shard_assignment.iter().map(|vs| Vec::with_capacity(vs.len())).collect();
        let mut i = all_validators.len();
        // Here we assign validator ids to all chunk only validators
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

        Ok(ChunkProducersAssignment {
            all_validators,
            validator_to_index,
            chunk_producers_settlement,
        })
    }

    /// Assigns chunk producers to shards given chunk and block producers before
    /// chunk only producers were enabled.
    pub(crate) fn assign_chunk_producers_to_shards(
        epoch_config: &EpochConfig,
        chunk_producers: Vec<ValidatorStake>,
        block_producers: &[ValidatorStake],
    ) -> Result<ChunkProducersAssignment, EpochError> {
        let mut all_validators: Vec<ValidatorStake> = Vec::with_capacity(chunk_producers.len());
        let mut validator_to_index = HashMap::new();
        let mut block_producers_settlement = Vec::with_capacity(block_producers.len());
        for (i, bp) in block_producers.into_iter().enumerate() {
            let id = i as ValidatorId;
            validator_to_index.insert(bp.account_id().clone(), id);
            block_producers_settlement.push(id);
            all_validators.push(bp.clone());
        }

        let shard_ids: Vec<_> = epoch_config.shard_layout.shard_ids().collect();
        if chunk_producers.is_empty() {
            // All validators tried to unstake?
            return Err(EpochError::NotEnoughValidators {
                num_validators: 0u64,
                num_shards: shard_ids.len() as NumShards,
            });
        }

        let mut id = 0usize;
        // Here we assign validators to chunks (we try to keep number of shards assigned for
        // each validator as even as possible). Note that in prod configuration number of seats
        // per shard is the same as maximal number of block producers, so normally all
        // validators would be assigned to all chunks
        let chunk_producers_settlement = shard_ids
            .iter()
            .map(|&shard_id| shard_id as usize)
            .map(|shard_id| {
                (0..epoch_config.num_block_producer_seats_per_shard[shard_id]
                    .min(block_producers_settlement.len() as u64))
                    .map(|_| {
                        let res = block_producers_settlement[id];
                        id = (id + 1) % block_producers_settlement.len();
                        res
                    })
                    .collect()
            })
            .collect();

        Ok(ChunkProducersAssignment {
            all_validators,
            validator_to_index,
            chunk_producers_settlement,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_crypto::{KeyType, PublicKey};
    use near_primitives::account::id::AccountIdRef;
    use near_primitives::epoch_manager::epoch_info::{EpochInfo, EpochInfoV3};
    use near_primitives::epoch_manager::ValidatorSelectionConfig;
    use near_primitives::shard_layout::ShardLayout;
    use near_primitives::types::validator_stake::ValidatorStake;
    use near_primitives::version::PROTOCOL_VERSION;
    use num_rational::Ratio;

    #[test]
    fn test_validator_assignment_all_block_producers() {
        // A simple sanity test. Given fewer proposals than the number of seats,
        // none of which has too little stake, they all get assigned as block and
        // chunk producers.
        let epoch_config = create_epoch_config(2, 100, Default::default());
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
            false,
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

        if checked_feature!("stable", StatelessValidationV0, PROTOCOL_VERSION) {
            // Validators are split between shards to balance number of validators.
            // Stakes don't matter for chunk producers.
            assert_eq!(epoch_info.chunk_producers_settlement(), &[vec![0, 2], vec![1]]);
        } else {
            // Validators are split between chunks to make roughly equal stakes
            // (in this case shard 0 has 2000, while shard 1 has 1300).
            assert_eq!(epoch_info.chunk_producers_settlement(), &[vec![0], vec![1, 2]]);
        }
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
            ValidatorSelectionConfig {
                num_chunk_producer_seats: num_bp_seats + num_cp_seats,
                num_chunk_validator_seats: num_bp_seats + num_cp_seats,
                num_chunk_only_producer_seats: num_cp_seats,
                ..Default::default()
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
            false,
        )
        .unwrap();

        assert_eq!(epoch_info.epoch_height(), prev_epoch_height + 1);

        // the top stakes are the chosen block producers
        let mut sorted_proposals = proposals;
        sorted_proposals.sort_unstable_by(|a, b| b.stake().cmp(&a.stake()));
        assert!(sorted_proposals.iter().take(num_bp_seats as usize).cloned().eq(epoch_info
            .block_producers_settlement()
            .into_iter()
            .map(|id| epoch_info.get_validator(*id))));

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
            kickout.get(AccountIdRef::new_or_panic("test1")).unwrap(),
            &ValidatorKickoutReason::NotEnoughStake { stake: test1_stake, threshold: 2011 },
        );
        assert_eq!(
            kickout.get(AccountIdRef::new_or_panic("test2")).unwrap(),
            &ValidatorKickoutReason::NotEnoughStake { stake: 2002, threshold: 2011 },
        );
    }

    // Test that the chunk validators' shard assignments will be shuffled or not shuffled
    // depending on the `shuffle_shard_assignment_for_chunk_producers` flag.
    #[test]
    fn test_validator_assignment_with_chunk_only_producers_with_shard_shuffling() {
        // Don't run test without stateless validation because it has slight
        // changes in validator epoch indexing.
        if !checked_feature!("stable", StatelessValidationV0, PROTOCOL_VERSION) {
            return;
        }

        let num_bp_seats = 10;
        let num_cp_seats = 30;
        let mut epoch_config = create_epoch_config(
            6,
            num_bp_seats,
            ValidatorSelectionConfig {
                num_chunk_producer_seats: num_bp_seats + num_cp_seats,
                num_chunk_validator_seats: num_bp_seats + num_cp_seats,
                num_chunk_only_producer_seats: num_cp_seats,
                ..Default::default()
            },
        );
        let prev_epoch_height = 3;
        let prev_epoch_info = create_prev_epoch_info::<&str>(prev_epoch_height, &[], &[]);
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
        let epoch_info_no_shuffling = proposals_to_epoch_info(
            &epoch_config,
            [1; 32],
            &prev_epoch_info,
            proposals.clone(),
            Default::default(),
            Default::default(),
            0,
            PROTOCOL_VERSION,
            false,
        )
        .unwrap();
        let epoch_info_no_shuffling_different_seed = proposals_to_epoch_info(
            &epoch_config,
            [2; 32],
            &prev_epoch_info,
            proposals.clone(),
            Default::default(),
            Default::default(),
            0,
            PROTOCOL_VERSION,
            false,
        )
        .unwrap();

        epoch_config.validator_selection_config.shuffle_shard_assignment_for_chunk_producers = true;
        let epoch_info_with_shuffling = proposals_to_epoch_info(
            &epoch_config,
            [1; 32],
            &prev_epoch_info,
            proposals.clone(),
            Default::default(),
            Default::default(),
            0,
            PROTOCOL_VERSION,
            false,
        )
        .unwrap();
        let epoch_info_with_shuffling_different_seed = proposals_to_epoch_info(
            &epoch_config,
            [2; 32],
            &prev_epoch_info,
            proposals,
            Default::default(),
            Default::default(),
            0,
            PROTOCOL_VERSION,
            false,
        )
        .unwrap();

        let target_settlement = vec![
            vec![0, 6, 12, 18, 24, 30, 36],
            vec![1, 7, 13, 19, 25, 31, 37],
            vec![2, 8, 14, 20, 26, 32, 38],
            vec![3, 9, 15, 21, 27, 33, 39],
            vec![4, 10, 16, 22, 28, 34],
            vec![5, 11, 17, 23, 29, 35],
        ];
        assert_eq!(epoch_info_no_shuffling.chunk_producers_settlement(), target_settlement,);

        assert_eq!(
            epoch_info_no_shuffling.chunk_producers_settlement(),
            epoch_info_no_shuffling_different_seed.chunk_producers_settlement()
        );

        let shuffled_settlement = [4, 2, 1, 0, 5, 3]
            .into_iter()
            .map(|i| target_settlement[i].clone())
            .collect::<Vec<_>>();
        assert_eq!(epoch_info_with_shuffling.chunk_producers_settlement(), shuffled_settlement);

        let shuffled_settlement = [3, 1, 0, 2, 5, 4]
            .into_iter()
            .map(|i| target_settlement[i].clone())
            .collect::<Vec<_>>();
        assert_eq!(
            epoch_info_with_shuffling_different_seed.chunk_producers_settlement(),
            shuffled_settlement,
        );
    }

    #[test]
    fn test_block_producer_sampling() {
        let num_shards = 4;
        let epoch_config = create_epoch_config(
            num_shards,
            2,
            ValidatorSelectionConfig {
                num_chunk_producer_seats: 2,
                num_chunk_validator_seats: 2,
                num_chunk_only_producer_seats: 0,
                ..Default::default()
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
            false,
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
            ValidatorSelectionConfig {
                num_chunk_producer_seats: 2 * num_shards,
                num_chunk_validator_seats: 2 * num_shards,
                num_chunk_only_producer_seats: 0,
                ..Default::default()
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
            false,
        )
        .unwrap();

        for shard_id in 0..num_shards {
            for h in 0..100_000 {
                let cp = epoch_info.sample_chunk_producer(h, shard_id);
                // Don't read too much into this. The reason the ValidatorId always
                // equals the ShardId is because the validators are assigned to shards in order.
                assert_eq!(cp, Some(shard_id))
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
            false,
        )
        .unwrap();

        for shard_id in 0..num_shards {
            let mut counts: [i32; 2] = [0, 0];
            for h in 0..100_000 {
                let cp = epoch_info.sample_chunk_producer(h, shard_id).unwrap();
                // if ValidatorId is in the second half then it is the lower
                // stake validator (because they are sorted by decreasing stake).
                let index = if cp >= num_shards { 1 } else { 0 };
                counts[index] += 1;
            }
            let diff = (2 * counts[1] - counts[0]).abs();
            assert!(diff < 500);
        }
    }

    fn get_epoch_info_for_chunk_validators_sampling() -> EpochInfo {
        let num_shards = 4;
        let epoch_config = create_epoch_config(
            num_shards,
            2 * num_shards,
            ValidatorSelectionConfig {
                num_chunk_producer_seats: 2 * num_shards,
                num_chunk_validator_seats: 2 * num_shards,
                num_chunk_only_producer_seats: 0,
                // for example purposes, we choose a higher ratio than in production
                minimum_stake_ratio: Ratio::new(1, 10),
                ..Default::default()
            },
        );
        let prev_epoch_height = 7;
        let prev_epoch_info = create_prev_epoch_info(prev_epoch_height, &["test1", "test2"], &[]);

        // Choosing proposals s.t. the `threshold` (i.e. seat price) calculated in
        // `proposals_to_epoch_info` below will be 100. For now, this `threshold` is used as the
        // stake required for a chunk validator mandate.
        //
        // Note that `proposals_to_epoch_info` will not include `test6` in the set of validators,
        // hence it will not hold a (partial) mandate
        let proposals = create_proposals(&[
            ("test1", 1500),
            ("test2", 1000),
            ("test3", 1000),
            ("test4", 260),
            ("test5", 140),
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
            false,
        )
        .unwrap();

        epoch_info
    }

    /// This test only verifies that chunk validator mandates are correctly wired up with
    /// `EpochInfo`. The internals of mandate assignment are tested in the module containing
    /// [`ValidatorMandates`].
    #[test]
    fn test_chunk_validators_sampling() {
        if !checked_feature!("stable", StatelessValidationV0, PROTOCOL_VERSION) {
            return;
        }

        let epoch_info = get_epoch_info_for_chunk_validators_sampling();
        // Given `epoch_info` and `proposals` above, the sample at a given height is deterministic.
        let height = 42;
        let expected_assignments = vec![
            vec![(4, 56), (1, 168), (2, 300), (3, 84), (0, 364)],
            vec![(3, 70), (1, 300), (4, 42), (2, 266), (0, 308)],
            vec![(4, 42), (1, 238), (3, 42), (0, 450), (2, 196)],
            vec![(2, 238), (1, 294), (3, 64), (0, 378)],
        ];
        assert_eq!(epoch_info.sample_chunk_validators(height), expected_assignments);
    }

    #[test]
    fn test_deterministic_chunk_validators_sampling() {
        if !checked_feature!("stable", StatelessValidationV0, PROTOCOL_VERSION) {
            return;
        }

        let epoch_info = get_epoch_info_for_chunk_validators_sampling();
        let height = 42;
        let assignment1 = epoch_info.sample_chunk_validators(height);
        let assignment2 = epoch_info.sample_chunk_validators(height);
        assert_eq!(assignment1, assignment2);
    }

    #[test]
    fn test_validator_assignment_ratio_condition() {
        // There are more seats than proposals, however the
        // lower proposals are too small relative to the total
        // (the reason we can't choose them is because the probability of them actually
        // being selected to make a block would be too low since it is done in
        // proportion to stake).
        let epoch_config = create_epoch_config(
            1,
            100,
            ValidatorSelectionConfig {
                num_chunk_producer_seats: 300,
                num_chunk_validator_seats: 300,
                num_chunk_only_producer_seats: 300,
                // for example purposes, we choose a higher ratio than in production
                minimum_stake_ratio: Ratio::new(1, 10),
                ..Default::default()
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
            false,
        )
        .unwrap();

        let fishermen: Vec<AccountId> =
            epoch_info.fishermen_iter().map(|v| v.take_account_id()).collect();
        assert!(fishermen.is_empty());

        // too low stakes are kicked out
        let kickout = epoch_info.validator_kickout();
        assert_eq!(kickout.len(), 2);
        #[cfg(feature = "protocol_feature_fix_staking_threshold")]
        let expected_threshold = 334;
        #[cfg(not(feature = "protocol_feature_fix_staking_threshold"))]
        let expected_threshold = 300;
        assert_eq!(
            kickout.get(AccountIdRef::new_or_panic("test5")).unwrap(),
            &ValidatorKickoutReason::NotEnoughStake { stake: 100, threshold: expected_threshold },
        );
        assert_eq!(
            kickout.get(AccountIdRef::new_or_panic("test6")).unwrap(),
            &ValidatorKickoutReason::NotEnoughStake { stake: 50, threshold: expected_threshold },
        );

        let bp_threshold = epoch_info.seat_price();
        let num_validators = epoch_info.validators_iter().len();
        let proposals = create_proposals(&[
            ("test1", 1000),
            ("test2", 1000),
            ("test3", 1000), // the total up to this point is 3000
            ("test4", 200),  // 200 is < 1/10 of 3000, so not validator, but can be fisherman
            ("test5", 100),  // 100 is even too small to be a fisherman, cannot get any role
            ("test6", 50),
            ("test7", bp_threshold),
        ]);
        let epoch_info = proposals_to_epoch_info(
            &epoch_config,
            [0; 32],
            &epoch_info,
            proposals,
            Default::default(),
            Default::default(),
            0,
            PROTOCOL_VERSION,
            false,
        )
        .unwrap();
        #[cfg(feature = "protocol_feature_fix_staking_threshold")]
        assert_eq!(num_validators + 1, epoch_info.validators_iter().len());

        let proposals = create_proposals(&[
            ("test1", 1000),
            ("test2", 1000),
            ("test3", 1000), // the total up to this point is 3000
            ("test4", 200),  // 200 is < 1/10 of 3000, so not validator, but can be fisherman
            ("test5", 100),  // 100 is even too small to be a fisherman, cannot get any role
            ("test6", 50),
            ("test7", bp_threshold - 1),
        ]);
        let epoch_info = proposals_to_epoch_info(
            &epoch_config,
            [0; 32],
            &epoch_info,
            proposals,
            Default::default(),
            Default::default(),
            0,
            PROTOCOL_VERSION,
            false,
        )
        .unwrap();
        assert_eq!(num_validators, epoch_info.validators_iter().len());
    }

    #[test]
    fn test_validator_assignment_with_kickout() {
        // kicked out validators are not selected
        let epoch_config = create_epoch_config(1, 100, Default::default());
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
            false,
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
        let epoch_config = create_epoch_config(1, 100, Default::default());
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
            false,
        )
        .unwrap();

        for (v, ((_, stake), reward)) in
            epoch_info.validators_iter().zip(validators.iter().zip(rewards.iter()))
        {
            assert_eq!(v.stake(), stake + reward);
        }
    }

    /// Create EpochConfig, only filling in the fields important for validator selection.
    fn create_epoch_config(
        num_shards: u64,
        num_block_producer_seats: u64,
        validator_selection_config: ValidatorSelectionConfig,
    ) -> EpochConfig {
        EpochConfig {
            epoch_length: 10,
            num_block_producer_seats,
            num_block_producer_seats_per_shard: vec![num_block_producer_seats; num_shards as usize],
            avg_hidden_validator_seats_per_shard: vec![0; num_shards as usize],
            block_producer_kickout_threshold: 0,
            chunk_producer_kickout_threshold: 0,
            chunk_validator_only_kickout_threshold: 0,
            target_validator_mandates_per_shard: 1,
            validator_max_kickout_stake_perc: 100,
            online_min_threshold: 0.into(),
            online_max_threshold: 0.into(),
            fishermen_threshold: 0,
            minimum_stake_divisor: 0,
            protocol_upgrade_stake_threshold: 0.into(),
            shard_layout: ShardLayout::v0(num_shards, 0),
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
            ValidatorStake::new(self.parse().unwrap(), PublicKey::empty(KeyType::ED25519), 100)
        }
    }

    impl IntoValidatorStake for (&str, Balance) {
        fn into_validator_stake(self) -> ValidatorStake {
            ValidatorStake::new(self.0.parse().unwrap(), PublicKey::empty(KeyType::ED25519), self.1)
        }
    }

    impl IntoValidatorStake for (String, Balance) {
        fn into_validator_stake(self) -> ValidatorStake {
            ValidatorStake::new(self.0.parse().unwrap(), PublicKey::empty(KeyType::ED25519), self.1)
        }
    }

    impl IntoValidatorStake for (&str, Balance, Proposal) {
        fn into_validator_stake(self) -> ValidatorStake {
            ValidatorStake::new(self.0.parse().unwrap(), PublicKey::empty(KeyType::ED25519), self.1)
        }
    }

    impl IntoValidatorStake for (String, Balance, Proposal) {
        fn into_validator_stake(self) -> ValidatorStake {
            ValidatorStake::new(self.0.parse().unwrap(), PublicKey::empty(KeyType::ED25519), self.1)
        }
    }

    impl<T: IntoValidatorStake + Copy> IntoValidatorStake for &T {
        fn into_validator_stake(self) -> ValidatorStake {
            (*self).into_validator_stake()
        }
    }
}

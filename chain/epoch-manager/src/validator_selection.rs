use crate::shard_assignment::{AssignmentRestrictions, assign_chunk_producers_to_shards};
use near_primitives::epoch_info::{EpochInfo, RngSeed};
use near_primitives::epoch_manager::EpochConfig;
use near_primitives::errors::EpochError;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, Balance, NumShards, ProtocolVersion, ValidatorId, ValidatorKickoutReason,
};
use near_primitives::validator_mandates::{ValidatorMandates, ValidatorMandatesConfig};
use num_rational::Ratio;
use rand::seq::SliceRandom;
use std::cmp::{self, Ordering};
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
) -> ValidatorRoles {
    let min_stake_ratio = {
        let rational = epoch_config.minimum_stake_ratio;
        Ratio::new(*rational.numer() as u128, *rational.denom() as u128)
    };

    let chunk_producer_proposals = order_proposals(proposals.values().cloned());
    let (chunk_producers, _, cp_stake_threshold) = select_chunk_producers(
        chunk_producer_proposals,
        epoch_config.num_chunk_producer_seats as usize,
        min_stake_ratio,
    );

    let block_producer_proposals = order_proposals(proposals.values().cloned());
    let (block_producers, _, bp_stake_threshold) = select_block_producers(
        block_producer_proposals,
        epoch_config.num_block_producer_seats as usize,
        min_stake_ratio,
    );

    let chunk_validator_proposals = order_proposals(proposals.values().cloned());
    let (chunk_validators, _, cv_stake_threshold) = select_validators(
        chunk_validator_proposals,
        epoch_config.num_chunk_validator_seats as usize,
        min_stake_ratio,
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
    chunk_producer_assignment_restrictions: Option<AssignmentRestrictions>,
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
    let minimum_validators_per_shard = epoch_config.minimum_validators_per_shard as usize;
    let mut prev_chunk_producers_assignment = vec![];
    for validator_ids in prev_epoch_info.chunk_producers_settlement() {
        let mut validator_stakes = vec![];
        for validator_id in validator_ids {
            validator_stakes.push(prev_epoch_info.get_validator(*validator_id));
        }
        prev_chunk_producers_assignment.push(validator_stakes);
    }

    let shard_ids: Vec<_> = epoch_config.shard_layout.shard_ids().collect();
    let shard_assignment = assign_chunk_producers_to_shards(
        chunk_producers.clone(),
        shard_ids.len() as NumShards,
        minimum_validators_per_shard,
        epoch_config.chunk_producer_assignment_changes_limit as usize,
        rng_seed,
        prev_chunk_producers_assignment,
        use_stable_shard_assignment,
        chunk_producer_assignment_restrictions,
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
    chunk_producer_assignment_restrictions: Option<AssignmentRestrictions>,
) -> Result<EpochInfo, EpochError> {
    debug_assert!(
        proposals.iter().map(|stake| stake.account_id()).collect::<HashSet<_>>().len()
            == proposals.len(),
        "Proposals should not have duplicates"
    );

    let num_shards = epoch_config
        .shard_layout
        .num_shards()
        .try_into()
        .expect("number of shards above usize range");
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
    let validator_roles = select_validators_from_proposals(epoch_config, proposals);

    // Add kickouts for validators which fell out of validator set.
    // Used for querying epoch info by RPC.
    let threshold = validator_roles.threshold;
    for OrderedValidatorStake(p) in &validator_roles.unselected_proposals {
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
    // TODO: this happens together because assignment logic is more subtle for
    // older protocol versions, consider decoupling it.
    let ChunkProducersAssignment {
        all_validators,
        validator_to_index,
        mut chunk_producers_settlement,
    } = get_chunk_producers_assignment(
        epoch_config,
        rng_seed,
        prev_epoch_info,
        &validator_roles,
        use_stable_shard_assignment,
        chunk_producer_assignment_restrictions,
    )?;

    if epoch_config.shuffle_shard_assignment_for_chunk_producers {
        chunk_producers_settlement.shuffle(&mut EpochInfo::shard_assignment_rng(&rng_seed));
    }

    // Get local indices for block producers.
    let block_producers_settlement = validator_roles
        .block_producers
        .into_iter()
        .map(|bp| validator_to_index[bp.account_id()])
        .collect();

    // Assign chunk validators to shards using validator mandates abstraction.
    let validator_mandates = {
        // Default production value chosen to 68 based on calculations for the
        // security of the mainnet protocol.
        // With this number of mandates per shard and 6 shards, the theory calculations predict the
        // protocol is secure for 40 years (at 90% confidence).
        let target_mandates_per_shard = epoch_config.target_validator_mandates_per_shard as usize;
        let validator_mandates_config =
            ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards);
        // We can use `all_validators` to construct mandates Since a validator's position in
        // `all_validators` corresponds to its `ValidatorId`
        ValidatorMandates::new(validator_mandates_config, &all_validators)
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
        } else if let Some(ValidatorKickoutReason::ProtocolVersionTooOld { .. }) =
            prev_epoch_info.validator_kickout().get(account_id)
        {
            // If the validator was kicked out because of an old protocol version in T-1,
            // it is not allowed back in T.
            continue;
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
) -> (Vec<ValidatorStake>, BinaryHeap<OrderedValidatorStake>, Balance) {
    select_validators(block_producer_proposals, max_num_selected, min_stake_ratio)
}

fn select_chunk_producers(
    all_proposals: BinaryHeap<OrderedValidatorStake>,
    max_num_selected: usize,
    min_stake_ratio: Ratio<u128>,
) -> (Vec<ValidatorStake>, BinaryHeap<OrderedValidatorStake>, Balance) {
    select_validators(all_proposals, max_num_selected, min_stake_ratio)
}

// Takes the top N proposals (by stake), or fewer if there are not enough or the
// next proposals is too small relative to the others. In the case where all N
// slots are filled, or the stake ratio falls too low, the threshold stake to be included
// is also returned.
fn select_validators(
    mut proposals: BinaryHeap<OrderedValidatorStake>,
    max_number_selected: usize,
    min_stake_ratio: Ratio<u128>,
) -> (Vec<ValidatorStake>, BinaryHeap<OrderedValidatorStake>, Balance) {
    let mut total_stake = 0;
    let n = cmp::min(max_number_selected, proposals.len());
    let mut validators = Vec::with_capacity(n);
    for _ in 0..n {
        let p = proposals.pop().unwrap().0;
        let p_stake = p.stake();
        let total_stake_with_p = total_stake + p_stake;
        if total_stake_with_p > 0 && Ratio::new(p_stake, total_stake_with_p) > min_stake_ratio {
            validators.push(p);
            total_stake = total_stake_with_p;
        } else {
            // p was not included, return it to the list of proposals
            proposals.push(OrderedValidatorStake(p));
            break;
        }
    }
    let threshold = if validators.len() == max_number_selected {
        // all slots were filled, so the threshold stake is 1 more than the current
        // smallest stake
        validators.last().unwrap().stake() + 1
    } else {
        // the stake ratio condition prevented all slots from being filled,
        // or there were fewer proposals than available slots,
        // so the threshold stake is whatever amount pass the stake ratio condition
        (min_stake_ratio * Ratio::from_integer(total_stake)
            / (Ratio::from_integer(1u128) - min_stake_ratio))
            .ceil()
            .to_integer()
    };
    (validators, proposals, threshold)
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

#[cfg(test)]
mod tests {
    use super::*;
    use near_crypto::{KeyType, PublicKey};
    use near_primitives::account::id::AccountIdRef;
    use near_primitives::epoch_info::{EpochInfo, EpochInfoV3};
    use near_primitives::shard_layout::ShardLayout;
    use near_primitives::types::NumSeats;
    use near_primitives::types::validator_stake::ValidatorStake;
    use near_primitives::version::PROTOCOL_VERSION;
    use num_rational::Ratio;

    #[test]
    fn test_validator_assignment_all_block_producers() {
        // A simple sanity test. Given fewer proposals than the number of seats,
        // none of which has too little stake, they all get assigned as block and
        // chunk producers.
        let epoch_config = create_epoch_config(2, 100, None, None, None);
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
            None,
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

        // Validators are split between shards to balance number of validators.
        // Stakes don't matter for chunk producers.
        assert_eq!(epoch_info.chunk_producers_settlement(), &[vec![0, 2], vec![1]]);
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
            Some(num_bp_seats + num_cp_seats),
            Some(num_bp_seats + num_cp_seats),
            None,
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
            None,
        )
        .unwrap();

        assert_eq!(epoch_info.epoch_height(), prev_epoch_height + 1);

        // the top stakes are the chosen block producers
        let mut sorted_proposals = proposals;
        sorted_proposals.sort_unstable_by(|a, b| b.stake().cmp(&a.stake()));
        assert!(
            sorted_proposals.iter().take(num_bp_seats as usize).cloned().eq(epoch_info
                .block_producers_settlement()
                .into_iter()
                .map(|id| epoch_info.get_validator(*id)))
        );

        // The top proposals are all chunk producers
        let mut chosen_chunk_producers: Vec<ValidatorStake> = epoch_info
            .chunk_producers_settlement()
            .into_iter()
            .flatten()
            .map(|id| epoch_info.get_validator(*id))
            .collect();
        chosen_chunk_producers.sort_unstable_by(|a, b| b.stake().cmp(&a.stake()));
        assert!(
            sorted_proposals
                .into_iter()
                .take((num_bp_seats + num_cp_seats) as usize)
                .eq(chosen_chunk_producers)
        );

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
        let num_bp_seats = 10;
        let num_cp_seats = 30;
        let mut epoch_config = create_epoch_config(
            6,
            num_bp_seats,
            Some(num_bp_seats + num_cp_seats),
            Some(num_bp_seats + num_cp_seats),
            None,
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
            None,
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
            None,
        )
        .unwrap();

        epoch_config.shuffle_shard_assignment_for_chunk_producers = true;
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
            None,
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
            None,
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
        let epoch_config = create_epoch_config(num_shards, 2, Some(2), Some(2), None);
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
            None,
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
            Some(2 * num_shards),
            Some(2 * num_shards),
            None,
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
            None,
        )
        .unwrap();

        let shard_layout = &epoch_config.shard_layout;
        for shard_info in shard_layout.shard_infos() {
            let shard_index = shard_info.shard_index();
            let shard_id = shard_info.shard_id();
            for h in 0..100_000 {
                let cp = epoch_info.sample_chunk_producer(shard_layout, shard_id, h);
                // Don't read too much into this. The reason the ValidatorId always
                // equals the ShardId is because the validators are assigned to shards in order.
                assert_eq!(cp, Some(shard_index as u64))
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
            None,
        )
        .unwrap();

        for shard_id in shard_layout.shard_ids() {
            let mut counts: [i32; 2] = [0, 0];
            for h in 0..100_000 {
                let cp = epoch_info.sample_chunk_producer(shard_layout, shard_id, h).unwrap();
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
            Some(2 * num_shards),
            Some(2 * num_shards),
            Some(Ratio::new(1, 10)),
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
            None,
        )
        .unwrap();

        epoch_info
    }

    /// This test only verifies that chunk validator mandates are correctly wired up with
    /// `EpochInfo`. The internals of mandate assignment are tested in the module containing
    /// [`ValidatorMandates`].
    #[test]
    fn test_chunk_validators_sampling() {
        let epoch_info = get_epoch_info_for_chunk_validators_sampling();
        // Given `epoch_info` and `proposals` above, the sample at a given height is deterministic.
        let height = 42;
        let expected_assignments = vec![
            vec![(2, 192), (0, 396), (1, 280)],
            vec![(1, 216), (2, 264), (0, 396)],
            vec![(0, 396), (2, 288), (1, 192)],
            vec![(2, 256), (1, 312), (0, 312)],
        ];
        assert_eq!(epoch_info.sample_chunk_validators(height), expected_assignments);
    }

    #[test]
    fn test_deterministic_chunk_validators_sampling() {
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
        let epoch_config =
            create_epoch_config(1, 100, Some(300), Some(300), Some(Ratio::new(1i32, 10i32)));
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
            None,
        )
        .unwrap();

        let fishermen = epoch_info.fishermen_iter().map(|v| v.take_account_id());
        assert_eq!(fishermen.count(), 0);

        // too low stakes are kicked out
        let kickout = epoch_info.validator_kickout();
        assert_eq!(kickout.len(), 2);
        let expected_threshold = 334;
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
            None,
        )
        .unwrap();
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
            None,
        )
        .unwrap();
        assert_eq!(num_validators, epoch_info.validators_iter().len());
    }

    #[test]
    fn test_validator_assignment_with_kickout() {
        // kicked out validators are not selected
        let epoch_config = create_epoch_config(1, 100, None, None, None);
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
            None,
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
        let epoch_config = create_epoch_config(1, 100, None, None, None);
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
            None,
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
        num_chunk_producer_seats: Option<NumSeats>,
        num_chunk_validator_seats: Option<NumSeats>,
        minimum_stake_ratio: Option<Ratio<i32>>,
    ) -> EpochConfig {
        let mut epoch_config = EpochConfig::minimal();
        epoch_config.epoch_length = 10;
        epoch_config.num_block_producer_seats = num_block_producer_seats;
        epoch_config.num_block_producer_seats_per_shard =
            vec![num_block_producer_seats; num_shards as usize];
        epoch_config.avg_hidden_validator_seats_per_shard = vec![0; num_shards as usize];
        epoch_config.target_validator_mandates_per_shard = 68;
        epoch_config.validator_max_kickout_stake_perc = 100;
        epoch_config.shard_layout = ShardLayout::multi_shard(num_shards, 0);

        if let Some(num_chunk_producer_seats) = num_chunk_producer_seats {
            epoch_config.num_chunk_producer_seats = num_chunk_producer_seats;
        }
        if let Some(num_chunk_validator_seats) = num_chunk_validator_seats {
            epoch_config.num_chunk_validator_seats = num_chunk_validator_seats;
        }
        if let Some(minimum_stake_ratio) = minimum_stake_ratio {
            epoch_config.minimum_stake_ratio = minimum_stake_ratio;
        }

        epoch_config
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

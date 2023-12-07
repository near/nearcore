use std::collections::HashMap;

use crate::types::{validator_stake::ValidatorStake, ValidatorId};
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::Balance;
use rand::{seq::SliceRandom, Rng};

/// Represents the configuration of [`ValidatorMandates`]. Its parameters are expected to remain
/// valid for one epoch.
#[derive(
    BorshSerialize, BorshDeserialize, Default, Copy, Clone, Debug, PartialEq, Eq, serde::Serialize,
)]
pub struct ValidatorMandatesConfig {
    /// The amount of stake that corresponds to one mandate.
    stake_per_mandate: Balance,
    /// The minimum number of mandates required per shard.
    min_mandates_per_shard: usize,
    /// The number of shards for the referenced epoch.
    num_shards: usize,
}

impl ValidatorMandatesConfig {
    /// Constructs a new configuration.
    ///
    /// # Panics
    ///
    /// Panics in the following cases:
    ///
    /// - If `stake_per_mandate` is 0 as this would lead to division by 0.
    /// - If `num_shards` is zero.
    pub fn new(
        stake_per_mandate: Balance,
        min_mandates_per_shard: usize,
        num_shards: usize,
    ) -> Self {
        assert!(stake_per_mandate > 0, "stake_per_mandate of 0 would lead to division by 0");
        assert!(num_shards > 0, "there should be at least one shard");
        Self { stake_per_mandate, min_mandates_per_shard, num_shards }
    }
}

/// The mandates for a set of validators given a [`ValidatorMandatesConfig`].
#[derive(
    BorshSerialize, BorshDeserialize, Default, Clone, Debug, PartialEq, Eq, serde::Serialize,
)]
pub struct ValidatorMandates {
    /// The configuration applied to the mandates.
    config: ValidatorMandatesConfig,
    /// Each element represents a validator mandate held by the validator with the given id.
    ///
    /// The id of a validator who holds `n >= 0` mandates occurs `n` times in the vector.
    mandates: Vec<ValidatorId>,
    /// Each element represents a partial validator mandate held by the validator with the given id.
    /// For example, an element `(1, 42)` represents the partial mandate of the validator with id 1
    /// which has a weight of 42.
    ///
    /// Validators whose stake can be distributed across mandates without remainder are not
    /// represented in this vector.
    partials: Vec<(ValidatorId, Balance)>,
}

impl ValidatorMandates {
    /// Initiates mandates corresponding to the provided `validators`. The validators must be sorted
    /// by id in ascending order, so the validator with `ValidatorId` equal to `i` is given by
    /// `validators[i]`.
    ///
    /// Only full mandates are assigned, partial mandates are dropped. For example, when the stake
    /// required for a mandate is 5 and a validator has staked 12, then it will obtain 2 mandates.
    pub fn new(config: ValidatorMandatesConfig, validators: &[ValidatorStake]) -> Self {
        let num_mandates_per_validator: Vec<u16> =
            validators.iter().map(|v| v.num_mandates(config.stake_per_mandate)).collect();
        let num_total_mandates =
            num_mandates_per_validator.iter().map(|&num| usize::from(num)).sum();
        let mut mandates: Vec<ValidatorId> = Vec::with_capacity(num_total_mandates);

        for i in 0..validators.len() {
            for _ in 0..num_mandates_per_validator[i] {
                // Each validator's position corresponds to its id.
                mandates.push(i as ValidatorId);
            }
        }

        let required_mandates = config.min_mandates_per_shard * config.num_shards;
        if mandates.len() < required_mandates {
            // TODO(#10014) dynamically lower `stake_per_mandate` to reach enough mandates
            panic!(
                "not enough validator mandates: got {}, need {}",
                mandates.len(),
                required_mandates
            );
        }

        // Not counting partials towards `required_mandates` as the weight of partials and its
        // distribution across shards may vary widely.
        //
        // Construct vector with capacity as most likely some validators' stake will not be evenly
        // divided by `config.stake_per_mandate`, i.e. some validators will have partials.
        let mut partials = Vec::with_capacity(validators.len());
        for i in 0..validators.len() {
            let partial_weight = validators[i].partial_mandate_weight(config.stake_per_mandate);
            if partial_weight > 0 {
                partials.push((i as ValidatorId, partial_weight));
            }
        }

        Self { config, mandates, partials }
    }

    /// Returns a validator assignment obtained by shuffling mandates.
    ///
    /// It clones mandates since [`ValidatorMandates`] is supposed to be valid for an epoch, while a
    /// new assignment is calculated at every height.
    pub fn sample<R>(&self, rng: &mut R) -> ValidatorMandatesAssignment
    where
        R: Rng + ?Sized,
    {
        let shuffled_mandates = self.shuffled_mandates(rng);
        let shuffled_partials = self.shuffled_partials(rng);

        // Distribute shuffled mandates and partials across shards. For each shard with `shard_id`
        // in `[0, num_shards)`, we take the elements of the vector with index `i` such that `i %
        // num_shards == shard_id`
        //
        // Assume, for example, there are 10 mandates and 4 shards. Then the shard with id 1 gets
        // assigned the mandates with indices 1, 5, and 9.
        //
        // TODO(#10014) shuffle shard ids to avoid a bias towards smaller shard ids
        let mut mandates_per_shard = Vec::with_capacity(self.config.num_shards);
        for shard_id in 0..self.config.num_shards {
            let mut assignments: HashMap<ValidatorId, AssignmentWeight> = HashMap::new();

            // For the current `shard_id`, collect mandates with index `i` such that
            // `i % num_shards == shard_id`.
            for idx in (shard_id..shuffled_mandates.len()).step_by(self.config.num_shards) {
                let id = shuffled_mandates[idx];
                assignments
                    .entry(id)
                    .and_modify(|assignment_weight| {
                        assignment_weight.num_mandates += 1;
                    })
                    .or_insert(AssignmentWeight::new(1, 0));
            }

            // For the current `shard_id`, collect partials with index `i` such that
            // `i % num_shards == shard_id`.
            for idx in (shard_id..shuffled_partials.len()).step_by(self.config.num_shards) {
                let (id, partial_weight) = shuffled_partials[idx];
                assignments
                    .entry(id)
                    .and_modify(|assignment_weight| {
                        assignment_weight.partial_weight += partial_weight;
                    })
                    .or_insert(AssignmentWeight::new(0, partial_weight));
            }

            mandates_per_shard.push(assignments)
        }

        mandates_per_shard
    }

    /// Clones the contained mandates and shuffles them. Cloning is required as a shuffle happens at
    /// every height while the `ValidatorMandates` are to be valid for an epoch.
    fn shuffled_mandates<R>(&self, rng: &mut R) -> Vec<ValidatorId>
    where
        R: Rng + ?Sized,
    {
        let mut shuffled_mandates = self.mandates.clone();
        shuffled_mandates.shuffle(rng);
        shuffled_mandates
    }

    /// Clones the contained partials and shuffles them. Cloning is required as a shuffle happens at
    /// every height while the `ValidatorMandates` are to be valid for an epoch.
    fn shuffled_partials<R>(&self, rng: &mut R) -> Vec<(ValidatorId, Balance)>
    where
        R: Rng + ?Sized,
    {
        let mut shuffled_partials = self.partials.clone();
        shuffled_partials.shuffle(rng);
        shuffled_partials
    }
}

/// Represents an assignment of [`ValidatorMandates`] for a specific height.
///
/// Contains one map per shard, with the position in the vector corresponding to `shard_id` in
/// `0..num_shards`. Each `HashMap` maps `ValidatorId`s to the number of mandates they have in the
/// corresponding shards. A validator whose id is not in a map has not been assigned to the shard.
///
/// For example, `mandates_per_shard[0][1]` maps to the [`AssignmentWeights`] validator with
/// `ValidatorId` 1 holds in shard with id 0.
pub type ValidatorMandatesAssignment = Vec<HashMap<ValidatorId, AssignmentWeight>>;

#[derive(Default, Clone, Debug, PartialEq, Eq)]
pub struct AssignmentWeight {
    pub num_mandates: u16,
    /// Stake assigned to this partial mandate.
    pub partial_weight: Balance,
}

impl AssignmentWeight {
    pub fn new(num_mandates: u16, partial_weight: Balance) -> Self {
        Self { num_mandates, partial_weight }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use near_crypto::PublicKey;
    use near_primitives_core::types::Balance;
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    use crate::{
        types::validator_stake::ValidatorStake, types::ValidatorId,
        validator_mandates::ValidatorMandatesConfig,
    };

    use super::{AssignmentWeight, ValidatorMandates, ValidatorMandatesAssignment};

    /// Returns a new, fixed RNG to be used only in tests. Using a fixed RNG facilitates testing as
    /// it makes outcomes based on that RNG deterministic.
    fn new_fixed_rng() -> ChaCha8Rng {
        ChaCha8Rng::seed_from_u64(42)
    }

    #[test]
    fn test_validator_mandates_config_new() {
        let stake_per_mandate = 10;
        let min_mandates_per_shard = 400;
        let num_shards = 4;
        assert_eq!(
            ValidatorMandatesConfig::new(stake_per_mandate, min_mandates_per_shard, num_shards),
            ValidatorMandatesConfig { stake_per_mandate, min_mandates_per_shard, num_shards },
        )
    }

    /// Constructs some `ValidatorStakes` for usage in tests.
    ///
    /// # Properties of the corresponding `ValidatorMandates`
    ///
    /// The mandates are (verified in [`test_validator_mandates_new`]):
    /// `vec![0, 0, 0, 1, 1, 3, 4, 4, 4]`
    ///
    /// The shuffling based on `new_fixed_rng` is (verified in
    /// [`test_validator_mandates_shuffled_mandates`]):
    /// `vec![0, 0, 0, 1, 1, 3, 4, 4, 4]`
    ///
    /// The partials are (verified in [`test_validator_mandates_new`]):
    /// `vec![(1, 7), (2, 9), (3, 2), (4, 5), (5, 4), (6, 6)]`
    ///
    /// The shuffled partials used in tests are (verified in
    /// [`test_validator_mandates_shuffled_partials`]):
    /// `vec![(1, 7), (4, 5), (2, 9), (3, 2), (6, 6), (5, 4)]`
    fn new_validator_stakes() -> Vec<ValidatorStake> {
        let new_vs = |account_id: &str, balance: Balance| -> ValidatorStake {
            ValidatorStake::new(
                account_id.parse().unwrap(),
                PublicKey::empty(near_crypto::KeyType::ED25519),
                balance,
            )
        };

        vec![
            new_vs("account_0", 30),
            new_vs("account_1", 27),
            new_vs("account_2", 9),
            new_vs("account_3", 12),
            new_vs("account_4", 35),
            new_vs("account_5", 4),
            new_vs("account_6", 6),
        ]
    }

    #[test]
    fn test_validator_mandates_new() {
        let validators = new_validator_stakes();
        let config = ValidatorMandatesConfig::new(10, 1, 4);
        let mandates = ValidatorMandates::new(config, &validators);

        // At 10 stake per mandate, the first validator holds three mandates, and so on.
        // Note that "account_2" holds no mandate as its stake is below the threshold.
        let expected_mandates: Vec<ValidatorId> = vec![0, 0, 0, 1, 1, 3, 4, 4, 4];
        assert_eq!(mandates.mandates, expected_mandates);

        // At 10 stake per mandate, the first validator holds no partial mandate, the second
        // validator holds a partial mandate with weight 7, and so on.
        let expected_partials: Vec<(ValidatorId, Balance)> =
            vec![(1, 7), (2, 9), (3, 2), (4, 5), (5, 4), (6, 6)];
        assert_eq!(mandates.partials, expected_partials);
    }

    #[test]
    fn test_validator_mandates_shuffled_mandates() {
        let validators = new_validator_stakes();
        let config = ValidatorMandatesConfig::new(10, 1, 4);
        let mandates = ValidatorMandates::new(config, &validators);
        let mut rng = new_fixed_rng();
        let assignment = mandates.shuffled_mandates(&mut rng);
        let expected_assignment: Vec<ValidatorId> = vec![0, 1, 1, 4, 4, 4, 0, 3, 0];
        assert_eq!(assignment, expected_assignment);
    }

    #[test]
    fn test_validator_mandates_shuffled_partials() {
        let validators = new_validator_stakes();
        let config = ValidatorMandatesConfig::new(10, 1, 4);
        let mandates = ValidatorMandates::new(config, &validators);
        let mut rng = new_fixed_rng();

        // Call `shuffled_mandates` before calling `shuffled_partials` using the same `rng` to
        // emulate what happens in [`ValidatorMandates::sample`]. Then `expected_assignment` below
        // equals the shuffled partial mandates assigned to shards in
        // `test_validator_mandates_sample_*`.
        let _ = mandates.shuffled_mandates(&mut rng);
        let assignment = mandates.shuffled_partials(&mut rng);
        let expected_assignment: Vec<(ValidatorId, Balance)> =
            vec![(1, 7), (4, 5), (2, 9), (3, 2), (6, 6), (5, 4)];
        assert_eq!(assignment, expected_assignment);
    }

    /// Test mandates per shard are collected correctly for `num_mandates % num_shards == 0` and
    /// `num_partials % num_shards == 0`.
    #[test]
    fn test_validator_mandates_sample_even() {
        // Choosing `num_shards` such that mandates and partials are distributed evenly.
        // Assignments in `test_validator_mandates_shuffled_*` can be used to construct
        // `expected_assignment` below.
        let config = ValidatorMandatesConfig::new(10, 1, 3);
        let expected_assignment: ValidatorMandatesAssignment = vec![
            HashMap::from([
                (0, AssignmentWeight::new(2, 0)),
                (4, AssignmentWeight::new(1, 0)),
                (1, AssignmentWeight::new(0, 7)),
                (3, AssignmentWeight::new(0, 2)),
            ]),
            HashMap::from([
                (1, AssignmentWeight::new(1, 0)),
                (4, AssignmentWeight::new(1, 5)),
                (3, AssignmentWeight::new(1, 0)),
                (6, AssignmentWeight::new(0, 6)),
            ]),
            HashMap::from([
                (0, AssignmentWeight::new(1, 0)),
                (1, AssignmentWeight::new(1, 0)),
                (4, AssignmentWeight::new(1, 0)),
                (2, AssignmentWeight::new(0, 9)),
                (5, AssignmentWeight::new(0, 4)),
            ]),
        ];
        assert_validator_mandates_sample(config, expected_assignment);
    }

    /// Test mandates per shard are collected correctly for `num_mandates % num_shards != 0` and
    /// `num_partials % num_shards != 0`.
    #[test]
    fn test_assigned_validator_mandates_get_mandates_for_shard_uneven() {
        // Choosing `num_shards` such that mandates and partials are distributed unevenly.
        // Assignments in `test_validator_mandates_shuffled_*` can be used to construct
        // `expected_assignment` below.
        let config = ValidatorMandatesConfig::new(10, 1, 4);
        let expected_mandates_per_shards: ValidatorMandatesAssignment = vec![
            HashMap::from([
                (0, AssignmentWeight::new(2, 0)),
                (4, AssignmentWeight::new(1, 0)),
                (1, AssignmentWeight::new(0, 7)),
                (6, AssignmentWeight::new(0, 6)),
            ]),
            HashMap::from([
                (1, AssignmentWeight::new(1, 0)),
                (4, AssignmentWeight::new(1, 5)),
                (5, AssignmentWeight::new(0, 4)),
            ]),
            HashMap::from([
                (0, AssignmentWeight::new(1, 0)),
                (1, AssignmentWeight::new(1, 0)),
                (2, AssignmentWeight::new(0, 9)),
            ]),
            HashMap::from([(4, AssignmentWeight::new(1, 0)), (3, AssignmentWeight::new(1, 2))]),
        ];
        assert_validator_mandates_sample(config, expected_mandates_per_shards);
    }

    /// Asserts mandates per shard are collected correctly.
    fn assert_validator_mandates_sample(
        config: ValidatorMandatesConfig,
        expected_assignment: ValidatorMandatesAssignment,
    ) {
        let validators = new_validator_stakes();
        let mandates = ValidatorMandates::new(config, &validators);

        let mut rng = new_fixed_rng();
        let assignment = mandates.sample(&mut rng);

        assert_eq!(assignment, expected_assignment);
    }
}

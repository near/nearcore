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
///
/// A mandate is a liability for a validator to validate a shard. Depending on its stake and the
/// `stake_per_mandate` specified in `ValidatorMandatesConfig`, a validator may hold multiple
/// mandates. Each mandate may be assigned to a different shard. The assignment of mandates to
/// shards is calculated with [`Self::sample`], typically at every height.
///
/// See #9983 for context and links to resources that introduce mandates.
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

    /// Returns a validator assignment obtained by shuffling mandates and assigning them to shards.
    /// Shard ids are shuffled as well in this process to avoid a bias lower shard ids, see
    /// [`ShuffledShardIds`].
    ///
    /// It clones mandates since [`ValidatorMandates`] is supposed to be valid for an epoch, while a
    /// new assignment is calculated at every height.
    pub fn sample<R>(&self, rng: &mut R) -> ValidatorMandatesAssignment
    where
        R: Rng + ?Sized,
    {
        // Shuffling shard ids to avoid a bias towards lower ids, see [`ShuffledShardIds`]. We
        // do two separate shuffes for full and partial mandates to reduce the likelihood of
        // assigning fewer full _and_ partial mandates to the _same_ shard.
        let shard_ids_for_mandates = ShuffledShardIds::new(rng, self.config.num_shards);
        let shard_ids_for_partials = ShuffledShardIds::new(rng, self.config.num_shards);

        let shuffled_mandates = self.shuffled_mandates(rng);
        let shuffled_partials = self.shuffled_partials(rng);

        // Distribute shuffled mandates and partials across shards. For each shard with `shard_id`
        // in `[0, num_shards)`, we take the elements of the vector with index `i` such that `i %
        // num_shards == shard_id`.
        //
        // Assume, for example, there are 10 mandates and 4 shards. Then for `shard_id = 1` we
        // collect the mandates with indices 1, 5, and 9.
        let mut mandates_per_shard: ValidatorMandatesAssignment =
            vec![HashMap::new(); self.config.num_shards];
        for shard_id in 0..self.config.num_shards {
            // Achieve shard id shuffling by writing to the position of the alias of `shard_id`.
            let mandates_assignment =
                &mut mandates_per_shard[shard_ids_for_mandates.get_alias(shard_id)];

            // For the current `shard_id`, collect mandates with index `i` such that
            // `i % num_shards == shard_id`.
            for idx in (shard_id..shuffled_mandates.len()).step_by(self.config.num_shards) {
                let validator_id = shuffled_mandates[idx];
                mandates_assignment
                    .entry(validator_id)
                    .and_modify(|assignment_weight| {
                        assignment_weight.num_mandates += 1;
                    })
                    .or_insert(AssignmentWeight::new(1, 0));
            }

            // Achieve shard id shuffling by writing to the position of the alias of `shard_id`.
            let partials_assignment =
                &mut mandates_per_shard[shard_ids_for_partials.get_alias(shard_id)];

            // For the current `shard_id`, collect partials with index `i` such that
            // `i % num_shards == shard_id`.
            for idx in (shard_id..shuffled_partials.len()).step_by(self.config.num_shards) {
                let (validator_id, partial_weight) = shuffled_partials[idx];
                partials_assignment
                    .entry(validator_id)
                    .and_modify(|assignment_weight| {
                        assignment_weight.partial_weight += partial_weight;
                    })
                    .or_insert(AssignmentWeight::new(0, partial_weight));
            }
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

/// When assigning mandates first to shards with lower ids, the shards with higher ids might end up
/// with fewer assigned mandates.
///
/// Assumes shard ids are in `[0, num_shards)`.
///
/// # Example
///
/// Assume there are 3 shards and 5 mandates. Assigning to shards with lower ids first, the first
/// two shards get 2 mandates each. For the third shard only 1 mandate remains.
///
/// # Shuffling to avoid bias
///
/// When mandates cannot be distributed evenly across shards, some shards will be assigned one
/// mandata less than others. Shuffling shard ids prevents a bias towards lower shard ids, as it is
/// no longer predictable which shard(s) will be assigned one mandate less.
#[derive(Default, Clone, Debug, PartialEq, Eq)]
struct ShuffledShardIds {
    /// Contains the shard ids `[0, num_shards)` in shuffled order.
    shuffled_ids: Vec<usize>,
}

impl ShuffledShardIds {
    fn new<R>(rng: &mut R, num_shards: usize) -> Self
    where
        R: Rng + ?Sized,
    {
        let mut shuffled_ids = (0..num_shards).collect::<Vec<_>>();
        shuffled_ids.shuffle(rng);
        Self { shuffled_ids }
    }

    /// Gets the alias of `shard_id` corresponding to the current shuffling.
    ///
    /// # Panics
    ///
    /// Panics if `shard_id >= num_shards`.
    fn get_alias(&self, shard_id: usize) -> usize {
        self.shuffled_ids[shard_id]
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

    use super::{
        AssignmentWeight, ShuffledShardIds, ValidatorMandates, ValidatorMandatesAssignment,
    };

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
    /// The partials are (verified in [`test_validator_mandates_new`]):
    /// `vec![(1, 7), (2, 9), (3, 2), (4, 5), (5, 4), (6, 6)]`
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
        // Testing with different `num_shards` values to verify the shuffles used in other tests.
        assert_validator_mandates_shuffled_mandates(3, vec![0, 1, 4, 4, 3, 1, 4, 0, 0]);
        assert_validator_mandates_shuffled_mandates(4, vec![0, 4, 1, 1, 0, 0, 4, 3, 4]);
    }

    fn assert_validator_mandates_shuffled_mandates(
        num_shards: usize,
        expected_assignment: Vec<ValidatorId>,
    ) {
        let validators = new_validator_stakes();
        let config = ValidatorMandatesConfig::new(10, 1, num_shards);
        let mandates = ValidatorMandates::new(config, &validators);
        let mut rng = new_fixed_rng();

        // Call methods that modify `rng` before shuffling mandates to emulate what happens in
        // [`ValidatorMandates::sample`]. Then `expected_assignment` below equals the shuffled
        // mandates assigned to shards in `test_validator_mandates_sample_*`.
        let _shard_ids_for_mandates = ShuffledShardIds::new(&mut rng, config.num_shards);
        let _shard_ids_for_partials = ShuffledShardIds::new(&mut rng, config.num_shards);
        let assignment = mandates.shuffled_mandates(&mut rng);
        assert_eq!(
            assignment, expected_assignment,
            "Unexpected shuffling for num_shards = {num_shards}"
        );
    }

    #[test]
    fn test_validator_mandates_shuffled_partials() {
        // Testing with different `num_shards` values to verify the shuffles used in other tests.
        assert_validator_mandates_shuffled_partials(
            3,
            vec![(3, 2), (4, 5), (1, 7), (2, 9), (5, 4), (6, 6)],
        );
        assert_validator_mandates_shuffled_partials(
            4,
            vec![(5, 4), (4, 5), (1, 7), (3, 2), (2, 9), (6, 6)],
        );
    }

    fn assert_validator_mandates_shuffled_partials(
        num_shards: usize,
        expected_assignment: Vec<(ValidatorId, Balance)>,
    ) {
        let validators = new_validator_stakes();
        let config = ValidatorMandatesConfig::new(10, 1, num_shards);
        let mandates = ValidatorMandates::new(config, &validators);
        let mut rng = new_fixed_rng();

        // Call methods that modify `rng` before shuffling mandates to emulate what happens in
        // [`ValidatorMandates::sample`]. Then `expected_assignment` below equals the shuffled
        // partials assigned to shards in `test_validator_mandates_sample_*`.
        let _shard_ids_for_mandates = ShuffledShardIds::new(&mut rng, config.num_shards);
        let _shard_ids_for_partials = ShuffledShardIds::new(&mut rng, config.num_shards);
        let _ = mandates.shuffled_mandates(&mut rng);
        let assignment = mandates.shuffled_partials(&mut rng);
        assert_eq!(
            assignment, expected_assignment,
            "Unexpected shuffling for num_shards = {num_shards}"
        );
    }

    /// Test mandates per shard are collected correctly for `num_mandates % num_shards == 0` and
    /// `num_partials % num_shards == 0`.
    #[test]
    fn test_validator_mandates_sample_even() {
        // Choosing `num_shards` such that mandates and partials are distributed evenly.
        // Assignments in `test_validator_mandates_shuffled_*` can be used to construct
        // `expected_assignment` below.
        // Note that shard ids are shuffled too, see `test_shuffled_shard_ids_new`.
        let config = ValidatorMandatesConfig::new(10, 1, 3);
        let expected_assignment: ValidatorMandatesAssignment = vec![
            HashMap::from([
                (4, AssignmentWeight::new(1, 0)),
                (1, AssignmentWeight::new(1, 7)),
                (0, AssignmentWeight::new(1, 0)),
                (6, AssignmentWeight::new(0, 6)),
            ]),
            HashMap::from([
                (1, AssignmentWeight::new(1, 0)),
                (3, AssignmentWeight::new(1, 0)),
                (0, AssignmentWeight::new(1, 0)),
                (4, AssignmentWeight::new(0, 5)),
                (5, AssignmentWeight::new(0, 4)),
            ]),
            HashMap::from([
                (0, AssignmentWeight::new(1, 0)),
                (4, AssignmentWeight::new(2, 0)),
                (3, AssignmentWeight::new(0, 2)),
                (2, AssignmentWeight::new(0, 9)),
            ]),
        ];
        assert_validator_mandates_sample(config, expected_assignment);
    }

    /// Test mandates per shard are collected correctly for `num_mandates % num_shards != 0` and
    /// `num_partials % num_shards != 0`.
    #[test]
    fn test_validator_mandates_sample_uneven() {
        // Choosing `num_shards` such that mandates and partials are distributed unevenly.
        // Assignments in `test_validator_mandates_shuffled_*` can be used to construct
        // `expected_assignment` below.
        // Note that shard ids are shuffled too, see `test_shuffled_shard_ids_new`.
        let config = ValidatorMandatesConfig::new(10, 1, 4);
        let expected_mandates_per_shards: ValidatorMandatesAssignment = vec![
            HashMap::from([
                (0, AssignmentWeight::new(2, 0)),
                (4, AssignmentWeight::new(1, 0)),
                (1, AssignmentWeight::new(0, 7)),
            ]),
            HashMap::from([
                (1, AssignmentWeight::new(1, 0)),
                (4, AssignmentWeight::new(1, 0)),
                (3, AssignmentWeight::new(0, 2)),
            ]),
            HashMap::from([
                (4, AssignmentWeight::new(1, 5)),
                (0, AssignmentWeight::new(1, 0)),
                (6, AssignmentWeight::new(0, 6)),
            ]),
            HashMap::from([
                (1, AssignmentWeight::new(1, 0)),
                (3, AssignmentWeight::new(1, 0)),
                (5, AssignmentWeight::new(0, 4)),
                (2, AssignmentWeight::new(0, 9)),
            ]),
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

    #[test]
    fn test_shuffled_shard_ids_new() {
        // Testing with different `num_shards` values to verify the shuffles used in other tests.
        // Doing two shuffles for each `num_shards` with the same RNG since shard ids are shuffled
        // twice (once for full and once for partial mandates).
        let mut rng_3_shards = new_fixed_rng();
        assert_shuffled_shard_ids(&mut rng_3_shards, 3, vec![2, 1, 0], "3 shards, 1st shuffle");
        assert_shuffled_shard_ids(&mut rng_3_shards, 3, vec![2, 1, 0], "3 shards, 2nd shuffle");
        let mut rng_4_shards = new_fixed_rng();
        assert_shuffled_shard_ids(&mut rng_4_shards, 4, vec![0, 2, 1, 3], "4 shards, 1st shuffle");
        assert_shuffled_shard_ids(&mut rng_4_shards, 4, vec![3, 2, 0, 1], "4 shards, 2nd shuffle");
    }

    fn assert_shuffled_shard_ids(
        rng: &mut ChaCha8Rng,
        num_shards: usize,
        expected_shuffling: Vec<usize>,
        test_descriptor: &str,
    ) {
        let shuffled_ids_full_mandates = ShuffledShardIds::new(rng, num_shards);
        assert_eq!(
            shuffled_ids_full_mandates,
            ShuffledShardIds { shuffled_ids: expected_shuffling },
            "Unexpected shuffling for {test_descriptor}",
        );
    }

    #[test]
    fn test_shuffled_shard_ids_get_alias() {
        let mut rng = new_fixed_rng();
        let shuffled_ids = ShuffledShardIds::new(&mut rng, 4);
        // See [`test_shuffled_shard_ids_new`] for the result of this shuffling.
        assert_eq!(shuffled_ids.get_alias(1), 2);
    }
}

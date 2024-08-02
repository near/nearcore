use crate::types::{validator_stake::ValidatorStake, ValidatorId};
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::Balance;
use near_schema_checker_lib::ProtocolSchema;

mod compute_price;

/// Represents the configuration of [`ValidatorMandates`]. Its parameters are expected to remain
/// valid for one epoch.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Default,
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    ProtocolSchema,
)]
pub struct ValidatorMandatesConfig {
    /// The desired number of mandates required per shard.
    target_mandates_per_shard: usize,
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
    pub fn new(target_mandates_per_shard: usize, num_shards: usize) -> Self {
        assert!(num_shards > 0, "there should be at least one shard");
        Self { target_mandates_per_shard, num_shards }
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
    BorshSerialize,
    BorshDeserialize,
    Default,
    Clone,
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    ProtocolSchema,
)]
pub struct ValidatorMandates {
    /// The configuration applied to the mandates.
    config: ValidatorMandatesConfig,
    /// The amount of stake a whole mandate is worth.
    stake_per_mandate: Balance,
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
        let stakes: Vec<Balance> = validators.iter().map(|v| v.stake()).collect();
        let stake_per_mandate = compute_price::compute_mandate_price(config, &stakes);
        let num_mandates_per_validator: Vec<u16> =
            validators.iter().map(|v| v.num_mandates(stake_per_mandate)).collect();
        let num_total_mandates =
            num_mandates_per_validator.iter().map(|&num| usize::from(num)).sum();
        let mut mandates: Vec<ValidatorId> = Vec::with_capacity(num_total_mandates);

        for i in 0..validators.len() {
            for _ in 0..num_mandates_per_validator[i] {
                // Each validator's position corresponds to its id.
                mandates.push(i as ValidatorId);
            }
        }

        // Not counting partials towards `required_mandates` as the weight of partials and its
        // distribution across shards may vary widely.
        //
        // Construct vector with capacity as most likely some validators' stake will not be evenly
        // divided by `config.stake_per_mandate`, i.e. some validators will have partials.
        let mut partials = Vec::with_capacity(validators.len());
        for i in 0..validators.len() {
            let partial_weight = validators[i].partial_mandate_weight(stake_per_mandate);
            if partial_weight > 0 {
                partials.push((i as ValidatorId, partial_weight));
            }
        }

        Self { config, stake_per_mandate, mandates, partials }
    }
}

#[cfg(feature = "rand")]
mod validator_mandates_sample {
    use super::*;
    use itertools::Itertools;
    use rand::{seq::SliceRandom, Rng};

    impl ValidatorMandates {
        /// Returns a validator assignment obtained by shuffling mandates and assigning them to shards.
        /// Shard ids are shuffled as well in this process to avoid a bias lower shard ids, see
        /// [`ShuffledShardIds`].
        ///
        /// It clones mandates since [`ValidatorMandates`] is supposed to be valid for an epoch, while a
        /// new assignment is calculated at every height.
        pub fn sample<R>(&self, rng: &mut R) -> ChunkValidatorStakeAssignment
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
            let stake_per_mandate = self.stake_per_mandate;
            let mut stake_assignment_per_shard =
                vec![std::collections::HashMap::new(); self.config.num_shards];
            for shard_id in 0..self.config.num_shards {
                // Achieve shard id shuffling by writing to the position of the alias of `shard_id`.
                let mandates_assignment =
                    &mut stake_assignment_per_shard[shard_ids_for_mandates.get_alias(shard_id)];

                // For the current `shard_id`, collect mandates with index `i` such that
                // `i % num_shards == shard_id`.
                for idx in (shard_id..shuffled_mandates.len()).step_by(self.config.num_shards) {
                    let validator_id = shuffled_mandates[idx];
                    *mandates_assignment.entry(validator_id).or_default() += stake_per_mandate;
                }

                // Achieve shard id shuffling by writing to the position of the alias of `shard_id`.
                let partials_assignment =
                    &mut stake_assignment_per_shard[shard_ids_for_partials.get_alias(shard_id)];

                // For the current `shard_id`, collect partials with index `i` such that
                // `i % num_shards == shard_id`.
                for idx in (shard_id..shuffled_partials.len()).step_by(self.config.num_shards) {
                    let (validator_id, partial_weight) = shuffled_partials[idx];
                    *partials_assignment.entry(validator_id).or_default() += partial_weight;
                }
            }

            // Deterministically shuffle the validator order for each shard
            let mut ordered_stake_assignment_per_shard = Vec::with_capacity(self.config.num_shards);
            for shard_id in 0..self.config.num_shards {
                // first sort the validators by id then shuffle using rng
                let stake_assignment = &stake_assignment_per_shard[shard_id];
                let mut ordered_validator_ids = stake_assignment.keys().sorted().collect_vec();
                ordered_validator_ids.shuffle(rng);
                let ordered_mandate_assignment = ordered_validator_ids
                    .into_iter()
                    .map(|validator_id| (*validator_id, stake_assignment[validator_id]))
                    .collect_vec();
                ordered_stake_assignment_per_shard.push(ordered_mandate_assignment);
            }

            ordered_stake_assignment_per_shard
        }

        /// Clones the contained mandates and shuffles them. Cloning is required as a shuffle happens at
        /// every height while the `ValidatorMandates` are to be valid for an epoch.
        pub(super) fn shuffled_mandates<R>(&self, rng: &mut R) -> Vec<ValidatorId>
        where
            R: Rng + ?Sized,
        {
            let mut shuffled_mandates = self.mandates.clone();
            shuffled_mandates.shuffle(rng);
            shuffled_mandates
        }

        /// Clones the contained partials and shuffles them. Cloning is required as a shuffle happens at
        /// every height while the `ValidatorMandates` are to be valid for an epoch.
        pub(super) fn shuffled_partials<R>(&self, rng: &mut R) -> Vec<(ValidatorId, Balance)>
        where
            R: Rng + ?Sized,
        {
            let mut shuffled_partials = self.partials.clone();
            shuffled_partials.shuffle(rng);
            shuffled_partials
        }
    }
}

/// Represents an assignment of [`ValidatorMandates`] for a specific height.
///
/// Contains one vec per shard, with the position in the vector corresponding to `shard_id` in
/// `0..num_shards`. Each element is a tuple of `ValidatorId`, total stake they have in the
/// corresponding shards. A validator whose id is not in any vec has not been assigned to the shard.
///
/// For example, `mandates_per_shard[0]` gives us the entries of shard with id 0.
/// Elements of `mandates_per_shard[0]` can be [(validator3, stake), (validator7, stake)]
pub type ChunkValidatorStakeAssignment = Vec<Vec<(ValidatorId, Balance)>>;

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

#[cfg(feature = "rand")]
impl ShuffledShardIds {
    fn new<R>(rng: &mut R, num_shards: usize) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        use rand::seq::SliceRandom;

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
    use near_crypto::PublicKey;
    use near_primitives_core::types::Balance;
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    use crate::{
        types::validator_stake::ValidatorStake, types::ValidatorId,
        validator_mandates::ValidatorMandatesConfig,
    };

    use super::{ChunkValidatorStakeAssignment, ShuffledShardIds, ValidatorMandates};

    /// Returns a new, fixed RNG to be used only in tests. Using a fixed RNG facilitates testing as
    /// it makes outcomes based on that RNG deterministic.
    fn new_fixed_rng() -> ChaCha8Rng {
        ChaCha8Rng::seed_from_u64(42)
    }

    #[test]
    fn test_validator_mandates_config_new() {
        let target_mandates_per_shard = 400;
        let num_shards = 4;
        assert_eq!(
            ValidatorMandatesConfig::new(target_mandates_per_shard, num_shards),
            ValidatorMandatesConfig { target_mandates_per_shard, num_shards },
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
        let config = ValidatorMandatesConfig::new(3, 4);
        let mandates = ValidatorMandates::new(config, &validators);

        // With 3 mandates per shard and 4 shards, we are looking for around 12 total mandates.
        // The total stake in `new_validator_stakes` is 123, so to get 12 mandates we need a price
        // close to 10. But the algorithm for computing price tries to make the number of _whole_
        // mandates equal to 12, and there are validators with partial mandates in the distribution,
        // therefore the price is set a little lower than 10.
        assert_eq!(mandates.stake_per_mandate, 8);

        // At 8 stake per mandate, the first validator holds three mandates, and so on.
        // Note that "account_5" and "account_6" hold no mandate as both their stakes are below the threshold.
        let expected_mandates: Vec<ValidatorId> = vec![0, 0, 0, 1, 1, 1, 2, 3, 4, 4, 4, 4];
        assert_eq!(mandates.mandates, expected_mandates);

        // The number of whole mandates is exactly equal to our target
        assert_eq!(mandates.mandates.len(), config.num_shards * config.target_mandates_per_shard);

        // At 8 stake per mandate, the first validator a partial mandate with weight 6, the second
        // validator holds a partial mandate with weight 3, and so on.
        let expected_partials: Vec<(ValidatorId, Balance)> =
            vec![(0, 6), (1, 3), (2, 1), (3, 4), (4, 3), (5, 4), (6, 6)];
        assert_eq!(mandates.partials, expected_partials);
    }

    #[test]
    fn test_validator_mandates_shuffled_mandates() {
        // Testing with different `num_shards` values to verify the shuffles used in other tests.
        assert_validator_mandates_shuffled_mandates(3, vec![0, 1, 4, 4, 3, 1, 4, 0, 0]);
        assert_validator_mandates_shuffled_mandates(4, vec![0, 0, 2, 1, 3, 4, 1, 1, 0, 4, 4, 4]);
    }

    fn assert_validator_mandates_shuffled_mandates(
        num_shards: usize,
        expected_assignment: Vec<ValidatorId>,
    ) {
        let validators = new_validator_stakes();
        let config = ValidatorMandatesConfig::new(3, num_shards);
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
            vec![(5, 4), (3, 4), (0, 6), (2, 1), (1, 3), (4, 3), (6, 6)],
        );
    }

    fn assert_validator_mandates_shuffled_partials(
        num_shards: usize,
        expected_assignment: Vec<(ValidatorId, Balance)>,
    ) {
        let validators = new_validator_stakes();
        let config = ValidatorMandatesConfig::new(3, num_shards);
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
        let config = ValidatorMandatesConfig::new(3, 3);
        let expected_assignment = vec![
            vec![(1, 17), (4, 10), (6, 06), (0, 10)],
            vec![(4, 05), (5, 04), (0, 10), (1, 10), (3, 10)],
            vec![(0, 10), (2, 09), (4, 20), (3, 02)],
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
        let config = ValidatorMandatesConfig::new(3, 4);
        let expected_mandates_per_shards = vec![
            vec![(3, 8), (6, 6), (0, 22)],
            vec![(4, 8), (2, 9), (1, 08)],
            vec![(0, 8), (3, 4), (4, 19)],
            vec![(4, 8), (5, 4), (1, 19)],
        ];
        assert_validator_mandates_sample(config, expected_mandates_per_shards);
    }

    /// Asserts mandates per shard are collected correctly.
    fn assert_validator_mandates_sample(
        config: ValidatorMandatesConfig,
        expected_assignment: ChunkValidatorStakeAssignment,
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

    #[test]
    fn test_deterministic_shuffle() {
        let config = ValidatorMandatesConfig::new(3, 4);
        let validators = new_validator_stakes();
        let mandates = ValidatorMandates::new(config, &validators);

        let mut rng1 = new_fixed_rng();
        let assignment1 = mandates.sample(&mut rng1);

        let mut rng2 = new_fixed_rng();
        let assignment2 = mandates.sample(&mut rng2);

        // Two assignments with the same RNG should be equal.
        assert_eq!(assignment1, assignment2);
    }
}

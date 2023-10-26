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
    /// The id of a validator who holds `n >= 0` mandates occurs `n` times in the vector.
    mandates: Vec<ValidatorId>,
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

        Self { config, mandates }
    }

    /// Returns a validator assignment obtained by shuffling mandates.
    ///
    /// It clones mandates since [`ValidatorMandates`] is supposed to be valid for an epoch, while a
    /// new assignment is calculated at every height.
    ///
    /// # Interpretation of the return value
    ///
    /// The returned vector contains one map per shard, with the position in the vector
    /// corresponding to `shard_id` in `0..num_shards`.
    ///
    /// Each `HashMap` maps `ValidatorId`s to the number of mandates they have in the corresponding
    /// shards. A validator whose id is not in a map has not been assigned to the shard.
    ///
    /// ## Example
    ///
    /// Let `res` be the return value of this function, then `res[0][1]` maps to the number of
    /// mandates validator with `ValidatorId` 1 holds in shard with id 0.
    pub fn sample<R>(&self, rng: &mut R) -> Vec<HashMap<ValidatorId, u16>>
    where
        R: Rng + ?Sized,
    {
        let shuffled_mandates = self.shuffle(rng);

        // Assign shuffled seat at position `i` to the shard with id `i % num_shards`.
        let mut assignments_per_shard = Vec::with_capacity(self.config.num_shards);
        for shard_id in 0..self.config.num_shards {
            let mut assignments = HashMap::new();
            for idx in (shard_id..shuffled_mandates.len()).step_by(self.config.num_shards) {
                let id = shuffled_mandates[idx];
                assignments.entry(id).and_modify(|counter| *counter += 1).or_insert(1);
            }
            assignments_per_shard.push(assignments)
        }

        assignments_per_shard
    }

    /// Clones the contained mandates and shuffles them. Cloning is required as a shuffle happens at
    /// every height while the `ValidatorMandates` are to be valid for an epoch.
    fn shuffle<R>(&self, rng: &mut R) -> Vec<ValidatorId>
    where
        R: Rng + ?Sized,
    {
        let mut shuffled_mandates = self.mandates.clone();
        shuffled_mandates.shuffle(rng);
        shuffled_mandates
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

    use super::ValidatorMandates;

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
    /// The shuffling based on `new_fixed_rng` is (verified in [`test_validator_mandates_shuffle`]):
    /// `vec![0, 0, 0, 1, 1, 3, 4, 4, 4]`
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
    }

    #[test]
    fn test_validator_mandates_shuffle() {
        let validators = new_validator_stakes();
        let config = ValidatorMandatesConfig::new(10, 1, 4);
        let mandates = ValidatorMandates::new(config, &validators);
        let mut rng = new_fixed_rng();
        let assignment = mandates.shuffle(&mut rng);
        let expected_assignment: Vec<ValidatorId> = vec![0, 1, 1, 4, 4, 4, 0, 3, 0];
        assert_eq!(assignment, expected_assignment);
    }

    /// Test mandates per shard are collected correctly if `num_mandates % num_shards == 0`.
    #[test]
    fn test_assigned_validator_mandates_get_mandates_for_shard_even() {
        // Choosing `num_shards` such that mandates are distributed evenly.
        let config = ValidatorMandatesConfig::new(10, 1, 3);
        let expected_mandates_per_shards: Vec<HashMap<ValidatorId, u16>> = vec![
            HashMap::from([(0, 2), (4, 1)]),
            HashMap::from([(1, 1), (3, 1), (4, 1)]),
            HashMap::from([(0, 1), (1, 1), (4, 1)]),
        ];
        assert_validator_mandates_sample(config, expected_mandates_per_shards);
    }

    /// Test mandates per shard are collected correctly if `num_mandates % num_shards != 0`.
    #[test]
    fn test_assigned_validator_mandates_get_mandates_for_shard_uneven() {
        // Choosing `num_shards` such that mandates are distributed unevenly.
        let config = ValidatorMandatesConfig::new(10, 1, 2);
        let expected_mandates_per_shards: Vec<HashMap<ValidatorId, u16>> =
            vec![HashMap::from([(0, 3), (1, 1), (4, 1)]), HashMap::from([(1, 1), (4, 2), (3, 1)])];
        assert_validator_mandates_sample(config, expected_mandates_per_shards);
    }

    /// Asserts mandates per shard are collected correctly.
    fn assert_validator_mandates_sample(
        config: ValidatorMandatesConfig,
        expected_mandates_per_shards: Vec<HashMap<ValidatorId, u16>>,
    ) {
        let validators = new_validator_stakes();
        let mandates = ValidatorMandates::new(config, &validators);

        let mut rng = new_fixed_rng();
        let mandates_per_shards = mandates.sample(&mut rng);

        assert_eq!(mandates_per_shards, expected_mandates_per_shards);
    }
}

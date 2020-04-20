use ethereum_types::U256;
use std::collections::HashMap;

use near_primitives::types::{AccountId, Balance, BlockChunkValidatorStats};
use num_rational::Rational;

#[derive(Clone)]
pub struct RewardCalculator {
    pub max_inflation_rate: Rational,
    pub num_blocks_per_year: u64,
    pub epoch_length: u64,
    pub protocol_reward_percentage: Rational,
    pub protocol_treasury_account: AccountId,
    pub online_min_threshold: Rational,
    pub online_max_threshold: Rational,
}

impl RewardCalculator {
    /// Calculate validator reward for an epoch based on their block and chunk production stats.
    /// Returns map of validators with their rewards and amount of newly minted tokens including to protocol's treasury.
    pub fn calculate_reward(
        &self,
        validator_block_chunk_stats: HashMap<AccountId, BlockChunkValidatorStats>,
        validator_stake: &HashMap<AccountId, Balance>,
        total_supply: Balance,
    ) -> (HashMap<AccountId, Balance>, Balance) {
        let mut res = HashMap::new();
        let num_validators = validator_block_chunk_stats.len();
        let epoch_total_reward = (U256::from(*self.max_inflation_rate.numer() as u64)
            * U256::from(total_supply)
            * U256::from(self.epoch_length)
            / (U256::from(self.num_blocks_per_year)
                * U256::from(*self.max_inflation_rate.denom() as u64)))
        .as_u128();
        let epoch_protocol_treasury = (U256::from(epoch_total_reward)
            * U256::from(*self.protocol_reward_percentage.numer() as u64)
            / U256::from(*self.protocol_reward_percentage.denom() as u64))
        .as_u128();
        res.insert(self.protocol_treasury_account.clone(), epoch_protocol_treasury);
        if num_validators == 0 {
            return (res, 0);
        }
        let epoch_validator_reward = epoch_total_reward - epoch_protocol_treasury;
        let mut epoch_actual_reward = epoch_protocol_treasury;
        let total_stake: Balance = validator_stake.values().sum();
        for (account_id, stats) in validator_block_chunk_stats {
            let reward = if stats.block_stats.expected == 0 || stats.chunk_stats.expected == 0 {
                0
            } else {
                let stake = *validator_stake
                    .get(&account_id)
                    .expect(&format!("{} is not a validator", account_id));
                // Online ratio is an average of block produced / expected and chunk produced / expected.
                (U256::from(epoch_validator_reward)
                    * U256::from(
                        stats.block_stats.produced * stats.chunk_stats.expected
                            + stats.chunk_stats.produced * stats.block_stats.expected,
                    )
                    * U256::from(stake)
                    / U256::from(stats.block_stats.expected * stats.chunk_stats.expected * 2)
                    / U256::from(total_stake))
                .as_u128()
            };
            res.insert(account_id, reward);
            epoch_actual_reward += reward;
        }
        (res, epoch_actual_reward)
    }
}

#[cfg(test)]
mod tests {
    use crate::RewardCalculator;
    use near_primitives::types::{BlockChunkValidatorStats, ValidatorStats};
    use num_rational::Rational;
    use std::collections::HashMap;

    /// Test reward calculation when validators are not fully online.
    #[test]
    fn test_reward_validator_different_online() {
        let reward_calculator = RewardCalculator {
            max_inflation_rate: Rational::new(1, 100),
            num_blocks_per_year: 1000,
            epoch_length: 1000,
            protocol_reward_percentage: Rational::new(0, 10),
            protocol_treasury_account: "near".to_string(),
            online_min_threshold: Rational::new(9, 10),
            online_max_threshold: Rational::new(9, 10),
        };
        let validator_block_chunk_stats = vec![(
            "test".to_string(),
            BlockChunkValidatorStats {
                block_stats: ValidatorStats { produced: 945, expected: 1000 },
                chunk_stats: ValidatorStats { produced: 945, expected: 1000 },
            },
        )]
        .into_iter()
        .collect::<HashMap<_, _>>();
        let validator_stake =
            vec![("test".to_string(), 500_000)].into_iter().collect::<HashMap<_, _>>();
        let total_supply = 1_000_000_000;
        let result = reward_calculator.calculate_reward(
            validator_block_chunk_stats,
            &validator_stake,
            total_supply,
        );
        assert_eq!(
            result.0,
            vec![("near".to_string(), 0), ("test".to_string(), 9_450_000u128)]
                .into_iter()
                .collect()
        );
        assert_eq!(result.1, 9_450_000u128);
    }

    /// Test that under an extreme setting (total supply 100b, epoch length half a day),
    /// reward calculation will not overflow.
    #[test]
    fn test_reward_no_overflow() {
        let reward_calculator = RewardCalculator {
            max_inflation_rate: Rational::new(5, 100),
            num_blocks_per_year: 60 * 60 * 24 * 365,
            // half a day
            epoch_length: 60 * 60 * 12,
            protocol_reward_percentage: Rational::new(1, 10),
            protocol_treasury_account: "near".to_string(),
            online_min_threshold: Rational::new(9, 10),
            online_max_threshold: Rational::new(9, 10),
        };
        let validator_block_chunk_stats = vec![(
            "test".to_string(),
            BlockChunkValidatorStats {
                block_stats: ValidatorStats { produced: 43200, expected: 43200 },
                chunk_stats: ValidatorStats { produced: 345600, expected: 345600 },
            },
        )]
        .into_iter()
        .collect::<HashMap<_, _>>();
        let validator_stake = vec![("test".to_string(), 500_000 * 10_u128.pow(24))]
            .into_iter()
            .collect::<HashMap<_, _>>();
        // some hypothetical large total supply (100b)
        let total_supply = 100_000_000_000 * 10_u128.pow(24);
        reward_calculator.calculate_reward(
            validator_block_chunk_stats,
            &validator_stake,
            total_supply,
        );
    }
}

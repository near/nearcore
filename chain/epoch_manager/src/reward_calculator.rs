use std::collections::HashMap;

use num_rational::Rational;
use primitive_types::U256;

use near_chain_configs::GenesisConfig;
use near_primitives::checked_feature;
use near_primitives::types::{AccountId, Balance, BlockChunkValidatorStats};
use near_primitives::version::{ProtocolVersion, ENABLE_INFLATION_PROTOCOL_VERSION};

pub(crate) const NUM_NS_IN_SECOND: u64 = 1_000_000_000;
pub const NUM_SECONDS_IN_A_YEAR: u64 = 24 * 60 * 60 * 365;

#[derive(Clone, Debug)]
pub struct RewardCalculator {
    pub max_inflation_rate: Rational,
    pub num_blocks_per_year: u64,
    pub epoch_length: u64,
    pub protocol_reward_rate: Rational,
    pub protocol_treasury_account: AccountId,
    pub online_min_threshold: Rational,
    pub online_max_threshold: Rational,
    pub num_seconds_per_year: u64,
}

impl RewardCalculator {
    pub fn new(config: &GenesisConfig) -> Self {
        RewardCalculator {
            max_inflation_rate: config.max_inflation_rate,
            num_blocks_per_year: config.num_blocks_per_year,
            epoch_length: config.epoch_length,
            protocol_reward_rate: config.protocol_reward_rate,
            protocol_treasury_account: config.protocol_treasury_account.to_string(),
            online_max_threshold: config.online_max_threshold,
            online_min_threshold: config.online_min_threshold,
            num_seconds_per_year: NUM_SECONDS_IN_A_YEAR,
        }
    }
    /// Calculate validator reward for an epoch based on their block and chunk production stats.
    /// Returns map of validators with their rewards and amount of newly minted tokens including to protocol's treasury.
    /// See spec https://nomicon.io/Economics/README.html#rewards-calculation
    pub fn calculate_reward(
        &self,
        validator_block_chunk_stats: HashMap<AccountId, BlockChunkValidatorStats>,
        validator_stake: &HashMap<AccountId, Balance>,
        total_supply: Balance,
        protocol_version: ProtocolVersion,
        genesis_protocol_version: ProtocolVersion,
        epoch_duration: u64,
    ) -> (HashMap<AccountId, Balance>, Balance) {
        let mut res = HashMap::new();
        let num_validators = validator_block_chunk_stats.len();
        let use_hardcoded_value = genesis_protocol_version < protocol_version
            && protocol_version >= ENABLE_INFLATION_PROTOCOL_VERSION;
        let max_inflation_rate =
            if use_hardcoded_value { Rational::new_raw(1, 20) } else { self.max_inflation_rate };
        let protocol_reward_rate =
            if use_hardcoded_value { Rational::new_raw(1, 10) } else { self.protocol_reward_rate };
        let epoch_total_reward: u128 =
            if checked_feature!("stable", RectifyInflation, protocol_version) {
                (U256::from(*max_inflation_rate.numer() as u64)
                    * U256::from(total_supply)
                    * U256::from(epoch_duration)
                    / (U256::from(self.num_seconds_per_year)
                        * U256::from(*max_inflation_rate.denom() as u64)
                        * U256::from(NUM_NS_IN_SECOND)))
                .as_u128()
            } else {
                (U256::from(*max_inflation_rate.numer() as u64)
                    * U256::from(total_supply)
                    * U256::from(self.epoch_length)
                    / (U256::from(self.num_blocks_per_year)
                        * U256::from(*max_inflation_rate.denom() as u64)))
                .as_u128()
            };
        let epoch_protocol_treasury = (U256::from(epoch_total_reward)
            * U256::from(*protocol_reward_rate.numer() as u64)
            / U256::from(*protocol_reward_rate.denom() as u64))
        .as_u128();
        res.insert(self.protocol_treasury_account.clone(), epoch_protocol_treasury);
        if num_validators == 0 {
            return (res, 0);
        }
        let epoch_validator_reward = epoch_total_reward - epoch_protocol_treasury;
        let mut epoch_actual_reward = epoch_protocol_treasury;
        let total_stake: Balance = validator_stake.values().sum();
        for (account_id, stats) in validator_block_chunk_stats {
            // Uptime is an average of block produced / expected and chunk produced / expected.
            let average_produced_numer = U256::from(
                stats.block_stats.produced * stats.chunk_stats.expected
                    + stats.chunk_stats.produced * stats.block_stats.expected,
            );
            let average_produced_denom =
                U256::from(2 * stats.chunk_stats.expected * stats.block_stats.expected);
            let online_min_numer = U256::from(*self.online_min_threshold.numer() as u64);
            let online_min_denom = U256::from(*self.online_min_threshold.denom() as u64);
            // If average of produced blocks below online min threshold, validator gets 0 reward.
            let reward = if average_produced_numer * online_min_denom
                < online_min_numer * average_produced_denom
                || stats.chunk_stats.expected == 0
                || stats.block_stats.expected == 0
            {
                0
            } else {
                let stake = *validator_stake
                    .get(&account_id)
                    .unwrap_or_else(|| panic!("{} is not a validator", account_id));
                // Online reward multiplier is min(1., (uptime - online_threshold_min) / (online_threshold_max - online_threshold_min).
                let online_max_numer = U256::from(*self.online_max_threshold.numer() as u64);
                let online_max_denom = U256::from(*self.online_max_threshold.denom() as u64);
                let online_numer =
                    online_max_numer * online_min_denom - online_min_numer * online_max_denom;
                let mut uptime_numer = (average_produced_numer * online_min_denom
                    - online_min_numer * average_produced_denom)
                    * online_max_denom;
                let uptime_denum = online_numer * average_produced_denom;
                // Apply min between 1. and computed uptime.
                uptime_numer =
                    if uptime_numer > uptime_denum { uptime_denum } else { uptime_numer };
                (U256::from(epoch_validator_reward) * uptime_numer * U256::from(stake)
                    / uptime_denum
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
    use crate::reward_calculator::NUM_NS_IN_SECOND;
    use crate::RewardCalculator;
    use near_primitives::types::{BlockChunkValidatorStats, ValidatorStats};
    use near_primitives::version::PROTOCOL_VERSION;
    use num_rational::Rational;
    use std::collections::HashMap;

    #[test]
    fn test_zero_produced_and_expected() {
        let epoch_length = 1;
        let reward_calculator = RewardCalculator {
            max_inflation_rate: Rational::new(0, 1),
            num_blocks_per_year: 1000000,
            epoch_length,
            protocol_reward_rate: Rational::new(0, 1),
            protocol_treasury_account: "near".to_string(),
            online_min_threshold: Rational::new(9, 10),
            online_max_threshold: Rational::new(1, 1),
            num_seconds_per_year: 1000000,
        };
        let validator_block_chunk_stats = vec![
            (
                "test1".to_string(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 0, expected: 0 },
                    chunk_stats: ValidatorStats { produced: 0, expected: 0 },
                },
            ),
            (
                "test2".to_string(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 0, expected: 1 },
                    chunk_stats: ValidatorStats { produced: 0, expected: 1 },
                },
            ),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();
        let validator_stake = vec![("test1".to_string(), 100), ("test2".to_string(), 100)]
            .into_iter()
            .collect::<HashMap<_, _>>();
        let total_supply = 1_000_000_000_000;
        let result = reward_calculator.calculate_reward(
            validator_block_chunk_stats,
            &validator_stake,
            total_supply,
            PROTOCOL_VERSION,
            PROTOCOL_VERSION,
            epoch_length * NUM_NS_IN_SECOND,
        );
        assert_eq!(
            result.0,
            vec![
                ("near".to_string(), 0u128),
                ("test1".to_string(), 0u128),
                ("test2".to_string(), 0u128)
            ]
            .into_iter()
            .collect::<HashMap<_, _>>()
        );
    }

    /// Test reward calculation when validators are not fully online.
    #[test]
    fn test_reward_validator_different_online() {
        let epoch_length = 1000;
        let reward_calculator = RewardCalculator {
            max_inflation_rate: Rational::new(1, 100),
            num_blocks_per_year: 1000,
            epoch_length,
            protocol_reward_rate: Rational::new(0, 10),
            protocol_treasury_account: "near".to_string(),
            online_min_threshold: Rational::new(9, 10),
            online_max_threshold: Rational::new(99, 100),
            num_seconds_per_year: 1000,
        };
        let validator_block_chunk_stats = vec![
            (
                "test1".to_string(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 945, expected: 1000 },
                    chunk_stats: ValidatorStats { produced: 945, expected: 1000 },
                },
            ),
            (
                "test2".to_string(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 999, expected: 1000 },
                    chunk_stats: ValidatorStats { produced: 999, expected: 1000 },
                },
            ),
            (
                "test3".to_string(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 850, expected: 1000 },
                    chunk_stats: ValidatorStats { produced: 850, expected: 1000 },
                },
            ),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();
        let validator_stake = vec![
            ("test1".to_string(), 500_000),
            ("test2".to_string(), 500_000),
            ("test3".to_string(), 500_000),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();
        let total_supply = 1_000_000_000;
        let result = reward_calculator.calculate_reward(
            validator_block_chunk_stats,
            &validator_stake,
            total_supply,
            PROTOCOL_VERSION,
            PROTOCOL_VERSION,
            epoch_length * NUM_NS_IN_SECOND,
        );
        // Total reward is 10_000_000. Divided by 3 equal stake validators - each gets 3_333_333.
        // test1 with 94.5% online gets 50% because of linear between (0.99-0.9) online.
        assert_eq!(
            result.0,
            vec![
                ("near".to_string(), 0),
                ("test1".to_string(), 1_666_666u128),
                ("test2".to_string(), 3_333_333u128),
                ("test3".to_string(), 0u128)
            ]
            .into_iter()
            .collect()
        );
        assert_eq!(result.1, 4_999_999u128);
    }

    /// Test that under an extreme setting (total supply 100b, epoch length half a day),
    /// reward calculation will not overflow.
    #[test]
    fn test_reward_no_overflow() {
        let epoch_length = 60 * 60 * 12;
        let reward_calculator = RewardCalculator {
            max_inflation_rate: Rational::new(5, 100),
            num_blocks_per_year: 60 * 60 * 24 * 365,
            // half a day
            epoch_length,
            protocol_reward_rate: Rational::new(1, 10),
            protocol_treasury_account: "near".to_string(),
            online_min_threshold: Rational::new(9, 10),
            online_max_threshold: Rational::new(1, 1),
            num_seconds_per_year: 60 * 60 * 24 * 365,
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
            PROTOCOL_VERSION,
            PROTOCOL_VERSION,
            epoch_length * NUM_NS_IN_SECOND,
        );
    }
}

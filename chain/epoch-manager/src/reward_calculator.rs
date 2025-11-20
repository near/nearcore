use std::collections::HashMap;

use near_primitives::epoch_manager::EpochConfig;
use near_primitives::types::{AccountId, Balance, BlockChunkValidatorStats};
use num_rational::Rational32;
use primitive_types::{U256, U512};

use crate::validator_stats::get_validator_online_ratio;

pub(crate) const NUM_NS_IN_SECOND: u64 = 1_000_000_000;
pub const NUM_SECONDS_IN_A_YEAR: u64 = 24 * 60 * 60 * 365;

/// Contains online thresholds for validators.
#[derive(Clone, Debug)]
struct ValidatorOnlineThresholds {
    /// Online minimum threshold below which validator doesn't receive reward.
    pub online_min_threshold: Rational32,
    /// Online maximum threshold above which validator gets full reward.
    pub online_max_threshold: Rational32,
    /// If set, contains a number between 0 and 100 (percentage), and endorsement ratio
    /// below this threshold will be treated 0, and otherwise be treated 1,
    /// before calculating the average uptime ratio of the validator.
    /// If not set, endorsement ratio will be used as is.
    pub endorsement_cutoff_threshold: Option<u8>,
}

/// Calculate validator reward for an epoch based on their block and chunk production stats.
/// Returns map of validators with their rewards and amount of newly minted tokens including to protocol's treasury.
/// See spec <https://nomicon.io/Economics/Economic#validator-rewards-calculation>.
pub fn calculate_reward(
    validator_block_chunk_stats: HashMap<AccountId, BlockChunkValidatorStats>,
    validator_stake: &HashMap<AccountId, Balance>,
    total_supply: Balance,
    epoch_duration_ns: u64,
    epoch_config: &EpochConfig,
) -> (HashMap<AccountId, Balance>, Balance) {
    let online_thresholds = ValidatorOnlineThresholds {
        online_min_threshold: epoch_config.online_min_threshold,
        online_max_threshold: epoch_config.online_max_threshold,
        endorsement_cutoff_threshold: Some(epoch_config.chunk_validator_only_kickout_threshold),
    };
    let mut res = HashMap::new();
    let num_validators = validator_block_chunk_stats.len();
    let epoch_total_reward = Balance::from_yoctonear(
        (U256::from(*epoch_config.max_inflation_rate.numer() as u64)
            * U256::from(total_supply.as_yoctonear())
            * U256::from(epoch_duration_ns)
            / (U256::from(NUM_SECONDS_IN_A_YEAR)
                * U256::from(*epoch_config.max_inflation_rate.denom() as u64)
                * U256::from(NUM_NS_IN_SECOND)))
        .as_u128(),
    );
    let epoch_protocol_treasury = Balance::from_yoctonear(
        (U256::from(epoch_total_reward.as_yoctonear())
            * U256::from(*epoch_config.protocol_reward_rate.numer() as u64)
            / U256::from(*epoch_config.protocol_reward_rate.denom() as u64))
        .as_u128(),
    );
    res.insert(epoch_config.protocol_treasury_account.clone(), epoch_protocol_treasury);
    if num_validators == 0 {
        return (res, Balance::ZERO);
    }
    let epoch_validator_reward = epoch_total_reward.checked_sub(epoch_protocol_treasury).unwrap();
    let mut epoch_actual_reward = epoch_protocol_treasury;
    let total_stake: Balance =
        validator_stake.values().fold(Balance::ZERO, |sum, item| sum.checked_add(*item).unwrap());
    for (account_id, stats) in validator_block_chunk_stats {
        let production_ratio =
            get_validator_online_ratio(&stats, online_thresholds.endorsement_cutoff_threshold);
        let average_produced_numer = production_ratio.numer();
        let average_produced_denom = production_ratio.denom();

        let expected_blocks = stats.block_stats.expected;
        let expected_chunks = stats.chunk_stats.expected();
        let expected_endorsements = stats.chunk_stats.endorsement_stats().expected;

        let online_min_numer = U256::from(*online_thresholds.online_min_threshold.numer() as u64);
        let online_min_denom = U256::from(*online_thresholds.online_min_threshold.denom() as u64);
        // If average of produced blocks below online min threshold, validator gets 0 reward.
        let reward = if average_produced_numer * online_min_denom
            < online_min_numer * average_produced_denom
            || (expected_chunks == 0 && expected_blocks == 0 && expected_endorsements == 0)
        {
            Balance::ZERO
        } else {
            // cspell:ignore denum
            let stake = *validator_stake
                .get(&account_id)
                .unwrap_or_else(|| panic!("{} is not a validator", account_id));
            // Online reward multiplier is min(1., (uptime - online_threshold_min) / (online_threshold_max - online_threshold_min).
            let online_max_numer =
                U256::from(*online_thresholds.online_max_threshold.numer() as u64);
            let online_max_denom =
                U256::from(*online_thresholds.online_max_threshold.denom() as u64);
            let online_numer =
                online_max_numer * online_min_denom - online_min_numer * online_max_denom;
            let mut uptime_numer = (average_produced_numer * online_min_denom
                - online_min_numer * average_produced_denom)
                * online_max_denom;
            let uptime_denum = online_numer * average_produced_denom;
            // Apply min between 1. and computed uptime.
            uptime_numer = if uptime_numer > uptime_denum { uptime_denum } else { uptime_numer };
            Balance::from_yoctonear(
                (U512::from(epoch_validator_reward.as_yoctonear())
                    * U512::from(uptime_numer)
                    * U512::from(stake.as_yoctonear())
                    / U512::from(uptime_denum)
                    / U512::from(total_stake.as_yoctonear()))
                .as_u128(),
            )
        };
        res.insert(account_id, reward);
        epoch_actual_reward = epoch_actual_reward.checked_add(reward).unwrap();
    }
    (res, epoch_actual_reward)
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_primitives::epoch_manager::EpochConfigStore;
    use near_primitives::types::{BlockChunkValidatorStats, ChunkStats, ValidatorStats};
    use num_rational::Ratio;
    use std::collections::HashMap;

    fn default_epoch_length_and_duration() -> (u64, u64) {
        (1000, NUM_NS_IN_SECOND * NUM_SECONDS_IN_A_YEAR)
    }

    fn create_test_epoch_config(
        epoch_length: u64,
        max_inflation_rate: Ratio<i32>,
        chunk_validator_only_kickout_threshold: u8,
    ) -> EpochConfig {
        let mut epoch_config = EpochConfig::minimal();
        epoch_config.epoch_length = epoch_length;
        epoch_config.max_inflation_rate = max_inflation_rate;
        epoch_config.protocol_reward_rate = Ratio::new(0, 1);
        epoch_config.online_min_threshold = Ratio::new(9, 10);
        epoch_config.online_max_threshold = Ratio::new(99, 100);
        epoch_config.chunk_validator_only_kickout_threshold =
            chunk_validator_only_kickout_threshold;

        epoch_config
    }

    #[test]
    fn test_zero_produced_and_expected() {
        let epoch_length = 1;
        let validator_block_chunk_stats = HashMap::from([
            (
                "test1".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 0, expected: 0 },
                    chunk_stats: ChunkStats::default(),
                },
            ),
            (
                "test2".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 0, expected: 1 },
                    chunk_stats: ChunkStats::new_with_production(0, 1),
                },
            ),
        ]);
        let validator_stake = HashMap::from([
            ("test1".parse().unwrap(), Balance::from_yoctonear(100)),
            ("test2".parse().unwrap(), Balance::from_yoctonear(100)),
        ]);
        let total_supply = Balance::from_yoctonear(1_000_000_000_000);
        let epoch_config = create_test_epoch_config(epoch_length, Ratio::new(0, 1), 0);
        let result = calculate_reward(
            validator_block_chunk_stats,
            &validator_stake,
            total_supply,
            epoch_length * NUM_NS_IN_SECOND,
            &epoch_config,
        );
        assert_eq!(
            result.0,
            HashMap::from([
                ("near".parse().unwrap(), Balance::ZERO),
                ("test1".parse().unwrap(), Balance::ZERO),
                ("test2".parse().unwrap(), Balance::ZERO)
            ])
        );
    }

    /// Test reward calculation when validators are not fully online.
    #[test]
    fn test_reward_validator_different_online() {
        let (epoch_length, epoch_duration) = default_epoch_length_and_duration();
        let max_inflation_rate = Ratio::new(1, 100);
        let validator_block_chunk_stats = HashMap::from([
            (
                "test1".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 945, expected: 1000 },
                    chunk_stats: ChunkStats::new_with_production(945, 1000),
                },
            ),
            (
                "test2".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 999, expected: 1000 },
                    chunk_stats: ChunkStats::new_with_production(999, 1000),
                },
            ),
            (
                "test3".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 850, expected: 1000 },
                    chunk_stats: ChunkStats::new_with_production(850, 1000),
                },
            ),
        ]);
        let validator_stake = HashMap::from([
            ("test1".parse().unwrap(), Balance::from_yoctonear(500_000)),
            ("test2".parse().unwrap(), Balance::from_yoctonear(500_000)),
            ("test3".parse().unwrap(), Balance::from_yoctonear(500_000)),
        ]);
        let total_supply = Balance::from_yoctonear(1_000_000_000);
        let epoch_config = create_test_epoch_config(epoch_length, max_inflation_rate, 0);
        let result = calculate_reward(
            validator_block_chunk_stats,
            &validator_stake,
            total_supply,
            epoch_duration,
            &epoch_config,
        );
        // Total reward is 10_000_000. Divided by 3 equal stake validators - each gets 3_333_333.
        // test1 with 94.5% online gets 50% because of linear between (0.99-0.9) online.
        assert_eq!(
            result.0,
            HashMap::from([
                ("near".parse().unwrap(), Balance::ZERO),
                ("test1".parse().unwrap(), Balance::from_yoctonear(1_666_666)),
                ("test2".parse().unwrap(), Balance::from_yoctonear(3_333_333)),
                ("test3".parse().unwrap(), Balance::ZERO)
            ])
        );
        assert_eq!(result.1, Balance::from_yoctonear(4_999_999));
    }

    /// Test reward calculation for chunk only or block only producers
    #[test]
    fn test_reward_chunk_only_producer() {
        let (epoch_length, epoch_duration) = default_epoch_length_and_duration();
        let max_inflation_rate = Ratio::new(1, 100);
        let validator_block_chunk_stats = HashMap::from([
            (
                "test1".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 945, expected: 1000 },
                    chunk_stats: ChunkStats::new_with_production(945, 1000),
                },
            ),
            // chunk only producer
            (
                "test2".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 0, expected: 0 },
                    chunk_stats: ChunkStats::new_with_production(999, 1000),
                },
            ),
            // block only producer (not implemented right now, just for testing)
            (
                "test3".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 945, expected: 1000 },
                    chunk_stats: ChunkStats::default(),
                },
            ),
            // a validator that expected blocks and chunks are both 0 (this could occur with very
            // small probability for validators with little stakes)
            (
                "test4".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 0, expected: 0 },
                    chunk_stats: ChunkStats::default(),
                },
            ),
        ]);
        let validator_stake = HashMap::from([
            ("test1".parse().unwrap(), Balance::from_yoctonear(500_000)),
            ("test2".parse().unwrap(), Balance::from_yoctonear(500_000)),
            ("test3".parse().unwrap(), Balance::from_yoctonear(500_000)),
            ("test4".parse().unwrap(), Balance::from_yoctonear(500_000)),
        ]);
        let total_supply = Balance::from_yoctonear(1_000_000_000);
        let epoch_config = create_test_epoch_config(epoch_length, max_inflation_rate, 0);
        let result = calculate_reward(
            validator_block_chunk_stats,
            &validator_stake,
            total_supply,
            epoch_duration,
            &epoch_config,
        );
        // Total reward is 10_000_000. Divided by 4 equal stake validators - each gets 2_500_000.
        // test1 with 94.5% online gets 50% because of linear between (0.99-0.9) online.
        {
            assert_eq!(
                result.0,
                HashMap::from([
                    ("near".parse().unwrap(), Balance::ZERO),
                    ("test1".parse().unwrap(), Balance::from_yoctonear(1_250_000)),
                    ("test2".parse().unwrap(), Balance::from_yoctonear(2_500_000)),
                    ("test3".parse().unwrap(), Balance::from_yoctonear(1_250_000)),
                    ("test4".parse().unwrap(), Balance::ZERO)
                ])
            );
            assert_eq!(result.1, Balance::from_yoctonear(5_000_000));
        }
    }

    #[test]
    fn test_reward_stateless_validation() {
        let (epoch_length, epoch_duration) = default_epoch_length_and_duration();
        let max_inflation_rate = Ratio::new(1, 100);
        let validator_block_chunk_stats = HashMap::from([
            // Blocks, chunks, endorsements
            (
                "test1".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 945, expected: 1000 },
                    chunk_stats: ChunkStats {
                        production: ValidatorStats { produced: 944, expected: 1000 },
                        endorsement: ValidatorStats { produced: 946, expected: 1000 },
                    },
                },
            ),
            // Chunks and endorsements
            (
                "test2".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 0, expected: 0 },
                    chunk_stats: ChunkStats {
                        production: ValidatorStats { produced: 998, expected: 1000 },
                        endorsement: ValidatorStats { produced: 1000, expected: 1000 },
                    },
                },
            ),
            // Blocks and endorsements
            (
                "test3".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 940, expected: 1000 },
                    chunk_stats: ChunkStats::new_with_endorsement(950, 1000),
                },
            ),
            // Endorsements only
            (
                "test4".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 0, expected: 0 },
                    chunk_stats: ChunkStats::new_with_endorsement(1000, 1000),
                },
            ),
        ]);
        let validator_stake = HashMap::from([
            ("test1".parse().unwrap(), Balance::from_yoctonear(500_000)),
            ("test2".parse().unwrap(), Balance::from_yoctonear(500_000)),
            ("test3".parse().unwrap(), Balance::from_yoctonear(500_000)),
            ("test4".parse().unwrap(), Balance::from_yoctonear(500_000)),
        ]);
        let total_supply = Balance::from_yoctonear(1_000_000_000);
        let epoch_config = create_test_epoch_config(epoch_length, max_inflation_rate, 0);
        let result = calculate_reward(
            validator_block_chunk_stats,
            &validator_stake,
            total_supply,
            epoch_duration,
            &epoch_config,
        );
        // Total reward is 10_000_000. Divided by 4 equal stake validators - each gets 2_500_000.
        {
            assert_eq!(
                result.0,
                HashMap::from([
                    ("near".parse().unwrap(), Balance::ZERO),
                    ("test1".parse().unwrap(), Balance::from_yoctonear(1_750_000)),
                    ("test2".parse().unwrap(), Balance::from_yoctonear(2_500_000)),
                    ("test3".parse().unwrap(), Balance::from_yoctonear(1_944_444)),
                    ("test4".parse().unwrap(), Balance::from_yoctonear(2_500_000))
                ])
            );
            assert_eq!(result.1, Balance::from_yoctonear(8_694_444));
        }
    }

    #[test]
    fn test_reward_stateless_validation_with_endorsement_cutoff() {
        let (epoch_length, epoch_duration) = default_epoch_length_and_duration();
        let max_inflation_rate = Ratio::new(1, 100);
        let validator_block_chunk_stats = HashMap::from([
            // Blocks, chunks, endorsements - endorsement ratio cutoff is exceeded
            (
                "test1".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 945, expected: 1000 },
                    chunk_stats: ChunkStats {
                        production: ValidatorStats { produced: 944, expected: 1000 },
                        endorsement: ValidatorStats { produced: 946, expected: 1000 },
                    },
                },
            ),
            // Blocks, chunks, endorsements - endorsement ratio cutoff is not exceeded
            (
                "test2".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 945, expected: 1000 },
                    chunk_stats: ChunkStats {
                        production: ValidatorStats { produced: 944, expected: 1000 },
                        endorsement: ValidatorStats { produced: 446, expected: 1000 },
                    },
                },
            ),
            // Endorsements only - endorsement ratio cutoff is exceeded
            (
                "test3".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 0, expected: 0 },
                    chunk_stats: ChunkStats::new_with_endorsement(946, 1000),
                },
            ),
            // Endorsements only - endorsement ratio cutoff is not exceeded
            (
                "test4".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 0, expected: 0 },
                    chunk_stats: ChunkStats::new_with_endorsement(446, 1000),
                },
            ),
        ]);
        let validator_stake = HashMap::from([
            ("test1".parse().unwrap(), Balance::from_yoctonear(500_000)),
            ("test2".parse().unwrap(), Balance::from_yoctonear(500_000)),
            ("test3".parse().unwrap(), Balance::from_yoctonear(500_000)),
            ("test4".parse().unwrap(), Balance::from_yoctonear(500_000)),
        ]);
        let total_supply = Balance::from_yoctonear(1_000_000_000);
        let epoch_config = create_test_epoch_config(epoch_length, max_inflation_rate, 50);
        let result = calculate_reward(
            validator_block_chunk_stats,
            &validator_stake,
            total_supply,
            epoch_duration,
            &epoch_config,
        );
        // "test2" does not get reward since its uptime ratio goes below online_min_threshold,
        // because its endorsement ratio is below the cutoff threshold.
        // "test4" does not get reward since its endorsement ratio is below the cutoff threshold.
        {
            assert_eq!(
                result.0,
                HashMap::from([
                    ("near".parse().unwrap(), Balance::ZERO),
                    ("test1".parse().unwrap(), Balance::from_yoctonear(1_750_000)),
                    ("test2".parse().unwrap(), Balance::ZERO),
                    ("test3".parse().unwrap(), Balance::from_yoctonear(2_500_000)),
                    ("test4".parse().unwrap(), Balance::ZERO)
                ])
            );
            assert_eq!(result.1, Balance::from_yoctonear(4_250_000));
        }
    }

    /// Test that under an extreme setting (total supply 100b, epoch length half a day),
    /// reward calculation will not overflow.
    #[test]
    fn test_reward_no_overflow() {
        let epoch_length = 60 * 60 * 12;
        let validator_block_chunk_stats = HashMap::from([(
            "test".parse().unwrap(),
            BlockChunkValidatorStats {
                block_stats: ValidatorStats { produced: 43200, expected: 43200 },
                chunk_stats: ChunkStats {
                    production: ValidatorStats { produced: 345600, expected: 345600 },
                    endorsement: ValidatorStats { produced: 345600, expected: 345600 },
                },
            },
        )]);
        let validator_stake =
            HashMap::from([("test".parse().unwrap(), Balance::from_near(500_000))]);
        // some hypothetical large total supply (100b)
        let total_supply = Balance::from_near(100_000_000_000);
        let epoch_duration = epoch_length * NUM_NS_IN_SECOND;
        let epoch_config = create_test_epoch_config(epoch_length, Ratio::new(1, 40), 0);
        calculate_reward(
            validator_block_chunk_stats,
            &validator_stake,
            total_supply,
            epoch_duration,
            &epoch_config,
        );
    }

    #[test]
    fn test_adjust_max_inflation() {
        let account_id: AccountId = "test1".parse().unwrap();
        let validator_stake = HashMap::from([(account_id.clone(), Balance::from_near(100))]);
        let total_supply = Balance::from_near(1_000_000_000);

        // Expected reward for protocol version 80, calculated using the reward formula:
        // epoch_total_reward = max_inflation_rate * total_supply * epoch_duration_ns / NUM_SECONDS_IN_A_YEAR / NUM_NS_IN_SECOND
        // With parameters:
        // - total_supply = 1_000_000_000 NEAR = 1_000_000_000_000_000_000_000_000_000 yoctoNEAR
        // - epoch_duration_ns = NUM_NS_IN_SECOND = 1_000_000_000 (1 second)
        // - max_inflation_rate = 1/20 = 0.05 (for protocol version 80)
        // - NUM_SECONDS_IN_A_YEAR = 31_536_000
        // Result: 0.05 * 1_000_000_000_000_000_000_000_000_000 * 1_000_000_000 / 31_536_000 / 1_000_000_000
        //       = 1_585_489_599_188_229_325_215_626 yoctoNEAR
        let reward = Balance::from_yoctonear(1_585_489_599_188_229_325_215_626);

        // Check rewards match the expected protocol version schedule.
        for chain_id in ["mainnet", "testnet"] {
            let epoch_configs = EpochConfigStore::for_chain_id(chain_id, None).unwrap();
            for (protocol_version, expected_total) in [
                // Prior to inflation reduction
                (80, reward),
                // After inflation reduction
                (81, reward.saturating_div(2)),
            ] {
                let epoch_config = epoch_configs.get_config(protocol_version);
                let validator_block_chunk_stats = HashMap::from([(
                    account_id.clone(),
                    BlockChunkValidatorStats {
                        block_stats: ValidatorStats { produced: 1, expected: 1 },
                        chunk_stats: ChunkStats::default(),
                    },
                )]);
                let epoch_duration = NUM_NS_IN_SECOND;
                let (rewards, total) = calculate_reward(
                    validator_block_chunk_stats,
                    &validator_stake,
                    total_supply,
                    epoch_duration,
                    &epoch_config,
                );
                assert_eq!(expected_total, total);
                let expected_protocol_reward = expected_total.checked_div(10).unwrap();
                let expected_validator_reward =
                    expected_total.checked_sub(expected_protocol_reward).unwrap();
                assert_eq!(
                    Some(&expected_protocol_reward),
                    rewards.get(&epoch_config.protocol_treasury_account),
                );
                assert_eq!(Some(&expected_validator_reward), rewards.get(&account_id));
            }
        }
    }
}

use std::cmp::max;
use std::collections::HashMap;

use near_primitives::types::{AccountId, Balance, BlockChunkValidatorStats};

#[derive(Clone)]
pub struct RewardCalculator {
    pub max_inflation_rate: u8,
    pub num_blocks_per_year: u64,
    pub epoch_length: u64,
    pub validator_reward_percentage: u8,
    pub protocol_reward_percentage: u8,
    pub protocol_treasury_account: AccountId,
}

impl RewardCalculator {
    /// Calculate validator reward for an epoch based on their block and chunk production stats.
    pub fn calculate_reward(
        &self,
        validator_block_chunk_stats: HashMap<AccountId, BlockChunkValidatorStats>,
        total_storage_rent: Balance,
        total_validator_reward: Balance,
        total_supply: Balance,
    ) -> (HashMap<AccountId, Balance>, Balance) {
        let mut res = HashMap::new();
        let num_validators = validator_block_chunk_stats.len();
        let max_inflation =
            u128::from(self.max_inflation_rate) * total_supply * u128::from(self.epoch_length)
                / (100 * u128::from(self.num_blocks_per_year));
        let epoch_fee = total_validator_reward + total_storage_rent;
        let inflation = if max_inflation > epoch_fee { max_inflation - epoch_fee } else { 0 };
        let epoch_total_reward = max(max_inflation, epoch_fee);
        let epoch_protocol_treasury =
            epoch_total_reward * u128::from(self.protocol_reward_percentage) / 100;
        res.insert(self.protocol_treasury_account.clone(), epoch_protocol_treasury);
        if num_validators == 0 {
            return (res, inflation);
        }
        let epoch_per_validator_reward =
            (epoch_total_reward - epoch_protocol_treasury) / num_validators as u128;
        for (account_id, stats) in validator_block_chunk_stats {
            let reward = if stats.block_stats.expected == 0 || stats.chunk_stats.expected == 0 {
                0
            } else {
                // Online ratio is an average of block produced / expected and chunk produced / expected.
                epoch_per_validator_reward
                    * u128::from(
                        stats.block_stats.produced * stats.chunk_stats.expected
                            + stats.chunk_stats.produced * stats.block_stats.expected,
                    )
                    / u128::from(stats.block_stats.expected * stats.chunk_stats.expected * 2)
            };
            res.insert(account_id, reward);
        }
        (res, inflation)
    }
}

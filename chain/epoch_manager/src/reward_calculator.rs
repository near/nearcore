use near_primitives::types::{AccountId, Balance, GasUsage};
use std::cmp::max;
use std::collections::HashMap;

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
    /// Calculate validator reward for an epoch based on their online ratio
    pub fn calculate_reward(
        &self,
        validator_online_ratio: HashMap<AccountId, (u64, u64)>,
        total_gas_used: GasUsage,
        gas_price: Balance,
        total_supply: Balance,
    ) -> HashMap<AccountId, Balance> {
        let mut res = HashMap::new();
        let num_validators = validator_online_ratio.len();
        let total_tx_fee = gas_price * total_gas_used as u128;
        let max_inflation =
            self.max_inflation_rate as u128 * total_supply * self.epoch_length as u128
                / (100 * self.num_blocks_per_year as u128);
        let epoch_total_reward =
            max(max_inflation, self.validator_reward_percentage as u128 * total_tx_fee / 100);
        let epoch_protocol_treasury =
            epoch_total_reward * self.protocol_reward_percentage as u128 / 100;
        res.insert(self.protocol_treasury_account.clone(), epoch_protocol_treasury);
        let epoch_per_validator_reward =
            (epoch_total_reward - epoch_protocol_treasury) / num_validators as u128;
        for (account_id, (num_blocks, expected_num_blocks)) in validator_online_ratio {
            let reward = if expected_num_blocks == 0 {
                0
            } else {
                epoch_per_validator_reward * num_blocks as u128 / expected_num_blocks as u128
            };
            res.insert(account_id, reward);
        }
        res
    }
}

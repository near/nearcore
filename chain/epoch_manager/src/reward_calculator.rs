use near_primitives::types::{AccountId, Balance, Gas};
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
        total_gas_used: Gas,
        gas_price: Balance,
        total_storage_rent: Balance,
        total_supply: Balance,
    ) -> (HashMap<AccountId, Balance>, Balance) {
        let mut res = HashMap::new();
        let num_validators = validator_online_ratio.len();
        let max_inflation =
            u128::from(self.max_inflation_rate) * total_supply * u128::from(self.epoch_length)
                / (100 * u128::from(self.num_blocks_per_year));
        let total_tx_fee = gas_price * u128::from(total_gas_used);
        let epoch_fee =
            u128::from(self.validator_reward_percentage) * total_tx_fee / 100 + total_storage_rent;
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
        for (account_id, (num_blocks, expected_num_blocks)) in validator_online_ratio {
            let reward = if expected_num_blocks == 0 {
                0
            } else {
                epoch_per_validator_reward * u128::from(num_blocks)
                    / u128::from(expected_num_blocks)
            };
            res.insert(account_id, reward);
        }
        (res, inflation)
    }
}

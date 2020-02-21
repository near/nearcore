use ethereum_types::U256;
use std::cmp::max;
use std::collections::HashMap;

use crate::types::SlashState;
use near_primitives::types::{AccountId, Balance, BlockChunkValidatorStats};
use near_primitives::utils::median;

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
        validator_stake: &HashMap<AccountId, Balance>,
        slashed: &HashMap<AccountId, SlashState>,
        total_storage_rent: Balance,
        total_validator_reward: Balance,
        total_supply: Balance,
    ) -> (HashMap<AccountId, Balance>, Balance) {
        let mut res = HashMap::new();
        let num_validators = validator_block_chunk_stats.len();
        let max_inflation = (U256::from(self.max_inflation_rate)
            * U256::from(total_supply)
            * U256::from(self.epoch_length)
            / (U256::from(100) * U256::from(self.num_blocks_per_year)))
        .as_u128();
        let epoch_fee = total_validator_reward + total_storage_rent;
        let inflation = if max_inflation > epoch_fee { max_inflation - epoch_fee } else { 0 };
        let epoch_total_reward = max(max_inflation, epoch_fee);
        let epoch_protocol_treasury =
            epoch_total_reward * u128::from(self.protocol_reward_percentage) / 100;
        res.insert(self.protocol_treasury_account.clone(), epoch_protocol_treasury);
        if num_validators == 0 {
            return (res, inflation);
        }
        let epoch_validator_reward = epoch_total_reward - epoch_protocol_treasury;
        let total_stake: Balance = validator_stake.values().sum();
        let mut blocks_produced = vec![];
        let mut chunks_produced = vec![];
        for (account_id, stats) in validator_block_chunk_stats.iter() {
            if !slashed.contains_key(account_id) {
                blocks_produced.push(stats.blocks_produced);
                chunks_produced.push(stats.chunks_produced);
            }
        }
        if !blocks_produced.is_empty() {
            assert!(!chunks_produced.is_empty());
            let blocks_produced_median = median(blocks_produced);
            let chunks_produced_median = median(chunks_produced);
            for (account_id, stats) in validator_block_chunk_stats {
                let stake = *validator_stake
                    .get(&account_id)
                    .expect(&format!("{} is not a validator", account_id));
                // Online ratio is an average of block produced / block produced median and chunk produced / chunk produced median.
                let mut div = blocks_produced_median * chunks_produced_median * 2;
                if div == 0 {
                    div = 1;
                }
                let reward = (U256::from(epoch_validator_reward)
                    * U256::from(
                        // TODO Alex Illia check the formula
                        stats.blocks_produced * chunks_produced_median
                            + stats.chunks_produced * blocks_produced_median,
                    )
                    * U256::from(stake)
                    / U256::from(div)
                    / U256::from(total_stake))
                .as_u128();
                res.insert(account_id, reward);
            }
        }
        (res, inflation)
    }
}

#[cfg(test)]
mod tests {
    use crate::RewardCalculator;
    use near_primitives::types::BlockChunkValidatorStats;
    use std::collections::HashMap;

    /// Test that under an extreme setting (total supply 100b, epoch length half a day),
    /// reward calculation will not overflow.
    #[test]
    fn test_reward_no_overflow() {
        let reward_calculator = RewardCalculator {
            max_inflation_rate: 5,
            num_blocks_per_year: 60 * 60 * 24 * 365,
            // half a day
            epoch_length: 60 * 60 * 12,
            validator_reward_percentage: 30,
            protocol_reward_percentage: 10,
            protocol_treasury_account: "near".to_string(),
        };
        let validator_block_chunk_stats = vec![(
            "test".to_string(),
            BlockChunkValidatorStats { blocks_produced: 43200, chunks_produced: 345600 },
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
            &HashMap::new(),
            0,
            10_u128.pow(24),
            total_supply,
        );
    }
}

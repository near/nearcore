use std::cmp::{max, min};

use primitives::types::{Balance, BlockIndex, Gas, Mana};

// Transaction Stakes structs
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TxStakeConfig {
    /// Common denumerator for mana accounting
    pub mana_common_denum: u64,
    /// How much mana you get for a single coin of stake (numerator).
    pub mana_per_coin_num: u64,
    /// Regeneration rate numerator of mana per block per coin of stake.
    /// Mana is regenerated only after gas is fully regenerated.
    pub mana_regen_per_block_per_coin_num: u64,

    /// Regeneration rate of gas per block per coin of stake.
    pub gas_regen_per_block_per_coin: Gas,
}

impl Default for TxStakeConfig {
    fn default() -> TxStakeConfig {
        TxStakeConfig {
            mana_common_denum: 1_000,
            /// Default is 10 mana per coin
            mana_per_coin_num: 10_000,
            /// Full mana regeneration within T blocks is:
            ///     mana_per_coin_num / T
            /// We use default 20 blocks for full regeneration
            mana_regen_per_block_per_coin_num: 10_000 / 20,
            /// For cheap transaction gas shouldn't be an issue, but for
            /// expensive we should count full gas limit as a few mana points
            /// Let's say full gas limit is 5 mana points
            /// If the full gas limit is 100K then equivalent of 1 mana is 20K
            /// We regenerate 10 mana per 20 blocks, it's 0.5 mana per block
            /// Which results in 0.5 * 20K = 10K gas per block per coin.
            gas_regen_per_block_per_coin: 10_000,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TxTotalStake {
    /// Used Mana numerator. Denumerator is common per config.
    mana_used_num: u64,
    /// Total amount of gas used. Without denumerator
    gas_used: Gas,
    /// Last update block index is used for mana and gas regeneration.
    /// Whenever we touch TxTotalStake we should use difference between the
    /// current and the last update block indices to recalculate and update
    /// mana_used and gas_used counters based on the current total_stake and
    /// the number of blocks from the last update.
    last_update_block_index: BlockIndex,
    total_active_stake: Balance,
    total_stake: Balance,
}

#[allow(unused)]
impl TxTotalStake {
    pub fn new(block_index: BlockIndex) -> TxTotalStake {
        TxTotalStake {
            mana_used_num: 0,
            gas_used: 0,
            last_update_block_index: block_index,
            total_active_stake: 0,
            total_stake: 0,
        }
    }

    /// Updates usage values and regenerates used mana and gas.
    /// Should always be called before modifying the stakes.
    pub fn update(&mut self, block_index: BlockIndex, config: &TxStakeConfig) {
        assert!(self.last_update_block_index <= block_index);
        if self.last_update_block_index == block_index {
            return;
        }
        let mut blocks_difference = block_index - self.last_update_block_index;
        self.last_update_block_index = block_index;
        if self.total_stake == 0 {
            return;
        }
        if self.gas_used > 0 {
            // Regenerating gas
            let gas_regen_rate = config.gas_regen_per_block_per_coin * self.total_stake;
            let blocks_needed = (self.gas_used + gas_regen_rate - 1) / gas_regen_rate;
            let blocks_regenerated = min(blocks_difference, blocks_needed);
            self.gas_used -= min(self.gas_used, blocks_regenerated * gas_regen_rate);
            blocks_difference -= blocks_regenerated;
        }
        if blocks_difference > 0 && self.mana_used_num > 0 {
            // Regenerating mana
            let mana_regen_rate_num = config.mana_regen_per_block_per_coin_num * self.total_stake;
            let blocks_needed =
                (self.mana_used_num + mana_regen_rate_num - 1) / mana_regen_rate_num;
            let blocks_regenerated = min(blocks_difference, blocks_needed);
            self.mana_used_num -= min(self.mana_used_num, blocks_regenerated * mana_regen_rate_num);
        }
    }

    /// Returns the available mana using the data from the last update.
    /// Make sure to update using the latest block index before calling it.
    pub fn available_mana(&self, config: &TxStakeConfig) -> Mana {
        // NEED to know the current block ID to add regeneration
        let mut mana_num = self.total_active_stake * config.mana_per_coin_num;
        mana_num -= self.mana_used_num;
        min(max(mana_num / config.mana_common_denum, 0), Mana::max_value().into()) as u32
    }

    pub fn charge_mana(&mut self, mana: Mana, config: &TxStakeConfig) {
        self.mana_used_num += u64::from(mana) * config.mana_common_denum;
    }

    pub fn refund_mana_and_charge_gas(
        &mut self,
        mana_refund: Mana,
        gas_used: Gas,
        config: &TxStakeConfig,
    ) {
        let mana_refund_num = u64::from(mana_refund) * config.mana_common_denum;
        if mana_refund_num >= self.mana_used_num {
            self.mana_used_num = 0
        } else {
            self.mana_used_num -= mana_refund_num;
        }
        self.gas_used += gas_used;
    }

    pub fn add_active_stake(&mut self, stake: Balance) {
        self.total_active_stake += stake;
        self.total_stake += stake;
    }
}

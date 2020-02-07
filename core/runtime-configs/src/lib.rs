//! Settings of the parameters of the runtime.
use serde::{Deserialize, Serialize};

use near_primitives::serialize::u128_dec_format;
use near_primitives::types::{Balance, NumBlocks};
use near_runtime_fees::RuntimeFeesConfig;
use near_vm_logic::VMConfig;

/// The structure that holds the parameters of the runtime, mostly economics.
#[derive(Default, Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct RuntimeConfig {
    /// The cost to store one byte of storage per block.
    #[serde(with = "u128_dec_format")]
    pub storage_cost_byte_per_block: Balance,
    /// The minimum number of blocks of storage rent an account has to maintain to prevent forced deletion.
    pub poke_threshold: NumBlocks,
    /// Costs of different actions that need to be performed when sending and processing transaction
    /// and receipts.
    pub transaction_costs: RuntimeFeesConfig,
    /// Config of wasm operations.
    pub wasm_config: VMConfig,
    /// The baseline cost to store account_id of short length per block.
    /// The original formula in NEP#0006 is `1,000 / (3 ^ (account_id.length - 2))` for cost per year.
    /// This value represents `1,000` above adjusted to use per block.
    #[serde(with = "u128_dec_format")]
    pub account_length_baseline_cost_per_block: Balance,
}

impl RuntimeConfig {
    pub fn free() -> Self {
        Self {
            storage_cost_byte_per_block: 0,
            poke_threshold: 0,
            transaction_costs: RuntimeFeesConfig::free(),
            wasm_config: VMConfig::free(),
            account_length_baseline_cost_per_block: 0,
        }
    }
}

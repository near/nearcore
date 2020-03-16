//! Settings of the parameters of the runtime.
use serde::{Deserialize, Serialize};

use near_primitives::serialize::u128_dec_format;
use near_primitives::types::Balance;
use near_runtime_fees::RuntimeFeesConfig;
use near_vm_logic::VMConfig;

/// The structure that holds the parameters of the runtime, mostly economics.
#[derive(Default, Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct RuntimeConfig {
    /// The required amount of account per byte.
    #[serde(with = "u128_dec_format")]
    pub storage_amount_per_byte: Balance,
    /// Costs of different actions that need to be performed when sending and processing transaction
    /// and receipts.
    pub transaction_costs: RuntimeFeesConfig,
    /// Config of wasm operations.
    pub wasm_config: VMConfig,
    /// The baseline cost to store account_id of short length per block.
    /// The original formula in NEP#0006 is `1,000 / (3 ^ (account_id.length - 2))` for cost per year.
    /// This value represents `1,000` above adjusted to use per block.
    #[serde(with = "u128_dec_format")]
    pub account_length_baseline_cost: Balance,
}

impl RuntimeConfig {
    pub fn free() -> Self {
        Self {
            storage_amount_per_byte: 0,
            transaction_costs: RuntimeFeesConfig::free(),
            wasm_config: VMConfig::free(),
            account_length_baseline_cost: 0,
        }
    }
}

//! Settings of the parameters of the runtime.
use serde::{Deserialize, Serialize};

use near_primitives::serialize::u128_dec_format;
use near_primitives::types::Balance;
use near_runtime_fees::RuntimeFeesConfig;
use near_vm_logic::VMConfig;

/// The structure that holds the parameters of the runtime, mostly economics.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct RuntimeConfig {
    /// Amount of yN per byte required to have on the account.
    /// See https://nomicon.io/Economics/README.html#state-stake for details.
    #[serde(with = "u128_dec_format")]
    pub storage_amount_per_byte: Balance,
    /// Costs of different actions that need to be performed when sending and processing transaction
    /// and receipts.
    pub transaction_costs: RuntimeFeesConfig,
    /// Config of wasm operations.
    pub wasm_config: VMConfig,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        RuntimeConfig {
            // See https://nomicon.io/Economics/README.html#general-variables for how it was calculated.
            storage_amount_per_byte: 909 * 100_000_000_000_000_000,
            transaction_costs: RuntimeFeesConfig::default(),
            wasm_config: VMConfig::default(),
        }
    }
}

impl RuntimeConfig {
    pub fn free() -> Self {
        Self {
            storage_amount_per_byte: 0,
            transaction_costs: RuntimeFeesConfig::free(),
            wasm_config: VMConfig::free(),
        }
    }
}

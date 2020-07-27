//! Settings of the parameters of the runtime.
use serde::{Deserialize, Serialize};

use near_primitives::serialize::u128_dec_format;
use near_primitives::types::{AccountId, Balance};
use near_runtime_fees::RuntimeFeesConfig;
use near_vm_logic::VMConfig;

/// The structure that holds the parameters of the runtime, mostly economics.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
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
    /// Config that defines rules for account creation.
    pub account_creation_config: AccountCreationConfig,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        RuntimeConfig {
            // See https://nomicon.io/Economics/README.html#general-variables for how it was calculated.
            storage_amount_per_byte: 909 * 100_000_000_000_000_000,
            transaction_costs: RuntimeFeesConfig::default(),
            wasm_config: VMConfig::default(),
            account_creation_config: AccountCreationConfig::default(),
        }
    }
}

impl RuntimeConfig {
    pub fn free() -> Self {
        Self {
            storage_amount_per_byte: 0,
            transaction_costs: RuntimeFeesConfig::free(),
            wasm_config: VMConfig::free(),
            account_creation_config: AccountCreationConfig::default(),
        }
    }
}

/// The structure describes configuration for creation of new accounts.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct AccountCreationConfig {
    /// The minimum length of the top-level account ID that is allowed to be created by any account.
    pub min_allowed_top_level_account_length: u8,
    /// The account ID of the account registrar. This account ID allowed to create top-level
    /// accounts of any valid length.
    pub registrar_account_id: AccountId,
}

impl Default for AccountCreationConfig {
    fn default() -> Self {
        Self {
            min_allowed_top_level_account_length: 0,
            registrar_account_id: AccountId::from("registrar"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_prepaid_gas() {
        let config = RuntimeConfig::default();
        assert!(
            config.wasm_config.limit_config.max_total_prepaid_gas
                / config.transaction_costs.min_receipt_with_function_call_gas()
                <= 63,
            "The maximum desired depth of receipts should be at most 63"
        );
    }
}

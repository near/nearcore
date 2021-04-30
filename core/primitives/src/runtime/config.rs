//! Settings of the parameters of the runtime.
use serde::{Deserialize, Serialize};

use crate::checked_feature;
use crate::config::VMConfig;
use crate::runtime::fees::RuntimeFeesConfig;
use crate::serialize::u128_dec_format;
use crate::types::{AccountId, Balance};
use crate::version::ProtocolVersion;
use std::sync::{Arc, Mutex};

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

lazy_static::lazy_static! {
    static ref LOWER_STORAGE_COST_CONFIG: Mutex<Option<Arc<RuntimeConfig>>> = Mutex::new(None);
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

    /// Returns a `RuntimeConfig` for the corresponding protocol version.
    /// It uses `genesis_runtime_config` to identify the original
    /// config and `protocol_version` for the current protocol version.
    pub fn from_protocol_version(
        genesis_runtime_config: &Arc<RuntimeConfig>,
        protocol_version: ProtocolVersion,
    ) -> Arc<Self> {
        if checked_feature!("stable", LowerStorageCost, protocol_version) {
            let mut config = LOWER_STORAGE_COST_CONFIG.lock().unwrap();
            config
                .get_or_insert_with(|| Arc::new(genesis_runtime_config.decrease_storage_cost()))
                .clone()
        } else {
            genesis_runtime_config.clone()
        }
    }

    /// Returns a new config with decreased storage cost.
    fn decrease_storage_cost(&self) -> Self {
        let mut config = self.clone();
        config.storage_amount_per_byte = 10u128.pow(19);
        config
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

    #[test]
    fn test_lower_cost() {
        let config = Arc::new(RuntimeConfig::default());
        let config_same = RuntimeConfig::from_protocol_version(&config, 0);
        assert_eq!(
            config_same.as_ref().storage_amount_per_byte,
            config.as_ref().storage_amount_per_byte
        );
        let config_lower = RuntimeConfig::from_protocol_version(&config, ProtocolVersion::MAX);
        assert!(
            config_lower.as_ref().storage_amount_per_byte < config.as_ref().storage_amount_per_byte
        );
    }
}

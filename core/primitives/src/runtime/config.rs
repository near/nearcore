//! Settings of the parameters of the runtime.
use serde::{Deserialize, Serialize};

use crate::checked_feature;
use crate::config::VMConfig;
use crate::runtime::fees::RuntimeFeesConfig;
use crate::serialize::u128_dec_format;
use crate::types::{AccountId, Balance, Gas};
use crate::version::ProtocolVersion;
use std::sync::Arc;

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

/// An actual runtime configuration the node will use.
///
/// This is constructed from the runtime configuration given in the genesis file
/// but i) may have it’s `max_gas_burnt_view` limit adjusted and ii) provides
/// a method which returns configuration with adjustments done through protocol
/// version upgrades.
pub struct ActualRuntimeConfig {
    /// The runtime configuration taken from the genesis file but with possibly
    /// modified `max_gas_burnt_view` limit.
    runtime_config: Arc<RuntimeConfig>,

    /// The runtime configuration with lower storage cost adjustment applied.
    with_lower_storage_cost: Arc<RuntimeConfig>,
}

impl ActualRuntimeConfig {
    /// Constructs a new object from specified genesis runtime config.
    ///
    /// If `max_gas_burnt_view` is provided, the property in wasm limit
    /// configuration will be adjusted to given value.
    pub fn new(genesis_runtime_config: RuntimeConfig, max_gas_burnt_view: Option<Gas>) -> Self {
        let mut config = genesis_runtime_config;
        if let Some(gas) = max_gas_burnt_view {
            config.wasm_config.limit_config.max_gas_burnt_view = gas;
        }
        let runtime_config = Arc::new(config.clone());

        // Adjust as per LowerStorageCost protocol feature.
        config.storage_amount_per_byte = 10u128.pow(19);
        let with_lower_storage_cost = Arc::new(config);

        Self { runtime_config, with_lower_storage_cost }
    }

    /// Returns a `RuntimeConfig` for the corresponding protocol version.
    ///
    /// Note that even if some old version is given as the argument, this may
    /// still return configuration which differs from configuration found in
    /// genesis file by the `max_gas_burnt_view` limit.
    pub fn for_protocol_version(&self, protocol_version: ProtocolVersion) -> &Arc<RuntimeConfig> {
        if checked_feature!("stable", LowerStorageCost, protocol_version) {
            &self.with_lower_storage_cost
        } else {
            &self.runtime_config
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
            registrar_account_id: "registrar".parse().unwrap(),
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
        let config = RuntimeConfig::default();
        let default_amount = config.storage_amount_per_byte;
        let config = ActualRuntimeConfig::new(config, None);
        let base_cfg = config.for_protocol_version(0);
        let new_cfg = config.for_protocol_version(ProtocolVersion::MAX);
        assert_eq!(default_amount, base_cfg.storage_amount_per_byte);
        assert!(default_amount > new_cfg.storage_amount_per_byte);
    }

    #[test]
    fn test_max_gas_burnt_view() {
        let config = ActualRuntimeConfig::new(RuntimeConfig::default(), Some(42));
        assert_eq!(42, config.for_protocol_version(0).wasm_config.limit_config.max_gas_burnt_view);
    }
}

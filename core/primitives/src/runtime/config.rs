//! Settings of the parameters of the runtime.
use serde::{Deserialize, Serialize};

use crate::checked_feature;
use crate::config::VMConfig;
use crate::runtime::fees::RuntimeFeesConfig;
use crate::serialize::u128_dec_format;
use crate::types::{AccountId, Balance};
use crate::version::ProtocolVersion;
#[cfg(feature = "protocol_feature_add_account_versions")]
use std::collections::HashMap;
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
    /// Storage usage delta that need to be added on migration of account from v1 to v2
    /// See https://github.com/near/nearcore/issues/3824
    #[serde(default = "RuntimeConfig::empty_storage_usage_delta")]
    #[serde(skip)]
    #[cfg(feature = "protocol_feature_add_account_versions")]
    pub storage_usage_delta: &'static HashMap<AccountId, u64>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        RuntimeConfig {
            // See https://nomicon.io/Economics/README.html#general-variables for how it was calculated.
            storage_amount_per_byte: 909 * 100_000_000_000_000_000,
            transaction_costs: RuntimeFeesConfig::default(),
            wasm_config: VMConfig::default(),
            account_creation_config: AccountCreationConfig::default(),
            #[cfg(feature = "protocol_feature_add_account_versions")]
            storage_usage_delta: &EMPTY_STORAGE_USAGE_DELTA,
        }
    }
}

lazy_static::lazy_static! {
    static ref LOWER_STORAGE_COST_CONFIG: Mutex<Option<Arc<RuntimeConfig>>> = Mutex::new(None);
}

#[cfg(feature = "protocol_feature_add_account_versions")]
lazy_static::lazy_static! {
    static ref STORAGE_USAGE_DELTA_FROM_FILE: HashMap<AccountId, u64> = RuntimeConfig::read_storage_usage_delta();
    static ref EMPTY_STORAGE_USAGE_DELTA: HashMap<AccountId, u64> = HashMap::new();
    // static ref STORAGE_USAGE_DELTA_CSV: str = include_str!("../../res/storage_usage_delta.csv");
}

#[cfg(feature = "protocol_feature_add_account_versions")]
lazy_static_include::lazy_static_include_str! {
    STORAGE_USAGE_DELTA_CSV => "res/storage_usage_delta.csv"
}

impl RuntimeConfig {
    #[cfg(feature = "protocol_feature_add_account_versions")]
    fn read_storage_usage_delta() -> HashMap<AccountId, u64> {
        let mut reader = csv::Reader::from_reader(STORAGE_USAGE_DELTA_CSV.as_bytes());
        let mut result = HashMap::new();
        for record in reader.records() {
            let record = record.expect("Malformed storage_usage_delta.csv");
            let account_id = &record[0];
            let delta = record[1].parse::<u64>().expect("Malformed storage_usage_delta.csv");
            result.insert(account_id.to_string() as AccountId, delta);
        }
        result
    }

    #[cfg(feature = "protocol_feature_add_account_versions")]
    fn empty_storage_usage_delta() -> &'static HashMap<AccountId, u64> {
        &EMPTY_STORAGE_USAGE_DELTA
    }

    pub fn free() -> Self {
        Self {
            storage_amount_per_byte: 0,
            transaction_costs: RuntimeFeesConfig::free(),
            wasm_config: VMConfig::free(),
            account_creation_config: AccountCreationConfig::default(),
            #[cfg(feature = "protocol_feature_add_account_versions")]
            storage_usage_delta: &EMPTY_STORAGE_USAGE_DELTA,
        }
    }

    /// Returns a `RuntimeConfig` for the corresponding protocol version.
    /// It uses `genesis_runtime_config` to identify the original
    /// config and `protocol_version` for the current protocol version.
    pub fn from_protocol_version(
        genesis_runtime_config: &Arc<RuntimeConfig>,
        protocol_version: ProtocolVersion,
        is_mainnet: bool,
    ) -> Arc<Self> {
        let result: Arc<RuntimeConfig>;
        if checked_feature!(
            "protocol_feature_lower_storage_cost",
            LowerStorageCost,
            protocol_version
        ) {
            let mut config = LOWER_STORAGE_COST_CONFIG.lock().unwrap();
            result = config
                .get_or_insert_with(|| Arc::new(genesis_runtime_config.decrease_storage_cost()))
                .clone()
        } else {
            result = genesis_runtime_config.clone();
        }
        #[cfg(feature = "protocol_feature_add_account_versions")]
        if is_mainnet
            && checked_feature!(
                "protocol_feature_add_account_versions",
                AccountVersions,
                protocol_version
            )
        {
            return Arc::new(result.add_storage_usage_delta()).clone();
        }
        #[cfg(not(feature = "protocol_feature_add_account_versions"))]
        let _ = is_mainnet;
        result
    }

    /// Returns a new config with decreased storage cost.
    fn decrease_storage_cost(&self) -> Self {
        let mut config = self.clone();
        config.storage_amount_per_byte = 10u128.pow(19);
        config
    }

    #[cfg(feature = "protocol_feature_add_account_versions")]
    fn add_storage_usage_delta(&self) -> Self {
        let mut config = self.clone();
        config.storage_usage_delta = &STORAGE_USAGE_DELTA_FROM_FILE;
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
    #[cfg(feature = "protocol_feature_add_account_versions")]
    use crate::hash::hash;
    #[cfg(feature = "protocol_feature_add_account_versions")]
    use crate::serialize::to_base;
    #[cfg(feature = "protocol_feature_add_account_versions")]
    use crate::version::ProtocolFeature::AccountVersions;

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
        let config_same = RuntimeConfig::from_protocol_version(&config, 0, false);
        assert_eq!(
            config_same.as_ref().storage_amount_per_byte,
            config.as_ref().storage_amount_per_byte
        );
        let config_lower =
            RuntimeConfig::from_protocol_version(&config, ProtocolVersion::MAX, false);
        assert!(
            config_lower.as_ref().storage_amount_per_byte < config.as_ref().storage_amount_per_byte
        );
    }

    #[test]
    #[cfg(feature = "protocol_feature_add_account_versions")]
    fn test_storage_usage_delta() {
        assert_eq!(
            to_base(hash(STORAGE_USAGE_DELTA_CSV.as_bytes())),
            "6vvz6vHkKekc6sEk2nzzJoWLBsiV7cy7Z5KMGKrzHGy1"
        );
        let config = Arc::new(RuntimeConfig::default());
        let config_same = RuntimeConfig::from_protocol_version(&config, 0, false);
        let config_with_delta =
            RuntimeConfig::from_protocol_version(&config, AccountVersions.protocol_version(), true);
        assert_eq!(config.storage_usage_delta.len(), 0);
        assert_eq!(config_same.storage_usage_delta.len(), 0);
        assert_eq!(config_with_delta.storage_usage_delta.len(), 3111);
    }
}

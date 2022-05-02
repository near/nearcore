//! Settings of the parameters of the runtime.
use serde::{Deserialize, Serialize};

use crate::config::VMConfig;
use crate::runtime::config_store::INITIAL_TESTNET_CONFIG;
use crate::runtime::fees::RuntimeFeesConfig;
use crate::runtime::parameter_table::ParameterTable;
use crate::serialize::u128_dec_format;
use crate::types::{AccountId, Balance};

use super::parameter_table::InvalidConfigError;

/// The structure that holds the parameters of the runtime, mostly economics.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    /// Amount of yN per byte required to have on the account.  See
    /// <https://nomicon.io/Economics/README.html#state-stake> for details.
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

impl RuntimeConfig {
    pub(crate) fn from_parameters(params: &ParameterTable) -> Result<Self, serde_json::Error> {
        serde_json::from_value(params.runtime_config_json())
    }

    pub fn initial_testnet_config() -> RuntimeConfig {
        Self::from_parameters_txt(INITIAL_TESTNET_CONFIG)
            .expect("Failed parsing initial testnet config")
    }

    pub(crate) fn from_parameters_txt(txt_file: &str) -> Result<RuntimeConfig, InvalidConfigError> {
        Self::from_parameters(&ParameterTable::from_txt(txt_file)?)
            .map_err(InvalidConfigError::WrongStructure)
    }

    pub fn test() -> Self {
        RuntimeConfig {
            // See https://nomicon.io/Economics/README.html#general-variables for how it was calculated.
            storage_amount_per_byte: 909 * 100_000_000_000_000_000,
            transaction_costs: RuntimeFeesConfig::test(),
            wasm_config: VMConfig::test(),
            account_creation_config: AccountCreationConfig::default(),
        }
    }

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
            registrar_account_id: "registrar".parse().unwrap(),
        }
    }
}

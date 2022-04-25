//! Settings of the parameters of the runtime.
use std::collections::BTreeMap;

use near_primitives_core::parameter::{load_parameters_from_txt, read_parameter, Parameter};
use serde::{Deserialize, Serialize};

use crate::config::VMConfig;
use crate::runtime::fees::RuntimeFeesConfig;
use crate::serialize::u128_dec_format;
use crate::types::{AccountId, Balance};

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
    pub fn from_parameters(params: &BTreeMap<Parameter, String>) -> Self {
        Self {
            storage_amount_per_byte: read_parameter(&params, Parameter::StorageAmountPerByte),
            transaction_costs: RuntimeFeesConfig::from_parameters(&params),
            wasm_config: VMConfig::from_parameters(&params),
            account_creation_config: AccountCreationConfig::from_parameters(&params),
        }
    }

    pub fn from_parameters_txt(txt_file: &[u8]) -> Self {
        Self::from_parameters(&load_parameters_from_txt(txt_file))
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
impl AccountCreationConfig {
    fn from_parameters(params: &BTreeMap<Parameter, String>) -> Self {
        Self {
            min_allowed_top_level_account_length: read_parameter(
                params,
                Parameter::MinAllowedTopLevelAccountLength,
            ),
            registrar_account_id: read_parameter(params, Parameter::RegistrarAccountId),
        }
    }
}

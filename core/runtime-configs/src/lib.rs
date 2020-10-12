//! Settings of the parameters of the runtime.
use serde::{Deserialize, Serialize};

use near_primitives::account::Account;
use near_primitives::serialize::u128_dec_format;
use near_primitives::types::{AccountId, Balance};
use near_primitives::version::ProtocolVersion;
use near_runtime_fees::RuntimeFeesConfig;
use near_vm_logic::VMConfig;
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

    /// Returns a `RuntimeConfig` for the corresponding protocol version.
    /// It uses `genesis_runtime_config` to keep the unchanged fees.
    /// TODO: https://github.com/nearprotocol/NEPs/issues/120
    pub fn from_protocol_version(
        genesis_runtime_config: &Arc<RuntimeConfig>,
        _protocol_version: ProtocolVersion,
    ) -> Arc<Self> {
        genesis_runtime_config.clone()
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

/// Checks if given account has enough balance for storage stake, and returns:
///  - None if account has enough balance,
///  - Some(insufficient_balance) if account doesn't have enough and how much need to be added,
///  - Err(message) if account has invalid storage usage or amount/locked.
///
/// Read details of state staking https://nomicon.io/Economics/README.html#state-stake
pub fn get_insufficient_storage_stake(
    account: &Account,
    runtime_config: &RuntimeConfig,
) -> Result<Option<Balance>, String> {
    let required_amount = Balance::from(account.storage_usage)
        .checked_mul(runtime_config.storage_amount_per_byte)
        .ok_or_else(|| {
            format!("Account's storage_usage {} overflows multiplication", account.storage_usage)
        })?;
    let available_amount = account.amount.checked_add(account.locked).ok_or_else(|| {
        format!(
            "Account's amount {} and locked {} overflow addition",
            account.amount, account.locked
        )
    })?;
    if available_amount >= required_amount {
        Ok(None)
    } else {
        Ok(Some(required_amount - available_amount))
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

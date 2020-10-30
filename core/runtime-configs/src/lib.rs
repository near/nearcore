//! Settings of the parameters of the runtime.
use serde::{Deserialize, Serialize};

use near_primitives::account::Account;
use near_primitives::serialize::u128_dec_format;
use near_primitives::types::{AccountId, Balance};
use near_primitives::version::ProtocolVersion;
use near_runtime_fees::{
    AccessKeyCreationConfig, ActionCreationConfig, DataReceiptCreationConfig, Fee,
    RuntimeFeesConfig,
};
use near_vm_logic::{ExtCostsConfig, VMConfig, VMLimitConfig};
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

lazy_static! {
    static ref FEE_UPGRADE_CONFIG: Mutex<Option<Arc<RuntimeConfig>>> = Mutex::new(None);
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
        genesis_protocol_version: ProtocolVersion,
        protocol_version: ProtocolVersion,
    ) -> Arc<Self> {
        if genesis_protocol_version < PROTOCOL_FEE_UPGRADE
            && protocol_version >= PROTOCOL_FEE_UPGRADE
        {
            let fee_config = FEE_UPGRADE_CONFIG.lock().unwrap();
            if let Some(fee_config) = fee_config {
                fee_config.clone()
            } else {
                let upgraded_config = Arc::new(genesis_runtime_config.upgrade_fees());
                *fee_config = Some(Arc::new(upgraded_config.clone()));
                upgraded_config
            }
        } else {
            genesis_runtime_config.clone()
        }
    }

    fn upgrade_fees(&self) -> Self {
        Self {
            storage_amount_per_byte: self.storage_amount_per_byte,
            transaction_costs: RuntimeFeesConfig {
                action_receipt_creation_config: Fee {
                    send_sir: 108059500000,
                    send_not_sir: 108059500000,
                    execution: 108059500000,
                },
                data_receipt_creation_config: DataReceiptCreationConfig {
                    base_cost: Fee {
                        send_sir: 4697339419375,
                        send_not_sir: 4697339419375,
                        execution: 4697339419375,
                    },
                    cost_per_byte: Fee {
                        send_sir: 59357464,
                        send_not_sir: 59357464,
                        execution: 59357464,
                    },
                },
                action_creation_config: ActionCreationConfig {
                    create_account_cost: Fee {
                        send_sir: 99607375000,
                        send_not_sir: 99607375000,
                        execution: 99607375000,
                    },
                    deploy_contract_cost: Fee {
                        send_sir: 184765750000,
                        send_not_sir: 184765750000,
                        execution: 184765750000,
                    },
                    deploy_contract_cost_per_byte: Fee {
                        send_sir: 6812999,
                        send_not_sir: 6812999,
                        execution: 6812999,
                    },
                    function_call_cost: Fee {
                        send_sir: 2319861500000,
                        send_not_sir: 2319861500000,
                        execution: 2319861500000,
                    },
                    function_call_cost_per_byte: Fee {
                        send_sir: 2235934,
                        send_not_sir: 2235934,
                        execution: 2235934,
                    },
                    transfer_cost: Fee {
                        send_sir: 115123062500,
                        send_not_sir: 115123062500,
                        execution: 115123062500,
                    },
                    stake_cost: Fee {
                        send_sir: 141715687500,
                        send_not_sir: 141715687500,
                        execution: 102217625000,
                    },
                    add_key_cost: AccessKeyCreationConfig {
                        full_access_cost: Fee {
                            send_sir: 101765125000,
                            send_not_sir: 101765125000,
                            execution: 101765125000,
                        },
                        function_call_cost: Fee {
                            send_sir: 102217625000,
                            send_not_sir: 102217625000,
                            execution: 102217625000,
                        },
                        function_call_cost_per_byte: Fee {
                            send_sir: 1925331,
                            send_not_sir: 1925331,
                            execution: 1925331,
                        },
                    },
                    delete_key_cost: Fee {
                        send_sir: 94946625000,
                        send_not_sir: 94946625000,
                        execution: 94946625000,
                    },
                    delete_account_cost: Fee {
                        send_sir: 147489000000,
                        send_not_sir: 147489000000,
                        execution: 147489000000,
                    },
                },
                storage_usage_config: self.transaction_costs.storage_usage_config.clone(),
                burnt_gas_reward: self.transaction_costs.burnt_gas_reward,
                pessimistic_gas_price_inflation_ratio: self
                    .transaction_costs
                    .pessimistic_gas_price_inflation_ratio,
            },
            wasm_config: VMConfig {
                ext_costs: ExtCostsConfig {
                    base: SAFETY_MULTIPLIER * 88256037,
                    contract_compile_base: SAFETY_MULTIPLIER * 11815321,
                    contract_compile_bytes: SAFETY_MULTIPLIER * 72250,
                    read_memory_base: SAFETY_MULTIPLIER * 869954400,
                    read_memory_byte: SAFETY_MULTIPLIER * 1267111,
                    write_memory_base: SAFETY_MULTIPLIER * 934598287,
                    write_memory_byte: SAFETY_MULTIPLIER * 907924,
                    read_register_base: SAFETY_MULTIPLIER * 839055062,
                    read_register_byte: SAFETY_MULTIPLIER * 32854,
                    write_register_base: SAFETY_MULTIPLIER * 955174162,
                    write_register_byte: SAFETY_MULTIPLIER * 1267188,
                    utf8_decoding_base: SAFETY_MULTIPLIER * 1037259687,
                    utf8_decoding_byte: SAFETY_MULTIPLIER * 97193493,
                    utf16_decoding_base: SAFETY_MULTIPLIER * 1181104350,
                    utf16_decoding_byte: SAFETY_MULTIPLIER * 54525831,
                    sha256_base: SAFETY_MULTIPLIER * 1513656750,
                    sha256_byte: SAFETY_MULTIPLIER * 8039117,
                    keccak256_base: SAFETY_MULTIPLIER * 1959830425,
                    keccak256_byte: SAFETY_MULTIPLIER * 7157035,
                    keccak512_base: SAFETY_MULTIPLIER * 1937129412,
                    keccak512_byte: SAFETY_MULTIPLIER * 12216567,
                    log_base: SAFETY_MULTIPLIER * 1181104350,
                    log_byte: SAFETY_MULTIPLIER * 4399597,
                    storage_write_base: SAFETY_MULTIPLIER * 21398912000,
                    storage_write_key_byte: SAFETY_MULTIPLIER * 23494289,
                    storage_write_value_byte: SAFETY_MULTIPLIER * 10339513,
                    storage_write_evicted_byte: SAFETY_MULTIPLIER * 10705769,
                    storage_read_base: SAFETY_MULTIPLIER * 18785615250,
                    storage_read_key_byte: SAFETY_MULTIPLIER * 10317511,
                    storage_read_value_byte: SAFETY_MULTIPLIER * 1870335,
                    storage_remove_base: SAFETY_MULTIPLIER * 17824343500,
                    storage_remove_key_byte: SAFETY_MULTIPLIER * 12740128,
                    storage_remove_ret_value_byte: SAFETY_MULTIPLIER * 3843852,
                    storage_has_key_base: SAFETY_MULTIPLIER * 18013298875,
                    storage_has_key_byte: SAFETY_MULTIPLIER * 10263615,
                    storage_iter_create_prefix_base: SAFETY_MULTIPLIER * 0,
                    storage_iter_create_prefix_byte: SAFETY_MULTIPLIER * 0,
                    storage_iter_create_range_base: SAFETY_MULTIPLIER * 0,
                    storage_iter_create_from_byte: SAFETY_MULTIPLIER * 0,
                    storage_iter_create_to_byte: SAFETY_MULTIPLIER * 0,
                    storage_iter_next_base: SAFETY_MULTIPLIER * 0,
                    storage_iter_next_key_byte: SAFETY_MULTIPLIER * 0,
                    storage_iter_next_value_byte: SAFETY_MULTIPLIER * 0,
                    touching_trie_node: SAFETY_MULTIPLIER * 5367318642,
                    promise_and_base: SAFETY_MULTIPLIER * 488337800,
                    promise_and_per_promise: SAFETY_MULTIPLIER * 1817392,
                    promise_return: SAFETY_MULTIPLIER * 186717462,
                    validator_stake_base: SAFETY_MULTIPLIER * 303944908800,
                    validator_total_stake_base: SAFETY_MULTIPLIER * 303944908800,
                },
                grow_mem_cost: 1,
                regular_op_cost: 3856371,
                limit_config: self.wasm_config.limit_config.clone(),
            },
            account_creation_config: self.account_creation_config.clone(),
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

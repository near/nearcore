//! Describes the various costs incurred by creating receipts.
//! We use the following abbreviation for readability:
//! * sir -- sender is receiver. Receipts that are directed by an account to itself are guaranteed
//!   to not be cross-shard which is cheaper than cross-shard. Conversely, when sender is not a
//!   receiver it might or might not be a cross-shard communication.
use enum_map::EnumMap;
use serde::{Deserialize, Serialize};

use crate::config::ActionCosts;
use crate::num_rational::Rational;
use crate::types::{Balance, Gas};

/// Costs associated with an object that can only be sent over the network (and executed
/// by the receiver).
/// NOTE: `send_sir` or `send_not_sir` fees are usually burned when the item is being created.
/// And `execution` fee is burned when the item is being executed.
#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct Fee {
    /// Fee for sending an object from the sender to itself, guaranteeing that it does not leave
    /// the shard.
    pub send_sir: Gas,
    /// Fee for sending an object potentially across the shards.
    pub send_not_sir: Gas,
    /// Fee for executing the object.
    pub execution: Gas,
}

impl Fee {
    #[inline]
    pub fn send_fee(&self, sir: bool) -> Gas {
        if sir {
            self.send_sir
        } else {
            self.send_not_sir
        }
    }

    pub fn exec_fee(&self) -> Gas {
        self.execution
    }

    /// The minimum fee to send and execute.
    pub fn min_send_and_exec_fee(&self) -> Gas {
        std::cmp::min(self.send_sir, self.send_not_sir) + self.execution
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct RuntimeFeesConfig {
    /// Gas fees for sending and executing actions.
    pub action_fees: EnumMap<ActionCosts, Fee>,

    /// Describes fees for storage.
    pub storage_usage_config: StorageUsageConfig,

    /// Fraction of the burnt gas to reward to the contract account for execution.
    pub burnt_gas_reward: Rational,

    /// Pessimistic gas price inflation ratio.
    pub pessimistic_gas_price_inflation_ratio: Rational,
}

/// Describes the cost of creating a data receipt, `DataReceipt`.
#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct DataReceiptCreationConfig {
    /// Base cost of creating a data receipt.
    /// Both `send` and `exec` costs are burned when a new receipt has input dependencies. The gas
    /// is charged for each input dependency. The dependencies are specified when a receipt is
    /// created using `promise_then` and `promise_batch_then`.
    /// NOTE: Any receipt with output dependencies will produce data receipts. Even if it fails.
    /// Even if the last action is not a function call (in case of success it will return empty
    /// value).
    pub base_cost: Fee,
    /// Additional cost per byte sent.
    /// Both `send` and `exec` costs are burned when a function call finishes execution and returns
    /// `N` bytes of data to every output dependency. For each output dependency the cost is
    /// `(send(sir) + exec()) * N`.
    pub cost_per_byte: Fee,
}

/// Describes the cost of creating a specific action, `Action`. Includes all variants.
#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct ActionCreationConfig {
    /// Base cost of creating an account.
    pub create_account_cost: Fee,

    /// Base cost of deploying a contract.
    pub deploy_contract_cost: Fee,
    /// Cost per byte of deploying a contract.
    pub deploy_contract_cost_per_byte: Fee,

    /// Base cost of calling a function.
    pub function_call_cost: Fee,
    /// Cost per byte of method name and arguments of calling a function.
    pub function_call_cost_per_byte: Fee,

    /// Base cost of making a transfer.
    pub transfer_cost: Fee,

    /// Base cost of staking.
    pub stake_cost: Fee,

    /// Base cost of adding a key.
    pub add_key_cost: AccessKeyCreationConfig,

    /// Base cost of deleting a key.
    pub delete_key_cost: Fee,

    /// Base cost of deleting an account.
    pub delete_account_cost: Fee,

    /// Base cost of a delegate action
    pub delegate_cost: Fee,
}

/// Describes the cost of creating an access key.
#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct AccessKeyCreationConfig {
    /// Base cost of creating a full access access-key.
    pub full_access_cost: Fee,
    /// Base cost of creating an access-key restricted to specific functions.
    pub function_call_cost: Fee,
    /// Cost per byte of method_names of creating a restricted access-key.
    pub function_call_cost_per_byte: Fee,
}

/// Describes cost of storage per block
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct StorageUsageConfig {
    /// Amount of yN per byte required to have on the account. See
    /// <https://nomicon.io/Economics/README.html#state-stake> for details.
    pub storage_amount_per_byte: Balance,
    /// Number of bytes for an account record, including rounding up for account id.
    pub num_bytes_account: u64,
    /// Additional number of bytes for a k/v record
    pub num_extra_bytes_record: u64,
}

impl RuntimeFeesConfig {
    /// Access action fee by `ActionCosts`.
    pub fn fee(&self, cost: ActionCosts) -> &Fee {
        &self.action_fees[cost]
    }

    pub fn test() -> Self {
        Self {
            storage_usage_config: StorageUsageConfig::test(),
            burnt_gas_reward: Rational::new(3, 10),
            pessimistic_gas_price_inflation_ratio: Rational::new(103, 100),
            action_fees: enum_map::enum_map! {
                ActionCosts::create_account => Fee {
                    send_sir: 99607375000,
                    send_not_sir: 99607375000,
                    execution: 99607375000,
                },
                ActionCosts::delete_account => Fee {
                    send_sir: 147489000000,
                    send_not_sir: 147489000000,
                    execution: 147489000000,
                },
                ActionCosts::deploy_contract_base => Fee {
                    send_sir: 184765750000,
                    send_not_sir: 184765750000,
                    execution: 184765750000,
                },
                ActionCosts::deploy_contract_byte => Fee {
                    send_sir: 6812999,
                    send_not_sir: 6812999,
                    execution: 6812999,
                },
                ActionCosts::function_call_base => Fee {
                    send_sir: 2319861500000,
                    send_not_sir: 2319861500000,
                    execution: 2319861500000,
                },
                ActionCosts::function_call_byte => Fee {
                    send_sir: 2235934,
                    send_not_sir: 2235934,
                    execution: 2235934,
                },
                ActionCosts::transfer => Fee {
                    send_sir: 115123062500,
                    send_not_sir: 115123062500,
                    execution: 115123062500,
                },
                ActionCosts::stake => Fee {
                    send_sir: 141715687500,
                    send_not_sir: 141715687500,
                    execution: 102217625000,
                },
                ActionCosts::add_full_access_key => Fee {
                    send_sir: 101765125000,
                    send_not_sir: 101765125000,
                    execution: 101765125000,
                },
                ActionCosts::add_function_call_key_base => Fee {
                    send_sir: 102217625000,
                    send_not_sir: 102217625000,
                    execution: 102217625000,
                },
                ActionCosts::add_function_call_key_byte => Fee {
                    send_sir: 1925331,
                    send_not_sir: 1925331,
                    execution: 1925331,
                },
                ActionCosts::delete_key => Fee {
                    send_sir: 94946625000,
                    send_not_sir: 94946625000,
                    execution: 94946625000,
                },
                ActionCosts::new_action_receipt => Fee {
                    send_sir: 108059500000,
                    send_not_sir: 108059500000,
                    execution: 108059500000,
                },
                ActionCosts::new_data_receipt_base => Fee {
                    send_sir: 4697339419375,
                    send_not_sir: 4697339419375,
                    execution: 4697339419375,
                },
                ActionCosts::new_data_receipt_byte => Fee {
                    send_sir: 59357464,
                    send_not_sir: 59357464,
                    execution: 59357464,
                },
                #[cfg(feature = "protocol_feature_nep366_delegate_action")]
                ActionCosts::delegate => Fee {
                    send_sir: 2319861500000,
                    send_not_sir: 2319861500000,
                    execution: 2319861500000,
                },
            },
        }
    }

    pub fn free() -> Self {
        Self {
            action_fees: enum_map::enum_map! {
                _ => Fee { send_sir: 0, send_not_sir: 0, execution: 0 }
            },
            storage_usage_config: StorageUsageConfig::free(),
            burnt_gas_reward: Rational::from_integer(0),
            pessimistic_gas_price_inflation_ratio: Rational::from_integer(0),
        }
    }

    /// The minimum amount of gas required to create and execute a new receipt with a function call
    /// action.
    /// This amount is used to determine how many receipts can be created, send and executed for
    /// some amount of prepaid gas using function calls.
    pub fn min_receipt_with_function_call_gas(&self) -> Gas {
        self.fee(ActionCosts::new_action_receipt).min_send_and_exec_fee()
            + self.fee(ActionCosts::function_call_base).min_send_and_exec_fee()
    }
}

impl StorageUsageConfig {
    pub fn test() -> Self {
        Self {
            num_bytes_account: 100,
            num_extra_bytes_record: 40,
            storage_amount_per_byte: 909 * 100_000_000_000_000_000,
        }
    }

    pub(crate) fn free() -> StorageUsageConfig {
        Self { num_bytes_account: 0, num_extra_bytes_record: 0, storage_amount_per_byte: 0 }
    }
}

/// Helper functions for computing Transfer fees.
/// In case of implicit account creation they always include extra fees for the CreateAccount and
/// AddFullAccessKey actions that are implicit.
/// We can assume that no overflow will happen here.
pub fn transfer_exec_fee(cfg: &RuntimeFeesConfig, is_receiver_implicit: bool) -> Gas {
    if is_receiver_implicit {
        cfg.fee(ActionCosts::create_account).exec_fee()
            + cfg.fee(ActionCosts::add_full_access_key).exec_fee()
            + cfg.fee(ActionCosts::transfer).exec_fee()
    } else {
        cfg.fee(ActionCosts::transfer).exec_fee()
    }
}

pub fn transfer_send_fee(
    cfg: &RuntimeFeesConfig,
    sender_is_receiver: bool,
    is_receiver_implicit: bool,
) -> Gas {
    if is_receiver_implicit {
        cfg.fee(ActionCosts::create_account).send_fee(sender_is_receiver)
            + cfg.fee(ActionCosts::add_full_access_key).send_fee(sender_is_receiver)
            + cfg.fee(ActionCosts::transfer).send_fee(sender_is_receiver)
    } else {
        cfg.fee(ActionCosts::transfer).send_fee(sender_is_receiver)
    }
}

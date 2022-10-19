//! Describes the various costs incurred by creating receipts.
//! We use the following abbreviation for readability:
//! * sir -- sender is receiver. Receipts that are directed by an account to itself are guaranteed
//!   to not be cross-shard which is cheaper than cross-shard. Conversely, when sender is not a
//!   receiver it might or might not be a cross-shard communication.
use serde::{Deserialize, Serialize};

use crate::num_rational::Rational;
use crate::types::Gas;

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
    fn min_send_and_exec_fee(&self) -> Gas {
        std::cmp::min(self.send_sir, self.send_not_sir) + self.execution
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct RuntimeFeesConfig {
    /// Describes the cost of creating an action receipt, `ActionReceipt`, excluding the actual cost
    /// of actions.
    /// - `send` cost is burned when a receipt is created using `promise_create` or
    ///     `promise_batch_create`
    /// - `exec` cost is burned when the receipt is being executed.
    pub action_receipt_creation_config: Fee,
    /// Describes the cost of creating a data receipt, `DataReceipt`.
    pub data_receipt_creation_config: DataReceiptCreationConfig,
    /// Describes the cost of creating a certain action, `Action`. Includes all variants.
    pub action_creation_config: ActionCreationConfig,
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
#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct StorageUsageConfig {
    /// Number of bytes for an account record, including rounding up for account id.
    pub num_bytes_account: u64,
    /// Additional number of bytes for a k/v record
    pub num_extra_bytes_record: u64,
}

impl RuntimeFeesConfig {
    pub fn test() -> Self {
        #[allow(clippy::unreadable_literal)]
        Self {
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
            storage_usage_config: StorageUsageConfig {
                // See Account in core/primitives/src/account.rs for the data structure.
                // TODO(2291): figure out value for the mainnet.
                num_bytes_account: 100,
                num_extra_bytes_record: 40,
            },
            burnt_gas_reward: Rational::new(3, 10),
            pessimistic_gas_price_inflation_ratio: Rational::new(103, 100),
        }
    }

    pub fn free() -> Self {
        let free = Fee { send_sir: 0, send_not_sir: 0, execution: 0 };
        RuntimeFeesConfig {
            action_receipt_creation_config: free.clone(),
            data_receipt_creation_config: DataReceiptCreationConfig {
                base_cost: free.clone(),
                cost_per_byte: free.clone(),
            },
            action_creation_config: ActionCreationConfig {
                create_account_cost: free.clone(),
                deploy_contract_cost: free.clone(),
                deploy_contract_cost_per_byte: free.clone(),
                function_call_cost: free.clone(),
                function_call_cost_per_byte: free.clone(),
                transfer_cost: free.clone(),
                stake_cost: free.clone(),
                add_key_cost: AccessKeyCreationConfig {
                    full_access_cost: free.clone(),
                    function_call_cost: free.clone(),
                    function_call_cost_per_byte: free.clone(),
                },
                delete_key_cost: free.clone(),
                delete_account_cost: free,
            },
            storage_usage_config: StorageUsageConfig {
                num_bytes_account: 0,
                num_extra_bytes_record: 0,
            },
            burnt_gas_reward: Rational::from_integer(0),
            pessimistic_gas_price_inflation_ratio: Rational::from_integer(0),
        }
    }

    /// The minimum amount of gas required to create and execute a new receipt with a function call
    /// action.
    /// This amount is used to determine how many receipts can be created, send and executed for
    /// some amount of prepaid gas using function calls.
    pub fn min_receipt_with_function_call_gas(&self) -> Gas {
        self.action_receipt_creation_config.min_send_and_exec_fee()
            + self.action_creation_config.function_call_cost.min_send_and_exec_fee()
    }
}

/// Helper functions for computing Transfer fees.
/// In case of implicit account creation they always include extra fees for the CreateAccount and
/// AddFullAccessKey actions that are implicit.
/// We can assume that no overflow will happen here.
pub fn transfer_exec_fee(cfg: &ActionCreationConfig, is_receiver_implicit: bool) -> Gas {
    if is_receiver_implicit {
        cfg.create_account_cost.exec_fee()
            + cfg.add_key_cost.full_access_cost.exec_fee()
            + cfg.transfer_cost.exec_fee()
    } else {
        cfg.transfer_cost.exec_fee()
    }
}

pub fn transfer_send_fee(
    cfg: &ActionCreationConfig,
    sender_is_receiver: bool,
    is_receiver_implicit: bool,
) -> Gas {
    if is_receiver_implicit {
        cfg.create_account_cost.send_fee(sender_is_receiver)
            + cfg.add_key_cost.full_access_cost.send_fee(sender_is_receiver)
            + cfg.transfer_cost.send_fee(sender_is_receiver)
    } else {
        cfg.transfer_cost.send_fee(sender_is_receiver)
    }
}

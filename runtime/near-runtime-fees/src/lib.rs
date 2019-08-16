//! Describes the various costs incurred by creating receipts.
//! We use the following abbreviation for readability:
//! * sir -- sender is receiver. Receipts that are directed by an account to itself are guaranteed
//!   to not be cross-shard which is cheaper than cross-shard. Conversely, when sender is not a
//!   receiver it might or might not be a cross-shard communication.
use serde::{Deserialize, Serialize};
pub type Gas = u64;

/// Costs associated with an object that can only be sent over the network (and executed
/// by the receiver).
#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct Fee {
    /// Fee for sending an object from the sender to itself, guaranteeing that it does not leave
    /// the shard.
    send_sir: Gas,
    /// Fee for sending an object potentially across the shards.
    send_not_sir: Gas,
    /// Fee for executing the object.
    execution: Gas,
}

impl Fee {
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
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct RuntimeFeesConfig {
    /// Describes the cost of creating an action receipt, `ActionReceipt`, excluding the actual cost
    /// of actions.
    pub action_receipt_creation_config: Fee,
    /// Describes the cost of creating a data receipt, `DataReceipt`.
    pub data_receipt_creation_config: DataReceiptCreationConfig,
    /// Describes the cost of creating a certain action, `Action`. Includes all variants.
    pub action_creation_config: ActionCreationConfig,
}

/// Describes the cost of creating a data receipt, `DataReceipt`.
#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct DataReceiptCreationConfig {
    /// Base cost of creating a data receipt.
    pub base_cost: Fee,
    /// Additional cost per byte sent.
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

impl Default for RuntimeFeesConfig {
    fn default() -> Self {
        Self {
            action_receipt_creation_config: Fee { send_sir: 1, send_not_sir: 1, execution: 1 },
            data_receipt_creation_config: DataReceiptCreationConfig {
                base_cost: Fee { send_sir: 1, send_not_sir: 1, execution: 1 },
                cost_per_byte: Fee { send_sir: 1, send_not_sir: 1, execution: 1 },
            },
            action_creation_config: ActionCreationConfig {
                create_account_cost: Fee { send_sir: 1, send_not_sir: 1, execution: 1 },
                deploy_contract_cost: Fee { send_sir: 1, send_not_sir: 1, execution: 1 },
                deploy_contract_cost_per_byte: Fee { send_sir: 1, send_not_sir: 1, execution: 1 },
                function_call_cost: Fee { send_sir: 1, send_not_sir: 1, execution: 1 },
                function_call_cost_per_byte: Fee { send_sir: 1, send_not_sir: 1, execution: 1 },
                transfer_cost: Fee { send_sir: 1, send_not_sir: 1, execution: 1 },
                stake_cost: Fee { send_sir: 1, send_not_sir: 1, execution: 1 },
                add_key_cost: AccessKeyCreationConfig {
                    full_access_cost: Fee { send_sir: 1, send_not_sir: 1, execution: 1 },
                    function_call_cost: Fee { send_sir: 1, send_not_sir: 1, execution: 1 },
                    function_call_cost_per_byte: Fee { send_sir: 1, send_not_sir: 1, execution: 1 },
                },
                delete_key_cost: Fee { send_sir: 1, send_not_sir: 1, execution: 1 },
                delete_account_cost: Fee { send_sir: 1, send_not_sir: 1, execution: 1 },
            },
        }
    }
}

//! Describes the various costs incurred by creating receipts.
//! We use the following abbreviation for readability:
//! * sir -- sender is receiver. Receipts that are directed by an account to itself are guaranteed
//!   to not be cross-shard which is cheaper than cross-shard. Conversely, when sender is not a
//!   receiver it might or might not be a cross-shard communication.
use serde::{Deserialize, Serialize};
pub type Gas = u64;

#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct Fraction {
    pub numerator: u64,
    pub denominator: u64,
}

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
    /// Costs for runtime externals
    pub ext_costs: ExtCostsConfig,
    /// Describes the cost of creating an action receipt, `ActionReceipt`, excluding the actual cost
    /// of actions.
    pub action_receipt_creation_config: Fee,
    /// Describes the cost of creating a data receipt, `DataReceipt`.
    pub data_receipt_creation_config: DataReceiptCreationConfig,
    /// Describes the cost of creating a certain action, `Action`. Includes all variants.
    pub action_creation_config: ActionCreationConfig,
    /// Describes fees for storage rent
    pub storage_usage_config: StorageUsageConfig,

    /// Fraction of the burnt gas to reward to the contract account for execution.
    pub burnt_gas_reward: Fraction,
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct ExtCostsConfig {
    /// Pay for reading contract input base
    pub input_base: Gas,
    /// Pay for reading contract input per byte
    pub input_per_byte: Gas,
    /// Storage trie read key base cost
    pub storage_read_base: Gas,
    /// Storage trie read key per byte cost
    pub storage_read_key_byte: Gas,
    /// Storage trie read value cost per byte cost
    pub storage_read_value_byte: Gas,
    /// Storage trie write key base cost
    pub storage_write_base: Gas,
    /// Storage trie write key per byte cost
    pub storage_write_key_byte: Gas,
    /// Storage trie write value per byte cost
    pub storage_write_value_byte: Gas,
    /// Storage trie check for key existence cost base
    pub storage_has_key_base: Gas,
    /// Storage trie check for key existence per key byte
    pub storage_has_key_byte: Gas,
    /// Remove key from trie base cost
    pub storage_remove_base: Gas,
    /// Remove key from trie per byte cost
    pub storage_remove_key_byte: Gas,
    /// Remove key from trie ret value byte cost
    pub storage_remove_ret_value_byte: Gas,
    /// Create trie prefix iterator cost base
    pub storage_iter_create_prefix_base: Gas,
    /// Create trie range iterator cost base
    pub storage_iter_create_range_base: Gas,
    /// Create trie iterator per key byte cost
    pub storage_iter_create_key_byte: Gas,
    /// Trie iterator per key base cost
    pub storage_iter_next_base: Gas,
    /// Trie iterator next key byte cost
    pub storage_iter_next_key_byte: Gas,
    /// Trie iterator next key byte cost
    pub storage_iter_next_value_byte: Gas,
    /// Base cost for reading from register
    pub read_register_base: Gas,
    /// Cost for reading byte from register
    pub read_register_byte: Gas,
    /// Base cost for writing into register
    pub write_register_base: Gas,
    /// Cost for writing byte into register
    pub write_register_byte: Gas,
    /// Base cost for guest memory read
    pub read_memory_base: Gas,
    /// Cost for guest memory read
    pub read_memory_byte: Gas,
    /// Base cost for guest memory write
    pub write_memory_base: Gas,
    /// Cost for guest memory write per byte
    pub write_memory_byte: Gas,
    /// Get account balance cost
    pub account_balance: Gas,
    /// Get prepaid gas cost
    pub prepaid_gas: Gas,
    /// Get used gas cost
    pub used_gas: Gas,
    /// Cost of getting random seed
    pub random_seed_base: Gas,
    /// Cost of getting random seed per byte
    pub random_seed_per_byte: Gas,
    /// Cost of getting sha256 base
    pub sha256: Gas,
    /// Cost of getting sha256 per byte
    pub sha256_byte: Gas,
    /// Get account attached_deposit base cost
    pub attached_deposit: Gas,
    /// Get storage usage cost
    pub storage_usage: Gas,
    /// Get a current block height base cost
    pub block_index: Gas,
    /// Get a current timestamp base cost
    pub block_timestamp: Gas,
    /// Cost for getting a current account base
    pub current_account_id: Gas,
    /// Cost for getting a current account per byte
    pub current_account_id_byte: Gas,
    /// Cost for getting a signer account id base
    pub signer_account_id: Gas,
    /// Cost for getting a signer account per byte
    pub signer_account_id_byte: Gas,
    /// Cost for getting a signer public key
    pub signer_account_pk: Gas,
    /// Cost for getting a signer public key per byte
    pub signer_account_pk_byte: Gas,
    /// Cost for getting a predecessor account
    pub predecessor_account_id: Gas,
    /// Cost for getting a predecessor account per byte
    pub predecessor_account_id_byte: Gas,
    /// Cost for calling promise_and
    pub promise_and_base: Gas,
    /// Cost for calling promise_and for each promise
    pub promise_and_per_promise: Gas,
    /// Cost for calling promise_result
    pub promise_result_base: Gas,
    /// Cost for calling promise_result per result byte
    pub promise_result_byte: Gas,
    /// Cost for calling promise_results_count
    pub promise_results_count: Gas,
    /// Cost for calling promise_return
    pub promise_return: Gas,
    /// Cost for calling logging
    pub log_base: Gas,
    /// Cost for logging per byte
    pub log_per_byte: Gas,
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

#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct StorageUsageConfig {
    /// Base storage usage for an account
    pub account_cost: Gas,
    /// Base cost for a k/v record
    pub data_record_cost: Gas,
    /// Cost per byte of key
    pub key_cost_per_byte: Gas,
    /// Cost per byte of value
    pub value_cost_per_byte: Gas,
    /// Cost per byte of contract code
    pub code_cost_per_byte: Gas,
}

impl Default for RuntimeFeesConfig {
    fn default() -> Self {
        Self {
            ext_costs: ExtCostsConfig {
                input_base: 1,
                input_per_byte: 1,
                storage_read_base: 1,
                storage_read_key_byte: 1,
                storage_read_value_byte: 1,
                storage_write_base: 1,
                storage_write_key_byte: 1,
                storage_write_value_byte: 1,
                storage_has_key_base: 1,
                storage_has_key_byte: 1,
                storage_remove_base: 1,
                storage_remove_key_byte: 1,
                storage_remove_ret_value_byte: 1,
                storage_iter_create_prefix_base: 1,
                storage_iter_create_range_base: 1,
                storage_iter_create_key_byte: 1,
                storage_iter_next_base: 1,
                storage_iter_next_key_byte: 1,
                storage_iter_next_value_byte: 1,
                read_register_base: 1,
                read_register_byte: 1,
                write_register_base: 1,
                write_register_byte: 1,
                read_memory_base: 1,
                read_memory_byte: 1,
                write_memory_base: 1,
                write_memory_byte: 1,
                account_balance: 1,
                prepaid_gas: 1,
                used_gas: 1,
                random_seed_base: 1,
                random_seed_per_byte: 1,
                sha256: 1,
                sha256_byte: 1,
                attached_deposit: 1,
                storage_usage: 1,
                block_index: 1,
                block_timestamp: 1,
                current_account_id: 1,
                current_account_id_byte: 1,
                signer_account_id: 1,
                signer_account_id_byte: 1,
                signer_account_pk: 1,
                signer_account_pk_byte: 1,
                predecessor_account_id: 1,
                predecessor_account_id_byte: 1,
                promise_and_base: 1,
                promise_and_per_promise: 1,
                promise_result_base: 1,
                promise_result_byte: 1,
                promise_results_count: 1,
                promise_return: 1,
                log_base: 1,
                log_per_byte: 1,
            },
            action_receipt_creation_config: Fee { send_sir: 10, send_not_sir: 10, execution: 10 },
            data_receipt_creation_config: DataReceiptCreationConfig {
                base_cost: Fee { send_sir: 10, send_not_sir: 10, execution: 10 },
                cost_per_byte: Fee { send_sir: 10, send_not_sir: 10, execution: 10 },
            },
            action_creation_config: ActionCreationConfig {
                create_account_cost: Fee { send_sir: 10, send_not_sir: 10, execution: 10 },
                deploy_contract_cost: Fee { send_sir: 10, send_not_sir: 10, execution: 10 },
                deploy_contract_cost_per_byte: Fee {
                    send_sir: 10,
                    send_not_sir: 10,
                    execution: 10,
                },
                function_call_cost: Fee { send_sir: 10, send_not_sir: 10, execution: 10 },
                function_call_cost_per_byte: Fee { send_sir: 10, send_not_sir: 10, execution: 10 },
                transfer_cost: Fee { send_sir: 10, send_not_sir: 10, execution: 10 },
                stake_cost: Fee { send_sir: 10, send_not_sir: 10, execution: 10 },
                add_key_cost: AccessKeyCreationConfig {
                    full_access_cost: Fee { send_sir: 10, send_not_sir: 10, execution: 10 },
                    function_call_cost: Fee { send_sir: 10, send_not_sir: 10, execution: 10 },
                    function_call_cost_per_byte: Fee {
                        send_sir: 10,
                        send_not_sir: 10,
                        execution: 10,
                    },
                },
                delete_key_cost: Fee { send_sir: 10, send_not_sir: 10, execution: 10 },
                delete_account_cost: Fee { send_sir: 10, send_not_sir: 10, execution: 10 },
            },
            storage_usage_config: StorageUsageConfig {
                account_cost: 100,
                data_record_cost: 40,
                key_cost_per_byte: 1,
                value_cost_per_byte: 1,
                code_cost_per_byte: 1,
            },
            burnt_gas_reward: Fraction { numerator: 3, denominator: 10 },
        }
    }
}

impl RuntimeFeesConfig {
    pub fn free() -> Self {
        let free = Fee { send_sir: 0, send_not_sir: 0, execution: 0 };
        RuntimeFeesConfig {
            ext_costs: ExtCostsConfig {
                input_base: 0,
                input_per_byte: 0,
                storage_read_base: 0,
                storage_read_key_byte: 0,
                storage_read_value_byte: 0,
                storage_write_base: 0,
                storage_write_key_byte: 0,
                storage_write_value_byte: 0,
                storage_has_key_base: 0,
                storage_has_key_byte: 0,
                storage_remove_base: 0,
                storage_remove_key_byte: 0,
                storage_remove_ret_value_byte: 0,
                storage_iter_create_prefix_base: 0,
                storage_iter_create_range_base: 0,
                storage_iter_create_key_byte: 0,
                storage_iter_next_base: 0,
                storage_iter_next_key_byte: 0,
                storage_iter_next_value_byte: 0,
                read_register_base: 0,
                read_register_byte: 0,
                write_register_base: 0,
                write_register_byte: 0,
                read_memory_base: 0,
                read_memory_byte: 0,
                write_memory_base: 0,
                write_memory_byte: 0,
                account_balance: 0,
                prepaid_gas: 0,
                used_gas: 0,
                random_seed_base: 0,
                random_seed_per_byte: 0,
                sha256: 0,
                sha256_byte: 0,
                attached_deposit: 0,
                storage_usage: 0,
                block_index: 0,
                block_timestamp: 0,
                current_account_id: 0,
                current_account_id_byte: 0,
                signer_account_id: 0,
                signer_account_id_byte: 0,
                signer_account_pk: 0,
                signer_account_pk_byte: 0,
                predecessor_account_id: 0,
                predecessor_account_id_byte: 0,
                promise_and_base: 0,
                promise_and_per_promise: 0,
                promise_result_base: 0,
                promise_result_byte: 0,
                promise_results_count: 0,
                promise_return: 0,
                log_base: 0,
                log_per_byte: 0,
            },
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
                delete_account_cost: free.clone(),
            },
            storage_usage_config: StorageUsageConfig {
                account_cost: 0,
                data_record_cost: 0,
                key_cost_per_byte: 0,
                value_cost_per_byte: 0,
                code_cost_per_byte: 0,
            },
            burnt_gas_reward: Fraction { numerator: 0, denominator: 1 },
        }
    }
}

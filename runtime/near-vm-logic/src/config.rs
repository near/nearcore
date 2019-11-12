use crate::types::Gas;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug, Hash, Serialize, Deserialize)]
pub struct VMConfig {
    /// Costs for runtime externals
    pub ext_costs: ExtCostsConfig,

    /// Gas cost of a growing memory by single page.
    pub grow_mem_cost: u32,
    /// Gas cost of a regular operation.
    pub regular_op_cost: u32,
    /// Max amount of gas that can be used, excluding gas attached to promises.
    pub max_gas_burnt: Gas,

    /// How tall the stack is allowed to grow?
    ///
    /// See https://wiki.parity.io/WebAssembly-StackHeight to find out
    /// how the stack frame cost is calculated.
    pub max_stack_height: u32,
    /// The initial number of memory pages.
    pub initial_memory_pages: u32,
    /// What is the maximal memory pages amount is allowed to have for
    /// a contract.
    pub max_memory_pages: u32,

    /// Limit of memory used by registers.
    pub registers_memory_limit: u64,
    /// Maximum number of bytes that can be stored in a single register.
    pub max_register_size: u64,
    /// Maximum number of registers that can be used simultaneously.
    pub max_number_registers: u64,

    /// Maximum number of log entries.
    pub max_number_logs: u64,
    /// Maximum length of a single log, in bytes.
    pub max_log_len: u64,
}

impl Default for VMConfig {
    fn default() -> VMConfig {
        VMConfig {
            ext_costs: ExtCostsConfig::default(),
            grow_mem_cost: 1,
            regular_op_cost: 1,
            max_gas_burnt: 10u64.pow(9),
            max_stack_height: 16 * 1024,        // 16Kib of stack.
            initial_memory_pages: 2u32.pow(10), // 64Mib of memory.
            max_memory_pages: 2u32.pow(11),     // 128Mib of memory.
            // By default registers are limited by 1GiB of memory.
            registers_memory_limit: 2u64.pow(30),
            // By default each register is limited by 100MiB of memory.
            max_register_size: 2u64.pow(20) * 100,
            // By default there is at most 100 registers.
            max_number_registers: 100,
            max_number_logs: 100,
            max_log_len: 500,
        }
    }
}

impl VMConfig {
    /// Computes non-cryptographically-proof hash. The computation is fast but not cryptographically
    /// secure.
    pub fn non_crypto_hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.hash(&mut s);
        s.finish()
    }

    pub fn free() -> Self {
        Self {
            ext_costs: ExtCostsConfig::free(),
            grow_mem_cost: 0,
            regular_op_cost: 0,
            max_gas_burnt: 10u64.pow(9),
            max_stack_height: 16 * 1024,
            initial_memory_pages: 17,
            max_memory_pages: 32,
            registers_memory_limit: 2u64.pow(30),
            max_register_size: 2u64.pow(20) * 100,
            max_number_registers: 100,
            max_number_logs: 100,
            max_log_len: 500,
        }
    }
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

impl Default for ExtCostsConfig {
    fn default() -> ExtCostsConfig {
        ExtCostsConfig {
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
        }
    }
}

impl ExtCostsConfig {
    fn free() -> ExtCostsConfig {
        ExtCostsConfig {
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
        }
    }
}

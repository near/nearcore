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
    /// Base cost for calling a host function.
    pub base: Gas,

    /// Base cost for guest memory read
    pub read_memory_base: Gas,
    /// Cost for guest memory read
    pub read_memory_byte: Gas,

    /// Base cost for guest memory write
    pub write_memory_base: Gas,
    /// Cost for guest memory write per byte
    pub write_memory_byte: Gas,

    /// Base cost for reading from register
    pub read_register_base: Gas,
    /// Cost for reading byte from register
    pub read_register_byte: Gas,

    /// Base cost for writing into register
    pub write_register_base: Gas,
    /// Cost for writing byte into register
    pub write_register_byte: Gas,

    /// Base cost of decoding utf8.
    pub utf8_decoding_base: Gas,
    /// Cost per bye of decoding utf8.
    pub utf8_decoding_byte: Gas,

    /// Base cost of decoding utf16.
    pub utf16_decoding_base: Gas,
    /// Cost per bye of decoding utf16.
    pub utf16_decoding_byte: Gas,

    /// Cost of getting sha256 base
    pub sha256_base: Gas,
    /// Cost of getting sha256 per byte
    pub sha256_byte: Gas,

    /// Cost for calling logging.
    pub log_base: Gas,
    /// Cost for logging per byte
    pub log_byte: Gas,

    // ###############
    // # Storage API #
    // ###############
    /// Storage trie write key base cost
    pub storage_write_base: Gas,
    /// Storage trie write key per byte cost
    pub storage_write_key_byte: Gas,
    /// Storage trie write value per byte cost
    pub storage_write_value_byte: Gas,
    /// Storage trie write cost per byte of evicted value.
    pub storage_write_evicted_byte: Gas,

    /// Storage trie read key base cost
    pub storage_read_base: Gas,
    /// Storage trie read key per byte cost
    pub storage_read_key_byte: Gas,
    /// Storage trie read value cost per byte cost
    pub storage_read_value_byte: Gas,

    /// Remove key from trie base cost
    pub storage_remove_base: Gas,
    /// Remove key from trie per byte cost
    pub storage_remove_key_byte: Gas,
    /// Remove key from trie ret value byte cost
    pub storage_remove_ret_value_byte: Gas,

    /// Storage trie check for key existence cost base
    pub storage_has_key_base: Gas,
    /// Storage trie check for key existence per key byte
    pub storage_has_key_byte: Gas,

    /// Create trie prefix iterator cost base
    pub storage_iter_create_prefix_base: Gas,
    /// Create trie prefix iterator cost per byte.
    pub storage_iter_create_prefix_byte: Gas,

    /// Create trie range iterator cost base
    pub storage_iter_create_range_base: Gas,
    /// Create trie range iterator cost per byte of from key.
    pub storage_iter_create_from_byte: Gas,
    /// Create trie range iterator cost per byte of to key.
    pub storage_iter_create_to_byte: Gas,

    /// Trie iterator per key base cost
    pub storage_iter_next_base: Gas,
    /// Trie iterator next key byte cost
    pub storage_iter_next_key_byte: Gas,
    /// Trie iterator next key byte cost
    pub storage_iter_next_value_byte: Gas,

    /// Cost per touched trie node
    pub touching_trie_node: Gas,

    // ###############
    // # Promise API #
    // ###############
    /// Cost for calling promise_and
    pub promise_and_base: Gas,
    /// Cost for calling promise_and for each promise
    pub promise_and_per_promise: Gas,
    /// Cost for calling promise_return
    pub promise_return: Gas,
}

impl Default for ExtCostsConfig {
    fn default() -> ExtCostsConfig {
        ExtCostsConfig {
            base: 1,
            read_memory_base: 1,
            read_memory_byte: 1,
            write_memory_base: 1,
            write_memory_byte: 1,
            read_register_base: 1,
            read_register_byte: 1,
            write_register_base: 1,
            write_register_byte: 1,
            utf8_decoding_base: 1,
            utf8_decoding_byte: 1,
            utf16_decoding_base: 1,
            utf16_decoding_byte: 1,
            sha256_base: 1,
            sha256_byte: 1,
            log_base: 1,
            log_byte: 1,
            storage_write_base: 1,
            storage_write_key_byte: 1,
            storage_write_value_byte: 1,
            storage_write_evicted_byte: 1,
            storage_read_base: 1,
            storage_read_key_byte: 1,
            storage_read_value_byte: 1,
            storage_remove_base: 1,
            storage_remove_key_byte: 1,
            storage_remove_ret_value_byte: 1,
            storage_has_key_base: 1,
            storage_has_key_byte: 1,
            storage_iter_create_prefix_base: 1,
            storage_iter_create_prefix_byte: 1,
            storage_iter_create_range_base: 1,
            storage_iter_create_from_byte: 1,
            storage_iter_create_to_byte: 1,
            storage_iter_next_base: 1,
            storage_iter_next_key_byte: 1,
            storage_iter_next_value_byte: 1,
            touching_trie_node: 1,
            promise_and_base: 1,
            promise_and_per_promise: 1,
            promise_return: 1,
        }
    }
}

impl ExtCostsConfig {
    fn free() -> ExtCostsConfig {
        ExtCostsConfig {
            base: 0,
            read_memory_base: 0,
            read_memory_byte: 0,
            write_memory_base: 0,
            write_memory_byte: 0,
            read_register_base: 0,
            read_register_byte: 0,
            write_register_base: 0,
            write_register_byte: 0,
            utf8_decoding_base: 0,
            utf8_decoding_byte: 0,
            utf16_decoding_base: 0,
            utf16_decoding_byte: 0,
            sha256_base: 0,
            sha256_byte: 0,
            log_base: 0,
            log_byte: 0,
            storage_write_base: 0,
            storage_write_key_byte: 0,
            storage_write_value_byte: 0,
            storage_write_evicted_byte: 0,
            storage_read_base: 0,
            storage_read_key_byte: 0,
            storage_read_value_byte: 0,
            storage_remove_base: 0,
            storage_remove_key_byte: 0,
            storage_remove_ret_value_byte: 0,
            storage_has_key_base: 0,
            storage_has_key_byte: 0,
            storage_iter_create_prefix_base: 0,
            storage_iter_create_prefix_byte: 0,
            storage_iter_create_range_base: 0,
            storage_iter_create_from_byte: 0,
            storage_iter_create_to_byte: 0,
            storage_iter_next_base: 0,
            storage_iter_next_key_byte: 0,
            storage_iter_next_value_byte: 0,
            touching_trie_node: 0,
            promise_and_base: 0,
            promise_and_per_promise: 0,
            promise_return: 0,
        }
    }
}

/// Strongly-typed representation of the fees for counting.
#[derive(Copy, Clone, Hash, PartialEq, Eq, Debug, PartialOrd, Ord)]
#[allow(non_camel_case_types)]
pub enum ExtCosts {
    base,
    read_memory_base,
    read_memory_byte,
    write_memory_base,
    write_memory_byte,
    read_register_base,
    read_register_byte,
    write_register_base,
    write_register_byte,
    utf8_decoding_base,
    utf8_decoding_byte,
    utf16_decoding_base,
    utf16_decoding_byte,
    sha256_base,
    sha256_byte,
    log_base,
    log_byte,
    storage_write_base,
    storage_write_key_byte,
    storage_write_value_byte,
    storage_write_evicted_byte,
    storage_read_base,
    storage_read_key_byte,
    storage_read_value_byte,
    storage_remove_base,
    storage_remove_key_byte,
    storage_remove_ret_value_byte,
    storage_has_key_base,
    storage_has_key_byte,
    storage_iter_create_prefix_base,
    storage_iter_create_prefix_byte,
    storage_iter_create_range_base,
    storage_iter_create_from_byte,
    storage_iter_create_to_byte,
    storage_iter_next_base,
    storage_iter_next_key_byte,
    storage_iter_next_value_byte,
    touching_trie_node,
    promise_and_base,
    promise_and_per_promise,
    promise_return,
}

impl ExtCosts {
    pub fn value(&self, config: &ExtCostsConfig) -> Gas {
        use ExtCosts::*;
        match self {
            base => config.base,
            read_memory_base => config.read_memory_base,
            read_memory_byte => config.read_memory_byte,
            write_memory_base => config.write_memory_base,
            write_memory_byte => config.write_memory_byte,
            read_register_base => config.read_register_base,
            read_register_byte => config.read_register_byte,
            write_register_base => config.write_register_base,
            write_register_byte => config.write_register_byte,
            utf8_decoding_base => config.utf8_decoding_base,
            utf8_decoding_byte => config.utf8_decoding_byte,
            utf16_decoding_base => config.utf16_decoding_base,
            utf16_decoding_byte => config.utf16_decoding_byte,
            sha256_base => config.sha256_base,
            sha256_byte => config.sha256_byte,
            log_base => config.log_base,
            log_byte => config.log_byte,
            storage_write_base => config.storage_write_base,
            storage_write_key_byte => config.storage_write_key_byte,
            storage_write_value_byte => config.storage_write_value_byte,
            storage_write_evicted_byte => config.storage_write_evicted_byte,
            storage_read_base => config.storage_read_base,
            storage_read_key_byte => config.storage_read_key_byte,
            storage_read_value_byte => config.storage_read_value_byte,
            storage_remove_base => config.storage_remove_base,
            storage_remove_key_byte => config.storage_remove_key_byte,
            storage_remove_ret_value_byte => config.storage_remove_ret_value_byte,
            storage_has_key_base => config.storage_has_key_base,
            storage_has_key_byte => config.storage_has_key_byte,
            storage_iter_create_prefix_base => config.storage_iter_create_prefix_base,
            storage_iter_create_prefix_byte => config.storage_iter_create_prefix_byte,
            storage_iter_create_range_base => config.storage_iter_create_range_base,
            storage_iter_create_from_byte => config.storage_iter_create_from_byte,
            storage_iter_create_to_byte => config.storage_iter_create_to_byte,
            storage_iter_next_base => config.storage_iter_next_base,
            storage_iter_next_key_byte => config.storage_iter_next_key_byte,
            storage_iter_next_value_byte => config.storage_iter_next_value_byte,
            touching_trie_node => config.touching_trie_node,
            promise_and_base => config.promise_and_base,
            promise_and_per_promise => config.promise_and_per_promise,
            promise_return => config.promise_return,
        }
    }
}

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

    /// Describes limits for VM and Runtime.
    pub limit_config: VMLimitConfig,
}

/// Describes limits for VM and Runtime.
#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct VMLimitConfig {
    /// Max amount of gas that can be used, excluding gas attached to promises.
    pub max_gas_burnt: Gas,
    /// Max burnt gas per view method.
    pub max_gas_burnt_view: Gas,

    /// How tall the stack is allowed to grow?
    ///
    /// See https://wiki.parity.io/WebAssembly-StackHeight to find out
    /// how the stack frame cost is calculated.
    pub max_stack_height: u32,

    /// The initial number of memory pages.
    /// NOTE: It's not a limiter itself, but it's a value we use for initial_memory_pages.
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
    /// Maximum total length in bytes of all log messages.
    pub max_total_log_length: u64,

    /// Max total prepaid gas for all function call actions per receipt.
    pub max_total_prepaid_gas: Gas,

    /// Max number of actions per receipt.
    pub max_actions_per_receipt: u64,
    /// Max total length of all method names (including terminating character) for a function call
    /// permission access key.
    pub max_number_bytes_method_names: u64,
    /// Max length of any method name (without terminating character).
    pub max_length_method_name: u64,
    /// Max length of arguments in a function call action.
    pub max_arguments_length: u64,
    /// Max length of returned data
    pub max_length_returned_data: u64,
    /// Max contract size
    pub max_contract_size: u64,
    /// Max storage key size
    pub max_length_storage_key: u64,
    /// Max storage value size
    pub max_length_storage_value: u64,
    /// Max number of promises that a function call can create
    pub max_promises_per_function_call_action: u64,
    /// Max number of input data dependencies
    pub max_number_input_data_dependencies: u64,
}

impl Default for VMConfig {
    fn default() -> VMConfig {
        VMConfig {
            ext_costs: ExtCostsConfig::default(),
            grow_mem_cost: 1,
            regular_op_cost: 3856371,
            limit_config: VMLimitConfig::default(),
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
            // We shouldn't have any costs in the limit config.
            limit_config: VMLimitConfig {
                max_gas_burnt: std::u64::MAX,
                max_gas_burnt_view: std::u64::MAX,
                ..Default::default()
            },
        }
    }
}

impl Default for VMLimitConfig {
    fn default() -> Self {
        Self {
            max_gas_burnt: 2 * 10u64.pow(14), // with 10**15 block gas limit this will allow 5 calls.
            max_gas_burnt_view: 2 * 10u64.pow(14), // same as `max_gas_burnt` for now

            // NOTE: Stack height has to be 16K, otherwise Wasmer produces non-deterministic results.
            // For experimentation try `test_stack_overflow`.
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
            // Total logs size is 16Kib
            max_total_log_length: 16 * 1024,

            // Fills 10 blocks. It defines how long a single receipt might live.
            max_total_prepaid_gas: 10 * 10u64.pow(15),

            // Safety limit. Unlikely to hit it for most common transactions and receipts.
            max_actions_per_receipt: 100,
            // Should be low enough to deserialize an access key without paying.
            max_number_bytes_method_names: 2000,
            max_length_method_name: 256,            // basic safety limit
            max_arguments_length: 4 * 2u64.pow(20), // 4 Mib
            max_length_returned_data: 4 * 2u64.pow(20), // 4 Mib
            max_contract_size: 4 * 2u64.pow(20),    // 4 Mib,

            max_length_storage_key: 4 * 2u64.pow(20), // 4 Mib
            max_length_storage_value: 4 * 2u64.pow(20), // 4 Mib
            // Safety limit and unlikely abusable.
            max_promises_per_function_call_action: 1024,
            // Unlikely to hit it for normal development.
            max_number_input_data_dependencies: 128,
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

    /// Cost of getting sha256 base
    pub keccak256_base: Gas,
    /// Cost of getting sha256 per byte
    pub keccak256_byte: Gas,

    /// Cost of getting sha256 base
    pub keccak512_base: Gas,
    /// Cost of getting sha256 per byte
    pub keccak512_byte: Gas,

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

// We multiply the actual computed costs by the fixed factor to ensure we
// have certain reserve for further gas price variation.
const SAFETY_MULTIPLIER: u64 = 3;

impl Default for ExtCostsConfig {
    fn default() -> ExtCostsConfig {
        ExtCostsConfig {
            base: SAFETY_MULTIPLIER * 88420586,
            read_memory_base: SAFETY_MULTIPLIER * 861350075,
            read_memory_byte: SAFETY_MULTIPLIER * 1267132,
            write_memory_base: SAFETY_MULTIPLIER * 926910575,
            write_memory_byte: SAFETY_MULTIPLIER * 907953,
            read_register_base: SAFETY_MULTIPLIER * 831208187,
            read_register_byte: SAFETY_MULTIPLIER * 32874,
            write_register_base: SAFETY_MULTIPLIER * 946991737,
            write_register_byte: SAFETY_MULTIPLIER * 1267215,
            utf8_decoding_base: SAFETY_MULTIPLIER * 1036987687,
            utf8_decoding_byte: SAFETY_MULTIPLIER * 96447551,
            utf16_decoding_base: SAFETY_MULTIPLIER * 1197896600,
            utf16_decoding_byte: SAFETY_MULTIPLIER * 55839774,
            sha256_base: SAFETY_MULTIPLIER * 1510015500,
            sha256_byte: SAFETY_MULTIPLIER * 8038767,
            keccak256_base: SAFETY_MULTIPLIER * 1955741062,
            keccak256_byte: SAFETY_MULTIPLIER * 7156548,
            keccak512_base: SAFETY_MULTIPLIER * 1932709550,
            keccak512_byte: SAFETY_MULTIPLIER * 12217327,
            log_base: SAFETY_MULTIPLIER * 802740412,
            log_byte: SAFETY_MULTIPLIER * 5287945,
            storage_write_base: SAFETY_MULTIPLIER * 15062406375,
            storage_write_key_byte: SAFETY_MULTIPLIER * 22148551,
            storage_write_value_byte: SAFETY_MULTIPLIER * 9894040,
            storage_write_evicted_byte: SAFETY_MULTIPLIER * 9646594,
            storage_read_base: SAFETY_MULTIPLIER * 10676432125,
            storage_read_key_byte: SAFETY_MULTIPLIER * 9487999,
            storage_read_value_byte: SAFETY_MULTIPLIER * 1096628,
            storage_remove_base: SAFETY_MULTIPLIER * 11958889625,
            storage_remove_key_byte: SAFETY_MULTIPLIER * 11780808,
            storage_remove_ret_value_byte: SAFETY_MULTIPLIER * 2434614,
            storage_has_key_base: SAFETY_MULTIPLIER * 10438341750,
            storage_has_key_byte: SAFETY_MULTIPLIER * 9458739,
            storage_iter_create_prefix_base: 0,
            storage_iter_create_prefix_byte: 0,
            storage_iter_create_range_base: 0,
            storage_iter_create_from_byte: 0,
            storage_iter_create_to_byte: 0,
            storage_iter_next_base: 0,
            storage_iter_next_key_byte: 0,
            storage_iter_next_value_byte: 0,
            touching_trie_node: SAFETY_MULTIPLIER * 1921372857,
            promise_and_base: SAFETY_MULTIPLIER * 491272265,
            promise_and_per_promise: SAFETY_MULTIPLIER * 1871144,
            promise_return: SAFETY_MULTIPLIER * 186097468,
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
            keccak256_base: 0,
            keccak256_byte: 0,
            keccak512_base: 0,
            keccak512_byte: 0,
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
    keccak256_base,
    keccak256_byte,
    keccak512_base,
    keccak512_byte,
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
            keccak256_base => config.keccak256_base,
            keccak256_byte => config.keccak256_byte,
            keccak512_base => config.keccak512_base,
            keccak512_byte => config.keccak512_byte,
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

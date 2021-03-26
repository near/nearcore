use crate::types::Gas;

use core::fmt;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug, Hash, Serialize, Deserialize, PartialEq, Eq)]
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
/// TODO #4139: consider switching to strongly-typed wrappers instead of raw quantities
#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
#[serde(default)]
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
    /// Max transaction size
    #[cfg(feature = "protocol_feature_tx_size_limit")]
    pub max_transaction_size: u64,
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

            // Updating the maximum prepaid gas to limit the maximum depth of a transaction to 64
            // blocks.
            // This based on `63 * min_receipt_with_function_call_gas()`. Where 63 is max depth - 1.
            max_total_prepaid_gas: 300 * 10u64.pow(12),

            // Safety limit. Unlikely to hit it for most common transactions and receipts.
            max_actions_per_receipt: 100,
            // Should be low enough to deserialize an access key without paying.
            max_number_bytes_method_names: 2000,
            max_length_method_name: 256,            // basic safety limit
            max_arguments_length: 4 * 2u64.pow(20), // 4 Mib
            max_length_returned_data: 4 * 2u64.pow(20), // 4 Mib
            max_contract_size: 4 * 2u64.pow(20),    // 4 Mib,
            #[cfg(feature = "protocol_feature_tx_size_limit")]
            max_transaction_size: 4 * 2u64.pow(20), // 4 Mib

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
#[serde(default)]
pub struct ExtCostsConfig {
    /// Base cost for calling a host function.
    pub base: Gas,

    /// Base cost of loading and compiling contract
    pub contract_compile_base: Gas,
    /// Cost of the execution to load and compile contract
    pub contract_compile_bytes: Gas,

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

    /// Base cost of decoding utf8. It's used for `log_utf8` and `panic_utf8`.
    pub utf8_decoding_base: Gas,
    /// Cost per byte of decoding utf8. It's used for `log_utf8` and `panic_utf8`.
    pub utf8_decoding_byte: Gas,

    /// Base cost of decoding utf16. It's used for `log_utf16`.
    pub utf16_decoding_base: Gas,
    /// Cost per byte of decoding utf16. It's used for `log_utf16`.
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

    /// Cost of getting ripemd160 base
    #[cfg(feature = "protocol_feature_evm")]
    pub ripemd160_base: Gas,
    /// Cost of getting ripemd160 per byte
    #[cfg(feature = "protocol_feature_evm")]
    pub ripemd160_byte: Gas,

    /// Cost of getting blake2b base
    #[cfg(feature = "protocol_feature_evm")]
    pub blake2b_base: Gas,
    /// Cost of getting blake2b per byte
    #[cfg(feature = "protocol_feature_evm")]
    pub blake2b_byte: Gas,
    /// Cost of calculating blake2b F compression base
    #[cfg(feature = "protocol_feature_evm")]
    pub blake2b_f_base: Gas,

    /// Cost of calling ecrecover
    #[cfg(feature = "protocol_feature_evm")]
    pub ecrecover_base: Gas,

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
    /// Cost for calling `promise_and`
    pub promise_and_base: Gas,
    /// Cost for calling `promise_and` for each promise
    pub promise_and_per_promise: Gas,
    /// Cost for calling `promise_return`
    pub promise_return: Gas,

    // ###############
    // # Validator API #
    // ###############
    /// Cost of calling `validator_stake`.
    pub validator_stake_base: Gas,
    /// Cost of calling `validator_total_stake`.
    pub validator_total_stake_base: Gas,

    // #############
    // # Alt BN128 #
    // #############
    /// Base cost for multiexp
    #[cfg(feature = "protocol_feature_alt_bn128")]
    pub alt_bn128_g1_multiexp_base: Gas,
    /// byte cost for multiexp
    #[cfg(feature = "protocol_feature_alt_bn128")]
    pub alt_bn128_g1_multiexp_byte: Gas,
    /// Base cost for sum
    #[cfg(feature = "protocol_feature_alt_bn128")]
    pub alt_bn128_g1_sum_base: Gas,
    /// byte cost for sum
    #[cfg(feature = "protocol_feature_alt_bn128")]
    pub alt_bn128_g1_sum_byte: Gas,
    /// sublinear cost for items
    #[cfg(feature = "protocol_feature_alt_bn128")]
    pub alt_bn128_g1_multiexp_sublinear: Gas,
    /// Base cost for pairing check
    #[cfg(feature = "protocol_feature_alt_bn128")]
    pub alt_bn128_pairing_check_base: Gas,
    /// Cost for pairing check per byte
    #[cfg(feature = "protocol_feature_alt_bn128")]
    pub alt_bn128_pairing_check_byte: Gas,
}

// We multiply the actual computed costs by the fixed factor to ensure we
// have certain reserve for further gas price variation.
const SAFETY_MULTIPLIER: u64 = 3;

impl Default for ExtCostsConfig {
    fn default() -> ExtCostsConfig {
        ExtCostsConfig {
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
            #[cfg(feature = "protocol_feature_evm")]
            ripemd160_base: SAFETY_MULTIPLIER * 1513656750, // TODO
            #[cfg(feature = "protocol_feature_evm")]
            ripemd160_byte: SAFETY_MULTIPLIER * 8039117, // TODO
            #[cfg(feature = "protocol_feature_evm")]
            blake2b_base: SAFETY_MULTIPLIER * 1513656750, // TODO
            #[cfg(feature = "protocol_feature_evm")]
            blake2b_byte: SAFETY_MULTIPLIER * 8039117, // TODO
            #[cfg(feature = "protocol_feature_evm")]
            blake2b_f_base: SAFETY_MULTIPLIER * 1513656750, // TODO
            #[cfg(feature = "protocol_feature_evm")]
            ecrecover_base: SAFETY_MULTIPLIER * 1000000000, // TODO
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
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_g1_multiexp_base: SAFETY_MULTIPLIER * 237668976500,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_g1_multiexp_byte: SAFETY_MULTIPLIER * 1111697487,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_g1_multiexp_sublinear: SAFETY_MULTIPLIER * 1441698,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_pairing_check_base: SAFETY_MULTIPLIER * 3228502967000,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_pairing_check_byte: SAFETY_MULTIPLIER * 8858396182,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_g1_sum_base: SAFETY_MULTIPLIER * 1058438125,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_g1_sum_byte: SAFETY_MULTIPLIER * 25406181,
        }
    }
}

impl ExtCostsConfig {
    fn free() -> ExtCostsConfig {
        ExtCostsConfig {
            base: 0,
            contract_compile_base: 0,
            contract_compile_bytes: 0,
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
            #[cfg(feature = "protocol_feature_evm")]
            ripemd160_base: 0,
            #[cfg(feature = "protocol_feature_evm")]
            ripemd160_byte: 0,
            #[cfg(feature = "protocol_feature_evm")]
            blake2b_base: 0,
            #[cfg(feature = "protocol_feature_evm")]
            blake2b_byte: 0,
            #[cfg(feature = "protocol_feature_evm")]
            blake2b_f_base: 0,
            #[cfg(feature = "protocol_feature_evm")]
            ecrecover_base: 0,
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
            validator_stake_base: 0,
            validator_total_stake_base: 0,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_g1_multiexp_base: 0,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_g1_multiexp_byte: 0,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_g1_multiexp_sublinear: 0,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_pairing_check_base: 0,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_pairing_check_byte: 0,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_g1_sum_base: 0,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_g1_sum_byte: 0,
        }
    }
}

/// Strongly-typed representation of the fees for counting.
#[derive(Copy, Clone, Hash, PartialEq, Eq, Debug, PartialOrd, Ord)]
#[allow(non_camel_case_types)]
pub enum ExtCosts {
    base,
    contract_compile_base,
    contract_compile_bytes,
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
    #[cfg(feature = "protocol_feature_evm")]
    ripemd160_base,
    #[cfg(feature = "protocol_feature_evm")]
    ripemd160_byte,
    #[cfg(feature = "protocol_feature_evm")]
    blake2b_base,
    #[cfg(feature = "protocol_feature_evm")]
    blake2b_byte,
    #[cfg(feature = "protocol_feature_evm")]
    blake2b_f_base,
    #[cfg(feature = "protocol_feature_evm")]
    ecrecover_base,
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
    validator_stake_base,
    validator_total_stake_base,
    #[cfg(feature = "protocol_feature_alt_bn128")]
    alt_bn128_g1_multiexp_base,
    #[cfg(feature = "protocol_feature_alt_bn128")]
    alt_bn128_g1_multiexp_byte,
    #[cfg(feature = "protocol_feature_alt_bn128")]
    alt_bn128_g1_multiexp_sublinear,
    #[cfg(feature = "protocol_feature_alt_bn128")]
    alt_bn128_pairing_check_base,
    #[cfg(feature = "protocol_feature_alt_bn128")]
    alt_bn128_pairing_check_byte,
    #[cfg(feature = "protocol_feature_alt_bn128")]
    alt_bn128_g1_sum_base,
    #[cfg(feature = "protocol_feature_alt_bn128")]
    alt_bn128_g1_sum_byte,

    // NOTE: this should be the last element of the enum.
    __count,
}

// Type of an action, used in fees logic.
#[derive(Copy, Clone, Hash, PartialEq, Eq, Debug, PartialOrd, Ord)]
#[allow(non_camel_case_types)]
pub enum ActionCosts {
    create_account,
    delete_account,
    deploy_contract,
    function_call,
    transfer,
    stake,
    add_key,
    delete_key,
    value_return,
    new_receipt,

    // NOTE: this should be the last element of the enum.
    __count,
}

impl fmt::Display for ActionCosts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", ActionCosts::name_of(*self as usize))
    }
}

impl ActionCosts {
    pub const fn count() -> usize {
        ActionCosts::__count as usize
    }

    pub fn name_of(index: usize) -> &'static str {
        vec![
            "create_account",
            "delete_account",
            "deploy_contract",
            "function_call",
            "transfer",
            "stake",
            "add_key",
            "delete_key",
            "value_return",
            "new_receipt",
        ][index]
    }
}

impl fmt::Display for ExtCosts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", ExtCosts::name_of(*self as usize))
    }
}

impl ExtCosts {
    pub fn value(self, config: &ExtCostsConfig) -> Gas {
        use ExtCosts::*;
        match self {
            base => config.base,
            contract_compile_base => config.contract_compile_base,
            contract_compile_bytes => config.contract_compile_bytes,
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
            #[cfg(feature = "protocol_feature_evm")]
            ripemd160_base => config.ripemd160_base,
            #[cfg(feature = "protocol_feature_evm")]
            ripemd160_byte => config.ripemd160_byte,
            #[cfg(feature = "protocol_feature_evm")]
            blake2b_base => config.blake2b_base,
            #[cfg(feature = "protocol_feature_evm")]
            blake2b_byte => config.blake2b_byte,
            #[cfg(feature = "protocol_feature_evm")]
            blake2b_f_base => config.blake2b_f_base,
            #[cfg(feature = "protocol_feature_evm")]
            ecrecover_base => config.ecrecover_base,
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
            validator_stake_base => config.validator_stake_base,
            validator_total_stake_base => config.validator_total_stake_base,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_g1_multiexp_base => config.alt_bn128_g1_multiexp_base,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_g1_multiexp_byte => config.alt_bn128_g1_multiexp_byte,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_g1_multiexp_sublinear => config.alt_bn128_g1_multiexp_sublinear,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_pairing_check_base => config.alt_bn128_pairing_check_base,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_pairing_check_byte => config.alt_bn128_pairing_check_byte,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_g1_sum_base => config.alt_bn128_g1_sum_base,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_g1_sum_byte => config.alt_bn128_g1_sum_byte,

            __count => unreachable!(),
        }
    }

    pub const fn count() -> usize {
        ExtCosts::__count as usize
    }

    pub fn name_of(index: usize) -> &'static str {
        vec![
            "base",
            "contract_compile_base",
            "contract_compile_bytes",
            "read_memory_base",
            "read_memory_byte",
            "write_memory_base",
            "write_memory_byte",
            "read_register_base",
            "read_register_byte",
            "write_register_base",
            "write_register_byte",
            "utf8_decoding_base",
            "utf8_decoding_byte",
            "utf16_decoding_base",
            "utf16_decoding_byte",
            "sha256_base",
            "sha256_byte",
            "keccak256_base",
            "keccak256_byte",
            "keccak512_base",
            "keccak512_byte",
            "ripemd160_base",
            "ripemd160_byte",
            "blake2b_base",
            "blake2b_byte",
            "ecrecover_base",
            "log_base",
            "log_byte",
            "storage_write_base",
            "storage_write_key_byte",
            "storage_write_value_byte",
            "storage_write_evicted_byte",
            "storage_read_base",
            "storage_read_key_byte",
            "storage_read_value_byte",
            "storage_remove_base",
            "storage_remove_key_byte",
            "storage_remove_ret_value_byte",
            "storage_has_key_base",
            "storage_has_key_byte",
            "storage_iter_create_prefix_base",
            "storage_iter_create_prefix_byte",
            "storage_iter_create_range_base",
            "storage_iter_create_from_byte",
            "storage_iter_create_to_byte",
            "storage_iter_next_base",
            "storage_iter_next_key_byte",
            "storage_iter_next_value_byte",
            "touching_trie_node",
            "promise_and_base",
            "promise_and_per_promise",
            "promise_return",
            "validator_stake_base",
            "validator_total_stake_base",
            #[cfg(feature = "protocol_feature_alt_bn128")]
            "alt_bn128_g1_multiexp_base",
            #[cfg(feature = "protocol_feature_alt_bn128")]
            "alt_bn128_g1_multiexp_byte",
            #[cfg(feature = "protocol_feature_alt_bn128")]
            "alt_bn128_g1_multiexp_sublinear",
            #[cfg(feature = "protocol_feature_alt_bn128")]
            "alt_bn128_pairing_check_base",
            #[cfg(feature = "protocol_feature_alt_bn128")]
            "alt_bn128_pairing_check_byte",
            #[cfg(feature = "protocol_feature_alt_bn128")]
            "alt_bn128_g1_sum_base",
            #[cfg(feature = "protocol_feature_alt_bn128")]
            "alt_bn128_g1_sum_byte",
        ][index]
    }
}

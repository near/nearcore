use crate::parameter::Parameter;
use crate::types::Gas;

use enum_map::{enum_map, EnumMap};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use strum::Display;

/// Dynamic configuration parameters required for the WASM runtime to
/// execute a smart contract.
///
/// This (`VMConfig`) and `RuntimeFeesConfig` combined are sufficient to define
/// protocol specific behavior of the contract runtime. The former contains
/// configuration for the WASM runtime specifically, while the latter contains
/// configuration for the transaction runtime and WASM runtime.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
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
pub struct VMLimitConfig {
    /// Max amount of gas that can be used, excluding gas attached to promises.
    pub max_gas_burnt: Gas,

    /// How tall the stack is allowed to grow?
    ///
    /// See <https://wiki.parity.io/WebAssembly-StackHeight> to find out
    /// how the stack frame cost is calculated.
    pub max_stack_height: u32,
    /// Whether a legacy version of stack limiting should be used, see
    /// [`StackLimiterVersion`].
    #[serde(default = "StackLimiterVersion::v0")]
    pub stack_limiter_version: StackLimiterVersion,

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
    pub max_transaction_size: u64,
    /// Max storage key size
    pub max_length_storage_key: u64,
    /// Max storage value size
    pub max_length_storage_value: u64,
    /// Max number of promises that a function call can create
    pub max_promises_per_function_call_action: u64,
    /// Max number of input data dependencies
    pub max_number_input_data_dependencies: u64,
    /// If present, stores max number of functions in one contract
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_functions_number_per_contract: Option<u64>,
    /// If present, stores the secondary stack limit as implemented by wasmer2.
    ///
    /// This limit should never be hit normally.
    #[serde(default = "wasmer2_stack_limit_default")]
    pub wasmer2_stack_limit: i32,
    /// If present, stores max number of locals declared globally in one contract
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_locals_per_contract: Option<u64>,
    /// Whether to enforce account_id well-formedness where it wasn't enforced
    /// historically.
    #[serde(default = "AccountIdValidityRulesVersion::v0")]
    pub account_id_validity_rules_version: AccountIdValidityRulesVersion,
}

fn wasmer2_stack_limit_default() -> i32 {
    100 * 1024
}

/// Our original code for limiting WASM stack was buggy. We fixed that, but we
/// still have to use old (`V0`) limiter for old protocol versions.
///
/// This struct here exists to enforce that the value in the config is either
/// `0` or `1`. We could have used a `bool` instead, but there's a chance that
/// our current impl isn't perfect either and would need further tweaks in the
/// future.
#[derive(
    Debug,
    Clone,
    Copy,
    Hash,
    PartialEq,
    Eq,
    serde_repr::Serialize_repr,
    serde_repr::Deserialize_repr,
)]
#[repr(u8)]
pub enum StackLimiterVersion {
    /// Old, buggy version, don't use it unless specifically to support old protocol version.
    V0,
    /// What we use in today's protocol.
    V1,
}

impl StackLimiterVersion {
    pub fn v0() -> StackLimiterVersion {
        StackLimiterVersion::V0
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    Hash,
    PartialEq,
    Eq,
    serde_repr::Serialize_repr,
    serde_repr::Deserialize_repr,
)]
#[repr(u8)]
pub enum AccountIdValidityRulesVersion {
    /// Skip account ID validation according to legacy rules.
    V0,
    /// Limit `receiver_id` in `FunctionCallPermission` to be a valid account ID.
    V1,
}

impl AccountIdValidityRulesVersion {
    fn v0() -> AccountIdValidityRulesVersion {
        AccountIdValidityRulesVersion::V0
    }
}

impl VMConfig {
    pub fn test() -> VMConfig {
        VMConfig {
            ext_costs: ExtCostsConfig::test(),
            grow_mem_cost: 1,
            regular_op_cost: (SAFETY_MULTIPLIER as u32) * 1285457,
            limit_config: VMLimitConfig::test(),
        }
    }

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
            limit_config: VMLimitConfig { max_gas_burnt: u64::MAX, ..VMLimitConfig::test() },
        }
    }
}

impl VMLimitConfig {
    pub fn test() -> Self {
        let max_contract_size = 4 * 2u64.pow(20);
        Self {
            max_gas_burnt: 2 * 10u64.pow(14), // with 10**15 block gas limit this will allow 5 calls.

            // NOTE: Stack height has to be 16K, otherwise Wasmer produces non-deterministic results.
            // For experimentation try `test_stack_overflow`.
            max_stack_height: 16 * 1024, // 16Kib of stack.
            stack_limiter_version: StackLimiterVersion::V1,
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
            max_contract_size,                      // 4 Mib,
            max_transaction_size: 4 * 2u64.pow(20), // 4 Mib

            max_length_storage_key: 4 * 2u64.pow(20), // 4 Mib
            max_length_storage_value: 4 * 2u64.pow(20), // 4 Mib
            // Safety limit and unlikely abusable.
            max_promises_per_function_call_action: 1024,
            // Unlikely to hit it for normal development.
            max_number_input_data_dependencies: 128,
            max_functions_number_per_contract: Some(10000),
            wasmer2_stack_limit: 200 * 1024,
            // To utilize a local in an useful way, at least two `local.*` instructions are
            // necessary (they only take constant operands indicating the local to access), which
            // is 4 bytes worth of code for each local.
            max_locals_per_contract: Some(max_contract_size / 4),
            account_id_validity_rules_version: AccountIdValidityRulesVersion::V1,
        }
    }
}

/// Configuration of view methods execution, during which no costs should be charged.
#[derive(Default, Clone, Serialize, Deserialize, Debug, Hash, PartialEq, Eq)]
pub struct ViewConfig {
    /// If specified, defines max burnt gas per view method.
    pub max_gas_burnt: Gas,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ExtCostsConfig {
    pub costs: EnumMap<ExtCosts, Gas>,
}

// We multiply the actual computed costs by the fixed factor to ensure we
// have certain reserve for further gas price variation.
const SAFETY_MULTIPLIER: u64 = 3;

impl ExtCostsConfig {
    pub fn cost(&self, param: ExtCosts) -> Gas {
        self.costs[param]
    }

    /// Convenience constructor to use in tests where the exact gas cost does
    /// not need to correspond to a specific protocol version.
    pub fn test() -> ExtCostsConfig {
        let costs = enum_map! {
            ExtCosts::base => SAFETY_MULTIPLIER * 88256037,
            ExtCosts::contract_loading_base => SAFETY_MULTIPLIER * 11815321,
            ExtCosts::contract_loading_bytes => SAFETY_MULTIPLIER * 72250,
            ExtCosts::read_memory_base => SAFETY_MULTIPLIER * 869954400,
            ExtCosts::read_memory_byte => SAFETY_MULTIPLIER * 1267111,
            ExtCosts::write_memory_base => SAFETY_MULTIPLIER * 934598287,
            ExtCosts::write_memory_byte => SAFETY_MULTIPLIER * 907924,
            ExtCosts::read_register_base => SAFETY_MULTIPLIER * 839055062,
            ExtCosts::read_register_byte => SAFETY_MULTIPLIER * 32854,
            ExtCosts::write_register_base => SAFETY_MULTIPLIER * 955174162,
            ExtCosts::write_register_byte => SAFETY_MULTIPLIER * 1267188,
            ExtCosts::utf8_decoding_base => SAFETY_MULTIPLIER * 1037259687,
            ExtCosts::utf8_decoding_byte => SAFETY_MULTIPLIER * 97193493,
            ExtCosts::utf16_decoding_base => SAFETY_MULTIPLIER * 1181104350,
            ExtCosts::utf16_decoding_byte => SAFETY_MULTIPLIER * 54525831,
            ExtCosts::sha256_base => SAFETY_MULTIPLIER * 1513656750,
            ExtCosts::sha256_byte => SAFETY_MULTIPLIER * 8039117,
            ExtCosts::keccak256_base => SAFETY_MULTIPLIER * 1959830425,
            ExtCosts::keccak256_byte => SAFETY_MULTIPLIER * 7157035,
            ExtCosts::keccak512_base => SAFETY_MULTIPLIER * 1937129412,
            ExtCosts::keccak512_byte => SAFETY_MULTIPLIER * 12216567,
            ExtCosts::ripemd160_base => SAFETY_MULTIPLIER * 284558362,
            #[cfg(feature = "protocol_feature_ed25519_verify")]
            ExtCosts::ed25519_verify_base => SAFETY_MULTIPLIER * 1513656750,
            #[cfg(feature = "protocol_feature_ed25519_verify")]
            ExtCosts::ed25519_verify_byte => SAFETY_MULTIPLIER * 7157035,
            ExtCosts::ripemd160_block => SAFETY_MULTIPLIER * 226702528,
            ExtCosts::ecrecover_base => SAFETY_MULTIPLIER * 1121789875000,
            ExtCosts::log_base => SAFETY_MULTIPLIER * 1181104350,
            ExtCosts::log_byte => SAFETY_MULTIPLIER * 4399597,
            ExtCosts::storage_write_base => SAFETY_MULTIPLIER * 21398912000,
            ExtCosts::storage_write_key_byte => SAFETY_MULTIPLIER * 23494289,
            ExtCosts::storage_write_value_byte => SAFETY_MULTIPLIER * 10339513,
            ExtCosts::storage_write_evicted_byte => SAFETY_MULTIPLIER * 10705769,
            ExtCosts::storage_read_base => SAFETY_MULTIPLIER * 18785615250,
            ExtCosts::storage_read_key_byte => SAFETY_MULTIPLIER * 10317511,
            ExtCosts::storage_read_value_byte => SAFETY_MULTIPLIER * 1870335,
            ExtCosts::storage_remove_base => SAFETY_MULTIPLIER * 17824343500,
            ExtCosts::storage_remove_key_byte => SAFETY_MULTIPLIER * 12740128,
            ExtCosts::storage_remove_ret_value_byte => SAFETY_MULTIPLIER * 3843852,
            ExtCosts::storage_has_key_base => SAFETY_MULTIPLIER * 18013298875,
            ExtCosts::storage_has_key_byte => SAFETY_MULTIPLIER * 10263615,
            ExtCosts::storage_iter_create_prefix_base => SAFETY_MULTIPLIER * 0,
            ExtCosts::storage_iter_create_prefix_byte => SAFETY_MULTIPLIER * 0,
            ExtCosts::storage_iter_create_range_base => SAFETY_MULTIPLIER * 0,
            ExtCosts::storage_iter_create_from_byte => SAFETY_MULTIPLIER * 0,
            ExtCosts::storage_iter_create_to_byte => SAFETY_MULTIPLIER * 0,
            ExtCosts::storage_iter_next_base => SAFETY_MULTIPLIER * 0,
            ExtCosts::storage_iter_next_key_byte => SAFETY_MULTIPLIER * 0,
            ExtCosts::storage_iter_next_value_byte => SAFETY_MULTIPLIER * 0,
            ExtCosts::touching_trie_node => SAFETY_MULTIPLIER * 5367318642,
            ExtCosts::read_cached_trie_node => SAFETY_MULTIPLIER * 760_000_000,
            ExtCosts::promise_and_base => SAFETY_MULTIPLIER * 488337800,
            ExtCosts::promise_and_per_promise => SAFETY_MULTIPLIER * 1817392,
            ExtCosts::promise_return => SAFETY_MULTIPLIER * 186717462,
            ExtCosts::validator_stake_base => SAFETY_MULTIPLIER * 303944908800,
            ExtCosts::validator_total_stake_base => SAFETY_MULTIPLIER * 303944908800,
            ExtCosts::alt_bn128_g1_multiexp_base => 713_000_000_000,
            ExtCosts::alt_bn128_g1_multiexp_element => 320_000_000_000,
            ExtCosts::alt_bn128_pairing_check_base => 9_686_000_000_000,
            ExtCosts::alt_bn128_pairing_check_element => 5_102_000_000_000,
            ExtCosts::alt_bn128_g1_sum_base => 3_000_000_000,
            ExtCosts::alt_bn128_g1_sum_element => 5_000_000_000,
        };
        ExtCostsConfig { costs }
    }

    fn free() -> ExtCostsConfig {
        ExtCostsConfig {
            costs: enum_map! {
                _ => 0
            },
        }
    }
}

/// Strongly-typed representation of the fees for counting.
#[derive(
    Copy,
    Clone,
    Hash,
    PartialEq,
    Eq,
    Debug,
    PartialOrd,
    Ord,
    Display,
    strum::EnumIter,
    enum_map::Enum,
)]
#[allow(non_camel_case_types)]
pub enum ExtCosts {
    base,
    contract_loading_base,
    contract_loading_bytes,
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
    ripemd160_base,
    ripemd160_block,
    #[cfg(feature = "protocol_feature_ed25519_verify")]
    ed25519_verify_base,
    #[cfg(feature = "protocol_feature_ed25519_verify")]
    ed25519_verify_byte,
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
    read_cached_trie_node,
    promise_and_base,
    promise_and_per_promise,
    promise_return,
    validator_stake_base,
    validator_total_stake_base,
    alt_bn128_g1_multiexp_base,
    alt_bn128_g1_multiexp_element,
    alt_bn128_pairing_check_base,
    alt_bn128_pairing_check_element,
    alt_bn128_g1_sum_base,
    alt_bn128_g1_sum_element,
}

// Type of an action, used in fees logic.
#[derive(
    Copy,
    Clone,
    Hash,
    PartialEq,
    Eq,
    Debug,
    PartialOrd,
    Ord,
    Display,
    strum::EnumIter,
    enum_map::Enum,
)]
#[allow(non_camel_case_types)]
pub enum ActionCosts {
    create_account,
    delete_account,
    deploy_contract_base,
    deploy_contract_byte,
    function_call_base,
    function_call_byte,
    transfer,
    stake,
    add_full_access_key,
    add_function_call_key_base,
    add_function_call_key_byte,
    delete_key,
    new_action_receipt,
    new_data_receipt_base,
    new_data_receipt_byte,
}

impl ExtCosts {
    pub fn value(self, config: &ExtCostsConfig) -> Gas {
        config.cost(self)
    }

    pub fn param(&self) -> Parameter {
        match self {
            ExtCosts::base => Parameter::WasmBase,
            ExtCosts::contract_loading_base => Parameter::WasmContractLoadingBase,
            ExtCosts::contract_loading_bytes => Parameter::WasmContractLoadingBytes,
            ExtCosts::read_memory_base => Parameter::WasmReadMemoryBase,
            ExtCosts::read_memory_byte => Parameter::WasmReadMemoryByte,
            ExtCosts::write_memory_base => Parameter::WasmWriteMemoryBase,
            ExtCosts::write_memory_byte => Parameter::WasmWriteMemoryByte,
            ExtCosts::read_register_base => Parameter::WasmReadRegisterBase,
            ExtCosts::read_register_byte => Parameter::WasmReadRegisterByte,
            ExtCosts::write_register_base => Parameter::WasmWriteRegisterBase,
            ExtCosts::write_register_byte => Parameter::WasmWriteRegisterByte,
            ExtCosts::utf8_decoding_base => Parameter::WasmUtf8DecodingBase,
            ExtCosts::utf8_decoding_byte => Parameter::WasmUtf8DecodingByte,
            ExtCosts::utf16_decoding_base => Parameter::WasmUtf16DecodingBase,
            ExtCosts::utf16_decoding_byte => Parameter::WasmUtf16DecodingByte,
            ExtCosts::sha256_base => Parameter::WasmSha256Base,
            ExtCosts::sha256_byte => Parameter::WasmSha256Byte,
            ExtCosts::keccak256_base => Parameter::WasmKeccak256Base,
            ExtCosts::keccak256_byte => Parameter::WasmKeccak256Byte,
            ExtCosts::keccak512_base => Parameter::WasmKeccak512Base,
            ExtCosts::keccak512_byte => Parameter::WasmKeccak512Byte,
            ExtCosts::ripemd160_base => Parameter::WasmRipemd160Base,
            ExtCosts::ripemd160_block => Parameter::WasmRipemd160Block,
            ExtCosts::ecrecover_base => Parameter::WasmEcrecoverBase,
            #[cfg(feature = "protocol_feature_ed25519_verify")]
            ExtCosts::ed25519_verify_base => Parameter::WasmEd25519VerifyBase,
            #[cfg(feature = "protocol_feature_ed25519_verify")]
            ExtCosts::ed25519_verify_byte => Parameter::WasmEd25519VerifyByte,
            ExtCosts::log_base => Parameter::WasmLogBase,
            ExtCosts::log_byte => Parameter::WasmLogByte,
            ExtCosts::storage_write_base => Parameter::WasmStorageWriteBase,
            ExtCosts::storage_write_key_byte => Parameter::WasmStorageWriteKeyByte,
            ExtCosts::storage_write_value_byte => Parameter::WasmStorageWriteValueByte,
            ExtCosts::storage_write_evicted_byte => Parameter::WasmStorageWriteEvictedByte,
            ExtCosts::storage_read_base => Parameter::WasmStorageReadBase,
            ExtCosts::storage_read_key_byte => Parameter::WasmStorageReadKeyByte,
            ExtCosts::storage_read_value_byte => Parameter::WasmStorageReadValueByte,
            ExtCosts::storage_remove_base => Parameter::WasmStorageRemoveBase,
            ExtCosts::storage_remove_key_byte => Parameter::WasmStorageRemoveKeyByte,
            ExtCosts::storage_remove_ret_value_byte => Parameter::WasmStorageRemoveRetValueByte,
            ExtCosts::storage_has_key_base => Parameter::WasmStorageHasKeyBase,
            ExtCosts::storage_has_key_byte => Parameter::WasmStorageHasKeyByte,
            ExtCosts::storage_iter_create_prefix_base => Parameter::WasmStorageIterCreatePrefixBase,
            ExtCosts::storage_iter_create_prefix_byte => Parameter::WasmStorageIterCreatePrefixByte,
            ExtCosts::storage_iter_create_range_base => Parameter::WasmStorageIterCreateRangeBase,
            ExtCosts::storage_iter_create_from_byte => Parameter::WasmStorageIterCreateFromByte,
            ExtCosts::storage_iter_create_to_byte => Parameter::WasmStorageIterCreateToByte,
            ExtCosts::storage_iter_next_base => Parameter::WasmStorageIterNextBase,
            ExtCosts::storage_iter_next_key_byte => Parameter::WasmStorageIterNextKeyByte,
            ExtCosts::storage_iter_next_value_byte => Parameter::WasmStorageIterNextValueByte,
            ExtCosts::touching_trie_node => Parameter::WasmTouchingTrieNode,
            ExtCosts::read_cached_trie_node => Parameter::WasmReadCachedTrieNode,
            ExtCosts::promise_and_base => Parameter::WasmPromiseAndBase,
            ExtCosts::promise_and_per_promise => Parameter::WasmPromiseAndPerPromise,
            ExtCosts::promise_return => Parameter::WasmPromiseReturn,
            ExtCosts::validator_stake_base => Parameter::WasmValidatorStakeBase,
            ExtCosts::validator_total_stake_base => Parameter::WasmValidatorTotalStakeBase,
            ExtCosts::alt_bn128_g1_multiexp_base => Parameter::WasmAltBn128G1MultiexpBase,
            ExtCosts::alt_bn128_g1_multiexp_element => Parameter::WasmAltBn128G1MultiexpElement,
            ExtCosts::alt_bn128_pairing_check_base => Parameter::WasmAltBn128PairingCheckBase,
            ExtCosts::alt_bn128_pairing_check_element => Parameter::WasmAltBn128PairingCheckElement,
            ExtCosts::alt_bn128_g1_sum_base => Parameter::WasmAltBn128G1SumBase,
            ExtCosts::alt_bn128_g1_sum_element => Parameter::WasmAltBn128G1SumElement,
        }
    }
}

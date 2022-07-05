use crate::types::Gas;

use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use strum::{Display, EnumCount};

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
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum StackLimiterVersion {
    /// Old, buggy version, don't use it unless specifically to support old protocol version.
    V0,
    /// What we use in today's protocol.
    V1,
}

impl StackLimiterVersion {
    fn v0() -> StackLimiterVersion {
        StackLimiterVersion::V0
    }
    fn repr(self) -> u32 {
        match self {
            StackLimiterVersion::V0 => 0,
            StackLimiterVersion::V1 => 1,
        }
    }
    fn from_repr(repr: u32) -> Option<StackLimiterVersion> {
        let res = match repr {
            0 => StackLimiterVersion::V0,
            1 => StackLimiterVersion::V1,
            _ => return None,
        };
        Some(res)
    }
}

impl Serialize for StackLimiterVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.repr().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for StackLimiterVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        u32::deserialize(deserializer).and_then(|repr| {
            StackLimiterVersion::from_repr(repr)
                .ok_or_else(|| serde::de::Error::custom("invalid stack_limiter_version"))
        })
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
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
    fn repr(self) -> u32 {
        match self {
            AccountIdValidityRulesVersion::V0 => 0,
            AccountIdValidityRulesVersion::V1 => 1,
        }
    }
    fn from_repr(repr: u32) -> Option<AccountIdValidityRulesVersion> {
        let res = match repr {
            0 => AccountIdValidityRulesVersion::V0,
            1 => AccountIdValidityRulesVersion::V1,
            _ => return None,
        };
        Some(res)
    }
}

impl Serialize for AccountIdValidityRulesVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.repr().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AccountIdValidityRulesVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        u32::deserialize(deserializer).and_then(|repr| {
            AccountIdValidityRulesVersion::from_repr(repr)
                .ok_or_else(|| serde::de::Error::custom("invalid account_id_validity_rules"))
        })
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

#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct ExtCostsConfig {
    /// Base cost for calling a host function.
    pub base: Gas,

    /// Base cost of loading a pre-compiled contract
    pub contract_loading_base: Gas,
    /// Cost per byte of loading a pre-compiled contract
    pub contract_loading_bytes: Gas,

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

    /// Cost of getting sha3 512 base
    pub sha3512_base: Gas,
    /// Cost of getting sha3 512 per byte
    pub sha3512_byte: Gas,

    /// Cost of getting blake2 256 base
    pub blake2_256_base: Gas,
    /// Cost of getting blake2 256 per byte
    pub blake2_256_byte: Gas,

    /// Cost of getting sha256 base
    pub keccak256_base: Gas,
    /// Cost of getting sha256 per byte
    pub keccak256_byte: Gas,

    /// Cost of getting sha256 base
    pub keccak512_base: Gas,
    /// Cost of getting sha256 per byte
    pub keccak512_byte: Gas,

    /// Cost of getting ripemd160 base
    pub ripemd160_base: Gas,
    /// Cost of getting ripemd160 per message block
    pub ripemd160_block: Gas,

    /// Cost of getting ed25519 base
    pub ed25519_verify_base: Gas,
    /// Cost of getting ed25519 per byte
    pub ed25519_verify_byte: Gas,

    /// Cost of getting sr25519 base
    pub sr25519_verify_base: Gas,
    /// Cost of getting sr25519 per byte
    pub sr25519_verify_byte: Gas,

    /// Cost of calling ecrecover
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

    /// Cost per reading trie node from DB
    pub touching_trie_node: Gas,
    /// Cost for reading trie node from memory
    #[serde(default = "default_read_cached_trie_node")]
    pub read_cached_trie_node: Gas,

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

    // Workaround to keep JSON serialization backwards-compatible
    // <https://github.com/near/nearcore/pull/6587#discussion_r876113324>.
    //
    // Remove once #5516 is fixed.
    #[serde(default, rename = "contract_compile_base")]
    pub _unused1: Gas,
    #[serde(default, rename = "contract_compile_bytes")]
    pub _unused2: Gas,

    // #############
    // # Alt BN128 #
    // #############
    /// Base cost for multiexp
    pub alt_bn128_g1_multiexp_base: Gas,
    /// Per element cost for multiexp
    pub alt_bn128_g1_multiexp_element: Gas,
    /// Base cost for sum
    pub alt_bn128_g1_sum_base: Gas,
    /// Per element cost for sum
    pub alt_bn128_g1_sum_element: Gas,
    /// Base cost for pairing check
    pub alt_bn128_pairing_check_base: Gas,
    /// Per element cost for pairing check
    pub alt_bn128_pairing_check_element: Gas,
}

fn default_read_cached_trie_node() -> Gas {
    SAFETY_MULTIPLIER * 760_000_000
}

// We multiply the actual computed costs by the fixed factor to ensure we
// have certain reserve for further gas price variation.
const SAFETY_MULTIPLIER: u64 = 3;

impl ExtCostsConfig {
    /// Convenience constructor to use in tests where the exact gas cost does
    /// not need to correspond to a specific protocol version.
    pub fn test() -> ExtCostsConfig {
        ExtCostsConfig {
            base: SAFETY_MULTIPLIER * 88256037,
            contract_loading_base: SAFETY_MULTIPLIER * 11815321,
            contract_loading_bytes: SAFETY_MULTIPLIER * 72250,
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
            sha3512_base: SAFETY_MULTIPLIER * 1513656750,
            sha3512_byte: SAFETY_MULTIPLIER * 8039117,
            blake2_256_base: SAFETY_MULTIPLIER * 1513656750,
            blake2_256_byte: SAFETY_MULTIPLIER * 8039117,
            keccak256_base: SAFETY_MULTIPLIER * 1959830425,
            keccak256_byte: SAFETY_MULTIPLIER * 7157035,
            keccak512_base: SAFETY_MULTIPLIER * 1937129412,
            keccak512_byte: SAFETY_MULTIPLIER * 12216567,
            ripemd160_base: SAFETY_MULTIPLIER * 284558362,
            ed25519_verify_base: SAFETY_MULTIPLIER * 1513656750,
            ed25519_verify_byte: SAFETY_MULTIPLIER * 7157035,
            sr25519_verify_base: SAFETY_MULTIPLIER * 1513656750,
            sr25519_verify_byte: SAFETY_MULTIPLIER * 7157035,
            // Cost per byte is 3542227. There are 64 bytes in a block.
            ripemd160_block: SAFETY_MULTIPLIER * 226702528,
            ecrecover_base: SAFETY_MULTIPLIER * 1121789875000,
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
            read_cached_trie_node: default_read_cached_trie_node(),
            promise_and_base: SAFETY_MULTIPLIER * 488337800,
            promise_and_per_promise: SAFETY_MULTIPLIER * 1817392,
            promise_return: SAFETY_MULTIPLIER * 186717462,
            validator_stake_base: SAFETY_MULTIPLIER * 303944908800,
            validator_total_stake_base: SAFETY_MULTIPLIER * 303944908800,
            _unused1: 0,
            _unused2: 0,
            alt_bn128_g1_multiexp_base: 713_000_000_000,
            alt_bn128_g1_multiexp_element: 320_000_000_000,
            alt_bn128_pairing_check_base: 9_686_000_000_000,
            alt_bn128_pairing_check_element: 5_102_000_000_000,
            alt_bn128_g1_sum_base: 3_000_000_000,
            alt_bn128_g1_sum_element: 5_000_000_000,
        }
    }

    fn free() -> ExtCostsConfig {
        ExtCostsConfig {
            base: 0,
            contract_loading_base: 0,
            contract_loading_bytes: 0,
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
            sha3512_base: 0,
            sha3512_byte: 0,
            blake2_256_base: 0,
            blake2_256_byte: 0,
            keccak256_base: 0,
            keccak256_byte: 0,
            keccak512_base: 0,
            keccak512_byte: 0,
            ripemd160_base: 0,
            ripemd160_block: 0,
            ed25519_verify_base: 0,
            ed25519_verify_byte: 0,
            sr25519_verify_base: 0,
            sr25519_verify_byte: 0,
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
            read_cached_trie_node: 0,
            promise_and_base: 0,
            promise_and_per_promise: 0,
            promise_return: 0,
            validator_stake_base: 0,
            validator_total_stake_base: 0,
            _unused1: 0,
            _unused2: 0,
            alt_bn128_g1_multiexp_base: 0,
            alt_bn128_g1_multiexp_element: 0,
            alt_bn128_pairing_check_base: 0,
            alt_bn128_pairing_check_element: 0,
            alt_bn128_g1_sum_base: 0,
            alt_bn128_g1_sum_element: 0,
        }
    }
}

/// Strongly-typed representation of the fees for counting.
#[derive(Copy, Clone, Hash, PartialEq, Eq, Debug, PartialOrd, Ord, EnumCount, Display)]
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
    sha3512_base,
    sha3512_byte,
    blake2_256_base,
    blake2_256_byte,
    keccak256_base,
    keccak256_byte,
    keccak512_base,
    keccak512_byte,
    ripemd160_base,
    ripemd160_block,
    ed25519_verify_base,
    ed25519_verify_byte,
    sr25519_verify_base,
    sr25519_verify_byte,
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
#[derive(Copy, Clone, Hash, PartialEq, Eq, Debug, PartialOrd, Ord, EnumCount, Display)]
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
}

impl ExtCosts {
    pub fn value(self, config: &ExtCostsConfig) -> Gas {
        use ExtCosts::*;
        match self {
            base => config.base,
            contract_loading_base => config.contract_loading_base,
            contract_loading_bytes => config.contract_loading_bytes,
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
            sha3512_base => config.sha3512_base,
            sha3512_byte => config.sha3512_byte,
            blake2_256_base => config.blake2_256_base,
            blake2_256_byte => config.blake2_256_byte,
            keccak256_base => config.keccak256_base,
            keccak256_byte => config.keccak256_byte,
            keccak512_base => config.keccak512_base,
            keccak512_byte => config.keccak512_byte,
            ripemd160_base => config.ripemd160_base,
            ripemd160_block => config.ripemd160_block,
            ed25519_verify_base => config.ed25519_verify_base,
            ed25519_verify_byte => config.ed25519_verify_byte,
            sr25519_verify_base => config.sr25519_verify_base,
            sr25519_verify_byte => config.sr25519_verify_byte,
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
            read_cached_trie_node => config.read_cached_trie_node,
            promise_and_base => config.promise_and_base,
            promise_and_per_promise => config.promise_and_per_promise,
            promise_return => config.promise_return,
            validator_stake_base => config.validator_stake_base,
            validator_total_stake_base => config.validator_total_stake_base,
            alt_bn128_g1_multiexp_base => config.alt_bn128_g1_multiexp_base,
            alt_bn128_g1_multiexp_element => config.alt_bn128_g1_multiexp_element,
            alt_bn128_pairing_check_base => config.alt_bn128_pairing_check_base,
            alt_bn128_pairing_check_element => config.alt_bn128_pairing_check_element,
            alt_bn128_g1_sum_base => config.alt_bn128_g1_sum_base,
            alt_bn128_g1_sum_element => config.alt_bn128_g1_sum_element,
        }
    }
}

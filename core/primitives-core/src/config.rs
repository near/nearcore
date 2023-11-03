use crate::parameter::Parameter;
use crate::types::{Compute, Gas};
use enum_map::{enum_map, EnumMap};
use std::hash::Hash;
use strum::Display;

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
    pub fn v0() -> AccountIdValidityRulesVersion {
        AccountIdValidityRulesVersion::V0
    }
}

/// Configuration of view methods execution, during which no costs should be charged.
#[derive(Default, Clone, serde::Serialize, serde::Deserialize, Debug, Hash, PartialEq, Eq)]
pub struct ViewConfig {
    /// If specified, defines max burnt gas per view method.
    pub max_gas_burnt: Gas,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ParameterCost {
    pub gas: Gas,
    pub compute: Compute,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ExtCostsConfig {
    pub costs: EnumMap<ExtCosts, ParameterCost>,
}

// We multiply the actual computed costs by the fixed factor to ensure we
// have certain reserve for further gas price variation.
const SAFETY_MULTIPLIER: u64 = 3;

impl ExtCostsConfig {
    pub fn gas_cost(&self, param: ExtCosts) -> Gas {
        self.costs[param].gas
    }

    pub fn compute_cost(&self, param: ExtCosts) -> Compute {
        self.costs[param].compute
    }

    /// Convenience constructor to use in tests where the exact gas cost does
    /// not need to correspond to a specific protocol version.
    pub fn test_with_undercharging_factor(factor: u64) -> ExtCostsConfig {
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
            ExtCosts::ed25519_verify_base => SAFETY_MULTIPLIER * 1513656750,
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
            // Here it should be `SAFETY_MULTIPLIER * 0` for consistency, but then
            // clippy complains with "this operation will always return zero" warning
            ExtCosts::storage_iter_create_prefix_base => 0,
            ExtCosts::storage_iter_create_prefix_byte => 0,
            ExtCosts::storage_iter_create_range_base => 0,
            ExtCosts::storage_iter_create_from_byte => 0,
            ExtCosts::storage_iter_create_to_byte => 0,
            ExtCosts::storage_iter_next_base => 0,
            ExtCosts::storage_iter_next_key_byte => 0,
            ExtCosts::storage_iter_next_value_byte => 0,
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
        }
        .map(|_, value| ParameterCost { gas: value, compute: value * factor });
        ExtCostsConfig { costs }
    }

    /// `test_with_undercharging_factor` with a factor of 1.
    pub fn test() -> ExtCostsConfig {
        Self::test_with_undercharging_factor(1)
    }
}

/// Strongly-typed representation of the fees for counting.
///
/// Do not change the enum discriminants here, they are used for borsh
/// (de-)serialization.
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
    base = 0,
    contract_loading_base = 1,
    contract_loading_bytes = 2,
    read_memory_base = 3,
    read_memory_byte = 4,
    write_memory_base = 5,
    write_memory_byte = 6,
    read_register_base = 7,
    read_register_byte = 8,
    write_register_base = 9,
    write_register_byte = 10,
    utf8_decoding_base = 11,
    utf8_decoding_byte = 12,
    utf16_decoding_base = 13,
    utf16_decoding_byte = 14,
    sha256_base = 15,
    sha256_byte = 16,
    keccak256_base = 17,
    keccak256_byte = 18,
    keccak512_base = 19,
    keccak512_byte = 20,
    ripemd160_base = 21,
    ripemd160_block = 22,
    ecrecover_base = 23,
    log_base = 24,
    log_byte = 25,
    storage_write_base = 26,
    storage_write_key_byte = 27,
    storage_write_value_byte = 28,
    storage_write_evicted_byte = 29,
    storage_read_base = 30,
    storage_read_key_byte = 31,
    storage_read_value_byte = 32,
    storage_remove_base = 33,
    storage_remove_key_byte = 34,
    storage_remove_ret_value_byte = 35,
    storage_has_key_base = 36,
    storage_has_key_byte = 37,
    storage_iter_create_prefix_base = 38,
    storage_iter_create_prefix_byte = 39,
    storage_iter_create_range_base = 40,
    storage_iter_create_from_byte = 41,
    storage_iter_create_to_byte = 42,
    storage_iter_next_base = 43,
    storage_iter_next_key_byte = 44,
    storage_iter_next_value_byte = 45,
    touching_trie_node = 46,
    read_cached_trie_node = 47,
    promise_and_base = 48,
    promise_and_per_promise = 49,
    promise_return = 50,
    validator_stake_base = 51,
    validator_total_stake_base = 52,
    alt_bn128_g1_multiexp_base = 53,
    alt_bn128_g1_multiexp_element = 54,
    alt_bn128_pairing_check_base = 55,
    alt_bn128_pairing_check_element = 56,
    alt_bn128_g1_sum_base = 57,
    alt_bn128_g1_sum_element = 58,
    ed25519_verify_base = 59,
    ed25519_verify_byte = 60,
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
    create_account = 0,
    delete_account = 1,
    deploy_contract_base = 2,
    deploy_contract_byte = 3,
    function_call_base = 4,
    function_call_byte = 5,
    transfer = 6,
    stake = 7,
    add_full_access_key = 8,
    add_function_call_key_base = 9,
    add_function_call_key_byte = 10,
    delete_key = 11,
    new_action_receipt = 12,
    new_data_receipt_base = 13,
    new_data_receipt_byte = 14,
    delegate = 15,
}

impl ExtCosts {
    pub fn gas(self, config: &ExtCostsConfig) -> Gas {
        config.gas_cost(self)
    }

    pub fn compute(self, config: &ExtCostsConfig) -> Compute {
        config.compute_cost(self)
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
            ExtCosts::ed25519_verify_base => Parameter::WasmEd25519VerifyBase,
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

use crate::parameter::Parameter;
use enum_map::{enum_map, EnumMap};
use near_account_id::AccountType;
use near_primitives_core::types::{Balance, Compute, Gas};
use num_rational::Rational32;

/// Costs associated with an object that can only be sent over the network (and executed
/// by the receiver).
/// NOTE: `send_sir` or `send_not_sir` fees are usually burned when the item is being created.
/// And `execution` fee is burned when the item is being executed.
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Hash, PartialEq, Eq)]
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
    pub fn min_send_and_exec_fee(&self) -> Gas {
        std::cmp::min(self.send_sir, self.send_not_sir) + self.execution
    }
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
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_p1_sum_base => SAFETY_MULTIPLIER * 5_323_753_562,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_p1_sum_element => SAFETY_MULTIPLIER * 1_849_277_517,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_p2_sum_base => SAFETY_MULTIPLIER * 6_127_214_375,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_p2_sum_element => SAFETY_MULTIPLIER * 4_795_078_317,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_g1_multiexp_base => SAFETY_MULTIPLIER * 5_343_057_937,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_g1_multiexp_element => SAFETY_MULTIPLIER * 303_492_318_049,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_g2_multiexp_base => SAFETY_MULTIPLIER * 6_125_490_750,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_g2_multiexp_element => SAFETY_MULTIPLIER * 662_693_769_698,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_map_fp_to_g1_base => SAFETY_MULTIPLIER * 429_295_312,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_map_fp_to_g1_element => SAFETY_MULTIPLIER * 83_966_509_235,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_map_fp2_to_g2_base => SAFETY_MULTIPLIER * 419_023_687,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_map_fp2_to_g2_element => SAFETY_MULTIPLIER * 296_426_876_275,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_pairing_base => SAFETY_MULTIPLIER * 703_404_906_937,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_pairing_element => SAFETY_MULTIPLIER * 780_117_299_425,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_p1_decompress_base => SAFETY_MULTIPLIER * 432_901_062,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_p1_decompress_element => SAFETY_MULTIPLIER * 26_847_602_796,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_p2_decompress_base => SAFETY_MULTIPLIER * 460_172_000,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_p2_decompress_element => SAFETY_MULTIPLIER * 54_562_325_721,
            // TODO(yield/resume): replicate fees here after estimation
            ExtCosts::yield_create_base => 300_000_000_000_000,
            ExtCosts::yield_create_byte => 300_000_000_000_000,
            ExtCosts::yield_resume_base => 300_000_000_000_000,
            ExtCosts::yield_resume_byte => 300_000_000_000_000,
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
    strum::Display,
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
    yield_create_base = 61,
    yield_create_byte = 62,
    yield_resume_base = 63,
    yield_resume_byte = 64,
    #[cfg(feature = "protocol_feature_bls12381")]
    bls12381_p1_sum_base = 65,
    #[cfg(feature = "protocol_feature_bls12381")]
    bls12381_p1_sum_element = 66,
    #[cfg(feature = "protocol_feature_bls12381")]
    bls12381_p2_sum_base = 67,
    #[cfg(feature = "protocol_feature_bls12381")]
    bls12381_p2_sum_element = 68,
    #[cfg(feature = "protocol_feature_bls12381")]
    bls12381_g1_multiexp_base = 69,
    #[cfg(feature = "protocol_feature_bls12381")]
    bls12381_g1_multiexp_element = 70,
    #[cfg(feature = "protocol_feature_bls12381")]
    bls12381_g2_multiexp_base = 71,
    #[cfg(feature = "protocol_feature_bls12381")]
    bls12381_g2_multiexp_element = 72,
    #[cfg(feature = "protocol_feature_bls12381")]
    bls12381_map_fp_to_g1_base = 73,
    #[cfg(feature = "protocol_feature_bls12381")]
    bls12381_map_fp_to_g1_element = 74,
    #[cfg(feature = "protocol_feature_bls12381")]
    bls12381_map_fp2_to_g2_base = 75,
    #[cfg(feature = "protocol_feature_bls12381")]
    bls12381_map_fp2_to_g2_element = 76,
    #[cfg(feature = "protocol_feature_bls12381")]
    bls12381_pairing_base = 77,
    #[cfg(feature = "protocol_feature_bls12381")]
    bls12381_pairing_element = 78,
    #[cfg(feature = "protocol_feature_bls12381")]
    bls12381_p1_decompress_base = 79,
    #[cfg(feature = "protocol_feature_bls12381")]
    bls12381_p1_decompress_element = 80,
    #[cfg(feature = "protocol_feature_bls12381")]
    bls12381_p2_decompress_base = 81,
    #[cfg(feature = "protocol_feature_bls12381")]
    bls12381_p2_decompress_element = 82,
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
    strum::Display,
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
            ExtCosts::yield_create_base => Parameter::WasmYieldCreateBase,
            ExtCosts::yield_create_byte => Parameter::WasmYieldCreateByte,
            ExtCosts::yield_resume_base => Parameter::WasmYieldResumeBase,
            ExtCosts::yield_resume_byte => Parameter::WasmYieldResumeBase,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_p1_sum_base => Parameter::WasmBls12381P1SumBase,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_p1_sum_element => Parameter::WasmBls12381P1SumElement,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_p2_sum_base => Parameter::WasmBls12381P2SumBase,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_p2_sum_element => Parameter::WasmBls12381P2SumElement,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_g1_multiexp_base => Parameter::WasmBls12381G1MultiexpBase,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_g1_multiexp_element => Parameter::WasmBls12381G1MultiexpElement,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_g2_multiexp_base => Parameter::WasmBls12381G2MultiexpBase,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_g2_multiexp_element => Parameter::WasmBls12381G2MultiexpElement,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_map_fp_to_g1_base => Parameter::WasmBls12381MapFpToG1Base,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_map_fp_to_g1_element => Parameter::WasmBls12381MapFpToG1Element,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_map_fp2_to_g2_base => Parameter::WasmBls12381MapFp2ToG2Base,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_map_fp2_to_g2_element => Parameter::WasmBls12381MapFp2ToG2Element,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_pairing_base => Parameter::WasmBls12381PairingBase,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_pairing_element => Parameter::WasmBls12381PairingElement,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_p1_decompress_base => Parameter::WasmBls12381P1DecompressBase,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_p1_decompress_element => Parameter::WasmBls12381P1DecompressElement,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_p2_decompress_base => Parameter::WasmBls12381P2DecompressBase,
            #[cfg(feature = "protocol_feature_bls12381")]
            ExtCosts::bls12381_p2_decompress_element => Parameter::WasmBls12381P2DecompressElement,
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct RuntimeFeesConfig {
    /// Gas fees for sending and executing actions.
    pub action_fees: EnumMap<ActionCosts, Fee>,

    /// Describes fees for storage.
    pub storage_usage_config: StorageUsageConfig,

    /// Fraction of the burnt gas to reward to the contract account for execution.
    pub burnt_gas_reward: Rational32,

    /// Pessimistic gas price inflation ratio.
    pub pessimistic_gas_price_inflation_ratio: Rational32,
}

/// Describes cost of storage per block
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct StorageUsageConfig {
    /// Amount of yN per byte required to have on the account. See
    /// <https://nomicon.io/Economics/README.html#state-stake> for details.
    pub storage_amount_per_byte: Balance,
    /// Number of bytes for an account record, including rounding up for account id.
    pub num_bytes_account: u64,
    /// Additional number of bytes for a k/v record
    pub num_extra_bytes_record: u64,
}

impl RuntimeFeesConfig {
    /// Access action fee by `ActionCosts`.
    pub fn fee(&self, cost: ActionCosts) -> &Fee {
        &self.action_fees[cost]
    }

    pub fn test() -> Self {
        Self {
            storage_usage_config: StorageUsageConfig::test(),
            burnt_gas_reward: Rational32::new(3, 10),
            pessimistic_gas_price_inflation_ratio: Rational32::new(103, 100),
            action_fees: enum_map::enum_map! {
                ActionCosts::create_account => Fee {
                    send_sir: 3_850_000_000_000,
                    send_not_sir: 3_850_000_000_000,
                    execution: 3_850_000_000_000,
                },
                ActionCosts::delete_account => Fee {
                    send_sir: 147489000000,
                    send_not_sir: 147489000000,
                    execution: 147489000000,
                },
                ActionCosts::deploy_contract_base => Fee {
                    send_sir: 184765750000,
                    send_not_sir: 184765750000,
                    execution: 184765750000,
                },
                ActionCosts::deploy_contract_byte => Fee {
                    send_sir: 6812999,
                    send_not_sir: 6812999,
                    execution: 6812999,
                },
                ActionCosts::function_call_base => Fee {
                    send_sir: 2319861500000,
                    send_not_sir: 2319861500000,
                    execution: 2319861500000,
                },
                ActionCosts::function_call_byte => Fee {
                    send_sir: 2235934,
                    send_not_sir: 2235934,
                    execution: 2235934,
                },
                ActionCosts::transfer => Fee {
                    send_sir: 115123062500,
                    send_not_sir: 115123062500,
                    execution: 115123062500,
                },
                ActionCosts::stake => Fee {
                    send_sir: 141715687500,
                    send_not_sir: 141715687500,
                    execution: 102217625000,
                },
                ActionCosts::add_full_access_key => Fee {
                    send_sir: 101765125000,
                    send_not_sir: 101765125000,
                    execution: 101765125000,
                },
                ActionCosts::add_function_call_key_base => Fee {
                    send_sir: 102217625000,
                    send_not_sir: 102217625000,
                    execution: 102217625000,
                },
                ActionCosts::add_function_call_key_byte => Fee {
                    send_sir: 1925331,
                    send_not_sir: 1925331,
                    execution: 1925331,
                },
                ActionCosts::delete_key => Fee {
                    send_sir: 94946625000,
                    send_not_sir: 94946625000,
                    execution: 94946625000,
                },
                ActionCosts::new_action_receipt => Fee {
                    send_sir: 108059500000,
                    send_not_sir: 108059500000,
                    execution: 108059500000,
                },
                ActionCosts::new_data_receipt_base => Fee {
                    send_sir: 4697339419375,
                    send_not_sir: 4697339419375,
                    execution: 4697339419375,
                },
                ActionCosts::new_data_receipt_byte => Fee {
                    send_sir: 59357464,
                    send_not_sir: 59357464,
                    execution: 59357464,
                },
                ActionCosts::delegate => Fee {
                    send_sir: 200_000_000_000,
                    send_not_sir: 200_000_000_000,
                    execution: 200_000_000_000,
                },
            },
        }
    }

    pub fn free() -> Self {
        Self {
            action_fees: enum_map::enum_map! {
                _ => Fee { send_sir: 0, send_not_sir: 0, execution: 0 }
            },
            storage_usage_config: StorageUsageConfig::free(),
            burnt_gas_reward: Rational32::from_integer(0),
            pessimistic_gas_price_inflation_ratio: Rational32::from_integer(0),
        }
    }

    /// The minimum amount of gas required to create and execute a new receipt with a function call
    /// action.
    /// This amount is used to determine how many receipts can be created, send and executed for
    /// some amount of prepaid gas using function calls.
    pub fn min_receipt_with_function_call_gas(&self) -> Gas {
        self.fee(ActionCosts::new_action_receipt).min_send_and_exec_fee()
            + self.fee(ActionCosts::function_call_base).min_send_and_exec_fee()
    }
}

impl StorageUsageConfig {
    pub fn test() -> Self {
        Self {
            num_bytes_account: 100,
            num_extra_bytes_record: 40,
            storage_amount_per_byte: 909 * 100_000_000_000_000_000,
        }
    }

    pub(crate) fn free() -> StorageUsageConfig {
        Self { num_bytes_account: 0, num_extra_bytes_record: 0, storage_amount_per_byte: 0 }
    }
}

/// Helper functions for computing Transfer fees.
/// In case of implicit account creation they include extra fees for the CreateAccount and
/// AddFullAccessKey (for NEAR-implicit account only) actions that are implicit.
/// We can assume that no overflow will happen here.
pub fn transfer_exec_fee(
    cfg: &RuntimeFeesConfig,
    implicit_account_creation_allowed: bool,
    eth_implicit_accounts_enabled: bool,
    receiver_account_type: AccountType,
) -> Gas {
    let transfer_fee = cfg.fee(ActionCosts::transfer).exec_fee();
    match (implicit_account_creation_allowed, eth_implicit_accounts_enabled, receiver_account_type)
    {
        // Regular transfer to a named account.
        (_, _, AccountType::NamedAccount) => transfer_fee,
        // No account will be created, just a regular transfer.
        (false, _, _) => transfer_fee,
        // No account will be created, just a regular transfer.
        (true, false, AccountType::EthImplicitAccount) => transfer_fee,
        // Extra fee for the CreateAccount.
        (true, true, AccountType::EthImplicitAccount) => {
            transfer_fee + cfg.fee(ActionCosts::create_account).exec_fee()
        }
        // Extra fees for the CreateAccount and AddFullAccessKey.
        (true, _, AccountType::NearImplicitAccount) => {
            transfer_fee
                + cfg.fee(ActionCosts::create_account).exec_fee()
                + cfg.fee(ActionCosts::add_full_access_key).exec_fee()
        }
    }
}

pub fn transfer_send_fee(
    cfg: &RuntimeFeesConfig,
    sender_is_receiver: bool,
    implicit_account_creation_allowed: bool,
    eth_implicit_accounts_enabled: bool,
    receiver_account_type: AccountType,
) -> Gas {
    let transfer_fee = cfg.fee(ActionCosts::transfer).send_fee(sender_is_receiver);
    match (implicit_account_creation_allowed, eth_implicit_accounts_enabled, receiver_account_type)
    {
        // Regular transfer to a named account.
        (_, _, AccountType::NamedAccount) => transfer_fee,
        // No account will be created, just a regular transfer.
        (false, _, _) => transfer_fee,
        // No account will be created, just a regular transfer.
        (true, false, AccountType::EthImplicitAccount) => transfer_fee,
        // Extra fee for the CreateAccount.
        (true, true, AccountType::EthImplicitAccount) => {
            transfer_fee + cfg.fee(ActionCosts::create_account).send_fee(sender_is_receiver)
        }
        // Extra fees for the CreateAccount and AddFullAccessKey.
        (true, _, AccountType::NearImplicitAccount) => {
            transfer_fee
                + cfg.fee(ActionCosts::create_account).send_fee(sender_is_receiver)
                + cfg.fee(ActionCosts::add_full_access_key).send_fee(sender_is_receiver)
        }
    }
}

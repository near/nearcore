use near_primitives::types::Gas;
use near_vm_logic::ExtCosts;

use crate::cases::ratio_to_gas;
use crate::cost::Cost;
use crate::cost_table::CostTable;
use crate::ext_costs_generator::ExtCostsGenerator;
use crate::runtime_fees_generator::{ReceiptFees, RuntimeFeesGenerator};
use crate::stats::Measurements;

/// Convert various measurements done during estimation into a final
/// `CostTable`.
///
/// Invariant: this code won't do any additional measurements.
pub(crate) fn measurements_to_costs(
    measurements: Measurements,
    (contract_compile_base, contract_compile_per_byte): (Gas, Gas),
    wasm_instr_cost: Gas,
) -> CostTable {
    let mut res = CostTable::default();

    let ext_costs = ExtCostsGenerator::new(&measurements).compute();
    for (ext_cost, ratio) in ext_costs {
        let gas = ratio_to_gas(measurements.gas_metric, ratio);
        let cost = Cost::from(ext_cost);
        res.add(cost, gas)
    }

    let runtime_fees = RuntimeFeesGenerator::new(&measurements).compute();
    for (receipt_fee, ratio) in runtime_fees {
        let mut gas = ratio_to_gas(measurements.gas_metric, ratio);
        match receipt_fee {
            ReceiptFees::ActionDeployContractBase => gas += contract_compile_base,
            ReceiptFees::ActionDeployContractPerByte => gas += contract_compile_per_byte,
            _ => (),
        }
        let cost = Cost::from(receipt_fee);
        res.add(cost, gas)
    }

    res.add(Cost::WasmInstruction, wasm_instr_cost);

    res
}

impl From<ExtCosts> for Cost {
    fn from(ext_cost: ExtCosts) -> Cost {
        use ExtCosts::*;

        match ext_cost {
            base => Cost::HostFunctionCall,
            contract_compile_base => Cost::ContractCompileBase,
            contract_compile_bytes => Cost::ContractCompileBytes,
            read_memory_base => Cost::ReadMemoryBase,
            read_memory_byte => Cost::ReadMemoryByte,
            write_memory_base => Cost::WriteMemoryBase,
            write_memory_byte => Cost::WriteMemoryByte,
            read_register_base => Cost::ReadRegisterBase,
            read_register_byte => Cost::ReadRegisterByte,
            write_register_base => Cost::WriteRegisterBase,
            write_register_byte => Cost::WriteRegisterByte,
            utf8_decoding_base => Cost::Utf8DecodingBase,
            utf8_decoding_byte => Cost::Utf8DecodingByte,
            utf16_decoding_base => Cost::Utf16DecodingBase,
            utf16_decoding_byte => Cost::Utf16DecodingByte,
            sha256_base => Cost::Sha256Base,
            sha256_byte => Cost::Sha256Byte,
            keccak256_base => Cost::Keccak256Base,
            keccak256_byte => Cost::Keccak256Byte,
            keccak512_base => Cost::Keccak512Base,
            keccak512_byte => Cost::Keccak512Byte,
            ripemd160_base => Cost::Ripemd160Base,
            ripemd160_block => Cost::Ripemd160Block,
            ecrecover_base => Cost::EcrecoverBase,
            log_base => Cost::LogBase,
            log_byte => Cost::LogByte,
            storage_write_base => Cost::StorageWriteBase,
            storage_write_key_byte => Cost::StorageWriteKeyByte,
            storage_write_value_byte => Cost::StorageWriteValueByte,
            storage_write_evicted_byte => Cost::StorageWriteEvictedByte,
            storage_read_base => Cost::StorageReadBase,
            storage_read_key_byte => Cost::StorageReadKeyByte,
            storage_read_value_byte => Cost::StorageReadValueByte,
            storage_remove_base => Cost::StorageRemoveBase,
            storage_remove_key_byte => Cost::StorageRemoveKeyByte,
            storage_remove_ret_value_byte => Cost::StorageRemoveRetValueByte,
            storage_has_key_base => Cost::StorageHasKeyBase,
            storage_has_key_byte => Cost::StorageHasKeyByte,
            storage_iter_create_prefix_base => Cost::StorageIterCreatePrefixBase,
            storage_iter_create_prefix_byte => Cost::StorageIterCreatePrefixByte,
            storage_iter_create_range_base => Cost::StorageIterCreateRangeBase,
            storage_iter_create_from_byte => Cost::StorageIterCreateFromByte,
            storage_iter_create_to_byte => Cost::StorageIterCreateToByte,
            storage_iter_next_base => Cost::StorageIterNextBase,
            storage_iter_next_key_byte => Cost::StorageIterNextKeyByte,
            storage_iter_next_value_byte => Cost::StorageIterNextValueByte,
            touching_trie_node => Cost::TouchingTrieNode,
            promise_and_base => Cost::PromiseAndBase,
            promise_and_per_promise => Cost::PromiseAndPerPromise,
            promise_return => Cost::PromiseReturn,
            validator_stake_base => Cost::ValidatorStakeBase,
            validator_total_stake_base => Cost::ValidatorTotalStakeBase,

            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_g1_multiexp_base => Cost::AltBn128G1MultiexpBase,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_g1_multiexp_byte => Cost::AltBn128G1MultiexpByte,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_g1_multiexp_sublinear => Cost::AltBn128G1MultiexpSublinear,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_pairing_check_base => Cost::AltBn128PairingCheckBase,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_pairing_check_byte => Cost::AltBn128PairingCheckByte,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_g1_sum_base => Cost::AltBn128G1SumBase,
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_g1_sum_byte => Cost::AltBn128G1SumByte,

            __count => unreachable!(),
        }
    }
}

impl From<ReceiptFees> for Cost {
    fn from(receipt_fee: ReceiptFees) -> Self {
        match receipt_fee {
            ReceiptFees::ActionReceiptCreation => Cost::ActionReceiptCreation,
            ReceiptFees::ActionSirReceiptCreation => Cost::ActionSirReceiptCreation,
            ReceiptFees::DataReceiptCreationBase => Cost::DataReceiptCreationBase,
            ReceiptFees::DataReceiptCreationPerByte => Cost::DataReceiptCreationPerByte,
            ReceiptFees::ActionCreateAccount => Cost::ActionCreateAccount,
            ReceiptFees::ActionDeployContractBase => Cost::ActionDeployContractBase,
            ReceiptFees::ActionDeployContractPerByte => Cost::ActionDeployContractPerByte,
            ReceiptFees::ActionFunctionCallBase => Cost::ActionFunctionCallBase,
            ReceiptFees::ActionFunctionCallPerByte => Cost::ActionFunctionCallPerByte,
            ReceiptFees::ActionTransfer => Cost::ActionTransfer,
            ReceiptFees::ActionStake => Cost::ActionStake,
            ReceiptFees::ActionAddFullAccessKey => Cost::ActionAddFullAccessKey,
            ReceiptFees::ActionAddFunctionAccessKeyBase => Cost::ActionAddFunctionAccessKeyBase,
            ReceiptFees::ActionAddFunctionAccessKeyPerByte => {
                Cost::ActionAddFunctionAccessKeyPerByte
            }
            ReceiptFees::ActionDeleteKey => Cost::ActionDeleteKey,
            ReceiptFees::ActionDeleteAccount => Cost::ActionDeleteAccount,
        }
    }
}

use near_primitives::runtime::config::AccountCreationConfig;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::runtime::fees::{
    AccessKeyCreationConfig, ActionCreationConfig, DataReceiptCreationConfig, Fee,
    RuntimeFeesConfig,
};
use near_primitives::types::Gas;
use near_primitives::version::PROTOCOL_VERSION;
use near_vm_logic::{ExtCostsConfig, VMConfig};
use node_runtime::config::RuntimeConfig;

use anyhow::Context;

use crate::cost::Cost;
use crate::cost_table::CostTable;

/// Turn a [`CostTable`] into a [`RuntimeConfig`].
///
/// Will fail if [`CostTable`] doesn't contain all costs.
///
/// Note that the actual [`RuntimeConfig`] we use is currently hard-coded -- we
/// don't really use this function in production.
pub fn costs_to_runtime_config(cost_table: &CostTable) -> anyhow::Result<RuntimeConfig> {
    let regular_op_cost = cost_table
        .get(Cost::WasmInstruction)
        .with_context(|| format!("undefined cost: {}", Cost::WasmInstruction))?;

    // Take latest VM limit config, because estimation doesn't affect it.
    // Note that if you run estimation against stable version, it doesn't catch updates of nightly
    // version.
    let config_store = RuntimeConfigStore::new(None);
    let latest_runtime_config = config_store.get_config(PROTOCOL_VERSION);
    let vm_limit_config = latest_runtime_config.wasm_config.limit_config.clone();

    let res = RuntimeConfig {
        // See https://nomicon.io/Economics/README.html#general-variables for how it was calculated.
        storage_amount_per_byte: 909 * 100_000_000_000_000_000,
        transaction_costs: runtime_fees_config(cost_table)?,
        wasm_config: VMConfig {
            ext_costs: ext_costs_config(cost_table)?,
            grow_mem_cost: 1,
            regular_op_cost: u32::try_from(regular_op_cost).unwrap(),
            limit_config: vm_limit_config,
        },
        account_creation_config: AccountCreationConfig::default(),
    };
    Ok(res)
}

fn runtime_fees_config(cost_table: &CostTable) -> anyhow::Result<RuntimeFeesConfig> {
    let fee = |cost: Cost| -> anyhow::Result<Fee> {
        let total_gas =
            cost_table.get(cost).with_context(|| format!("undefined cost: {}", cost))?;
        // Split the total cost evenly between send and execution fee.
        Ok(Fee { send_sir: total_gas / 2, send_not_sir: total_gas / 2, execution: total_gas / 2 })
    };

    let config_store = RuntimeConfigStore::new(None);
    let actual_fees_config = &config_store.get_config(PROTOCOL_VERSION).transaction_costs;
    let res = RuntimeFeesConfig {
        action_receipt_creation_config: fee(Cost::ActionReceiptCreation)?,
        data_receipt_creation_config: DataReceiptCreationConfig {
            base_cost: fee(Cost::DataReceiptCreationBase)?,
            cost_per_byte: fee(Cost::DataReceiptCreationPerByte)?,
        },
        action_creation_config: ActionCreationConfig {
            create_account_cost: fee(Cost::ActionCreateAccount)?,
            deploy_contract_cost: fee(Cost::ActionDeployContractBase)?,
            deploy_contract_cost_per_byte: fee(Cost::ActionDeployContractPerByte)?,
            function_call_cost: fee(Cost::ActionFunctionCallBase)?,
            function_call_cost_per_byte: fee(Cost::ActionFunctionCallPerByte)?,
            transfer_cost: fee(Cost::ActionTransfer)?,
            stake_cost: fee(Cost::ActionStake)?,
            add_key_cost: AccessKeyCreationConfig {
                full_access_cost: fee(Cost::ActionAddFullAccessKey)?,
                function_call_cost: fee(Cost::ActionAddFunctionAccessKeyBase)?,
                function_call_cost_per_byte: fee(Cost::ActionAddFunctionAccessKeyPerByte)?,
            },
            delete_key_cost: fee(Cost::ActionDeleteKey)?,
            delete_account_cost: fee(Cost::ActionDeleteAccount)?,
        },
        ..actual_fees_config.clone()
    };
    Ok(res)
}

fn ext_costs_config(cost_table: &CostTable) -> anyhow::Result<ExtCostsConfig> {
    let get = |cost: Cost| -> anyhow::Result<Gas> {
        cost_table.get(cost).with_context(|| format!("undefined cost: {}", cost))
    };

    let res = ExtCostsConfig {
        base: get(Cost::HostFunctionCall)?,
        contract_loading_base: 0,
        contract_loading_bytes: 0,
        read_memory_base: get(Cost::ReadMemoryBase)?,
        read_memory_byte: get(Cost::ReadMemoryByte)?,
        write_memory_base: get(Cost::WriteMemoryBase)?,
        write_memory_byte: get(Cost::WriteMemoryByte)?,
        read_register_base: get(Cost::ReadRegisterBase)?,
        read_register_byte: get(Cost::ReadRegisterByte)?,
        write_register_base: get(Cost::WriteRegisterBase)?,
        write_register_byte: get(Cost::WriteRegisterByte)?,
        utf8_decoding_base: get(Cost::Utf8DecodingBase)?,
        utf8_decoding_byte: get(Cost::Utf8DecodingByte)?,
        utf16_decoding_base: get(Cost::Utf16DecodingBase)?,
        utf16_decoding_byte: get(Cost::Utf16DecodingByte)?,
        sha256_base: get(Cost::Sha256Base)?,
        sha256_byte: get(Cost::Sha256Byte)?,
        keccak256_base: get(Cost::Keccak256Base)?,
        keccak256_byte: get(Cost::Keccak256Byte)?,
        keccak512_base: get(Cost::Keccak512Base)?,
        keccak512_byte: get(Cost::Keccak512Byte)?,
        ripemd160_base: get(Cost::Ripemd160Base)?,
        ripemd160_block: get(Cost::Ripemd160Block)?,
        ecrecover_base: get(Cost::EcrecoverBase)?,
        log_base: get(Cost::LogBase)?,
        log_byte: get(Cost::LogByte)?,
        storage_write_base: get(Cost::StorageWriteBase)?,
        storage_write_key_byte: get(Cost::StorageWriteKeyByte)?,
        storage_write_value_byte: get(Cost::StorageWriteValueByte)?,
        storage_write_evicted_byte: get(Cost::StorageWriteEvictedByte)?,
        storage_read_base: get(Cost::StorageReadBase)?,
        storage_read_key_byte: get(Cost::StorageReadKeyByte)?,
        storage_read_value_byte: get(Cost::StorageReadValueByte)?,
        storage_remove_base: get(Cost::StorageRemoveBase)?,
        storage_remove_key_byte: get(Cost::StorageRemoveKeyByte)?,
        storage_remove_ret_value_byte: get(Cost::StorageRemoveRetValueByte)?,
        storage_has_key_base: get(Cost::StorageHasKeyBase)?,
        storage_has_key_byte: get(Cost::StorageHasKeyByte)?,
        // TODO: storage_iter_* operations below are deprecated, so just hardcode zero price,
        // and remove those operations ASAP.
        storage_iter_create_prefix_base: 0,
        storage_iter_create_prefix_byte: 0,
        storage_iter_create_range_base: 0,
        storage_iter_create_from_byte: 0,
        storage_iter_create_to_byte: 0,
        storage_iter_next_base: 0,
        storage_iter_next_key_byte: 0,
        storage_iter_next_value_byte: 0,
        touching_trie_node: get(Cost::TouchingTrieNode)?,
        read_cached_trie_node: get(Cost::ReadCachedTrieNode)?,
        promise_and_base: get(Cost::PromiseAndBase)?,
        promise_and_per_promise: get(Cost::PromiseAndPerPromise)?,
        promise_return: get(Cost::PromiseReturn)?,
        // TODO: accurately price host functions that expose validator information.
        validator_stake_base: 303944908800,
        validator_total_stake_base: 303944908800,
        _unused1: 0,
        _unused2: 0,
        #[cfg(feature = "protocol_feature_alt_bn128")]
        alt_bn128_g1_sum_base: get(Cost::AltBn128G1SumBase)?,
        #[cfg(feature = "protocol_feature_alt_bn128")]
        alt_bn128_g1_sum_element: get(Cost::AltBn128G1SumElement)?,
        #[cfg(feature = "protocol_feature_alt_bn128")]
        alt_bn128_g1_multiexp_base: get(Cost::AltBn128G1MultiexpBase)?,
        #[cfg(feature = "protocol_feature_alt_bn128")]
        alt_bn128_g1_multiexp_element: get(Cost::AltBn128G1MultiexpElement)?,
        #[cfg(feature = "protocol_feature_alt_bn128")]
        alt_bn128_pairing_check_base: get(Cost::AltBn128PairingCheckBase)?,
        #[cfg(feature = "protocol_feature_alt_bn128")]
        alt_bn128_pairing_check_element: get(Cost::AltBn128PairingCheckElement)?,
    };

    Ok(res)
}

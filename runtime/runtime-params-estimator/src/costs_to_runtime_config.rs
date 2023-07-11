use near_primitives::runtime::config::AccountCreationConfig;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::runtime::fees::{Fee, RuntimeFeesConfig};
use near_primitives::version::PROTOCOL_VERSION;
use near_vm_runner::logic::{ActionCosts, ExtCosts, ExtCostsConfig, ParameterCost, VMConfig};
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
        fees: runtime_fees_config(cost_table)?,
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
    let actual_fees_config = &config_store.get_config(PROTOCOL_VERSION).fees;
    let res = RuntimeFeesConfig {
        action_fees: enum_map::enum_map! {
            ActionCosts::create_account => fee(Cost::ActionCreateAccount)?,
            ActionCosts::delegate => fee(Cost::ActionDelegate)?,
            ActionCosts::delete_account => fee(Cost::ActionDeleteAccount)?,
            ActionCosts::deploy_contract_base => fee(Cost::ActionDeployContractBase)?,
            ActionCosts::deploy_contract_byte => fee(Cost::ActionDeployContractPerByte)?,
            ActionCosts::function_call_base => fee(Cost::ActionFunctionCallBase)?,
            ActionCosts::function_call_byte => fee(Cost::ActionFunctionCallPerByte)?,
            ActionCosts::transfer => fee(Cost::ActionTransfer)?,
            ActionCosts::stake => fee(Cost::ActionStake)?,
            ActionCosts::add_full_access_key => fee(Cost::ActionAddFullAccessKey)?,
            ActionCosts::add_function_call_key_base => fee(Cost::ActionAddFunctionAccessKeyBase)?,
            ActionCosts::add_function_call_key_byte => fee(Cost::ActionAddFunctionAccessKeyPerByte)?,
            ActionCosts::delete_key => fee(Cost::ActionDeleteKey)?,
            ActionCosts::new_action_receipt => fee(Cost::ActionReceiptCreation)?,
            ActionCosts::new_data_receipt_base => fee(Cost::DataReceiptCreationBase)?,
            ActionCosts::new_data_receipt_byte => fee(Cost::DataReceiptCreationPerByte)?,
        },
        ..actual_fees_config.clone()
    };
    Ok(res)
}

fn ext_costs_config(cost_table: &CostTable) -> anyhow::Result<ExtCostsConfig> {
    Ok(ExtCostsConfig {
        costs: enum_map::enum_map! {
            // TODO: storage_iter_* operations below are deprecated, so just hardcode zero price,
            // and remove those operations ASAP.
            ExtCosts::storage_iter_create_prefix_base => 0,
            ExtCosts::storage_iter_create_prefix_byte => 0,
            ExtCosts::storage_iter_create_range_base => 0,
            ExtCosts::storage_iter_create_from_byte => 0,
            ExtCosts::storage_iter_create_to_byte => 0,
            ExtCosts::storage_iter_next_base => 0,
            ExtCosts::storage_iter_next_key_byte => 0,
            ExtCosts::storage_iter_next_value_byte => 0,
            // TODO: accurately price host functions that expose validator information.
            ExtCosts::validator_stake_base => 303944908800,
            ExtCosts::validator_total_stake_base => 303944908800,
            cost => {
                let estimation = estimation(cost).with_context(|| format!("external WASM cost has no estimation defined: {}", cost))?;
                cost_table.get(estimation).with_context(|| format!("undefined external WASM cost: {}", cost))?
            },
        }.map(|_, value| ParameterCost { gas: value, compute: value }),
    })
}

fn estimation(cost: ExtCosts) -> Option<Cost> {
    Some(match cost {
        ExtCosts::base => Cost::HostFunctionCall,
        ExtCosts::read_memory_base => Cost::ReadMemoryBase,
        ExtCosts::read_memory_byte => Cost::ReadMemoryByte,
        ExtCosts::write_memory_base => Cost::WriteMemoryBase,
        ExtCosts::write_memory_byte => Cost::WriteMemoryByte,
        ExtCosts::read_register_base => Cost::ReadRegisterBase,
        ExtCosts::read_register_byte => Cost::ReadRegisterByte,
        ExtCosts::write_register_base => Cost::WriteRegisterBase,
        ExtCosts::write_register_byte => Cost::WriteRegisterByte,
        ExtCosts::utf8_decoding_base => Cost::Utf8DecodingBase,
        ExtCosts::utf8_decoding_byte => Cost::Utf8DecodingByte,
        ExtCosts::utf16_decoding_base => Cost::Utf16DecodingBase,
        ExtCosts::utf16_decoding_byte => Cost::Utf16DecodingByte,
        ExtCosts::sha256_base => Cost::Sha256Base,
        ExtCosts::sha256_byte => Cost::Sha256Byte,
        ExtCosts::keccak256_base => Cost::Keccak256Base,
        ExtCosts::keccak256_byte => Cost::Keccak256Byte,
        ExtCosts::keccak512_base => Cost::Keccak512Base,
        ExtCosts::keccak512_byte => Cost::Keccak512Byte,
        ExtCosts::ripemd160_base => Cost::Ripemd160Base,
        ExtCosts::ripemd160_block => Cost::Ripemd160Block,
        ExtCosts::ecrecover_base => Cost::EcrecoverBase,
        ExtCosts::ed25519_verify_base => Cost::Ed25519VerifyBase,
        ExtCosts::ed25519_verify_byte => Cost::Ed25519VerifyByte,
        ExtCosts::log_base => Cost::LogBase,
        ExtCosts::log_byte => Cost::LogByte,
        ExtCosts::storage_write_base => Cost::StorageWriteBase,
        ExtCosts::storage_write_key_byte => Cost::StorageWriteKeyByte,
        ExtCosts::storage_write_value_byte => Cost::StorageWriteValueByte,
        ExtCosts::storage_write_evicted_byte => Cost::StorageWriteEvictedByte,
        ExtCosts::storage_read_base => Cost::StorageReadBase,
        ExtCosts::storage_read_key_byte => Cost::StorageReadKeyByte,
        ExtCosts::storage_read_value_byte => Cost::StorageReadValueByte,
        ExtCosts::storage_remove_base => Cost::StorageRemoveBase,
        ExtCosts::storage_remove_key_byte => Cost::StorageRemoveKeyByte,
        ExtCosts::storage_remove_ret_value_byte => Cost::StorageRemoveRetValueByte,
        ExtCosts::storage_has_key_base => Cost::StorageHasKeyBase,
        ExtCosts::storage_has_key_byte => Cost::StorageHasKeyByte,
        ExtCosts::touching_trie_node => Cost::TouchingTrieNode,
        ExtCosts::read_cached_trie_node => Cost::ReadCachedTrieNode,
        ExtCosts::promise_and_base => Cost::PromiseAndBase,
        ExtCosts::promise_and_per_promise => Cost::PromiseAndPerPromise,
        ExtCosts::promise_return => Cost::PromiseReturn,
        ExtCosts::alt_bn128_g1_sum_base => Cost::AltBn128G1SumBase,
        ExtCosts::alt_bn128_g1_sum_element => Cost::AltBn128G1SumElement,
        ExtCosts::alt_bn128_g1_multiexp_base => Cost::AltBn128G1MultiexpBase,
        ExtCosts::alt_bn128_g1_multiexp_element => Cost::AltBn128G1MultiexpElement,
        ExtCosts::alt_bn128_pairing_check_base => Cost::AltBn128PairingCheckBase,
        ExtCosts::alt_bn128_pairing_check_element => Cost::AltBn128PairingCheckElement,
        _ => return None,
    })
}

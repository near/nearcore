mod cache;
mod compile_errors;
mod fuzzers;
mod regression_tests;
mod rs_contract;
mod runtime_errors;
pub(crate) mod test_builder;
mod ts_contract;
mod wasm_validation;

#[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
use crate::config::ContractPrepareVersion;
use crate::logic::{Config, VMContext};
use crate::vm_kind::VMKind;
use near_primitives_core::version::PROTOCOL_VERSION;

const CURRENT_ACCOUNT_ID: &str = "alice";
const SIGNER_ACCOUNT_ID: &str = "bob";
const SIGNER_ACCOUNT_PK: [u8; 3] = [0, 1, 2];
const PREDECESSOR_ACCOUNT_ID: &str = "carol";

impl crate::logic::Config {
    pub fn test() -> Self {
        use crate::logic::StorageGetMode;
        use near_vm_runner::logic::ContractPrepareVersion as ExtContractPrepareVersion;
        use near_vm_runner::logic::StorageGetMode as ExtStorageGetMode;
        use near_vm_runner::VMKind as ExtVMKind;
        let store = near_primitives::runtime::config_store::RuntimeConfigStore::test();
        let config = store.get_config(PROTOCOL_VERSION).wasm_config.clone();
        Self {
            ext_costs: config.ext_costs,
            grow_mem_cost: config.grow_mem_cost,
            regular_op_cost: config.regular_op_cost,
            vm_kind: match config.vm_kind {
                ExtVMKind::Wasmer0 => VMKind::Wasmer0,
                ExtVMKind::Wasmer2 => VMKind::Wasmer2,
                ExtVMKind::NearVm => VMKind::NearVm,
                ExtVMKind::Wasmtime => VMKind::Wasmtime,
            },
            disable_9393_fix: config.disable_9393_fix,
            storage_get_mode: match config.storage_get_mode {
                ExtStorageGetMode::Trie => StorageGetMode::Trie,
                ExtStorageGetMode::FlatStorage => StorageGetMode::FlatStorage,
            },
            fix_contract_loading_cost: config.fix_contract_loading_cost,
            implicit_account_creation: config.implicit_account_creation,
            math_extension: config.math_extension,
            ed25519_verify: config.ed25519_verify,
            alt_bn128: config.alt_bn128,
            function_call_weight: config.function_call_weight,
            limit_config: crate::config::LimitConfig {
                max_gas_burnt: config.limit_config.max_gas_burnt,
                max_stack_height: config.limit_config.max_stack_height,
                contract_prepare_version: match config.limit_config.contract_prepare_version {
                    ExtContractPrepareVersion::V0 => ContractPrepareVersion::V0,
                    ExtContractPrepareVersion::V1 => ContractPrepareVersion::V1,
                    ExtContractPrepareVersion::V2 => ContractPrepareVersion::V2,
                },
                initial_memory_pages: config.limit_config.initial_memory_pages,
                max_memory_pages: config.limit_config.max_memory_pages,
                registers_memory_limit: config.limit_config.registers_memory_limit,
                max_register_size: config.limit_config.max_register_size,
                max_number_registers: config.limit_config.max_number_registers,
                max_number_logs: config.limit_config.max_number_logs,
                max_total_log_length: config.limit_config.max_total_log_length,
                max_total_prepaid_gas: config.limit_config.max_total_prepaid_gas,
                max_actions_per_receipt: config.limit_config.max_actions_per_receipt,
                max_number_bytes_method_names: config.limit_config.max_number_bytes_method_names,
                max_length_method_name: config.limit_config.max_length_method_name,
                max_arguments_length: config.limit_config.max_arguments_length,
                max_length_returned_data: config.limit_config.max_length_returned_data,
                max_contract_size: config.limit_config.max_contract_size,
                max_transaction_size: config.limit_config.max_transaction_size,
                max_length_storage_key: config.limit_config.max_length_storage_key,
                max_length_storage_value: config.limit_config.max_length_storage_value,
                max_promises_per_function_call_action: config
                    .limit_config
                    .max_promises_per_function_call_action,
                max_number_input_data_dependencies: config
                    .limit_config
                    .max_number_input_data_dependencies,
                max_functions_number_per_contract: config
                    .limit_config
                    .max_functions_number_per_contract,
                wasmer2_stack_limit: config.limit_config.wasmer2_stack_limit,
                max_locals_per_contract: config.limit_config.max_locals_per_contract,
                account_id_validity_rules_version: config
                    .limit_config
                    .account_id_validity_rules_version,
            },
        }
    }
}

pub(crate) fn with_vm_variants(#[allow(unused)] cfg: &Config, runner: impl Fn(VMKind) -> ()) {
    #[cfg(all(feature = "wasmer0_vm", target_arch = "x86_64"))]
    runner(VMKind::Wasmer0);

    #[cfg(feature = "wasmtime_vm")]
    runner(VMKind::Wasmtime);

    #[cfg(all(feature = "wasmer2_vm", target_arch = "x86_64"))]
    runner(VMKind::Wasmer2);

    #[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
    if cfg.limit_config.contract_prepare_version == ContractPrepareVersion::V2 {
        runner(VMKind::NearVm);
    }
}

fn create_context(input: Vec<u8>) -> VMContext {
    VMContext {
        current_account_id: CURRENT_ACCOUNT_ID.parse().unwrap(),
        signer_account_id: SIGNER_ACCOUNT_ID.parse().unwrap(),
        signer_account_pk: Vec::from(&SIGNER_ACCOUNT_PK[..]),
        predecessor_account_id: PREDECESSOR_ACCOUNT_ID.parse().unwrap(),
        input,
        block_height: 10,
        block_timestamp: 42,
        epoch_height: 1,
        account_balance: 2u128,
        account_locked_balance: 0,
        storage_usage: 12,
        attached_deposit: 2u128,
        prepaid_gas: 10_u64.pow(14),
        random_seed: vec![0, 1, 2],
        view_config: None,
        output_data_receivers: vec![],
    }
}

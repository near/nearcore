use serde_json::Value;

use near_client_primitives::types::GetProtocolConfigError;
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::config::{RpcProtocolConfigError, RpcProtocolConfigRequest};
use near_primitives::types::BlockReference;

use super::{parse_params, RpcFrom, RpcInto, RpcRequest};

impl RpcRequest for RpcProtocolConfigRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        parse_params::<BlockReference>(value).map(|block_reference| Self { block_reference })
    }
}

impl RpcFrom<actix::MailboxError> for RpcProtocolConfigError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<GetProtocolConfigError> for RpcProtocolConfigError {
    fn rpc_from(error: GetProtocolConfigError) -> Self {
        match error {
            GetProtocolConfigError::UnknownBlock(error_message) => {
                Self::UnknownBlock { error_message }
            }
            GetProtocolConfigError::IOError(error_message) => Self::InternalError { error_message },
            GetProtocolConfigError::Unreachable(ref error_message) => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcProtocolConfigError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}

impl RpcFrom<near_chain_configs::ProtocolConfigView>
    for near_jsonrpc_primitives::types::config::ProtocolConfigView
{
    fn rpc_from(config: near_chain_configs::ProtocolConfigView) -> Self {
        near_jsonrpc_primitives::types::config::ProtocolConfigView {
            protocol_version: config.protocol_version,
            genesis_time: config.genesis_time,
            chain_id: config.chain_id,
            genesis_height: config.genesis_height,
            num_block_producer_seats: config.num_block_producer_seats,
            num_block_producer_seats_per_shard: config.num_block_producer_seats_per_shard,
            avg_hidden_validator_seats_per_shard: config.avg_hidden_validator_seats_per_shard,
            dynamic_resharding: config.dynamic_resharding,
            protocol_upgrade_stake_threshold: config.protocol_upgrade_stake_threshold,
            epoch_length: config.epoch_length,
            gas_limit: config.gas_limit,
            min_gas_price: config.min_gas_price,
            max_gas_price: config.max_gas_price,
            block_producer_kickout_threshold: config.block_producer_kickout_threshold,
            chunk_producer_kickout_threshold: config.chunk_producer_kickout_threshold,
            online_min_threshold: config.online_min_threshold,
            online_max_threshold: config.online_max_threshold,
            gas_price_adjustment_rate: config.gas_price_adjustment_rate,
            runtime_config: config.runtime_config.rpc_into(),
            transaction_validity_period: config.transaction_validity_period,
            protocol_reward_rate: config.protocol_reward_rate,
            max_inflation_rate: config.max_inflation_rate,
            num_blocks_per_year: config.num_blocks_per_year,
            protocol_treasury_account: config.protocol_treasury_account,
            fishermen_threshold: config.fishermen_threshold,
            minimum_stake_divisor: config.minimum_stake_divisor,
        }
    }
}

impl RpcFrom<near_primitives::runtime::config::RuntimeConfig>
    for near_jsonrpc_primitives::types::config::RuntimeConfig
{
    fn rpc_from(config: near_primitives::runtime::config::RuntimeConfig) -> Self {
        near_jsonrpc_primitives::types::config::RuntimeConfig {
            storage_amount_per_byte: config.storage_amount_per_byte,
            transaction_costs: config.transaction_costs.rpc_into(),
            wasm_config: config.wasm_config.rpc_into(),
            account_creation_config: config.account_creation_config.rpc_into(),
        }
    }
}

impl RpcFrom<near_primitives::runtime::fees::RuntimeFeesConfig>
    for near_jsonrpc_primitives::types::config::RuntimeFeesConfig
{
    fn rpc_from(config: near_primitives::runtime::fees::RuntimeFeesConfig) -> Self {
        near_jsonrpc_primitives::types::config::RuntimeFeesConfig {
            action_receipt_creation_config: config.action_receipt_creation_config.rpc_into(),
            data_receipt_creation_config: config.data_receipt_creation_config.rpc_into(),
            action_creation_config: config.action_creation_config.rpc_into(),
            storage_usage_config: config.storage_usage_config.rpc_into(),
            burnt_gas_reward: config.burnt_gas_reward,
            pessimistic_gas_price_inflation_ratio: config.pessimistic_gas_price_inflation_ratio,
        }
    }
}

impl RpcFrom<near_primitives::config::VMConfig>
    for near_jsonrpc_primitives::types::config::VMConfig
{
    fn rpc_from(config: near_primitives::config::VMConfig) -> Self {
        near_jsonrpc_primitives::types::config::VMConfig {
            ext_costs: config.ext_costs.rpc_into(),
            grow_mem_cost: config.grow_mem_cost,
            regular_op_cost: config.regular_op_cost,
            limit_config: config.limit_config.rpc_into(),
        }
    }
}

impl RpcFrom<near_primitives::runtime::config::AccountCreationConfig>
    for near_jsonrpc_primitives::types::config::AccountCreationConfig
{
    fn rpc_from(config: near_primitives::runtime::config::AccountCreationConfig) -> Self {
        near_jsonrpc_primitives::types::config::AccountCreationConfig {
            min_allowed_top_level_account_length: config.min_allowed_top_level_account_length,
            registrar_account_id: config.registrar_account_id,
        }
    }
}

impl RpcFrom<near_primitives::runtime::fees::DataReceiptCreationConfig>
    for near_jsonrpc_primitives::types::config::DataReceiptCreationConfig
{
    fn rpc_from(config: near_primitives::runtime::fees::DataReceiptCreationConfig) -> Self {
        near_jsonrpc_primitives::types::config::DataReceiptCreationConfig {
            base_cost: config.base_cost.rpc_into(),
            cost_per_byte: config.cost_per_byte.rpc_into(),
        }
    }
}

impl RpcFrom<near_primitives::runtime::fees::ActionCreationConfig>
    for near_jsonrpc_primitives::types::config::ActionCreationConfig
{
    fn rpc_from(config: near_primitives::runtime::fees::ActionCreationConfig) -> Self {
        near_jsonrpc_primitives::types::config::ActionCreationConfig {
            create_account_cost: config.create_account_cost.rpc_into(),
            deploy_contract_cost: config.deploy_contract_cost.rpc_into(),
            deploy_contract_cost_per_byte: config.deploy_contract_cost_per_byte.rpc_into(),
            function_call_cost: config.function_call_cost.rpc_into(),
            function_call_cost_per_byte: config.function_call_cost_per_byte.rpc_into(),
            transfer_cost: config.transfer_cost.rpc_into(),
            stake_cost: config.stake_cost.rpc_into(),
            add_key_cost: config.add_key_cost.rpc_into(),
            delete_key_cost: config.delete_key_cost.rpc_into(),
            delete_account_cost: config.delete_account_cost.rpc_into(),
        }
    }
}

impl RpcFrom<near_primitives::runtime::fees::AccessKeyCreationConfig>
    for near_jsonrpc_primitives::types::config::AccessKeyCreationConfig
{
    fn rpc_from(config: near_primitives::runtime::fees::AccessKeyCreationConfig) -> Self {
        near_jsonrpc_primitives::types::config::AccessKeyCreationConfig {
            full_access_cost: config.full_access_cost.rpc_into(),
            function_call_cost: config.function_call_cost.rpc_into(),
            function_call_cost_per_byte: config.function_call_cost_per_byte.rpc_into(),
        }
    }
}

impl RpcFrom<near_primitives::runtime::fees::StorageUsageConfig>
    for near_jsonrpc_primitives::types::config::StorageUsageConfig
{
    fn rpc_from(config: near_primitives::runtime::fees::StorageUsageConfig) -> Self {
        near_jsonrpc_primitives::types::config::StorageUsageConfig {
            num_bytes_account: config.num_bytes_account,
            num_extra_bytes_record: config.num_extra_bytes_record,
        }
    }
}

impl RpcFrom<near_primitives::config::ExtCostsConfig>
    for near_jsonrpc_primitives::types::config::ExtCostsConfig
{
    fn rpc_from(config: near_primitives::config::ExtCostsConfig) -> Self {
        near_jsonrpc_primitives::types::config::ExtCostsConfig {
            base: config.base,
            contract_loading_base: config.contract_loading_base,
            contract_loading_bytes: config.contract_loading_bytes,
            read_memory_base: config.read_memory_base,
            read_memory_byte: config.read_memory_byte,
            write_memory_base: config.write_memory_base,
            write_memory_byte: config.write_memory_byte,
            read_register_base: config.read_register_base,
            read_register_byte: config.read_register_byte,
            write_register_base: config.write_register_base,
            write_register_byte: config.write_register_byte,
            utf8_decoding_base: config.utf8_decoding_base,
            utf8_decoding_byte: config.utf8_decoding_byte,
            utf16_decoding_base: config.utf16_decoding_base,
            utf16_decoding_byte: config.utf16_decoding_byte,
            sha256_base: config.sha256_base,
            sha256_byte: config.sha256_byte,
            keccak256_base: config.keccak256_base,
            keccak256_byte: config.keccak256_byte,
            keccak512_base: config.keccak512_base,
            keccak512_byte: config.keccak512_byte,
            ripemd160_base: config.ripemd160_base,
            ripemd160_block: config.ripemd160_block,
            ecrecover_base: config.ecrecover_base,
            log_base: config.log_base,
            log_byte: config.log_byte,
            storage_write_base: config.storage_write_base,
            storage_write_key_byte: config.storage_write_key_byte,
            storage_write_value_byte: config.storage_write_value_byte,
            storage_write_evicted_byte: config.storage_write_evicted_byte,
            storage_read_base: config.storage_read_base,
            storage_read_key_byte: config.storage_read_key_byte,
            storage_read_value_byte: config.storage_read_value_byte,
            storage_remove_base: config.storage_remove_base,
            storage_remove_key_byte: config.storage_remove_key_byte,
            storage_remove_ret_value_byte: config.storage_remove_ret_value_byte,
            storage_has_key_base: config.storage_has_key_base,
            storage_has_key_byte: config.storage_has_key_byte,
            storage_iter_create_prefix_base: config.storage_iter_create_prefix_base,
            storage_iter_create_prefix_byte: config.storage_iter_create_prefix_byte,
            storage_iter_create_range_base: config.storage_iter_create_range_base,
            storage_iter_create_from_byte: config.storage_iter_create_from_byte,
            storage_iter_create_to_byte: config.storage_iter_create_to_byte,
            storage_iter_next_base: config.storage_iter_next_base,
            storage_iter_next_key_byte: config.storage_iter_next_key_byte,
            storage_iter_next_value_byte: config.storage_iter_next_value_byte,
            touching_trie_node: config.touching_trie_node,
            read_cached_trie_node: config.read_cached_trie_node,
            promise_and_base: config.promise_and_base,
            promise_and_per_promise: config.promise_and_per_promise,
            promise_return: config.promise_return,
            validator_stake_base: config.validator_stake_base,
            validator_total_stake_base: config.validator_total_stake_base,
            _unused1: config._unused1,
            _unused2: config._unused2,
            alt_bn128_g1_multiexp_base: config.alt_bn128_g1_multiexp_base,
            alt_bn128_g1_multiexp_element: config.alt_bn128_g1_multiexp_element,
            alt_bn128_g1_sum_base: config.alt_bn128_g1_sum_base,
            alt_bn128_g1_sum_element: config.alt_bn128_g1_sum_element,
            alt_bn128_pairing_check_base: config.alt_bn128_pairing_check_base,
            alt_bn128_pairing_check_element: config.alt_bn128_pairing_check_element,
        }
    }
}

impl RpcFrom<near_primitives::config::StackLimiterVersion>
    for near_jsonrpc_primitives::types::config::StackLimiterVersion
{
    fn rpc_from(version: near_primitives::config::StackLimiterVersion) -> Self {
        match version {
            near_primitives::config::StackLimiterVersion::V0 => {
                near_jsonrpc_primitives::types::config::StackLimiterVersion::V0
            }
            near_primitives::config::StackLimiterVersion::V1 => {
                near_jsonrpc_primitives::types::config::StackLimiterVersion::V1
            }
        }
    }
}

impl RpcFrom<near_primitives::config::VMLimitConfig>
    for near_jsonrpc_primitives::types::config::VMLimitConfig
{
    fn rpc_from(config: near_primitives::config::VMLimitConfig) -> Self {
        near_jsonrpc_primitives::types::config::VMLimitConfig {
            max_gas_burnt: config.max_gas_burnt,
            max_stack_height: config.max_stack_height,
            stack_limiter_version: config.stack_limiter_version.rpc_into(),
            initial_memory_pages: config.initial_memory_pages,
            max_memory_pages: config.max_memory_pages,
            registers_memory_limit: config.registers_memory_limit,
            max_register_size: config.max_register_size,
            max_number_registers: config.max_number_registers,
            max_number_logs: config.max_number_logs,
            max_total_log_length: config.max_total_log_length,
            max_total_prepaid_gas: config.max_total_prepaid_gas,
            max_actions_per_receipt: config.max_actions_per_receipt,
            max_number_bytes_method_names: config.max_number_bytes_method_names,
            max_length_method_name: config.max_length_method_name,
            max_arguments_length: config.max_arguments_length,
            max_length_returned_data: config.max_length_returned_data,
            max_contract_size: config.max_contract_size,
            max_transaction_size: config.max_transaction_size,
            max_length_storage_key: config.max_length_storage_key,
            max_length_storage_value: config.max_length_storage_value,
            max_promises_per_function_call_action: config.max_promises_per_function_call_action,
            max_number_input_data_dependencies: config.max_number_input_data_dependencies,
            max_functions_number_per_contract: config.max_functions_number_per_contract,
            wasmer2_stack_limit: config.wasmer2_stack_limit,
            max_locals_per_contract: config.max_locals_per_contract,
        }
    }
}

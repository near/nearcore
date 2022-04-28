use near_primitives_core::config::{
    wasmer2_stack_limit_default, ExtCostsConfig, StackLimiterVersion, VMConfig, VMLimitConfig,
};
use near_primitives_core::parameter::Parameter;
use near_primitives_core::runtime::fees::{
    AccessKeyCreationConfig, ActionCreationConfig, DataReceiptCreationConfig, Fee,
    RuntimeFeesConfig, StorageUsageConfig,
};
use num_rational::Rational;
use std::collections::BTreeMap;
use std::str::FromStr;

pub(crate) trait FromParameterTable {
    fn from_parameters(params: &ParameterTable) -> Self;
}

pub(crate) struct ParameterTable {
    params: BTreeMap<Parameter, String>,
}

impl ParameterTable {
    pub(crate) fn from_txt(arg: &str) -> ParameterTable {
        let parameters = arg.lines().filter_map(|line| {
            let trimmed = line.trim().to_owned();
            if trimmed.starts_with("#") || trimmed.is_empty() {
                None
            } else {
                let (key, value) = trimmed
                    .split_once(":")
                    .expect("Parameter name and value must be separated by a ':'");
                let typed_key: Parameter = key
                    .trim()
                    .parse()
                    .unwrap_or_else(|_err| panic!("Unexpected parameter `{key}`."));
                Some((typed_key, value.trim().to_owned()))
            }
        });
        ParameterTable { params: BTreeMap::from_iter(parameters) }
    }

    pub(crate) fn get<F: std::str::FromStr>(&self, key: Parameter) -> F {
        let key_str: &'static str = key.into();
        self.get_optional(key).unwrap_or_else(|| panic!("Missing parameter `{key_str}`"))
    }

    pub(crate) fn get_optional<F: std::str::FromStr>(&self, key: Parameter) -> Option<F> {
        let key_str: &'static str = key.into();
        Some(self.params.get(&key)?.parse().unwrap_or_else(|_err| {
            panic!("Could not parse parameter `{key_str}` as `{}`.", std::any::type_name::<F>())
        }))
    }

    fn fee(&self, key: &str) -> Fee {
        Fee {
            send_sir: self.get(Parameter::from_str(&(key.to_owned() + "_send_sir")).unwrap()),
            send_not_sir: self
                .get(Parameter::from_str(&(key.to_owned() + "_send_not_sir")).unwrap()),
            execution: self.get(Parameter::from_str(&(key.to_owned() + "_execution")).unwrap()),
        }
    }

    pub(crate) fn apply_diff(&mut self, diff: ParameterTable) {
        self.params.extend(diff.params)
    }
}

impl FromParameterTable for VMConfig {
    fn from_parameters(params: &ParameterTable) -> VMConfig {
        VMConfig {
            ext_costs: ExtCostsConfig::from_parameters(params),
            grow_mem_cost: params.get(Parameter::WasmGrowMemCost),
            regular_op_cost: params.get(Parameter::WasmRegularOpCost),
            limit_config: VMLimitConfig::from_parameters(params),
        }
    }
}

impl FromParameterTable for VMLimitConfig {
    fn from_parameters(params: &ParameterTable) -> VMLimitConfig {
        Self {
            max_gas_burnt: params.get(Parameter::MaxGasBurnt),
            max_stack_height: params.get(Parameter::MaxStackHeight),
            stack_limiter_version: StackLimiterVersion::from_repr(
                params.get(Parameter::StackLimiterVersion),
            )
            .expect("Invalid stack limiter version config"),
            initial_memory_pages: params.get(Parameter::InitialMemoryPages),
            max_memory_pages: params.get(Parameter::MaxMemoryPages),
            registers_memory_limit: params.get(Parameter::RegistersMemoryLimit),
            max_register_size: params.get(Parameter::MaxRegisterSize),
            max_number_registers: params.get(Parameter::MaxNumberRegisters),
            max_number_logs: params.get(Parameter::MaxNumberLogs),
            max_total_log_length: params.get(Parameter::MaxTotalLogLength),
            max_total_prepaid_gas: params.get(Parameter::MaxTotalPrepaidGas),
            max_actions_per_receipt: params.get(Parameter::MaxActionsPerReceipt),
            max_number_bytes_method_names: params.get(Parameter::MaxNumberBytesMethodNames),
            max_length_method_name: params.get(Parameter::MaxLengthMethodName),
            max_arguments_length: params.get(Parameter::MaxArgumentsLength),
            max_length_returned_data: params.get(Parameter::MaxLengthReturnedData),
            max_contract_size: params.get(Parameter::MaxContractSize),
            max_transaction_size: params.get(Parameter::MaxTransactionSize),
            max_length_storage_key: params.get(Parameter::MaxLengthStorageKey),
            max_length_storage_value: params.get(Parameter::MaxLengthStorageValue),
            max_promises_per_function_call_action: params
                .get(Parameter::MaxPromisesPerFunctionCallAction),
            max_number_input_data_dependencies: params
                .get(Parameter::MaxNumberInputDataDependencies),
            max_functions_number_per_contract: params
                .get_optional(Parameter::MaxFunctionsNumberPerContract),
            wasmer2_stack_limit: params
                .get_optional(Parameter::Wasmer2StackLimit)
                .unwrap_or_else(wasmer2_stack_limit_default),
            max_locals_per_contract: params.get_optional(Parameter::MaxLocalsPerContract),
        }
    }
}

impl FromParameterTable for ExtCostsConfig {
    fn from_parameters(params: &ParameterTable) -> ExtCostsConfig {
        ExtCostsConfig {
            base: params.get(Parameter::WasmHostFunctionBase),
            contract_loading_base: params.get(Parameter::WasmContractLoadingBase),
            contract_loading_bytes: params.get(Parameter::WasmContractLoadingBytes),
            read_memory_base: params.get(Parameter::WasmReadMemoryBase),
            read_memory_byte: params.get(Parameter::WasmReadMemoryByte),
            write_memory_base: params.get(Parameter::WasmWriteMemoryBase),
            write_memory_byte: params.get(Parameter::WasmWriteMemoryByte),
            read_register_base: params.get(Parameter::WasmReadRegisterBase),
            read_register_byte: params.get(Parameter::WasmReadRegisterByte),
            write_register_base: params.get(Parameter::WasmWriteRegisterBase),
            write_register_byte: params.get(Parameter::WasmWriteRegisterByte),
            utf8_decoding_base: params.get(Parameter::WasmUtf8DecodingBase),
            utf8_decoding_byte: params.get(Parameter::WasmUtf8DecodingByte),
            utf16_decoding_base: params.get(Parameter::WasmUtf16DecodingBase),
            utf16_decoding_byte: params.get(Parameter::WasmUtf16DecodingByte),
            sha256_base: params.get(Parameter::WasmSha256Base),
            sha256_byte: params.get(Parameter::WasmSha256Byte),
            keccak256_base: params.get(Parameter::WasmKeccak256Base),
            keccak256_byte: params.get(Parameter::WasmKeccak256Byte),
            keccak512_base: params.get(Parameter::WasmKeccak512Base),
            keccak512_byte: params.get(Parameter::WasmKeccak512Byte),
            ripemd160_base: params.get(Parameter::WasmRipemd160Base),
            ripemd160_block: params.get(Parameter::WasmRipemd160Block),
            ecrecover_base: params.get(Parameter::WasmEcrecoverBase),
            log_base: params.get(Parameter::WasmLogBase),
            log_byte: params.get(Parameter::WasmLogByte),
            storage_write_base: params.get(Parameter::WasmStorageWriteBase),
            storage_write_key_byte: params.get(Parameter::WasmStorageWriteKeyByte),
            storage_write_value_byte: params.get(Parameter::WasmStorageWriteValueByte),
            storage_write_evicted_byte: params.get(Parameter::WasmStorageWriteEvictedByte),
            storage_read_base: params.get(Parameter::WasmStorageReadBase),
            storage_read_key_byte: params.get(Parameter::WasmStorageReadKeyByte),
            storage_read_value_byte: params.get(Parameter::WasmStorageReadValueByte),
            storage_remove_base: params.get(Parameter::WasmStorageRemoveBase),
            storage_remove_key_byte: params.get(Parameter::WasmStorageRemoveKeyByte),
            storage_remove_ret_value_byte: params.get(Parameter::WasmStorageRemoveRetValueByte),
            storage_has_key_base: params.get(Parameter::WasmStorageHasKeyBase),
            storage_has_key_byte: params.get(Parameter::WasmStorageHasKeyByte),
            storage_iter_create_prefix_base: params.get(Parameter::WasmStorageIterCreatePrefixBase),
            storage_iter_create_prefix_byte: params.get(Parameter::WasmStorageIterCreatePrefixByte),
            storage_iter_create_range_base: params.get(Parameter::WasmStorageIterCreateRangeBase),
            storage_iter_create_from_byte: params.get(Parameter::WasmStorageIterCreateFromByte),
            storage_iter_create_to_byte: params.get(Parameter::WasmStorageIterCreateToByte),
            storage_iter_next_base: params.get(Parameter::WasmStorageIterNextBase),
            storage_iter_next_key_byte: params.get(Parameter::WasmStorageIterNextKeyByte),
            storage_iter_next_value_byte: params.get(Parameter::WasmStorageIterNextValueByte),
            touching_trie_node: params.get(Parameter::WasmTouchingTrieNode),
            read_cached_trie_node: params.get(Parameter::WasmReadCachedTrieNode),
            promise_and_base: params.get(Parameter::WasmPromiseAndBase),
            promise_and_per_promise: params.get(Parameter::WasmPromiseAndPerPromise),
            promise_return: params.get(Parameter::WasmPromiseReturn),
            validator_stake_base: params.get(Parameter::WasmValidatorStakeBase),
            validator_total_stake_base: params.get(Parameter::WasmValidatorTotalStakeBase),
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_g1_multiexp_base: params.get(Parameter::WasmAltBn128G1MultiexpBase),
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_g1_multiexp_element: params.get(Parameter::WasmAltBn128G1MultiexpElement),
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_pairing_check_base: params.get(Parameter::WasmAltBn128PairingCheckBase),
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_pairing_check_element: params.get(Parameter::WasmAltBn128PairingCheckElement),
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_g1_sum_base: params.get(Parameter::WasmAltBn128G1SumBase),
            #[cfg(feature = "protocol_feature_alt_bn128")]
            alt_bn128_g1_sum_element: params.get(Parameter::WasmAltBn128G1SumElement),
        }
    }
}

impl FromParameterTable for RuntimeFeesConfig {
    fn from_parameters(params: &ParameterTable) -> Self {
        Self {
            action_receipt_creation_config: params.fee("action_receipt_creation"),
            data_receipt_creation_config: DataReceiptCreationConfig {
                base_cost: params.fee("data_receipt_creation_base"),
                cost_per_byte: params.fee("data_receipt_creation_per_byte"),
            },
            action_creation_config: ActionCreationConfig {
                create_account_cost: params.fee("action_create_account"),
                deploy_contract_cost: params.fee("action_deploy_contract"),
                deploy_contract_cost_per_byte: params.fee("action_deploy_contract_per_byte"),
                function_call_cost: params.fee("action_function_call"),
                function_call_cost_per_byte: params.fee("action_function_call_per_byte"),
                transfer_cost: params.fee("action_transfer"),
                stake_cost: params.fee("action_stake"),
                add_key_cost: AccessKeyCreationConfig {
                    full_access_cost: params.fee("action_add_full_access_key"),
                    function_call_cost: params.fee("action_add_function_call_key"),
                    function_call_cost_per_byte: params
                        .fee("action_add_function_call_key_per_byte"),
                },
                delete_key_cost: params.fee("action_delete_key"),
                delete_account_cost: params.fee("action_delete_account"),
            },
            storage_usage_config: StorageUsageConfig {
                num_bytes_account: params.get(Parameter::StorageNumBytesAccount),
                num_extra_bytes_record: params.get(Parameter::StorageNumExtraBytesRecord),
            },
            burnt_gas_reward: Rational::new(
                params.get(Parameter::BurntGasRewardNumerator),
                params.get(Parameter::BurntGasRewardDenominator),
            ),
            pessimistic_gas_price_inflation_ratio: Rational::new(
                params.get(Parameter::PessimisticGasPriceInflationNumerator),
                params.get(Parameter::PessimisticGasPriceInflationDenominator),
            ),
        }
    }
}

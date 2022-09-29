use crate::config::{ActionCosts, ExtCosts};
use crate::profile::Cost;
use std::slice;

/// Protocol configuration parameter which may change between protocol versions.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    strum::IntoStaticStr,
    strum::EnumString,
    Debug,
    strum::Display,
)]
#[strum(serialize_all = "snake_case")]
pub enum Parameter {
    // Gas economics config
    BurntGasRewardNumerator,
    BurntGasRewardDenominator,
    PessimisticGasPriceInflationNumerator,
    PessimisticGasPriceInflationDenominator,

    // Account creation config
    MinAllowedTopLevelAccountLength,
    RegistrarAccountId,

    // Storage usage config
    StorageAmountPerByte,
    StorageNumBytesAccount,
    StorageNumExtraBytesRecord,

    // Static action costs
    // send_sir / send_not_sir is burned when creating a receipt on the signer shard.
    // (SIR = signer is receiver, which guarantees the receipt is local.)
    // Execution is burned when applying receipt on receiver shard.
    ActionReceiptCreationSendSir,
    ActionReceiptCreationSendNotSir,
    ActionReceiptCreationExecution,
    DataReceiptCreationBaseSendSir,
    DataReceiptCreationBaseSendNotSir,
    DataReceiptCreationBaseExecution,
    DataReceiptCreationPerByteSendSir,
    DataReceiptCreationPerByteSendNotSir,
    DataReceiptCreationPerByteExecution,
    ActionCreateAccountSendSir,
    ActionCreateAccountSendNotSir,
    ActionCreateAccountExecution,
    ActionDeleteAccountSendSir,
    ActionDeleteAccountSendNotSir,
    ActionDeleteAccountExecution,
    ActionDeployContractSendSir,
    ActionDeployContractSendNotSir,
    ActionDeployContractExecution,
    ActionDeployContractPerByteSendSir,
    ActionDeployContractPerByteSendNotSir,
    ActionDeployContractPerByteExecution,
    ActionFunctionCallSendSir,
    ActionFunctionCallSendNotSir,
    ActionFunctionCallExecution,
    ActionFunctionCallPerByteSendSir,
    ActionFunctionCallPerByteSendNotSir,
    ActionFunctionCallPerByteExecution,
    ActionTransferSendSir,
    ActionTransferSendNotSir,
    ActionTransferExecution,
    ActionStakeSendSir,
    ActionStakeSendNotSir,
    ActionStakeExecution,
    ActionAddFullAccessKeySendSir,
    ActionAddFullAccessKeySendNotSir,
    ActionAddFullAccessKeyExecution,
    ActionAddFunctionCallKeySendSir,
    ActionAddFunctionCallKeySendNotSir,
    ActionAddFunctionCallKeyExecution,
    ActionAddFunctionCallKeyPerByteSendSir,
    ActionAddFunctionCallKeyPerByteSendNotSir,
    ActionAddFunctionCallKeyPerByteExecution,
    ActionDeleteKeySendSir,
    ActionDeleteKeySendNotSir,
    ActionDeleteKeyExecution,

    // Smart contract dynamic gas costs
    WasmRegularOpCost,
    WasmGrowMemCost,
    /// Base cost for a host function
    WasmBase,
    WasmContractLoadingBase,
    WasmContractLoadingBytes,
    WasmReadMemoryBase,
    WasmReadMemoryByte,
    WasmWriteMemoryBase,
    WasmWriteMemoryByte,
    WasmReadRegisterBase,
    WasmReadRegisterByte,
    WasmWriteRegisterBase,
    WasmWriteRegisterByte,
    WasmUtf8DecodingBase,
    WasmUtf8DecodingByte,
    WasmUtf16DecodingBase,
    WasmUtf16DecodingByte,
    WasmSha256Base,
    WasmSha256Byte,
    WasmKeccak256Base,
    WasmKeccak256Byte,
    WasmKeccak512Base,
    WasmKeccak512Byte,
    WasmRipemd160Base,
    WasmRipemd160Block,
    WasmEcrecoverBase,
    #[cfg(feature = "protocol_feature_ed25519_verify")]
    WasmEd25519VerifyBase,
    #[cfg(feature = "protocol_feature_ed25519_verify")]
    WasmEd25519VerifyByte,
    WasmLogBase,
    WasmLogByte,
    WasmStorageWriteBase,
    WasmStorageWriteKeyByte,
    WasmStorageWriteValueByte,
    WasmStorageWriteEvictedByte,
    WasmStorageReadBase,
    WasmStorageReadKeyByte,
    WasmStorageReadValueByte,
    WasmStorageRemoveBase,
    WasmStorageRemoveKeyByte,
    WasmStorageRemoveRetValueByte,
    WasmStorageHasKeyBase,
    WasmStorageHasKeyByte,
    WasmStorageIterCreatePrefixBase,
    WasmStorageIterCreatePrefixByte,
    WasmStorageIterCreateRangeBase,
    WasmStorageIterCreateFromByte,
    WasmStorageIterCreateToByte,
    WasmStorageIterNextBase,
    WasmStorageIterNextKeyByte,
    WasmStorageIterNextValueByte,
    WasmTouchingTrieNode,
    WasmReadCachedTrieNode,
    WasmPromiseAndBase,
    WasmPromiseAndPerPromise,
    WasmPromiseReturn,
    WasmValidatorStakeBase,
    WasmValidatorTotalStakeBase,
    WasmAltBn128G1MultiexpBase,
    WasmAltBn128G1MultiexpElement,
    WasmAltBn128PairingCheckBase,
    WasmAltBn128PairingCheckElement,
    WasmAltBn128G1SumBase,
    WasmAltBn128G1SumElement,

    // Smart contract limits
    MaxGasBurnt,
    MaxGasBurntView,
    MaxStackHeight,
    StackLimiterVersion,
    InitialMemoryPages,
    MaxMemoryPages,
    RegistersMemoryLimit,
    MaxRegisterSize,
    MaxNumberRegisters,
    MaxNumberLogs,
    MaxTotalLogLength,
    MaxTotalPrepaidGas,
    MaxActionsPerReceipt,
    MaxNumberBytesMethodNames,
    MaxLengthMethodName,
    MaxArgumentsLength,
    MaxLengthReturnedData,
    MaxContractSize,
    MaxTransactionSize,
    MaxLengthStorageKey,
    MaxLengthStorageValue,
    MaxPromisesPerFunctionCallAction,
    MaxNumberInputDataDependencies,
    MaxFunctionsNumberPerContract,
    Wasmer2StackLimit,
    MaxLocalsPerContract,
    AccountIdValidityRulesVersion,
}

#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    strum::IntoStaticStr,
    strum::EnumString,
    Debug,
    strum::Display,
)]
#[strum(serialize_all = "snake_case")]
pub enum FeeParameter {
    ActionReceiptCreation,
    DataReceiptCreationBase,
    DataReceiptCreationPerByte,
    ActionCreateAccount,
    ActionDeleteAccount,
    ActionDeployContract,
    ActionDeployContractPerByte,
    ActionFunctionCall,
    ActionFunctionCallPerByte,
    ActionTransfer,
    ActionStake,
    ActionAddFullAccessKey,
    ActionAddFunctionCallKey,
    ActionAddFunctionCallKeyPerByte,
    ActionDeleteKey,
}

impl Parameter {
    /// Iterate through all parameters that define external gas costs that may
    /// be charged during WASM execution. These are essentially all costs from
    /// host function calls. Note that the gas cost for regular WASM operation
    /// is treated separately and therefore not included in this list.
    pub fn ext_costs() -> slice::Iter<'static, Parameter> {
        [
            Parameter::WasmBase,
            Parameter::WasmContractLoadingBase,
            Parameter::WasmContractLoadingBytes,
            Parameter::WasmReadMemoryBase,
            Parameter::WasmReadMemoryByte,
            Parameter::WasmWriteMemoryBase,
            Parameter::WasmWriteMemoryByte,
            Parameter::WasmReadRegisterBase,
            Parameter::WasmReadRegisterByte,
            Parameter::WasmWriteRegisterBase,
            Parameter::WasmWriteRegisterByte,
            Parameter::WasmUtf8DecodingBase,
            Parameter::WasmUtf8DecodingByte,
            Parameter::WasmUtf16DecodingBase,
            Parameter::WasmUtf16DecodingByte,
            Parameter::WasmSha256Base,
            Parameter::WasmSha256Byte,
            Parameter::WasmKeccak256Base,
            Parameter::WasmKeccak256Byte,
            Parameter::WasmKeccak512Base,
            Parameter::WasmKeccak512Byte,
            Parameter::WasmRipemd160Base,
            Parameter::WasmRipemd160Block,
            Parameter::WasmEcrecoverBase,
            #[cfg(feature = "protocol_feature_ed25519_verify")]
            Parameter::WasmEd25519VerifyBase,
            #[cfg(feature = "protocol_feature_ed25519_verify")]
            Parameter::WasmEd25519VerifyByte,
            Parameter::WasmLogBase,
            Parameter::WasmLogByte,
            Parameter::WasmStorageWriteBase,
            Parameter::WasmStorageWriteKeyByte,
            Parameter::WasmStorageWriteValueByte,
            Parameter::WasmStorageWriteEvictedByte,
            Parameter::WasmStorageReadBase,
            Parameter::WasmStorageReadKeyByte,
            Parameter::WasmStorageReadValueByte,
            Parameter::WasmStorageRemoveBase,
            Parameter::WasmStorageRemoveKeyByte,
            Parameter::WasmStorageRemoveRetValueByte,
            Parameter::WasmStorageHasKeyBase,
            Parameter::WasmStorageHasKeyByte,
            Parameter::WasmStorageIterCreatePrefixBase,
            Parameter::WasmStorageIterCreatePrefixByte,
            Parameter::WasmStorageIterCreateRangeBase,
            Parameter::WasmStorageIterCreateFromByte,
            Parameter::WasmStorageIterCreateToByte,
            Parameter::WasmStorageIterNextBase,
            Parameter::WasmStorageIterNextKeyByte,
            Parameter::WasmStorageIterNextValueByte,
            Parameter::WasmTouchingTrieNode,
            Parameter::WasmReadCachedTrieNode,
            Parameter::WasmPromiseAndBase,
            Parameter::WasmPromiseAndPerPromise,
            Parameter::WasmPromiseReturn,
            Parameter::WasmValidatorStakeBase,
            Parameter::WasmValidatorTotalStakeBase,
            Parameter::WasmAltBn128G1MultiexpBase,
            Parameter::WasmAltBn128G1MultiexpElement,
            Parameter::WasmAltBn128PairingCheckBase,
            Parameter::WasmAltBn128PairingCheckElement,
            Parameter::WasmAltBn128G1SumBase,
            Parameter::WasmAltBn128G1SumElement,
        ]
        .iter()
    }

    /// Iterate through all parameters that define numerical limits for
    /// contracts that are executed in the WASM VM.
    pub fn vm_limits() -> slice::Iter<'static, Parameter> {
        [
            Parameter::MaxGasBurnt,
            Parameter::MaxStackHeight,
            Parameter::StackLimiterVersion,
            Parameter::InitialMemoryPages,
            Parameter::MaxMemoryPages,
            Parameter::RegistersMemoryLimit,
            Parameter::MaxRegisterSize,
            Parameter::MaxNumberRegisters,
            Parameter::MaxNumberLogs,
            Parameter::MaxTotalLogLength,
            Parameter::MaxTotalPrepaidGas,
            Parameter::MaxActionsPerReceipt,
            Parameter::MaxNumberBytesMethodNames,
            Parameter::MaxLengthMethodName,
            Parameter::MaxArgumentsLength,
            Parameter::MaxLengthReturnedData,
            Parameter::MaxContractSize,
            Parameter::MaxTransactionSize,
            Parameter::MaxLengthStorageKey,
            Parameter::MaxLengthStorageValue,
            Parameter::MaxPromisesPerFunctionCallAction,
            Parameter::MaxNumberInputDataDependencies,
            Parameter::MaxFunctionsNumberPerContract,
            Parameter::Wasmer2StackLimit,
            Parameter::MaxLocalsPerContract,
            Parameter::AccountIdValidityRulesVersion,
        ]
        .iter()
    }

    pub fn cost(self) -> Option<Cost> {
        match self {
            // wasm op
            Parameter::WasmRegularOpCost => Some(Cost::WasmInstruction),
            // wasm execution ext costs
            Parameter::WasmBase => Some(Cost::ExtCost { ext_cost_kind: ExtCosts::base }),
            Parameter::WasmContractLoadingBase => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::contract_loading_base })
            }
            Parameter::WasmContractLoadingBytes => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::contract_loading_bytes })
            }
            Parameter::WasmReadMemoryBase => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::read_memory_base })
            }
            Parameter::WasmReadMemoryByte => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::read_memory_byte })
            }
            Parameter::WasmWriteMemoryBase => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::write_memory_base })
            }
            Parameter::WasmWriteMemoryByte => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::write_memory_byte })
            }
            Parameter::WasmReadRegisterBase => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::read_register_base })
            }
            Parameter::WasmReadRegisterByte => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::read_register_byte })
            }
            Parameter::WasmWriteRegisterBase => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::write_register_base })
            }
            Parameter::WasmWriteRegisterByte => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::write_register_byte })
            }
            Parameter::WasmUtf8DecodingBase => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::utf8_decoding_base })
            }
            Parameter::WasmUtf8DecodingByte => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::utf8_decoding_byte })
            }
            Parameter::WasmUtf16DecodingBase => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::utf16_decoding_base })
            }
            Parameter::WasmUtf16DecodingByte => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::utf16_decoding_byte })
            }
            Parameter::WasmSha256Base => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::sha256_base })
            }
            Parameter::WasmSha256Byte => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::sha256_byte })
            }
            Parameter::WasmKeccak256Base => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::keccak256_base })
            }
            Parameter::WasmKeccak256Byte => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::keccak256_byte })
            }
            Parameter::WasmKeccak512Base => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::keccak512_base })
            }
            Parameter::WasmKeccak512Byte => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::keccak512_byte })
            }
            Parameter::WasmRipemd160Base => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::ripemd160_base })
            }
            Parameter::WasmRipemd160Block => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::ripemd160_block })
            }
            Parameter::WasmEcrecoverBase => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::ecrecover_base })
            }
            #[cfg(feature = "protocol_feature_ed25519_verify")]
            Parameter::WasmEd25519VerifyBase => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::ed25519_verify_base })
            }
            #[cfg(feature = "protocol_feature_ed25519_verify")]
            Parameter::WasmEd25519VerifyByte => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::ed25519_verify_byte })
            }
            Parameter::WasmLogBase => Some(Cost::ExtCost { ext_cost_kind: ExtCosts::log_base }),
            Parameter::WasmLogByte => Some(Cost::ExtCost { ext_cost_kind: ExtCosts::log_byte }),
            Parameter::WasmStorageWriteBase => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::storage_write_base })
            }
            Parameter::WasmStorageWriteKeyByte => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::storage_write_key_byte })
            }
            Parameter::WasmStorageWriteValueByte => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::storage_write_value_byte })
            }
            Parameter::WasmStorageWriteEvictedByte => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::storage_write_evicted_byte })
            }
            Parameter::WasmStorageReadBase => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::storage_read_base })
            }
            Parameter::WasmStorageReadKeyByte => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::storage_read_key_byte })
            }
            Parameter::WasmStorageReadValueByte => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::storage_read_value_byte })
            }
            Parameter::WasmStorageRemoveBase => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::storage_remove_base })
            }
            Parameter::WasmStorageRemoveKeyByte => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::storage_remove_key_byte })
            }
            Parameter::WasmStorageRemoveRetValueByte => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::storage_remove_ret_value_byte })
            }
            Parameter::WasmStorageHasKeyBase => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::storage_has_key_base })
            }
            Parameter::WasmStorageHasKeyByte => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::storage_has_key_byte })
            }
            Parameter::WasmStorageIterCreatePrefixBase => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_prefix_base })
            }
            Parameter::WasmStorageIterCreatePrefixByte => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_prefix_byte })
            }
            Parameter::WasmStorageIterCreateRangeBase => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_range_base })
            }
            Parameter::WasmStorageIterCreateFromByte => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_from_byte })
            }
            Parameter::WasmStorageIterCreateToByte => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_to_byte })
            }
            Parameter::WasmStorageIterNextBase => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_next_base })
            }
            Parameter::WasmStorageIterNextKeyByte => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_next_key_byte })
            }
            Parameter::WasmStorageIterNextValueByte => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_next_value_byte })
            }
            Parameter::WasmTouchingTrieNode => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::touching_trie_node })
            }
            Parameter::WasmReadCachedTrieNode => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::read_cached_trie_node })
            }
            Parameter::WasmPromiseAndBase => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::promise_and_base })
            }
            Parameter::WasmPromiseAndPerPromise => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::promise_and_per_promise })
            }
            Parameter::WasmPromiseReturn => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::promise_return })
            }
            Parameter::WasmValidatorStakeBase => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::validator_stake_base })
            }
            Parameter::WasmValidatorTotalStakeBase => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::validator_total_stake_base })
            }
            Parameter::WasmAltBn128G1MultiexpBase => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_g1_multiexp_base })
            }
            Parameter::WasmAltBn128G1MultiexpElement => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_g1_multiexp_element })
            }
            Parameter::WasmAltBn128PairingCheckBase => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_pairing_check_base })
            }
            Parameter::WasmAltBn128PairingCheckElement => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_pairing_check_element })
            }
            Parameter::WasmAltBn128G1SumBase => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_g1_sum_base })
            }
            Parameter::WasmAltBn128G1SumElement => {
                Some(Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_g1_sum_element })
            }

            Parameter::WasmGrowMemCost => todo!(),

            // actions
            Parameter::ActionCreateAccountSendSir => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::create_account })
            }
            Parameter::ActionCreateAccountSendNotSir => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::create_account })
            }
            Parameter::ActionCreateAccountExecution => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::create_account })
            }
            Parameter::ActionDeleteAccountSendSir => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::delete_account })
            }
            Parameter::ActionDeleteAccountSendNotSir => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::delete_account })
            }
            Parameter::ActionDeleteAccountExecution => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::delete_account })
            }
            Parameter::ActionDeployContractSendSir => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::deploy_contract })
            }
            Parameter::ActionDeployContractSendNotSir => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::deploy_contract })
            }
            Parameter::ActionDeployContractExecution => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::deploy_contract })
            }
            Parameter::ActionDeployContractPerByteSendSir => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::deploy_contract })
            }
            Parameter::ActionDeployContractPerByteSendNotSir => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::deploy_contract })
            }
            Parameter::ActionDeployContractPerByteExecution => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::deploy_contract })
            }
            Parameter::ActionFunctionCallSendSir => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::function_call })
            }
            Parameter::ActionFunctionCallSendNotSir => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::function_call })
            }
            Parameter::ActionFunctionCallExecution => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::function_call })
            }
            Parameter::ActionFunctionCallPerByteSendSir => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::function_call })
            }
            Parameter::ActionFunctionCallPerByteSendNotSir => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::function_call })
            }
            Parameter::ActionFunctionCallPerByteExecution => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::function_call })
            }
            Parameter::ActionTransferSendSir => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::transfer })
            }
            Parameter::ActionTransferSendNotSir => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::transfer })
            }
            Parameter::ActionTransferExecution => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::transfer })
            }
            Parameter::ActionStakeSendSir => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::stake })
            }
            Parameter::ActionStakeSendNotSir => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::stake })
            }
            Parameter::ActionStakeExecution => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::stake })
            }
            Parameter::ActionAddFullAccessKeySendSir => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::add_key })
            }
            Parameter::ActionAddFullAccessKeySendNotSir => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::add_key })
            }
            Parameter::ActionAddFullAccessKeyExecution => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::add_key })
            }
            Parameter::ActionAddFunctionCallKeySendSir => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::add_key })
            }
            Parameter::ActionAddFunctionCallKeySendNotSir => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::add_key })
            }
            Parameter::ActionAddFunctionCallKeyExecution => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::add_key })
            }
            Parameter::ActionAddFunctionCallKeyPerByteSendSir => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::add_key })
            }
            Parameter::ActionAddFunctionCallKeyPerByteSendNotSir => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::add_key })
            }
            Parameter::ActionAddFunctionCallKeyPerByteExecution => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::add_key })
            }
            Parameter::ActionDeleteKeySendSir => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::delete_key })
            }
            Parameter::ActionDeleteKeySendNotSir => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::delete_key })
            }
            Parameter::ActionDeleteKeyExecution => {
                Some(Cost::ActionCost { action_cost_kind: ActionCosts::delete_key })
            }

            // vm limits
            Parameter::MaxGasBurnt
            | Parameter::MaxStackHeight
            | Parameter::StackLimiterVersion
            | Parameter::InitialMemoryPages
            | Parameter::MaxMemoryPages
            | Parameter::RegistersMemoryLimit
            | Parameter::MaxRegisterSize
            | Parameter::MaxNumberRegisters
            | Parameter::MaxNumberLogs
            | Parameter::MaxTotalLogLength
            | Parameter::MaxTotalPrepaidGas
            | Parameter::MaxActionsPerReceipt
            | Parameter::MaxNumberBytesMethodNames
            | Parameter::MaxLengthMethodName
            | Parameter::MaxArgumentsLength
            | Parameter::MaxLengthReturnedData
            | Parameter::MaxContractSize
            | Parameter::MaxTransactionSize
            | Parameter::MaxLengthStorageKey
            | Parameter::MaxLengthStorageValue
            | Parameter::MaxPromisesPerFunctionCallAction
            | Parameter::MaxNumberInputDataDependencies
            | Parameter::MaxFunctionsNumberPerContract
            | Parameter::Wasmer2StackLimit
            | Parameter::MaxLocalsPerContract
            | Parameter::AccountIdValidityRulesVersion => None,

            // other
            Parameter::BurntGasRewardNumerator
            | Parameter::BurntGasRewardDenominator
            | Parameter::PessimisticGasPriceInflationNumerator
            | Parameter::PessimisticGasPriceInflationDenominator
            | Parameter::MinAllowedTopLevelAccountLength
            | Parameter::RegistrarAccountId
            | Parameter::StorageAmountPerByte
            | Parameter::StorageNumBytesAccount
            | Parameter::StorageNumExtraBytesRecord
            | Parameter::ActionReceiptCreationSendSir
            | Parameter::ActionReceiptCreationSendNotSir
            | Parameter::ActionReceiptCreationExecution
            | Parameter::DataReceiptCreationBaseSendSir
            | Parameter::DataReceiptCreationBaseSendNotSir
            | Parameter::DataReceiptCreationBaseExecution
            | Parameter::DataReceiptCreationPerByteSendSir
            | Parameter::DataReceiptCreationPerByteSendNotSir
            | Parameter::DataReceiptCreationPerByteExecution
            | Parameter::MaxGasBurntView => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Parameter;

    #[test]
    fn test_parameter_to_cost() {
        for param in Parameter::ext_costs() {
            assert!(param.cost().is_some());
        }

        for param in Parameter::vm_limits() {
            assert!(param.cost().is_none());
        }
    }
}

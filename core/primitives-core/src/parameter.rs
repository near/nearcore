use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader},
    str::FromStr,
};

/// Protocol configuration parameter which may change between protocol versions.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, strum::IntoStaticStr, strum::EnumString)]
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
    WasmHostFunctionBase,
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
}

pub fn load_parameters_from_txt(arg: &[u8]) -> BTreeMap<Parameter, String> {
    let parameters = BufReader::new(arg).lines().filter_map(|line| {
        let trimmed = line.unwrap().trim().to_owned();
        if trimmed.starts_with("#") || trimmed.is_empty() {
            None
        } else {
            let mut iter = trimmed.split(":");
            let key_str = iter.next().unwrap().trim();
            let typed_key = Parameter::from_str(key_str)
                .unwrap_or_else(|_err| panic!("Unexpected parameter `{key_str}`."));
            let value =
                iter.next().expect("Parameter name and value must be separated by a ':'").trim();
            Some((typed_key.to_owned(), value.to_owned()))
        }
    });
    BTreeMap::from_iter(parameters)
}

pub fn read_parameter<F: std::str::FromStr>(
    params: &BTreeMap<Parameter, String>,
    key: Parameter,
) -> F {
    let key_str: &'static str = key.into();
    read_optional_parameter(params, key).unwrap_or_else(|| panic!("Missing parameter `{key_str}`"))
}

pub fn read_optional_parameter<F: std::str::FromStr>(
    params: &BTreeMap<Parameter, String>,
    key: Parameter,
) -> Option<F> {
    let key_str: &'static str = key.into();
    Some(params.get(&key)?.parse().unwrap_or_else(|_err| {
        panic!("Could not parse parameter `{key_str}` as `{}`.", std::any::type_name::<F>())
    }))
}

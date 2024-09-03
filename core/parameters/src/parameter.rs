use crate::cost::ActionCosts;
use std::slice;

/// Protocol configuration parameter which may change between protocol versions.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Debug,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum Parameter {
    // Gas economics config
    BurntGasReward,
    PessimisticGasPriceInflation,

    /// Stateless validation config
    /// Size limit for storage proof generated while executing receipts in a chunk.
    /// After this limit is reached we defer execution of any new receipts.
    MainStorageProofSizeSoftLimit,
    /// Hard limit on the size of storage proof generated while executing a single receipt.
    PerReceiptStorageProofSizeLimit,
    /// Soft size limit of storage proof used to validate new transactions in ChunkStateWitness.
    NewTransactionsValidationStateSizeSoftLimit,
    /// Maximum size of transactions contained inside ChunkStateWitness.
    /// A witness contains transactions from both the previous chunk and the current one.
    /// This parameter limits the sum of sizes of transactions from both of those chunks.
    CombinedTransactionsSizeLimit,
    /// The standard size limit for outgoing receipts aimed at a single shard.
    /// This limit is pretty small to keep the size of source_receipt_proofs under control.
    /// It limits the total sum of outgoing receipts, not individual receipts.
    OutgoingReceiptsUsualSizeLimit,
    /// Large size limit for outgoing receipts to a shard, used when it's safe
    /// to send a lot of receipts without making the state witness too large.
    /// It limits the total sum of outgoing receipts, not individual receipts.
    OutgoingReceiptsBigSizeLimit,

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
    ActionDelegate,

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
    WasmEd25519VerifyBase,
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
    WasmStorageSmallReadBase,
    WasmStorageSmallReadKeyByte,
    WasmStorageSmallReadValueByte,
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
    WasmYieldCreateBase,
    WasmYieldCreateByte,
    WasmYieldResumeBase,
    WasmYieldResumeByte,
    WasmBls12381P1SumBase,
    WasmBls12381P1SumElement,
    WasmBls12381P2SumBase,
    WasmBls12381P2SumElement,
    WasmBls12381G1MultiexpBase,
    WasmBls12381G1MultiexpElement,
    WasmBls12381G2MultiexpBase,
    WasmBls12381G2MultiexpElement,
    WasmBls12381MapFpToG1Base,
    WasmBls12381MapFpToG1Element,
    WasmBls12381MapFp2ToG2Base,
    WasmBls12381MapFp2ToG2Element,
    WasmBls12381PairingBase,
    WasmBls12381PairingElement,
    WasmBls12381P1DecompressBase,
    WasmBls12381P1DecompressElement,
    WasmBls12381P2DecompressBase,
    WasmBls12381P2DecompressElement,

    // Smart contract limits
    MaxGasBurnt,
    MaxGasBurntView,
    MaxStackHeight,
    ContractPrepareVersion,
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
    MaxReceiptSize,
    MaxLengthStorageKey,
    MaxLengthStorageValue,
    MaxPromisesPerFunctionCallAction,
    MaxNumberInputDataDependencies,
    MaxFunctionsNumberPerContract,
    Wasmer2StackLimit,
    MaxLocalsPerContract,
    AccountIdValidityRulesVersion,
    YieldTimeoutLengthInBlocks,
    MaxYieldPayloadSize,

    // Contract runtime features
    #[strum(serialize = "disable_9393_fix")]
    Disable9393Fix,
    FlatStorageReads,
    ImplicitAccountCreation,
    FixContractLoadingCost,
    MathExtension,
    Ed25519Verify,
    AltBn128,
    FunctionCallWeight,
    VmKind,
    EthImplicitAccounts,
    YieldResume,
    DiscardCustomSections,

    // Congestion Control
    MaxCongestionIncomingGas,
    MaxCongestionOutgoingGas,
    MaxCongestionMemoryConsumption,
    MaxCongestionMissedChunks,

    MaxOutgoingGas,
    MinOutgoingGas,
    AllowedShardOutgoingGas,
    MaxTxGas,
    MinTxGas,
    RejectTxCongestionThreshold,
}

#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Debug,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
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
    ActionDelegate,
}

impl Parameter {
    /// Iterate through all parameters that define numerical limits for
    /// contracts that are executed in the WASM VM.
    pub fn vm_limits() -> slice::Iter<'static, Parameter> {
        [
            Parameter::MaxGasBurnt,
            Parameter::MaxStackHeight,
            Parameter::ContractPrepareVersion,
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
            Parameter::MaxReceiptSize,
            Parameter::MaxLengthStorageKey,
            Parameter::MaxLengthStorageValue,
            Parameter::MaxPromisesPerFunctionCallAction,
            Parameter::MaxNumberInputDataDependencies,
            Parameter::MaxFunctionsNumberPerContract,
            Parameter::Wasmer2StackLimit,
            Parameter::MaxLocalsPerContract,
            Parameter::AccountIdValidityRulesVersion,
            Parameter::YieldTimeoutLengthInBlocks,
            Parameter::MaxYieldPayloadSize,
            Parameter::PerReceiptStorageProofSizeLimit,
        ]
        .iter()
    }
}

// TODO: consider renaming parameters to "action_{ActionCosts}" and deleting
// `FeeParameter` all together.
impl From<ActionCosts> for FeeParameter {
    fn from(other: ActionCosts) -> Self {
        match other {
            ActionCosts::create_account => Self::ActionCreateAccount,
            ActionCosts::delete_account => Self::ActionDeleteAccount,
            ActionCosts::delegate => Self::ActionDelegate,
            ActionCosts::deploy_contract_base => Self::ActionDeployContract,
            ActionCosts::deploy_contract_byte => Self::ActionDeployContractPerByte,
            ActionCosts::function_call_base => Self::ActionFunctionCall,
            ActionCosts::function_call_byte => Self::ActionFunctionCallPerByte,
            ActionCosts::transfer => Self::ActionTransfer,
            ActionCosts::stake => Self::ActionStake,
            ActionCosts::add_full_access_key => Self::ActionAddFullAccessKey,
            ActionCosts::add_function_call_key_base => Self::ActionAddFunctionCallKey,
            ActionCosts::add_function_call_key_byte => Self::ActionAddFunctionCallKeyPerByte,
            ActionCosts::delete_key => Self::ActionDeleteKey,
            ActionCosts::new_action_receipt => Self::ActionReceiptCreation,
            ActionCosts::new_data_receipt_base => Self::DataReceiptCreationBase,
            ActionCosts::new_data_receipt_byte => Self::DataReceiptCreationPerByte,
        }
    }
}

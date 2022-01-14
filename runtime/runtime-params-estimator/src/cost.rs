use std::fmt;
use std::str::FromStr;

/// Kinds of things we measure in parameter estimator and charge for in runtime.
///
/// TODO: Deduplicate this enum with `ExtCosts` and `ActionCosts`.
#[derive(Copy, Clone, PartialEq, Eq, Debug, PartialOrd, Ord)]
#[repr(u8)]
pub enum Cost {
    ActionReceiptCreation,
    ActionSirReceiptCreation,
    DataReceiptCreationBase,
    DataReceiptCreationPerByte,
    ActionCreateAccount,
    ActionDeployContractBase,
    ActionDeployContractPerByte,
    ActionFunctionCallBase,
    ActionFunctionCallPerByte,
    ActionFunctionCallBaseV2,
    ActionFunctionCallPerByteV2,
    ActionTransfer,
    ActionStake,
    ActionAddFullAccessKey,
    ActionAddFunctionAccessKeyBase,
    ActionAddFunctionAccessKeyPerByte,
    ActionDeleteKey,
    ActionDeleteAccount,

    HostFunctionCall,
    WasmInstruction,
    ReadMemoryBase,
    ReadMemoryByte,
    WriteMemoryBase,
    WriteMemoryByte,
    ReadRegisterBase,
    ReadRegisterByte,
    WriteRegisterBase,
    WriteRegisterByte,
    Utf8DecodingBase,
    Utf8DecodingByte,
    Utf16DecodingBase,
    Utf16DecodingByte,
    Sha256Base,
    Sha256Byte,
    Keccak256Base,
    Keccak256Byte,
    Keccak512Base,
    Keccak512Byte,
    Ripemd160Base,
    Ripemd160Block,
    EcrecoverBase,
    LogBase,
    LogByte,
    StorageWriteBase,
    StorageWriteKeyByte,
    StorageWriteValueByte,
    StorageWriteEvictedByte,
    StorageReadBase,
    StorageReadKeyByte,
    StorageReadValueByte,
    StorageRemoveBase,
    StorageRemoveKeyByte,
    StorageRemoveRetValueByte,
    StorageHasKeyBase,
    StorageHasKeyByte,
    StorageIterCreatePrefixBase,
    StorageIterCreatePrefixByte,
    StorageIterCreateRangeBase,
    StorageIterCreateFromByte,
    StorageIterCreateToByte,
    StorageIterNextBase,
    StorageIterNextKeyByte,
    StorageIterNextValueByte,
    TouchingTrieNode,
    PromiseAndBase,
    PromiseAndPerPromise,
    PromiseReturn,
    ValidatorStakeBase,
    ValidatorTotalStakeBase,

    AltBn128G1MultiexpBase,
    AltBn128G1MultiexpByte,
    AltBn128G1MultiexpSublinear,
    AltBn128PairingCheckBase,
    AltBn128PairingCheckByte,
    AltBn128G1SumBase,
    AltBn128G1SumByte,

    // Costs used only in estimator
    ContractCompileBase,  // TODO: Needs estimation function
    ContractCompileBytes, // TODO: Needs estimation function
    GasMeteringBase,
    GasMeteringOp,
    RocksDbInsertValueByte,
    RocksDbReadValueByte,
    IoReadByte,
    IoWriteByte,
    CpuBenchmarkSha256,
    OneCPUInstruction,
    OneNanosecond,

    __Count,
}

impl Cost {
    pub fn all() -> impl Iterator<Item = Cost> {
        (0..(Cost::__Count as u8)).map(Cost::try_from).map(Result::unwrap)
    }
}

impl TryFrom<u8> for Cost {
    type Error = ();

    fn try_from(d: u8) -> Result<Self, ()> {
        if d < (Cost::__Count as u8) {
            // SAFETY: `#[repr(u8)]`, the above range, and the language spec
            // guarantee that `d` can be cast to the enum.
            Ok(unsafe { std::mem::transmute::<u8, Cost>(d) })
        } else {
            Err(())
        }
    }
}

impl fmt::Display for Cost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl FromStr for Cost {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Ridiculously inefficient, but shouldn't mater.
        for cost in Cost::all() {
            if cost.to_string() == s {
                return Ok(cost);
            }
        }
        Err(())
    }
}

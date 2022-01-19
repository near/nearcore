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
    /// Estimates `ExtCost::storage_write_base` which is charged once per call to `write_storage`.
    StorageWriteBase,
    /// Estimates `ExtCost::storage_write_key_byte` which is charged for each byte in keys of `write_storage` calls.
    StorageWriteKeyByte,
    /// Estimates `ExtCost::storage_write_value_byte` which is charged for each byte in values of `write_storage` calls.
    StorageWriteValueByte,
    /// Estimates `ExtCosts::storage_write_evicted_byte` which is charged for each byte in a value that is overwritten in `write_storage` calls.
    StorageWriteEvictedByte,
    /// Estimates `ExtCost::storage_read_base` which is charged once per call to `read_storage`.
    StorageReadBase,
    /// Estimates `ExtCost::storage_read_key_byte` which is charged for each byte in keys of `read_storage` calls.
    StorageReadKeyByte,
    /// Estimates `ExtCost::storage_read_value_byte` which is charged for each byte in values of `read_storage` calls.
    StorageReadValueByte,
    /// Estimates `ExtCost::storage_remove_base` which is charged once per call to `storage_remove`.
    StorageRemoveBase,
    /// Estimates `ExtCost::storage_remove_key_byte` which is charged for each byte in keys of `storage_remove` calls.
    StorageRemoveKeyByte,
    /// Estimates `ExtCost::storage_remove_value_byte` which is charged for each byte in values of `storage_remove` calls.
    StorageRemoveRetValueByte,
    /// Estimates `ExtCost::storage_has_key_base` which is charged once per call to `storage_has_key`.
    StorageHasKeyBase,
    /// Estimates `ExtCost::storage_has_key_byte` which is charged for each byte in calls to `storage_has_key`.
    StorageHasKeyByte,
    /// DEPRECATED: Was charged in `storage_iter_prefix`
    StorageIterCreatePrefixBase,
    /// DEPRECATED: Was charged in `storage_iter_prefix`
    StorageIterCreatePrefixByte,
    /// DEPRECATED: Was charged in `storage_iter_range`
    StorageIterCreateRangeBase,
    /// DEPRECATED: Was charged in `storage_iter_range`
    StorageIterCreateFromByte,
    /// DEPRECATED: Was charged in `storage_iter_range`
    StorageIterCreateToByte,
    /// DEPRECATED: Was charged in `storage_iter_next`
    StorageIterNextBase,
    /// DEPRECATED: Was charged in `storage_iter_next`
    StorageIterNextKeyByte,
    /// DEPRECATED: Was charged in `storage_iter_next`
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
    /// Cost of inserting a new value directly into a RocksDB instance.
    /// In default settings, this is an alternative estimation for `StorageWriteValueByte`, measured in a more controlled setup.
    /// Using the extra flags prefixed with `rdb-`, this can be used to measure the impact of various RocksDB settings on insertions.
    RocksDbInsertValueByte,
    /// Cost of reading values directly from a RocksDB instance.
    /// In default settings, this is an alternative estimation for `StorageReadValueByte`, measured in a more controlled setup.
    /// Using the extra flags prefixed with `rdb-`, this can be used to measure the impact of various RocksDB settings on read performance.
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

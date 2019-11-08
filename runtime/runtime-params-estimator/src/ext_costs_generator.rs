use crate::cases::Metric;
use crate::stats::{DataStats, Measurements};
use std::collections::{BTreeMap, HashMap};

/// Costs for calling external functions expressed in micros as floats.
#[derive(Clone, Copy, Hash, PartialEq, Eq, Debug)]
enum ExtCostsFloat {
    InputBase,
    InputPerByte,
    StorageReadBase,
    StorageReadKeyByte,
    StorageReadValueByte,
    StorageWriteBase,
    StorageWriteKeyByte,
    StorageWriteValueByte,
    StorageHasKeyBase,
    StorageHasKeyByte,
    StorageRemoveBase,
    StorageRemoveKeyByte,
    StorageRemoveRetValueByte,
    StorageIterCreatePrefixBase,
    StorageIterCreateRangeBase,
    StorageIterCreateKeyByte,
    StorageIterNextBase,
    StorageIterNextKeyByte,
    StorageIterNextValueByte,
    ReadRegisterBase,
    ReadRegisterByte,
    WriteRegisterBase,
    WriteRegisterByte,
    ReadMemoryBase,
    ReadMemoryByte,
    WriteMemoryBase,
    WriteMemoryByte,
    AccountBalance,
    PrepaidGas,
    UsedGas,
    RandomSeedBase,
    RandomSeedPerByte,
    Sha256,
    Sha256Byte,
    AttachedDeposit,
    StorageUsage,
    BlockIndex,
    BlockTimestamp,
    CurrentAccountId,
    CurrentAccountIdByte,
    SignerAccountId,
    SignerAccountIdByte,
    SignerAccountPK,
    SignerAccountPKByte,
    PredecessorAccountId,
    PredecessorAccountIdByte,
    PromiseAndBase,
    PromiseAndPerPromise,
    PromiseResultBase,
    PromiseResultByte,
    PromiseResultsCount,
    PromiseReturn,
    LogBase,
    LogPerByte,
}

pub struct ExtCostsGenerator {
    aggregated: BTreeMap<Metric, DataStats>,
}

impl ExtCostsGenerator {
    pub fn new(measurement: &Measurements) -> Self {
        let aggregated = measurement.aggregate();
        Self { aggregated }
    }

    /// Compute ext costs in microseconds as floats.
    fn ext_costs_in_micros(&self) -> HashMap<ExtCostsFloat, f64> {
        let mut res = HashMap::new();
        // We ignore the fact that we also pass 10 bytes.
        res.insert(
            ExtCostsFloat::InputBase,
            (self.aggregated[&Metric::CallInput1KW10B].sigma_micros() as f64
                - self.aggregated[&Metric::CallNoop].sigma_micros() as f64)
                / (1000f64),
        );
        res.insert(
            ExtCostsFloat::InputPerByte,
            (self.aggregated[&Metric::CallInput1KW10KB].sigma_micros() as f64
                - self.aggregated[&Metric::CallInput1KW10B].sigma_micros() as f64)
                / (1000f64)
                / (10f64 * 1024f64 - 10f64),
        );

        let fixture_10b = self.aggregated[&Metric::CallInput1KW10B].sigma_micros() as f64
            + self.aggregated[&Metric::CallInputRegisterLen1KW10B].sigma_micros() as f64
            + self.aggregated[&Metric::CallInputReadRegister1KW10B].sigma_micros() as f64;
        let fixture_1kb = self.aggregated[&Metric::CallInput1KW1KB].sigma_micros() as f64
            + self.aggregated[&Metric::CallInputRegisterLen1KW1KB].sigma_micros() as f64
            + self.aggregated[&Metric::CallInputReadRegister1KW1KB].sigma_micros() as f64;
        let fixture_10kb = self.aggregated[&Metric::CallInput1KW10KB].sigma_micros() as f64
            + self.aggregated[&Metric::CallInputRegisterLen1KW10KB].sigma_micros() as f64
            + self.aggregated[&Metric::CallInputReadRegister1KW10KB].sigma_micros() as f64;
        res.insert(
            ExtCostsFloat::StorageReadBase,
            (self.aggregated[&Metric::CallStorageRead100W10B].sigma_micros() as f64 - fixture_10b)
                / (100f64),
        );
        // TODO: Here and below we charge operations that use key or value as the cost of both key and value.
        res.insert(
            ExtCostsFloat::StorageReadKeyByte,
            (self.aggregated[&Metric::CallStorageRead100W1KB].sigma_micros() as f64
                - self.aggregated[&Metric::CallStorageRead100W10B].sigma_micros() as f64
                - fixture_1kb)
                / (100f64 * (1024f64 - 10f64)),
        );
        res.insert(ExtCostsFloat::StorageReadValueByte, res[&ExtCostsFloat::StorageReadKeyByte]);

        res.insert(
            ExtCostsFloat::StorageWriteBase,
            (self.aggregated[&Metric::CallStorageRead100W10B].sigma_micros() as f64 - fixture_10b)
                / (100f64),
        );
        res.insert(
            ExtCostsFloat::StorageWriteKeyByte,
            (self.aggregated[&Metric::CallStorageWrite100W1KB].sigma_micros() as f64
                - self.aggregated[&Metric::CallStorageWrite100W10B].sigma_micros() as f64
                - fixture_1kb
                + fixture_10b)
                / (100f64 * (1024f64 - 10f64)),
        );
        res.insert(ExtCostsFloat::StorageWriteValueByte, res[&ExtCostsFloat::StorageWriteKeyByte]);

        res.insert(
            ExtCostsFloat::StorageHasKeyBase,
            (self.aggregated[&Metric::CallStorageHasKey100W10B].sigma_micros() as f64
                - fixture_10b)
                / (100f64),
        );
        res.insert(
            ExtCostsFloat::StorageHasKeyByte,
            (self.aggregated[&Metric::CallStorageHasKey100W1KB].sigma_micros() as f64
                - self.aggregated[&Metric::CallStorageHasKey100W10B].sigma_micros() as f64
                - fixture_1kb
                + fixture_10b)
                / (100f64 * (1024f64 - 10f64)),
        );

        res.insert(
            ExtCostsFloat::StorageRemoveBase,
            (self.aggregated[&Metric::CallStorageRemove100W10B].sigma_micros() as f64
                - fixture_10b)
                / (100f64),
        );
        res.insert(
            ExtCostsFloat::StorageRemoveKeyByte,
            (self.aggregated[&Metric::CallStorageRemove100W1KB].sigma_micros() as f64
                - self.aggregated[&Metric::CallStorageRemove100W10B].sigma_micros() as f64
                - fixture_1kb
                + fixture_10b)
                / (100f64 * (1024f64 - 10f64)),
        );
        // TODO: Here we are overcharging StorageRemoveKeyByte and StorageRemoveRetValueByte
        res.insert(
            ExtCostsFloat::StorageRemoveRetValueByte,
            res[&ExtCostsFloat::StorageRemoveKeyByte],
        );

        res.insert(
            ExtCostsFloat::StorageIterCreatePrefixBase,
            (self.aggregated[&Metric::CallStorageIterPrefix100W10B].sigma_micros() as f64
                - fixture_10b)
                / (100f64),
        );

        res.insert(
            ExtCostsFloat::StorageIterCreateRangeBase,
            (self.aggregated[&Metric::CallStorageIterRange100W10B].sigma_micros() as f64
                - fixture_10b)
                / (100f64),
        );
        res.insert(
            ExtCostsFloat::StorageIterCreateKeyByte,
            (self.aggregated[&Metric::CallStorageIterPrefix100W1KB].sigma_micros() as f64
                - self.aggregated[&Metric::CallStorageIterPrefix100W10B].sigma_micros() as f64
                - fixture_1kb
                + fixture_10b)
                / (100f64 * (1024f64 - 10f64)),
        );

        res.insert(
            ExtCostsFloat::StorageIterNextBase,
            (self.aggregated[&Metric::CallStorageIterNext1KW10B].sigma_micros() as f64
                - fixture_10b)
                / (1000f64),
        );
        res.insert(
            ExtCostsFloat::StorageIterNextKeyByte,
            (self.aggregated[&Metric::CallStorageIterNext1KW1KB].sigma_micros() as f64
                - self.aggregated[&Metric::CallStorageIterNext1KW10B].sigma_micros() as f64
                - fixture_1kb
                + fixture_10b)
                / (1000f64),
        );
        res.insert(
            ExtCostsFloat::StorageIterNextValueByte,
            res[&ExtCostsFloat::StorageIterNextKeyByte],
        );

        // We overcharge reading register as reading input.
        res.insert(
            ExtCostsFloat::ReadRegisterBase,
            self.aggregated[&Metric::CallInput1KW10B].sigma_micros() as f64 / (1000f64),
        );
        res.insert(
            ExtCostsFloat::ReadRegisterByte,
            (self.aggregated[&Metric::CallInput1KW10KB].sigma_micros() as f64
                - self.aggregated[&Metric::CallInput1KW10B].sigma_micros() as f64)
                / (1000f64 * (10f64 * 1024f64 - 10f64)),
        );

        // We overcharge writing to register like writing to storage.
        res.insert(ExtCostsFloat::WriteRegisterBase, res[&ExtCostsFloat::StorageWriteBase]);
        res.insert(ExtCostsFloat::WriteRegisterByte, res[&ExtCostsFloat::StorageWriteKeyByte]);

        res.insert(
            ExtCostsFloat::AccountBalance,
            (self.aggregated[&Metric::CallAccountBalance10K].sigma_micros() as f64
                - self.aggregated[&Metric::CallNoop].sigma_micros() as f64)
                / (10_000f64),
        );
        res.insert(
            ExtCostsFloat::PrepaidGas,
            (self.aggregated[&Metric::CallPrepaidGas10K].sigma_micros() as f64
                - self.aggregated[&Metric::CallNoop].sigma_micros() as f64)
                / (10_000f64),
        );
        res.insert(
            ExtCostsFloat::UsedGas,
            (self.aggregated[&Metric::CallUsedGas10K].sigma_micros() as f64
                - self.aggregated[&Metric::CallNoop].sigma_micros() as f64)
                / (10_000f64),
        );
        res.insert(
            ExtCostsFloat::UsedGas,
            (self.aggregated[&Metric::CallRandomSeed10K].sigma_micros() as f64
                - self.aggregated[&Metric::CallNoop].sigma_micros() as f64)
                / (10_000f64),
        );
        res.insert(
            ExtCostsFloat::Sha256,
            self.aggregated[&Metric::CallSHA25610KW10B].sigma_micros() as f64 - fixture_10b,
        );
        res.insert(
            ExtCostsFloat::Sha256Byte,
            self.aggregated[&Metric::CallSHA25610KW1KB].sigma_micros() as f64
                - self.aggregated[&Metric::CallSHA25610KW1KB].sigma_micros() as f64
                - fixture_10b
                + fixture_1kb,
        );
        res.insert(
            ExtCostsFloat::AttachedDeposit,
            (self.aggregated[&Metric::CallAttachedDeposit10K].sigma_micros() as f64
                - self.aggregated[&Metric::CallNoop].sigma_micros() as f64)
                / (10_000f64),
        );
        res.insert(
            ExtCostsFloat::StorageUsage,
            (self.aggregated[&Metric::CallStorageUsage10K].sigma_micros() as f64
                - self.aggregated[&Metric::CallNoop].sigma_micros() as f64)
                / (10_000f64),
        );
        res.insert(
            ExtCostsFloat::BlockIndex,
            (self.aggregated[&Metric::CallBlockIndex10K].sigma_micros() as f64
                - self.aggregated[&Metric::CallNoop].sigma_micros() as f64)
                / (10_000f64),
        );
        // TODO: Here we equate block index processing time and time stamp processing time.
        res.insert(ExtCostsFloat::BlockTimestamp, res[&ExtCostsFloat::BlockIndex]);
        res.insert(
            ExtCostsFloat::CurrentAccountId,
            (self.aggregated[&Metric::CallCurrentAccountId10K].sigma_micros() as f64
                - self.aggregated[&Metric::CallNoop].sigma_micros() as f64)
                / (10_000f64),
        );
        res.insert(
            ExtCostsFloat::SignerAccountId,
            (self.aggregated[&Metric::CallSignerAccountId10K].sigma_micros() as f64
                - self.aggregated[&Metric::CallNoop].sigma_micros() as f64)
                / (10_000f64),
        );
        res.insert(
            ExtCostsFloat::SignerAccountPK,
            (self.aggregated[&Metric::CallSignerAccountPK10K].sigma_micros() as f64
                - self.aggregated[&Metric::CallNoop].sigma_micros() as f64)
                / (10_000f64),
        );
        res.insert(
            ExtCostsFloat::PredecessorAccountId,
            (self.aggregated[&Metric::CallPredecessorAccountId10K].sigma_micros() as f64
                - self.aggregated[&Metric::CallNoop].sigma_micros() as f64)
                / (10_000f64),
        );
        res
    }
}
impl std::fmt::Display for ExtCostsGenerator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        for (k, v) in self.ext_costs_in_micros() {
            writeln!(f, "{:?}\t\t\t\t{}", k, v)?;
        }
        Ok(())
    }
}

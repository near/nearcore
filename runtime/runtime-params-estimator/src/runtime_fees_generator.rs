use crate::cases::Metric;
use crate::stats::{DataStats, Measurements};
use std::collections::BTreeMap;

pub struct RuntimeFeesGenerator {
    aggregated: BTreeMap<Metric, DataStats>,
}

/// Fees for receipts and actions expressed in micros as floats.
#[derive(Clone, Copy, Hash, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub enum ReceiptFeesFloat {
    ActionReceiptCreation,
    DataReceiptCreationBase,
    DataReceiptCreationPerByte,
    ActionCreateAccount,
    ActionDeployContractBase,
    ActionDeployContractPerByte,
    ActionFunctionCallBase,
    ActionFunctionCallPerByte,
    ActionTransfer,
    ActionStake,
    ActionAddFullAccessKey,
    ActionAddFunctionAccessKeyBase,
    ActionAddFunctionAccessKeyPerByte,
    ActionDeleteKey,
    ActionDeleteAccount,
}

impl RuntimeFeesGenerator {
    pub fn new(measurement: &Measurements) -> Self {
        let aggregated = measurement.aggregate();
        Self { aggregated }
    }

    /// Compute fees for receipts and actions in microseconds as floats.
    pub fn compute(&self) -> BTreeMap<ReceiptFeesFloat, f64> {
        let mut res: BTreeMap<ReceiptFeesFloat, f64> = Default::default();
        res.insert(
            ReceiptFeesFloat::ActionReceiptCreation,
            self.aggregated[&Metric::Receipt].upper() as f64,
        );
        res.insert(
            ReceiptFeesFloat::DataReceiptCreationBase,
            self.aggregated[&Metric::data_receipt_10b_1000].upper() as f64 / 1000f64,
        );
        res.insert(
            ReceiptFeesFloat::DataReceiptCreationPerByte,
            self.aggregated[&Metric::data_receipt_100kib_1000].upper() as f64
                / (1000f64 * 100f64 * 1024f64),
        );
        res.insert(
            ReceiptFeesFloat::ActionCreateAccount,
            (self.aggregated[&Metric::ActionCreateAccount].upper() as i64
                - self.aggregated[&Metric::Receipt].upper() as i64) as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionDeployContractBase,
            // TODO: This is a base cost, so we should not be charging for bytes here.
            // We ignore the fact that this includes 10K contract.
            (self.aggregated[&Metric::ActionDeploy10K].upper() as i64
                - self.aggregated[&Metric::Receipt].upper() as i64) as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionDeployContractPerByte,
            ((self.aggregated[&Metric::ActionDeploy1M].upper() as i64
                - self.aggregated[&Metric::ActionDeploy100K].upper() as i64) as f64)
                / (1024f64 * 1024f64 - 100f64 * 1024f64),
        );
        res.insert(
            ReceiptFeesFloat::ActionFunctionCallBase,
            (self.aggregated[&Metric::noop].upper() - self.aggregated[&Metric::Receipt].upper())
                as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionFunctionCallPerByte,
            (self.aggregated[&Metric::noop_1MiB].upper() as i64
                - self.aggregated[&Metric::noop].upper() as i64) as f64
                / (1024f64 * 1024f64),
        );
        res.insert(
            ReceiptFeesFloat::ActionTransfer,
            (self.aggregated[&Metric::ActionTransfer].upper() as i64
                - self.aggregated[&Metric::Receipt].upper() as i64) as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionStake,
            (self.aggregated[&Metric::ActionStake].upper() as i64
                - self.aggregated[&Metric::Receipt].upper() as i64) as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionAddFullAccessKey,
            (self.aggregated[&Metric::ActionAddFullAccessKey].upper() as i64
                - self.aggregated[&Metric::Receipt].upper() as i64) as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionAddFunctionAccessKeyBase,
            (self.aggregated[&Metric::ActionAddFunctionAccessKey1Method].upper() as i64
                - self.aggregated[&Metric::Receipt].upper() as i64) as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionAddFunctionAccessKeyPerByte,
            // These are 1k methods each 10bytes long.
            (self.aggregated[&Metric::ActionAddFunctionAccessKey1000Methods].upper() as i64
                - self.aggregated[&Metric::ActionAddFunctionAccessKey1Method].upper() as i64)
                as f64
                / (1000f64 * 10f64),
        );
        res.insert(
            ReceiptFeesFloat::ActionDeleteKey,
            (self.aggregated[&Metric::ActionDeleteAccessKey].upper() as i64
                - self.aggregated[&Metric::Receipt].upper() as i64) as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionDeleteAccount,
            (self.aggregated[&Metric::ActionDeleteAccount].upper() as i64
                - self.aggregated[&Metric::Receipt].upper() as i64) as f64,
        );
        res
    }
}

impl std::fmt::Display for RuntimeFeesGenerator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (k, v) in self.compute() {
            writeln!(f, "{:?}\t\t\t\t{}", k, v)?;
        }
        Ok(())
    }
}

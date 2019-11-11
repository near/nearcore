use crate::cases::Metric;
use crate::stats::{DataStats, Measurements};
use near_runtime_fees::{
    AccessKeyCreationConfig, ActionCreationConfig, DataReceiptCreationConfig, Fee, Fraction,
    RuntimeFeesConfig, StorageUsageConfig,
};
use std::collections::{BTreeMap, HashMap};

pub struct RuntimeFeesGenerator {
    aggregated: BTreeMap<Metric, DataStats>,
}

/// Fees for receipts and actions expressed in micros as floats.
#[derive(Clone, Copy, Hash, PartialEq, Eq, Debug)]
enum ReceiptFeesFloat {
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
    fn receipt_fees_in_micros(&self) -> HashMap<ReceiptFeesFloat, f64> {
        let mut res = HashMap::new();
        res.insert(
            ReceiptFeesFloat::ActionReceiptCreation,
            self.aggregated[&Metric::Receipt].upper() as f64,
        );
        res.insert(
            ReceiptFeesFloat::DataReceiptCreationBase,
            self.aggregated[&Metric::CallPromiseBatchCreateThen10K].upper() as f64
                - self.aggregated[&Metric::CallPromiseBatchCreate10K].upper() as f64,
        );
        res.insert(
            ReceiptFeesFloat::DataReceiptCreationPerByte,
            // TODO: Currently we set it to 0, because we need to figure out how to compute it.
            0f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionCreateAccount,
            self.aggregated[&Metric::ActionCreateAccount].upper() as f64
                - self.aggregated[&Metric::Receipt].upper() as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionDeployContractBase,
            // TODO: This is a base cost, so we should not be charging for bytes here.
            // We ignore the fact that this includes 10K contract.
            self.aggregated[&Metric::ActionDeploy10K].upper() as f64
                - self.aggregated[&Metric::Receipt].upper() as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionDeployContractPerByte,
            (self.aggregated[&Metric::ActionDeploy1M].upper() as f64
                - self.aggregated[&Metric::ActionDeploy100K].upper() as f64)
                / (1024f64 * 1024f64 - 100f64 * 1024f64),
        );
        res.insert(
            ReceiptFeesFloat::ActionFunctionCallBase,
            (self.aggregated[&Metric::CallNoop].upper() - self.aggregated[&Metric::Receipt].upper())
                as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionFunctionCallPerByte,
            // TODO: Currently we set it to 0, because we need to figure out how to compute it.
            0f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionTransfer,
            self.aggregated[&Metric::ActionTransfer].upper() as f64
                - self.aggregated[&Metric::Receipt].upper() as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionStake,
            self.aggregated[&Metric::ActionStake].upper() as f64
                - self.aggregated[&Metric::Receipt].upper() as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionAddFullAccessKey,
            self.aggregated[&Metric::ActionAddFullAccessKey].upper() as f64
                - self.aggregated[&Metric::Receipt].upper() as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionAddFunctionAccessKeyBase,
            self.aggregated[&Metric::ActionAddFunctionAccessKey1Method].upper() as f64
                - self.aggregated[&Metric::Receipt].upper() as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionAddFunctionAccessKeyPerByte,
            // These are 1k methods each 10bytes long.
            (self.aggregated[&Metric::ActionAddFunctionAccessKey1000Methods].upper() as f64
                - self.aggregated[&Metric::ActionAddFunctionAccessKey1Method].upper() as f64)
                / (1000f64 * 10f64),
        );
        res.insert(
            ReceiptFeesFloat::ActionDeleteKey,
            self.aggregated[&Metric::ActionDeleteAccessKey].upper() as f64
                - self.aggregated[&Metric::Receipt].upper() as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionDeleteAccount,
            self.aggregated[&Metric::ActionDeleteAccount].upper() as f64
                - self.aggregated[&Metric::Receipt].upper() as f64,
        );
        res
    }
}

impl std::fmt::Display for RuntimeFeesGenerator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        for (k, v) in self.receipt_fees_in_micros() {
            writeln!(f, "{:?}\t\t\t\t{}", k, v)?;
        }
        Ok(())
    }
}

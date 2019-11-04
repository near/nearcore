use crate::cases::Metric;
use crate::stats::{DataStats, Measurements};
use near_runtime_fees::{
    AccessKeyCreationConfig, ActionCreationConfig, DataReceiptCreationConfig, ExtCostsConfig, Fee,
    Fraction, RuntimeFeesConfig, StorageUsageConfig,
};
use std::collections::{BTreeMap, HashMap};

pub struct RuntimeFeesGenerator {
    measurement: Measurements,
    aggregated: BTreeMap<Metric, DataStats>,
}

/// Fees for receipts and actions measured in floats.
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
    pub fn new(measurement: Measurements) -> Self {
        let aggregated = measurement.aggregate();
        Self { measurement, aggregated }
    }

    /// Compute fees for receipts and actions in microseconds as floats.
    fn receipt_fees_in_micros(&self) -> HashMap<ReceiptFeesFloat, f64> {
        let mut res = HashMap::new();
        res.insert(
            ReceiptFeesFloat::ActionReceiptCreation,
            self.aggregated[&Metric::Receipt].sigma_micros() as f64,
        );
        res.insert(
            ReceiptFeesFloat::DataReceiptCreationBase,
            self.aggregated[&Metric::CallPromiseBatchCreateThen10K].sigma_micros() as f64
                - self.aggregated[&Metric::CallPromiseBatchCreate10K].sigma_micros() as f64,
        );
        res.insert(
            ReceiptFeesFloat::DataReceiptCreationPerByte,
            // TODO: Currently we set it to 0, because we need to figure out how to compute it.
            0f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionCreateAccount,
            self.aggregated[&Metric::ActionCreateAccount].sigma_micros() as f64
                - self.aggregated[&Metric::Receipt].sigma_micros() as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionDeployContractBase,
            // TODO: This is a base cost, so we should not be charging for bytes here.
            // We ignore the fact that this includes 10K contract.
            self.aggregated[&Metric::ActionDeploy10K].sigma_micros() as f64
                - self.aggregated[&Metric::Receipt].sigma_micros() as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionDeployContractPerByte,
            (self.aggregated[&Metric::ActionDeploy1M].sigma_micros() as f64
                - self.aggregated[&Metric::ActionDeploy100K].sigma_micros() as f64)
                / (1024f64 * 1024f64 - 100f64 * 1024f64),
        );
        res.insert(
            ReceiptFeesFloat::ActionFunctionCallBase,
            (self.aggregated[&Metric::CallNoop].sigma_micros()
                - self.aggregated[&Metric::Receipt].sigma_micros()) as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionFunctionCallPerByte,
            // TODO: Currently we set it to 0, because we need to figure out how to compute it.
            0f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionTransfer,
            self.aggregated[&Metric::ActionTransfer].sigma_micros() as f64
                - self.aggregated[&Metric::Receipt].sigma_micros() as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionStake,
            self.aggregated[&Metric::ActionStake].sigma_micros() as f64
                - self.aggregated[&Metric::Receipt].sigma_micros() as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionAddFullAccessKey,
            self.aggregated[&Metric::ActionAddFullAccessKey].sigma_micros() as f64
                - self.aggregated[&Metric::Receipt].sigma_micros() as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionAddFunctionAccessKeyBase,
            self.aggregated[&Metric::ActionAddFunctionAccessKey1Method].sigma_micros() as f64
                - self.aggregated[&Metric::Receipt].sigma_micros() as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionAddFunctionAccessKeyPerByte,
            // These are 1k methods each 10bytes long.
            (self.aggregated[&Metric::ActionAddFunctionAccessKey1000Methods].sigma_micros() as f64
                - self.aggregated[&Metric::ActionAddFunctionAccessKey1Method].sigma_micros()
                    as f64)
                / (1000f64 * 10f64),
        );
        res.insert(
            ReceiptFeesFloat::ActionDeleteKey,
            self.aggregated[&Metric::ActionDeleteAccessKey].sigma_micros() as f64
                - self.aggregated[&Metric::Receipt].sigma_micros() as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionDeleteAccount,
            self.aggregated[&Metric::ActionDeleteAccount].sigma_micros() as f64
                - self.aggregated[&Metric::Receipt].sigma_micros() as f64,
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

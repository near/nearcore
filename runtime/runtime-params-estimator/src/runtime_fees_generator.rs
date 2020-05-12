use crate::cases::Metric;
use crate::stats::{DataStats, Measurements};
use num_rational::Ratio;
use std::collections::BTreeMap;

pub struct RuntimeFeesGenerator {
    aggregated: BTreeMap<Metric, DataStats>,
}

/// Fees for receipts and actions.
#[derive(Clone, Copy, Hash, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub enum ReceiptFees {
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

    /// Compute fees for receipts and actions in measurment units, keeps result as rational.
    pub fn compute(&self) -> BTreeMap<ReceiptFees, Ratio<u64>> {
        let mut res: BTreeMap<ReceiptFees, Ratio<u64>> = Default::default();
        res.insert(
            ReceiptFees::ActionReceiptCreation,
            Ratio::new(self.aggregated[&Metric::Receipt].upper(), 1),
        );
        res.insert(
            ReceiptFees::DataReceiptCreationBase,
            Ratio::new(self.aggregated[&Metric::data_receipt_10b_1000].upper(), 1000),
        );
        res.insert(
            ReceiptFees::DataReceiptCreationPerByte,
            Ratio::new(
                self.aggregated[&Metric::data_receipt_100kib_1000].upper(),
                1000 * 100 * 1024,
            ),
        );
        res.insert(
            ReceiptFees::ActionCreateAccount,
            Ratio::new(self.aggregated[&Metric::ActionCreateAccount].upper(), 1),
        );
        res.insert(
            ReceiptFees::ActionDeployContractBase,
            // TODO: This is a base cost, so we should not be charging for bytes here.
            // We ignore the fact that this includes 10K contract.
            Ratio::new(self.aggregated[&Metric::ActionDeploy10K].upper(), 1),
        );
        res.insert(
            ReceiptFees::ActionDeployContractPerByte,
            Ratio::new(self.aggregated[&Metric::ActionDeploy1M].upper(), 1024 * 1024),
        );
        res.insert(
            ReceiptFees::ActionFunctionCallBase,
            Ratio::new(self.aggregated[&Metric::noop].upper(), 1),
        );
        res.insert(
            ReceiptFees::ActionFunctionCallPerByte,
            Ratio::new(self.aggregated[&Metric::noop_1MiB].upper(), 1024 * 1024),
        );
        res.insert(
            ReceiptFees::ActionTransfer,
            Ratio::new(self.aggregated[&Metric::ActionTransfer].upper(), 1),
        );
        res.insert(
            ReceiptFees::ActionStake,
            Ratio::new(self.aggregated[&Metric::ActionStake].upper(), 1),
        );
        res.insert(
            ReceiptFees::ActionAddFullAccessKey,
            Ratio::new(self.aggregated[&Metric::ActionAddFullAccessKey].upper(), 1),
        );
        res.insert(
            ReceiptFees::ActionAddFunctionAccessKeyBase,
            Ratio::new(self.aggregated[&Metric::ActionAddFunctionAccessKey1Method].upper(), 1),
        );
        res.insert(
            ReceiptFees::ActionAddFunctionAccessKeyPerByte,
            // These are 1k methods each 10bytes long.
            Ratio::new(self.aggregated[&Metric::ActionAddFunctionAccessKey1000Methods].upper(), 10),
        );
        res.insert(
            ReceiptFees::ActionDeleteKey,
            Ratio::new(self.aggregated[&Metric::ActionDeleteAccessKey].upper(), 1),
        );
        res.insert(
            ReceiptFees::ActionDeleteAccount,
            Ratio::new(self.aggregated[&Metric::ActionDeleteAccount].upper(), 1),
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

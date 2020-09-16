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
    ActionSirReceiptCreation,
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
            ReceiptFees::ActionSirReceiptCreation,
            Ratio::new(self.aggregated[&Metric::SirReceipt].upper(), 1),
        );
        res.insert(
            ReceiptFees::DataReceiptCreationBase,
            Ratio::new(
                self.aggregated[&Metric::data_receipt_10b_1000]
                    .upper_with_base(&self.aggregated[&Metric::data_receipt_base_10b_1000]),
                1000,
            ),
        );
        // Note we subtract base that has a denominator as well. This is fine, since per byte
        // denominator is much larger (100K times)
        res.insert(
            ReceiptFees::DataReceiptCreationPerByte,
            Ratio::new(
                self.aggregated[&Metric::data_receipt_100kib_1000]
                    .upper_with_base(&self.aggregated[&Metric::data_receipt_10b_1000]),
                1000 * 100 * 1024,
            ),
        );
        res.insert(
            ReceiptFees::ActionCreateAccount,
            Ratio::new(
                self.aggregated[&Metric::ActionCreateAccount]
                    .upper_with_base(&self.aggregated[&Metric::Receipt]),
                1,
            ),
        );
        res.insert(
            ReceiptFees::ActionDeployContractBase,
            // We ignore the fact that this includes a 143 bytes contract.
            Ratio::new(
                self.aggregated[&Metric::ActionDeploySmallest]
                    .upper_with_base(&self.aggregated[&Metric::SirReceipt]),
                1,
            ),
        );
        res.insert(
            ReceiptFees::ActionDeployContractPerByte,
            Ratio::new(
                self.aggregated[&Metric::ActionDeploy1M]
                    .upper_with_base(&self.aggregated[&Metric::ActionDeploySmallest]),
                1024 * 1024,
            ),
        );
        res.insert(
            ReceiptFees::ActionFunctionCallBase,
            Ratio::new(
                self.aggregated[&Metric::noop].upper_with_base(&self.aggregated[&Metric::Receipt]),
                1,
            ),
        );
        res.insert(
            ReceiptFees::ActionFunctionCallPerByte,
            Ratio::new(
                self.aggregated[&Metric::noop_1MiB]
                    .upper_with_base(&self.aggregated[&Metric::noop]),
                1024 * 1024,
            ),
        );
        res.insert(
            ReceiptFees::ActionTransfer,
            Ratio::new(
                self.aggregated[&Metric::ActionTransfer]
                    .upper_with_base(&self.aggregated[&Metric::Receipt]),
                1,
            ),
        );
        res.insert(
            ReceiptFees::ActionStake,
            Ratio::new(
                self.aggregated[&Metric::ActionStake]
                    .upper_with_base(&self.aggregated[&Metric::SirReceipt]),
                1,
            ),
        );
        res.insert(
            ReceiptFees::ActionAddFullAccessKey,
            Ratio::new(
                self.aggregated[&Metric::ActionAddFullAccessKey]
                    .upper_with_base(&self.aggregated[&Metric::SirReceipt]),
                1,
            ),
        );
        res.insert(
            ReceiptFees::ActionAddFunctionAccessKeyBase,
            Ratio::new(
                self.aggregated[&Metric::ActionAddFunctionAccessKey1Method]
                    .upper_with_base(&self.aggregated[&Metric::SirReceipt]),
                1,
            ),
        );
        res.insert(
            ReceiptFees::ActionAddFunctionAccessKeyPerByte,
            // These are 1k methods each 10bytes long.
            Ratio::new(
                self.aggregated[&Metric::ActionAddFunctionAccessKey1000Methods]
                    .upper_with_base(&self.aggregated[&Metric::ActionAddFunctionAccessKey1Method]),
                10 * 1000,
            ),
        );
        res.insert(
            ReceiptFees::ActionDeleteKey,
            Ratio::new(
                self.aggregated[&Metric::ActionDeleteAccessKey]
                    .upper_with_base(&self.aggregated[&Metric::SirReceipt]),
                1,
            ),
        );
        res.insert(
            ReceiptFees::ActionDeleteAccount,
            Ratio::new(
                self.aggregated[&Metric::ActionDeleteAccount]
                    .upper_with_base(&self.aggregated[&Metric::SirReceipt]),
                1,
            ),
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

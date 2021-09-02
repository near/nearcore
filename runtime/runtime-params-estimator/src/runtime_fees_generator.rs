use crate::cases::Metric;
use crate::stats::{DataStats, Measurements};
use num_rational::Ratio;
use std::collections::BTreeMap;

pub struct RuntimeFeesGenerator {
    aggregated: BTreeMap<Metric, DataStats>,
    result: BTreeMap<ReceiptFees, Ratio<u64>>,
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
        Self { aggregated, result: BTreeMap::default() }
    }

    fn extract(&mut self, metric: Metric, denom: u64, fee: ReceiptFees) {
        if let Some(value) = self.aggregated.get(&metric) {
            let value = value.upper();
            let ratio = Ratio::new(value, denom);
            self.result.insert(fee, ratio);
        }
    }

    fn extract_with_base(
        &mut self,
        metric: Metric,
        base_metric: Metric,
        denom: u64,
        fee: ReceiptFees,
    ) {
        if let Some(value) = self.aggregated.get(&metric) {
            if let Some(base_value) = self.aggregated.get(&base_metric) {
                let value = value.upper_with_base(base_value);
                let ratio = Ratio::new(value, denom);
                self.result.insert(fee, ratio);
            }
        }
    }

    /// Compute fees for receipts and actions in measurment units, keeps result as rational.
    pub fn compute(mut self) -> BTreeMap<ReceiptFees, Ratio<u64>> {
        use Metric::*;

        self.extract(Receipt, 1, ReceiptFees::ActionReceiptCreation);
        self.extract(SirReceipt, 1, ReceiptFees::ActionSirReceiptCreation);

        self.extract_with_base(
            data_receipt_10b_1000,
            data_receipt_base_10b_1000,
            1000,
            ReceiptFees::DataReceiptCreationBase,
        );

        // Note we subtract base that has a denominator as well. This is fine, since per byte
        // denominator is much larger (100K times)
        self.extract_with_base(
            data_receipt_100kib_1000,
            data_receipt_10b_1000,
            1000 * 100 * 1024,
            ReceiptFees::DataReceiptCreationPerByte,
        );
        self.extract_with_base(ActionCreateAccount, Receipt, 1, ReceiptFees::ActionCreateAccount);

        // We ignore the fact that this includes a 143 bytes contract.
        self.extract_with_base(
            ActionDeploySmallest,
            SirReceipt,
            1,
            ReceiptFees::ActionDeployContractBase,
        );
        self.extract_with_base(
            ActionDeploy1M,
            ActionDeploySmallest,
            1024 * 1024,
            ReceiptFees::ActionDeployContractPerByte,
        );
        self.extract_with_base(noop, SirReceipt, 1, ReceiptFees::ActionFunctionCallBase);
        self.extract_with_base(
            noop_1MiB,
            noop,
            1024 * 1024,
            ReceiptFees::ActionFunctionCallPerByte,
        );
        self.extract_with_base(ActionTransfer, Receipt, 1, ReceiptFees::ActionTransfer);
        self.extract_with_base(ActionStake, SirReceipt, 1, ReceiptFees::ActionStake);
        self.extract_with_base(
            ActionAddFullAccessKey,
            SirReceipt,
            1,
            ReceiptFees::ActionAddFullAccessKey,
        );
        self.extract_with_base(
            ActionAddFunctionAccessKey1Method,
            SirReceipt,
            1,
            ReceiptFees::ActionAddFunctionAccessKeyBase,
        );
        // These are 1k methods each 10bytes long.
        self.extract_with_base(
            ActionAddFunctionAccessKey1000Methods,
            ActionAddFunctionAccessKey1Method,
            10 * 1000,
            ReceiptFees::ActionAddFunctionAccessKeyPerByte,
        );
        self.extract_with_base(ActionDeleteAccessKey, SirReceipt, 1, ReceiptFees::ActionDeleteKey);
        self.extract_with_base(
            ActionDeleteAccount,
            SirReceipt,
            1,
            ReceiptFees::ActionDeleteAccount,
        );

        self.result
    }
}

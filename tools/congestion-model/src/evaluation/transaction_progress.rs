use crate::{GGas, Transaction};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TransactionStatus {
    Init,
    Pending,
    Failed,
    FinishedSuccess,
}

impl Transaction {
    pub(crate) fn status(&self) -> TransactionStatus {
        if !self.dropped_receipts.is_empty() {
            return TransactionStatus::Failed;
        }

        if !self.pending_receipts.is_empty() {
            return TransactionStatus::Pending;
        }

        if self.executed_receipts.is_empty() {
            return TransactionStatus::Init;
        }

        return TransactionStatus::FinishedSuccess;
    }

    pub(crate) fn gas_burnt(&self) -> GGas {
        if self.future_receipts.contains_key(&self.initial_receipt) {
            return 0;
        }
        let receipts_gas: GGas =
            self.executed_receipts.values().map(|receipt| receipt.gas_burnt()).sum();
        receipts_gas + self.tx_conversion_cost
    }
}

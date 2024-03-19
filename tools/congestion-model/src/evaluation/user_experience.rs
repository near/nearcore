use crate::{Model, Round, Transaction, TransactionStatus};

#[derive(Debug, Clone)]
pub struct UserExperience {
    pub successful_tx_delay_avg: Round,
    pub successful_tx_delay_median: Round,
    pub successful_tx_delay_90th_percentile: Round,
    pub rejected_tx_delay_avg: Round,
    pub rejected_tx_delay_median: Round,
    pub rejected_tx_delay_90th_percentile: Round,
    pub unresolved_transactions: u64,
}

impl Model {
    pub fn user_experience(&self) -> UserExperience {
        let mut successful_delays = vec![];
        let mut rejected_delays = vec![];
        let mut unresolved_transactions = 0;
        for tx in self.transactions.all_transactions() {
            match tx.status() {
                TransactionStatus::Init | TransactionStatus::Pending => {
                    unresolved_transactions += 1
                }
                TransactionStatus::Failed => rejected_delays.push(tx.delay()),
                TransactionStatus::FinishedSuccess => successful_delays.push(tx.delay()),
            }
        }

        successful_delays.sort();
        rejected_delays.sort();

        UserExperience {
            successful_tx_delay_avg: avg(&successful_delays),
            successful_tx_delay_median: percentile(&successful_delays, 50),
            successful_tx_delay_90th_percentile: percentile(&successful_delays, 90),
            rejected_tx_delay_avg: avg(&rejected_delays),
            rejected_tx_delay_median: percentile(&rejected_delays, 50),
            rejected_tx_delay_90th_percentile: percentile(&rejected_delays, 90),
            unresolved_transactions,
        }
    }
}

impl Transaction {
    pub fn delay(&self) -> Round {
        self.last_change() - self.submitted_at
    }

    pub fn last_change(&self) -> Round {
        let mut last_change = self.submitted_at;
        for receipt in self.executed_receipts.values() {
            last_change = last_change.max(receipt.executed_at.unwrap());
        }
        for receipt in self.dropped_receipts.values() {
            last_change = last_change.max(receipt.dropped_at.unwrap());
        }
        last_change
    }
}

fn avg(data: &[u64]) -> u64 {
    if data.is_empty() {
        0
    } else {
        data.iter().copied().sum::<u64>() / data.len() as u64
    }
}

/// Input must be sorted
fn percentile(data: &[u64], p: usize) -> u64 {
    if data.is_empty() {
        0
    } else {
        data[(data.len() - 1) * p / 100]
    }
}

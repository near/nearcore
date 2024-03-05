pub use transaction_progress::TransactionStatus;

use crate::{GGas, Model, ShardId};
use std::collections::HashMap;

pub mod summary_table;
mod transaction_progress;

#[derive(Debug, Clone)]
pub struct ShardQueueLengths {
    pub unprocessed_incoming_transactions: u64,
    pub incoming_receipts: u64,
    pub queued_receipts: u64,
}

#[derive(Debug, Clone)]
pub struct GasThroughput {
    pub total: GGas,
}

#[derive(Debug, Clone)]
pub struct Progress {
    pub finished_transactions: usize,
    pub pending_transactions: usize,
    pub waiting_transactions: usize,
    pub failed_transactions: usize,
}

impl Model {
    pub fn queue_lengths(&self) -> HashMap<ShardId, ShardQueueLengths> {
        let mut out = HashMap::new();
        for shard in self.shard_ids.clone() {
            let unprocessed_incoming_transactions =
                self.queues.incoming_transactions(shard).len() as u64;
            let incoming_receipts = self.queues.incoming_receipts(shard).len() as u64;
            let total_shard_receipts: u64 =
                self.queues.shard_queues(shard).map(|q| q.len() as u64).sum();

            let shard_stats = ShardQueueLengths {
                unprocessed_incoming_transactions,
                incoming_receipts,
                queued_receipts: total_shard_receipts - incoming_receipts,
            };
            out.insert(shard, shard_stats);
        }
        out
    }

    pub fn gas_throughput(&self) -> GasThroughput {
        GasThroughput { total: self.transactions.all_transactions().map(|tx| tx.gas_burnt()).sum() }
    }

    pub fn progress(&self) -> Progress {
        let mut finished_transactions = 0;
        let mut pending_transactions = 0;
        let mut waiting_transactions = 0;
        let mut failed_transactions = 0;

        for tx in self.transactions.all_transactions() {
            match tx.status() {
                TransactionStatus::Init => waiting_transactions += 1,
                TransactionStatus::Pending => pending_transactions += 1,
                TransactionStatus::Failed => failed_transactions += 1,
                TransactionStatus::FinishedSuccess => finished_transactions += 1,
            }
        }

        Progress {
            finished_transactions,
            pending_transactions,
            waiting_transactions,
            failed_transactions,
        }
    }
}

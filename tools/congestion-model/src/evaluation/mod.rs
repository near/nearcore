use chrono::{Duration, Utc};
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

// The stats writer can be used to dump stats into a CSV file.
pub type StatsWriter = Option<Box<csv::Writer<std::fs::File>>>;

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

    pub fn write_stats_header(&self, stats_writer: &mut StatsWriter) {
        let Some(stats_writer) = stats_writer else { return };

        stats_writer.write_field("time").unwrap();
        stats_writer.write_field("round").unwrap();

        stats_writer.write_field("finished_transactions").unwrap();
        stats_writer.write_field("pending_transactions").unwrap();
        stats_writer.write_field("waiting_transactions").unwrap();
        stats_writer.write_field("failed_transactions").unwrap();

        for shard_id in self.shard_ids.clone() {
            for queue in self.queues.shard_queues(shard_id) {
                let field_name = format!("shard_{}_queue_{}", shard_id, queue.name());
                stats_writer.write_field(format!("{field_name}_len")).unwrap();
                stats_writer.write_field(format!("{field_name}_bytes")).unwrap();
                stats_writer.write_field(format!("{field_name}_gas")).unwrap();
            }
        }
        stats_writer.write_record(None::<&[u8]>).unwrap();
    }

    pub fn write_stats_values(
        &self,
        stats_writer: &mut StatsWriter,
        start_time: chrono::prelude::DateTime<Utc>,
        round: usize,
    ) {
        let Some(stats_writer) = stats_writer else { return };

        let time = start_time + Duration::seconds(round as i64);

        stats_writer.write_field(format!("{:?}", time)).unwrap();
        stats_writer.write_field(format!("{}", round)).unwrap();

        // This is slow and takes up to 10s for the slowest workloads and strategies.
        let progress = self.progress();
        stats_writer.write_field(format!("{}", progress.finished_transactions)).unwrap();
        stats_writer.write_field(format!("{}", progress.pending_transactions)).unwrap();
        stats_writer.write_field(format!("{}", progress.waiting_transactions)).unwrap();
        stats_writer.write_field(format!("{}", progress.failed_transactions)).unwrap();

        for shard_id in self.shard_ids.clone() {
            for queue in self.queues.shard_queues(shard_id) {
                stats_writer.write_field(format!("{}", queue.len())).unwrap();
                stats_writer.write_field(format!("{}", queue.size())).unwrap();
                stats_writer.write_field(format!("{}", queue.attached_gas())).unwrap();
            }
        }

        stats_writer.write_record(None::<&[u8]>).unwrap();
    }
}

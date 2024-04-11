use chrono::{Duration, Utc};
pub use queue_lengths::{QueueStats, ShardQueueLengths};
pub use transaction_progress::TransactionStatus;
pub use user_experience::UserExperience;

use crate::{GGas, Model};

mod queue_lengths;
pub mod summary_table;
mod transaction_progress;
mod user_experience;

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

        stats_writer.write_field("successful_tx_delay_avg").unwrap();
        stats_writer.write_field("successful_tx_delay_median").unwrap();
        stats_writer.write_field("successful_tx_delay_90th_percentile").unwrap();
        stats_writer.write_field("rejected_tx_delay_avg").unwrap();
        stats_writer.write_field("rejected_tx_delay_median").unwrap();
        stats_writer.write_field("rejected_tx_delay_90th_percentile").unwrap();
        stats_writer.write_field("unresolved_transactions").unwrap();

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

        let user_exp = self.user_experience();
        stats_writer.write_field(user_exp.successful_tx_delay_avg.to_string()).unwrap();
        stats_writer.write_field(user_exp.successful_tx_delay_median.to_string()).unwrap();
        stats_writer.write_field(user_exp.successful_tx_delay_90th_percentile.to_string()).unwrap();
        stats_writer.write_field(user_exp.rejected_tx_delay_avg.to_string()).unwrap();
        stats_writer.write_field(user_exp.rejected_tx_delay_median.to_string()).unwrap();
        stats_writer.write_field(user_exp.rejected_tx_delay_90th_percentile.to_string()).unwrap();
        stats_writer.write_field(user_exp.unresolved_transactions.to_string()).unwrap();

        stats_writer.write_record(None::<&[u8]>).unwrap();
    }
}

impl std::ops::Sub for GasThroughput {
    type Output = GasThroughput;

    fn sub(self, rhs: Self) -> Self::Output {
        Self { total: self.total - rhs.total }
    }
}

impl std::ops::Div<usize> for GasThroughput {
    type Output = GasThroughput;

    fn div(self, rhs: usize) -> Self::Output {
        Self { total: self.total / rhs as u64 }
    }
}

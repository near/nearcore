use super::{GasThroughput, Progress, ShardQueueLengths, UserExperience};
use crate::PGAS;

pub fn print_summary_header() {
    println!(
        "{:<25}{:<25}{:>25}{:>25}{:>16}{:>16}{:>16}{:>16}{:>16}",
        "WORKLOAD",
        "STRATEGY",
        "BURNT GAS",
        "TRANSACTIONS FINISHED",
        "MEDIAN TX DELAY",
        "90p TX DELAY",
        "MAX QUEUE LEN",
        "MAX QUEUE SIZE",
        "MAX QUEUE PGAS",
    );
}

pub fn print_summary_row(
    workload: &str,
    strategy: &str,
    progress: &Progress,
    throughput: &GasThroughput,
    max_queues: &ShardQueueLengths,
    user_experience: &UserExperience,
) {
    println!(
        "{workload:<25}{strategy:<25}{:>20} PGas{:>25}{:>16}{:>16}{:>16}{:>16}{:>16}",
        throughput.total / PGAS,
        progress.finished_transactions,
        user_experience.successful_tx_delay_median,
        user_experience.successful_tx_delay_90th_percentile,
        max_queues.queued_receipts.num,
        bytesize::ByteSize::b(max_queues.queued_receipts.size),
        max_queues.queued_receipts.gas / PGAS,
    );
}

use super::{GasThroughput, Progress, ShardQueueLengths};
use crate::PGAS;

pub fn print_summary_header() {
    println!(
        "{:<25}{:<25}{:>25}{:>25}{:>16}{:>16}{:>16}",
        "WORKLOAD",
        "STRATEGY",
        "BURNT GAS",
        "TRANSACTIONS FINISHED",
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
) {
    println!(
        "{workload:<25}{strategy:<25}{:>20} PGas{:>25}{:>16}{:>16}{:>16}",
        throughput.total / PGAS,
        progress.finished_transactions,
        max_queues.queued_receipts.num,
        bytesize::ByteSize::b(max_queues.queued_receipts.size),
        max_queues.queued_receipts.gas / PGAS,
    );
}

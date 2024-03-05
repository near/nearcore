use crate::{Model, PGAS};

pub fn print_summary_header() {
    println!(
        "{:<25}{:<25}{:>25}{:>25}{:>25}",
        "WORKLOAD", "STRATEGY", "BURNT GAS", "TRANSACTIONS FINISHED", "MAX QUEUE LEN",
    );
}

pub fn print_summary_row(model: &Model, workload: &str, strategy: &str) {
    let queues = model.queue_lengths();
    let throughput = model.gas_throughput();
    let progress = model.progress();

    let mut max_queue_len = 0;
    for q in queues.values() {
        let len = q.incoming_receipts + q.queued_receipts;
        max_queue_len = len.max(max_queue_len);
    }

    println!(
        "{workload:<25}{strategy:<25}{:>20} PGas{:>25}{:>25}",
        throughput.total / PGAS,
        progress.finished_transactions,
        max_queue_len
    );
}

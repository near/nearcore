use crate::remote_node::{get_result, RemoteNode};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Stats measured while executing load testing on the node.
pub struct Stats {
    /// Block index at the beginning of the testing.
    pub from_block_index: Option<u64>,
    /// Timestamp at the beginning of the testing.
    pub from_timestamp: Option<Instant>,
    /// Block index at the end of the testing.
    pub to_block_index: Option<u64>,
    /// Timestamp at the end of the testing.
    pub to_timestamp: Option<Instant>,
    /// Counter for outgoing transactions.
    pub out_tx_counter: AtomicU64,
    pub out_tx_counter_frozen: Option<u64>,
    /// Number of committed transactions.
    pub committed_transactions: Option<u64>,
}

impl std::fmt::Display for Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        let from_block_index = self.from_block_index.unwrap();
        let to_block_index = self.to_block_index.unwrap();
        let blocks_passed = to_block_index - from_block_index + 1;
        let time_passed =
            self.to_timestamp.unwrap().duration_since(self.from_timestamp.unwrap()).as_secs();
        let bps = (blocks_passed as f64) / (time_passed as f64);
        let total_txs = self.committed_transactions.unwrap();

        write!(f, "Start block:\t{}\n", from_block_index)?;
        write!(f, "End block:\t{}\n", to_block_index)?;
        write!(f, "Time passed:\t{} secs\n", time_passed)?;
        write!(f, "Blocks per second:\t{:.2}\n", bps)?;
        write!(f, "Transactions per second:\t{}\n", total_txs / time_passed)?;
        write!(
            f,
            "Outgoing transactions per second:\t{}\n",
            self.out_tx_counter_frozen.unwrap() / time_passed
        )?;
        write!(f, "Transactions per block:\t{}", total_txs / blocks_passed)?;
        Ok(())
    }
}

impl Stats {
    pub fn new() -> Self {
        Self {
            from_block_index: None,
            from_timestamp: None,
            to_block_index: None,
            to_timestamp: None,
            out_tx_counter: AtomicU64::new(0),
            out_tx_counter_frozen: None,
            committed_transactions: None,
        }
    }

    /// Count one outgoing transaction.
    pub fn inc_out_tx(&self) {
        self.out_tx_counter.fetch_add(1, Ordering::SeqCst);
    }

    /// Measure stats from this moment.
    pub fn measure_from(&mut self, node: &RemoteNode) {
        self.from_block_index = Some(get_result(|| node.get_current_block_index()));
        self.from_timestamp = Some(Instant::now());
    }

    /// Measure stats to this moment.
    pub fn measure_to(&mut self, node: &RemoteNode) {
        self.to_block_index = Some(get_result(|| node.get_current_block_index()));
        self.to_timestamp = Some(Instant::now());
        self.out_tx_counter_frozen = Some(self.out_tx_counter.load(Ordering::SeqCst));
    }

    /// Measures number of transactions that were committed.
    pub fn collect_transactions(&mut self, node: &RemoteNode) {
        let mut curr_block_index = self.from_block_index.unwrap() + 1;
        let mut total_tx = 0u64;
        loop {
            total_tx += get_result(|| node.get_transactions(curr_block_index));
            curr_block_index += 1;
            if curr_block_index > self.to_block_index.unwrap() {
                break;
            }
        }
        self.committed_transactions = Some(total_tx);
    }
}

use crate::remote_node::{RemoteNode, MAX_BLOCKS_FETCH};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Maximum number of times we retry a single RPC.
const MAX_RETRIES_PER_RPC: usize = 10;
const MAX_RETRIES_REACHED_ERR: &str = "Exceeded maximum number of retries per RPC";

/// Stats measured while executing load testing on the node.
pub struct Stats {
    /// Block height at the beginning of the testing.
    pub from_height: Option<u64>,
    /// Timestamp at the beginning of the testing.
    pub from_timestamp: Option<Instant>,
    /// Block height at the end of the testing.
    pub to_height: Option<u64>,
    /// Timestamp at the end of the testing.
    pub to_timestamp: Option<Instant>,
    /// Counter for outgoing transactions.
    pub out_tx_counter: AtomicU64,
    /// Number of committed transactions.
    pub committed_transacionts: Option<u64>,
}

impl std::fmt::Display for Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        let from_height = self.from_height.unwrap();
        let to_height = self.to_height.unwrap();
        let blocks_passed = to_height - from_height + 1;
        let time_passed =
            self.to_timestamp.unwrap().duration_since(self.from_timestamp.unwrap()).as_secs();
        let bps = (blocks_passed as f64) / (time_passed as f64);
        let total_txs = self.committed_transacionts.unwrap();

        write!(f, "Start block:\t{}\n", from_height)?;
        write!(f, "End block:\t{}\n", to_height)?;
        write!(f, "Time passed:\t{} secs\n", time_passed)?;
        write!(f, "Blocks per second:\t{:.2}\n", bps)?;
        write!(f, "Transactions per second:\t{}\n", total_txs / time_passed)?;
        write!(
            f,
            "Outgoing transactions per second:\t{}\n",
            self.out_tx_counter.load(Ordering::SeqCst) / time_passed
        )?;
        write!(f, "Transactions per block:\t{}", total_txs / blocks_passed)?;
        Ok(())
    }
}

fn get_result<F, T>(f: F) -> T
where
    F: Fn() -> Result<T, Box<dyn std::error::Error>>,
{
    for i in 0..MAX_RETRIES_PER_RPC {
        match f() {
            Ok(r) => return r,
            Err(err) => {
                if i == MAX_RETRIES_PER_RPC - 1 {
                    panic!("{}: {}", MAX_RETRIES_REACHED_ERR, err);
                }
            }
        };
    }
    unreachable!()
}

impl Stats {
    pub fn new() -> Self {
        Self {
            from_height: None,
            from_timestamp: None,
            to_height: None,
            to_timestamp: None,
            out_tx_counter: AtomicU64::new(0),
            committed_transacionts: None,
        }
    }

    /// Count one outgoing transaction.
    pub fn inc_out_tx(&self) {
        self.out_tx_counter.fetch_add(1, Ordering::SeqCst);
    }

    /// Measure stats from this moment.
    pub fn measure_from(&mut self, node: &RemoteNode) {
        self.from_height = Some(get_result(|| node.get_current_height()));
        self.from_timestamp = Some(Instant::now());
    }

    /// Measure stats to this moment.
    pub fn measure_to(&mut self, node: &RemoteNode) {
        self.to_height = Some(get_result(|| node.get_current_height()));
        self.to_timestamp = Some(Instant::now());
    }

    /// Measures number of transactions that were committed.
    pub fn collect_transactions(&mut self, node: &RemoteNode) {
        let mut curr_height = self.from_height.unwrap();
        let mut total_tx = 0u64;
        loop {
            total_tx +=
                get_result(|| node.get_transactions(curr_height, curr_height + MAX_BLOCKS_FETCH));
            curr_height += MAX_BLOCKS_FETCH + 1;
            if curr_height > self.to_height.unwrap() {
                break;
            }
        }
        self.committed_transacionts = Some(total_tx);
    }
}

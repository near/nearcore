use std::sync::atomic::{AtomicU64, Ordering};

pub fn timestamp_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

const TGAS: u64 = 1024 * 1024 * 1024 * 1024;

pub struct ProgressReporter {
    pub cnt: AtomicU64,
    // Timestamp to make relative measurements of block processing speed (in ms)
    pub ts: AtomicU64,
    pub all: u64,
    pub skipped: AtomicU64,
    // Fields below get cleared after each print.
    pub empty_blocks: AtomicU64,
    pub non_empty_blocks: AtomicU64,
    // Total gas burned (in TGas)
    pub tgas_burned: AtomicU64,
}

impl ProgressReporter {
    pub fn inc_and_report_progress(&self, gas_burnt: u64) {
        let ProgressReporter { cnt, ts, all, skipped, empty_blocks, non_empty_blocks, tgas_burned } =
            self;
        if gas_burnt == 0 {
            empty_blocks.fetch_add(1, Ordering::Relaxed);
        } else {
            non_empty_blocks.fetch_add(1, Ordering::Relaxed);
            tgas_burned.fetch_add(gas_burnt / TGAS, Ordering::Relaxed);
        }

        const PRINT_PER: u64 = 100;
        let prev = cnt.fetch_add(1, Ordering::Relaxed);
        if (prev + 1) % PRINT_PER == 0 {
            let prev_ts = ts.load(Ordering::Relaxed);
            let new_ts = timestamp_ms();
            let per_second = (PRINT_PER as f64 / (new_ts - prev_ts) as f64) * 1000.0;
            ts.store(new_ts, Ordering::Relaxed);
            let secs_remaining = (all - prev) as f64 / per_second;
            let avg_gas = if non_empty_blocks.load(Ordering::Relaxed) == 0 {
                0.0
            } else {
                tgas_burned.load(Ordering::Relaxed) as f64
                    / non_empty_blocks.load(Ordering::Relaxed) as f64
            };

            println!(
                "Processed {} blocks, {:.4} blocks per second ({} skipped), {:.2} secs remaining {} empty blocks {:.2} avg gas per non-empty block",
                prev + 1,
                per_second,
                skipped.load(Ordering::Relaxed),
                secs_remaining,
                empty_blocks.load(Ordering::Relaxed),
                avg_gas,
            );
            empty_blocks.store(0, Ordering::Relaxed);
            non_empty_blocks.store(0, Ordering::Relaxed);
            tgas_burned.store(0, Ordering::Relaxed);
        }
    }
}

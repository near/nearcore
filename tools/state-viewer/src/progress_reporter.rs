use std::sync::atomic::{AtomicU64, Ordering};

pub fn timestamp_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

const TGAS: u64 = 1024 * 1024 * 1024 * 1024;

pub fn default_indicatif(len: Option<u64>) -> indicatif::ProgressBar {
    indicatif::ProgressBar::with_draw_target(
        len,
        indicatif::ProgressDrawTarget::stderr_with_hz(5)
    ).with_style(indicatif::ProgressStyle::with_template(
        "{prefix}{pos}/{len} blocks applied in {elapsed} at a rate of {per_sec}. {eta} remaining. {msg}"
    ).unwrap())
}

pub struct ProgressReporter {
    pub cnt: AtomicU64,
    pub skipped: AtomicU64,
    // Fields below get cleared after each print.
    pub empty_blocks: AtomicU64,
    pub non_empty_blocks: AtomicU64,
    // Total gas burned (in TGas)
    pub tgas_burned: AtomicU64,
    pub indicatif: indicatif::ProgressBar,
}

impl ProgressReporter {
    pub fn inc_and_report_progress(&self, block_height: u64, gas_burnt: u64) {
        let ProgressReporter {
            cnt,
            skipped,
            empty_blocks,
            non_empty_blocks,
            tgas_burned,
            indicatif,
        } = self;
        if gas_burnt == 0 {
            empty_blocks.fetch_add(1, Ordering::Relaxed);
        } else {
            non_empty_blocks.fetch_add(1, Ordering::Relaxed);
            tgas_burned.fetch_add(gas_burnt / TGAS, Ordering::Relaxed);
        }

        const PRINT_PER: u64 = 100;
        let prev = cnt.fetch_add(1, Ordering::Relaxed);
        let current = 1 + prev;
        indicatif.set_position(current);
        if current % PRINT_PER == 0 {
            let avg_gas = if non_empty_blocks.load(Ordering::Relaxed) == 0 {
                0.0
            } else {
                tgas_burned.load(Ordering::Relaxed) as f64
                    / non_empty_blocks.load(Ordering::Relaxed) as f64
            };

            indicatif.set_message(format!(
                "Skipped {skipped} blocks. \
                 Over last 100 blocks to height {block_height}: {empty} empty blocks, averaging {avg_gas:.2} Tgas per non-empty block",
                skipped = skipped.load(Ordering::Relaxed),
                empty = empty_blocks.load(Ordering::Relaxed),
            ));
            empty_blocks.store(0, Ordering::Relaxed);
            non_empty_blocks.store(0, Ordering::Relaxed);
            tgas_burned.store(0, Ordering::Relaxed);
        }
    }
}

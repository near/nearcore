use crate::cases::Metric;
use crate::stats::Measurements;
use crate::testbed::RuntimeTestbed;
use indicatif::{ProgressBar, ProgressStyle};
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{Action, SignedTransaction};
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

/// Get account id from its index.
pub fn get_account_id(account_index: usize) -> String {
    format!("near_{}_{}", account_index, account_index)
}

/// Total number of transactions that we need to prepare.
fn total_transactions(config: &Config) -> usize {
    config.block_sizes.iter().sum::<usize>() * config.iter_per_block
}

fn warmup_total_transactions(config: &Config) -> usize {
    config.block_sizes.iter().sum::<usize>() * config.warmup_iters_per_block
}

/// Configuration which we use to run measurements.
#[derive(Debug, Clone)]
pub struct Config {
    /// How many warm up iterations per block should we run.
    pub warmup_iters_per_block: usize,
    /// How many iterations per block are we going to try.
    pub iter_per_block: usize,
    /// Total active accounts.
    pub active_accounts: usize,
    /// Number of the transactions in the block.
    pub block_sizes: Vec<usize>,
    /// Where state dump is located in case we need to create a testbed.
    pub state_dump_path: String,
}

/// Measure the speed of transactions containing certain simple actions.
pub fn measure_actions(
    metric: Metric,
    measurements: &mut Measurements,
    config: &Config,
    testbed: Option<RuntimeTestbed>,
    actions: Vec<Action>,
    sender_is_receiver: bool,
    use_unique_accounts: bool,
) -> RuntimeTestbed {
    let mut nonces: HashMap<usize, u64> = HashMap::new();
    let mut accounts_used = HashSet::new();
    let mut f = || {
        let account_idx = loop {
            let x = rand::thread_rng().gen::<usize>() % config.active_accounts;
            if use_unique_accounts && accounts_used.contains(&x) {
                continue;
            }
            break x;
        };
        let other_account_idx = loop {
            if sender_is_receiver {
                break account_idx;
            }
            let x = rand::thread_rng().gen::<usize>() % config.active_accounts;
            if use_unique_accounts && accounts_used.contains(&x) || x == account_idx {
                continue;
            }
            break x;
        };
        accounts_used.insert(account_idx);
        accounts_used.insert(other_account_idx);
        let account_id = get_account_id(account_idx);
        let other_account_id = get_account_id(other_account_idx);

        let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);
        let nonce = *nonces.entry(account_idx).and_modify(|x| *x += 1).or_insert(1);

        SignedTransaction::from_actions(
            nonce as u64,
            account_id,
            other_account_id,
            &signer,
            actions.clone(),
            CryptoHash::default(),
        )
    };
    measure_transactions(metric, measurements, config, testbed, &mut f, false)
}

// TODO: super-ugly, can achieve the same via higher-level wrappers over POSIX read().
#[inline(always)]
pub unsafe fn syscall3(mut n: usize, a1: usize, a2: usize, a3: usize) -> usize {
    asm!("syscall"
         : "+{rax}"(n)
         : "{rdi}"(a1) "{rsi}"(a2) "{rdx}"(a3)
         : "rcx", "r11", "memory"
         : "volatile");
    n
}

const CATCH_BASE: usize = 0xcafebabe;

fn start_count_instructions() {
    let mut buf: i8 = 0;
    unsafe {
        syscall3(
            0, /* sys_read */
            CATCH_BASE,
            std::mem::transmute::<*mut i8, usize>(&mut buf),
            1,
        );
    }
}

fn end_count_instructions() -> u64 {
    let mut result: u64 = 0;
    unsafe {
        syscall3(
            0, /* sys_read */
            CATCH_BASE + 1,
            std::mem::transmute::<*mut u64, usize>(&mut result),
            8,
        );
    }
    result
}

/// Measure the speed of the transactions, given a transactions-generator function.
/// Returns testbed so that it can be reused.
pub fn measure_transactions<F>(
    metric: Metric,
    measurements: &mut Measurements,
    config: &Config,
    testbed: Option<RuntimeTestbed>,
    f: &mut F,
    allow_failures: bool,
) -> RuntimeTestbed
where
    F: FnMut() -> SignedTransaction,
{
    let mut testbed = match testbed {
        Some(x) => {
            println!("{:?}. Reusing testbed.", metric);
            x
        }
        None => {
            let path = PathBuf::from(config.state_dump_path.as_str());
            println!("{:?}. Preparing testbed. Loading state.", metric);
            RuntimeTestbed::from_state_dump(&path)
        }
    };

    let bar = ProgressBar::new(warmup_total_transactions(config) as _);
    bar.set_style(ProgressStyle::default_bar().template(
        "[elapsed {elapsed_precise} remaining {eta_precise}] Warm up {bar} {pos:>7}/{len:7} {msg}",
    ));
    for block_size in config.block_sizes.clone() {
        for _ in 0..config.warmup_iters_per_block {
            let block: Vec<_> = (0..block_size).map(|_| (*f)()).collect();
            testbed.process_block(&block, allow_failures);
            bar.inc(block_size as _);
            bar.set_message(format!("Block size: {}", block_size).as_str());
        }
    }
    bar.finish();

    let bar = ProgressBar::new(total_transactions(config) as _);
    bar.set_style(ProgressStyle::default_bar().template(
        "[elapsed {elapsed_precise} remaining {eta_precise}] Measuring {bar} {pos:>7}/{len:7} {msg}",
    ));
    node_runtime::EXT_COSTS_COUNTER.with(|f| {
        f.borrow_mut().clear();
    });
    for block_size in config.block_sizes.clone() {
        for _ in 0..config.iter_per_block {
            let block: Vec<_> = (0..block_size).map(|_| (*f)()).collect();
            start_count_instructions();
            testbed.process_block(&block, allow_failures);
            let end_count = end_count_instructions();
            measurements.record_measurement(metric.clone(), block_size, end_count);
            bar.inc(block_size as _);
            bar.set_message(format!("Block size: {}", block_size).as_str());
        }
    }
    testbed.process_blocks_until_no_receipts(allow_failures);
    bar.finish();
    measurements.print();
    testbed
}

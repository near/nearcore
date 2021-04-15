use crate::cases::Metric;
use crate::stats::Measurements;
use crate::testbed::RuntimeTestbed;
use indicatif::{ProgressBar, ProgressStyle};
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{Action, SignedTransaction};
use near_vm_runner::VMKind;
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::os::raw::c_void;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Get account id from its index.
pub fn get_account_id(account_index: usize) -> String {
    format!("near_{}_{}", account_index, account_index)
}

/// Total number of transactions that we need to prepare.
pub fn total_transactions(config: &Config) -> usize {
    config.block_sizes.iter().sum::<usize>() * config.iter_per_block
}

fn warmup_total_transactions(config: &Config) -> usize {
    config.block_sizes.iter().sum::<usize>() * config.warmup_iters_per_block
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GasMetric {
    // If we measure gas in number of executed instructions, must run under simulator.
    ICount,
    // If we measure gas in elapsed time.
    Time,
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
    pub state_dump_path: PathBuf,
    /// Metric used for counting.
    pub metric: GasMetric,
    /// VMKind used
    pub vm_kind: VMKind,
    /// Whether to measure ActionCreationConfig
    pub disable_measure_action_creation: bool,
    /// Whether to measure Transaction
    pub disable_measure_transaction: bool,
}

/// Measure the speed of transactions containing certain simple actions.
pub fn measure_actions(
    metric: Metric,
    measurements: &mut Measurements,
    config: &Config,
    testbed: Option<Arc<Mutex<RuntimeTestbed>>>,
    actions: Vec<Action>,
    sender_is_receiver: bool,
    use_unique_accounts: bool,
    nonces: &mut HashMap<usize, u64>,
) -> Arc<Mutex<RuntimeTestbed>> {
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

// We use several "magical" file descriptors to interact with the plugin in QEMU
// intercepting read syscall. Plugin counts instructions executed and amount of data transferred
// by IO operations. We "normalize" all those costs into instruction count.
const CATCH_BASE: u32 = 0xcafebabe;
const HYPERCALL_START_COUNTING: u32 = 0;
const HYPERCALL_STOP_AND_GET_INSTRUCTIONS_EXECUTED: u32 = 1;
const HYPERCALL_GET_BYTES_READ: u32 = 2;
const HYPERCALL_GET_BYTES_WRITTEN: u32 = 3;

// See runtime/runtime-params-estimator/emu-cost/README.md for the motivation of constant values.
const READ_BYTE_COST: u64 = 27;
const WRITE_BYTE_COST: u64 = 47;

fn hypercall(index: u32) -> u64 {
    let mut result: u64 = 0;
    unsafe {
        libc::read((CATCH_BASE + index) as i32, &mut result as *mut _ as *mut c_void, 8);
    }
    result
}

pub enum Consumed {
    Instant(Instant),
    None,
}

fn start_count_instructions() -> Consumed {
    hypercall(HYPERCALL_START_COUNTING);
    Consumed::None
}

fn end_count_instructions() -> u64 {
    const USE_IO_COSTS: bool = true;
    if USE_IO_COSTS {
        let result_insn = hypercall(HYPERCALL_STOP_AND_GET_INSTRUCTIONS_EXECUTED);
        let result_read = hypercall(HYPERCALL_GET_BYTES_READ);
        let result_written = hypercall(HYPERCALL_GET_BYTES_WRITTEN);

        result_insn + result_read * READ_BYTE_COST + result_written * WRITE_BYTE_COST
    } else {
        hypercall(HYPERCALL_STOP_AND_GET_INSTRUCTIONS_EXECUTED)
    }
}

fn start_count_time() -> Consumed {
    Consumed::Instant(Instant::now())
}

fn end_count_time(consumed: &Consumed) -> u64 {
    match *consumed {
        Consumed::Instant(instant) => instant.elapsed().as_nanos().try_into().unwrap(),
        Consumed::None => panic!("Must not be so"),
    }
}

pub fn start_count(metric: GasMetric) -> Consumed {
    return match metric {
        GasMetric::ICount => start_count_instructions(),
        GasMetric::Time => start_count_time(),
    };
}

pub fn end_count(metric: GasMetric, consumed: &Consumed) -> u64 {
    return match metric {
        GasMetric::ICount => end_count_instructions(),
        GasMetric::Time => end_count_time(consumed),
    };
}

/// Measure the speed of the transactions, given a transactions-generator function.
/// Returns testbed so that it can be reused.
pub fn measure_transactions<F>(
    metric: Metric,
    measurements: &mut Measurements,
    config: &Config,
    testbed: Option<Arc<Mutex<RuntimeTestbed>>>,
    f: &mut F,
    allow_failures: bool,
) -> Arc<Mutex<RuntimeTestbed>>
where
    F: FnMut() -> SignedTransaction,
{
    let testbed = match testbed.clone() {
        Some(x) => {
            println!("{:?}. Reusing testbed.", metric);
            x
        }
        None => {
            println!("{:?}. Preparing testbed. Loading state.", metric);
            Arc::new(Mutex::new(RuntimeTestbed::from_state_dump(&config.state_dump_path)))
        }
    };
    let testbed_clone = testbed.clone();

    if config.warmup_iters_per_block > 0 {
        let bar = ProgressBar::new(warmup_total_transactions(config) as _);
        bar.set_style(ProgressStyle::default_bar().template(
            "[elapsed {elapsed_precise} remaining {eta_precise}] Warm up {bar} {pos:>7}/{len:7} {msg}",
        ));
        for block_size in config.block_sizes.clone() {
            for _ in 0..config.warmup_iters_per_block {
                let block: Vec<_> = (0..block_size).map(|_| (*f)()).collect();
                let mut testbed_inner = testbed_clone.lock().unwrap();
                testbed_inner.process_block(&block, allow_failures);
                bar.inc(block_size as _);
                bar.set_message(format!("Block size: {}", block_size).as_str());
            }
        }
        let mut testbed_inner = testbed_clone.lock().unwrap();
        testbed_inner.process_blocks_until_no_receipts(allow_failures);
        bar.finish();
    }

    let bar = ProgressBar::new(total_transactions(config) as _);
    bar.set_style(ProgressStyle::default_bar().template(
        "[elapsed {elapsed_precise} remaining {eta_precise}] Measuring {bar} {pos:>7}/{len:7} {msg}",
    ));
    node_runtime::with_ext_cost_counter(|cc| cc.clear());
    for _ in 0..config.iter_per_block {
        for block_size in config.block_sizes.clone() {
            let block: Vec<_> = (0..block_size).map(|_| (*f)()).collect();
            let mut testbed_inner = testbed_clone.lock().unwrap();
            let start = start_count(config.metric);
            testbed_inner.process_block(&block, allow_failures);
            testbed_inner.process_blocks_until_no_receipts(allow_failures);
            let measured = end_count(config.metric, &start);
            measurements.record_measurement(metric.clone(), block_size, measured);
            bar.inc(block_size as _);
            bar.set_message(format!("Block size: {}", block_size).as_str());
        }
    }
    bar.finish();
    measurements.print();
    testbed
}

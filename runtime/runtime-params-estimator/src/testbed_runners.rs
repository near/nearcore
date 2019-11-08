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
use std::time::Instant;

/// Get account id from its index.
pub fn get_account_id(account_index: usize) -> String {
    format!("near_{}_{}", account_index, account_index)
}

/// Block sizes that we are going to try running with.
fn block_sizes(config: &Config) -> Vec<usize> {
    (config.smallest_block_size_pow2..=config.largest_block_size_pow2)
        .map(|x| 2usize.pow(x))
        .collect()
}

/// Total number of transactions that we need to prepare.
fn total_transactions(config: &Config) -> usize {
    block_sizes(config).iter().sum::<usize>() * config.iter_per_block
}

fn warmup_total_transactions(config: &Config) -> usize {
    block_sizes(config).iter().sum::<usize>() * config.warmup_iters_per_block
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
    /// Smallest size of the block expressed as power of 2.
    pub smallest_block_size_pow2: u32,
    /// Largest size of the block expressed as power of 2.
    pub largest_block_size_pow2: u32,
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
    for block_size in block_sizes(config) {
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
    let mut gas_per_transaction = 0;
    for block_size in block_sizes(config) {
        for _ in 0..config.iter_per_block {
            let block: Vec<_> = (0..block_size).map(|_| (*f)()).collect();
            let start_time = Instant::now();
            gas_per_transaction += testbed.process_block(&block, allow_failures);
            let end_time = Instant::now();
            measurements.record_measurement(metric.clone(), block_size, end_time - start_time);
            bar.inc(block_size as _);
            bar.set_message(format!("Block size: {}", block_size).as_str());
        }
    }
    //    gas_per_transaction /= block_sizes(config).into_iter().sum::<Gas>();
    testbed.process_blocks_until_no_receipts(allow_failures);
    bar.finish();
    measurements.print();
    testbed
}

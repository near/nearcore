use indicatif::{ProgressBar, ProgressIterator, ProgressStyle};
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{Action, FunctionCallAction, SignedTransaction, TransferAction};
use rand::Rng;
use runtime_params_estimator::stats::Measurements;
use runtime_params_estimator::RuntimeTestbed;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

/// How many warm up iterations per block should we run.
const WARMUP_ITERATIONS_PER_BLOCK: usize = 3;
/// How many iterations per block are we going to try.
const ITER_PER_BLOCK: usize = 10;
/// Total active accounts.
const ACTIVE_ACCOUNTS: usize = 10_000;

/// Get account id from its index.
fn get_account_id(account_index: usize) -> String {
    format!("near_{}_{}", account_index, account_index)
}

/// Block sizes that we are going to try running with.
fn block_sizes() -> Vec<usize> {
    (4..=11).map(|x| 2usize.pow(x)).collect()
}

/// Total number of transactions that we need to prepare.
fn total_transactions() -> usize {
    block_sizes().iter().sum::<usize>() * ITER_PER_BLOCK
}

fn warmup_total_transactions() -> usize {
    block_sizes().iter().sum::<usize>() * WARMUP_ITERATIONS_PER_BLOCK
}

fn measure_actions(name: &'static str, actions: Vec<Action>) -> Measurements {
    let path = PathBuf::from("/tmp/data");

    println!("Preparing testbed. Loading state.");
    let mut testbed = RuntimeTestbed::from_state_dump(&path);

    let mut nonces: HashMap<usize, u64> = HashMap::new();
    let transaction_indices: Vec<_> =
        (0..(total_transactions() + warmup_total_transactions())).collect();
    let transactions: Vec<_> = transaction_indices
        .iter()
        .progress_count(transaction_indices.len() as _)
        .map(|_| {
            let account_idx = rand::thread_rng().gen::<usize>() % ACTIVE_ACCOUNTS;
            let mut other_account_idx = rand::thread_rng().gen::<usize>() % (ACTIVE_ACCOUNTS - 1);
            if other_account_idx >= account_idx {
                other_account_idx += 1;
            }
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
        })
        .collect();

    let bar = ProgressBar::new(warmup_total_transactions() as _);
    bar.set_style(ProgressStyle::default_bar().template(
        "[elapsed {elapsed_precise} remaining {eta_precise}] Warm up {bar} {pos:>7}/{len:7} {msg}",
    ));
    let mut transactions = transactions.as_slice();
    for block_size in block_sizes() {
        for _ in 0..WARMUP_ITERATIONS_PER_BLOCK {
            let (curr, rem) = transactions.split_at(block_size);
            testbed.process_block(curr, false);
            transactions = rem;
            bar.inc(block_size as _);
            bar.set_message(format!("Block size: {}", block_size).as_str());
        }
    }
    bar.finish();

    // Block_size -> times per iteration.
    let mut measurements = Measurements::new(name);

    let bar = ProgressBar::new(total_transactions() as _);
    bar.set_style(ProgressStyle::default_bar().template(
        "[elapsed {elapsed_precise} remaining {eta_precise}] Measuring {bar} {pos:>7}/{len:7} {msg}",
    ));
    let mut prev_time = Instant::now();
    for block_size in block_sizes() {
        for _ in 0..ITER_PER_BLOCK {
            let (curr, rem) = transactions.split_at(block_size);
            testbed.process_block(curr, false);
            let curr_time = Instant::now();
            measurements.record_measurement(block_size, curr_time - prev_time);
            prev_time = curr_time;

            transactions = rem;
            bar.inc(block_size as _);
            bar.set_message(format!("Block size: {}", block_size).as_str());
        }
    }
    bar.finish();
    println!("Measurement results: {}", measurements.stats());
    measurements
}
fn main() {
    let empty = measure_actions("receipt only", vec![]);
    let transfer =
        measure_actions("transfer", vec![Action::Transfer(TransferAction { deposit: 1 })]);

    const KV_PER_CONTRACT: usize = 10;
    let mut arg = [0u8; std::mem::size_of::<u64>() * 2];
    arg[..std::mem::size_of::<u64>()].copy_from_slice(&KV_PER_CONTRACT.to_le_bytes());
    arg[std::mem::size_of::<u64>()..]
        .copy_from_slice(&(rand::thread_rng().gen::<u64>() % 1000000000).to_le_bytes());
    let function_call = Action::FunctionCall(FunctionCallAction {
        method_name: "benchmark_storage_8b".to_string(),
        args: (&arg).to_vec(),
        gas: 10_000_000,
        deposit: 0,
    });
    let function_call = measure_actions("function call", vec![function_call]);

    Measurements::plot(&[empty, transfer, function_call]);
}

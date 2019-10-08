use indicatif::{ProgressBar, ProgressIterator, ProgressStyle};
use near_crypto::{BlsPublicKey, InMemorySigner, KeyType};
use near_primitives::account::{AccessKey, AccessKeyPermission, FunctionCallPermission};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeleteKeyAction,
    FunctionCallAction, SignedTransaction, StakeAction, TransferAction,
};
use rand::Rng;
use runtime_params_estimator::stats::Measurements;
use runtime_params_estimator::RuntimeTestbed;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Instant;

/// How many warm up iterations per block should we run.
const WARMUP_ITERATIONS_PER_BLOCK: usize = 3;
/// How many iterations per block are we going to try.
const ITER_PER_BLOCK: usize = 10;
/// Total active accounts.
const ACTIVE_ACCOUNTS: usize = 100_000;

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

/// Measure the speed of transactions containing certain simple actions.
fn measure_actions(
    name: &'static str,
    actions: Vec<Action>,
    sender_is_receiver: bool,
    use_unique_accounts: bool,
) -> Measurements {
    let mut nonces: HashMap<usize, u64> = HashMap::new();
    let mut accounts_used = HashSet::new();
    let f = || {
        let account_idx = loop {
            let x = rand::thread_rng().gen::<usize>() % ACTIVE_ACCOUNTS;
            if use_unique_accounts && accounts_used.contains(&x) {
                continue;
            }
            break x;
        };
        let other_account_idx = loop {
            if sender_is_receiver {
                break account_idx;
            }
            let x = rand::thread_rng().gen::<usize>() % ACTIVE_ACCOUNTS;
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
    measure_transactions(name, f)
}

/// Measure the speed of the transactions, given a transactions-generator function.
fn measure_transactions<F>(name: &'static str, mut f: F) -> Measurements
where
    F: FnMut() -> SignedTransaction,
{
    let path = PathBuf::from("/tmp/data");

    println!("{}. Preparing testbed. Loading state.", name);
    let mut testbed = RuntimeTestbed::from_state_dump(&path);

    let transaction_indices: Vec<_> =
        (0..(total_transactions() + warmup_total_transactions())).collect();
    let transactions: Vec<_> = transaction_indices
        .iter()
        .progress_count(transaction_indices.len() as _)
        .map(|_| f())
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
    println!("{} measurement results: {}", name, measurements.stats());
    measurements
}

fn main() {
    // Measure the speed of processing empty receipts.
    let empty = measure_actions("receipt only", vec![], false, false);

    // Measure the speed of processing simple transfers.
    let transfer = measure_actions(
        "transfer",
        vec![Action::Transfer(TransferAction { deposit: 1 })],
        false,
        false,
    );

    // Measure the speed of processing function calls.
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
    let function_call = measure_actions("function call", vec![function_call], false, false);

    // Measure the speed of creating account.
    let mut nonces: HashMap<usize, u64> = HashMap::new();
    let f = || {
        let account_idx = rand::thread_rng().gen::<usize>() % ACTIVE_ACCOUNTS;
        let account_id = get_account_id(account_idx);
        let other_account_id = format!("random_account_{}", rand::thread_rng().gen::<usize>());
        let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);
        let nonce = *nonces.entry(account_idx).and_modify(|x| *x += 1).or_insert(1);
        SignedTransaction::from_actions(
            nonce as u64,
            account_id,
            other_account_id,
            &signer,
            vec![Action::CreateAccount(CreateAccountAction {})],
            CryptoHash::default(),
        )
    };
    let create_account = measure_transactions("create_account", f);

    // Measure the speed of deleting an account.
    let mut nonces: HashMap<usize, u64> = HashMap::new();
    let mut deleted_accounts = HashSet::new();
    let mut beneficiaries = HashSet::new();
    let f = || {
        let account_idx = loop {
            let x = rand::thread_rng().gen::<usize>() % ACTIVE_ACCOUNTS;
            if !deleted_accounts.contains(&x) && !beneficiaries.contains(&x) {
                break x;
            }
        };
        let beneficiary_idx = loop {
            let x = rand::thread_rng().gen::<usize>() % ACTIVE_ACCOUNTS;
            if !deleted_accounts.contains(&x) && x != account_idx {
                break x;
            }
        };
        deleted_accounts.insert(account_idx);
        beneficiaries.insert(beneficiary_idx);
        let account_id = get_account_id(account_idx);
        let beneficiary_id = get_account_id(beneficiary_idx);
        let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);
        let nonce = *nonces.entry(account_idx).and_modify(|x| *x += 1).or_insert(1);
        SignedTransaction::from_actions(
            nonce as u64,
            account_id.clone(),
            account_id.clone(),
            &signer,
            vec![Action::DeleteAccount(DeleteAccountAction { beneficiary_id })],
            CryptoHash::default(),
        )
    };
    let delete_account = measure_transactions("delete_account", f);

    // Measure the speed of adding a full access key.
    let add_access_key_full = measure_actions(
        "add access key full",
        vec![Action::AddKey(AddKeyAction {
            public_key: serde_json::from_str(
                "\"ed25519:DcA2MzgpJbrUATQLLceocVckhhAqrkingax4oJ9kZ847\"",
            )
            .unwrap(),
            access_key: AccessKey { nonce: 0, permission: AccessKeyPermission::FullAccess },
        })],
        true,
        true,
    );

    // Measure the speed of adding a function call access key.
    let add_access_key_function_call = measure_actions(
        "add access key full",
        vec![Action::AddKey(AddKeyAction {
            public_key: serde_json::from_str(
                "\"ed25519:DcA2MzgpJbrUATQLLceocVckhhAqrkingax4oJ9kZ847\"",
            )
            .unwrap(),
            access_key: AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: Some(100),
                    receiver_id: get_account_id(0),
                    method_names: vec![
                        "method1".to_string(),
                        "method2".to_string(),
                        "method3".to_string(),
                    ],
                }),
            },
        })],
        true,
        true,
    );

    // Measure the speed of deleting an access key.
    let mut nonces: HashMap<usize, u64> = HashMap::new();
    // Accounts with deleted access keys.
    let mut deleted_accounts = HashSet::new();
    let f = || {
        let account_idx = loop {
            let x = rand::thread_rng().gen::<usize>() % ACTIVE_ACCOUNTS;
            if !deleted_accounts.contains(&x) {
                break x;
            }
        };
        deleted_accounts.insert(account_idx);
        let account_id = get_account_id(account_idx);
        let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);
        let nonce = *nonces.entry(account_idx).and_modify(|x| *x += 1).or_insert(1);
        SignedTransaction::from_actions(
            nonce as u64,
            account_id.clone(),
            account_id.clone(),
            &signer,
            vec![Action::DeleteKey(DeleteKeyAction { public_key: signer.public_key.clone() })],
            CryptoHash::default(),
        )
    };
    let delete_access_key = measure_transactions("delete_access_key", f);

    // Measure the speed of staking.
    let stake = measure_actions(
        "stake",
        vec![Action::Stake(StakeAction { stake: 1, public_key: BlsPublicKey::empty() })],
        true,
        true,
    );

    Measurements::plot(&[
        empty,
        transfer,
        function_call,
        create_account,
        delete_account,
        add_access_key_full,
        add_access_key_function_call,
        delete_access_key,
        stake,
    ]);
}

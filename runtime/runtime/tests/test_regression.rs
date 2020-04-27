//! * Creates runtime with 100_000 accounts;
//! * Creates 1_000_000 random transactions between these accounts;
//! * Processes 10_000 transactions per block.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use indicatif::{ProgressBar, ProgressStyle};
use rand::seq::SliceRandom;
use rand::Rng;

use near_crypto::{InMemorySigner, KeyType};
use near_primitives::account::{AccessKey, Account};
use near_primitives::contract::ContractCode;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::state_record::StateRecord;
use near_primitives::transaction::{
    Action, ExecutionStatus, FunctionCallAction, SignedTransaction, TransferAction,
};
use near_primitives::types::StateChangeCause;
use near_store::test_utils::create_trie;
use near_store::{
    create_store, get_account, set_access_key, set_account, set_code, Trie, TrieUpdate,
};
use near_vm_logic::types::Balance;

pub mod runtime_group_tools;
use runtime_group_tools::StandaloneRuntime;

const DISPLAY_PROGRESS_BAR: bool = false;

// We are sending one transaction from each account, so the following should be true:
// NUM_ACCOUNTS >= BLOCK_SIZE * NUM_BLOCKS
const NUM_ACCOUNTS: usize = 100_000;
const BLOCK_SIZE: usize = 1000;
const NUM_BLOCKS: usize = 10;

// How many storage read/writes tiny contract will do.
const KV_PER_CONTRACT: usize = 10;

/// Initial balance used in tests.
pub const TESTING_INIT_BALANCE: u128 = 1_000_000_000 * NEAR_BASE;

/// One NEAR, divisible by 10^24.
pub const NEAR_BASE: u128 = 1_000_000_000_000_000_000_000_000;

const TESTING_INIT_STAKE: Balance = 50_000_000;

enum TransactionType {
    Transfer,
    ContractCall,
}

enum DataBaseType {
    Disk,
    InMemory,
}

fn get_account_id(account_index: usize) -> String {
    format!("near_{}", account_index)
}

fn template_test(transaction_type: TransactionType, db_type: DataBaseType, expected_avg_tps: u64) {
    // Create runtime with no records in the trie.
    let tmpdir = tempfile::Builder::new().prefix("storage").tempdir().unwrap();
    let trie = match db_type {
        DataBaseType::Disk => {
            let store = create_store(tmpdir.path().to_str().unwrap());
            Arc::new(Trie::new(store))
        }
        DataBaseType::InMemory => create_trie(),
    };
    let runtime_signer =
        InMemorySigner::from_seed(&get_account_id(0), KeyType::ED25519, &get_account_id(0));
    let mut runtime = StandaloneRuntime::new(runtime_signer.clone(), &[], trie);

    let mut rng = rand::thread_rng();
    let account_indices: Vec<_> = (0..NUM_ACCOUNTS).collect();
    let total_num_transactions = BLOCK_SIZE * NUM_BLOCKS;
    let sending_account_indices: HashSet<_> =
        account_indices.choose_multiple(&mut rng, total_num_transactions).collect();

    let mut transactions = Vec::with_capacity(total_num_transactions);

    // Add accounts in chunks of 1000 for memory efficiency reasons.
    const CHUNK_SIZE: usize = 1000;
    let chunked_accounts = account_indices.chunks(CHUNK_SIZE).collect::<Vec<_>>();
    let bar = ProgressBar::new(chunked_accounts.len() as _);
    bar.set_style(ProgressStyle::default_bar().template(
        "[elapsed {elapsed_precise} remaining {eta_precise}] Preparing {bar} {pos:>7}/{len:7}",
    ));
    let wasm_binary: &[u8] = include_bytes!("./tiny-contract-rs/res/tiny_contract_rs.wasm");
    let code_hash = hash(wasm_binary);
    for chunk in chunked_accounts {
        let mut state_update = TrieUpdate::new(runtime.trie.clone(), runtime.root);
        // Put state records directly into trie and save them separately to compute storage usage.
        let mut records = Vec::with_capacity(CHUNK_SIZE * 3);
        for account_index in chunk {
            let account_id = get_account_id(*account_index);
            let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);
            let account = Account {
                amount: TESTING_INIT_BALANCE,
                locked: TESTING_INIT_STAKE,
                code_hash,
                storage_usage: 0,
            };
            set_account(&mut state_update, account_id.clone(), &account);
            let account_record = StateRecord::Account { account_id: account_id.clone(), account };
            records.push(account_record);
            let access_key_record = StateRecord::AccessKey {
                account_id: account_id.clone(),
                public_key: signer.public_key.clone().into(),
                access_key: AccessKey::full_access().into(),
            };
            set_access_key(
                &mut state_update,
                account_id.clone(),
                signer.public_key.clone(),
                &AccessKey::full_access(),
            );
            records.push(access_key_record);
            let code = ContractCode::new(wasm_binary.to_vec());
            set_code(&mut state_update, account_id.clone(), &code);
            let contract_record = StateRecord::Contract {
                account_id: account_id.clone(),
                code: wasm_binary.to_vec(),
            };
            records.push(contract_record);

            // Check if this account sends transactions.
            if sending_account_indices.contains(&account_index) {
                let other_account_index = loop {
                    let choice = account_indices.choose(&mut rng).unwrap();
                    if choice != account_index {
                        break choice;
                    }
                };

                let action = match transaction_type {
                    TransactionType::Transfer => Action::Transfer(TransferAction { deposit: 1 }),
                    TransactionType::ContractCall => {
                        let mut arg = [0u8; std::mem::size_of::<u64>() * 2];
                        arg[..std::mem::size_of::<u64>()]
                            .copy_from_slice(&KV_PER_CONTRACT.to_le_bytes());
                        arg[std::mem::size_of::<u64>()..]
                            .copy_from_slice(&(rng.gen::<u64>() % 1000000000).to_le_bytes());
                        Action::FunctionCall(FunctionCallAction {
                            method_name: "benchmark_storage_8b".to_string(),
                            args: (&arg).to_vec(),
                            gas: 10u64.pow(18),
                            deposit: 0,
                        })
                    }
                };
                let transaction = SignedTransaction::from_actions(
                    1 as u64,
                    account_id.clone(),
                    get_account_id(*other_account_index).clone(),
                    &signer,
                    vec![action],
                    CryptoHash::default(),
                );
                transactions.push(transaction);
            }
        }
        // Compute storage usage and update accounts.
        for (account_id, storage_usage) in runtime.runtime.compute_storage_usage(&records) {
            let mut account = get_account(&state_update, &account_id)
                .expect("Genesis storage error")
                .expect("Account must exist");
            account.storage_usage = storage_usage;
            set_account(&mut state_update, account_id.clone(), &account);
        }
        state_update.commit(StateChangeCause::InitialState);
        let trie = state_update.trie.clone();
        let (store_update, root) = state_update
            .finalize()
            .expect("Genesis state update failed")
            .0
            .into(trie)
            .expect("Genesis state update failed");
        store_update.commit().unwrap();
        runtime.root = root;
        if DISPLAY_PROGRESS_BAR {
            bar.inc(1);
        }
    }
    bar.finish();
    transactions.shuffle(&mut rng);

    // Submit transactions one chunk at a time.
    let mut prev_receipts = vec![];
    let mut _successful_transactions = 0usize;
    let mut failed_transactions = 0usize;
    let chunks = transactions.chunks(BLOCK_SIZE);
    let bar = ProgressBar::new(chunks.len() as _);
    bar.set_style(ProgressStyle::default_bar().template(
        "[elapsed {elapsed_precise} remaining {eta_precise}] {bar} {pos:>7}/{len:7} {msg}",
    ));
    let mut processed_transactions = 0usize;
    let start_timer = Instant::now();
    let mut prev_block: Option<Instant> = None;
    let mut avg_tps = 0;
    for chunk in chunks {
        let (curr_receipts, transactions_logs) = runtime.process_block(&prev_receipts, &chunk);
        prev_receipts = curr_receipts;
        for tl in transactions_logs {
            match tl.outcome.status {
                ExecutionStatus::SuccessValue(_) | ExecutionStatus::SuccessReceiptId(_) => {
                    _successful_transactions += 1
                }
                ExecutionStatus::Failure(_) => failed_transactions += 1,
                _ => {}
            }
        }
        processed_transactions += chunk.len();
        let secs_elapsed = start_timer.elapsed().as_secs();
        avg_tps = if secs_elapsed > 0 { processed_transactions as u64 / secs_elapsed } else { 0 };

        if let Some(prev_block) = prev_block {
            bar.println(format!("{}ms per block", prev_block.elapsed().as_millis()));
        }
        prev_block = Some(Instant::now());
        if DISPLAY_PROGRESS_BAR {
            bar.inc(1);
        }
        bar.set_message(
            format!("avg tps: {} failed_transactions: {}", avg_tps, failed_transactions).as_str(),
        );
    }
    bar.finish();
    assert_eq!(
        failed_transactions, 0,
        "Expected no failed transactions, but got {}",
        failed_transactions
    );
    assert!(
        expected_avg_tps <= avg_tps,
        "Expected at least {} TPS but got {} TPS",
        expected_avg_tps,
        avg_tps
    );
}

#[test]
fn test_transfer_disk() {
    template_test(TransactionType::Transfer, DataBaseType::Disk, 100);
}

#[test]
fn test_transfer_memory() {
    template_test(TransactionType::Transfer, DataBaseType::InMemory, 100);
}

#[test]
fn test_contract_call_disk() {
    template_test(TransactionType::ContractCall, DataBaseType::Disk, 90);
}

#[test]
fn test_contract_call_memory() {
    template_test(TransactionType::ContractCall, DataBaseType::InMemory, 90);
}

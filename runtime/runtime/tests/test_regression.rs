//! * Creates runtime with 100_000 accounts;
//! * Creates 1_000_000 random transactions between these accounts;
//! * Processes 10_000 transactions per block.

pub mod runtime_group_tools;
use runtime_group_tools::StandaloneRuntime;

use indicatif::{ParallelProgressIterator, ProgressBar, ProgressStyle};
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::account::AccessKey;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::serialize::to_base64;
use near_primitives::transaction::{
    Action, FunctionCallAction, SignedTransaction, TransactionStatus, TransferAction,
};
use near_primitives::views::AccountView;
use near_store::test_utils::create_trie;
use near_store::{create_store, Trie};
use near_vm_logic::types::Balance;
use node_runtime::StateRecord;
use rand::seq::SliceRandom;
use rand::Rng;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::sync::Arc;
use std::time::Instant;
use tempdir::TempDir;

const NUM_ACCOUNTS: usize = 100_000;
const TRANSACTIONS_PER_ACCOUNT: usize = 100;
const TRANSACTIONS_PER_BLOCK: usize = 1_000;
// How many storage read/writes tiny contract will do.
const KV_PER_CONTRACT: usize = 10;

const TESTING_INIT_BALANCE: Balance = 1_000_000_000_000_000;
const TESTING_INIT_STAKE: Balance = 50_000_000;

enum TransactionType {
    Transfer,
    ContractCall,
}

enum DataBaseType {
    Disk,
    InMemory,
}

fn template_test(transaction_type: TransactionType, db_type: DataBaseType) {
    // Create signers.
    let signer_ids: Vec<_> = (0..NUM_ACCOUNTS).collect();
    let signers: Vec<_> = signer_ids
        .par_iter()
        .progress()
        .map(|i| {
            let account_id = format!("near_{}", i);
            InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id)
        })
        .collect();

    // Create state records for the signer.
    let wasm_binary: &[u8] = include_bytes!("./tiny-contract-rs/res/tiny_contract_rs.wasm");
    let wasm_binary_base64 = to_base64(wasm_binary);
    let code_hash = hash(wasm_binary);
    let state_records: Vec<Vec<StateRecord>> = signers
        .par_iter()
        .progress()
        .map(|signer| {
            let account_id = signer.account_id.clone();
            vec![
                StateRecord::Account {
                    account_id: account_id.to_string(),
                    account: AccountView {
                        amount: TESTING_INIT_BALANCE,
                        staked: TESTING_INIT_STAKE,
                        code_hash: code_hash.clone().into(),
                        storage_usage: 0,
                        storage_paid_at: 0,
                    },
                },
                StateRecord::AccessKey {
                    account_id: account_id.to_string(),
                    public_key: signer.public_key.into(),
                    access_key: AccessKey::full_access().into(),
                },
                StateRecord::Contract {
                    account_id: account_id.to_string(),
                    code: wasm_binary_base64.clone(),
                },
            ]
        })
        .collect();
    let state_records: Vec<_> = state_records.into_iter().flatten().collect();

    // Create runtime with all the records.
    let tmpdir = TempDir::new("storage").unwrap();
    let trie = match db_type {
        DataBaseType::Disk => {
            let store = create_store(tmpdir.path().to_str().unwrap());
            Arc::new(Trie::new(store))
        }
        DataBaseType::InMemory => create_trie(),
    };

    let mut runtime = StandaloneRuntime::new(signers[0].clone(), &state_records, trie);

    // Create transactions.
    let transactions: Vec<_> = signers
        .par_iter()
        .progress()
        .map(|signer| {
            let mut rng = rand::thread_rng();
            (0..TRANSACTIONS_PER_ACCOUNT)
                .map(|nonce| {
                    let other_signer = loop {
                        let choice = signers.choose(&mut rng).unwrap();
                        if choice.account_id != signer.account_id {
                            break choice;
                        }
                    };

                    let action = match &transaction_type {
                        TransactionType::ContractCall => {
                            let mut arg = [0u8; std::mem::size_of::<u64>() * 2];
                            arg[..std::mem::size_of::<u64>()]
                                .copy_from_slice(&KV_PER_CONTRACT.to_le_bytes());
                            arg[std::mem::size_of::<u64>()..]
                                .copy_from_slice(&(rng.gen::<u64>() % 1000000000).to_le_bytes());
                            Action::FunctionCall(FunctionCallAction {
                                method_name: "benchmark_storage_8b".to_string(),
                                args: (&arg).to_vec(),
                                gas: 10_000_000,
                                deposit: 0,
                            })
                        }
                        TransactionType::Transfer => {
                            Action::Transfer(TransferAction { deposit: 1 })
                        }
                    };
                    SignedTransaction::from_actions(
                        nonce as u64,
                        signer.account_id.clone(),
                        other_signer.account_id.clone(),
                        signer,
                        vec![action],
                        CryptoHash::default(),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect();
    let mut transactions: Vec<_> = transactions.into_iter().flatten().collect();
    transactions.shuffle(&mut rand::thread_rng());

    let mut prev_receipts = vec![];
    let mut successful_transactions = 0usize;
    let mut failed_transactions = 0usize;
    let chunks = transactions.chunks(TRANSACTIONS_PER_BLOCK);
    let bar = ProgressBar::new(chunks.len() as _);
    bar.set_style(ProgressStyle::default_bar().template(
        "[elapsed {elapsed_precise} remaining {eta_precise}] {bar} {pos:>7}/{len:7} {msg}",
    ));
    let mut processed_transactions = 0usize;
    let start_timer = Instant::now();
    for chunk in chunks {
        let (curr_receipts, transactions_logs) = runtime.process_block(&prev_receipts, &chunk);
        prev_receipts = curr_receipts;
        for tl in transactions_logs {
            match tl.result.status {
                TransactionStatus::Completed => successful_transactions += 1,
                TransactionStatus::Failed => failed_transactions += 1,
                _ => {}
            }
        }
        processed_transactions += chunk.len();
        let secs_elapsed = start_timer.elapsed().as_secs();
        let avg_tps =
            if secs_elapsed > 0 { processed_transactions as u64 / secs_elapsed } else { 0 };
        bar.inc(1);
        bar.set_message(format!("avg tps: {}", avg_tps).as_str());
    }
    bar.finish();
}

#[test]
fn test_transfer_disk() {
    template_test(TransactionType::Transfer, DataBaseType::Disk);
}

#[test]
fn test_transfer_memory() {
    template_test(TransactionType::Transfer, DataBaseType::InMemory);
}

#[test]
fn test_contract_call_disk() {
    template_test(TransactionType::ContractCall, DataBaseType::Disk);
}

#[test]
fn test_contract_call_memory() {
    template_test(TransactionType::ContractCall, DataBaseType::InMemory);
}

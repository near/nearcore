use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use bencher::{Bencher, benchmark_group, benchmark_main};
use serde_json::Value;

use client::{BlockProductionResult, Client};
use configs::{ChainSpec, ClientConfig};
use primitives::block_traits::SignedBlock;
use primitives::chain::ChainPayload;
use primitives::hash::CryptoHash;
use primitives::signer::{BlockSigner, InMemorySigner, TransactionSigner};
use primitives::transaction::{
    CreateAccountTransaction, DeployContractTransaction, FinalTransactionStatus,
    FunctionCallTransaction, SendMoneyTransaction, SignedTransaction, TransactionBody,
};

const TMP_DIR: &str = "./tmp_bench/";
const ALICE_ACC_ID: &str = "alice.near";
const BOB_ACC_ID: &str = "bob.near";
const CONTRACT_ID: &str = "contract.near";
const DEFAULT_BALANCE: u64 = 10_000_000;
const DEFAULT_STAKE: u64 = 1_000_000;

fn get_chain_spec() -> (ChainSpec, Arc<InMemorySigner>, Arc<InMemorySigner>) {
    let genesis_wasm = include_bytes!("../../../core/wasm/runtest/res/wasm_with_mem.wasm").to_vec();
    let alice_signer = InMemorySigner::from_seed(ALICE_ACC_ID, ALICE_ACC_ID);
    let bob_signer = InMemorySigner::from_seed(BOB_ACC_ID, BOB_ACC_ID);
    let spec = ChainSpec {
        accounts: vec![
            (
                ALICE_ACC_ID.to_string(),
                alice_signer.public_key().to_readable(),
                DEFAULT_BALANCE,
                DEFAULT_STAKE,
            ),
            (BOB_ACC_ID.to_string(), bob_signer.public_key().to_readable(), DEFAULT_BALANCE, DEFAULT_STAKE),
        ],
        initial_authorities: vec![(
            ALICE_ACC_ID.to_string(),
            alice_signer.public_key().to_readable(),
            alice_signer.bls_public_key().to_readable(),
            DEFAULT_STAKE,
        )],
        genesis_wasm,
        beacon_chain_epoch_length: 1,
        beacon_chain_num_seats_per_slot: 1,
        boot_nodes: vec![],
    };
    (spec, Arc::new(alice_signer), Arc::new(bob_signer))
}

fn get_client(test_name: &str) -> (Client, Arc<InMemorySigner>, Arc<InMemorySigner>) {
    let mut base_path = Path::new(TMP_DIR).to_owned();
    base_path.push(test_name);
    if base_path.exists() {
        std::fs::remove_dir_all(base_path.clone()).unwrap();
    }
    let mut cfg = ClientConfig::default();
    cfg.base_path = base_path;
    let (chain_spec, alice_signer, bob_signer) = get_chain_spec();
    cfg.chain_spec = chain_spec;
    cfg.log_level = log::LevelFilter::Off;
    (Client::new(&cfg), alice_signer, bob_signer)
}

/// Produces blocks by consuming batches of transactions. Runs until all receipts are processed.
fn produce_blocks(batches: &mut Vec<Vec<SignedTransaction>>, client: &mut Client) {
    let mut prev_receipt_blocks = vec![];
    let mut transactions;
    let mut next_block_idx = client.shard_client.chain.best_index() + 1;
    loop {
        if batches.is_empty() {
            if prev_receipt_blocks.is_empty() {
                // We ran out of the transactions and receipts to process.
                break;
            } else {
                // We ran out of the transactions but we still need to finish processing receipts.
                transactions = vec![];
            }
        } else {
            transactions = batches.remove(0);
        }
        let payload = ChainPayload::new(transactions, prev_receipt_blocks);
        if let BlockProductionResult::Success(_beacon_block, shard_block) =
            client.try_produce_block(next_block_idx, payload)
        {
            prev_receipt_blocks = client
                .shard_client
                .get_receipt_block(shard_block.index(), shard_block.shard_id())
                .map(|b| vec![b])
                .unwrap_or(vec![]);
        } else {
            panic!("Block production should always succeed");
        }

        next_block_idx += 1;
    }
}

/// Create transactions that would deploy the contract on the blockchain.
/// Returns transactions and the new nonce.
fn deploy_test_contract(
    deployer_signer: Arc<InMemorySigner>,
    mut next_nonce: u64,
) -> (SignedTransaction, SignedTransaction, u64) {
    // Create account for the contract.
    let contract_signer = Arc::new(InMemorySigner::from_seed(CONTRACT_ID, CONTRACT_ID));
    let t_create = CreateAccountTransaction {
        nonce: next_nonce,
        originator: ALICE_ACC_ID.to_string(),
        new_account_id: CONTRACT_ID.to_string(),
        amount: 10,
        public_key: contract_signer.public_key().0[..].to_vec(),
    };
    let t_create = TransactionBody::CreateAccount(t_create).sign(deployer_signer);
    next_nonce += 1;

    // Create contract deployment transaction.
    let wasm_binary: &[u8] = include_bytes!("../../../tests/hello.wasm");
    let t_deploy = DeployContractTransaction {
        nonce: next_nonce,
        contract_id: CONTRACT_ID.to_string(),
        wasm_byte_array: wasm_binary.to_vec(),
    };
    let t_deploy = TransactionBody::DeployContract(t_deploy).sign(contract_signer);
    next_nonce += 1;

    (t_create, t_deploy, next_nonce)
}

/// Create transaction that calls the contract.
fn call_contract(
    deployer_signer: Arc<InMemorySigner>,
    mut next_nonce: u64,
    method_name: &str,
    args: &str,
) -> (SignedTransaction, u64) {
    let t = FunctionCallTransaction {
        nonce: next_nonce,
        originator: ALICE_ACC_ID.to_string(),
        contract_id: CONTRACT_ID.to_string(),
        method_name: method_name.as_bytes().to_vec(),
        args: args.as_bytes().to_vec(),
        amount: 0,
    };
    let t = TransactionBody::FunctionCall(t).sign(deployer_signer);
    next_nonce += 1;
    (t, next_nonce)
}

/// Verifies that the status of all submitted transactions is `Complete`.
fn verify_transaction_statuses(hashes: &Vec<CryptoHash>, client: &mut Client) {
    for h in hashes {
        assert_eq!(
            client.shard_client.get_transaction_final_result(h).status,
            FinalTransactionStatus::Completed,
            "Transaction was not completed {:?}",
            client.shard_client.get_transaction_final_result(h)
        );
    }
}

/// Runs multiple money transactions per block.
fn money_transaction_blocks(bench: &mut Bencher) {
    let num_blocks = 10usize;
    let transactions_per_block = 100usize;
    let (mut client, alice_signer, bob_signer) = get_client("money_transaction_blocks");

    let mut batches = vec![];
    let mut alice_nonce = 1;
    let mut bob_nonce = 1;
    let mut hashes = vec![];
    for block_idx in 0..num_blocks {
        let mut batch = vec![];
        for _transaction_idx in 0..transactions_per_block {
            let (mut t, signer);
            if block_idx % 2 == 0 {
                t = SendMoneyTransaction {
                    nonce: alice_nonce,
                    originator: ALICE_ACC_ID.to_string(),
                    receiver: BOB_ACC_ID.to_string(),
                    amount: 1,
                };
                signer = alice_signer.clone();
                alice_nonce += 1;
            } else {
                t = SendMoneyTransaction {
                    nonce: bob_nonce,
                    originator: BOB_ACC_ID.to_string(),
                    receiver: ALICE_ACC_ID.to_string(),
                    amount: 1,
                };
                signer = bob_signer.clone();
                bob_nonce += 1;
            }
            let t = TransactionBody::SendMoney(t).sign(signer);
            hashes.push(t.body.get_hash());
            batch.push(t);
        }
        batches.push(batch);
    }
    bench.iter(|| {
        produce_blocks(&mut batches, &mut client);
    });
    verify_transaction_statuses(&hashes, &mut client);
}

/// Executes multiple contract calls that write a lot of storage per block.
fn heavy_storage_blocks(bench: &mut Bencher) {
    let method_name = "benchmark_storage";
    let args = "{\"n\":1000}";
    let num_blocks = 10usize;
    let transactions_per_block = 100usize;
    let (mut client, alice_signer, _) = get_client("heavy_storage_blocks");
    let mut batches = vec![];
    let mut next_nonce = 1;
    // Store the hashes of the transactions we are submitting.
    let mut hashes = vec![];

    // First run the client until the contract account is created.
    let (t_create, t_deploy, _next_nonce) = deploy_test_contract(alice_signer.clone(), next_nonce);
    next_nonce = _next_nonce;
    hashes.extend(vec![t_create.body.get_hash(), t_deploy.body.get_hash()]);
    produce_blocks(&mut vec![vec![t_create]], &mut client);
    // First run the client until the contract is deployed.
    produce_blocks(&mut vec![vec![t_deploy]], &mut client);

    for _block_idx in 0..num_blocks {
        let mut batch = vec![];
        for _transaction_idx in 0..transactions_per_block {
            let (t, _next_nonce) = call_contract(alice_signer.clone(), next_nonce, method_name, args);
            next_nonce = _next_nonce;
            hashes.push(t.body.get_hash());
            batch.push(t);
        }
        batches.push(batch);
    }

    bench.iter(|| {
        produce_blocks(&mut batches, &mut client);
    });

    verify_transaction_statuses(&hashes, &mut client);
}

/// Execute multiple contract calls that get and set variables multiple times per block.
fn set_get_values_blocks(bench: &mut Bencher) {
    let num_blocks = 10usize;
    let transactions_per_block = 100usize;
    let (mut client, alice_signer, _) = get_client("set_get_values_blocks");
    let mut batches = vec![];
    let mut next_nonce = 1;
    // Store the hashes of the transactions we are submitting.
    let mut hashes = vec![];
    // These are the values that we expect to be set in the storage.
    let mut expected_storage = HashMap::new();

    // First run the client until the contract account is created.
    let (t_create, t_deploy, _next_nonce) = deploy_test_contract(alice_signer.clone(), next_nonce);
    next_nonce = _next_nonce;
    hashes.extend(vec![t_create.body.get_hash(), t_deploy.body.get_hash()]);
    produce_blocks(&mut vec![vec![t_create]], &mut client);
    // First run the client until the contract is deployed.
    produce_blocks(&mut vec![vec![t_deploy]], &mut client);

    for block_idx in 0..num_blocks {
        let mut batch = vec![];
        for transaction_idx in 0..transactions_per_block {
            let key = block_idx * transactions_per_block + transaction_idx;
            let value = key * 10;
            expected_storage.insert(key, value);
            let (t, _next_nonce) = call_contract(
                alice_signer.clone(),
                next_nonce,
                "setKeyValue",
                format!("{{\"key\":\"{}\", \"value\":\"{}\"}}", key, value).as_str(),
            );
            next_nonce = _next_nonce;
            hashes.push(t.body.get_hash());
            batch.push(t);
        }
        batches.push(batch);
    }

    bench.iter(|| {
        produce_blocks(&mut batches, &mut client);

        // As a part of the benchmark get values from the storage and verify that they are correct.

        for (k, v) in expected_storage.iter() {
            let state_update = client.shard_client.get_state_update();
            let best_index = client.shard_client.chain.best_index();
            let mut logs = vec![];
            let res = client
                .shard_client
                .trie_viewer
                .call_function(
                    state_update,
                    best_index,
                    &CONTRACT_ID.to_string(),
                    &"getValueByKey".to_string(),
                    format!("{{\"key\":\"{}\"}}", k).as_bytes(),
                    &mut logs,
                )
                .unwrap();
            let res = std::str::from_utf8(&res).unwrap();
            let res: Value = serde_json::from_str(res).unwrap();
            assert_eq!(res["result"], format!("{}", v));
        }
    });

    verify_transaction_statuses(&hashes, &mut client);
}

benchmark_group!(benches, money_transaction_blocks, heavy_storage_blocks, set_get_values_blocks);
benchmark_main!(benches);

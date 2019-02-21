use bencher::{benchmark_group, benchmark_main, Bencher};

use client::{BlockProductionResult, ChainConsensusBlockBody, Client};
use configs::{ChainSpec, ClientConfig};
use primitives::aggregate_signature::{BlsPublicKey, BlsSecretKey};
use primitives::block_traits::SignedBlock;
use primitives::chain::{ChainPayload, ReceiptBlock};
use primitives::hash::CryptoHash;
use primitives::signature::{sign, SecretKey as SK, DEFAULT_SIGNATURE};
use primitives::test_utils::get_key_pair_from_seed;
use primitives::transaction::{
    CreateAccountTransaction, DeployContractTransaction, FinalTransactionStatus,
    FunctionCallTransaction, SendMoneyTransaction, SignedTransaction, TransactionBody,
};
use primitives::types::{MessageDataBody, SignedMessageData};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::Path;

const TMP_DIR: &str = "./tmp_bench/";
const ALICE_ACC_ID: &str = "alice.near";
const BOB_ACC_ID: &str = "bob.near";
const CONTRACT_ID: &str = "contract.near";
const DEFAULT_BALANCE: u64 = 10_000_000;
const DEFAULT_STAKE: u64 = 1_000_000;

fn get_bls_keys(seed: [u32; 4]) -> (BlsPublicKey, BlsSecretKey) {
    use rand::{SeedableRng, XorShiftRng};
    let mut rng = XorShiftRng::from_seed(seed);
    let secret_key = BlsSecretKey::generate_from_rng(&mut rng);
    let public_key = secret_key.get_public_key();
    (public_key, secret_key)
}

fn get_chain_spec() -> (ChainSpec, SK, SK) {
    let genesis_wasm = include_bytes!("../../../core/wasm/runtest/res/wasm_with_mem.wasm").to_vec();
    let (alice_pk_bls, _alice_sk_bls) = get_bls_keys([1, 1, 1, 1]);
    let (alice_pk, alice_sk) = get_key_pair_from_seed(ALICE_ACC_ID);
    let (bob_pk, bob_sk) = get_key_pair_from_seed(BOB_ACC_ID);
    let spec = ChainSpec {
        accounts: vec![
            (
                ALICE_ACC_ID.to_string(),
                alice_pk.clone().to_readable(),
                DEFAULT_BALANCE,
                DEFAULT_STAKE,
            ),
            (BOB_ACC_ID.to_string(), bob_pk.to_readable(), DEFAULT_BALANCE, DEFAULT_STAKE),
        ],
        initial_authorities: vec![(
            ALICE_ACC_ID.to_string(),
            alice_pk_bls.to_readable(),
            DEFAULT_STAKE,
        )],
        genesis_wasm,
        beacon_chain_epoch_length: 1,
        beacon_chain_num_seats_per_slot: 1,
        boot_nodes: vec![],
    };
    (spec, alice_sk, bob_sk)
}

fn get_client(test_name: &str) -> (Client, SK, SK) {
    let mut base_path = Path::new(TMP_DIR).to_owned();
    base_path.push(test_name);
    if base_path.exists() {
        std::fs::remove_dir_all(base_path.clone()).unwrap();
    }
    let mut cfg = ClientConfig::default();
    cfg.base_path = base_path;
    let (chain_spec, alice_sk, bob_sk) = get_chain_spec();
    cfg.chain_spec = chain_spec;
    cfg.log_level = log::LevelFilter::Off;
    (Client::new(&cfg), alice_sk, bob_sk)
}

/// Constructs consensus block from transactions and receipt blocks.
fn transaction_and_receipts_to_consensus(
    transactions: Vec<SignedTransaction>,
    receipts: Vec<ReceiptBlock>,
    beacon_block_index: u64,
) -> ChainConsensusBlockBody {
    ChainConsensusBlockBody {
        messages: vec![SignedMessageData {
            owner_sig: DEFAULT_SIGNATURE,
            hash: 0,
            body: MessageDataBody {
                owner_uid: 0,
                parents: HashSet::new(),
                epoch: 0,
                payload: ChainPayload { transactions, receipts },
                endorsements: vec![],
            },
            beacon_block_index,
        }],
        beacon_block_index,
    }
}

/// Produces blocks by consuming batches of transactions. Runs until all receipts are processed.
fn produce_blocks(batches: &mut Vec<Vec<SignedTransaction>>, client: &mut Client) {
    let mut prev_receipt_blocks = vec![];
    let mut transactions;
    let mut next_block_idx = client.shard_chain.chain.best_index() + 1;
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
        let consensus = transaction_and_receipts_to_consensus(
            transactions,
            prev_receipt_blocks,
            next_block_idx,
        );
        if let BlockProductionResult::Success(_beacon_block, shard_block) =
            client.try_produce_block(consensus)
        {
            prev_receipt_blocks = client
                .shard_chain
                .get_receipt_block(shard_block.index(), shard_block.shard_id())
                .map(|b| vec![b])
                .unwrap_or(vec![]);
        } else {
            panic!("Block production should always succeed");
        }

        next_block_idx += 1;
    }
}

fn sign_transaction(t: TransactionBody, sk: &SK) -> SignedTransaction {
    let hash = t.get_hash();
    let signature = sign(hash.as_ref(), sk);
    SignedTransaction::new(signature, t)
}

/// Create transactions that would deploy the contract on the blockchain.
/// Returns transactions and the new nonce.
fn deploy_test_contract(
    deployer_sk: &SK,
    mut next_nonce: u64,
) -> (SignedTransaction, SignedTransaction, u64) {
    // Create account for the contract.
    let (contract_pk, contract_sk) = get_key_pair_from_seed(CONTRACT_ID);
    let t_create = CreateAccountTransaction {
        nonce: next_nonce,
        originator: ALICE_ACC_ID.to_string(),
        new_account_id: CONTRACT_ID.to_string(),
        amount: 10,
        public_key: contract_pk.0[..].to_vec(),
    };
    let t_create = TransactionBody::CreateAccount(t_create);
    let t_create = sign_transaction(t_create, &deployer_sk);

    next_nonce += 1;

    // Create contract deployment transaction.
    let wasm_binary: &[u8] = include_bytes!("../../../tests/hello.wasm");
    let t_deploy = DeployContractTransaction {
        nonce: next_nonce,
        contract_id: CONTRACT_ID.to_string(),
        wasm_byte_array: wasm_binary.to_vec(),
    };
    let t_deploy = TransactionBody::DeployContract(t_deploy);
    let t_deploy = sign_transaction(t_deploy, &contract_sk);
    next_nonce += 1;

    (t_create, t_deploy, next_nonce)
}

/// Create transaction that calls the contract.
fn call_contract(
    deployer_sk: &SK,
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
    let t = TransactionBody::FunctionCall(t);
    let t = sign_transaction(t, &deployer_sk);
    next_nonce += 1;
    (t, next_nonce)
}

/// Verifies that the status of all submitted transactions is `Complete`.
fn verify_transaction_statuses(hashes: &Vec<CryptoHash>, client: &mut Client) {
    for h in hashes {
        assert_eq!(
            client.shard_chain.get_transaction_final_result(h).status,
            FinalTransactionStatus::Completed,
            "Transaction was not completed {:?}",
            client.shard_chain.get_transaction_final_result(h)
        );
    }
}

/// Runs multiple money transactions per block.
fn money_transaction_blocks(bench: &mut Bencher) {
    let num_blocks = 10usize;
    let transactions_per_block = 100usize;
    let (mut client, secret_key_alice, secret_key_bob) = get_client("money_transaction_blocks");

    let mut batches = vec![];
    let mut alice_nonce = 1;
    let mut bob_nonce = 1;
    let mut hashes = vec![];
    for block_idx in 0..num_blocks {
        let mut batch = vec![];
        for _transaction_idx in 0..transactions_per_block {
            let (mut t, sk);
            if block_idx % 2 == 0 {
                t = SendMoneyTransaction {
                    nonce: alice_nonce,
                    originator: ALICE_ACC_ID.to_string(),
                    receiver: BOB_ACC_ID.to_string(),
                    amount: 1,
                };
                sk = &secret_key_alice;
                alice_nonce += 1;
            } else {
                t = SendMoneyTransaction {
                    nonce: bob_nonce,
                    originator: BOB_ACC_ID.to_string(),
                    receiver: ALICE_ACC_ID.to_string(),
                    amount: 1,
                };
                sk = &secret_key_bob;
                bob_nonce += 1;
            }
            let t = TransactionBody::SendMoney(t);
            let t = sign_transaction(t, &sk);
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
    let (mut client, secret_key_alice, _) = get_client("heavy_storage_blocks");
    let mut batches = vec![];
    let mut next_nonce = 1;
    // Store the hashes of the transactions we are submitting.
    let mut hashes = vec![];

    // First run the client until the contract account is created.
    let (t_create, t_deploy, _next_nonce) = deploy_test_contract(&secret_key_alice, next_nonce);
    next_nonce = _next_nonce;
    hashes.extend(vec![t_create.body.get_hash(), t_deploy.body.get_hash()]);
    produce_blocks(&mut vec![vec![t_create]], &mut client);
    // First run the client until the contract is deployed.
    produce_blocks(&mut vec![vec![t_deploy]], &mut client);

    for _block_idx in 0..num_blocks {
        let mut batch = vec![];
        for _transaction_idx in 0..transactions_per_block {
            let (t, _next_nonce) = call_contract(&secret_key_alice, next_nonce, method_name, args);
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
    let (mut client, secret_key_alice, _) = get_client("set_get_values_blocks");
    let mut batches = vec![];
    let mut next_nonce = 1;
    // Store the hashes of the transactions we are submitting.
    let mut hashes = vec![];
    // These are the values that we expect to be set in the storage.
    let mut expected_storage = HashMap::new();

    // First run the client until the contract account is created.
    let (t_create, t_deploy, _next_nonce) = deploy_test_contract(&secret_key_alice, next_nonce);
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
                &secret_key_alice,
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
            let state_update = client.shard_chain.get_state_update();
            let best_index = client.shard_chain.chain.best_index();
            let res = client
                .shard_chain
                .trie_viewer
                .call_function(
                    state_update,
                    best_index,
                    &CONTRACT_ID.to_string(),
                    &"getValueByKey".to_string(),
                    format!("{{\"key\":\"{}\"}}", k).as_bytes(),
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

#[macro_use]
extern crate bencher;

use bencher::Bencher;

extern crate storage;

use std::sync::Arc;

use client::BlockProductionResult;
use client::ChainConsensusBlockBody;
use client::Client;
use configs::ChainSpec;
use configs::ClientConfig;
use primitives::aggregate_signature::BlsPublicKey;
use primitives::aggregate_signature::BlsSecretKey;
use primitives::beacon::SignedBeaconBlock;
use primitives::block_traits::SignedBlock;
use primitives::chain::ChainPayload;
use primitives::hash::CryptoHash;
use primitives::signature::sign;
use primitives::signature::SecretKey as SK;
use primitives::signature::DEFAULT_SIGNATURE;
use primitives::test_utils::get_key_pair_from_seed;
use primitives::transaction::SendMoneyTransaction;
use primitives::transaction::SignedTransaction;
use primitives::transaction::TransactionBody;
use primitives::types::MessageDataBody;
use primitives::types::SignedMessageData;
use std::collections::HashSet;
use std::io;
use std::io::Write;
use std::path::Path;
use std::sync::RwLock;
use storage::create_storage;
use storage::storages::GenericStorage;
use storage::BeaconChainStorage;
use storage::ShardChainStorage;

const TMP_DIR: &str = "./tmp_bench/";
const ALICE_ACC_ID: &str = "alice.near";
const BOB_ACC_ID: &str = "bob.near";

fn get_bls_keys(seed: [u32; 4]) -> (BlsPublicKey, BlsSecretKey) {
    use rand::{SeedableRng, XorShiftRng};
    let mut rng = XorShiftRng::from_seed(seed);
    let secret_key = BlsSecretKey::generate_from_rng(&mut rng);
    let public_key = secret_key.get_public_key();
    (public_key, secret_key)
}

fn get_chain_spec() -> (ChainSpec, SK, SK) {
    let genesis_wasm = include_bytes!("../../../core/wasm/runtest/res/wasm_with_mem.wasm").to_vec();
    let (alice_pk_bls, alice_sk_bls) = get_bls_keys([1, 1, 1, 1]);
    let (alice_pk, alice_sk) = get_key_pair_from_seed(ALICE_ACC_ID);
    let (bob_pk, bob_sk) = get_key_pair_from_seed(BOB_ACC_ID);
    let balance = 10000;
    let stake = 10;
    let spec = ChainSpec {
        accounts: vec![
            (ALICE_ACC_ID.to_string(), alice_pk.clone().to_readable(), balance, stake),
            (BOB_ACC_ID.to_string(), bob_pk.to_readable(), balance, stake),
        ],
        initial_authorities: vec![(ALICE_ACC_ID.to_string(), alice_pk_bls.to_readable(), stake)],
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
    println!("Looking at: {}", std::env::current_dir().unwrap().to_str().unwrap());
    if base_path.exists() {
        std::fs::remove_dir_all(base_path.clone()).unwrap();
    }
    let mut cfg = ClientConfig::default();
    cfg.base_path = base_path;
    let (chain_spec, alice_sk, bob_sk) = get_chain_spec();
    cfg.chain_spec = chain_spec;
    (Client::new(&cfg), alice_sk, bob_sk)
}

fn produce_blocks(bench: &mut Bencher, mut batches: Vec<Vec<SignedTransaction>>, client: Client) {
    bench.iter(move || {
        let mut prev_receipt_blocks = vec![];
        for (block_idx, batch) in batches.drain(..).enumerate() {
            let consensus = ChainConsensusBlockBody {
                messages: vec![SignedMessageData {
                    owner_sig: DEFAULT_SIGNATURE,
                    hash: 0,
                    body: MessageDataBody {
                        owner_uid: 0,
                        parents: HashSet::new(),
                        epoch: 0,
                        payload: ChainPayload {
                            transactions: batch,
                            receipts: prev_receipt_blocks,
                        },
                        endorsements: vec![],
                    },
                    beacon_block_index: block_idx as u64 + 2,
                }],
                beacon_block_index: block_idx as u64 + 2,
            };
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
        }
    });
}

fn produce_blocks_money(bench: &mut Bencher) {
    let num_blocks = 10usize;
    let transactions_per_block = 1000usize;
    let (client, secret_key_alice, secret_key_bob) = get_client("produce_blocks_money");

    let mut batches = vec![];
    let mut direction = true;
    let mut nonce = 0;
    for block_idx in 0..num_blocks {
        let mut batch = vec![];
        for transaction_idx in 0..transactions_per_block {
            let (t, sk) = if direction {
                (
                    SendMoneyTransaction {
                        nonce,
                        originator: ALICE_ACC_ID.to_string(),
                        receiver: BOB_ACC_ID.to_string(),
                        amount: 1,
                    },
                    &secret_key_alice,
                )
            } else {
                (
                    SendMoneyTransaction {
                        nonce,
                        originator: BOB_ACC_ID.to_string(),
                        receiver: ALICE_ACC_ID.to_string(),
                        amount: 1,
                    },
                    &secret_key_bob,
                )
            };
            let t = TransactionBody::SendMoney(t);
            let hash = t.get_hash();
            let signature = sign(hash.as_ref(), sk);
            batch.push(SignedTransaction::new(signature, t));
            direction = !direction;
            if !direction {
                nonce += 1;
            }
        }
        batches.push(batch);
    }
    produce_blocks(bench, batches, client);
}

benchmark_group!(benches, produce_blocks_money);
benchmark_main!(benches);

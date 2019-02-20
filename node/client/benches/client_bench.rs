#[macro_use]
extern crate bencher;

use bencher::Bencher;

extern crate storage;

use std::sync::Arc;

use client::BlockProductionResult;
use client::ChainConsensusBlockBody;
use client::Client;
use configs::ClientConfig;
use primitives::beacon::SignedBeaconBlock;
use primitives::chain::ChainPayload;
use primitives::hash::CryptoHash;
use primitives::signature::DEFAULT_SIGNATURE;
use primitives::transaction::SendMoneyTransaction;
use primitives::transaction::SignedTransaction;
use primitives::transaction::TransactionBody;
use primitives::types::MessageDataBody;
use primitives::types::SignedMessageData;
use std::collections::HashSet;
use std::path::Path;
use std::sync::RwLock;
use storage::create_storage;
use storage::storages::GenericStorage;
use storage::BeaconChainStorage;
use storage::ShardChainStorage;
use primitives::block_traits::SignedBlock;
use std::io;
use std::io::Write;
use node_runtime::test_utils::generate_test_chain_spec;
use primitives::signature::SecretKey;
use primitives::signature::sign;
use primitives::test_utils::get_key_pair_from_seed;

const TMP_DIR: &str = "./tmp_bench/";

fn get_client(test_name: &str) -> (Client, SecretKey, SecretKey) {
    let mut base_path = Path::new(TMP_DIR).to_owned();
    base_path.push(test_name);
    println!("Looking at: {}", std::env::current_dir().unwrap().to_str().unwrap());
    if base_path.exists() {
        std::fs::remove_dir_all(base_path.clone()).unwrap();
    }
    let mut cfg = ClientConfig::default();
    cfg.base_path = base_path;
    let (chain_spec, _, secret_key_alice)  = generate_test_chain_spec();
    cfg.chain_spec = chain_spec;
    let secret_key_bob = get_key_pair_from_seed("bob.near").1;
    (Client::new(&cfg), secret_key_alice, secret_key_bob)
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
                (SendMoneyTransaction {
                    nonce,
                    originator: "alice.near".to_string(),
                    receiver: "bob.near".to_string(),
                    amount: 1,
                },
                &secret_key_alice)

            } else {
                (SendMoneyTransaction {
                    nonce,
                    originator: "bob.near".to_string(),
                    receiver: "alice.near".to_string(),
                    amount: 1,
                },
                &secret_key_bob)
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

    println!("STARTING AT: {}", client.beacon_chain.chain.best_block().index());
    bench.iter(move || {
        let mut prev_receipt_blocks = vec![];
        for (block_idx, batch) in batches.drain(..).enumerate() {
            println!("BLOCK: {}", client.beacon_chain.chain.best_block().index());
            io::stdout().flush().expect("Could not flush stdout");
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
                    .map(|b| {
                        println!("RECEIPTS");
                        vec![b]
                    }).unwrap_or(vec![]);
            } else {
                panic!("Block production should always succeed");
            }
        }
    });
}


benchmark_group!(benches, produce_blocks_money);
benchmark_main!(benches);

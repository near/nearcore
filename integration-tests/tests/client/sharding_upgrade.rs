use std::collections::{HashMap, HashSet};

use crate::process_blocks::{create_nightshade_runtimes, set_block_protocol_version};
use near_chain::{ChainGenesis, Provenance};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_logger_utils::init_test_logger;
use near_primitives::epoch_manager::ShardConfig;
use near_primitives::shard_layout::{account_id_to_shard_uid, ShardLayout};
use near_primitives::transaction::SignedTransaction;
use near_primitives::version::ProtocolFeature;
use near_primitives::views::QueryRequest;
use near_store::test_utils::gen_accounts;
use nearcore::config::GenesisExt;
use nearcore::NEAR_BASE;

#[test]
fn test_shard_layout_upgrade() {
    init_test_logger();
    let epoch_length = 5;
    let mut rng = rand::thread_rng();
    let mut account_ids = vec!["test0".parse().unwrap(), "test1".parse().unwrap()];
    account_ids.extend(gen_accounts(&mut rng, 100).into_iter().collect::<HashSet<_>>());
    let mut genesis = Genesis::test(account_ids.clone(), 2);
    let simple_nightshade_protocol_version = ProtocolFeature::SimpleNightshade.protocol_version();
    genesis.config.epoch_length = epoch_length;
    genesis.config.protocol_version = simple_nightshade_protocol_version - 1;
    let new_num_shards = 4;
    let simple_nightshade_shard_layout = ShardLayout::v1(
        vec!["test0"].into_iter().map(|s| s.parse().unwrap()).collect(),
        vec!["abc", "foo"].into_iter().map(|s| s.parse().unwrap()).collect(),
        Some(vec![vec![0, 1, 2, 3]]),
        1,
    );
    genesis.config.simple_nightshade_shard_config = Some(ShardConfig {
        num_block_producer_seats_per_shard: vec![2; new_num_shards],
        avg_hidden_validator_seats_per_shard: vec![0; new_num_shards],
        shard_layout: simple_nightshade_shard_layout.clone(),
    });
    let chain_genesis = ChainGenesis::from(&genesis);
    let mut env = TestEnv::builder(chain_genesis)
        .clients_count(2)
        .validator_seats(2)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 2))
        .build();

    // ShardLayout changes at epoch 2
    // Test that state is caught up correctly at epoch 1 (block height 6-10)
    let mut txs = vec![];
    let mut added_accounts = vec![];
    let mut nonce = 100;
    for i in 1..=16 {
        // add some transactions right before state split
        if i == 4 || i == 10 {
            let new_accounts = gen_accounts(&mut rng, 100);
            let signer0 =
                InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
            for account_id in new_accounts.iter() {
                let signer = InMemorySigner::from_seed(
                    account_id.clone(),
                    KeyType::ED25519,
                    account_id.as_ref(),
                );
                let tx = SignedTransaction::create_account(
                    nonce,
                    "test0".parse().unwrap(),
                    account_id.clone(),
                    NEAR_BASE,
                    signer.public_key(),
                    &signer0,
                    env.clients[0].chain.genesis_block().hash().clone(),
                );
                nonce += 1;
                txs.push(tx.get_hash());
                added_accounts.push(account_id.clone());

                for j in 0..2 {
                    env.clients[j].process_tx(tx.clone(), false, false);
                }
            }
        }

        let head = env.clients[0].chain.head().unwrap();
        let epoch_id = env.clients[0]
            .runtime_adapter
            .get_epoch_id_from_prev_block(&head.last_block_hash)
            .unwrap();
        let shard_layout = env.clients[0].runtime_adapter.get_shard_layout(&epoch_id).unwrap();
        println!(
            "producing block {} {} last block {:?} for epoch id {:?} shards {}",
            i,
            head.height + 1,
            head.last_block_hash,
            epoch_id,
            shard_layout.num_shards()
        );

        // produce block
        let block_producer =
            env.clients[0].runtime_adapter.get_block_producer(&epoch_id, i).unwrap();
        let block_producer_client = env.client(&block_producer);
        let mut block = block_producer_client.produce_block(i).unwrap().unwrap();
        // upgrade to new protocol version but in the second epoch one node vote for the old version.
        if i != 10 {
            set_block_protocol_version(
                &mut block,
                block_producer.clone(),
                simple_nightshade_protocol_version,
            );
        }
        for j in 0..2 {
            env.process_block(j, block.clone(), Provenance::NONE);
        }

        env.process_partial_encoded_chunks();

        // after state split, check chunk extra exists and the states are correct
        if i >= 6 {
            let mut state_roots = HashMap::new();

            for account_id in &account_ids {
                let shard_uid =
                    account_id_to_shard_uid(account_id, &simple_nightshade_shard_layout);
                for j in 0..2 {
                    if env.clients[j].runtime_adapter.cares_about_shard(
                        None,
                        block.header().prev_hash(),
                        shard_uid.shard_id(),
                        true,
                    ) {
                        if !state_roots.contains_key(&shard_uid) {
                            let chunk_extra = env.clients[j]
                                .chain
                                .get_chunk_extra(block.hash(), &shard_uid)
                                .unwrap();
                            state_roots.insert(shard_uid.clone(), chunk_extra.state_root().clone());
                        }
                        env.clients[j]
                            .runtime_adapter
                            .query(
                                shard_uid,
                                &state_roots[&shard_uid],
                                block.header().height(),
                                0,
                                block.header().prev_hash(),
                                block.hash(),
                                block.header().epoch_id(),
                                &QueryRequest::ViewAccount { account_id: account_id.clone() },
                            )
                            .unwrap();
                    }
                }
            }
        }
    }

    let head = env.clients[0].chain.head().unwrap();
    let block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap().clone();
    for chunk in block.chunks().iter() {
        assert_eq!(block.header().height(), chunk.height_included());
    }
    for account_id in &account_ids {
        let shard_uid = account_id_to_shard_uid(account_id, &simple_nightshade_shard_layout);
        for j in 0..2 {
            if env.clients[j].runtime_adapter.cares_about_shard(
                None,
                block.header().prev_hash(),
                shard_uid.shard_id(),
                true,
            ) {
                env.clients[j]
                    .runtime_adapter
                    .query(
                        shard_uid,
                        &block
                            .chunks()
                            .get(shard_uid.shard_id() as usize)
                            .unwrap()
                            .prev_state_root(),
                        block.header().height(),
                        0,
                        block.header().prev_hash(),
                        block.hash(),
                        block.header().epoch_id(),
                        &QueryRequest::ViewAccount { account_id: account_id.clone() },
                    )
                    .unwrap();
            }
        }
    }

    // check execution outcomes
    for (i, id) in txs.iter().enumerate() {
        let account_id = &added_accounts[i];
        let shard_uid = account_id_to_shard_uid(account_id, &simple_nightshade_shard_layout);
        for j in 0..2 {
            if env.clients[j].runtime_adapter.cares_about_shard(
                None,
                block.header().prev_hash(),
                shard_uid.shard_id(),
                true,
            ) {
                let execution_outcome = env.clients[0].chain.get_execution_outcome(id).unwrap();
                assert_eq!(execution_outcome.outcome_with_id.outcome.receipt_ids.len(), 1);
            }
        }
    }
}

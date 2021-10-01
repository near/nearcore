use std::collections::HashSet;

use crate::process_blocks::{create_nightshade_runtimes, set_block_protocol_version};
use near_chain::{ChainGenesis, Provenance};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_logger_utils::init_test_logger;
use near_primitives::account::id::AccountId;
use near_primitives::block::Block;
use near_primitives::epoch_manager::ShardConfig;
use near_primitives::shard_layout::{account_id_to_shard_uid, ShardLayout};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::ProtocolVersion;
use near_primitives::version::ProtocolFeature;
use near_primitives::views::QueryRequest;
use near_store::test_utils::gen_accounts;
use nearcore::config::GenesisExt;
use nearcore::NEAR_BASE;

const SIMPLE_NIGHTSHADE_PROTOCOL_VERSION: ProtocolVersion =
    ProtocolFeature::SimpleNightshade.protocol_version();

// Checks that account exists in the state after `block` is processed
// This function checks both state_root from chunk extra and state root from chunk header, if
// the corresponding chunk is included in the block
fn check_account(env: &mut TestEnv, account_id: &AccountId, block: &Block) {
    let prev_hash = block.header().prev_hash();
    let shard_layout =
        env.clients[0].runtime_adapter.get_shard_layout_from_prev_block(prev_hash).unwrap();
    let shard_uid = account_id_to_shard_uid(account_id, &shard_layout);
    let shard_id = shard_uid.shard_id();
    for (i, me) in env.validators.iter().enumerate() {
        if env.clients[i].runtime_adapter.cares_about_shard(Some(me), prev_hash, shard_id, true) {
            let state_root = env.clients[i]
                .chain
                .get_chunk_extra(block.hash(), &shard_uid)
                .unwrap()
                .state_root()
                .clone();
            env.clients[i]
                .runtime_adapter
                .query(
                    shard_uid,
                    &state_root,
                    block.header().height(),
                    0,
                    prev_hash,
                    block.hash(),
                    block.header().epoch_id(),
                    &QueryRequest::ViewAccount { account_id: account_id.clone() },
                )
                .unwrap();

            let chunk = &block.chunks()[shard_id as usize];
            if chunk.height_included() == block.header().height() {
                env.clients[i]
                    .runtime_adapter
                    .query(
                        shard_uid,
                        &chunk.prev_state_root(),
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

fn setup_test_env(
    epoch_length: u64,
    num_clients: usize,
    num_validators: usize,
    initial_accounts: Vec<AccountId>,
) -> TestEnv {
    init_test_logger();
    let mut genesis = Genesis::test(initial_accounts, 2);
    // Set kickout threshold to 50 because chunks in the first block won't be produced (a known issue)
    // We don't want the validators get kicked out because of that
    genesis.config.chunk_producer_kickout_threshold = 50;
    genesis.config.epoch_length = epoch_length;
    genesis.config.protocol_version = SIMPLE_NIGHTSHADE_PROTOCOL_VERSION - 1;
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

    TestEnv::builder(chain_genesis)
        .clients_count(num_clients)
        .validator_seats(num_validators)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 2))
        .build()
}

/// Test shard layout upgrade. This function runs `env` to produce and process blocks
/// from 1 to 3 * epoch_length + 1, ie, to the beginning of epoch 3.
/// Epoch 0: 1 shard
/// Epoch 1: 1 shard, state split happens
/// Epoch 2: shard layout upgrades to simple_night_shade_shard,
/// `init_txs` are added before any block is produced
/// `txs_before_shard_split` are added before the last block of epoch 0, so they might be included
/// in the last block of epoch 0, before state split happens
/// `txs_before_shard_change` are added before the last block of epoch 1, so they might be included
/// in the last block of epoch 1, before sharding change happens
/// This functions checks
/// 1) all transactions are processed successfully
/// 2) all accounts in `initial_accounts_to_check` always exists in state at every step
/// 2) all accounts in `final_accounts_to_check` exist in the final state
fn test_shard_layout_upgrade_helper(
    env: &mut TestEnv,
    init_txs: Vec<SignedTransaction>,
    txs_before_shard_split: Vec<SignedTransaction>,
    txs_before_shard_change: Vec<SignedTransaction>,
    initial_accounts_to_check: &[AccountId],
    final_accounts_to_check: &[AccountId],
) {
    let num_validators = env.validators.len();
    let epoch_length = env.clients[0].config.epoch_length;
    for tx in init_txs.iter() {
        for j in 0..num_validators {
            env.clients[j].process_tx(tx.clone(), false, false);
        }
    }

    // ShardLayout changes at epoch 2
    // Test that state is caught up correctly at epoch 1 (block height epoch_length+1..=2*epoch_length)
    for i in 1..=3 * epoch_length + 1 {
        if i == epoch_length - 1 {
            for tx in txs_before_shard_split.iter() {
                for j in 0..num_validators {
                    env.clients[j].process_tx(tx.clone(), false, false);
                }
            }
        }

        if i == 2 * epoch_length - 1 {
            for tx in txs_before_shard_change.iter() {
                for j in 0..num_validators {
                    env.clients[j].process_tx(tx.clone(), false, false);
                }
            }
        }
        let head = env.clients[0].chain.head().unwrap();
        let epoch_id = env.clients[0]
            .runtime_adapter
            .get_epoch_id_from_prev_block(&head.last_block_hash)
            .unwrap();

        // produce block
        let block_producer =
            env.clients[0].runtime_adapter.get_block_producer(&epoch_id, i).unwrap();
        let block_producer_client = env.client(&block_producer);
        let mut block = block_producer_client.produce_block(i).unwrap().unwrap();
        set_block_protocol_version(
            &mut block,
            block_producer.clone(),
            SIMPLE_NIGHTSHADE_PROTOCOL_VERSION,
        );
        // process block, this also triggers chunk producers for the next block to produce chunks
        for j in 0..num_validators {
            env.process_block(j, block.clone(), Provenance::NONE);
        }

        env.process_partial_encoded_chunks();

        // after state split, check chunk extra exists and the states are correct
        if i >= epoch_length + 1 {
            for account_id in initial_accounts_to_check {
                check_account(env, account_id, &block);
            }
        }
    }

    // check final state has all accounts
    let head = env.clients[0].chain.head().unwrap();
    let block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap().clone();
    for chunk in block.chunks().iter() {
        assert_eq!(block.header().height(), chunk.height_included());
    }
    for account_id in final_accounts_to_check {
        check_account(env, account_id, &block)
    }

    // check execution outcomes
    let mut txs = init_txs;
    txs.extend(txs_before_shard_split);
    txs.extend(txs_before_shard_change);
    let shard_layout = env.clients[0]
        .runtime_adapter
        .get_shard_layout_from_prev_block(&head.last_block_hash)
        .unwrap();
    for tx in txs.iter() {
        let id = &tx.get_hash();
        let account_id = &tx.transaction.signer_id;
        let shard_uid = account_id_to_shard_uid(account_id, &shard_layout);
        for (i, account_id) in env.validators.iter().enumerate() {
            if env.clients[i].runtime_adapter.cares_about_shard(
                Some(account_id),
                block.header().prev_hash(),
                shard_uid.shard_id(),
                true,
            ) {
                let execution_outcome =
                    env.clients[i].chain.get_final_transaction_result(id).unwrap();
                assert!(execution_outcome.status.as_success().is_some());
            }
        }
    }
}

// test some shard layout upgrade with some simple transactions to create accounts
#[test]
fn test_shard_layout_upgrade_simple() {
    let mut rng = rand::thread_rng();
    let mut initial_accounts = vec!["test0".parse().unwrap(), "test1".parse().unwrap()];
    initial_accounts.extend(gen_accounts(&mut rng, 100).into_iter().collect::<HashSet<_>>());

    let mut env = setup_test_env(5, 2, 2, initial_accounts.clone());

    let mut nonce = 100;

    let genesis_hash = env.clients[0].chain.genesis_block().hash().clone();
    let mut all_accounts = initial_accounts.clone();
    let generate_create_accounts_txs: &mut dyn FnMut(usize) -> Vec<SignedTransaction> =
        &mut |max_size: usize| -> Vec<SignedTransaction> {
            let new_accounts = gen_accounts(&mut rng, max_size);
            let signer0 =
                InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
            let mut txs = vec![];
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
                    genesis_hash.clone(),
                );
                nonce += 1;
                txs.push(tx);
                all_accounts.push(account_id.clone());
            }
            txs
        };
    test_shard_layout_upgrade_helper(
        &mut env,
        vec![],
        generate_create_accounts_txs(100),
        generate_create_accounts_txs(100),
        &initial_accounts,
        &all_accounts,
    );
}

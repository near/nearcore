use near_async::messaging::CanSend;
use near_async::time::{FakeClock, Utc};
use near_chain::{Block, Provenance};
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_chunks::shards_manager_actor::CHUNK_REQUEST_SWITCH_TO_FULL_FETCH;

use near_chunks::test_utils::ShardsManagerResendChunkRequests;
use near_client::test_utils::TestEnv;
use near_client::ProcessTxResponse;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::Tip;

use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::EpochId;

use near_primitives_core::types::AccountId;

use near_store::test_utils::create_test_store;
use near_store::{ShardUId, TrieConfig};
use nearcore::test_utils::TestEnvNightshadeSetupExt;
use rand::seq::IteratorRandom;
use rand::{thread_rng, Rng};
use std::collections::{HashMap, HashSet};

const ONE_NEAR: u128 = 1_000_000_000_000_000_000_000_000;

#[test]
fn test_in_memory_trie_node_consistency() {
    // Recommended to run with RUST_LOG=memtrie=debug,chunks=error,info
    init_test_logger();
    let initial_balance = 10000 * ONE_NEAR;
    let accounts =
        (0..100).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let mut clock = FakeClock::new(Utc::UNIX_EPOCH);
    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&clock.clock())
        // Use the latest protocol version. Otherwise, the version may be too
        // old that e.g. blocks don't even store previous heights.
        .protocol_version_latest()
        // We'll test with 4 shards. This can be any number, but we want to test
        // the case when some shards are loaded into memory and others are not.
        // We pick the boundaries so that each shard would get some transactions.
        .shard_layout_simple_v1(&["account3", "account5", "account7"])
        // We're going to send NEAR between accounts and then assert at the end
        // that these transactions have been processed correctly, so here we set
        // the gas price to 0 so that we don't have to calculate gas cost.
        .gas_prices_free()
        // Set the block gas limit high enough so we don't have to worry about
        // transactions being throttled.
        .gas_limit_one_petagas()
        // Set the validity period high enough so even if a transaction gets
        // included a few blocks later it won't be rejected.
        .transaction_validity_period(100)
        // Make two validators. In this test we don't care about validators but
        // the TestEnv framework works best if all clients are validators. So
        // since we are using two clients, make two validators.
        .validators_desired_roles(&["account0", "account1"], &[])
        // We don't care about epoch transitions in this test, and epoch
        // transitions means validator selection, which can kick out validators
        // (due to our test purposefully skipping blocks to create forks), and
        // that's annoying to deal with. So set this to a high value to stay
        // within a single epoch.
        .epoch_length(10000);

    for account in &accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let genesis = genesis_builder.build();

    // Create two stores, one for each node. We'll be reusing the stores later
    // to emulate node restarts.
    let stores = vec![create_test_store(), create_test_store()];
    let mut env = TestEnv::builder(&genesis.config)
        .clock(clock.clock())
        .clients(vec!["account0".parse().unwrap(), "account1".parse().unwrap()])
        .stores(stores.clone())
        .track_all_shards()
        .nightshade_runtimes_with_trie_config(
            &genesis,
            vec![
                TrieConfig::default(), // client 0 does not load in-memory tries
                TrieConfig {
                    // client 1 loads two of four shards into in-memory tries
                    load_mem_tries_for_shards: vec![
                        ShardUId { version: 1, shard_id: 0 },
                        ShardUId { version: 1, shard_id: 2 },
                    ],
                    ..Default::default()
                },
            ],
        )
        .build();

    // Sanity check that we should have two block producers.
    assert_eq!(
        env.clients[0]
            .epoch_manager
            .get_epoch_block_producers_ordered(
                &EpochId::default(),
                &env.clients[0].chain.head().unwrap().last_block_hash
            )
            .unwrap()
            .len(),
        2
    );

    // First, start up the nodes from genesis. This ensures that in-memory
    // tries works correctly when starting up an empty node for the first time.
    let mut nonces =
        accounts.iter().map(|account| (account.clone(), 0)).collect::<HashMap<AccountId, u64>>();
    let mut balances = accounts
        .iter()
        .map(|account| (account.clone(), initial_balance))
        .collect::<HashMap<AccountId, u128>>();

    run_chain_for_some_blocks_while_sending_money_around(
        &mut clock,
        &mut env,
        &mut nonces,
        &mut balances,
        100,
        true,
    );
    // Sanity check that in-memory tries are loaded, and garbage collected properly.
    // We should have 4 roots for each loaded shard, because we maintain in-memory
    // roots until (and including) the prev block of the last final block. So if the
    // head is N, then we have roots for N, N - 1, N - 2 (final), and N - 3.
    assert_eq!(num_memtrie_roots(&env, 0, "s0.v1".parse().unwrap()), None);
    assert_eq!(num_memtrie_roots(&env, 0, "s1.v1".parse().unwrap()), None);
    assert_eq!(num_memtrie_roots(&env, 0, "s2.v1".parse().unwrap()), None);
    assert_eq!(num_memtrie_roots(&env, 0, "s3.v1".parse().unwrap()), None);
    assert_eq!(num_memtrie_roots(&env, 1, "s0.v1".parse().unwrap()), Some(4));
    assert_eq!(num_memtrie_roots(&env, 1, "s1.v1".parse().unwrap()), None);
    assert_eq!(num_memtrie_roots(&env, 1, "s2.v1".parse().unwrap()), Some(4));
    assert_eq!(num_memtrie_roots(&env, 1, "s3.v1".parse().unwrap()), None);

    // Restart nodes, and change some configs.
    drop(env);
    let mut env = TestEnv::builder(&genesis.config)
        .clock(clock.clock())
        .clients(vec!["account0".parse().unwrap(), "account1".parse().unwrap()])
        .stores(stores.clone())
        .track_all_shards()
        .nightshade_runtimes_with_trie_config(
            &genesis,
            vec![
                TrieConfig::default(),
                TrieConfig {
                    load_mem_tries_for_shards: vec![
                        ShardUId { version: 1, shard_id: 0 },
                        ShardUId { version: 1, shard_id: 1 }, // shard 2 changed to shard 1.
                    ],
                    ..Default::default()
                },
            ],
        )
        .build();
    run_chain_for_some_blocks_while_sending_money_around(
        &mut clock,
        &mut env,
        &mut nonces,
        &mut balances,
        100,
        true,
    );
    assert_eq!(num_memtrie_roots(&env, 0, "s0.v1".parse().unwrap()), None);
    assert_eq!(num_memtrie_roots(&env, 0, "s1.v1".parse().unwrap()), None);
    assert_eq!(num_memtrie_roots(&env, 0, "s2.v1".parse().unwrap()), None);
    assert_eq!(num_memtrie_roots(&env, 0, "s3.v1".parse().unwrap()), None);
    assert_eq!(num_memtrie_roots(&env, 1, "s0.v1".parse().unwrap()), Some(4));
    assert_eq!(num_memtrie_roots(&env, 1, "s1.v1".parse().unwrap()), Some(4));
    assert_eq!(num_memtrie_roots(&env, 1, "s2.v1".parse().unwrap()), None);
    assert_eq!(num_memtrie_roots(&env, 1, "s3.v1".parse().unwrap()), None);

    // Restart again, but this time flip the nodes.
    drop(env);
    let mut env = TestEnv::builder(&genesis.config)
        .clock(clock.clock())
        .clients(vec!["account0".parse().unwrap(), "account1".parse().unwrap()])
        .stores(stores)
        .track_all_shards()
        .nightshade_runtimes_with_trie_config(
            &genesis,
            vec![
                // client 0 now loads in-memory tries
                TrieConfig {
                    load_mem_tries_for_shards: vec![
                        ShardUId { version: 1, shard_id: 1 },
                        ShardUId { version: 1, shard_id: 3 },
                    ],
                    ..Default::default()
                },
                // client 1 no longer loads in-memory tries
                TrieConfig::default(),
            ],
        )
        .build();
    run_chain_for_some_blocks_while_sending_money_around(
        &mut clock,
        &mut env,
        &mut nonces,
        &mut balances,
        100,
        true,
    );
    assert_eq!(num_memtrie_roots(&env, 0, "s0.v1".parse().unwrap()), None);
    assert_eq!(num_memtrie_roots(&env, 0, "s1.v1".parse().unwrap()), Some(4));
    assert_eq!(num_memtrie_roots(&env, 0, "s2.v1".parse().unwrap()), None);
    assert_eq!(num_memtrie_roots(&env, 0, "s3.v1".parse().unwrap()), Some(4));
    assert_eq!(num_memtrie_roots(&env, 1, "s0.v1".parse().unwrap()), None);
    assert_eq!(num_memtrie_roots(&env, 1, "s1.v1".parse().unwrap()), None);
    assert_eq!(num_memtrie_roots(&env, 1, "s2.v1".parse().unwrap()), None);
    assert_eq!(num_memtrie_roots(&env, 1, "s3.v1".parse().unwrap()), None);
}

// Returns the block producer for the height of head + height_offset.
fn get_block_producer(env: &TestEnv, head: &Tip, height_offset: u64) -> AccountId {
    let client = &env.clients[0];
    let epoch_manager = &client.epoch_manager;
    let parent_hash = &head.last_block_hash;
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(parent_hash).unwrap();
    let height = head.height + height_offset;
    let block_producer = epoch_manager.get_block_producer(&epoch_id, height).unwrap();
    block_producer
}

fn check_block_does_not_have_missing_chunks(block: &Block) {
    for chunk in block.chunks().iter() {
        if !chunk.is_new_chunk(block.header().height()) {
            panic!(
                "Block at height {} is produced without all chunks; the test setup is faulty",
                block.header().height()
            );
        }
    }
}

/// Runs the chain for some number of blocks, sending money around randomly between
/// the test accounts, updating the corresponding nonces and balances. At the end,
/// check that the balances are correct, i.e. the transactions have been executed
/// correctly. If this runs successfully, it would also mean that the two nodes
/// being tested are consistent with each other. If, for example, there is a state
/// root mismatch issue, the two nodes would not be able to apply each others'
/// blocks because the block hashes would be different.
fn run_chain_for_some_blocks_while_sending_money_around(
    clock: &mut FakeClock,
    env: &mut TestEnv,
    nonces: &mut HashMap<AccountId, u64>,
    balances: &mut HashMap<AccountId, u128>,
    num_rounds: usize,
    track_all_shards: bool,
) {
    // Run the chain for some extra blocks, to ensure that all transactions are
    // included in the chain and are executed completely.
    let mut total_txs_included_in_chunks = 0;
    let mut num_chunks_not_found_in_all_clients = 0;
    for round in 0..(num_rounds + 10) {
        let heads = env
            .clients
            .iter()
            .map(|client| client.chain.head().unwrap().last_block_hash)
            .collect::<HashSet<_>>();
        assert_eq!(heads.len(), 1, "All clients should have the same head");
        let tip = env.clients[0].chain.head().unwrap();

        if round < num_rounds {
            // Make 50 random transactions that send money between random accounts.
            for _ in 0..50 {
                let sender = nonces.keys().choose(&mut thread_rng()).unwrap().clone();
                let receiver = nonces.keys().choose(&mut thread_rng()).unwrap().clone();
                let nonce = nonces.get_mut(&sender).unwrap();
                *nonce += 1;

                let txn = SignedTransaction::send_money(
                    *nonce,
                    sender.clone(),
                    receiver.clone(),
                    &create_user_test_signer(&sender).into(),
                    ONE_NEAR,
                    tip.last_block_hash,
                );
                // Process the txn in all shards, because they may not always
                // get a chance to produce the txn if they don't track the shard.
                for client in &mut env.clients {
                    match client.process_tx(txn.clone(), false, false) {
                        ProcessTxResponse::NoResponse => panic!("No response"),
                        ProcessTxResponse::InvalidTx(err) => panic!("Invalid tx: {}", err),
                        _ => {}
                    }
                }
                *balances.get_mut(&sender).unwrap() -= ONE_NEAR;
                *balances.get_mut(&receiver).unwrap() += ONE_NEAR;
            }
        }

        let cur_block_producer = get_block_producer(&env, &tip, 1);
        let next_block_producer = get_block_producer(&env, &tip, 2);
        println!("Producing block at height {} by {}", tip.height + 1, cur_block_producer);
        let block = env.client(&cur_block_producer).produce_block(tip.height + 1).unwrap().unwrap();
        if round > 0 {
            check_block_does_not_have_missing_chunks(&block);
        }

        // Let's produce some skip blocks too so that we test that in-memory tries are able to
        // deal with forks.
        // At the end, finish with a bunch of non-skip blocks so that we can test that in-memory
        // trie garbage collection works properly (final block is N - 2 so we should keep no more
        // than 3 roots).
        let mut skip_block = None;
        if cur_block_producer != next_block_producer
            && round < num_rounds
            && thread_rng().gen_bool(0.5)
        {
            println!(
                "Producing skip block at height {} by {}",
                tip.height + 2,
                next_block_producer
            );
            // Produce some skip blocks too so that we test that in-memory tries are able to deal
            // with forks.
            skip_block = Some(
                env.client(&next_block_producer).produce_block(tip.height + 2).unwrap().unwrap(),
            );
            if round > 0 {
                check_block_does_not_have_missing_chunks(&skip_block.as_ref().unwrap());
            }
        }

        let block_processed =
            if let Some(skip_block) = &skip_block { skip_block.clone() } else { block.clone() };
        // Apply height + 1 block.
        for i in 0..env.clients.len() {
            println!(
                "  Applying block at height {} at {}",
                block.header().height(),
                env.get_client_id(i)
            );
            let blocks_processed =
                env.clients[i].process_block_test(block.clone().into(), Provenance::NONE).unwrap();
            assert_eq!(blocks_processed, vec![*block.hash()]);
        }
        // Apply skip block if one was produced.
        if let Some(skip_block) = skip_block {
            for i in 0..env.clients.len() {
                println!(
                    "  Applying skip block at height {} at {}",
                    skip_block.header().height(),
                    env.get_client_id(i)
                );
                let blocks_processed = env.clients[i]
                    .process_block_test(skip_block.clone().into(), Provenance::NONE)
                    .unwrap();
                assert_eq!(blocks_processed, vec![*skip_block.hash()]);
            }
        }

        for chunk in block_processed.chunks().iter() {
            let mut chunks_found = 0;
            for i in 0..env.clients.len() {
                let client = &env.clients[i];
                if let Ok(chunk) = client.chain.get_chunk(&chunk.chunk_hash()) {
                    if chunks_found == 0 {
                        total_txs_included_in_chunks += chunk.transactions().len();
                    }
                    chunks_found += 1;
                }
            }
            if chunks_found == 0 {
                panic!("Chunk {:?} not found in any client", chunk.chunk_hash());
            }
            if chunks_found != env.clients.len() {
                num_chunks_not_found_in_all_clients += 1;
            }
        }

        // Send partial encoded chunks around so that the newly produced chunks
        // can be included and processed in the next block. Having to do this
        // sucks, because this test has nothing to do with partial encoded
        // chunks, but it is the unfortunate reality when using TestEnv with
        // multiple nodes.
        clock.advance(CHUNK_REQUEST_SWITCH_TO_FULL_FETCH);
        for i in 0..env.clients.len() {
            env.shards_manager_adapters[i].send(ShardsManagerResendChunkRequests);
        }
        env.process_partial_encoded_chunks();
        for j in 0..env.clients.len() {
            env.process_shards_manager_responses_and_finish_processing_blocks(j);
        }
        env.propagate_chunk_state_witnesses_and_endorsements(false);
    }

    assert_eq!(total_txs_included_in_chunks, 50 * num_rounds);
    if track_all_shards {
        assert_eq!(num_chunks_not_found_in_all_clients, 0);
    } else {
        assert!(num_chunks_not_found_in_all_clients > 0);
    }

    for (account, balance) in balances {
        assert_eq!(
            env.query_balance(account.clone()),
            *balance,
            "Balance mismatch for {}",
            account,
        );
    }
}

/// Returns the number of memtrie roots for the given client and shard, or
/// None if that shard does not load memtries.
fn num_memtrie_roots(env: &TestEnv, client_id: usize, shard: ShardUId) -> Option<usize> {
    Some(
        env.clients[client_id]
            .runtime_adapter
            .get_tries()
            .get_mem_tries(shard)?
            .read()
            .unwrap()
            .num_roots(),
    )
}

/// Base case for testing in-memory tries consistency with state sync.
/// This base case does not use in-memory tries. We leave this test here
/// nonetheless, because single-shard tracking setup is difficult to get
/// right.
fn test_in_memory_trie_consistency_with_state_sync_base_case(track_all_shards: bool) {
    // Recommended to run with RUST_LOG=memtrie=debug,chunks=error,info
    init_test_logger();
    let initial_balance = 10000 * ONE_NEAR;
    let accounts =
        (0..100).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    // We'll test with 4 shards. This can be any number, but we want to test
    // the case when some shards are loaded into memory and others are not.
    // We pick the boundaries so that each shard would get some transactions.
    const NUM_VALIDATORS_PER_SHARD: usize = 1;
    const NUM_VALIDATORS: usize = NUM_VALIDATORS_PER_SHARD * 4;

    let mut clock = FakeClock::new(Utc::UNIX_EPOCH);
    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&clock.clock())
        .genesis_height(10000)
        // Use the latest protocol version. Otherwise, the version may be too
        // old that e.g. blocks don't even store previous heights.
        .protocol_version_latest()
        // We'll test with 4 shards. This can be any number, but we want to test
        // the case when some shards are loaded into memory and others are not.
        // We pick the boundaries so that each shard would get some transactions.
        .shard_layout_simple_v1(&["account3", "account5", "account7"])
        // We're going to send NEAR between accounts and then assert at the end
        // that these transactions have been processed correctly, so here we set
        // the gas price to 0 so that we don't have to calculate gas cost.
        .gas_prices_free()
        // Set the block gas limit high enough so we don't have to worry about
        // transactions being throttled.
        .gas_limit_one_petagas()
        // Set the validity period high enough so even if a transaction gets
        // included a few blocks later it won't be rejected.
        .transaction_validity_period(1000)
        // Make NUM_VALIDATORS validators.
        .validators_desired_roles(
            &accounts[0..NUM_VALIDATORS].iter().map(|a| a.as_str()).collect::<Vec<_>>(),
            &[],
        )
        .minimum_validators_per_shard(NUM_VALIDATORS_PER_SHARD as u64)
        // Disable kickouts or else the short epoch length will kick out some validators.
        .kickouts_disabled()
        // Test epoch transitions.
        .epoch_length(10);

    for account in &accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let genesis = genesis_builder.build();

    let stores = (0..NUM_VALIDATORS).map(|_| create_test_store()).collect::<Vec<_>>();
    let mut env = TestEnv::builder(&genesis.config)
        .clock(clock.clock())
        .clients((0..NUM_VALIDATORS).map(|i| format!("account{}", i).parse().unwrap()).collect())
        .stores(stores)
        .maybe_track_all_shards(track_all_shards)
        .nightshade_runtimes_with_trie_config(
            &genesis,
            // Don't load any memtries.
            (0..NUM_VALIDATORS).map(|_| TrieConfig::default()).collect(),
        )
        .build();

    // Sanity check that we should have 4 block producers.
    assert_eq!(
        env.clients[0]
            .epoch_manager
            .get_epoch_block_producers_ordered(
                &EpochId::default(),
                &env.clients[0].chain.head().unwrap().last_block_hash
            )
            .unwrap()
            .len(),
        NUM_VALIDATORS
    );

    // Start the nodes from genesis, and then send transactions.
    let mut nonces =
        accounts.iter().map(|account| (account.clone(), 0)).collect::<HashMap<AccountId, u64>>();
    let mut balances = accounts
        .iter()
        .map(|account| (account.clone(), initial_balance))
        .collect::<HashMap<AccountId, u128>>();

    run_chain_for_some_blocks_while_sending_money_around(
        &mut clock,
        &mut env,
        &mut nonces,
        &mut balances,
        100,
        track_all_shards,
    );
    // Assert that indeed no memtries are loaded.
    for i in 0..NUM_VALIDATORS {
        for shard_id in 0..4 {
            assert_eq!(num_memtrie_roots(&env, i, ShardUId { version: 1, shard_id }), None);
        }
    }
}

#[test]
fn test_in_memory_trie_consistency_with_state_sync_base_case_track_single_shard() {
    test_in_memory_trie_consistency_with_state_sync_base_case(false);
}

#[test]
fn test_in_memory_trie_consistency_with_state_sync_base_case_track_all_shards() {
    test_in_memory_trie_consistency_with_state_sync_base_case(true);
}

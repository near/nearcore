use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use itertools::Itertools as _;
use near_async::messaging::{CanSend as _, Handler as _};
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestEpochConfigBuilder;
use near_client::{ProcessTxRequest, Query};
use near_network::types::NetworkRequests;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockReference, NumSeats, ShardId};
use near_primitives::views::{QueryRequest, QueryResponseKind};

use crate::setup::builder::TestLoopBuilder;
use crate::utils::ONE_NEAR;
use crate::utils::rotating_validators_runner::RotatingValidatorsRunner;
use crate::utils::transactions::get_anchor_hash;

/// Verifies that fetching of random parts works properly by issuing transactions during the
/// third epoch, and then making sure that the balances are correct for the next three epochs.
/// If random one parts fetched during the epoch preceding the epoch a block producer is
/// assigned to were to have incorrect receipts, the balances in the fourth epoch would have
/// been incorrect due to wrong receipts applied during the third epoch.
#[test]
fn ultra_slow_test_catchup_random_single_part_sync() {
    test_catchup_random_single_part_sync_common(RandomSinglePartTest {
        skip_24: false,
        change_balances: false,
        send_tx_height: 22,
    })
}

// Same test as `test_catchup_random_single_part_sync`, but skips the chunks on height 23 and 24
// It causes all the receipts to be applied only on height 25, which is the next epoch.
// It tests that the incoming receipts are property synced through epochs
#[test]
fn ultra_slow_test_catchup_random_single_part_sync_skip_24() {
    test_catchup_random_single_part_sync_common(RandomSinglePartTest {
        skip_24: true,
        change_balances: false,
        send_tx_height: 22,
    })
}

#[test]
fn ultra_slow_test_catchup_random_single_part_sync_send_24() {
    test_catchup_random_single_part_sync_common(RandomSinglePartTest {
        skip_24: false,
        change_balances: false,
        send_tx_height: 24,
    })
}

// Make sure that transactions are at least applied.
#[test]
fn ultra_slow_test_catchup_random_single_part_sync_non_zero_amounts() {
    test_catchup_random_single_part_sync_common(RandomSinglePartTest {
        skip_24: false,
        change_balances: true,
        send_tx_height: 22,
    })
}

// Use another height to send txs.
#[test]
fn ultra_slow_test_catchup_random_single_part_sync_height_9() {
    test_catchup_random_single_part_sync_common(RandomSinglePartTest {
        skip_24: false,
        change_balances: false,
        send_tx_height: 9,
    })
}

struct RandomSinglePartTest {
    skip_24: bool,
    change_balances: bool,
    send_tx_height: u64,
}

/// Allows testing tx application when validators are being changes over epochs. In particular useful for testing on epoch boundaries.
fn test_catchup_random_single_part_sync_common(
    RandomSinglePartTest { skip_24, change_balances, send_tx_height }: RandomSinglePartTest,
) {
    init_test_logger();

    let validators: Vec<Vec<AccountId>> = [
        vec!["test1.1", "test1.2", "test1.3", "test1.4"],
        vec!["test2.1", "test2.2", "test2.3", "test2.4"],
        vec![
            "test3.1", "test3.2", "test3.3", "test3.4", "test3.5", "test3.6", "test3.7", "test3.8",
        ],
    ]
    .iter()
    .map(|vec| vec.iter().map(|account| account.parse().unwrap()).collect())
    .collect();
    let seats: NumSeats = 8;

    let stake = ONE_NEAR;
    let mut runner = RotatingValidatorsRunner::new(stake, validators);

    let test_accounts: Vec<AccountId> =
        (0..16).map(|i| format!("test_account{i}").parse().unwrap()).collect_vec();

    let all_accounts =
        runner.all_validators_accounts().iter().chain(test_accounts.iter()).cloned().collect_vec();

    let num_shards = 4;
    let epoch_length = 8;
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(runner.genesis_validators_spec(seats, seats, seats))
        .add_user_accounts_simple(&all_accounts, stake)
        .shard_layout(ShardLayout::multi_shard(num_shards, 3))
        .build();

    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .minimum_validators_per_shard(2)
        .build_store_for_genesis_protocol_version();

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(runner.all_validators_accounts())
        .epoch_config_store(epoch_config_store)
        .build()
        .warmup();

    for node_datas in &env.node_datas {
        let peer_actor_handle = node_datas.peer_manager_sender.actor_handle();
        let peer_actor = env.test_loop.data.get_mut(&peer_actor_handle);
        peer_actor.register_override_handler(Box::new(move |request| -> Option<NetworkRequests> {
            if let NetworkRequests::PartialEncodedChunkMessage { partial_encoded_chunk, .. } =
                &request
            {
                if skip_24 {
                    if partial_encoded_chunk.header.height_created() == 23
                        || partial_encoded_chunk.header.height_created() == 24
                    {
                        return None;
                    }
                }
            }
            Some(request)
        }));
    }

    let client_actor_handle = &env.node_datas[0].client_sender.actor_handle();
    runner.run_until(
        &mut env,
        |test_loop_data| {
            let client = &test_loop_data.get(client_actor_handle).client;
            let head = client.chain.head().unwrap();
            head.height >= send_tx_height
        },
        Duration::seconds(send_tx_height as i64),
    );

    let clients = env
        .node_datas
        .iter()
        .map(|test_data| &env.test_loop.data.get(&test_data.client_sender.actor_handle()).client)
        .collect_vec();

    let mut tx_count = 0;
    let anchor_hash = get_anchor_hash(&clients);
    for (i, sender) in test_accounts.iter().enumerate() {
        for (j, receiver) in test_accounts.iter().enumerate() {
            let mut amount = (((i + j + 17) * 701) % 42 + 1) as u128;
            if change_balances {
                if i > j {
                    amount = 2;
                } else {
                    amount = 1;
                }
            }
            println!("VALUES {:?} {:?} {:?}", sender.to_string(), receiver.to_string(), amount);

            let tx = SignedTransaction::send_money(
                (12345 + tx_count) as u64,
                sender.clone(),
                receiver.clone(),
                &create_user_test_signer(&sender),
                amount,
                anchor_hash,
            );
            for node in &env.node_datas {
                node.rpc_handler_sender.send(ProcessTxRequest {
                    transaction: tx.clone(),
                    is_forwarded: false,
                    check_only: false,
                });
            }
            tx_count += 1;
        }
    }
    assert_eq!(tx_count, 16 * 16);

    runner.run_for_an_epoch(&mut env);
    runner.run_for_an_epoch(&mut env);

    let mut amounts = HashMap::new();

    assert_eq!(env.node_datas.len(), 16);
    let view_client_handlers =
        env.node_datas.iter().map(|data| data.view_client_sender.actor_handle()).collect_vec();

    let client = &env.test_loop.data.get(client_actor_handle).client;
    let head = client.chain.head().unwrap();
    let start_height = head.height;
    let target_height = epoch_length * 6 + 2;
    runner.run_until(
        &mut env,
        |test_loop_data| {
            // We want to make sure that amounts stay the same as epochs and validator sets are
            // changing over several epochs.
            for handler in &view_client_handlers {
                let view_client = test_loop_data.get_mut(handler);
                for test_account in &test_accounts {
                    let Ok(response) = view_client.handle(Query::new(
                        BlockReference::latest(),
                        QueryRequest::ViewAccount { account_id: test_account.clone() },
                    )) else {
                        continue;
                    };
                    let QueryResponseKind::ViewAccount(view_account_result) = response.kind else {
                        continue;
                    };
                    let amount = view_account_result.amount;
                    match amounts.entry(test_account.clone()) {
                        Entry::Occupied(entry) => {
                            assert_eq!(
                                *entry.get(),
                                amount,
                                "OCCUPIED {entry:?}; value != {amount}"
                            );
                        }
                        Entry::Vacant(entry) => {
                            println!("VACANT {:?}", entry);
                            if change_balances {
                                assert_ne!(amount % 100, 0);
                            } else {
                                assert_eq!(amount % 100, 0);
                            }
                            entry.insert(amount);
                        }
                    }
                }
            }
            let client = &test_loop_data.get(client_actor_handle).client;
            let head = client.chain.head().unwrap();
            head.height >= target_height
        },
        Duration::seconds((target_height - start_height) as i64),
    );

    for test_account in &test_accounts {
        match amounts.entry(test_account.clone()) {
            Entry::Occupied(_) => {
                continue;
            }
            Entry::Vacant(entry) => {
                println!("TEST ACCOUNT = {:?}, ENTRY = {:?}", test_account, entry);
                assert!(false);
            }
        }
    }

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

/// Makes sure that consecutive blocks are produced by 12 validators split into three epochs.
/// For extra coverage doesn't allow block propagation of some heights (and expects the blocks
/// to be skipped)
/// This test would fail if at any point validators got stuck with state sync, or block
/// production stalled for any other reason.
#[test]
fn slow_test_catchup_sanity_blocks_produced() {
    let validators: Vec<Vec<AccountId>> = [
        vec!["test1.1", "test1.2", "test1.3", "test1.4"],
        vec!["test2.1", "test2.2", "test2.3", "test2.4"],
        vec![
            "test3.1", "test3.2", "test3.3", "test3.4", "test3.5", "test3.6", "test3.7", "test3.8",
        ],
    ]
    .iter()
    .map(|vec| vec.iter().map(|account| account.parse().unwrap()).collect())
    .collect();
    let seats: NumSeats = 8;

    let stake = ONE_NEAR;
    let mut runner = RotatingValidatorsRunner::new(stake, validators);

    let accounts = runner.all_validators_accounts();
    let num_shards = 4;
    let epoch_length = 7;
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(runner.genesis_validators_spec(seats, seats, seats))
        .add_user_accounts_simple(&accounts, stake)
        .shard_layout(ShardLayout::multi_shard(num_shards, 3))
        .build();

    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .minimum_validators_per_shard(2)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(accounts)
        .epoch_config_store(epoch_config_store)
        .config_modifier(move |config, _| {
            let block_prod_time = Duration::milliseconds(2000);
            config.min_block_production_delay = block_prod_time;
            config.max_block_production_delay = 3 * block_prod_time;
            config.max_block_wait_delay = 3 * block_prod_time;
        })
        .build()
        .warmup();

    let heights = Rc::new(RefCell::new(HashMap::new()));
    for node_datas in &env.node_datas {
        let check_height = {
            let heights = heights.clone();
            move |hash: CryptoHash, height| match heights.borrow_mut().entry(hash) {
                Entry::Occupied(entry) => {
                    assert_eq!(*entry.get(), height);
                }
                Entry::Vacant(entry) => {
                    entry.insert(height);
                }
            }
        };

        let peer_actor_handle = node_datas.peer_manager_sender.actor_handle();
        let peer_actor = env.test_loop.data.get_mut(&peer_actor_handle);
        peer_actor.register_override_handler(Box::new(move |request| -> Option<NetworkRequests> {
            if let NetworkRequests::Block { block } = &request {
                check_height(*block.hash(), block.header().height());

                if block.header().height() % 10 == 5 {
                    check_height(*block.header().prev_hash(), block.header().height() - 2);
                } else {
                    check_height(*block.header().prev_hash(), block.header().height() - 1);
                }

                // Do not propagate blocks at %10=4
                if block.header().height() % 10 == 4 {
                    return None;
                }
            }
            Some(request)
        }));
    }

    let client_actor_handle = &env.node_datas[0].client_sender.actor_handle();
    runner.run_until(
        &mut env,
        |test_loop_data| {
            let client = &test_loop_data.get(client_actor_handle).client;
            let head = client.chain.head().unwrap();
            head.height >= epoch_length * 6
        },
        Duration::seconds(120),
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

#[test]
fn slow_test_all_chunks_accepted() {
    init_test_logger();

    // In case the test starts failing, first try increasing the epoch length.
    let epoch_length = 9;
    let last_height = epoch_length * 8 + 2;
    let validators: Vec<Vec<AccountId>> = [
        vec!["test1.1", "test1.2", "test1.3", "test1.4"],
        vec!["test2.1", "test2.2", "test2.3", "test2.4"],
        vec![
            "test3.1", "test3.2", "test3.3", "test3.4", "test3.5", "test3.6", "test3.7", "test3.8",
        ],
    ]
    .iter()
    .map(|vec| vec.iter().map(|account| account.parse().unwrap()).collect())
    .collect();
    let seats: NumSeats = 8;

    let stake = ONE_NEAR;
    let mut runner = RotatingValidatorsRunner::new(stake, validators);

    let accounts = runner.all_validators_accounts();
    let num_shards = 4;
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(runner.genesis_validators_spec(seats, seats, seats))
        .add_user_accounts_simple(&accounts, stake)
        .shard_layout(ShardLayout::multi_shard(num_shards, 3))
        .build();

    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(accounts)
        .epoch_config_store(epoch_config_store)
        .build()
        .warmup();

    let seen_chunk_same_sender = Rc::new(RefCell::new(HashSet::<(AccountId, u64, ShardId)>::new()));
    for node_datas in &env.node_datas {
        let seen_chunk_same_sender = seen_chunk_same_sender.clone();
        let peer_actor_handle = node_datas.peer_manager_sender.actor_handle();
        let peer_actor = env.test_loop.data.get_mut(&peer_actor_handle);
        peer_actor.register_override_handler(Box::new(move |msg| -> Option<NetworkRequests> {
            match msg {
                NetworkRequests::PartialEncodedChunkMessage {
                    ref account_id,
                    ref partial_encoded_chunk,
                } => {
                    let header = &partial_encoded_chunk.header;
                    if seen_chunk_same_sender.borrow().contains(&(
                        account_id.clone(),
                        header.height_created(),
                        header.shard_id(),
                    )) {
                        println!("=== SAME CHUNK AGAIN!");
                        assert!(false);
                    };
                    seen_chunk_same_sender.borrow_mut().insert((
                        account_id.clone(),
                        header.height_created(),
                        header.shard_id(),
                    ));
                }
                NetworkRequests::Block { ref block } => {
                    if block.header().chunks_included() != num_shards {
                        println!(
                            "BLOCK WITH {:?} CHUNKS, {:?}",
                            block.header().chunks_included(),
                            block
                        );
                        assert!(false);
                    }
                }
                _ => (),
            }
            Some(msg)
        }));
    }

    let client_actor_handle = &env.node_datas[0].client_sender.actor_handle();
    runner.run_until(
        &mut env,
        |test_loop_data| {
            let client = &test_loop_data.get(client_actor_handle).client;
            let head = client.chain.head().unwrap();
            head.height >= last_height
        },
        Duration::seconds(last_height as i64),
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

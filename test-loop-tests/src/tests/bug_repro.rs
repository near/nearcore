use std::cell::RefCell;
use std::cmp::max;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{
    create_account_ids, create_validators_spec, validators_spec_clients_with_rpc,
};
use itertools::Itertools as _;
use near_async::messaging::CanSend as _;
use near_async::time::Duration;
use near_chain::Block;
use near_chain_configs::TrackedShardsConfig;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::ProcessTxRequest;
use near_crypto::InMemorySigner;
use near_network::client::{BlockApproval, BlockResponse};
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_network::types::NetworkRequests;
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance};
use parking_lot::RwLock;
use rand::{Rng as _, thread_rng};

#[test]
fn slow_test_repro_1183() {
    init_test_logger();

    let seed: u64 = thread_rng().r#gen();
    println!("RNG seed: {seed}. If test fails use it to find the issue.");
    let rng: rand::rngs::StdRng = rand::SeedableRng::seed_from_u64(seed);
    let rng = Arc::new(RwLock::new(rng));

    let block_producers = ["test1", "test2", "test3", "test4"];
    let validators_spec = ValidatorsSpec::desired_roles(&block_producers, &[]);
    let num_shards = 4;
    let shard_layout = ShardLayout::multi_shard(num_shards, 3);
    let epoch_length = 5;

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .build();

    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .minimum_validators_per_shard(2)
        .build_store_for_genesis_protocol_version();

    let clients = block_producers.into_iter().map(|a| a.parse().unwrap()).collect_vec();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .build()
        .warmup();

    let last_block: Arc<RwLock<Option<Arc<Block>>>> = Arc::new(RwLock::new(None));
    let delayed_one_parts: Arc<RwLock<Vec<NetworkRequests>>> = Arc::new(RwLock::new(vec![]));

    for node in &env.node_datas {
        let node_datas = env.node_datas.clone();
        let peer_id = node.peer_id.clone();

        let last_block = last_block.clone();
        let delayed_one_parts = delayed_one_parts.clone();
        let clients = clients.clone();
        let rng = rng.clone();

        let peer_actor_handle = node.peer_manager_sender.actor_handle();
        let peer_actor = env.test_loop.data.get_mut(&peer_actor_handle);
        peer_actor.register_override_handler(Box::new(move |request| -> Option<NetworkRequests> {
            if let NetworkRequests::Block { block } = &request {
                let mut last_block = last_block.write();
                let mut delayed_one_parts = delayed_one_parts.write();

                if let Some(last_block) = last_block.clone() {
                    for node in &node_datas {
                        node.client_sender.send(
                            BlockResponse {
                                block: last_block.clone(),
                                peer_id: peer_id.clone(),
                                was_requested: false,
                            }
                            .span_wrap(),
                        )
                    }
                }
                for delayed_message in delayed_one_parts.iter() {
                    if let NetworkRequests::PartialEncodedChunkMessage {
                        account_id,
                        partial_encoded_chunk,
                        ..
                    } = delayed_message
                    {
                        for (i, name) in clients.iter().enumerate() {
                            if name == account_id {
                                node_datas[i].shards_manager_sender.send(
                                    ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(
                                        partial_encoded_chunk.clone().into(),
                                    ),
                                );
                            }
                        }
                    } else {
                        assert!(false);
                    }
                }

                let mut nonce_delta = 0;
                for from in &["test1", "test2", "test3", "test4"] {
                    for to in &["test1", "test2", "test3", "test4"] {
                        let (from, to): (AccountId, AccountId) =
                            (from.parse().unwrap(), to.parse().unwrap());
                        for node in &node_datas {
                            node.rpc_handler_sender.send(ProcessTxRequest {
                                transaction: SignedTransaction::send_money(
                                    block.header().height() * 16 + nonce_delta,
                                    from.clone(),
                                    to.clone(),
                                    &InMemorySigner::test_signer(&from),
                                    Balance::from_yoctonear(1),
                                    *block.header().prev_hash(),
                                ),
                                is_forwarded: false,
                                check_only: false,
                            });
                            nonce_delta += 1
                        }
                    }
                }

                *last_block = Some(block.clone());
                *delayed_one_parts = vec![];
                None
            } else if let NetworkRequests::PartialEncodedChunkMessage { .. } = &request {
                let mut rng = rng.write();
                if rng.gen_bool(0.5) {
                    Some(request)
                } else {
                    delayed_one_parts.write().push(request.clone());
                    None
                }
            } else {
                Some(request)
            }
        }));
    }

    let client_actor_handle = &env.node_datas[1].client_sender.actor_handle();
    env.test_loop.run_until(
        |test_loop_data| {
            let client = &test_loop_data.get(client_actor_handle).client;
            let head = client.chain.head().unwrap();
            head.height >= 25
        },
        Duration::seconds(60),
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_sync_from_archival_node() {
    init_test_logger();

    let block_producers = ["test1", "test2", "test3", "test4"];
    let validators_spec = ValidatorsSpec::desired_roles(&block_producers, &[]);
    let num_shards = 4;
    let shard_layout = ShardLayout::multi_shard(num_shards, 3);
    let epoch_length = 6;
    let block_prod_time = Duration::milliseconds(100);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);

    let clients = block_producers.into_iter().map(|a| a.parse().unwrap()).collect_vec();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .cold_storage_archival_clients([clients[0].clone()].into())
        .config_modifier(move |config, idx| {
            config.min_block_production_delay = block_prod_time;
            config.max_block_production_delay = 3 * block_prod_time;
            config.max_block_wait_delay = 3 * block_prod_time;
            // Archival node
            if idx == 0 {
                config.tracked_shards_config = TrackedShardsConfig::AllShards;
            }
        })
        .build()
        .warmup();

    let largest_height = Arc::new(RwLock::new(0));
    let blocks = Arc::new(RwLock::new(HashMap::new()));
    let block_counter = Arc::new(RwLock::new(0));

    let client_senders = env.node_datas.iter().map(|data| data.client_sender.clone()).collect_vec();

    for node in &env.node_datas {
        let client_senders = client_senders.clone();
        let largest_height = largest_height.clone();
        let blocks = blocks.clone();
        let block_counter = block_counter.clone();

        let peer_id = node.peer_id.clone();

        let peer_actor_handle = node.peer_manager_sender.actor_handle();
        let peer_actor = env.test_loop.data.get_mut(&peer_actor_handle);
        peer_actor.register_override_handler(Box::new(move |request| -> Option<NetworkRequests> {
            let mut block_counter = block_counter.write();

            if let NetworkRequests::Block { block } = &request {
                let mut largest_height = largest_height.write();
                *largest_height = max(block.header().height(), *largest_height);
            }
            if *largest_height.read() <= 30 {
                match &request {
                    NetworkRequests::Block { block } => {
                        for (i, sender) in client_senders.iter().enumerate() {
                            if i != 3 {
                                sender.send(
                                    BlockResponse {
                                        block: block.clone(),
                                        peer_id: peer_id.clone(),
                                        was_requested: false,
                                    }
                                    .span_wrap(),
                                )
                            }
                        }
                        if block.header().height() <= 10 {
                            blocks.write().insert(*block.hash(), block.clone());
                        }
                        None
                    }
                    NetworkRequests::Approval { approval_message } => {
                        for (i, sender) in client_senders.iter().enumerate() {
                            if i != 3 {
                                sender.send(
                                    BlockApproval(
                                        approval_message.approval.clone(),
                                        peer_id.clone(),
                                    )
                                    .span_wrap(),
                                )
                            }
                        }
                        None
                    }
                    _ => Some(request),
                }
            } else {
                if *block_counter > 10 {
                    panic!("incorrect rebroadcasting of blocks");
                }
                for (_, block) in blocks.write().drain() {
                    client_senders[3].send(
                        BlockResponse { block, peer_id: peer_id.clone(), was_requested: false }
                            .span_wrap(),
                    );
                }
                match &request {
                    NetworkRequests::Block { block } => {
                        if block.header().height() <= 10 {
                            *block_counter += 1;
                        }
                        Some(request)
                    }
                    _ => Some(request),
                }
            }
        }));
    }

    env.test_loop.run_until(|_| *largest_height.read() >= 50, Duration::seconds(20));

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

#[test]
fn slow_test_long_gap_between_blocks() {
    init_test_logger();

    let block_producers = ["test1", "test2"];
    let validators_spec = ValidatorsSpec::desired_roles(&block_producers, &[]);
    let num_shards = 2;
    let shard_layout = ShardLayout::multi_shard(num_shards, 3);
    let epoch_length = 1000;
    let target_height = 600;
    let block_prod_time = Duration::milliseconds(100);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);

    let clients = block_producers.into_iter().map(|a| a.parse().unwrap()).collect_vec();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .config_modifier(move |config, _| {
            config.min_block_production_delay = block_prod_time;
            config.max_block_production_delay = 3 * block_prod_time;
            config.max_block_wait_delay = 3 * block_prod_time;
        })
        .build()
        .warmup();

    for node in &env.node_datas {
        let peer_actor_handle = node.peer_manager_sender.actor_handle();
        let peer_actor = env.test_loop.data.get_mut(&peer_actor_handle);
        peer_actor.register_override_handler(Box::new(move |request| -> Option<NetworkRequests> {
            match &request {
                NetworkRequests::Approval { approval_message } => {
                    if approval_message.approval.target_height < target_height {
                        return None;
                    } else {
                        if approval_message.target == "test1" {
                            return Some(request);
                        } else {
                            return None;
                        }
                    }
                }
                _ => return Some(request),
            }
        }));
    }

    let client_actor_handle = &env.node_datas[1].client_sender.actor_handle();
    env.test_loop.run_until(
        |test_loop_data| {
            let client = &test_loop_data.get(client_actor_handle).client;
            let head = client.chain.final_head().unwrap();
            head.height > target_height
        },
        Duration::seconds(3 * 70),
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

/// 1 RPC node, 1 validator node, 1 shard
/// Submit the same transaction twice, it should get forwarded to the validator node twice.
/// Should work both when the RPC node has a `validator_signer` and when it doesn't.
/// Reproduces an issue where the RPC didn't forward retried transactions when
/// the `validator_signer` was set. (See https://github.com/near/nearcore/pull/14958)
#[test]
fn test_rpc_forwards_retried_transaction() {
    init_test_logger();

    let shard_layout = ShardLayout::single_shard();
    let user_accounts = create_account_ids(["account0"]);
    let initial_balance = Balance::from_near(1_000_000);
    let validators_spec = create_validators_spec(1, 0);
    let clients = validators_spec_clients_with_rpc(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&user_accounts, initial_balance)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();
    let rpc_data_idx = env.rpc_data_idx();

    // First test the case where `validator_signer` is set.
    assert!(env.rpc_node().client().validator_signer.get().is_some());

    // Record ForwardTx messages sent by the RPC node
    let forward_tx_requests = Rc::new(RefCell::new(Vec::new()));
    let forward_tx_requests_clone = forward_tx_requests.clone();
    env.node_datas[rpc_data_idx].register_override_handler(
        &mut env.test_loop.data,
        Box::new(move |nr| {
            match &nr {
                NetworkRequests::ForwardTx(account, transaction) => forward_tx_requests_clone
                    .borrow_mut()
                    .push((account.clone(), transaction.get_hash())),
                _ => {}
            }
            Some(nr)
        }),
    );

    // Submit tx1 twice
    let tx1 = SignedTransaction::send_money(
        1,
        user_accounts[0].clone(),
        user_accounts[0].clone(),
        &create_user_test_signer(&user_accounts[0]),
        Balance::from_near(1),
        env.rpc_node().head().last_block_hash,
    );
    env.node_datas[rpc_data_idx].rpc_handler_sender.send(ProcessTxRequest {
        transaction: tx1.clone(),
        is_forwarded: false,
        check_only: false,
    });
    env.node_datas[rpc_data_idx].rpc_handler_sender.send(ProcessTxRequest {
        transaction: tx1.clone(),
        is_forwarded: false,
        check_only: false,
    });

    // Run TestLoop to process the transaction requests
    env.rpc_runner().run_for_number_of_blocks(1);

    // There should be two ForwardTx(validator0, tx1) messages recorded.
    let validator_acc: AccountId = "validator0".parse().unwrap();
    assert_eq!(
        forward_tx_requests.borrow_mut().as_slice(),
        &[(validator_acc.clone(), tx1.get_hash()), (validator_acc.clone(), tx1.get_hash())]
    );
    forward_tx_requests.borrow_mut().clear();

    // Now set validator_signer to None.
    env.rpc_node().client().validator_signer.update(None);

    // Submit tx2 twice
    let tx2 = SignedTransaction::send_money(
        2,
        user_accounts[0].clone(),
        user_accounts[0].clone(),
        &create_user_test_signer(&user_accounts[0]),
        Balance::from_near(1),
        env.rpc_node().head().last_block_hash,
    );
    env.node_datas[rpc_data_idx].rpc_handler_sender.send(ProcessTxRequest {
        transaction: tx2.clone(),
        is_forwarded: false,
        check_only: false,
    });
    env.node_datas[rpc_data_idx].rpc_handler_sender.send(ProcessTxRequest {
        transaction: tx2.clone(),
        is_forwarded: false,
        check_only: false,
    });

    // Run TestLoop for a bit
    env.rpc_runner().run_for_number_of_blocks(1);

    // There should be two ForwardTx(validator0, tx2) messages recorded.
    assert_eq!(
        forward_tx_requests.borrow_mut().as_slice(),
        &[(validator_acc.clone(), tx2.get_hash()), (validator_acc, tx2.get_hash())]
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

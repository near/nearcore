use std::cmp::max;
use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools as _;
use near_async::messaging::CanSend as _;
use near_async::time::Duration;
use near_chain::Block;
use near_chain_configs::TrackedShardsConfig;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::{BlockApproval, BlockResponse, ProcessTxRequest};
use near_crypto::InMemorySigner;
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_network::types::NetworkRequests;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use parking_lot::RwLock;
use rand::{Rng as _, thread_rng};

use crate::setup::builder::TestLoopBuilder;

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

    let last_block: Arc<RwLock<Option<Block>>> = Arc::new(RwLock::new(None));
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
                        node.client_sender.send(BlockResponse {
                            block: last_block.clone(),
                            peer_id: peer_id.clone(),
                            was_requested: false,
                        })
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
                                    1,
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
fn slow_test_sync_from_archival_node() {
    init_test_logger();

    let block_producers = ["test1", "test2", "test3", "test4"];
    let validators_spec = ValidatorsSpec::desired_roles(&block_producers, &[]);
    let num_shards = 4;
    let shard_layout = ShardLayout::multi_shard(num_shards, 3);
    let epoch_length = 4;
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
        .archival_clients([clients[0].clone()].into())
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
                                sender.send(BlockResponse {
                                    block: block.clone(),
                                    peer_id: peer_id.clone(),
                                    was_requested: false,
                                })
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
                                sender.send(BlockApproval(
                                    approval_message.approval.clone(),
                                    peer_id.clone(),
                                ))
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
                    client_senders[3].send(BlockResponse {
                        block,
                        peer_id: peer_id.clone(),
                        was_requested: false,
                    });
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

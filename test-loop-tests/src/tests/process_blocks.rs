use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools as _;
use near_async::messaging::CanSend as _;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::BlockResponse;
use near_crypto::{KeyType, PublicKey};
use near_network::types::{NetworkRequests, ReasonForBan};
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::hash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::validator_stake::ValidatorStake;
use parking_lot::RwLock;

use crate::setup::builder::TestLoopBuilder;

#[derive(Clone)]
enum InvalidBlockMode {
    /// Header is invalid
    InvalidHeader,
    /// Block is ill-formed (roots check fail)
    IllFormed,
    /// Block is invalid for other reasons
    InvalidBlock,
}

fn ban_peer_for_invalid_block_common(mode: InvalidBlockMode) {
    init_test_logger();

    let block_producers = ["test1", "test2", "test3", "test4"];
    let validators_spec = ValidatorsSpec::desired_roles(&block_producers, &[]);
    let shard_layout = ShardLayout::single_shard();
    let epoch_length = 100;

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
        .build()
        .warmup();

    let client_actor_handle = &env.node_datas[0].client_sender.actor_handle();
    let client = &env.test_loop.data.get(&client_actor_handle).client;
    let epoch_manager = client.epoch_manager.clone();

    let ban_counter: Arc<RwLock<usize>> = Arc::new(RwLock::new(0));
    let bad_block_send_height = 8;
    for node in &env.node_datas {
        let ban_counter = ban_counter.clone();
        let epoch_manager = epoch_manager.clone();
        let mode = mode.clone();

        let peer_actor_handle = node.peer_manager_sender.actor_handle();
        let peer_actor = env.test_loop.data.get_mut(&peer_actor_handle);
        peer_actor.register_override_handler(Box::new(move |request| -> Option<NetworkRequests> {
            let mut ban_counter = ban_counter.write();
            match request {
                NetworkRequests::Block { mut block } => {
                    let epoch_id = block.header().epoch_id();
                    let height = block.header().height();
                    if height == bad_block_send_height {
                        let (bp, _) = epoch_manager
                            .get_block_producer_info(&epoch_id, height)
                            .unwrap()
                            .account_and_stake();
                        let validator_signer = create_test_signer(bp.as_str());
                        match mode {
                            InvalidBlockMode::InvalidHeader => {
                                // produce an invalid block with invalid header.
                                block.mut_header().set_chunk_mask(vec![]);
                                block.mut_header().resign(&validator_signer);
                            }
                            InvalidBlockMode::IllFormed => {
                                // produce an ill-formed block
                                block.mut_header().set_chunk_headers_root(hash(&[1]));
                                block.mut_header().resign(&validator_signer);
                            }
                            InvalidBlockMode::InvalidBlock => {
                                // produce an invalid block whose invalidity cannot be verified by just
                                // having its header.
                                let proposals = vec![ValidatorStake::new(
                                    bp,
                                    PublicKey::empty(KeyType::ED25519),
                                    0,
                                )];

                                block.mut_header().set_prev_validator_proposals(proposals);
                                block.mut_header().resign(&validator_signer);
                            }
                        }
                    }
                    Some(NetworkRequests::Block { block })
                }
                NetworkRequests::BanPeer { ref peer_id, ref ban_reason } => match mode {
                    InvalidBlockMode::InvalidHeader | InvalidBlockMode::IllFormed => {
                        assert_eq!(ban_reason, &ReasonForBan::BadBlockHeader);
                        *ban_counter += 1;
                        if *ban_counter > 3 {
                            panic!("more bans than expected");
                        }
                        None
                    }
                    InvalidBlockMode::InvalidBlock => {
                        panic!("banning peer {:?} unexpectedly for {:?}", peer_id, ban_reason);
                    }
                },
                _ => Some(request),
            }
        }));
    }

    env.test_loop.run_until(
        |test_loop_data| {
            let client = &test_loop_data.get(client_actor_handle).client;
            let head = client.chain.head().unwrap();
            head.height >= 25
        },
        Duration::seconds(60),
    );

    let ban_counter = *ban_counter.read();
    match mode {
        InvalidBlockMode::InvalidHeader | InvalidBlockMode::IllFormed => {
            assert_eq!(ban_counter, 3);
        }
        InvalidBlockMode::InvalidBlock => {
            assert_eq!(ban_counter, 0);
        }
    }

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

/// If a peer sends a block whose header is valid and passes basic validation, the peer is not banned.
#[test]
fn test_not_ban_peer_for_invalid_block() {
    ban_peer_for_invalid_block_common(InvalidBlockMode::InvalidBlock);
}

/// If a peer sends a block whose header is invalid, we should ban them and do not forward the block
#[test]
fn test_ban_peer_for_invalid_block_header() {
    ban_peer_for_invalid_block_common(InvalidBlockMode::InvalidHeader);
}

/// If a peer sends a block that is ill-formed, we should ban them and do not forward the block
#[test]
fn test_ban_peer_for_ill_formed_block() {
    ban_peer_for_invalid_block_common(InvalidBlockMode::IllFormed);
}

#[test]
fn test_produce_block_with_approvals_arrived_early() {
    init_test_logger();

    let block_producers = ["test1", "test2", "test3", "test4"];
    let validators_spec = ValidatorsSpec::desired_roles(&block_producers, &[]);
    let shard_layout = ShardLayout::multi_shard(4, 3);
    let epoch_length = 100;

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
        .build()
        .warmup();

    let client_actor_handle = &env.node_datas[0].client_sender.actor_handle();
    let client = &env.test_loop.data.get(&client_actor_handle).client;
    let epoch_manager = client.epoch_manager.clone();

    let block_holder: Arc<RwLock<Option<BlockResponse>>> = Arc::new(RwLock::new(None));
    let approval_counter: Arc<RwLock<usize>> = Arc::new(RwLock::new(0));
    let client_senders: HashMap<_, _> = env
        .node_datas
        .iter()
        .map(|datas| (datas.account_id.clone(), datas.client_sender.clone()))
        .collect();

    let block_withholding_height = 10;
    let epoch_id = client.chain.head().unwrap().epoch_id;

    let (block_producer_for_next_height, _) = epoch_manager
        .get_block_producer_info(&epoch_id, block_withholding_height + 1)
        .unwrap()
        .account_and_stake();
    let (block_producer_for_height, _) = epoch_manager
        .get_block_producer_info(&epoch_id, block_withholding_height)
        .unwrap()
        .account_and_stake();
    // With the same block producer - block withholding wouldn't test much.
    assert_ne!(block_producer_for_height, block_producer_for_next_height);

    for node in &env.node_datas {
        let my_account_id = node.account_id.clone();
        let peer_id = node.peer_id.clone();

        let approval_counter = approval_counter.clone();
        let block_holder = block_holder.clone();
        let client_senders = client_senders.clone();
        let block_producer_for_next_height = block_producer_for_next_height.clone();

        let peer_actor_handle = node.peer_manager_sender.actor_handle();
        let peer_actor = env.test_loop.data.get_mut(&peer_actor_handle);
        peer_actor.register_override_handler(Box::new(move |request| -> Option<NetworkRequests> {
            let mut approval_counter = approval_counter.write();
            match &request {
                NetworkRequests::Block { block } => {
                    if block.header().height() == block_withholding_height {
                        for (account, sender) in &client_senders {
                            if *account == block_producer_for_next_height
                                || *account == my_account_id
                            {
                                continue;
                            }
                            sender.send(BlockResponse {
                                block: block.clone(),
                                peer_id: peer_id.clone(),
                                was_requested: false,
                            });
                        }
                        *block_holder.write() = Some(BlockResponse {
                            block: block.clone(),
                            peer_id: peer_id.clone(),
                            was_requested: false,
                        });
                        return None;
                    }
                    Some(request)
                }
                NetworkRequests::Approval { approval_message } => {
                    if approval_message.target == block_producer_for_next_height
                        && approval_message.approval.target_height == block_withholding_height + 1
                    {
                        *approval_counter += 1;
                    }
                    if *approval_counter == 3 {
                        let block_response = block_holder.read().clone().unwrap();
                        client_senders[&block_producer_for_next_height].send(block_response);
                    }
                    Some(request)
                }
                _ => Some(request),
            }
        }));
    }

    env.test_loop.run_until(
        |test_loop_data| {
            let client = &test_loop_data.get(client_actor_handle).client;
            let head = client.chain.final_head().unwrap();
            head.height >= block_withholding_height + 1
        },
        Duration::seconds(60),
    );

    // Block after delayed one should still be produced though approvals for it arrived before the
    // block.
    let client = &env.test_loop.data.get(client_actor_handle).client;
    assert!(client.chain.get_block_by_height(block_withholding_height + 1).is_ok());

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

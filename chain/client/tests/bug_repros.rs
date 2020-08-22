// This test tracks tests that reproduce previously fixed bugs to make sure the regressions we
// fix do not resurface

use std::cmp::max;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use actix::{Addr, System};
use rand::{thread_rng, Rng};

use futures::FutureExt;
use near_chain::test_utils::account_id_to_shard_id;
use near_client::test_utils::setup_mock_all_validators;
use near_client::{ClientActor, GetBlock, ViewClientActor};
use near_crypto::{InMemorySigner, KeyType};
use near_logger_utils::init_test_logger;
use near_network::types::NetworkRequests::PartialEncodedChunkMessage;
use near_network::{NetworkClientMessages, NetworkRequests, NetworkResponses, PeerInfo};
use near_primitives::block::Block;
use near_primitives::transaction::SignedTransaction;

#[test]
fn repro_1183() {
    let validator_groups = 2;
    init_test_logger();
    System::run(move || {
        let connectors: Arc<RwLock<Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>>> =
            Arc::new(RwLock::new(vec![]));

        let validators = vec![vec!["test1", "test2", "test3", "test4"]];
        let key_pairs = vec![
            PeerInfo::random(),
            PeerInfo::random(),
            PeerInfo::random(),
            PeerInfo::random(), // 4
        ];

        let connectors1 = connectors.clone();
        let validators2 = validators.clone();
        let last_block: Arc<RwLock<Option<Block>>> = Arc::new(RwLock::new(None));
        let delayed_one_parts: Arc<RwLock<Vec<NetworkRequests>>> = Arc::new(RwLock::new(vec![]));
        let (_, conn, _) = setup_mock_all_validators(
            validators.clone(),
            key_pairs.clone(),
            validator_groups,
            true,
            200,
            false,
            false,
            5,
            false,
            vec![false; validators.iter().map(|x| x.len()).sum()],
            false,
            Arc::new(RwLock::new(Box::new(move |_account_id: String, msg: &NetworkRequests| {
                if let NetworkRequests::Block { block } = msg {
                    let mut last_block = last_block.write().unwrap();
                    let mut delayed_one_parts = delayed_one_parts.write().unwrap();

                    if let Some(last_block) = last_block.clone() {
                        for (client, _) in connectors1.write().unwrap().iter() {
                            client.do_send(NetworkClientMessages::Block(
                                last_block.clone(),
                                PeerInfo::random().id,
                                false,
                            ))
                        }
                    }
                    for delayed_message in delayed_one_parts.iter() {
                        if let PartialEncodedChunkMessage {
                            account_id,
                            partial_encoded_chunk,
                            ..
                        } = delayed_message
                        {
                            for (i, name) in validators2.iter().flatten().enumerate() {
                                if &name.to_string() == account_id {
                                    connectors1.write().unwrap()[i].0.do_send(
                                        NetworkClientMessages::PartialEncodedChunk(
                                            partial_encoded_chunk.clone(),
                                        ),
                                    );
                                }
                            }
                        } else {
                            assert!(false);
                        }
                    }

                    let mut nonce_delta = 0;
                    for from in vec!["test1", "test2", "test3", "test4"] {
                        for to in vec!["test1", "test2", "test3", "test4"] {
                            connectors1.write().unwrap()
                                [account_id_to_shard_id(&from.to_string(), 4) as usize]
                                .0
                                .do_send(NetworkClientMessages::Transaction {
                                    transaction: SignedTransaction::send_money(
                                        block.header().height() * 16 + nonce_delta,
                                        from.to_string(),
                                        to.to_string(),
                                        &InMemorySigner::from_seed(from, KeyType::ED25519, from),
                                        1,
                                        *block.header().prev_hash(),
                                    ),
                                    is_forwarded: false,
                                    check_only: false,
                                });
                            nonce_delta += 1
                        }
                    }

                    *last_block = Some(block.clone());
                    *delayed_one_parts = vec![];

                    if block.header().height() >= 25 {
                        System::current().stop();
                    }
                    (NetworkResponses::NoResponse, false)
                } else if let NetworkRequests::PartialEncodedChunkMessage { .. } = msg {
                    if thread_rng().gen_bool(0.5) {
                        (NetworkResponses::NoResponse, true)
                    } else {
                        let msg2 = msg.clone();
                        delayed_one_parts.write().unwrap().push(msg2);
                        (NetworkResponses::NoResponse, false)
                    }
                } else {
                    (NetworkResponses::NoResponse, true)
                }
            }))),
        );
        *connectors.write().unwrap() = conn;

        near_network::test_utils::wait_or_panic(60000);
    })
    .unwrap();
}

#[test]
fn test_sync_from_achival_node() {
    init_test_logger();
    let validators = vec![vec!["test1", "test2", "test3", "test4"]];
    let key_pairs =
        vec![PeerInfo::random(), PeerInfo::random(), PeerInfo::random(), PeerInfo::random()];
    let largest_height = Arc::new(RwLock::new(0));
    let blocks = Arc::new(RwLock::new(HashMap::new()));
    let epoch_length = 4;

    System::run(move || {
        let network_mock: Arc<
            RwLock<Box<dyn FnMut(String, &NetworkRequests) -> (NetworkResponses, bool)>>,
        > = Arc::new(RwLock::new(Box::new(|_: String, _: &NetworkRequests| {
            (NetworkResponses::NoResponse, true)
        })));
        let (_, conns, _) = setup_mock_all_validators(
            validators.clone(),
            key_pairs,
            1,
            true,
            100,
            false,
            false,
            epoch_length,
            false,
            vec![true, false, false, false],
            false,
            network_mock.clone(),
        );
        let mut block_counter = 0;
        *network_mock.write().unwrap() =
            Box::new(move |_: String, msg: &NetworkRequests| -> (NetworkResponses, bool) {
                if let NetworkRequests::Block { block } = msg {
                    let mut largest_height = largest_height.write().unwrap();
                    *largest_height = max(block.header().height(), *largest_height);
                }
                if *largest_height.read().unwrap() >= 50 {
                    System::current().stop();
                }
                if *largest_height.read().unwrap() <= 30 {
                    match msg {
                        NetworkRequests::Block { block } => {
                            for (i, (client, _)) in conns.clone().into_iter().enumerate() {
                                if i != 3 {
                                    client.do_send(NetworkClientMessages::Block(
                                        block.clone(),
                                        PeerInfo::random().id,
                                        false,
                                    ))
                                }
                            }
                            if block.header().height() <= 10 {
                                blocks.write().unwrap().insert(*block.hash(), block.clone());
                            }
                            (NetworkResponses::NoResponse, false)
                        }
                        NetworkRequests::Approval { approval_message } => {
                            for (i, (client, _)) in conns.clone().into_iter().enumerate() {
                                if i != 3 {
                                    client.do_send(NetworkClientMessages::BlockApproval(
                                        approval_message.approval.clone(),
                                        PeerInfo::random().id,
                                    ))
                                }
                            }
                            (NetworkResponses::NoResponse, false)
                        }
                        _ => (NetworkResponses::NoResponse, true),
                    }
                } else {
                    if block_counter > 10 {
                        panic!("incorrect rebroadcasting of blocks");
                    }
                    for (_, block) in blocks.write().unwrap().drain() {
                        conns[3].0.do_send(NetworkClientMessages::Block(
                            block,
                            PeerInfo::random().id,
                            false,
                        ));
                    }
                    match msg {
                        NetworkRequests::Block { block } => {
                            if block.header().height() <= 10 {
                                block_counter += 1;
                            }
                            (NetworkResponses::NoResponse, true)
                        }
                        _ => (NetworkResponses::NoResponse, true),
                    }
                }
            });

        near_network::test_utils::wait_or_panic(20000);
    })
    .unwrap();
}

#[test]
fn test_long_gap_between_blocks() {
    init_test_logger();
    let validators = vec![vec!["test1", "test2"]];
    let key_pairs = vec![PeerInfo::random(), PeerInfo::random()];
    let epoch_length = 1000;
    let target_height = 600;

    System::run(move || {
        let network_mock: Arc<
            RwLock<Box<dyn FnMut(String, &NetworkRequests) -> (NetworkResponses, bool)>>,
        > = Arc::new(RwLock::new(Box::new(|_: String, _: &NetworkRequests| {
            (NetworkResponses::NoResponse, true)
        })));
        let (_, conns, _) = setup_mock_all_validators(
            validators.clone(),
            key_pairs,
            1,
            true,
            10,
            false,
            false,
            epoch_length,
            true,
            vec![false, false],
            false,
            network_mock.clone(),
        );
        *network_mock.write().unwrap() =
            Box::new(move |_: String, msg: &NetworkRequests| -> (NetworkResponses, bool) {
                match msg {
                    NetworkRequests::Approval { approval_message } => {
                        actix::spawn(conns[1].1.send(GetBlock::latest()).then(move |res| {
                            let res = res.unwrap().unwrap();
                            if res.header.height > target_height {
                                System::current().stop();
                            }
                            futures::future::ready(())
                        }));
                        if approval_message.approval.target_height < target_height {
                            (NetworkResponses::NoResponse, false)
                        } else {
                            if approval_message.target == "test1".to_string() {
                                (NetworkResponses::NoResponse, true)
                            } else {
                                (NetworkResponses::NoResponse, false)
                            }
                        }
                    }
                    _ => (NetworkResponses::NoResponse, true),
                }
            });

        near_network::test_utils::wait_or_panic(60000);
    })
    .unwrap();
}

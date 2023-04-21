// This test tracks tests that reproduce previously fixed bugs to make sure the regressions we
// fix do not resurface

use std::cmp::max;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use actix::System;
use futures::FutureExt;
use near_async::messaging::CanSend;
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use rand::{thread_rng, Rng};

use crate::adapter::{BlockApproval, BlockResponse, ProcessTxRequest};
use crate::test_utils::{setup_mock_all_validators, ActorHandlesForTesting};
use crate::GetBlock;
use near_actix_test_utils::run_actix;
use near_chain::test_utils::{account_id_to_shard_id, ValidatorSchedule};
use near_crypto::{InMemorySigner, KeyType};
use near_network::types::NetworkRequests::PartialEncodedChunkMessage;
use near_network::types::PeerInfo;
use near_network::types::{
    NetworkRequests, NetworkResponses, PeerManagerMessageRequest, PeerManagerMessageResponse,
};
use near_o11y::testonly::init_test_logger;
use near_o11y::WithSpanContextExt;
use near_primitives::block::Block;
use near_primitives::transaction::SignedTransaction;

#[test]
fn repro_1183() {
    init_test_logger();
    run_actix(async {
        let connectors: Arc<RwLock<Vec<ActorHandlesForTesting>>> = Arc::new(RwLock::new(vec![]));

        let vs = ValidatorSchedule::new()
            .num_shards(4)
            .block_producers_per_epoch(vec![vec![
                "test1".parse().unwrap(),
                "test2".parse().unwrap(),
                "test3".parse().unwrap(),
                "test4".parse().unwrap(),
            ]])
            .validator_groups(2);
        let validators = vs.all_block_producers().cloned().collect::<Vec<_>>();
        let key_pairs = vec![
            PeerInfo::random(),
            PeerInfo::random(),
            PeerInfo::random(),
            PeerInfo::random(), // 4
        ];

        let connectors1 = connectors.clone();
        let last_block: Arc<RwLock<Option<Block>>> = Arc::new(RwLock::new(None));
        let delayed_one_parts: Arc<RwLock<Vec<NetworkRequests>>> = Arc::new(RwLock::new(vec![]));
        let (_, conn, _) = setup_mock_all_validators(
            vs,
            key_pairs,
            true,
            200,
            false,
            false,
            5,
            false,
            vec![false; validators.len()],
            vec![true; validators.len()],
            false,
            Box::new(move |_, _account_id: _, msg: &PeerManagerMessageRequest| {
                if let NetworkRequests::Block { block } = msg.as_network_requests_ref() {
                    let mut last_block = last_block.write().unwrap();
                    let mut delayed_one_parts = delayed_one_parts.write().unwrap();

                    if let Some(last_block) = last_block.clone() {
                        for actor_handles in connectors1.write().unwrap().iter() {
                            actor_handles.client_actor.do_send(
                                BlockResponse {
                                    block: last_block.clone(),
                                    peer_id: PeerInfo::random().id,
                                    was_requested: false,
                                }
                                .with_span_context(),
                            )
                        }
                    }
                    for delayed_message in delayed_one_parts.iter() {
                        if let PartialEncodedChunkMessage {
                            account_id,
                            partial_encoded_chunk,
                            ..
                        } = delayed_message
                        {
                            for (i, name) in validators.iter().enumerate() {
                                if name == account_id {
                                    connectors1.write().unwrap()[i].shards_manager_adapter.send(
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
                    for from in ["test1", "test2", "test3", "test4"].iter() {
                        for to in ["test1", "test2", "test3", "test4"].iter() {
                            let (from, to) = (from.parse().unwrap(), to.parse().unwrap());
                            connectors1.write().unwrap()[account_id_to_shard_id(&from, 4) as usize]
                                .client_actor
                                .do_send(
                                    ProcessTxRequest {
                                        transaction: SignedTransaction::send_money(
                                            block.header().height() * 16 + nonce_delta,
                                            from.clone(),
                                            to,
                                            &InMemorySigner::from_seed(
                                                from.clone(),
                                                KeyType::ED25519,
                                                from.as_ref(),
                                            ),
                                            1,
                                            *block.header().prev_hash(),
                                        ),
                                        is_forwarded: false,
                                        check_only: false,
                                    }
                                    .with_span_context(),
                                );
                            nonce_delta += 1
                        }
                    }

                    *last_block = Some(block.clone());
                    *delayed_one_parts = vec![];

                    if block.header().height() >= 25 {
                        System::current().stop();
                    }
                    (NetworkResponses::NoResponse.into(), false)
                } else if let NetworkRequests::PartialEncodedChunkMessage { .. } =
                    msg.as_network_requests_ref()
                {
                    if thread_rng().gen_bool(0.5) {
                        (NetworkResponses::NoResponse.into(), true)
                    } else {
                        delayed_one_parts
                            .write()
                            .unwrap()
                            .push(msg.as_network_requests_ref().clone());
                        (NetworkResponses::NoResponse.into(), false)
                    }
                } else {
                    (NetworkResponses::NoResponse.into(), true)
                }
            }),
        );
        *connectors.write().unwrap() = conn;

        near_network::test_utils::wait_or_panic(60000);
    });
}

#[test]
fn test_sync_from_archival_node() {
    init_test_logger();
    let vs = ValidatorSchedule::new().num_shards(4).block_producers_per_epoch(vec![vec![
        "test1".parse().unwrap(),
        "test2".parse().unwrap(),
        "test3".parse().unwrap(),
        "test4".parse().unwrap(),
    ]]);
    let key_pairs =
        vec![PeerInfo::random(), PeerInfo::random(), PeerInfo::random(), PeerInfo::random()];
    let largest_height = Arc::new(RwLock::new(0));
    let blocks = Arc::new(RwLock::new(HashMap::new()));
    let epoch_length = 4;

    run_actix(async move {
        let mut block_counter = 0;
        setup_mock_all_validators(
            vs,
            key_pairs,
            true,
            100,
            false,
            false,
            epoch_length,
            false,
            vec![true, false, false, false],
            vec![false, true, true, true],
            false,
            Box::new(
                move |conns,
                      _,
                      msg: &PeerManagerMessageRequest|
                      -> (PeerManagerMessageResponse, bool) {
                    let msg = msg.as_network_requests_ref();
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
                                for (i, actor_handles) in conns.iter().enumerate() {
                                    if i != 3 {
                                        actor_handles.client_actor.do_send(
                                            BlockResponse {
                                                block: block.clone(),
                                                peer_id: PeerInfo::random().id,
                                                was_requested: false,
                                            }
                                            .with_span_context(),
                                        )
                                    }
                                }
                                if block.header().height() <= 10 {
                                    blocks.write().unwrap().insert(*block.hash(), block.clone());
                                }
                                (NetworkResponses::NoResponse.into(), false)
                            }
                            NetworkRequests::Approval { approval_message } => {
                                for (i, actor_handles) in conns.into_iter().enumerate() {
                                    if i != 3 {
                                        actor_handles.client_actor.do_send(
                                            BlockApproval(
                                                approval_message.approval.clone(),
                                                PeerInfo::random().id,
                                            )
                                            .with_span_context(),
                                        )
                                    }
                                }
                                (NetworkResponses::NoResponse.into(), false)
                            }
                            _ => (NetworkResponses::NoResponse.into(), true),
                        }
                    } else {
                        if block_counter > 10 {
                            panic!("incorrect rebroadcasting of blocks");
                        }
                        for (_, block) in blocks.write().unwrap().drain() {
                            conns[3].client_actor.do_send(
                                BlockResponse {
                                    block,
                                    peer_id: PeerInfo::random().id,
                                    was_requested: false,
                                }
                                .with_span_context(),
                            );
                        }
                        match msg {
                            NetworkRequests::Block { block } => {
                                if block.header().height() <= 10 {
                                    block_counter += 1;
                                }
                                (NetworkResponses::NoResponse.into(), true)
                            }
                            _ => (NetworkResponses::NoResponse.into(), true),
                        }
                    }
                },
            ),
        );
        near_network::test_utils::wait_or_panic(20000);
    });
}

#[test]
fn test_long_gap_between_blocks() {
    init_test_logger();
    let vs = ValidatorSchedule::new()
        .num_shards(2)
        .block_producers_per_epoch(vec![vec!["test1".parse().unwrap(), "test2".parse().unwrap()]]);
    let key_pairs = vec![PeerInfo::random(), PeerInfo::random()];
    let epoch_length = 1000;
    let target_height = 600;

    run_actix(async move {
        setup_mock_all_validators(
            vs,
            key_pairs,
            true,
            10,
            false,
            false,
            epoch_length,
            true,
            vec![false, false],
            vec![true, true],
            false,
            Box::new(
                move |conns,
                      _,
                      msg: &PeerManagerMessageRequest|
                      -> (PeerManagerMessageResponse, bool) {
                    match msg.as_network_requests_ref() {
                        NetworkRequests::Approval { approval_message } => {
                            let actor = conns[1]
                                .view_client_actor
                                .send(GetBlock::latest().with_span_context());
                            let actor = actor.then(move |res| {
                                let res = res.unwrap().unwrap();
                                if res.header.height > target_height {
                                    System::current().stop();
                                }
                                futures::future::ready(())
                            });
                            actix::spawn(actor);
                            if approval_message.approval.target_height < target_height {
                                (NetworkResponses::NoResponse.into(), false)
                            } else {
                                if approval_message.target.as_ref() == "test1" {
                                    (NetworkResponses::NoResponse.into(), true)
                                } else {
                                    (NetworkResponses::NoResponse.into(), false)
                                }
                            }
                        }
                        _ => (NetworkResponses::NoResponse.into(), true),
                    }
                },
            ),
        );

        near_network::test_utils::wait_or_panic(60000);
    });
}

// This test tracks tests that reproduce previously fixed bugs to make sure the regressions we
// fix do not resurface

use actix::{Addr, System};
use near_chain::test_utils::account_id_to_shard_id;
use near_client::test_utils::setup_mock_all_validators;
use near_client::{ClientActor, ViewClientActor};
use near_crypto::{InMemorySigner, KeyType};
use near_network::types::NetworkRequests::PartialEncodedChunkMessage;
use near_network::{NetworkClientMessages, NetworkRequests, NetworkResponses, PeerInfo};
use near_primitives::block::Block;
use near_primitives::test_utils::init_test_logger;
use near_primitives::transaction::SignedTransaction;
use rand::{thread_rng, Rng};
use std::sync::{Arc, RwLock};

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
        let (_, conn) = setup_mock_all_validators(
            validators.clone(),
            key_pairs.clone(),
            validator_groups,
            true,
            200,
            false,
            false,
            5,
            false,
            Arc::new(RwLock::new(move |_account_id: String, msg: &NetworkRequests| {
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
                                .do_send(NetworkClientMessages::Transaction(
                                    SignedTransaction::send_money(
                                        block.header.inner_lite.height * 16 + nonce_delta,
                                        from.to_string(),
                                        to.to_string(),
                                        &InMemorySigner::from_seed(from, KeyType::ED25519, from),
                                        1,
                                        block.header.prev_hash,
                                    ),
                                ));
                            nonce_delta += 1
                        }
                    }

                    *last_block = Some(block.clone());
                    *delayed_one_parts = vec![];

                    if block.header.inner_lite.height >= 25 {
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
            })),
        );
        *connectors.write().unwrap() = conn;

        near_network::test_utils::wait_or_panic(60000);
    })
    .unwrap();
}

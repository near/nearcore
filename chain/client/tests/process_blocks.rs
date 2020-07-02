use std::collections::HashSet;
use std::convert::TryFrom;
use std::iter::FromIterator;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use actix::System;
use futures::{future, FutureExt};
use num_rational::Rational;

use near_chain::chain::{check_refcount_map, NUM_EPOCHS_TO_KEEP_STORE_DATA};
use near_chain::{Block, ChainGenesis, ChainStoreAccess, ErrorKind, Provenance, RuntimeAdapter};
use near_chain_configs::{ClientConfig, Genesis};
use near_chunks::{ChunkStatus, ShardsManager};
use near_client::test_utils::setup_mock_all_validators;
use near_client::test_utils::{setup_client, setup_mock, TestEnv};
use near_client::{Client, GetBlock, GetBlockWithMerkleTree};
use near_crypto::{InMemorySigner, KeyType, Signature, Signer};
use near_logger_utils::init_test_logger;
#[cfg(feature = "metric_recorder")]
use near_network::recorder::MetricRecorder;
use near_network::routing::EdgeInfo;
use near_network::test_utils::{wait_or_panic, MockNetworkAdapter};
use near_network::types::{NetworkInfo, PeerChainInfo};
use near_network::{
    FullPeerInfo, NetworkClientMessages, NetworkClientResponses, NetworkRequests, NetworkResponses,
    PeerInfo,
};
use near_primitives::block::{Approval, ApprovalInner};
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::{merklize, verify_hash};
use near_primitives::sharding::{EncodedShardChunk, ReedSolomonWrapper};
use near_primitives::transaction::{SignedTransaction, Transaction};
use near_primitives::types::{BlockHeight, EpochId, MerkleHash, NumBlocks};
use near_primitives::utils::to_timestamp;
use near_primitives::validator_signer::{InMemoryValidatorSigner, ValidatorSigner};
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{QueryRequest, QueryResponseKind};
use near_store::test_utils::create_test_store;
use neard::config::{GenesisExt, TESTING_INIT_BALANCE, TESTING_INIT_STAKE};
use neard::NEAR_BASE;

/// Runs block producing client and stops after network mock received two blocks.
#[test]
fn produce_two_blocks() {
    init_test_logger();
    System::run(|| {
        let count = Arc::new(AtomicUsize::new(0));
        setup_mock(
            vec!["test"],
            "test",
            true,
            false,
            Box::new(move |msg, _ctx, _| {
                if let NetworkRequests::Block { .. } = msg {
                    count.fetch_add(1, Ordering::Relaxed);
                    if count.load(Ordering::Relaxed) >= 2 {
                        System::current().stop();
                    }
                }
                NetworkResponses::NoResponse
            }),
        );
        near_network::test_utils::wait_or_panic(5000);
    })
    .unwrap();
}

/// Runs block producing client and sends it a transaction.
#[test]
// TODO: figure out how to re-enable it correctly
#[ignore]
fn produce_blocks_with_tx() {
    let mut encoded_chunks: Vec<EncodedShardChunk> = vec![];
    init_test_logger();
    System::run(|| {
        let (client, view_client) = setup_mock(
            vec!["test"],
            "test",
            true,
            false,
            Box::new(move |msg, _ctx, _| {
                if let NetworkRequests::PartialEncodedChunkMessage {
                    account_id: _,
                    partial_encoded_chunk,
                } = msg
                {
                    let header = partial_encoded_chunk.header.clone();
                    let height = header.inner.height_created as usize;
                    assert!(encoded_chunks.len() + 2 >= height);

                    // the following two lines must match data_parts and total_parts in KeyValueRuntimeAdapter
                    let data_parts = 12 + 2 * (((height - 1) as usize) % 4);
                    let total_parts = 1 + data_parts * (1 + ((height - 1) as usize) % 3);
                    if encoded_chunks.len() + 2 == height {
                        encoded_chunks
                            .push(EncodedShardChunk::from_header(header.clone(), total_parts));
                    }
                    for part in partial_encoded_chunk.parts.iter() {
                        encoded_chunks[height - 2].content.parts[part.part_ord as usize] =
                            Some(part.part.clone());
                    }

                    let parity_parts = total_parts - data_parts;
                    let mut rs = ReedSolomonWrapper::new(data_parts, parity_parts);

                    if let ChunkStatus::Complete(_) = ShardsManager::check_chunk_complete(
                        &mut encoded_chunks[height - 2],
                        &mut rs,
                    ) {
                        let chunk = encoded_chunks[height - 2].decode_chunk(data_parts).unwrap();
                        if chunk.transactions.len() > 0 {
                            System::current().stop();
                        }
                    }
                }
                NetworkResponses::NoResponse
            }),
        );
        near_network::test_utils::wait_or_panic(5000);
        actix::spawn(view_client.send(GetBlock::latest()).then(move |res| {
            let block_hash = res.unwrap().unwrap().header.hash;
            client.do_send(NetworkClientMessages::Transaction {
                transaction: SignedTransaction::empty(block_hash),
                is_forwarded: false,
                check_only: false,
            });
            future::ready(())
        }))
    })
    .unwrap();
}

/// Runs client that receives a block from network and announces header to the network with approval.
/// Need 3 block producers, to receive approval.
#[test]
fn receive_network_block() {
    init_test_logger();
    System::run(|| {
        // The first header announce will be when the block is received. We don't immediately endorse
        // it. The second header announce will happen with the endorsement a little later.
        let first_header_announce = Arc::new(RwLock::new(true));
        let (client, view_client) = setup_mock(
            vec!["test2", "test1", "test3"],
            "test2",
            true,
            false,
            Box::new(move |msg, _ctx, _| {
                if let NetworkRequests::Approval { .. } = msg {
                    let mut first_header_announce = first_header_announce.write().unwrap();
                    if *first_header_announce {
                        *first_header_announce = false;
                    } else {
                        System::current().stop();
                    }
                }
                NetworkResponses::NoResponse
            }),
        );
        actix::spawn(view_client.send(GetBlockWithMerkleTree::latest()).then(move |res| {
            let (last_block, mut block_merkle_tree) = res.unwrap().unwrap();
            let signer = InMemoryValidatorSigner::from_seed("test1", KeyType::ED25519, "test1");
            block_merkle_tree.insert(last_block.header.hash);
            let block = Block::produce(
                PROTOCOL_VERSION,
                &last_block.header.clone().into(),
                last_block.header.height + 1,
                last_block.chunks.into_iter().map(Into::into).collect(),
                EpochId::default(),
                if last_block.header.prev_hash == CryptoHash::default() {
                    EpochId(last_block.header.hash)
                } else {
                    EpochId(last_block.header.next_epoch_id.clone())
                },
                vec![],
                Rational::from_integer(0),
                0,
                100,
                None,
                vec![],
                vec![],
                &signer,
                last_block.header.next_bp_hash,
                block_merkle_tree.root(),
            );
            client.do_send(NetworkClientMessages::Block(block, PeerInfo::random().id, false));
            future::ready(())
        }));
        near_network::test_utils::wait_or_panic(5000);
    })
    .unwrap();
}

/// Include approvals to the next block in newly produced block.
#[test]
fn produce_block_with_approvals() {
    init_test_logger();
    let validators = vec![
        "test1", "test2", "test3", "test4", "test5", "test6", "test7", "test8", "test9", "test10",
    ];
    System::run(|| {
        let (client, view_client) = setup_mock(
            validators.clone(),
            "test1",
            true,
            false,
            Box::new(move |msg, _ctx, _| {
                if let NetworkRequests::Block { block } = msg {
                    // Below we send approvals from all the block producers except for test1 and test2
                    // test1 will only create their approval for height 10 after their doomslug timer
                    // runs 10 iterations, which is way further in the future than them producing the
                    // block
                    if block.header().num_approvals() == validators.len() as u64 - 2 {
                        System::current().stop();
                    } else if block.header().height() == 10 {
                        println!("{}", block.header().height());
                        println!(
                            "{} != {} -2 (height: {})",
                            block.header().num_approvals(),
                            validators.len(),
                            block.header().height()
                        );

                        assert!(false);
                    }
                }
                NetworkResponses::NoResponse
            }),
        );
        actix::spawn(view_client.send(GetBlockWithMerkleTree::latest()).then(move |res| {
            let (last_block, mut block_merkle_tree) = res.unwrap().unwrap();
            let signer1 = InMemoryValidatorSigner::from_seed("test2", KeyType::ED25519, "test2");
            block_merkle_tree.insert(last_block.header.hash);
            let block = Block::produce(
                PROTOCOL_VERSION,
                &last_block.header.clone().into(),
                last_block.header.height + 1,
                last_block.chunks.into_iter().map(Into::into).collect(),
                EpochId::default(),
                if last_block.header.prev_hash == CryptoHash::default() {
                    EpochId(last_block.header.hash)
                } else {
                    EpochId(last_block.header.next_epoch_id.clone())
                },
                vec![],
                Rational::from_integer(0),
                0,
                100,
                Some(0),
                vec![],
                vec![],
                &signer1,
                last_block.header.next_bp_hash,
                block_merkle_tree.root(),
            );
            client.do_send(NetworkClientMessages::Block(
                block.clone(),
                PeerInfo::random().id,
                false,
            ));

            for i in 3..11 {
                let s = if i > 10 { "test1".to_string() } else { format!("test{}", i) };
                let signer = InMemoryValidatorSigner::from_seed(&s, KeyType::ED25519, &s);
                let approval = Approval::new(
                    *block.hash(),
                    block.header().height(),
                    10, // the height at which "test1" is producing
                    &signer,
                );
                client
                    .do_send(NetworkClientMessages::BlockApproval(approval, PeerInfo::random().id));
            }

            future::ready(())
        }));
        near_network::test_utils::wait_or_panic(5000);
    })
    .unwrap();
}

/// When approvals arrive early, they should be properly cached.
#[test]
fn produce_block_with_approvals_arrived_early() {
    init_test_logger();
    let validators = vec![vec!["test1", "test2", "test3", "test4"]];
    let key_pairs =
        vec![PeerInfo::random(), PeerInfo::random(), PeerInfo::random(), PeerInfo::random()];
    let block_holder: Arc<RwLock<Option<Block>>> = Arc::new(RwLock::new(None));
    System::run(move || {
        let mut approval_counter = 0;
        let network_mock: Arc<
            RwLock<Box<dyn FnMut(String, &NetworkRequests) -> (NetworkResponses, bool)>>,
        > = Arc::new(RwLock::new(Box::new(|_: String, _: &NetworkRequests| {
            (NetworkResponses::NoResponse, true)
        })));
        let (_, conns) = setup_mock_all_validators(
            validators.clone(),
            key_pairs,
            1,
            true,
            100,
            false,
            false,
            100,
            true,
            vec![false; validators.iter().map(|x| x.len()).sum()],
            network_mock.clone(),
        );
        *network_mock.write().unwrap() =
            Box::new(move |_: String, msg: &NetworkRequests| -> (NetworkResponses, bool) {
                match msg {
                    NetworkRequests::Block { block } => {
                        if block.header().height() == 3 {
                            for (i, (client, _)) in conns.clone().into_iter().enumerate() {
                                if i > 0 {
                                    client.do_send(NetworkClientMessages::Block(
                                        block.clone(),
                                        PeerInfo::random().id,
                                        false,
                                    ))
                                }
                            }
                            *block_holder.write().unwrap() = Some(block.clone());
                            return (NetworkResponses::NoResponse, false);
                        } else if block.header().height() == 4 {
                            System::current().stop();
                        }
                        (NetworkResponses::NoResponse, true)
                    }
                    NetworkRequests::Approval { approval_message } => {
                        if approval_message.target == "test1".to_string()
                            && approval_message.approval.target_height == 4
                        {
                            approval_counter += 1;
                        }
                        if approval_counter == 3 {
                            let block = block_holder.read().unwrap().clone().unwrap();
                            conns[0].0.do_send(NetworkClientMessages::Block(
                                block,
                                PeerInfo::random().id,
                                false,
                            ));
                        }
                        (NetworkResponses::NoResponse, true)
                    }
                    _ => (NetworkResponses::NoResponse, true),
                }
            });

        near_network::test_utils::wait_or_panic(5000);
    })
    .unwrap();
}

/// Sends one invalid block followed by one valid block, and checks that client announces only valid block.
#[test]
fn invalid_blocks() {
    init_test_logger();
    System::run(|| {
        let (client, view_client) = setup_mock(
            vec!["test"],
            "other",
            false,
            false,
            Box::new(move |msg, _ctx, _client_actor| {
                match msg {
                    NetworkRequests::Block { block } => {
                        assert_eq!(block.header().height(), 1);
                        assert_eq!(
                            block.header().prev_state_root(),
                            &merklize(&vec![MerkleHash::default()]).0
                        );
                        System::current().stop();
                    }
                    _ => {}
                };
                NetworkResponses::NoResponse
            }),
        );
        actix::spawn(view_client.send(GetBlockWithMerkleTree::latest()).then(move |res| {
            let (last_block, mut block_merkle_tree) = res.unwrap().unwrap();
            let signer = InMemoryValidatorSigner::from_seed("test", KeyType::ED25519, "test");
            // Send block with invalid chunk mask
            let mut block = Block::produce(
                PROTOCOL_VERSION,
                &last_block.header.clone().into(),
                last_block.header.height + 1,
                last_block.chunks.iter().cloned().map(Into::into).collect(),
                EpochId::default(),
                if last_block.header.prev_hash == CryptoHash::default() {
                    EpochId(last_block.header.hash)
                } else {
                    EpochId(last_block.header.next_epoch_id.clone())
                },
                vec![],
                Rational::from_integer(0),
                0,
                100,
                Some(0),
                vec![],
                vec![],
                &signer,
                last_block.header.next_bp_hash,
                CryptoHash::default(),
            );
            block.mut_header().get_mut().inner_rest.chunk_mask = vec![];
            client.do_send(NetworkClientMessages::Block(
                block.clone(),
                PeerInfo::random().id,
                false,
            ));

            // Send proper block.
            block_merkle_tree.insert(last_block.header.hash);
            let block2 = Block::produce(
                PROTOCOL_VERSION,
                &last_block.header.clone().into(),
                last_block.header.height + 1,
                last_block.chunks.into_iter().map(Into::into).collect(),
                EpochId::default(),
                if last_block.header.prev_hash == CryptoHash::default() {
                    EpochId(last_block.header.hash)
                } else {
                    EpochId(last_block.header.next_epoch_id.clone())
                },
                vec![],
                Rational::from_integer(0),
                0,
                100,
                Some(0),
                vec![],
                vec![],
                &signer,
                last_block.header.next_bp_hash,
                block_merkle_tree.root(),
            );
            client.do_send(NetworkClientMessages::Block(block2, PeerInfo::random().id, false));
            future::ready(())
        }));
        near_network::test_utils::wait_or_panic(5000);
    })
    .unwrap();
}

/// Runs two validators runtime with only one validator online.
/// Present validator produces blocks on it's height after deadline.
#[test]
fn skip_block_production() {
    init_test_logger();
    System::run(|| {
        setup_mock(
            vec!["test1", "test2"],
            "test2",
            true,
            false,
            Box::new(move |msg, _ctx, _client_actor| {
                match msg {
                    NetworkRequests::Block { block } => {
                        if block.header().height() > 3 {
                            System::current().stop();
                        }
                    }
                    _ => {}
                };
                NetworkResponses::NoResponse
            }),
        );
        wait_or_panic(10000);
    })
    .unwrap();
}

/// Runs client that requests syncing headers from peers.
#[test]
fn client_sync_headers() {
    init_test_logger();
    System::run(|| {
        let peer_info1 = PeerInfo::random();
        let peer_info2 = peer_info1.clone();
        let (client, _) = setup_mock(
            vec!["test"],
            "other",
            false,
            false,
            Box::new(move |msg, _ctx, _client_actor| match msg {
                NetworkRequests::BlockHeadersRequest { hashes, peer_id } => {
                    assert_eq!(*peer_id, peer_info1.id);
                    assert_eq!(hashes.len(), 1);
                    // TODO: check it requests correct hashes.
                    System::current().stop();
                    NetworkResponses::NoResponse
                }
                _ => NetworkResponses::NoResponse,
            }),
        );
        client.do_send(NetworkClientMessages::NetworkInfo(NetworkInfo {
            active_peers: vec![FullPeerInfo {
                peer_info: peer_info2.clone(),
                chain_info: PeerChainInfo {
                    genesis_id: Default::default(),
                    height: 5,
                    tracked_shards: vec![],
                },
                edge_info: EdgeInfo::default(),
            }],
            num_active_peers: 1,
            peer_max_count: 1,
            highest_height_peers: vec![FullPeerInfo {
                peer_info: peer_info2.clone(),
                chain_info: PeerChainInfo {
                    genesis_id: Default::default(),
                    height: 5,
                    tracked_shards: vec![],
                },
                edge_info: EdgeInfo::default(),
            }],
            sent_bytes_per_sec: 0,
            received_bytes_per_sec: 0,
            known_producers: vec![],
            #[cfg(feature = "metric_recorder")]
            metric_recorder: MetricRecorder::default(),
        }));
        wait_or_panic(2000);
    })
    .unwrap();
}

fn produce_blocks(client: &mut Client, num: u64) {
    for i in 1..num {
        let b = client.produce_block(i).unwrap().unwrap();
        let (mut accepted_blocks, _) = client.process_block(b, Provenance::PRODUCED);
        let more_accepted_blocks = client.run_catchup(&vec![]).unwrap();
        accepted_blocks.extend(more_accepted_blocks);
        for accepted_block in accepted_blocks {
            client.on_block_accepted(
                accepted_block.hash,
                accepted_block.status,
                accepted_block.provenance,
            );
        }
    }
}

#[test]
fn test_process_invalid_tx() {
    init_test_logger();
    let store = create_test_store();
    let network_adapter = Arc::new(MockNetworkAdapter::default());
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.transaction_validity_period = 10;
    let mut client = setup_client(
        store,
        vec![vec!["test1"]],
        1,
        1,
        Some("test1"),
        false,
        network_adapter,
        chain_genesis,
    );
    let signer = InMemorySigner::from_seed("test1", KeyType::ED25519, "test1");
    let tx = SignedTransaction::new(
        Signature::empty(KeyType::ED25519),
        Transaction {
            signer_id: "".to_string(),
            public_key: signer.public_key(),
            nonce: 0,
            receiver_id: "".to_string(),
            block_hash: *client.chain.genesis().hash(),
            actions: vec![],
        },
    );
    produce_blocks(&mut client, 12);
    assert_eq!(
        client.process_tx(tx, false, false),
        NetworkClientResponses::InvalidTx(InvalidTxError::Expired)
    );
    let tx2 = SignedTransaction::new(
        Signature::empty(KeyType::ED25519),
        Transaction {
            signer_id: "".to_string(),
            public_key: signer.public_key(),
            nonce: 0,
            receiver_id: "".to_string(),
            block_hash: hash(&[1]),
            actions: vec![],
        },
    );
    assert_eq!(
        client.process_tx(tx2, false, false),
        NetworkClientResponses::InvalidTx(InvalidTxError::Expired)
    );
}

/// If someone produce a block with Utc::now() + 1 min, we should produce a block with valid timestamp
#[test]
fn test_time_attack() {
    init_test_logger();
    let store = create_test_store();
    let network_adapter = Arc::new(MockNetworkAdapter::default());
    let chain_genesis = ChainGenesis::test();
    let mut client = setup_client(
        store,
        vec![vec!["test1"]],
        1,
        1,
        Some("test1"),
        false,
        network_adapter,
        chain_genesis,
    );
    let signer = InMemoryValidatorSigner::from_seed("test1", KeyType::ED25519, "test1");
    let genesis = client.chain.get_block_by_height(0).unwrap();
    let mut b1 = Block::empty_with_height(genesis, 1, &signer);
    b1.mut_header().get_mut().inner_lite.timestamp =
        to_timestamp(b1.header().timestamp() + chrono::Duration::seconds(60));
    b1.mut_header().resign(&signer);

    let _ = client.process_block(b1, Provenance::NONE);

    let b2 = client.produce_block(2).unwrap().unwrap();
    assert!(client.process_block(b2, Provenance::PRODUCED).1.is_ok());
}

// TODO: use real runtime for this test
#[test]
#[ignore]
fn test_invalid_approvals() {
    init_test_logger();
    let store = create_test_store();
    let network_adapter = Arc::new(MockNetworkAdapter::default());
    let chain_genesis = ChainGenesis::test();
    let mut client = setup_client(
        store,
        vec![vec!["test1"]],
        1,
        1,
        Some("test1"),
        false,
        network_adapter,
        chain_genesis,
    );
    let signer = InMemoryValidatorSigner::from_seed("test1", KeyType::ED25519, "test1");
    let genesis = client.chain.get_block_by_height(0).unwrap();
    let mut b1 = Block::empty_with_height(genesis, 1, &signer);
    b1.mut_header().get_mut().inner_rest.approvals = (0..100)
        .map(|i| {
            Some(
                InMemoryValidatorSigner::from_seed(
                    &format!("test{}", i),
                    KeyType::ED25519,
                    &format!("test{}", i),
                )
                .sign_approval(&ApprovalInner::Endorsement(*genesis.hash()), 1),
            )
        })
        .collect();
    b1.mut_header().resign(&signer);

    let (_, tip) = client.process_block(b1, Provenance::NONE);
    match tip {
        Err(e) => match e.kind() {
            ErrorKind::InvalidApprovals => {}
            _ => assert!(false, "wrong error: {}", e),
        },
        _ => assert!(false, "succeeded, tip: {:?}", tip),
    }
}

#[test]
fn test_no_double_sign() {
    let mut env = TestEnv::new(ChainGenesis::test(), 1, 1);
    let _ = env.clients[0].produce_block(1).unwrap().unwrap();
    // Second time producing with the same height should fail.
    assert_eq!(env.clients[0].produce_block(1).unwrap(), None);
}

#[test]
fn test_invalid_gas_price() {
    init_test_logger();
    let store = create_test_store();
    let network_adapter = Arc::new(MockNetworkAdapter::default());
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.min_gas_price = 100;
    let mut client = setup_client(
        store,
        vec![vec!["test1"]],
        1,
        1,
        Some("test1"),
        false,
        network_adapter,
        chain_genesis,
    );
    let signer = InMemoryValidatorSigner::from_seed("test1", KeyType::ED25519, "test1");
    let genesis = client.chain.get_block_by_height(0).unwrap();
    let mut b1 = Block::empty_with_height(genesis, 1, &signer);
    b1.mut_header().get_mut().inner_rest.gas_price = 0;
    b1.mut_header().resign(&signer);

    let (_, result) = client.process_block(b1, Provenance::NONE);
    match result {
        Err(e) => match e.kind() {
            ErrorKind::InvalidGasPrice => {}
            _ => assert!(false, "wrong error: {}", e),
        },
        _ => assert!(false, "succeeded, tip: {:?}", result),
    }
}

#[test]
fn test_invalid_height() {
    let mut env = TestEnv::new(ChainGenesis::test(), 1, 1);
    let b1 = env.clients[0].produce_block(1).unwrap().unwrap();
    let _ = env.clients[0].process_block(b1.clone(), Provenance::PRODUCED);
    let signer = InMemoryValidatorSigner::from_seed("test0", KeyType::ED25519, "test0");
    let b2 = Block::empty_with_height(&b1, std::u64::MAX, &signer);
    let (_, tip) = env.clients[0].process_block(b2, Provenance::NONE);
    match tip {
        Err(e) => match e.kind() {
            ErrorKind::InvalidBlockHeight => {}
            _ => assert!(false, "wrong error: {}", e),
        },
        _ => assert!(false, "succeeded, tip: {:?}", tip),
    }
}

#[test]
fn test_minimum_gas_price() {
    let min_gas_price = 100;
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.min_gas_price = min_gas_price;
    chain_genesis.gas_price_adjustment_rate = Rational::new(1, 10);
    let mut env = TestEnv::new(chain_genesis, 1, 1);
    for i in 1..=100 {
        env.produce_block(0, i);
    }
    let block = env.clients[0].chain.get_block_by_height(100).unwrap();
    assert!(block.header().gas_price() >= min_gas_price);
}

fn test_gc_with_epoch_length_common(epoch_length: NumBlocks) {
    let store = create_test_store();
    let mut genesis = Genesis::test(vec!["test0", "test1"], 1);
    genesis.config.epoch_length = epoch_length;
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(neard::NightshadeRuntime::new(
        Path::new("."),
        store,
        Arc::new(genesis),
        vec![],
        vec![],
    ))];
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    let mut env = TestEnv::new_with_runtime(chain_genesis, 1, 1, runtimes);
    let mut blocks = vec![];
    for i in 1..=epoch_length * (NUM_EPOCHS_TO_KEEP_STORE_DATA + 1) {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        blocks.push(block);
    }
    for i in 1..=epoch_length * (NUM_EPOCHS_TO_KEEP_STORE_DATA + 1) {
        println!("height = {}", i);
        if i < epoch_length {
            let block_hash = *blocks[i as usize - 1].hash();
            assert!(matches!(
                env.clients[0].chain.get_block(&block_hash).unwrap_err().kind(),
                ErrorKind::BlockMissing(missing_block_hash) if missing_block_hash == block_hash
            ));
            assert!(matches!(
                env.clients[0].chain.get_block_by_height(i).unwrap_err().kind(),
                ErrorKind::BlockMissing(missing_block_hash) if missing_block_hash == block_hash
            ));
            assert!(env.clients[0]
                .chain
                .mut_store()
                .get_all_block_hashes_by_height(i as BlockHeight)
                .is_err());
        } else {
            assert!(env.clients[0].chain.get_block(&blocks[i as usize - 1].hash()).is_ok());
            assert!(env.clients[0].chain.get_block_by_height(i).is_ok());
            assert!(env.clients[0]
                .chain
                .mut_store()
                .get_all_block_hashes_by_height(i as BlockHeight)
                .is_ok());
        }
    }
    assert_eq!(env.clients[0].chain.store().chunk_tail().unwrap(), epoch_length - 1);
    assert!(check_refcount_map(&mut env.clients[0].chain).is_ok());
}

#[test]
fn test_gc_with_epoch_length() {
    for i in 3..20 {
        test_gc_with_epoch_length_common(i);
    }
}

/// When an epoch is very long there should not be anything garbage collected unexpectedly
#[test]
fn test_gc_long_epoch() {
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0", "test1"], 5);
    genesis.config.epoch_length = epoch_length;
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![
        Arc::new(neard::NightshadeRuntime::new(
            Path::new("."),
            create_test_store(),
            Arc::new(genesis.clone()),
            vec![],
            vec![],
        )),
        Arc::new(neard::NightshadeRuntime::new(
            Path::new("."),
            create_test_store(),
            Arc::new(genesis),
            vec![],
            vec![],
        )),
    ];
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    let mut env = TestEnv::new_with_runtime(chain_genesis, 2, 5, runtimes);
    let num_blocks = 100;
    let mut blocks = vec![];

    for i in 1..=num_blocks {
        if i < epoch_length || i == num_blocks {
            let block_producer = env.clients[0]
                .runtime_adapter
                .get_block_producer(&EpochId(CryptoHash::default()), i)
                .unwrap();
            if block_producer == "test0".to_string() {
                let block = env.clients[0].produce_block(i).unwrap().unwrap();
                env.process_block(0, block.clone(), Provenance::PRODUCED);
                blocks.push(block);
            }
        }
    }
    for block in blocks {
        assert!(env.clients[0].chain.get_block(&block.hash()).is_ok());
        assert!(env.clients[0]
            .chain
            .mut_store()
            .get_all_block_hashes_by_height(block.header().height())
            .is_ok());
    }
    assert!(check_refcount_map(&mut env.clients[0].chain).is_ok());
}

#[test]
fn test_gc_block_skips() {
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = 5;
    let mut env = TestEnv::new(chain_genesis.clone(), 1, 1);
    for i in 1..=1000 {
        if i % 2 == 0 {
            env.produce_block(0, i);
        }
    }
    let mut env = TestEnv::new(chain_genesis.clone(), 1, 1);
    for i in 1..=1000 {
        if i % 2 == 1 {
            env.produce_block(0, i);
        }
    }
    // Epoch skips
    let mut env = TestEnv::new(chain_genesis, 1, 1);
    for i in 1..=1000 {
        if i % 9 == 7 {
            env.produce_block(0, i);
        }
    }
    assert!(check_refcount_map(&mut env.clients[0].chain).is_ok());
}

#[test]
fn test_gc_execution_outcome() {
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0", "test1"], 1);
    genesis.config.epoch_length = epoch_length;
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(neard::NightshadeRuntime::new(
        Path::new("."),
        create_test_store(),
        Arc::new(genesis.clone()),
        vec![],
        vec![],
    ))];
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    let mut env = TestEnv::new_with_runtime(chain_genesis, 1, 1, runtimes);
    let genesis_hash = *env.clients[0].chain.genesis().hash();
    let signer = InMemorySigner::from_seed("test0", KeyType::ED25519, "test0");
    let tx = SignedTransaction::send_money(
        1,
        "test0".to_string(),
        "test1".to_string(),
        &signer,
        100,
        genesis_hash,
    );
    let tx_hash = tx.get_hash();

    env.clients[0].process_tx(tx, false, false);
    for i in 1..epoch_length {
        env.produce_block(0, i);
    }
    assert!(env.clients[0].chain.get_final_transaction_result(&tx_hash).is_ok());

    for i in epoch_length..=epoch_length * 6 + 1 {
        env.produce_block(0, i);
    }
    assert!(env.clients[0].chain.get_final_transaction_result(&tx_hash).is_err());
}

#[test]
fn test_tx_forwarding() {
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = 100;
    let mut env = TestEnv::new(chain_genesis, 50, 50);
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let genesis_hash = *genesis_block.hash();
    // forward to 2 chunk producers
    env.clients[0].process_tx(SignedTransaction::empty(genesis_hash), false, false);
    assert_eq!(env.network_adapters[0].requests.read().unwrap().len(), 4);
}

#[test]
fn test_tx_forwarding_no_double_forwarding() {
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = 100;
    let mut env = TestEnv::new(chain_genesis, 50, 50);
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let genesis_hash = *genesis_block.hash();
    env.clients[0].process_tx(SignedTransaction::empty(genesis_hash), true, false);
    assert!(env.network_adapters[0].requests.read().unwrap().is_empty());
}

#[test]
fn test_tx_forward_around_epoch_boundary() {
    let epoch_length = 4;
    let mut genesis = Genesis::test(vec!["test0", "test1"], 1);
    genesis.config.num_block_producer_seats = 2;
    genesis.config.num_block_producer_seats_per_shard = vec![2];
    genesis.config.epoch_length = epoch_length;
    let create_runtime = |store| -> neard::NightshadeRuntime {
        neard::NightshadeRuntime::new(
            Path::new("."),
            store,
            Arc::new(genesis.clone()),
            vec![],
            vec![],
        )
    };
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![
        Arc::new(create_runtime(create_test_store())),
        Arc::new(create_runtime(create_test_store())),
        Arc::new(create_runtime(create_test_store())),
    ];
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    chain_genesis.gas_limit = genesis.config.gas_limit;
    let mut env = TestEnv::new_with_runtime(chain_genesis, 3, 2, runtimes);
    let genesis_hash = *env.clients[0].chain.genesis().hash();
    let signer = InMemorySigner::from_seed("test1", KeyType::ED25519, "test1");
    let tx = SignedTransaction::stake(
        1,
        "test1".to_string(),
        &signer,
        TESTING_INIT_STAKE,
        signer.public_key.clone(),
        genesis_hash,
    );
    env.clients[0].process_tx(tx, false, false);

    for i in 1..epoch_length * 2 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        for j in 0..3 {
            if j != 1 {
                let provenance = if j == 0 { Provenance::PRODUCED } else { Provenance::NONE };
                env.process_block(j, block.clone(), provenance);
            }
        }
    }
    let tx = SignedTransaction::send_money(
        1,
        "test1".to_string(),
        "test0".to_string(),
        &signer,
        1,
        genesis_hash,
    );
    env.clients[2].process_tx(tx, false, false);
    let mut accounts_to_forward = HashSet::new();
    for request in env.network_adapters[2].requests.read().unwrap().iter() {
        if let NetworkRequests::ForwardTx(account_id, _) = request {
            accounts_to_forward.insert(account_id.clone());
        }
    }
    assert_eq!(
        accounts_to_forward,
        HashSet::from_iter(vec!["test0".to_string(), "test1".to_string()])
    );
}

/// Blocks that have already been gc'ed should not be accepted again.
#[test]
fn test_not_resync_old_blocks() {
    let store = create_test_store();
    let mut genesis = Genesis::test(vec!["test0", "test1"], 1);
    let epoch_length = 5;
    genesis.config.epoch_length = epoch_length;
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(neard::NightshadeRuntime::new(
        Path::new("."),
        store,
        Arc::new(genesis),
        vec![],
        vec![],
    ))];
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    let mut env = TestEnv::new_with_runtime(chain_genesis, 1, 1, runtimes);
    let mut blocks = vec![];
    for i in 1..=epoch_length * (NUM_EPOCHS_TO_KEEP_STORE_DATA + 1) {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        blocks.push(block);
    }
    for i in 2..epoch_length {
        let block = blocks[i as usize - 1].clone();
        assert!(env.clients[0].chain.get_block(&block.hash()).is_err());
        let (_, res) = env.clients[0].process_block(block, Provenance::NONE);
        assert!(matches!(res, Err(x) if matches!(x.kind(), ErrorKind::Orphan)));
        assert_eq!(env.clients[0].chain.orphans_len(), 0);
    }
}

#[test]
fn test_gc_tail_update() {
    let mut genesis = Genesis::test(vec!["test0", "test1"], 1);
    let epoch_length = 2;
    genesis.config.epoch_length = epoch_length;
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![
        Arc::new(neard::NightshadeRuntime::new(
            Path::new("."),
            create_test_store(),
            Arc::new(genesis.clone()),
            vec![],
            vec![],
        )),
        Arc::new(neard::NightshadeRuntime::new(
            Path::new("."),
            create_test_store(),
            Arc::new(genesis),
            vec![],
            vec![],
        )),
    ];
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    let mut env = TestEnv::new_with_runtime(chain_genesis, 2, 1, runtimes);
    let mut blocks = vec![];
    for i in 1..=epoch_length * (NUM_EPOCHS_TO_KEEP_STORE_DATA + 1) {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        blocks.push(block);
    }
    let headers = blocks.iter().map(|b| b.header().clone()).collect::<Vec<_>>();
    env.clients[1].sync_block_headers(headers).unwrap();
    // simulate save sync hash block
    let prev_sync_block = blocks[blocks.len() - 3].clone();
    let sync_block = blocks[blocks.len() - 2].clone();
    env.clients[1].chain.reset_data_pre_state_sync(*sync_block.hash()).unwrap();
    let mut store_update = env.clients[1].chain.mut_store().store_update();
    store_update.save_block(prev_sync_block.clone());
    store_update.inc_block_refcount(&prev_sync_block.hash()).unwrap();
    store_update.save_block(sync_block.clone());
    store_update.commit().unwrap();
    env.clients[1]
        .chain
        .reset_heads_post_state_sync(&None, *sync_block.hash(), |_| {}, |_| {}, |_| {})
        .unwrap();
    env.process_block(1, blocks.pop().unwrap(), Provenance::NONE);
    assert_eq!(env.clients[1].chain.store().tail().unwrap(), prev_sync_block.header().height());
    assert!(check_refcount_map(&mut env.clients[0].chain).is_ok());
    assert!(check_refcount_map(&mut env.clients[1].chain).is_ok());
}

/// Test that transaction does not become invalid when there is some gas price change.
#[test]
fn test_gas_price_change() {
    init_test_logger();
    let mut genesis = Genesis::test(vec!["test0", "test1"], 1);
    let target_num_tokens_left = NEAR_BASE / 10 + 1;
    let send_money_total_gas = genesis
        .config
        .runtime_config
        .transaction_costs
        .action_creation_config
        .transfer_cost
        .send_fee(false)
        + genesis
            .config
            .runtime_config
            .transaction_costs
            .action_receipt_creation_config
            .send_fee(false)
        + genesis
            .config
            .runtime_config
            .transaction_costs
            .action_creation_config
            .transfer_cost
            .exec_fee()
        + genesis.config.runtime_config.transaction_costs.action_receipt_creation_config.exec_fee();
    let min_gas_price = target_num_tokens_left / send_money_total_gas as u128;
    let gas_limit = 1000000000000;
    let gas_price_adjustment_rate = Rational::new(1, 10);

    genesis.config.min_gas_price = min_gas_price;
    genesis.config.gas_limit = gas_limit;
    genesis.config.gas_price_adjustment_rate = gas_price_adjustment_rate;
    genesis.config.runtime_config.storage_amount_per_byte = 0;
    let genesis = Arc::new(genesis);
    let chain_genesis = ChainGenesis::from(&genesis);
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(neard::NightshadeRuntime::new(
        Path::new("."),
        create_test_store(),
        genesis,
        vec![],
        vec![],
    ))];
    let mut env = TestEnv::new_with_runtime(chain_genesis, 1, 1, runtimes);
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let genesis_hash = *genesis_block.hash();
    let signer = InMemorySigner::from_seed("test1", KeyType::ED25519, "test1");
    let tx = SignedTransaction::send_money(
        1,
        "test1".to_string(),
        "test0".to_string(),
        &signer,
        TESTING_INIT_BALANCE
            - target_num_tokens_left
            - send_money_total_gas as u128 * min_gas_price,
        genesis_hash,
    );
    env.clients[0].process_tx(tx, false, false);
    env.produce_block(0, 1);
    let tx = SignedTransaction::send_money(
        2,
        "test1".to_string(),
        "test0".to_string(),
        &signer,
        1,
        genesis_hash,
    );
    env.clients[0].process_tx(tx, false, false);
    for i in 2..=4 {
        env.produce_block(0, i);
    }
}

#[test]
fn test_gas_price_overflow() {
    let mut genesis = Genesis::test(vec!["test0", "test1"], 1);
    let min_gas_price = 1000000;
    let max_gas_price = 10_u128.pow(20);
    let gas_limit = 450000000000;
    let gas_price_adjustment_rate = Rational::from_integer(1);
    genesis.config.min_gas_price = min_gas_price;
    genesis.config.gas_limit = gas_limit;
    genesis.config.gas_price_adjustment_rate = gas_price_adjustment_rate;
    genesis.config.transaction_validity_period = 100000;
    genesis.config.epoch_length = 43200;
    genesis.config.max_gas_price = max_gas_price;
    let genesis = Arc::new(genesis);
    let chain_genesis = ChainGenesis::from(&genesis);
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(neard::NightshadeRuntime::new(
        Path::new("."),
        create_test_store(),
        genesis,
        vec![],
        vec![],
    ))];
    let mut env = TestEnv::new_with_runtime(chain_genesis, 1, 1, runtimes);
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let genesis_hash = *genesis_block.hash();
    let signer = InMemorySigner::from_seed("test1", KeyType::ED25519, "test1");
    for i in 1..100 {
        let tx = SignedTransaction::send_money(
            i,
            "test1".to_string(),
            "test0".to_string(),
            &signer,
            1,
            genesis_hash,
        );
        env.clients[0].process_tx(tx, false, false);
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        assert!(block.header().gas_price() <= max_gas_price);
        env.process_block(0, block, Provenance::PRODUCED);
    }
}

#[test]
fn test_invalid_block_root() {
    let mut env = TestEnv::new(ChainGenesis::test(), 1, 1);
    let mut b1 = env.clients[0].produce_block(1).unwrap().unwrap();
    let signer = InMemoryValidatorSigner::from_seed("test0", KeyType::ED25519, "test0");
    b1.mut_header().get_mut().inner_lite.block_merkle_root = CryptoHash::default();
    b1.mut_header().resign(&signer);
    let (_, tip) = env.clients[0].process_block(b1, Provenance::NONE);
    match tip {
        Err(e) => match e.kind() {
            ErrorKind::InvalidBlockMerkleRoot => {}
            _ => assert!(false, "wrong error: {}", e),
        },
        _ => assert!(false, "succeeded, tip: {:?}", tip),
    }
}

#[test]
fn test_incorrect_validator_key_produce_block() {
    let genesis = Genesis::test(vec!["test0", "test1"], 2);
    let genesis = Arc::new(genesis);
    let chain_genesis = ChainGenesis::from(&genesis);
    let runtime_adapter: Arc<dyn RuntimeAdapter> = Arc::new(neard::NightshadeRuntime::new(
        Path::new("."),
        create_test_store(),
        genesis,
        vec![],
        vec![],
    ));
    let signer = Arc::new(InMemoryValidatorSigner::from_seed("test0", KeyType::ED25519, "seed"));
    let mut config = ClientConfig::test(true, 10, 20, 2, false);
    config.epoch_length = chain_genesis.epoch_length;
    let mut client = Client::new(
        config,
        chain_genesis,
        runtime_adapter,
        Arc::new(MockNetworkAdapter::default()),
        Some(signer),
        false,
    )
    .unwrap();
    let res = client.produce_block(1);
    assert!(matches!(res, Ok(None)));
}

fn test_block_merkle_proof_with_len(n: NumBlocks) {
    let mut env = TestEnv::new(ChainGenesis::test(), 1, 1);
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap().clone();
    let mut blocks = vec![genesis_block.clone()];
    for i in 1..n {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        blocks.push(block.clone());
        env.process_block(0, block, Provenance::PRODUCED);
    }
    let head = blocks.pop().unwrap();
    let root = head.header().block_merkle_root();
    for block in blocks {
        let proof = env.clients[0].chain.get_block_proof(&block.hash(), &head.hash()).unwrap();
        assert!(verify_hash(*root, &proof, *block.hash()));
    }
}

#[test]
fn test_block_merkle_proof() {
    for i in 0..50 {
        test_block_merkle_proof_with_len(i);
    }
}

#[test]
fn test_block_merkle_proof_same_hash() {
    let mut env = TestEnv::new(ChainGenesis::test(), 1, 1);
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap().clone();
    let proof =
        env.clients[0].chain.get_block_proof(&genesis_block.hash(), &genesis_block.hash()).unwrap();
    assert!(proof.is_empty());
}

#[test]
fn test_data_reset_before_state_sync() {
    let mut genesis = Genesis::test(vec!["test0"], 1);
    let epoch_length = 5;
    genesis.config.epoch_length = epoch_length;
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(neard::NightshadeRuntime::new(
        Path::new("."),
        create_test_store(),
        Arc::new(genesis.clone()),
        vec![],
        vec![],
    ))];
    let mut env = TestEnv::new_with_runtime(ChainGenesis::test(), 1, 1, runtimes);
    let signer = InMemorySigner::from_seed("test0", KeyType::ED25519, "test0");
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let genesis_hash = *genesis_block.hash();
    let tx = SignedTransaction::create_account(
        1,
        "test0".to_string(),
        "test_account".to_string(),
        NEAR_BASE,
        signer.public_key(),
        &signer,
        genesis_hash,
    );
    env.clients[0].process_tx(tx, false, false);
    for i in 1..5 {
        env.produce_block(0, i);
    }
    // check that the new account exists
    let head = env.clients[0].chain.head().unwrap();
    let head_block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap().clone();
    let response = env.clients[0]
        .runtime_adapter
        .query(
            0,
            &head_block.chunks()[0].inner.prev_state_root,
            head.height,
            0,
            &head.last_block_hash,
            head_block.header().epoch_id(),
            &QueryRequest::ViewAccount { account_id: "test_account".to_string() },
        )
        .unwrap();
    assert!(matches!(response.kind, QueryResponseKind::ViewAccount(_)));
    env.clients[0].chain.reset_data_pre_state_sync(*head_block.hash()).unwrap();
    // account should not exist after clearing state
    let response = env.clients[0].runtime_adapter.query(
        0,
        &head_block.chunks()[0].inner.prev_state_root,
        head.height,
        0,
        &head.last_block_hash,
        head_block.header().epoch_id(),
        &QueryRequest::ViewAccount { account_id: "test_account".to_string() },
    );
    assert!(response.is_err());
}

#[test]
fn test_sync_hash_validity() {
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0", "test1"], 1);
    genesis.config.epoch_length = epoch_length;
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(neard::NightshadeRuntime::new(
        Path::new("."),
        create_test_store(),
        Arc::new(genesis.clone()),
        vec![],
        vec![],
    ))];
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    let mut env = TestEnv::new_with_runtime(chain_genesis, 1, 1, runtimes);
    for i in 1..19 {
        env.produce_block(0, i);
    }
    for i in 0..19 {
        let block_hash = env.clients[0].chain.get_header_by_height(i).unwrap().hash().clone();
        let res = env.clients[0].chain.check_sync_hash_validity(&block_hash);
        println!("height {:?} -> {:?}", i, res);
        if i == 11 || i == 16 {
            assert!(res.unwrap())
        } else {
            assert!(!res.unwrap())
        }
    }
    let bad_hash = CryptoHash::try_from("7tkzFg8RHBmMw1ncRJZCCZAizgq4rwCftTKYLce8RU8t").unwrap();
    let res = env.clients[0].chain.check_sync_hash_validity(&bad_hash);
    println!("bad hash -> {:?}", res.is_ok());
    match res {
        Ok(_) => assert!(false),
        Err(e) => match e.kind() {
            ErrorKind::DBNotFoundErr(_) => { /* the only expected error */ }
            _ => assert!(false),
        },
    }
}

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use actix::System;
use borsh::BorshSerialize;
use futures::{future, FutureExt};

use near_chain::{Block, ChainGenesis, ErrorKind, Provenance};
use near_chunks::{ChunkStatus, ShardsManager};
use near_client::test_utils::{setup_client, setup_mock, MockNetworkAdapter, TestEnv};
use near_client::{Client, GetBlock};
use near_crypto::{InMemorySigner, KeyType, Signature, Signer};
use near_network::routing::EdgeInfo;
use near_network::test_utils::wait_or_panic;
use near_network::types::{NetworkInfo, PeerChainInfo};
use near_network::{
    FullPeerInfo, NetworkClientMessages, NetworkClientResponses, NetworkRequests, NetworkResponses,
    PeerInfo,
};
use near_primitives::block::{Approval, BlockHeader};
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::merklize;
use near_primitives::sharding::EncodedShardChunk;
use near_primitives::test_utils::init_test_logger;
use near_primitives::transaction::{SignedTransaction, Transaction};
use near_primitives::types::{EpochId, MerkleHash};
use near_primitives::utils::to_timestamp;
use near_store::test_utils::create_test_store;

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
    use reed_solomon_erasure::galois_8::ReedSolomon;
    let mut encoded_chunks: Vec<EncodedShardChunk> = vec![];
    init_test_logger();
    System::run(|| {
        let (client, view_client) = setup_mock(
            vec!["test"],
            "test",
            true,
            Box::new(move |msg, _ctx, _| {
                if let NetworkRequests::PartialEncodedChunkMessage {
                    account_id: _,
                    partial_encoded_chunk,
                } = msg
                {
                    let header = partial_encoded_chunk.header.clone().unwrap();
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
                    let rs = ReedSolomon::new(data_parts, parity_parts).unwrap();

                    if let ChunkStatus::Complete(_) =
                        ShardsManager::check_chunk_complete(&mut encoded_chunks[height - 2], &rs)
                    {
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
        actix::spawn(view_client.send(GetBlock::Best).then(move |res| {
            let header: BlockHeader = res.unwrap().unwrap().header.into();
            let block_hash = header.hash;
            client
                .do_send(NetworkClientMessages::Transaction(SignedTransaction::empty(block_hash)));
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
        let (client, view_client) = setup_mock(
            vec!["test2", "test1", "test3"],
            "test2",
            true,
            Box::new(move |msg, _ctx, _| {
                if let NetworkRequests::BlockHeaderAnnounce { approval_message, .. } = msg {
                    assert!(approval_message.is_some());
                    System::current().stop();
                }
                NetworkResponses::NoResponse
            }),
        );
        actix::spawn(view_client.send(GetBlock::Best).then(move |res| {
            let last_block = res.unwrap().unwrap();
            let signer = InMemorySigner::from_seed("test1", KeyType::ED25519, "test1");
            let block = Block::produce(
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
                0,
                0,
                None,
                vec![],
                vec![],
                &signer,
                0.into(),
                CryptoHash::default(),
                CryptoHash::default(),
                last_block.header.next_bp_hash,
            );
            client.do_send(NetworkClientMessages::Block(block, PeerInfo::random().id, false));
            future::ready(())
        }));
        near_network::test_utils::wait_or_panic(5000);
    })
    .unwrap();
}

/// Runs client that receives a block from network and announces header to the network.
#[test]
fn receive_network_block_header() {
    let block_holder: Arc<RwLock<Option<Block>>> = Arc::new(RwLock::new(None));
    init_test_logger();
    System::run(|| {
        let block_holder1 = block_holder.clone();
        let (client, view_client) = setup_mock(
            vec!["test"],
            "other",
            true,
            Box::new(move |msg, _ctx, client_addr| match msg {
                NetworkRequests::BlockRequest { hash, peer_id } => {
                    let block = block_holder1.read().unwrap().clone().unwrap();
                    assert_eq!(hash, &block.hash());
                    actix::spawn(
                        client_addr
                            .send(NetworkClientMessages::Block(block, peer_id.clone(), false))
                            .map(drop),
                    );
                    NetworkResponses::NoResponse
                }
                NetworkRequests::BlockHeaderAnnounce { .. } => {
                    System::current().stop();
                    NetworkResponses::NoResponse
                }
                _ => NetworkResponses::NoResponse,
            }),
        );
        actix::spawn(view_client.send(GetBlock::Best).then(move |res| {
            let last_block = res.unwrap().unwrap();
            let signer = InMemorySigner::from_seed("test", KeyType::ED25519, "test");
            let block = Block::produce(
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
                0,
                0,
                None,
                vec![],
                vec![],
                &signer,
                0.into(),
                CryptoHash::default(),
                CryptoHash::default(),
                last_block.header.next_bp_hash,
            );
            client.do_send(NetworkClientMessages::BlockHeader(
                block.header.clone(),
                PeerInfo::random().id,
            ));
            *block_holder.write().unwrap() = Some(block);
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
            Box::new(move |msg, _ctx, _| {
                if let NetworkRequests::Block { block } = msg {
                    if block.header.num_approvals() == validators.len() as u64 - 1 {
                        System::current().stop();
                    } else {
                        println!(
                            "{:?}",
                            block
                                .header
                                .inner_rest
                                .approvals
                                .iter()
                                .map(|x| x.account_id.clone())
                                .collect::<Vec<_>>()
                        );
                        println!("{} != {} -1 ", block.header.num_approvals(), validators.len());
                        assert!(false);
                    }
                }
                NetworkResponses::NoResponse
            }),
        );
        actix::spawn(view_client.send(GetBlock::Best).then(move |res| {
            let last_block = res.unwrap().unwrap();
            let signer1 = InMemorySigner::from_seed("test2", KeyType::ED25519, "test2");
            let block = Block::produce(
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
                0,
                0,
                Some(0),
                vec![],
                vec![],
                &signer1,
                0.into(),
                CryptoHash::default(),
                CryptoHash::default(),
                last_block.header.next_bp_hash,
            );
            for i in 3..11 {
                let s = if i > 10 { "test1".to_string() } else { format!("test{}", i) };
                let signer = InMemorySigner::from_seed(&s, KeyType::ED25519, &s);
                let approval = Approval::new(block.hash(), block.hash(), &signer, s.to_string());
                client
                    .do_send(NetworkClientMessages::BlockApproval(approval, PeerInfo::random().id));
            }

            client.do_send(NetworkClientMessages::Block(block, PeerInfo::random().id, false));

            future::ready(())
        }));
        near_network::test_utils::wait_or_panic(5000);
    })
    .unwrap();
}

/// Sends 2 invalid blocks followed by valid block, and checks that client announces only valid block.
#[test]
fn invalid_blocks() {
    init_test_logger();
    System::run(|| {
        let (client, view_client) = setup_mock(
            vec!["test"],
            "other",
            false,
            Box::new(move |msg, _ctx, _client_actor| {
                match msg {
                    NetworkRequests::BlockHeaderAnnounce { header, approval_message } => {
                        assert_eq!(header.inner_lite.height, 1);
                        assert_eq!(
                            header.inner_lite.prev_state_root,
                            merklize(&vec![MerkleHash::default()]).0
                        );
                        assert_eq!(*approval_message, None);
                        System::current().stop();
                    }
                    _ => {}
                };
                NetworkResponses::NoResponse
            }),
        );
        actix::spawn(view_client.send(GetBlock::Best).then(move |res| {
            let last_block = res.unwrap().unwrap();
            let signer = InMemorySigner::from_seed("test", KeyType::ED25519, "test");
            // Send invalid state root.
            let mut block = Block::produce(
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
                0,
                0,
                Some(0),
                vec![],
                vec![],
                &signer,
                0.into(),
                CryptoHash::default(),
                CryptoHash::default(),
                last_block.header.next_bp_hash,
            );
            block.header.inner_lite.prev_state_root = hash(&[1]);
            client.do_send(NetworkClientMessages::Block(
                block.clone(),
                PeerInfo::random().id,
                false,
            ));
            // Send block that builds on invalid one.
            let block2 = Block::produce(
                &block.header.clone().into(),
                block.header.inner_lite.height + 1,
                block.chunks.clone(),
                EpochId::default(),
                if last_block.header.prev_hash == CryptoHash::default() {
                    EpochId(last_block.header.hash)
                } else {
                    EpochId(last_block.header.next_epoch_id.clone())
                },
                vec![],
                0,
                0,
                Some(0),
                vec![],
                vec![],
                &signer,
                0.into(),
                CryptoHash::default(),
                CryptoHash::default(),
                last_block.header.next_bp_hash,
            );
            client.do_send(NetworkClientMessages::Block(block2, PeerInfo::random().id, false));
            // Send proper block.
            let block3 = Block::produce(
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
                0,
                0,
                Some(0),
                vec![],
                vec![],
                &signer,
                0.into(),
                CryptoHash::default(),
                CryptoHash::default(),
                last_block.header.next_bp_hash,
            );
            client.do_send(NetworkClientMessages::Block(block3, PeerInfo::random().id, false));
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
            Box::new(move |msg, _ctx, _client_actor| {
                match msg {
                    NetworkRequests::Block { block } => {
                        if block.header.inner_lite.height > 3 {
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
                    score: 4.into(),
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
                    score: 4.into(),
                    tracked_shards: vec![],
                },
                edge_info: EdgeInfo::default(),
            }],
            sent_bytes_per_sec: 0,
            received_bytes_per_sec: 0,
            known_producers: vec![],
        }));
        wait_or_panic(2000);
    })
    .unwrap();
}

fn produce_blocks(client: &mut Client, num: u64) {
    for i in 1..num {
        let b = client.produce_block(i, Duration::from_millis(100)).unwrap().unwrap();
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
            block_hash: client.chain.genesis().hash(),
            actions: vec![],
        },
    );
    produce_blocks(&mut client, 12);
    assert_eq!(client.process_tx(tx), NetworkClientResponses::InvalidTx(InvalidTxError::Expired));
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
    assert_eq!(client.process_tx(tx2), NetworkClientResponses::InvalidTx(InvalidTxError::Expired));
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
        network_adapter,
        chain_genesis,
    );
    let signer = InMemorySigner::from_seed("test1", KeyType::ED25519, "test1");
    let genesis = client.chain.get_block_by_height(0).unwrap();
    let mut b1 = Block::empty_with_height(genesis, 1, &signer);
    b1.header.inner_lite.timestamp =
        to_timestamp(b1.header.timestamp() + chrono::Duration::seconds(60));
    let hash = hash(&b1.header.inner_rest.try_to_vec().expect("Failed to serialize"));
    b1.header.hash = hash;
    b1.header.signature = signer.sign(hash.as_ref());
    let _ = client.process_block(b1, Provenance::NONE);

    let b2 = client.produce_block(2, Duration::from_secs(1)).unwrap().unwrap();
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
        network_adapter,
        chain_genesis,
    );
    let signer = InMemorySigner::from_seed("test1", KeyType::ED25519, "test1");
    let genesis = client.chain.get_block_by_height(0).unwrap();
    let mut b1 = Block::empty_with_height(genesis, 1, &signer);
    b1.header.inner_rest.approvals = (0..100)
        .map(|i| Approval {
            account_id: format!("test{}", i).to_string(),
            reference_hash: genesis.hash(),
            parent_hash: genesis.hash(),
            signature: InMemorySigner::from_seed(
                &format!("test{}", i),
                KeyType::ED25519,
                &format!("test{}", i),
            )
            .sign(Approval::get_data_for_sig(&genesis.hash(), &genesis.hash()).as_ref()),
        })
        .collect();
    let hash = hash(&b1.header.inner_rest.try_to_vec().expect("Failed to serialize"));
    b1.header.hash = hash;
    b1.header.signature = signer.sign(hash.as_ref());
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
    let _ = env.clients[0].produce_block(1, Duration::from_millis(10)).unwrap().unwrap();
    // Second time producing with the same height should fail.
    assert_eq!(env.clients[0].produce_block(1, Duration::from_millis(10)).unwrap(), None);
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
        network_adapter,
        chain_genesis,
    );
    let signer = InMemorySigner::from_seed("test1", KeyType::ED25519, "test1");
    let genesis = client.chain.get_block_by_height(0).unwrap();
    let mut b1 = Block::empty_with_height(genesis, 1, &signer);
    b1.header.inner_rest.gas_price = 0;
    let hash = hash(&b1.header.inner_rest.try_to_vec().expect("Failed to serialize"));
    b1.header.hash = hash;
    b1.header.signature = signer.sign(hash.as_ref());

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
    let b1 = env.clients[0].produce_block(1, Duration::from_millis(10)).unwrap().unwrap();
    let _ = env.clients[0].process_block(b1.clone(), Provenance::PRODUCED);
    let signer = InMemorySigner::from_seed("test0", KeyType::ED25519, "test0");
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
    chain_genesis.gas_price_adjustment_rate = 10;
    let mut env = TestEnv::new(chain_genesis, 1, 1);
    for i in 1..=100 {
        env.produce_block(0, i);
    }
    let block = env.clients[0].chain.get_block_by_height(100).unwrap();
    assert!(block.header.inner_rest.gas_price >= min_gas_price);
}

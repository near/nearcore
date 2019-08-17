use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use actix::System;
use futures::{future, Future};

use near_chain::{Block, BlockApproval};
use near_chunks::{ChunkStatus, ShardsManager};
use near_client::test_utils::setup_mock;
use near_client::GetBlock;
use near_network::test_utils::wait_or_panic;
use near_network::types::{FullPeerInfo, NetworkInfo, PeerChainInfo};
use near_network::{NetworkClientMessages, NetworkRequests, NetworkResponses, PeerInfo};
use near_primitives::crypto::signer::InMemorySigner;
use near_primitives::hash::hash;
use near_primitives::merkle::merklize;
use near_primitives::sharding::EncodedShardChunk;
use near_primitives::test_utils::init_test_logger;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{EpochId, MerkleHash};

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
        let (client, _) = setup_mock(
            vec!["test"],
            "test",
            true,
            Box::new(move |msg, _ctx, _| {
                if let NetworkRequests::ChunkOnePartMessage { account_id: _, header_and_part } = msg
                {
                    let height = header_and_part.header.height_created as usize;
                    assert!(encoded_chunks.len() + 2 >= height);

                    // the following two lines must match data_parts and total_parts in KeyValueRuntimeAdapter
                    let data_parts = 12 + 2 * (((height - 1) as usize) % 4);
                    let total_parts = 1 + data_parts * (1 + ((height - 1) as usize) % 3);
                    if encoded_chunks.len() + 2 == height {
                        encoded_chunks.push(EncodedShardChunk::from_header(
                            header_and_part.header.clone(),
                            total_parts,
                        ));
                    }
                    encoded_chunks[height - 2].content.parts[header_and_part.part_id as usize] =
                        Some(header_and_part.part.clone());

                    if let ChunkStatus::Complete(_) = ShardsManager::check_chunk_complete(
                        data_parts,
                        total_parts,
                        &mut encoded_chunks[height - 2],
                    ) {
                        let chunk =
                            ShardsManager::decode_chunk(data_parts, &encoded_chunks[height - 2])
                                .unwrap();
                        if chunk.transactions.len() > 0 {
                            System::current().stop();
                        }
                    }
                }
                NetworkResponses::NoResponse
            }),
        );
        client.do_send(NetworkClientMessages::Transaction(SignedTransaction::empty()));
        near_network::test_utils::wait_or_panic(5000);
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
                if let NetworkRequests::BlockHeaderAnnounce { approval, .. } = msg {
                    assert!(approval.is_some());
                    System::current().stop();
                }
                NetworkResponses::NoResponse
            }),
        );
        actix::spawn(view_client.send(GetBlock::Best).then(move |res| {
            let last_block = res.unwrap().unwrap();
            let signer = Arc::new(InMemorySigner::from_seed("test1", "test1"));
            let block = Block::produce(
                &last_block.header,
                last_block.header.height + 1,
                last_block.chunks.clone(),
                EpochId::default(),
                vec![],
                HashMap::default(),
                0,
                0,
                signer,
            );
            client.do_send(NetworkClientMessages::Block(block, PeerInfo::random().id, false));
            future::result(Ok(()))
        }));
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
                    assert_eq!(hash.clone(), block.hash());
                    actix::spawn(
                        client_addr
                            .send(NetworkClientMessages::Block(block, peer_id.clone(), false))
                            .then(|_| futures::future::ok(())),
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
            let signer = Arc::new(InMemorySigner::from_seed("test", "test"));
            let block = Block::produce(
                &last_block.header,
                last_block.header.height + 1,
                last_block.chunks.clone(),
                EpochId::default(),
                vec![],
                HashMap::default(),
                0,
                0,
                signer,
            );
            client.do_send(NetworkClientMessages::BlockHeader(
                block.header.clone(),
                PeerInfo::random().id,
            ));
            *block_holder.write().unwrap() = Some(block);
            future::result(Ok(()))
        }));
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
                    if block.header.approval_sigs.len() == validators.len() - 2 {
                        System::current().stop();
                    }
                }
                NetworkResponses::NoResponse
            }),
        );
        actix::spawn(view_client.send(GetBlock::Best).then(move |res| {
            let last_block = res.unwrap().unwrap();
            let signer1 = Arc::new(InMemorySigner::from_seed("test2", "test2"));
            let block = Block::produce(
                &last_block.header,
                last_block.header.height + 1,
                last_block.chunks.clone(),
                EpochId::default(),
                vec![],
                HashMap::default(),
                0,
                0,
                signer1,
            );
            for i in 3..11 {
                let s = if i > 10 { "test1".to_string() } else { format!("test{}", i) };
                let signer = Arc::new(InMemorySigner::from_seed(&s, &s));
                let block_approval =
                    BlockApproval::new(block.hash(), &*signer, "test2".to_string());
                client.do_send(NetworkClientMessages::BlockApproval(
                    s.to_string(),
                    block_approval.hash,
                    block_approval.signature,
                    PeerInfo::random().id,
                ));
            }

            client.do_send(NetworkClientMessages::Block(block, PeerInfo::random().id, false));

            future::result(Ok(()))
        }));
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
                    NetworkRequests::BlockHeaderAnnounce { header, approval } => {
                        assert_eq!(header.height, 1);
                        assert_eq!(
                            header.prev_state_root,
                            merklize(&vec![MerkleHash::default()]).0
                        );
                        assert_eq!(*approval, None);
                        System::current().stop();
                    }
                    _ => {}
                };
                NetworkResponses::NoResponse
            }),
        );
        actix::spawn(view_client.send(GetBlock::Best).then(move |res| {
            let last_block = res.unwrap().unwrap();
            let signer = Arc::new(InMemorySigner::from_seed("test", "test"));
            // Send invalid state root.
            let mut block = Block::produce(
                &last_block.header,
                last_block.header.height + 1,
                last_block.chunks.clone(),
                EpochId::default(),
                vec![],
                HashMap::default(),
                0,
                0,
                signer.clone(),
            );
            block.header.prev_state_root = hash(&[1]);
            client.do_send(NetworkClientMessages::Block(
                block.clone(),
                PeerInfo::random().id,
                false,
            ));
            // Send block that builds on invalid one.
            let block2 = Block::produce(
                &block.header,
                block.header.height + 1,
                block.chunks.clone(),
                EpochId::default(),
                vec![],
                HashMap::default(),
                0,
                0,
                signer.clone(),
            );
            client.do_send(NetworkClientMessages::Block(block2, PeerInfo::random().id, false));
            // Send proper block.
            let block3 = Block::produce(
                &last_block.header,
                last_block.header.height + 1,
                last_block.chunks.clone(),
                EpochId::default(),
                vec![],
                HashMap::default(),
                0,
                0,
                signer,
            );
            client.do_send(NetworkClientMessages::Block(block3, PeerInfo::random().id, false));
            future::result(Ok(()))
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
                        if block.header.height > 3 {
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
        let _ = setup_mock(
            vec!["test"],
            "other",
            false,
            Box::new(move |msg, _ctx, _client_actor| match msg {
                NetworkRequests::FetchInfo => NetworkResponses::Info(NetworkInfo {
                    num_active_peers: 1,
                    peer_max_count: 1,
                    most_weight_peers: vec![FullPeerInfo {
                        peer_info: peer_info1.clone(),
                        chain_info: PeerChainInfo {
                            genesis: Default::default(),
                            height: 5,
                            total_weight: 100.into(),
                        },
                    }],
                    sent_bytes_per_sec: 0,
                    received_bytes_per_sec: 0,
                    known_producers: vec![],
                }),
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
        wait_or_panic(1000);
    })
    .unwrap();
}

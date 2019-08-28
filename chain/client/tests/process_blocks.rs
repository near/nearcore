use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use actix::System;
use futures::{future, Future};

use near_chain::{Block, BlockApproval};
use near_client::test_utils::setup_mock;
use near_client::GetBlock;
use near_crypto::{InMemorySigner, KeyType, PublicKey, Signature};
use near_network::test_utils::wait_or_panic;
use near_network::types::{FullPeerInfo, NetworkInfo, PeerChainInfo};
use near_network::{NetworkClientMessages, NetworkRequests, NetworkResponses, PeerInfo};
use near_primitives::block::BlockHeader;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::test_utils::{init_integration_logger, init_test_logger};
use near_primitives::transaction::{SignedTransaction, Transaction};
use near_primitives::types::MerkleHash;

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
fn produce_blocks_with_tx() {
    let count = Arc::new(AtomicUsize::new(0));
    init_integration_logger();
    System::run(|| {
        let (client, view_client) = setup_mock(
            vec!["test"],
            "test",
            true,
            Box::new(move |msg, _ctx, _| {
                if let NetworkRequests::Block { block } = msg {
                    count.fetch_add(block.transactions.len(), Ordering::Relaxed);
                    if count.load(Ordering::Relaxed) >= 1 {
                        System::current().stop();
                    }
                }
                NetworkResponses::NoResponse
            }),
        );
        actix::spawn(view_client.send(GetBlock::Best).then(move |res| {
            let header: BlockHeader = res.unwrap().unwrap().header.into();
            let block_hash = header.hash;
            client.do_send(NetworkClientMessages::Transaction(SignedTransaction::new(
                Signature::empty(KeyType::ED25519),
                Transaction {
                    signer_id: "".to_string(),
                    public_key: PublicKey::empty(KeyType::ED25519),
                    nonce: 0,
                    receiver_id: "".to_string(),
                    block_hash,
                    actions: vec![],
                },
            )));
            future::ok(())
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
                if let NetworkRequests::BlockHeaderAnnounce { approval, .. } = msg {
                    assert!(approval.is_some());
                    System::current().stop();
                }
                NetworkResponses::NoResponse
            }),
        );
        actix::spawn(view_client.send(GetBlock::Best).then(move |res| {
            let last_block = res.unwrap().unwrap();
            let signer = Arc::new(InMemorySigner::from_seed("test1", KeyType::ED25519, "test1"));
            let block = Block::produce(
                &last_block.header.clone().into(),
                last_block.header.height + 1,
                MerkleHash::default(),
                CryptoHash::default(),
                vec![],
                HashMap::default(),
                vec![],
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
                    assert_eq!(hash, &block.hash());
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
            let signer = Arc::new(InMemorySigner::from_seed("test", KeyType::ED25519, "test"));
            let block = Block::produce(
                &last_block.header.clone().into(),
                last_block.header.height + 1,
                MerkleHash::default(),
                CryptoHash::default(),
                vec![],
                HashMap::default(),
                vec![],
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
    System::run(|| {
        let (client, view_client) = setup_mock(
            vec!["test3", "test1", "test2"],
            "test2",
            true,
            Box::new(move |msg, _ctx, _| {
                if let NetworkRequests::Block { block } = msg {
                    assert!(block.header.inner.approval_sigs.len() > 0);
                    System::current().stop();
                }
                NetworkResponses::NoResponse
            }),
        );
        actix::spawn(view_client.send(GetBlock::Best).then(move |res| {
            let last_block = res.unwrap().unwrap();
            let signer1 = Arc::new(InMemorySigner::from_seed("test1", KeyType::ED25519, "test1"));
            let signer3 = Arc::new(InMemorySigner::from_seed("test3", KeyType::ED25519, "test3"));
            let block = Block::produce(
                &last_block.header.clone().into(),
                last_block.header.height + 1,
                MerkleHash::default(),
                CryptoHash::default(),
                vec![],
                HashMap::default(),
                vec![],
                signer1,
            );
            let block_approval = BlockApproval::new(block.hash(), &*signer3, "test2".to_string());
            client.do_send(NetworkClientMessages::Block(block, PeerInfo::random().id, false));
            client.do_send(NetworkClientMessages::BlockApproval(
                "test3".to_string(),
                block_approval.hash,
                block_approval.signature,
            ));
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
                        assert_eq!(header.inner.height, 1);
                        assert_eq!(header.inner.prev_state_root, MerkleHash::default());
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
            let signer = Arc::new(InMemorySigner::from_seed("test", KeyType::ED25519, "test"));
            // Send invalid state root.
            let block = Block::produce(
                &last_block.header.clone().into(),
                last_block.header.height + 1,
                hash(&[0]),
                CryptoHash::default(),
                vec![],
                HashMap::default(),
                vec![],
                signer.clone(),
            );
            client.do_send(NetworkClientMessages::Block(
                block.clone(),
                PeerInfo::random().id,
                false,
            ));
            // Send block that builds on invalid one.
            let block2 = Block::produce(
                &block.header.clone().into(),
                block.header.inner.height + 1,
                hash(&[1]),
                CryptoHash::default(),
                vec![],
                HashMap::default(),
                vec![],
                signer.clone(),
            );
            client.do_send(NetworkClientMessages::Block(block2, PeerInfo::random().id, false));
            // Send proper block.
            let block3 = Block::produce(
                &last_block.header.clone().into(),
                last_block.header.height + 1,
                MerkleHash::default(),
                CryptoHash::default(),
                vec![],
                HashMap::default(),
                vec![],
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
                        if block.header.inner.height > 3 {
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
                NetworkRequests::FetchInfo { level: _ } => NetworkResponses::Info(NetworkInfo {
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
                    routes: None,
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

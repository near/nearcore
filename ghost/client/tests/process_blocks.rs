use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use actix::actors::mocker::Mocker;
use actix::{
    Actor, ActorContext, Addr, Arbiter, AsyncContext, Context, Recipient, System, WrapFuture,
};
use chrono::{DateTime, Utc};
use futures::{future, Future};

use near_chain::{test_utils::KeyValueRuntime, Block, BlockHeader, RuntimeAdapter};
use near_client::{ClientActor, ClientConfig, GetBlock};
use near_network::types::{FullPeerInfo, PeerChainInfo};
use near_network::{
    NetworkClientMessages, NetworkRequests, NetworkResponses, PeerInfo, PeerManagerActor,
};
use near_store::test_utils::create_test_store;
use primitives::crypto::signer::InMemorySigner;
use primitives::test_utils::init_test_logger;
use primitives::transaction::SignedTransaction;
use primitives::types::{AccountId, BlockId, MerkleHash};

type NetworkMock = Mocker<PeerManagerActor>;

fn setup(
    authorities: Vec<&str>,
    account_id: &str,
    skip_sync_wait: bool,
    recipient: Recipient<NetworkRequests>,
) -> ClientActor {
    let store = create_test_store();
    let runtime = Arc::new(KeyValueRuntime::new_with_authorities(
        store.clone(),
        authorities.into_iter().map(Into::into).collect(),
    ));
    let signer = Arc::new(InMemorySigner::from_seed(account_id, account_id));
    ClientActor::new(
        ClientConfig::test(skip_sync_wait),
        store,
        runtime,
        recipient,
        Some(signer.into()),
    )
    .unwrap()
}

fn setup_mock(
    authorities: Vec<&'static str>,
    account_id: &'static str,
    skip_sync_wait: bool,
    mut network_mock: Box<
        FnMut(&NetworkRequests, &mut Context<NetworkMock>, Addr<ClientActor>) -> NetworkResponses,
    >,
) -> Addr<ClientActor> {
    ClientActor::create(move |ctx| {
        let client_addr = ctx.address();
        let pm = NetworkMock::mock(Box::new(move |msg, ctx| {
            let msg = msg.downcast_ref::<NetworkRequests>().unwrap();
            let resp = network_mock(msg, ctx, client_addr.clone());
            Box::new(Some(resp))
        }))
        .start();
        setup(authorities, account_id, skip_sync_wait, pm.recipient())
    })
}

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
                if let NetworkRequests::BlockAnnounce { .. } = msg {
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
    init_test_logger();
    System::run(|| {
        let client = setup_mock(
            vec!["test"],
            "test",
            true,
            Box::new(move |msg, _ctx, _| {
                if let NetworkRequests::BlockAnnounce { block } = msg {
                    count.fetch_add(block.transactions.len(), Ordering::Relaxed);
                    if count.load(Ordering::Relaxed) >= 1 {
                        System::current().stop();
                    }
                }
                NetworkResponses::NoResponse
            }),
        );
        client.do_send(NetworkClientMessages::Transaction(SignedTransaction::empty()));
    })
    .unwrap();
}

/// Runs client that receives a block from network and announces header to the network.
#[test]
fn receive_network_block() {
    init_test_logger();
    System::run(|| {
        let client = setup_mock(
            vec!["test"],
            "other",
            true,
            Box::new(move |msg, _ctx, _| {
                if let NetworkRequests::BlockHeaderAnnounce { header } = msg {
                    System::current().stop();
                }
                NetworkResponses::NoResponse
            }),
        );
        actix::spawn(client.send(GetBlock::Best).then(move |res| {
            let last_block = res.unwrap().unwrap();
            let signer = Arc::new(InMemorySigner::from_seed("test", "test"));
            let block = Block::produce(&last_block.header, MerkleHash::default(), vec![], signer);
            client.do_send(NetworkClientMessages::Block(block, PeerInfo::random(), false));
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
        let client = setup_mock(
            vec!["test"],
            "other",
            true,
            Box::new(move |msg, ctx, client_addr| match msg {
                NetworkRequests::BlockRequest { hash, peer_info } => {
                    let block = block_holder1.read().unwrap().clone().unwrap();
                    assert_eq!(hash.clone(), block.hash());
                    actix::spawn(
                        client_addr
                            .send(NetworkClientMessages::Block(block, peer_info.clone(), false))
                            .then(|_| futures::future::ok(())),
                    );
                    NetworkResponses::NoResponse
                }
                NetworkRequests::BlockHeaderAnnounce { header } => {
                    System::current().stop();
                    NetworkResponses::NoResponse
                }
                _ => NetworkResponses::NoResponse,
            }),
        );
        actix::spawn(client.send(GetBlock::Best).then(move |res| {
            let last_block = res.unwrap().unwrap();
            let signer = Arc::new(InMemorySigner::from_seed("test", "test"));
            let block = Block::produce(&last_block.header, MerkleHash::default(), vec![], signer);
            client.do_send(NetworkClientMessages::BlockHeader(
                block.header.clone(),
                PeerInfo::random(),
            ));
            *block_holder.write().unwrap() = Some(block);
            future::result(Ok(()))
        }));
    })
    .unwrap();
}

/// Runs client that syncs with peers.
#[test]
fn client_sync() {
    init_test_logger();
    System::run(|| {
        let client = setup_mock(
            vec!["test"],
            "other",
            false,
            Box::new(move |msg, _ctx, _client_actor| match msg {
                NetworkRequests::FetchInfo => {
                    System::current().stop();
                    NetworkResponses::Info {
                        num_active_peers: 1,
                        peer_max_count: 1,
                        max_weight_peer: Some(FullPeerInfo {
                            peer_info: PeerInfo::random(),
                            chain_info: PeerChainInfo { height: 5, total_weight: 100.into() },
                        }),
                    }
                }
                // NetworkRequests::
                _ => NetworkResponses::NoResponse,
            }),
        );
    })
    .unwrap();
}

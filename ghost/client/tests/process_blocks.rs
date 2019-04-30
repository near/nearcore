use actix::actors::mocker::Mocker;
use actix::{Actor, Addr, Arbiter, Context, Recipient, System};
use chrono::{DateTime, Utc};
use futures::{future, Future};
use near_chain::{test_utils::KeyValueRuntime, Block, BlockHeader, RuntimeAdapter};
use near_client::{ClientActor, ClientConfig, GetBlock};
use near_network::{
    NetworkClientMessages, NetworkRequests, NetworkResponses, PeerInfo, PeerManagerActor,
};
use near_store::test_utils::create_test_store;
use primitives::crypto::signer::InMemorySigner;
use primitives::test_utils::init_test_logger;
use primitives::transaction::SignedTransaction;
use primitives::types::{AccountId, BlockId, MerkleHash};
use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

type NetworkMock = Mocker<PeerManagerActor>;

fn setup(
    authorities: Vec<&str>,
    account_id: &str,
    skip_sync_wait: bool,
    recipient: Recipient<NetworkRequests>,
) -> Addr<ClientActor> {
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
    .start()
}

fn setup_mock(
    authorities: Vec<&str>,
    account_id: &str,
    skip_sync_wait: bool,
    mut network_mock: Box<FnMut(&NetworkRequests, &mut Context<NetworkMock>) -> NetworkResponses>,
) -> Addr<ClientActor> {
    let pm = NetworkMock::mock(Box::new(move |msg, ctx| {
        let msg = msg.downcast_ref::<NetworkRequests>().unwrap();
        let resp = network_mock(msg, ctx);
        Box::new(Some(resp))
    }))
    .start();
    setup(authorities, account_id, skip_sync_wait, pm.recipient())
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
            Box::new(move |msg, _ctx| {
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
            Box::new(move |msg, _ctx| {
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
            Box::new(move |msg, _ctx| {
                if let NetworkRequests::BlockHeaderAnnounce { header } = msg {
                    System::current().stop();
                }
                NetworkResponses::NoResponse
            }),
        );
        actix::spawn(client.send(GetBlock::Best).then(move |res| {
            let last_block = res.unwrap().unwrap();
            let block = Block::produce(&last_block.header, MerkleHash::default(), vec![]);
            client.do_send(NetworkClientMessages::Block(block, PeerInfo::random(), false));
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
            Box::new(move |msg, _ctx| match msg {
                NetworkRequests::FetchInfo => {
                    System::current().stop();
                    NetworkResponses::Info { num_active_peers: 1, peer_max_count: 1 }
                }
                _ => NetworkResponses::NoResponse,
            }),
        );
    });
}

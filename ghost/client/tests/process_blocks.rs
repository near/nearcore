use actix::actors::mocker::Mocker;
use actix::{Actor, Addr, Context, System, Recipient, Arbiter};
use futures::{future, Future};
use chrono::{DateTime, Utc};
use near_chain::{test_utils::KeyValueRuntime, Block, BlockHeader, RuntimeAdapter};
use near_client::{ClientActor, ClientConfig, GetBlock};
use near_network::{NetworkRequests, PeerInfo, PeerManagerActor, NetworkMessages, NetworkResponses};
use near_store::test_utils::create_test_store;
use primitives::crypto::signer::InMemorySigner;
use primitives::test_utils::init_test_logger;
use primitives::transaction::SignedTransaction;
use primitives::types::{AccountId, MerkleHash, BlockId};
use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

type NetworkMock = Mocker<PeerManagerActor>;

fn setup(authorities: Vec<&str>, account_id: &str, recipient: Recipient<NetworkRequests>) -> Addr<ClientActor> {
    let store = create_test_store();
    let runtime = Arc::new(KeyValueRuntime::new_with_authorities(
        store.clone(),
        authorities.into_iter().map(Into::into).collect(),
    ));
    let signer = Arc::new(InMemorySigner::from_seed(account_id, account_id));
    ClientActor::new(ClientConfig::test(), store, runtime, recipient, Some(signer.into()))
        .unwrap()
        .start()
}

fn setup_mock(
    authorities: Vec<&str>,
    account_id: &str,
    mut network_mock: Box<FnMut(&NetworkRequests, &mut Context<NetworkMock>) -> NetworkResponses>,
) -> Addr<ClientActor> {
    let pm = NetworkMock::mock(Box::new(move |msg, ctx| {
        let msg = msg.downcast_ref::<NetworkRequests>().unwrap();
        let resp = network_mock(msg, ctx);
        Box::new(Some(resp))
    }))
    .start();
    setup(authorities, account_id, pm.recipient())
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
        client.do_send(NetworkMessages::Transaction(SignedTransaction::empty()));
    })
    .unwrap();
}

// Runs client that receives a block from network and announces header to the network.
#[test]
fn receive_network_block() {
    init_test_logger();
    System::run(|| {
        let client = setup_mock(vec!["test"], "other", Box::new(move |msg, _ctx| {
            if let NetworkRequests::BlockHeaderAnnounce { header } = msg {
                System::current().stop();
            }
            NetworkResponses::NoResponse
        }));
        actix::spawn(client.send(GetBlock::Best).then(move |res| {
            let last_block = res.unwrap().unwrap();
            let block = Block::produce(&last_block.header, MerkleHash::default(), vec![]);
            client.do_send(NetworkMessages::Block(block, PeerInfo::random(), false));
            future::result(Ok(()))
        }));
    }).unwrap();
}

// Runs two clients that produce blocks.
//#[test]
//fn two_clients() {
//    let count = Arc::new(AtomicUsize::new(0));
//    init_test_logger();
//    System::run(|| {
//        let authorities = vec!["test1", "test2"];
//        let client1 = setup_mock(
//            authorities.clone(),
//            "test1",
//            Box::new(move |msg, _ctx| {
//                if let NetworkRequests::BlockAnnounce { block } = msg {
//                    count.fetch_add(block.transactions.len(), Ordering::Relaxed);
//                    if count.load(Ordering::Relaxed) >= 1 {
//                        System::current().stop();
//                    }
//                }
//            }),
//        );
//        let client2 = setup_mock(
//            authorities,
//            "test2",
//            Box::new(move |msg, _ctx| {
//                //            if let NetworkRequests::BlockAnnounce { block } = msg {
//                //                count.fetch_add(block.transactions.len(), Ordering::Relaxed);
//                //                if count.load(Ordering::Relaxed) >= 1 {
//                //                    System::current().stop();
//                //                }
//                //            }
//            }),
//        );
//        client1.do_send(NetworkMessages::Transaction(SignedTransaction::empty()));
//    })
//    .unwrap();
//}

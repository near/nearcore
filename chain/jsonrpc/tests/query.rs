use actix::{Actor, Addr, System};
use futures::future;
use futures::future::Future;
use protobuf::Message;

use near_client::test_utils::setup_no_network;
use near_client::ViewClientActor;
use near_jsonrpc::client::new_client;
use near_jsonrpc::{start_http, RpcConfig};
use near_network::test_utils::{open_port, wait_or_panic, WaitOrTimeout};
use near_primitives::crypto::signer::InMemorySigner;
use near_primitives::serialize::to_base;
use near_primitives::test_utils::init_test_logger;
use near_primitives::transaction::{FinalTransactionStatus, TransactionBody};
use near_protos::signed_transaction as transaction_proto;

fn start_all(validator: bool) -> (Addr<ViewClientActor>, String) {
    let (client_addr, view_client_addr) =
        setup_no_network(vec!["test1", "test2"], if validator { "test1" } else { "other" }, true);

    let addr = format!("127.0.0.1:{}", open_port());
    start_http(RpcConfig::new(&addr), client_addr.clone(), view_client_addr.clone());
    (view_client_addr, addr)
}

// TODO: move to another file
/// Test sending transaction via json rpc without waiting.
#[test]
fn test_send_tx_async() {
    init_test_logger();

    System::run(|| {
        let (_view_client_addr, addr) = start_all(true);

        let mut client = new_client(&format!("http://{}", addr));
        let signer = InMemorySigner::from_seed("test1", "test1");
        let tx = TransactionBody::send_money(1, "test1", "test2", 100).sign(&signer);
        let tx_hash: String = (&tx.get_hash()).into();
        let tx_hash2 = tx_hash.clone();
        let proto: transaction_proto::SignedTransaction = tx.into();
        actix::spawn(
            client
                .broadcast_tx_async(to_base(&proto.write_to_bytes().unwrap()))
                .map_err(|_| ())
                .map(move |result| assert_eq!(tx_hash, result)),
        );
        WaitOrTimeout::new(
            Box::new(move |_| {
                actix::spawn(
                    client.tx(tx_hash2.clone()).map_err(|err| println!("Error: {:?}", err)).map(
                        |result| {
                            if result.status == FinalTransactionStatus::Completed {
                                System::current().stop();
                            }
                        },
                    ),
                )
            }),
            100,
            1000,
        )
        .start();
    })
    .unwrap();
}

/// Test sending transaction and waiting for it to be committed to a block.
#[test]
fn test_send_tx_commit() {
    init_test_logger();

    System::run(|| {
        let (_view_client_addr, addr) = start_all(true);

        let mut client = new_client(&format!("http://{}", addr));
        let signer = InMemorySigner::from_seed("test1", "test1");
        let tx = TransactionBody::send_money(1, "test1", "test2", 100).sign(&signer);
        let proto: transaction_proto::SignedTransaction = tx.into();
        actix::spawn(
            client
                .broadcast_tx_commit(to_base(&proto.write_to_bytes().unwrap()))
<<<<<<< HEAD
                .map_err(|why| {
                    System::current().stop();
                    panic!(why);
                })
=======
                .map_err(|_| ())
>>>>>>> Ref #833: Removed ToBytes trait, cleaned up usage of BaseEncoder/From/Into traits.
                .map(move |result| {
                    assert_eq!(result.status, FinalTransactionStatus::Completed);
                    System::current().stop();
                }),
        );
        wait_or_panic(10000);
    })
    .unwrap();
}

/// Retrieve blocks via json rpc
#[test]
fn test_block() {
    init_test_logger();

    System::run(|| {
        let (_view_client_addr, addr) = start_all(false);

        let mut client = new_client(&format!("http://{}", addr));
        actix::spawn(client.block(0).then(|res| {
            assert_eq!(res.unwrap().header.height, 0);
            System::current().stop();
            future::result(Ok(()))
        }));
    })
    .unwrap();
}

/// Connect to json rpc and query it.
#[test]
fn test_query() {
    init_test_logger();

    System::run(|| {
        let (_view_client_addr, addr) = start_all(false);

        let mut client = new_client(&format!("http://{}", addr));
        actix::spawn(client.query("account/test".to_string(), vec![]).then(|res| {
            assert!(res.is_ok());
            System::current().stop();
            future::result(Ok(()))
        }));
    })
    .unwrap();
}

/// Retrieve client status.
#[test]
fn test_status() {
    init_test_logger();

    System::run(|| {
        let (_view_client_addr, addr) = start_all(false);

        let mut client = new_client(&format!("http://{}", addr));
        actix::spawn(client.status().then(|res| {
            let res = res.unwrap();
            assert_eq!(res.chain_id, "unittest");
            assert_eq!(res.sync_info.latest_block_height, 0);
            assert_eq!(res.sync_info.syncing, false);
            System::current().stop();
            future::result(Ok(()))
        }));
    })
    .unwrap();
}

/// Check health fails when node is absent.
#[test]
fn test_health_fail() {
    init_test_logger();

    System::run(|| {
        let mut client = new_client(&"http://127.0.0.1:12322/health");
        actix::spawn(client.health().then(|res| {
            assert!(res.is_err());
            System::current().stop();
            future::result(Ok(()))
        }));
    })
    .unwrap();
}

/// Retrieve client health.
#[test]
fn test_health_ok() {
    init_test_logger();

    System::run(|| {
        let (_view_client_addr, addr) = start_all(false);

        let mut client = new_client(&format!("http://{}", addr));
        actix::spawn(client.health().then(|res| {
            assert!(res.is_ok());
            System::current().stop();
            future::result(Ok(()))
        }));
    })
    .unwrap();
}

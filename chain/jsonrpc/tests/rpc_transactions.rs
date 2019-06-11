use actix::{Actor, System};
use futures::future::Future;
use protobuf::Message;

use near_jsonrpc::client::new_client;
use near_jsonrpc::test_utils::start_all;
use near_network::test_utils::{wait_or_panic, WaitOrTimeout};
use near_primitives::crypto::signer::InMemorySigner;
use near_primitives::serialize::to_base;
use near_primitives::test_utils::init_test_logger;
use near_primitives::transaction::{FinalTransactionStatus, TransactionBody};
use near_protos::signed_transaction as transaction_proto;

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
                .map_err(|why| {
                    System::current().stop();
                    panic!(why);
                })
                .map(move |result| {
                    assert_eq!(result.status, FinalTransactionStatus::Completed);
                    System::current().stop();
                }),
        );
        wait_or_panic(10000);
    })
    .unwrap();
}

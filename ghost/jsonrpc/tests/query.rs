use actix::{Actor, System};
use futures::future;
use futures::future::Future;
use protobuf::Message;

use near_client::test_utils::setup_mock;
use near_client::GetBlock;
use near_jsonrpc::client::new_client;
use near_jsonrpc::start_http;
use near_network::test_utils::{open_port, WaitOrTimeout};
use near_network::NetworkResponses;
use near_protos::signed_transaction as transaction_proto;
use primitives::crypto::signer::InMemorySigner;
use primitives::test_utils::init_test_logger;
use primitives::transaction::TransactionBody;

// TODO: move to another file
#[test]
fn test_send_tx() {
    init_test_logger();

    System::run(|| {
        let client_addr = setup_mock(
            vec!["test1", "test2"],
            "test1",
            true,
            Box::new(move |_msg, _ctx, _| NetworkResponses::NoResponse),
        );

        let addr = format!("127.0.0.1:{}", open_port());
        start_http(&addr, client_addr.clone());

        let mut client = new_client(&format!("http://{}", addr));
        let signer = InMemorySigner::from_seed("test1", "test1");
        let tx = TransactionBody::send_money(1, "test1", "test2", 100).sign(&signer);
        let tx: transaction_proto::SignedTransaction = tx.into();
        assert!(client.broadcast_tx_sync(tx.write_to_bytes().unwrap()).call().is_ok());
        WaitOrTimeout::new(
            Box::new(move |_| {
                actix::spawn(client_addr.send(GetBlock::Best).then(move |res| {
                    let last_block = res.unwrap().unwrap();
                    // TODO: something better then checking that recent blocks have tx.
                    //                if last_block.transactions.len() != 0 {
                    if last_block.header.height > 3 {
                        System::current().stop();
                    }
                    future::result(Ok(()))
                }))
            }),
            100,
            1000,
        )
        .start();
    })
    .unwrap();
}

/// Connect to json rpc and query it.
#[test]
fn test_query() {
    init_test_logger();

    System::run(|| {
        let client_addr = setup_mock(
            vec!["test"],
            "test",
            true,
            Box::new(move |_msg, _ctx, _| NetworkResponses::NoResponse),
        );

        let addr = format!("127.0.0.1:{}", open_port());
        start_http(&addr, client_addr);

        let mut client = new_client(&format!("http://{}", addr));
        let result =
            client.query("account/test".to_string(), vec![], "0".to_string(), false).call();
        println!("res: {:?}", result);
        System::current().stop();
    })
    .unwrap();
}

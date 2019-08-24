use std::sync::{Arc, Mutex};

use actix::{Actor, System};
use borsh::Serializable;
use futures::future::Future;

use near_client::GetBlock;
use near_jsonrpc::client::new_client;
use near_jsonrpc::test_utils::start_all;
use near_network::test_utils::{wait_or_panic, WaitOrTimeout};
use near_primitives::block::BlockHeader;
use near_primitives::crypto::signer::InMemorySigner;
use near_primitives::serialize::to_base64;
use near_primitives::test_utils::init_test_logger;
use near_primitives::transaction::SignedTransaction;
use near_primitives::views::FinalTransactionStatus;

/// Test sending transaction via json rpc without waiting.
#[test]
fn test_send_tx_async() {
    init_test_logger();

    System::run(|| {
        let (view_client, addr) = start_all(true);

        let mut client = new_client(&format!("http://{}", addr.clone()));
        let validity_period = 10;

        let tx_hash2 = Arc::new(Mutex::new(None));
        let tx_hash2_1 = tx_hash2.clone();
        let tx_hash2_2 = tx_hash2.clone();

        actix::spawn(view_client.send(GetBlock::Best).then(move |res| {
            let header: BlockHeader = res.unwrap().unwrap().header.into();
            let block_hash = header.hash;
            let signer = InMemorySigner::from_seed("test1", "test1");
            let tx = SignedTransaction::send_money(
                1,
                "test1".to_string(),
                "test2".to_string(),
                Arc::new(signer),
                100,
                block_hash,
                validity_period,
            );
            let bytes = tx.try_to_vec().unwrap();
            let tx_hash: String = (&tx.get_hash()).into();
            *tx_hash2_1.lock().unwrap() = Some(tx.get_hash());
            client
                .broadcast_tx_async(to_base64(&bytes))
                .map_err(|_| ())
                .map(move |result| assert_eq!(tx_hash, result))
        }));
        let mut client1 = new_client(&format!("http://{}", addr));
        WaitOrTimeout::new(
            Box::new(move |_| {
                if let Some(tx_hash) = *tx_hash2_2.lock().unwrap() {
                    actix::spawn(
                        client1
                            .tx((&tx_hash).into())
                            .map_err(|err| println!("Error: {:?}", err))
                            .map(|result| {
                                if result.status == FinalTransactionStatus::Completed {
                                    System::current().stop();
                                }
                            }),
                    )
                }
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
        let (view_client, addr) = start_all(true);

        let mut client = new_client(&format!("http://{}", addr));

        actix::spawn(view_client.send(GetBlock::Best).then(move |res| {
            let header: BlockHeader = res.unwrap().unwrap().header.into();
            let block_hash = header.hash;
            let signer = InMemorySigner::from_seed("test1", "test1");
            let tx = SignedTransaction::send_money(
                1,
                "test1".to_string(),
                "test2".to_string(),
                Arc::new(signer),
                100,
                block_hash,
                10,
            );
            let bytes = tx.try_to_vec().unwrap();
            client
                .broadcast_tx_commit(to_base64(&bytes))
                .map_err(|why| {
                    System::current().stop();
                    panic!(why);
                })
                .map(move |result| {
                    assert_eq!(result.status, FinalTransactionStatus::Completed);
                    System::current().stop();
                })
        }));
        wait_or_panic(10000);
    })
    .unwrap();
}

use std::sync::{Arc, Mutex};

use actix::{Actor, System};
use borsh::BorshSerialize;
use futures::{future, FutureExt, TryFutureExt};

use near_client::GetBlock;
use near_crypto::{InMemorySigner, KeyType};
use near_jsonrpc::client::new_client;
use near_jsonrpc::test_utils::{start_all, start_all_with_validity_period};
use near_network::test_utils::{wait_or_panic, WaitOrTimeout};
use near_primitives::block::BlockHeader;
use near_primitives::hash::hash;
use near_primitives::serialize::to_base64;
use near_primitives::test_utils::{init_integration_logger, init_test_logger};
use near_primitives::transaction::SignedTransaction;
use near_primitives::views::FinalExecutionStatus;

/// Test sending transaction via json rpc without waiting.
#[test]
fn test_send_tx_async() {
    init_test_logger();

    System::run(|| {
        let (view_client, addr) = start_all(true);

        let mut client = new_client(&format!("http://{}", addr.clone()));

        let tx_hash2 = Arc::new(Mutex::new(None));
        let tx_hash2_1 = tx_hash2.clone();
        let tx_hash2_2 = tx_hash2.clone();
        let signer_account_id = "test1".to_string();

        actix::spawn(view_client.send(GetBlock::Best).then(move |res| {
            let header: BlockHeader = res.unwrap().unwrap().header.into();
            let block_hash = header.hash;
            let signer = InMemorySigner::from_seed("test1", KeyType::ED25519, "test1");
            let tx = SignedTransaction::send_money(
                1,
                signer_account_id,
                "test2".to_string(),
                &signer,
                100,
                block_hash,
            );
            let bytes = tx.try_to_vec().unwrap();
            let tx_hash: String = (&tx.get_hash()).into();
            *tx_hash2_1.lock().unwrap() = Some(tx.get_hash());
            client
                .broadcast_tx_async(to_base64(&bytes))
                .map_ok(move |result| assert_eq!(tx_hash, result))
                .map(drop)
        }));
        let mut client1 = new_client(&format!("http://{}", addr));
        WaitOrTimeout::new(
            Box::new(move |_| {
                let signer_account_id = "test1".to_string();
                if let Some(tx_hash) = *tx_hash2_2.lock().unwrap() {
                    actix::spawn(
                        client1
                            .tx((&tx_hash).into(), signer_account_id)
                            .map_err(|err| println!("Error: {:?}", err))
                            .map_ok(|result| {
                                if let FinalExecutionStatus::SuccessValue(_) = result.status {
                                    System::current().stop();
                                }
                            })
                            .map(drop),
                    )
                }
            }),
            100,
            2000,
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
            let signer = InMemorySigner::from_seed("test1", KeyType::ED25519, "test1");
            let tx = SignedTransaction::send_money(
                1,
                "test1".to_string(),
                "test2".to_string(),
                &signer,
                100,
                block_hash,
            );
            let bytes = tx.try_to_vec().unwrap();
            client
                .broadcast_tx_commit(to_base64(&bytes))
                .map_err(|why| {
                    System::current().stop();
                    panic!(why);
                })
                .map_ok(move |result| {
                    assert_eq!(result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
                    System::current().stop();
                })
                .map(drop)
        }));
        wait_or_panic(10000);
    })
    .unwrap();
}

/// Test that expired transaction should be rejected
#[test]
fn test_expired_tx() {
    init_integration_logger();
    System::run(|| {
        let (view_client, addr) = start_all_with_validity_period(true, 1, false);

        let block_hash = Arc::new(Mutex::new(None));
        let block_height = Arc::new(Mutex::new(None));

        WaitOrTimeout::new(
            Box::new(move |_| {
                let block_hash = block_hash.clone();
                let block_height = block_height.clone();
                let mut client = new_client(&format!("http://{}", addr));
                actix::spawn(view_client.send(GetBlock::Best).then(move |res| {
                    let header: BlockHeader = res.unwrap().unwrap().header.into();
                    let hash = block_hash.lock().unwrap().clone();
                    let height = block_height.lock().unwrap().clone();
                    if let Some(block_hash) = hash {
                        if let Some(height) = height {
                            if header.inner_lite.height - height >= 2 {
                                let signer =
                                    InMemorySigner::from_seed("test1", KeyType::ED25519, "test1");
                                let tx = SignedTransaction::send_money(
                                    1,
                                    "test1".to_string(),
                                    "test2".to_string(),
                                    &signer,
                                    100,
                                    block_hash,
                                );
                                let bytes = tx.try_to_vec().unwrap();
                                actix::spawn(
                                    client
                                        .broadcast_tx_commit(to_base64(&bytes))
                                        .map_err(|_| {
                                            System::current().stop();
                                        })
                                        .map(|_| ()),
                                );
                            }
                        }
                    } else {
                        *block_hash.lock().unwrap() = Some(header.hash);
                        *block_height.lock().unwrap() = Some(header.inner_lite.height);
                    };
                    future::ready(())
                }));
            }),
            100,
            1000,
        )
        .start();
    })
    .unwrap();
}

/// Test sending transaction based on a different fork should be rejected
#[test]
fn test_replay_protection() {
    init_test_logger();

    System::run(|| {
        let (_, addr) = start_all(true);

        let mut client = new_client(&format!("http://{}", addr));
        let signer = InMemorySigner::from_seed("test1", KeyType::ED25519, "test1");
        let tx = SignedTransaction::send_money(
            1,
            "test1".to_string(),
            "test2".to_string(),
            &signer,
            100,
            hash(&[1]),
        );
        let bytes = tx.try_to_vec().unwrap();
        actix::spawn(
            client
                .broadcast_tx_commit(to_base64(&bytes))
                .map_err(|_| {
                    System::current().stop();
                })
                .map(move |_| panic!("transaction should not succeed")),
        );
        wait_or_panic(10000);
    })
    .unwrap();
}
